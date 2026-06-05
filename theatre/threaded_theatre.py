from __future__ import annotations

import enum
import itertools
import logging
import os
import queue
import threading
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import (
    CancelledError,
    Executor,
    Future,
    ThreadPoolExecutor,
)
from contextvars import copy_context
from dataclasses import dataclass, field
from typing import Any

from theatre.interfaces import Actor, ActorSheet, Address, System

logger = logging.getLogger(__name__)


class RequestCancelled(Exception):
    def __init__(self, req):
        self.req = req


class MaxIdleException(Exception):
    def __init__(self, idle_count, max_idle):
        self.idle_count = idle_count
        self.max_idle = max_idle


class CancellableTask:
    __slots__ = ("_future", "_interrupt")

    def __init__(self, future: Future, interrupt: threading.Event | None = None):
        self._future = future
        self._interrupt = interrupt

    def cancel(self) -> bool:
        if self._interrupt is not None:
            self._interrupt.set()
        return self._future.cancel()

    def done(self) -> bool:
        return self._future.done()

    def cancelled(self) -> bool:
        return self._future.cancelled()

    def exception(self) -> BaseException | None:
        return self._future.exception()

    def result(self) -> Any:
        return self._future.result()

    @property
    def future(self) -> Future:
        return self._future


class ActorAddress(Address, tuple):
    def __new__(cls, pid, theatre_id, coroutine_id):
        return tuple.__new__(cls, (pid, theatre_id, coroutine_id))

    def __str__(self):
        return "#[{}]".format("-".join(map(str, self)))

    def __repr__(self):
        return "ActorAddress({})".format(", ".join(map(str, self)))


class State:
    @dataclass(frozen=True)
    class Init:
        """Actor freshly minted"""

    @dataclass(frozen=True)
    class Waiting:
        """Actor yielded a request, not yet dispatched"""

        request: object

    @dataclass(frozen=True)
    class Awaiting:
        """Request dispatched, waiting for response_future to complete"""

        request: object
        response_future: CancellableTask

    @dataclass(frozen=True)
    class Executing:
        """Request fulfilled, actor executing until next yield"""

        future: CancellableTask

    @dataclass(frozen=True)
    class Terminated:
        """Actor finished execution"""

        cause: Exit | Signal

    @dataclass(frozen=True)
    class Receiving:
        """Actor waiting for a message in mailbox"""

        request: object
        timeout_task: CancellableTask


ActorState = (
    State.Init
    | State.Waiting
    | State.Awaiting
    | State.Executing
    | State.Receiving
    | State.Terminated
)


class Signal(enum.Enum):
    KILL = enum.auto()
    INT = enum.auto()
    TERM = enum.auto()


class _Signal(Exception):
    def __str__(self):
        return f"Signal.{type(self).__name__}"

    def __postinit__(self, *args):
        super().__init__(args)


class Signal:
    class KILL(_Signal): pass
    class INT(_Signal): pass

    @dataclass()
    class MailboxFull(_Signal):
        actor: ActorAddress

    @dataclass()
    class ActorTerminated(_Signal):
        actor: ActorAddress
        cause: Exit | _Signal


class Event:
    @dataclass(frozen=True)
    class ActorEvent:
        actor: ActorAddress

    @dataclass(frozen=True)
    class RequestCompleted(ActorEvent):
        request: Any
        future: Future

    @dataclass(frozen=True)
    class EndOfScene(ActorEvent):
        future: Future

    @dataclass(frozen=True)
    class Message(ActorEvent):
        message: Any
        sender: ActorAddress

    @dataclass(frozen=True)
    class ExternalRequest:
        request: Any
        result_future: Future

    class Stop(Exception):
        def __init__(self, reason: str):
            super().__init__(f"Stopping: {reason}")
            self.reason = reason

    @dataclass(frozen=True)
    class RegisterCondition:
        predicate: Callable[[Play], bool]
        projection: Callable[[Play], Any]
        future: Future

    @dataclass(frozen=True)
    class Signal:
        actor: ActorAddress
        signal: Signal

    @dataclass(frozen=True)
    class SignalAll:
        signal: Signal

    @dataclass(frozen=True)
    class LinkTrap:
        linker: ActorAddress
        linked: ActorAddress
        future: Future

    @dataclass(frozen=True)
    class ReceiveTimeout:
        actor: ActorAddress
        request: object
        timeout_task: CancellableTask


class RequestResult:
    """
    request handling resolution state
    """
    @dataclass
    class Terminate:
        cause: Exit | Signal

    @dataclass
    class AwaitFuture:
        request: object
        future: CancellableTask

    @dataclass
    class Park:
        request: object
        timeout_task: CancellableTask | None

    @dataclass
    class ResumeWithValue:
        value: Any

    @dataclass
    class ResumeWithError:
        exc: BaseException

@dataclass
class NormalExit:
    value: Any


class ErrorExit(Exception):
    __match_args__ = ("cause", "context")

    def __init__(self, cause, context=None):
        self.cause = cause
        self.context = context


Exit = NormalExit | ErrorExit


class DestinationNotFound(Exception):
    def __init__(self, destination: ActorAddress):
        self.destination = destination


class ActorCancelled(Exception):
    def __init__(self, actor: ActorAddress):
        self.actor = actor


class ActorTerminated(Exception):
    def __init__(self, actor: ActorAddress, cause: Exit | Signal):
        self.actor = actor
        self.cause = cause


class ReceiveTimeout(Exception):
    def __init__(self, request: object):
        self.request = request


class ActorSignaled(Exception):
    def __init__(self, actor: ActorAddress, signal: Signal):
        self.actor = actor
        self.signal = signal


class MailboxFull(Exception):
    pass


class _NoMatch(Exception):
    pass


class Mailbox:
    def __init__(self, maxlen: int):
        self._items: deque[Any] = deque(maxlen=maxlen)
        self._maxlen: int = maxlen

    def append(self, msg: Any) -> None:
        if len(self._items) >= self._maxlen:
            raise MailboxFull()
        self._items.append(msg)

    def pop_matching(self, filter_fn: Callable[[Any], bool] | None = None) -> Any:
        if filter_fn is None:
            if self._items:
                return self._items.popleft()
            raise _NoMatch()
        for i, msg in enumerate(self._items):
            if filter_fn(msg):
                del self._items[i]
                return msg
        raise _NoMatch()

    def __len__(self) -> int:
        return len(self._items)

    def __bool__(self) -> bool:
        return bool(self._items)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._items)


class UnsupportedRequest(Exception):
    def __init__(self, actor: ActorAddress, req: Any):
        self.actor = actor
        self.request = req


def curtain_call(**kwargs):
    kwargs.setdefault("executor", ThreadPoolExecutor())
    return Theatre(**kwargs)


def drain(queue_: queue.Queue[Event], timeout=None) -> Iterator[Event]:
    try:
        event = queue_.get(timeout=timeout)
    except queue.Empty:
        return
    else:
        yield event
    while True:
        try:
            event = queue_.get_nowait()
        except queue.Empty:
            return
        else:
            yield event


@dataclass
class Play:
    states: dict[ActorAddress, ActorState]
    actors: dict[ActorAddress, ActorSheet]
    conditions: list[Event.RegisterCondition] = field(default_factory=list)
    runnable: deque[ActorAddress] = field(default_factory=deque)


@dataclass
class Stage:
    executor: Executor
    events: queue.Queue
    logger: logging.Logger

    def submit_performance(self, addr, fn, *args, interrupt=None):
        self.logger.debug(
            f"submitting performance for actor {addr}: {fn.__qualname__}{args!r}"
        )
        fut = self.executor.submit(fn, *args)
        task = CancellableTask(future=fut, interrupt=interrupt)
        fut.add_done_callback(
            lambda f: self.events.put(Event.EndOfScene(actor=addr, future=task))
        )
        return task

    def submit_request(self, addr, request, fn, *args, interrupt=None):
        self.logger.debug(
            f"submitting request for actor {addr}: {request!r} ({fn.__qualname__}{args})"
        )
        fut = self.executor.submit(fn, *args)
        task = CancellableTask(future=fut, interrupt=interrupt)
        fut.add_done_callback(
            lambda f: self.events.put(
                Event.RequestCompleted(actor=addr, request=request, future=task)
            )
        )
        return task


class StateMachine:
    def cancel_pending_task(self, actor, play):
        state = play.states[actor]
        logger.debug(f"actor({actor}): cancelling pending tasks for state {state}")
        match state:
            case State.Receiving(timeout_task=tfut):
                if tfut:
                    tfut.cancel()
            case (
                State.Awaiting(response_future=future)
                | State.Executing(future)
            ):
                future.cancel()
            case _:
                pass

    def terminate(self, addr, cause, play):
        self.cancel_pending_task(addr, play)
        play.states[addr] = State.Terminated(cause=cause)

    def await_future(self, addr, request, future, play):
        assert isinstance(play.states[addr], State.Waiting)
        play.states[addr] = State.Awaiting(request=request, response_future=future)

    def park(self, addr, request, timeout_task, play):
        assert isinstance(play.states[addr], State.Waiting)
        play.states[addr] = State.Receiving(request=request, timeout_task=timeout_task)

    def initiate(self, addr, play, stage):
        assert addr not in play.states
        play.states[addr] = State.Init()

    def resume_with_value(self, addr, value, play, stage):
        assert not isinstance(play.states[addr], State.Executing)
        sheet = play.actors[addr]
        future = stage.submit_performance(addr, sheet.performance.send, value)
        play.states[addr] = State.Executing(future=future)

    def resume_with_error(self, addr, exc, play, stage):
        assert not isinstance(play.states[addr], State.Executing)
        sheet = play.actors[addr]
        future = stage.submit_performance(addr, sheet.performance.throw, exc)
        play.states[addr] = State.Executing(future=future)

    def interrupt(self, addr, exc, play, stage):
        state = play.states[addr]
        stage.logger.debug("interrupting actor(%s) in state %s", addr, state)
        sheet = play.actors[addr]
        match state:
            case State.Executing(future=fut):
                def exec_chain():
                    result = fut.result()
                    stage.logger.debug("actor(%s): dismissing request %s to signal interruption", addr, result)
                    return sheet.performance.throw(exc)

                exec_future = stage.submit_performance(addr, exec_chain)
                play.states[addr] = State.Executing(future=exec_future)
            case _:
                self.cancel_pending_task(addr, play)
                if not isinstance(state, State.Terminated):
                    exec_future = stage.submit_performance(
                        addr, sheet.performance.throw, exc
                    )
                    play.states[addr] = State.Executing(future=exec_future)

    def process(self, addr, play, stage, request_handler) -> bool:
        stage.logger.debug("processing state of actor(%s)", addr)
        if addr not in play.states:
            return False
        state = play.states[addr]
        stage.logger.debug("actor(%s) in state %s", addr, state)
        sheet = play.actors[addr]

        match state:
            case State.Init():
                self.resume_with_value(addr, None, play, stage)
                return True

            case State.Waiting(request=req):
                match request_handler(addr, req):
                    case RequestResult.Terminate(cause):
                        self.terminate(addr, cause, play)
                    case RequestResult.AwaitFuture(request, future):
                        self.await_future(addr, request, future, play)
                    case RequestResult.Park(request, timeout_task):
                        self.park(addr, request, timeout_task, play)
                    case RequestResult.ResumeWithValue(value):
                        self.resume_with_value(addr, value, play, stage)
                    case RequestResult.ResumeWithError(exc):
                        self.resume_with_error(addr, exc, play, stage)
                    case result:
                        raise RuntimeError(f"Unexpected request handling outcome: {result}")
                return True

            case State.Awaiting(request=req, response_future=fut) if fut.done():
                logger.debug(f"actor({addr}) request({req}) response ready")
                if fut.cancelled():
                    logger.debug(f"actor({addr}) request({req}) cancelled")
                    exec_future = stage.submit_performance(
                        addr, sheet.performance.throw, RequestCancelled(req)
                    )
                elif exception := fut.exception():
                    logger.debug(f"actor({addr}) request({req}) failed: {exception}")
                    exec_future = stage.submit_performance(
                        addr, sheet.performance.throw, exception
                    )
                else:
                    logger.debug(f"actor({addr}) request({req}) succeeded")
                    exec_future = stage.submit_performance(
                        addr, sheet.performance.send, fut.result()
                    )

                play.states[addr] = State.Executing(future=exec_future)
                return True

            case State.Executing(future=fut) if fut.done():
                try:
                    req = fut.result()
                except StopIteration as ex:
                    play.states[addr] = State.Terminated(cause=NormalExit(ex.value))
                    logger.debug(f"actor {addr} terminated with value {ex.value}")
                except CancelledError as ex:
                    logger.debug(f"actor {addr} cancelled during init")
                    wrap = Signal.INT()
                    wrap.__cause__ = wrap.__context__ = ex
                    play.states[addr] = State.Terminated(cause=ErrorExit(wrap))
                except Exception as ex:
                    play.states[addr] = State.Terminated(cause=ErrorExit(ex))
                    logger.debug(f"actor {addr} died: {ex}")
                else:
                    play.states[addr] = State.Waiting(request=req)
                    logger.debug(f"actor {addr} now pending request {req}")
                return True

            case State.Executing(future=fut):
                logger.debug(f"actor({addr}) still executing (future {fut})")
                return False

            case State.Receiving(request=request, timeout_task=tfut):
                assert not tfut or not tfut.cancelled(), (
                    "Receiving state should not be observed with cancelled timeout"
                )
                if tfut and tfut.done():
                    logger.debug(
                        f"actor({addr}): Receiving state observed with completed timeout task"
                    )
                    return False
                if self._try_receive(addr, play, stage):
                    return True

                return False

            case State.Terminated(cause=cause):
                match cause:
                    case ErrorExit(error):
                        logger.debug(f"actor {addr} terminated with error: {error}")
                    case NormalExit(value):
                        logger.debug(f"actor {addr} terminated with value {value}")
                    case Signal():
                        logger.debug(f"actor({addr}) terminated with signal {cause}")
                return False

    def _try_receive(self, addr, play, stage):
        match play.states[addr]:
            case State.Receiving(request=request, timeout_task=tfut):
                if tfut and tfut.done():
                    return False
                sheet = play.actors[addr]
                filter_fn = (
                    request.filter if isinstance(request, System.receive) else None
                )
                try:
                    msg = sheet.mailbox.pop_matching(filter_fn)
                except _NoMatch:
                    logger.debug(f"actor({addr}) request({request}) still unsatisfied")
                    return False
                else:
                    logger.debug(f"actor({addr}) request({request}) satisfied: {msg}")
                    if tfut:
                        tfut.cancel()
                    self.resume_with_value(addr, msg, play, stage)
                    return True
            case state:
                raise RuntimeError(f"tried receiving from non-Receiving state {state}")


@dataclass(frozen=True)
class ActorInfo:
    address: ActorAddress
    script: str
    props: Any
    state_name: str
    mailbox_size: int = 0
    exit_cause: Exit | Signal | None = None

    @classmethod
    def from_run_state(cls, sheet: ActorSheet, state: ActorState) -> ActorInfo:
        return cls(
            address=sheet.address,
            script=getattr(sheet.script, "__qualname__", str(sheet.script)),
            props=sheet.props,
            state_name=type(state).__name__,
            mailbox_size=len(sheet.mailbox),
            exit_cause=state.cause if isinstance(state, State.Terminated) else None,
        )


class Theatre:
    def __init__(
        self, executor: Executor, queue_size=1024, clock_tick=1, max_idle=None
    ):
        self.queue_size = queue_size
        self.executor = executor
        self.clock_tick = clock_tick
        self.max_idle = max_idle

        self._logger = logger.getChild(str(id(self)))
        self._counter = itertools.count()
        self._events = queue.Queue()
        self._stage = Stage(
            executor=self.executor, events=self._events, logger=self._logger
        )
        self._sm = StateMachine()
        # set when starting run loop
        self._play = None
        self._thread = None

    def __str__(self):
        return f"Theatre({self.queue_size=},{self.max_idle=},{self.clock_tick=})"

    def make_addr(self, performance) -> ActorAddress:
        addr = ActorAddress(os.getpid(), id(self), id(performance))
        return addr

    def _create_actor(self, script, props):
        mailbox = Mailbox(maxlen=self.queue_size)
        actor_coro = script(*props)
        addr = self.make_addr(actor_coro)
        return ActorSheet(
            address=addr,
            script=script,
            props=props,
            performance=actor_coro,
            mailbox=mailbox,
            context=copy_context(),
        )

    def _link(self, owner: ActorAddress, target: ActorAddress):
        # register link callback
        self._logger.debug(
            f"registering link condition: owner({owner}) <- target({target})"
        )
        future = Future()

        def get_termination_cause(play):
            return play.states[target].cause

        def link_callback(fut: Future):
            self._logger.debug(
                f"link trap callback: signaling link trap event owner({owner}) <- target({target})"
            )
            self._events.put(Event.LinkTrap(linker=owner, linked=target, future=fut))

        future.add_done_callback(link_callback)
        condition = Event.RegisterCondition(
            predicate=lambda play: (
                target in play.states
                and isinstance(play.states[target], State.Terminated)
            ),
            projection=get_termination_cause,
            future=future,
        )
        self._play.conditions.append(condition)

    def _send(self, address: ActorAddress, message: Any, sender: ActorAddress):
        if address not in self._play.actors:
            raise DestinationNotFound(address)
        elif (destination_state := self._play.states.get(address)) and isinstance(destination_state, State.Terminated):
            raise Signal.ActorTerminated(address, destination_state.cause)
        else:
            self._events.put(Event.Message(actor=address, message=message, sender=sender))

    def _handle_signal(self, actor: ActorAddress, signal: Signal):
        state = self._play.states[actor]
        self._logger.debug(f"{signal} sent to actor {actor} in state {state}")
        match signal:
            case Signal.MailboxFull():
                self._sm.interrupt(actor, signal, self._play, self._stage)
            case Signal.ActorTerminated():
                self._sm.interrupt(actor, signal, self._play, self._stage)
            case Signal.KILL():
                match state:
                    case State.Terminated():
                        pass
                    case _:
                        self._sm.terminate(actor, signal, self._play)
            case Signal.INT():
                match state:
                    case State.Terminated():
                        pass
                    case _:
                        self._sm.interrupt(
                            actor, signal, self._play, self._stage
                        )
            case _:
                raise NotImplementedError()

    def _receive(self, actor: ActorSheet, request: System.receive):
        addr = actor.address
        try:
            msg = actor.mailbox.pop_matching(request.filter)
        except _NoMatch:
            self._logger.debug(
                f"Parking actor({addr}) on receive request ({request})"
            )
            timeout_task = None
            if request.timeout is not None:
                self._logger.debug(
                    f"actor({addr}): Scheduling timeout for receive request in {request.timeout}s"
                )
                interrupt = threading.Event()

                def delayed():
                    if interrupt.wait(timeout=request.timeout):
                        return
                    else:
                        raise ReceiveTimeout(request)

                fut = self._stage.executor.submit(delayed)
                timeout_task = CancellableTask(future=fut, interrupt=interrupt)

                def timeout_callback(f):
                    self._logger.debug(
                        f"Receive timeout triggered ({f.cancelled()=},{f.exception()=})"
                    )
                    if not (interrupt.is_set() or f.cancelled()):
                        self._events.put(
                            Event.ReceiveTimeout(
                                actor=addr,
                                request=request,
                                timeout_task=timeout_task,
                            )
                        )

                fut.add_done_callback(timeout_callback)
            return RequestResult.Park(request, timeout_task)
        else:
            self._logger.debug(
                f"actor({addr}) request {request} satisfied directly: {msg}"
            )
            return RequestResult.ResumeWithValue(msg)

    def _handle_request(self, addr, request):
        self._logger.debug(f"handling request: actor({addr}), request({request})")
        sheet = self._play.actors[addr]

        match request:
            case System.exit(value):
                self._logger.debug(f"actor({addr}) terminated with value {value}")
                return RequestResult.Terminate(NormalExit(value))
            case System.spawn_link(script, props):
                child = self._spawn(script, props, play=self._play)
                self._link(addr, child)
                return RequestResult.ResumeWithValue(child)
            case System.spawn(script, props):
                child = self._spawn(script, props, play=self._play)
                return RequestResult.ResumeWithValue(child)
            case System.whoami():
                return RequestResult.ResumeWithValue(addr)
            case System.send(dest_addr, msg):
                try:
                    self._send(dest_addr, msg, addr)
                except Exception as ex:
                    return RequestResult.ResumeWithError(ex)
                else:
                    return RequestResult.ResumeWithValue(None)
            case System.receive():
                return self._receive(sheet, request)
            case System.sleep(n):
                interrupt = threading.Event()

                def delayed():
                    if interrupt.wait(timeout=n):
                        raise Signal.INT()

                resp_future = self._stage.submit_request(
                    addr, request, delayed, interrupt=interrupt
                )
                return RequestResult.AwaitFuture(request, resp_future)
            case System.link(target=actor):
                if actor not in self._play.actors:
                    return RequestResult.ResumeWithError(DestinationNotFound(actor))
                else:
                    self._link(addr, actor)
                    return RequestResult.ResumeWithValue(None)
            case _:
                self._logger.debug(f"unexpected request {request}")
                return RequestResult.ResumeWithError(UnsupportedRequest(addr, request))

    def _handle_external_request(self, request, result_future: Future):
        try:
            match request:
                case System.spawn(script, props):
                    address = self._spawn(script=script, props=props)
                    result_future.set_result(address)
                case System.send(address, message):
                    try:
                        self._send(address, message, ActorAddress(0,0,0))
                    except Exception as ex:
                        result_future.set_exception(ex)
                    else:
                        result_future.set_result(None)
                case _:
                    result_future.set_exception(NotImplementedError(request))
        except Exception as ex:
            if not result_future.done():
                result_future.set_exception(ex)
            raise

    def _handle_event(self, event: Event) -> None:
        self._logger.debug(f"Handling event {event=}")
        match event:
            case Event.Stop():
                # received stop signal
                # for graceful shutdown: cancel any pending future,
                # transition all actors state to Terminated?
                self._logger.debug("Pulled Stop event from queue")
                raise event
            case Event.EndOfScene(actor=actor, future=future):
                if actor not in self._play.states:
                    self._logger.debug(f"Stale event: actor {actor} gone")
                    return
                actor_state = self._play.states[actor]
                match actor_state:
                    case State.Executing(future=fut):
                        assert future.done()
                        if future is not fut:
                            self._logger.debug(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        else:
                            self._play.runnable.append(actor)
                        return
                    case State.Receiving():
                        # already transitioned to Receiving state from receive request
                        pass
                    case state:
                        self._logger.debug(
                            f"Stale event: actor {actor} has unexpected state {state}"
                        )

            case Event.RequestCompleted(actor=actor, request=request, future=future):
                if actor not in self._play.states:
                    self._logger.debug(f"Stale event: actor {actor} gone")
                    return
                actor_state = self._play.states[actor]
                match actor_state:
                    case State.Awaiting(request=req, response_future=fut):
                        assert future.done()
                        if req is not request or fut is not future:
                            self._logger.debug(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        self._play.runnable.append(actor)
                    case state:
                        self._logger.debug(
                            f"Stale event: actor {actor} has unexpected state {state}"
                        )

            case Event.Message(actor=actor, message=message, sender=sender):
                match self._play.states.get(actor):
                    case State.Terminated(cause=cause):
                        self._events.put(Event.Signal(sender, Signal.ActorTerminated(actor, cause)))
                    case _:
                        try:
                            self._play.actors[actor].mailbox.append(message)
                        except MailboxFull:
                            self._events.put(Event.Signal(sender, Signal.MailboxFull(actor)))
                        else:
                            self._logger.debug(
                                f"actor({actor}) mailbox now has {len(self._play.actors[actor].mailbox)} messages"
                            )
                            self._play.runnable.append(actor)
            case Event.RegisterCondition():
                self._play.conditions.append(event)
            case Event.ExternalRequest(request, result_future):
                self._handle_external_request(request, result_future)
            case Event.Signal(actor, signal):
                if not isinstance(self._play.states[actor], State.Terminated):
                    self._handle_signal(actor, signal)
                    self._play.runnable.append(actor)
            case Event.SignalAll(signal):
                for actor in self._play.actors:
                    if not isinstance(self._play.states[actor], State.Terminated):
                        self._handle_signal(actor, signal)
                        self._play.runnable.append(actor)
            case Event.LinkTrap(linker, linked, future):
                linker_state = self._play.states[linker]
                self._logger.debug(
                    f"handling link trap: target({linked}) -> owner({linker}, state={linker_state})"
                )
                match linker_state:
                    case State.Terminated():
                        self._logger.debug(
                            f"Link owner {linker} terminated before handling link trap for target {linked}"
                        )
                    case _:
                        cause = future.result()
                        self._sm.interrupt(
                            linker, Signal.ActorTerminated(linked, cause), self._play, self._stage
                        )
                        self._play.runnable.append(linker)
            case Event.ReceiveTimeout(
                actor=actor, request=request, timeout_task=future
            ):
                assert future.done()
                if actor not in self._play.states:
                    self._logger.debug(f"Unknown actor {actor}, ignoring")
                    return
                state = self._play.states[actor]
                match state:
                    case State.Receiving(request=req, timeout_task=tfut):
                        assert future is tfut
                        assert req == request
                        self._logger.debug(
                            f"actor({actor}): receive request {req} timed out"
                        )
                        self._sm.interrupt(
                            actor, ReceiveTimeout(request=req), self._play, self._stage
                        )
                        self._play.runnable.append(actor)
                    case _:
                        self._logger.debug(
                            f"actor({actor}): ignoring stale receive timeout while actor in state {state}"
                        )
            case _:
                self._logger.debug(f"Unknown event {event=}")

    def _process_conditions(self):
        triggered_conditions = []
        for condition in self._play.conditions:
            try:
                if condition.predicate(self._play):
                    self._logger.debug(f"Condition predicate satisified {condition=}")
                    try:
                        result = condition.projection(self._play)
                    except Exception as ex:
                        self._logger.debug(
                            f"Condition projection raised {condition=}: {ex}"
                        )
                        condition.future.set_exception(ex)
                    else:
                        self._logger.debug(
                            f"Condition projection successful {condition=}: {result}"
                        )
                        condition.future.set_result(result)
                    finally:
                        triggered_conditions.append(condition)
            except Exception as ex:
                self._logger.debug(
                    f"Exception from condition predicate {condition.predicate=}: {ex}"
                )
                condition.future.set_exception(ex)
                continue
        for condition in triggered_conditions:
            self._play.conditions.remove(condition)

    def _run_loop(self):
        stop_reason = None
        loop_count = itertools.count()
        idle_count = 0
        cnt = 0
        events = []
        try:
            assert self._play

            while not stop_reason:
                cnt = next(loop_count)
                if self.max_idle and idle_count >= self.max_idle:
                    self._logger.debug(
                        f"(%d) Reached max idle count ({self.max_idle=}), stopping",
                        cnt
                    )
                    stop_reason = "idle"
                    break
                self._logger.debug(f"Running main loop ({cnt})")
                alive_count = sum(
                    1
                    for s in self._play.states.values()
                    if not isinstance(s, State.Terminated)
                )
                self._logger.debug(f"{alive_count} actors on stage")
                self._logger.debug(f"{threading.active_count()} active threads")

                events = list(drain(self._events, timeout=self.clock_tick))
                if not events:
                    self._logger.debug(
                        f"({cnt}) No events in last cycle ({self.clock_tick}s)"
                    )
                    idle_count += 1
                    continue

                idle_count = 0

                self._logger.debug(f"{len(events)} events to handle")
                for event in events:
                    self._handle_event(event)
                    self._logger.debug(f"Handled event {event}")

                # now check for new actors ready to act
                for newbie in tuple(actor for actor in self._play.actors if actor not in self._play.states):
                    self._logger.debug("(%d) Kicking-off new actor %s", cnt, newbie)
                    self._sm.initiate(newbie, self._play, self._stage)
                    self._play.runnable.append(newbie)

                # let actors play up to their steady state
                while self._play.runnable:
                    self._logger.debug("(%d) %d actors in runnable state", cnt, len(self._play.runnable))
                    addr = self._play.runnable.popleft()
                    if addr in self._play.states:
                        if self._sm.process(addr, self._play, self._stage, self._handle_request):
                            self._play.runnable.append(addr)

                self._process_conditions()
        except Event.Stop as ex:
            self._logger.info(
                "(%d) Stop signal received, terminating event loop: %s",
                cnt,
                ex
            )
            stop_reason = ("signal", ex)
        except BaseException as ex:
            self._logger.exception("(%d) Theatre run loop raised exception: %s", cnt, ex)
            stop_reason = ("error", ex)
            raise
        finally:
            self._logger.info(f"Terminating play: {stop_reason=} {idle_count=}")
            match stop_reason:
                case "idle":
                    assert self.max_idle and idle_count >= self.max_idle
                    exception = (
                        MaxIdleException(idle_count, self.max_idle)
                    )
                case ("signal", event):
                    exception = event
                case ("error", error):
                    exception = error
                case _:
                    exception = Exception(f"dunny why stop: {stop_reason}")

            # make sure pending future-bearing events are resolved
            events += list(drain(self._events, timeout=0.0))
            for event in events:
                match event:
                    case Event.RegisterCondition(future=fut) \
                        | Event.ExternalRequest(result_future=fut) \
                        if not fut.done():
                        self._logger.debug("Aborting future for event: %s", event)
                        fut.set_exception(exception)
                    case _:
                        continue

            self._logger.debug("Aborting %d registered conditions", len(self._play.conditions))
            for condition in self._play.conditions:
                if not condition.future.done():
                    condition.future.set_exception(
                        exception
                    )

    def _stop(self):
        assert self._thread and self._thread.is_alive()
        self._events.put(Event.Stop("externally requested"))

    def _start(self):
        self._play = Play(states={}, actors={})
        self._thread = threading.Thread(
            name=f"theatre-{id(self)}", target=self._run_loop
        )
        self._logger.info(f"Starting theatre's run loop thread {self._thread=}")
        self._thread.start()

    def _spawn(self, script: Actor, props: tuple, play=None):
        logger.debug(f"Processing spawn request {script=} {props=}")
        play = play or self._play
        sheet = self._create_actor(script, props)
        play.actors[sheet.address] = sheet
        return sheet.address

    def _create_task(self):
        fut = Future()
        return fut

    def _request(self, request):
        future = self._create_task()
        self._events.put(Event.ExternalRequest(request=request, result_future=future))
        return future

    def spawn(self, script, *props):
        assert self._thread.is_alive()
        future = self._request(
            System.spawn(
                script=script,
                props=props,
            )
        )
        new_address = future.result()
        return new_address

    def send(self, address: ActorAddress, message):
        future = self._request(System.send(address, message))
        return future.result()

    def run(self, protagonist: Actor, *props):
        if not (self._thread and self._thread.is_alive()):
            raise RuntimeError("No running run loop thread!")

        protagonist_address = self.spawn(protagonist, *props)

        return self.spotlight(protagonist_address)

    def wait_ensemble(self):
        # wait for all actors to terminate
        future = self._create_task()
        self._events.put(
            Event.RegisterCondition(
                predicate=lambda play: all(
                    isinstance(state, State.Terminated)
                    for state in play.states.values()
                ),
                projection=lambda play: [
                    (addr, state.cause) for addr, state in play.states.items()
                ],
                future=future,
            )
        )
        return future.result()

    def spotlight(self, actor: ActorAddress):
        # wait for a specific actor to terminate
        future = self._create_task()
        self._events.put(
            Event.RegisterCondition(
                predicate=lambda play: (
                    actor in play.states
                    and isinstance(play.states[actor], State.Terminated)
                ),
                projection=lambda play: play.states[actor].cause,
                future=future,
            )
        )
        match future.result():
            case NormalExit(value):
                return value
            case _Signal() as signal:
                raise signal
            case ErrorExit(cause=error):
                raise error

    def census(self):
        future = self._create_task()
        self._events.put(
            Event.RegisterCondition(
                predicate=lambda play: True,
                projection=lambda play: [
                    ActorInfo.from_run_state(play.actors[addr], play.states[addr])
                    for addr in play.actors
                ],
                future=future,
            )
        )
        return future.result()

    def cancel(self, actor: ActorAddress):
        self._events.put(Event.Signal(actor, Signal.INT()))

    def kill(self, actor: ActorAddress):
        self._events.put(Event.Signal(actor, Signal.KILL()))

    def signal(self, actor: ActorAddress, signal: Signal):
        self._events.put(Event.Signal(actor, signal))

    def signal_all(self, signal: Signal):
        self._events.put(Event.SignalAll(signal))

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc, typ, tb):
        self._logger.info("Tearing down the stage")
        if self._thread and self._thread.is_alive():
            self._logger.debug("Sending SIGINT to all actors")
            self.signal_all(Signal.INT())
            self._logger.debug("Stopping theatre's run loop")
            self._stop()
            self._logger.debug("Joining on run loop thread")
            self._thread.join()

        # cancel pending tasks if exception is raised
        # else gracefully complete remaining tasks
        self._logger.debug(
            "Shutting down thread pool (and %scancelling all pending tasks)",
            "" if exc else "not ",
        )
        self.executor.shutdown(cancel_futures=bool(exc))
