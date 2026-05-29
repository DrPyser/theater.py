import os
import threading
import queue
import enum
import time
import itertools
import logging
from concurrent.futures import (
    ThreadPoolExecutor,
    Future,
    as_completed,
    CancelledError,
    Executor,
)
from collections import deque
from collections.abc import Iterator
from contextvars import copy_context
from typing import NewType, Any, Callable
from dataclasses import dataclass, field
from theatre.interfaces import Actor, Exit, ActorSheet, System, Address

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
        """Actor initializing - executing code before first yield"""

        future: CancellableTask

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

    def __str__(self):
        return f"SIG{self.name}"


class Event:
    @dataclass
    class ActorEvent:
        actor: ActorAddress

    @dataclass
    class RequestCompleted(ActorEvent):
        request: Any
        future: Future

    @dataclass
    class EndOfScene(ActorEvent):
        future: Future

    @dataclass
    class MessageDelivered(ActorEvent):
        pass

    @dataclass
    class ExternalRequest:
        request: Any
        result_future: Future

    class Stop(Exception):
        def __init__(self):
            super().__init__("Stopping play")

    @dataclass
    class RegisterCondition:
        predicate: Callable[[Play], bool]
        projection: Callable[[Play], Any]
        future: Future

    @dataclass
    class Signal:
        actor: ActorAddress
        signal: Signal

    @dataclass
    class SignalAll:
        signal: Signal

    @dataclass
    class LinkTrap:
        linker: ActorAddress
        linked: ActorAddress
        future: Future

    @dataclass
    class ReceiveTimeout:
        actor: ActorAddress
        request: object
        timeout_task: CancellableTask


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
        # set when starting run loop
        self._play = None
        self._thread = None

    def make_addr(self, performance) -> ActorAddress:
        addr = ActorAddress(os.getpid(), id(self), id(performance))
        return addr

    def _submit_performance(self, addr, fn, *args, interrupt=None):
        self._logger.debug(
            f"submitting performance for actor {addr}: {fn.__qualname__}{args!r}"
        )
        fut = self.executor.submit(fn, *args)
        fut.add_done_callback(
            lambda f: self._events.put(Event.EndOfScene(actor=addr, future=f))
        )
        return CancellableTask(future=fut, interrupt=interrupt)

    def _submit_request(self, addr, request, fn, *args, interrupt=None):
        self._logger.debug(
            f"submitting request for actor {addr}: {request!r} ({fn.__qualname__}{args})"
        )
        fut = self.executor.submit(fn, *args)
        task = CancellableTask(future=fut, interrupt=interrupt)
        fut.add_done_callback(
            lambda f: self._events.put(
                Event.RequestCompleted(actor=addr, request=request, future=task)
            )
        )
        return task

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

    def _link(self, owner: ActorAddress, target: ActorAddress, play: Play):
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
        play.conditions.append(condition)

    def _cancel_pending_task(self, actor, play):
        state = play.states[actor]
        self._logger.debug(
            f"actor({actor}): cancelling pending tasks for state {state}"
        )
        match state:
            case State.Receiving(timeout_task=tfut):
                if tfut:
                    tfut.cancel()
            case (
                State.Init(future)
                | State.Awaiting(response_future=future)
                | State.Executing(future)
            ):
                future.cancel()
            case _:  # no pending future or state to cleanup
                pass

    def _send(self, address: ActorAddress, message, future: Future, play: Play):
        if destination := play.actors.get(address):
            match play.states[address]:
                case State.Terminated(cause=cause):
                    future.set_exception(ActorTerminated(address, cause))
                case _:
                    try:
                        destination.mailbox.append(message)
                    except MailboxFull as ex:
                        future.set_exception(ex)
                    else:
                        self._events.put(Event.MessageDelivered(actor=address))
                        future.set_result(None)
        else:
            future.set_exception(DestinationNotFound(address))

    def _handle_signal(self, actor: ActorAddress, signal: Signal, play: Play):
        sheet = play.actors[actor]
        state = play.states[actor]
        self._logger.debug(f"{signal} sent to actor {actor} in state {state}")
        match signal:
            case Signal.KILL:
                match state:
                    case State.Terminated():
                        pass
                    case _:
                        self._cancel_pending_task(actor, play)
                        play.states[actor] = State.Terminated(cause=signal)
            case Signal.INT:
                match play.states[actor]:
                    case State.Terminated():
                        pass
                    case _:
                        self._cancel_pending_task(actor, play)
                        exec_future = self._submit_performance(
                            actor, sheet.performance.throw, ActorCancelled(actor)
                        )
                        play.states[actor] = State.Executing(exec_future)
            case _:
                raise NotImplementedError()

    def _handle_request(self, addr, request, play: Play):
        self._logger.debug(f"handling request: actor({addr}), request({request})")
        sheet = play.actors[addr]

        match request:
            case System.exit(value):
                self._logger.debug(f"actor({addr}) terminated with value {value}")
                play.states[addr] = State.Terminated(cause=NormalExit(value))
            case System.spawn_link(script, props):
                child = self._spawn(script, props, play=play)
                self._link(addr, child, play)
                resp_future = Future()
                resp_future.set_result(child)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case System.spawn(script, props):
                child = self._spawn(script, props, play=play)
                resp_future = Future()
                resp_future.set_result(child)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case System.whoami():
                resp_future = Future()
                resp_future.set_result(addr)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case System.send(dest_addr, msg):
                resp_future = Future()
                self._send(dest_addr, msg, resp_future, play)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case System.receive(filter=filter_fn, timeout=timeout):
                try:
                    msg = sheet.mailbox.pop_matching(filter_fn)
                except _NoMatch:
                    self._logger.debug(
                        f"Parking actor({addr}) on receive request ({request})"
                    )
                    timeout_task = None
                    if timeout is not None:
                        self._logger.debug(
                            f"actor({addr}): Scheduling timeout for receive request in {timeout}s"
                        )
                        # setup timeout task
                        interrupt = threading.Event()

                        def delayed():
                            if interrupt.wait(timeout=timeout):
                                # interrupt was set, timeout was cancelled
                                return
                            else:
                                # interrupt was not set, wait terminated by timeout
                                raise ReceiveTimeout(request)

                        fut = self.executor.submit(delayed)
                        timeout_task = CancellableTask(future=fut, interrupt=interrupt)

                        # on completion, timeout event is queued
                        # event handler should check that actor is still in Receiving state
                        # and handling MessageReceived should cancel corresponding timeout task
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
                    play.states[addr] = State.Receiving(
                        request=request, timeout_task=timeout_task
                    )
                else:
                    self._logger.debug(
                        f"actor({addr}) request {request} satisfied directly: {msg}"
                    )
                    resp_future = Future()
                    resp_future.set_result(msg)
                    play.states[addr] = State.Awaiting(
                        request=request, response_future=resp_future
                    )
            case System.sleep(n):
                interrupt = threading.Event()

                def delayed():
                    if interrupt.wait(timeout=n):
                        raise ActorCancelled(addr)

                resp_future = self._submit_request(
                    addr, request, delayed, interrupt=interrupt
                )
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case System.link(target=actor):
                if actor not in play.states:
                    # target actor does not exist
                    future = self._submit_performance(
                        addr, sheet.performance.throw, DestinationNotFound(actor)
                    )
                    play.states[addr] = State.Executing(future=future)
                else:
                    self._link(addr, actor, play)
                    future = self._submit_performance(
                        addr, sheet.performance.send, None
                    )
                    play.states[addr] = State.Executing(future=future)
            case _:
                self._logger.debug(f"unexpected request {request}")
                future = self._submit_performance(
                    addr, sheet.performance.throw, UnsupportedRequest(addr, request)
                )
                play.states[addr] = State.Executing(future=future)

    def _handle_external_request(self, request, result_future: Future, play: Play):
        match request:
            case System.spawn(script, props):
                address = self._spawn(script=script, props=props)
                result_future.set_result(address)
                self._chain_transitions(address, play)
            case System.send(address, message):
                self._send(address, message, result_future, play)
            case _:
                result_future.set_exception(NotImplementedError(request))

    def _process_state(self, addr: ActorAddress, play: Play) -> bool:
        if addr not in play.states:
            return False
        state = play.states[addr]
        sheet = play.actors[addr]

        match state:
            case State.Init(future) if future.done():
                try:
                    req = future.result()
                except StopIteration as ex:
                    play.states[addr] = State.Terminated(cause=NormalExit(ex.value))
                    self._logger.debug(
                        f"actor {addr} terminated during init with value {ex.value}"
                    )
                except CancelledError as ex:
                    self._logger.debug(f"actor {addr} cancelled during init")
                    wrap = ActorCancelled(addr)
                    wrap.__cause__ = wrap.__context__ = ex
                    play.states[addr] = State.Terminated(cause=ErrorExit(wrap))
                except Exception as ex:
                    play.states[addr] = State.Terminated(cause=ErrorExit(ex))
                    self._logger.debug(f"actor {addr} died during init: {ex}")
                else:
                    play.states[addr] = State.Waiting(request=req)
                    self._logger.debug(
                        f"actor {addr} initialized, pending request {req}"
                    )
                return True

            case State.Waiting(request=req):
                self._handle_request(addr, req, play)
                return True

            case State.Awaiting(request=req, response_future=fut) if fut.done():
                self._logger.debug(f"actor({addr}) request({req}) response ready")
                if fut.cancelled():
                    self._logger.debug(f"actor({addr}) request({req}) cancelled")
                    exec_future = self._submit_performance(
                        addr, sheet.performance.throw, RequestCancelled(req)
                    )
                elif exception := fut.exception():
                    self._logger.debug(
                        f"actor({addr}) request({req}) failed: {exception}"
                    )
                    exec_future = self._submit_performance(
                        addr, sheet.performance.throw, exception
                    )
                else:
                    self._logger.debug(f"actor({addr}) request({req}) succeeded")
                    exec_future = self._submit_performance(
                        addr, sheet.performance.send, fut.result()
                    )

                play.states[addr] = State.Executing(future=exec_future)
                return True

            case State.Executing(future=fut) if fut.done():
                try:
                    req = fut.result()
                except StopIteration as ex:
                    play.states[addr] = State.Terminated(cause=NormalExit(ex.value))
                    self._logger.debug(f"actor {addr} terminated with value {ex.value}")
                except Exception as ex:
                    play.states[addr] = State.Terminated(cause=ErrorExit(ex))
                    self._logger.debug(f"actor {addr} died: {ex}")
                else:
                    play.states[addr] = State.Waiting(request=req)
                    self._logger.debug(f"actor {addr} now pending request {req}")
                return True

            case State.Executing(future=fut):
                self._logger.debug(f"actor({addr}) still executing (future {fut})")
                return False

            case State.Receiving(request=request, timeout_task=tfut):
                self._logger.debug(
                    f"actor ({addr}) still in Receiving state for request {request=}"
                )
                assert not tfut or not tfut.cancelled(), (
                    "Receiving state should not be observed with cancelled timeout"
                )
                if tfut and tfut.done():
                    # TODO: can throw to actor now
                    # leaving it to callback event handler to propagate to actor
                    # and transition state
                    self._logger.debug(
                        f"actor({addr}): Receiving state observed with completed timeout task"
                    )
                    pass

                return False

            case State.Terminated(cause=cause):
                match cause:
                    case ErrorExit(error):
                        self._logger.debug(
                            f"actor {addr} terminated with error: {error}"
                        )
                    case NormalExit(value):
                        self._logger.debug(
                            f"actor {addr} terminated with value {value}"
                        )
                    case Signal():
                        self._logger.debug(
                            f"actor({addr}) terminated with signal {cause}"
                        )
                # TODO: handle terminated actors (links, cleanup)
                return False

    def _chain_transitions(self, actor: ActorAddress, play: Play) -> None:
        self._logger.debug(f"Chaining transitions for actor {actor}")
        state = play.states[actor]
        while self._process_state(actor, play):
            if actor not in play.states:
                self._logger.debug(
                    f"State of actor {actor} disappeared during transition from state {state}"
                )
                break
            self._logger.debug(
                f"Transitioned actor {actor}: {state} -> {play.states[actor]}"
            )
            state = play.states[actor]

    def _handle_event(self, event: Event, play: Play) -> None:
        self._logger.debug(f"Handling event {event=}")
        match event:
            case Event.Stop():
                # received stop signal
                # for graceful shutdown: cancel any pending future,
                # transition all actors state to Terminated?
                self._logger.debug("Pulled Stop event from queue")
                raise event
            case Event.EndOfScene(actor=actor, future=future):
                if actor not in play.states:
                    self._logger.debug(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case State.Executing(future=fut) | State.Init(future=fut):
                        assert future.done()
                        if future is not fut:
                            self._logger.debug(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        self._chain_transitions(actor, play)
                        return
                    case State.Receiving():
                        # already transitioned to Receiving state from receive request
                        pass
                    case state:
                        self._logger.debug(
                            f"Stale event: actor {actor} has unexpected state {state}"
                        )

            case Event.RequestCompleted(actor=actor, request=request, future=future):
                if actor not in play.states:
                    self._logger.debug(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case State.Awaiting(request=req, response_future=fut):
                        assert future.done()
                        if req is not request or fut is not future:
                            self._logger.debug(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        self._chain_transitions(actor, play)
                    case state:
                        self._logger.debug(
                            f"Stale event: actor {actor} has unexpected state {state}"
                        )

            case Event.MessageDelivered(actor=actor):
                self._logger.debug(
                    f"actor({actor}) mailbox now has {len(play.actors[actor].mailbox)} messages"
                )
                if actor not in play.states:
                    self._logger.debug(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case State.Receiving(request=request, timeout_task=tfut):
                        sheet = play.actors[actor]
                        if tfut and tfut.done():
                            # timeout expired but event not processed yet
                            # skip and let event be processed
                            self._logger.debug(
                                f"actor({actor}): message received but timeout already triggered"
                            )
                            return
                        filter_fn = (
                            request.filter
                            if isinstance(request, System.receive)
                            else None
                        )
                        try:
                            msg = sheet.mailbox.pop_matching(filter_fn)
                        except _NoMatch:
                            self._logger.debug(
                                f"actor({actor}) request({request}) still insatisfied"
                            )
                            pass
                        else:
                            self._logger.debug(
                                f"actor({actor}) request({request}) satisfied: {msg}"
                            )
                            if tfut:
                                tfut.cancel()
                            resp_future = Future()
                            resp_future.set_result(msg)
                            play.states[actor] = State.Awaiting(
                                request=request, response_future=resp_future
                            )
                            self._chain_transitions(actor, play)
                    case _:
                        self._logger.debug(f"actor({actor}) not in Receiving state")
                        pass
            case Event.RegisterCondition(predicate=pred, projection=proj, future=fut):
                self._play.conditions.append(event)
            case Event.ExternalRequest(request, result_future):
                self._handle_external_request(request, result_future, play)
            case Event.Signal(actor, signal):
                if not isinstance(play.states[actor], State.Terminated):
                    self._handle_signal(actor, signal, play)
                    self._chain_transitions(actor, play)
            case Event.SignalAll(signal):
                for actor in play.actors:
                    if not isinstance(play.states[actor], State.Terminated):
                        self._handle_signal(actor, signal, play)
                        self._chain_transitions(actor, play)
            case Event.LinkTrap(linker, linked, future):
                linker_sheet = play.actors[linker]
                linker_state = play.states[linker]
                self._logger.debug(
                    f"handling link trap: target({linked}) -> owner({linker}, state={linker_state})"
                )
                match linker_state:
                    case State.Terminated():
                        self._logger.debug(
                            f"Link owner {linker} terminated before handling link trap for target {linked}"
                        )
                    case State.Executing(exec_future):
                        self._logger.debug(
                            f"link owner {linker} in Executing state, chaining trap propagation"
                        )

                        def exec_chain():
                            try:
                                req = exec_future.result()
                            except Exception:
                                raise
                            else:
                                self._logger.debug(
                                    f"actor({linker}): ignoring request {req} to signal link trap from {linked}"
                                )
                                linker_sheet.performance.throw(
                                    ActorTerminated(linked, future.result())
                                )

                        new_exec_future = self._submit_performance(linker, exec_chain)
                        play.states[linker] = State.Executing(new_exec_future)
                    case State.Awaiting(response_future=fut):
                        self._logger.debug(
                            f"link owner {linker} in Awaiting state, cancelling task and propagating trap"
                        )
                        fut.cancel()
                        exec_future = self._submit_performance(
                            linker,
                            linker_sheet.performance.throw,
                            ActorTerminated(linked, future.result()),
                        )
                        play.states[linker] = State.Executing(exec_future)
                    case _:
                        exec_future = self._submit_performance(
                            linker,
                            linker_sheet.performance.throw,
                            ActorTerminated(linked, future.result()),
                        )
                        play.states[linker] = State.Executing(exec_future)
                self._chain_transitions(linker, play)
            case Event.ReceiveTimeout(
                actor=actor, request=request, timeout_task=future
            ):
                assert future.done()
                if actor not in play.states:
                    self._logger.debug(f"Unknown actor {actor}, ignoring")
                    return
                state = play.states[actor]
                sheet = play.actors[actor]
                match state:
                    case State.Receiving(request=req, timeout_task=tfut):
                        assert future is tfut
                        assert req == request
                        self._logger.debug(
                            f"actor({actor}): receive request {req} timed out"
                        )
                        exec_future = self._submit_performance(
                            actor,
                            sheet.performance.throw,
                            ReceiveTimeout(request=req),
                        )
                        play.states[actor] = State.Executing(exec_future)
                    case _:
                        self._logger.debug(
                            f"actor({actor}): ignoring stale receive timeout while actor in state {state}"
                        )
                self._chain_transitions(actor, play)
            case _:
                self._logger.debug(f"Unknown event {event=}")

    def _process_conditions(self, play):
        triggered_conditions = []
        for condition in play.conditions:
            try:
                if condition.predicate(play):
                    self._logger.debug(f"Condition predicate satisified {condition=}")
                    try:
                        result = condition.projection(play)
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
                continue
        for condition in triggered_conditions:
            play.conditions.remove(condition)

    def _run_loop(self):
        try:
            assert self._play

            loop_count = itertools.count()
            idle_count = 0
            stop = False
            stop_idle = False
            while not stop:
                if self.max_idle and idle_count >= self.max_idle:
                    self._logger.debug(
                        f"Reached max idle count ({self.max_idle=}), stopping"
                    )
                    stop_idle = True
                    break
                cnt = next(loop_count)
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
                    try:
                        self._handle_event(event, self._play)
                    except Event.Stop:
                        self._logger.debug(
                            f"({loop_count}) Stop exception raised, terminating event loop"
                        )
                        stop = True
                        break
                    self._logger.debug(f"Handled event {event}")

                self._process_conditions(self._play)

            self._logger.info(f"Terminating play: {stop=} {idle_count=}")
            for condition in self._play.conditions:
                if stop_idle:
                    assert self.max_idle and idle_count >= self.max_idle
                    condition.future.set_exception(
                        MaxIdleException(idle_count, self.max_idle)
                    )
                elif stop:
                    condition.future.set_exception(Event.Stop())
                else:
                    condition.future.set_exception(Exception("dunny why stop"))
        except BaseException as ex:
            self._logger.exception("Theatre run loop raised exception: %s", ex)
            raise

    def _stop(self):
        assert self._thread and self._thread.is_alive()
        self._events.put(Event.Stop())

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
        play.states[sheet.address] = State.Init(
            future=self._submit_performance(sheet.address, sheet.performance.send, None)
        )
        return sheet.address

    def _request(self, request):
        future = Future()
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
        future = Future()
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
        future = Future()
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
            case ErrorExit(cause=error):
                raise error
            case Signal() as signal:
                raise ActorSignaled(actor, signal)

    def cancel(self, actor: ActorAddress):
        self._events.put(Event.Signal(actor, Signal.INT))

    def kill(self, actor: ActorAddress):
        self._events.put(Event.Signal(actor, Signal.KILL))

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
            self.signal_all(Signal.INT)
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
