import threading
import queue
import enum
import time
import itertools
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
from traceback import print_exc
from theatre.interfaces import receive, Actor, Exit, ActorSheet, send, spawn


def create_actor_sheet(actor_script, props, addr, mailbox):
    return ActorSheet(
        address=addr,
        script=actor_script,
        play=actor_script(*props),
        props=props,
        mailbox=mailbox(),
        context=copy_context(),
    )


class RequestCancelled(Exception):
    def __init__(self, req):
        self.req = req


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


ActorAddr = NewType("ActorAddr", int)


class State:
    @dataclass
    class Init:
        """Actor initializing - executing code before first yield"""

        future: CancellableTask

    @dataclass
    class Waiting:
        """Actor yielded a request, not yet dispatched"""

        request: object

    @dataclass
    class Awaiting:
        """Request dispatched, waiting for response_future to complete"""

        request: object
        response_future: CancellableTask

    @dataclass
    class Executing:
        """Request fulfilled, actor executing until next yield"""

        future: CancellableTask

    @dataclass
    class Terminated:
        """Actor finished execution"""

        cause: Exit | Signal

    @dataclass
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
        actor: ActorAddr

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
    class SpawnRequested:
        script: Actor
        props: tuple[Any, ...]
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
        actor: ActorAddr
        signal: Signal

    @dataclass
    class SignalAll:
        signal: Signal

    @dataclass
    class LinkTrap:
        linker: ActorAddr
        linked: ActorAddr
        future: Future

    @dataclass
    class ReceiveTimeout:
        actor: ActorAddr
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
    def __init__(self, destination: ActorAddr):
        self.destination = destination


class ActorCancelled(Exception):
    def __init__(self, actor: ActorAddr):
        self.actor = actor


class ActorTerminated(Exception):
    def __init__(self, actor: ActorAddr, cause: Exit | Signal):
        self.actor = actor
        self.cause = cause


class ReceiveTimeout(Exception):
    def __init__(self, request: object):
        self.request = request


class ActorSignaled(Exception):
    def __init__(self, actor: ActorAddr, signal: Signal):
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
    def __init__(self, actor: ActorAddr, req: Any):
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
    states: dict[ActorAddr, ActorState]
    actors: dict[ActorAddr, ActorSheet]
    conditions: list[Event.RegisterCondition] = field(default_factory=list)


@dataclass
class link:
    target: ActorAddr


@dataclass
class spawn_link(spawn):
    pass


class Theatre:
    @dataclass
    class self:
        pass

    @dataclass
    class sleep:
        duration: float

    @dataclass
    class exit:
        value: Any = None

    def __init__(self, executor: Executor, queue_size=1024, clock_tick=1):
        self._counter = itertools.count()
        self.queue_size = queue_size
        self.executor = executor
        self.clock_tick = clock_tick
        self._events = queue.Queue()

        # set when starting run loop
        self._play = None
        self._thread = None

    def make_addr(self, script, props) -> ActorAddr:
        addr = hash((script, props, next(self._counter)))
        return ActorAddr(addr)

    def _submit_performance(self, addr, fn, *args, interrupt=None):
        print(f"submitting performance for actor {addr}: {fn.__qualname__}{args!r}")
        fut = self.executor.submit(fn, *args)
        fut.add_done_callback(
            lambda f: self._events.put(Event.EndOfScene(actor=addr, future=f))
        )
        return CancellableTask(future=fut, interrupt=interrupt)

    def _submit_request(self, addr, request, fn, *args, interrupt=None):
        print(
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
        addr = self.make_addr(script, props)
        mailbox = lambda: Mailbox(maxlen=self.queue_size)
        sheet = create_actor_sheet(script, props, addr, mailbox)
        return sheet

    def _link(self, owner: ActorAddr, target: ActorAddr, play: Play):
        # register link callback
        print(f"registering link condition: owner({owner}) <- target({target})")
        future = Future()

        def get_termination_cause(play):
            return play.states[target].cause

        def link_callback(fut: Future):
            print(
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

    def _handle_signal(self, actor: ActorAddr, signal: Signal, play: Play):
        sheet = play.actors[actor]
        state = play.states[actor]
        match signal:
            case Signal.KILL:
                match state:
                    case State.Terminated():
                        # nothing to do
                        print(f"SIGKILL sent to terminated actor {actor}")
                    case State.Receiving(timeout_task=tfut):
                        print(
                            f"actor({actor}) received SIGKILL while receiving; cancelling timeout"
                        )
                        if tfut:
                            tfut.cancel()
                        play.states[actor] = State.Terminated(cause=signal)
                    case (
                        State.Init(future)
                        | State.Awaiting(response_future=future)
                        | State.Executing(future)
                    ):
                        print(
                            f"actor({actor}) received SIGKILL during execution of future {future}; cancelling and terminating"
                        )
                        future.cancel()
                        play.states[actor] = State.Terminated(cause=signal)
                    case _:  # no pending future or state to cleanup
                        print(
                            f"actor({actor}) received SIGKILL while in state {state}; terminating"
                        )
                        play.states[actor] = State.Terminated(cause=signal)
            case Signal.INT:
                match play.states[actor]:
                    case State.Terminated():
                        print(f"SIGINT sent to terminated actor {actor}")
                        pass
                    case State.Receiving(request=request, timeout_task=tfut):
                        print(
                            f"actor({actor}) received SIGINT while receiving; cancelling timeout and scheduling signal handling"
                        )
                        if tfut:
                            tfut.cancel()
                        exec_future = self._submit_performance(
                            actor, sheet.play.throw, ActorCancelled(actor)
                        )
                        play.states[actor] = State.Executing(exec_future)
                    case (
                        State.Init(future)
                        | State.Awaiting(response_future=future)
                        | State.Executing(future)
                    ):
                        future.cancel()
                        exec_future = self._submit_performance(
                            actor, sheet.play.throw, ActorCancelled(actor)
                        )
                        play.states[actor] = State.Executing(exec_future)
                    case state:
                        print(
                            f"actor({actor}) received SIGINT while in state {state}; scheduling signal handling opportunity"
                        )
                        exec_future = self._submit_performance(
                            actor, sheet.play.throw, ActorCancelled(actor)
                        )
                        play.states[actor] = State.Executing(exec_future)
            case _:
                raise NotImplementedError()

    def _handle_request(self, addr, request, play: Play):
        print(f"handling request: actor({addr}), request({request})")
        sheet = play.actors[addr]

        match request:
            case Theatre.exit(value):
                print(f"actor({addr}) terminated with value {value}")
                play.states[addr] = State.Terminated(cause=NormalExit(value))
            case spawn_link(script, props):
                child = self._spawn(script, props, play=play)
                self._link(addr, child, play)
                resp_future = Future()
                resp_future.set_result(child)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case spawn(script, props):
                child = self._spawn(script, props, play=play)
                resp_future = Future()
                resp_future.set_result(child)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case Theatre.self():
                resp_future = Future()
                resp_future.set_result(addr)
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case send(dest_addr, msg):
                resp_future = Future()
                if destination := play.actors.get(dest_addr):
                    match play.states[dest_addr]:
                        case State.Terminated(cause=cause):
                            resp_future.set_exception(ActorTerminated(dest_addr, cause))
                        case _:
                            try:
                                destination.mailbox.append(msg)
                            except MailboxFull as ex:
                                resp_future.set_exception(ex)
                            else:
                                self._events.put(
                                    Event.MessageDelivered(actor=dest_addr)
                                )
                                resp_future.set_result(None)
                else:
                    resp_future.set_exception(DestinationNotFound(dest_addr))
                play.states[addr] = State.Awaiting(
                    request=request, response_future=resp_future
                )
            case receive(filter=filter_fn, timeout=timeout):
                try:
                    msg = sheet.mailbox.pop_matching(filter_fn)
                except _NoMatch:
                    print(f"Parking actor({addr}) on receive request ({request})")
                    timeout_task = None
                    if timeout is not None:
                        print(
                            f"actor({addr}): Scheduling timeout for receive request in {timeout}s"
                        )
                        # setup timeout task
                        interrupt = threading.Event()

                        def delayed():
                            if interrupt.wait(timeout=timeout):
                                if not interrupt.is_set():
                                    raise ReceiveTimeout(request)

                        fut = self.executor.submit(delayed)
                        timeout_task = CancellableTask(future=fut, interrupt=interrupt)

                        # on completion, timeout event is queued
                        # event handler should check that actor is still in Receiving state
                        # and handling MessageReceived should cancel corresponding timeout task
                        def timeout_callback(f):
                            # if not f.cancelled() and isinstance(f.exception(), ReceiveTimeout):
                            print(f"Receive timeout triggered ({f.cancelled()=},{f.exception()=})")
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
                    print(f"actor({addr}) request {request} satisfied directly: {msg}")
                    resp_future = Future()
                    resp_future.set_result(msg)
                    play.states[addr] = State.Awaiting(
                        request=request, response_future=resp_future
                    )
            case Theatre.sleep(n):
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
            case link(target=actor):
                if actor not in play.states:
                    # target actor does not exist
                    future = self._submit_performance(
                        addr, sheet.play.throw, DestinationNotFound(actor)
                    )
                    play.states[addr] = State.Executing(future=future)
                else:
                    self._link(addr, actor, play)
                    future = self._submit_performance(addr, sheet.play.send, None)
                    play.states[addr] = State.Executing(future=future)
            case _:
                print(f"unexpected request {request}")
                future = self._submit_performance(
                    addr, sheet.play.throw, UnsupportedRequest(addr, request)
                )
                play.states[addr] = State.Executing(future=future)

    def _process_state(self, addr: ActorAddr, play: Play) -> bool:
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
                    print(f"actor {addr} terminated during init with value {ex.value}")
                except CancelledError as ex:
                    print(f"actor {addr} cancelled during init")
                    wrap = ActorCancelled(addr)
                    wrap.__cause__ = wrap.__context__ = ex
                    play.states[addr] = State.Terminated(cause=ErrorExit(wrap))
                except Exception as ex:
                    play.states[addr] = State.Terminated(cause=ErrorExit(ex))
                    print(f"actor {addr} died during init: {ex}")
                else:
                    play.states[addr] = State.Waiting(request=req)
                    print(f"actor {addr} initialized, pending request {req}")
                return True

            case State.Waiting(request=req):
                self._handle_request(addr, req, play)
                return True

            case State.Awaiting(request=req, response_future=fut) if fut.done():
                print(f"actor({addr}) request({req}) response ready")
                if fut.cancelled():
                    print(f"actor({addr}) request({req}) cancelled")
                    exec_future = self._submit_performance(
                        addr, sheet.play.throw, RequestCancelled(req)
                    )
                elif exception := fut.exception():
                    print(f"actor({addr}) request({req}) failed: {exception}")
                    exec_future = self._submit_performance(
                        addr, sheet.play.throw, exception
                    )
                else:
                    print(f"actor({addr}) request({req}) succeeded")
                    exec_future = self._submit_performance(
                        addr, sheet.play.send, fut.result()
                    )

                play.states[addr] = State.Executing(future=exec_future)
                return True

            case State.Executing(future=fut) if fut.done():
                try:
                    req = fut.result()
                except StopIteration as ex:
                    play.states[addr] = State.Terminated(cause=NormalExit(ex.value))
                    print(f"actor {addr} terminated with value {ex.value}")
                except Exception as ex:
                    play.states[addr] = State.Terminated(cause=ErrorExit(ex))
                    print(f"actor {addr} died: {ex}")
                else:
                    play.states[addr] = State.Waiting(request=req)
                    print(f"actor {addr} now pending request {req}")
                return True

            case State.Executing(future=fut):
                print(f"actor({addr}) still executing (future {fut})")
                return False

            case State.Receiving(request=request, timeout_task=tfut):
                print(f"actor ({addr}) still in Receiving state for request {request=}")
                assert not tfut or not tfut.cancelled(), (
                    "Receiving state should not be observed with cancelled timeout"
                )
                if tfut and tfut.done():
                    # TODO: can throw to actor now
                    # leaving it to callback event handler to propagate to actor
                    # and transition state
                    print(
                        f"actor({addr}): Receiving state observed with completed timeout task"
                    )
                    pass

                return False

            case State.Terminated(cause=cause):
                match cause:
                    case ErrorExit(error):
                        print(f"actor {addr} terminated with error: {error}")
                    case NormalExit(value):
                        print(f"actor {addr} terminated with value {value}")
                    case Signal():
                        print(f"actor({addr}) terminated with signal {cause}")
                # TODO: handle terminated actors (links, cleanup)
                return False

    def _chain_transitions(self, actor: ActorAddr, play: Play) -> None:
        print(f"Chaining transitions for actor {actor}")
        state = play.states[actor]
        while self._process_state(actor, play):
            if actor not in play.states:
                print(
                    f"State of actor {actor} disappeared during transition from state {state}"
                )
                break
            print(f"Transitioned actor {actor}: {state} -> {play.states[actor]}")
            state = play.states[actor]

    def _handle_event(self, event: Event, play: Play) -> None:
        print(f"Handling event {event=}")
        match event:
            case Event.Stop():
                # received stop signal
                # for graceful shutdown: cancel any pending future,
                # transition all actors state to Terminated?
                print("Pulled Stop event from queue")
                raise event
            case Event.EndOfScene(actor=actor, future=future):
                if actor not in play.states:
                    print(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case State.Executing(future=fut) | State.Init(future=fut):
                        assert future.done()
                        if future is not fut:
                            print(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        self._chain_transitions(actor, play)
                        return
                    case State.Receiving():
                        # already transitioned to Receiving state from receive request
                        pass
                    case state:
                        print(
                            f"Stale event: actor {actor} has unexpected state {state}"
                        )

            case Event.RequestCompleted(actor=actor, request=request, future=future):
                if actor not in play.states:
                    print(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case State.Awaiting(request=req, response_future=fut):
                        assert future.done()
                        if req is not request or fut is not future:
                            print(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        self._chain_transitions(actor, play)
                    case state:
                        print(
                            f"Stale event: actor {actor} has unexpected state {state}"
                        )

            case Event.MessageDelivered(actor=actor):
                print(
                    f"actor({actor}) mailbox now has {len(play.actors[actor].mailbox)} messages"
                )
                if actor not in play.states:
                    print(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case State.Receiving(request=request, timeout_task=tfut):
                        sheet = play.actors[actor]
                        if tfut and tfut.done():
                            # timeout expired but event not processed yet
                            # skip and let event be processed
                            print(
                                f"actor({actor}): message received but timeout already triggered"
                            )
                            return
                        filter_fn = (
                            request.filter if isinstance(request, receive) else None
                        )
                        try:
                            msg = sheet.mailbox.pop_matching(filter_fn)
                        except _NoMatch:
                            print(
                                f"actor({actor}) request({request}) still insatisfied"
                            )
                            pass
                        else:
                            print(f"actor({actor}) request({request}) satisfied: {msg}")
                            if tfut:
                                tfut.cancel()
                            resp_future = Future()
                            resp_future.set_result(msg)
                            play.states[actor] = State.Awaiting(
                                request=request, response_future=resp_future
                            )
                            self._chain_transitions(actor, play)
                    case _:
                        print(f"actor({actor}) not in Receiving state")
                        pass
            case Event.RegisterCondition(predicate=pred, projection=proj, future=fut):
                self._play.conditions.append(event)
            case Event.SpawnRequested(script, props, result_future):
                address = self._spawn(script=script, props=props)
                result_future.set_result(address)
                self._chain_transitions(address, play)
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
                print(
                    f"handling link trap: target({linked}) -> owner({linker}, state={linker_state})"
                )
                match linker_state:
                    case State.Terminated():
                        print(
                            f"Link owner {linker} terminated before handling link trap for target {linked}"
                        )
                    case State.Executing(exec_future):
                        print(
                            f"link owner {linker} in Executing state, chaining trap propagation"
                        )

                        def exec_chain():
                            try:
                                req = exec_future.result()
                            except Exception:
                                raise
                            else:
                                print(
                                    f"actor({linker}): ignoring request {req} to signal link trap from {linked}"
                                )
                                linker_sheet.play.throw(
                                    ActorTerminated(linked, future.result())
                                )

                        new_exec_future = self._submit_performance(linker, exec_chain)
                        play.states[linker] = State.Executing(new_exec_future)
                    case State.Awaiting(response_future=fut):
                        print(
                            f"link owner {linker} in Awaiting state, cancelling task and propagating trap"
                        )
                        fut.cancel()
                        exec_future = self._submit_performance(
                            linker,
                            linker_sheet.play.throw,
                            ActorTerminated(linked, future.result()),
                        )
                        play.states[linker] = State.Executing(exec_future)
                    case _:
                        exec_future = self._submit_performance(
                            linker,
                            linker_sheet.play.throw,
                            ActorTerminated(linked, future.result()),
                        )
                        play.states[linker] = State.Executing(exec_future)
                self._chain_transitions(linker, play)
            case Event.ReceiveTimeout(
                actor=actor, request=request, timeout_task=future
            ):
                assert future.done()
                if actor not in play.states:
                    print(f"Unknown actor {actor}, ignoring")
                    return
                state = play.states[actor]
                sheet = play.actors[actor]
                match state:
                    case State.Receiving(request=req, timeout_task=tfut):
                        assert future is tfut
                        assert req == request
                        print(f"actor({actor}): receive request {req} timed out")
                        exec_future = self._submit_performance(
                            actor,
                            sheet.play.throw,
                            ReceiveTimeout(request=req),
                        )
                        play.states[actor] = State.Executing(exec_future)
                    case _:
                        print(
                            f"actor({actor}): ignoring stale receive timeout while actor in state {state}"
                        )
                self._chain_transitions(actor, play)
            case _:
                print(f"Unknown event {event=}")

    def _process_conditions(self, play):
        triggered_conditions = []
        for condition in play.conditions:
            try:
                if condition.predicate(play):
                    print(f"Condition predicate satisified {condition=}")
                    try:
                        result = condition.projection(play)
                    except Exception as ex:
                        print(f"Condition projection raised {condition=}: {ex}")
                        condition.future.set_exception(ex)
                    else:
                        print(
                            f"Condition projection successful {condition=}: {result}"
                        )
                        condition.future.set_result(result)
                    finally:
                        triggered_conditions.append(condition)
            except Exception as ex:
                print(
                    f"Exception from condition predicate {condition.predicate=}: {ex}"
                )
                continue
        for condition in triggered_conditions:
            play.conditions.remove(condition)

    def _run_loop(self):
        try:
            assert self._play

            loop_count = itertools.count()
            stop = False
            while not stop:
                cnt = next(loop_count)
                print(f"Running main loop ({cnt})")
                alive_count = sum(
                    1
                    for s in self._play.states.values()
                    if not isinstance(s, State.Terminated)
                )
                print(f"{alive_count} actors on stage")
                print(f"{threading.active_count()} active threads")

                events = list(drain(self._events, timeout=self.clock_tick))
                if not events:
                    print(f"({cnt}) No events in last cycle ({self.clock_tick}s)")
                    continue

                print(f"{len(events)} events to handle")
                for event in events:
                    try:
                        self._handle_event(event, self._play)
                    except Event.Stop:
                        print(
                            f"({loop_count}) Stop exception raised, terminating event loop"
                        )
                        stop = True
                        break
                    print(f"Handled event {event}")

                self._process_conditions(self._play)

            print(f"Terminating play: {stop=}")
        except BaseException as ex:
            print(f"Theatre run loop raised exception: {ex}")
            print_exc()
            raise

    def _stop(self):
        assert self._thread and self._thread.is_alive()
        self._events.put(Event.Stop())

    def _start(self):
        self._play = Play(states={}, actors={})
        self._thread = threading.Thread(
            name=f"theatre-{id(self)}", target=self._run_loop
        )
        print(f"Starting run loop thread {self._thread=}")
        self._thread.start()

    def _spawn(self, script: Actor, props: tuple, play=None):
        print(f"Processing spawn request {script=} {props=}")
        play = play or self._play
        sheet = self._create_actor(script, props)
        play.actors[sheet.address] = sheet
        play.states[sheet.address] = State.Init(
            future=self._submit_performance(sheet.address, sheet.play.send, None)
        )
        return sheet.address

    def spawn(self, script, *props):
        assert self._thread.is_alive()
        result_future = Future()
        event = Event.SpawnRequested(
            script=script,
            props=props,
            result_future=result_future,
        )
        self._events.put(event)
        print(f"Awaiting spawn request response {result_future=}")
        new_address = result_future.result()
        return new_address

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

    def spotlight(self, actor: ActorAddr):
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

    def cancel(self, actor: ActorAddr):
        self._events.put(Event.Signal(actor, Signal.INT))

    def kill(self, actor: ActorAddr):
        self._events.put(Event.Signal(actor, Signal.KILL))

    def signal(self, actor: ActorAddr, signal: Signal):
        self._events.put(Event.Signal(actor, signal))

    def signal_all(self, signal: Signal):
        self._events.put(Event.SignalAll(signal))

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc, typ, tb):
        if self._thread and self._thread.is_alive():
            self.signal_all(Signal.INT)
            self._stop()
            self._thread.join()
        # cancel pending tasks if exception is raised
        # else gracefully complete remaining tasks
        self.executor.shutdown(cancel_futures=bool(exc))
