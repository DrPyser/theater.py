from theatre.interfaces import receive, Actor, Exit, ActorSheet, send, spawn
import threading
import queue
from concurrent.futures import (
    ThreadPoolExecutor,
    Future,
    as_completed,
    CancelledError,
    Executor,
)
import itertools
from collections import deque
from collections.abc import Iterator
from contextvars import copy_context
from typing import NewType, Any, Callable
from dataclasses import dataclass, field
import time
from traceback import print_exc


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


ActorAddr = NewType("ActorAddr", int)


@dataclass
class Init:
    """Actor initializing - executing code before first yield"""

    future: Future


@dataclass
class Waiting:
    """Actor yielded a request, not yet dispatched"""

    request: object


@dataclass
class Awaiting:
    """Request dispatched, waiting for response_future to complete"""

    request: object
    response_future: Future


@dataclass
class Executing:
    """Request fulfilled, actor executing until next yield"""

    future: Future


@dataclass
class Terminated:
    """Actor finished execution"""

    result: object | None = None
    error: Exception | None = None


@dataclass
class Receiving:
    """Actor waiting for a message in mailbox"""

    request: object


ActorState = Init | Waiting | Awaiting | Executing | Receiving | Terminated


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
        protagonist: bool
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
class NormalExit:
    value: Any


class ErrorExit(Exception):
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
    def __init__(self, actor: ActorAddr, exit: Exit):
        self.actor = actor
        self.exit = exit

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
    protagonist: ActorAddr | None = None
    protagonist_result: Future | None = None
    conditions: list[Event.RegisterCondition] = field(default_factory=list)


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

    def _submit_performance(self, addr, fn, *args):
        print(f"submitting performance for actor {addr}: {fn.__qualname__}({args!r})")
        fut = self.executor.submit(fn, *args)
        fut.add_done_callback(
            lambda f: self._events.put(Event.EndOfScene(actor=addr, future=f))
        )
        return fut

    def _submit_request(self, addr, request, fn, *args):
        print(f"submitting request for actor {addr}: {request!r} ({fn.__qualname__})")
        fut = self.executor.submit(fn, *args)
        fut.add_done_callback(
            lambda f: self._events.put(
                Event.RequestCompleted(actor=addr, request=request, future=f)
            )
        )
        return fut

    def _create_actor(self, script, props):
        addr = self.make_addr(script, props)
        mailbox = lambda: queue.Queue(self.queue_size)
        sheet = create_actor_sheet(script, props, addr, mailbox)
        return sheet

    def _handle_request(self, addr, request, play: Play):
        print(f"handling request: actor({addr}), request({request})")
        sheet = play.actors[addr]

        match request:
            case Theatre.exit(value):
                print(f"actor({addr}) terminated with value {value}")
                play.states[addr] = Terminated(result=value)

            case spawn(script, props):
                child = self._spawn(script, props, play=play)
                resp_future = Future()
                resp_future.set_result(child)
                play.states[addr] = Awaiting(
                    request=request, response_future=resp_future
                )
            case Theatre.self():
                resp_future = Future()
                resp_future.set_result(addr)
                play.states[addr] = Awaiting(
                    request=request, response_future=resp_future
                )
            case send(dest_addr, msg):
                resp_future = Future()
                if destination := play.actors.get(dest_addr):
                    match play.states[dest_addr]:
                        case Terminated(result=result, error=error):
                            resp_future.set_exception(ActorTerminated(
                                dest_addr,
                                ErrorExit(error) if error else NormalExit(result)
                            ))
                        case _:
                            destination.mailbox.put(msg)
                            self._events.put(Event.MessageDelivered(actor=dest_addr))
                            resp_future.set_result(None)
                else:
                    resp_future.set_exception(DestinationNotFound(dest_addr))
                play.states[addr] = Awaiting(
                    request=request, response_future=resp_future
                )
            case receive():
                try:
                    msg = sheet.mailbox.get_nowait()
                except queue.Empty:
                    play.states[addr] = Receiving(request=request)
                else:
                    resp_future = Future()
                    resp_future.set_result(msg)
                    play.states[addr] = Awaiting(
                        request=request, response_future=resp_future
                    )
            case Theatre.sleep(n):

                def delayed():
                    time.sleep(n)

                resp_future = self._submit_request(addr, request, delayed)
                play.states[addr] = Awaiting(
                    request=request, response_future=resp_future
                )
            case _:
                print(f"unexpected request {request}")
                future = self._submit_performance(
                    addr, sheet.play.throw, UnsupportedRequest(addr, request)
                )
                play.states[addr] = Executing(future=future)

    def _process_state(self, addr: ActorAddr, play: Play) -> bool:
        if addr not in play.states:
            return False
        state = play.states[addr]
        sheet = play.actors[addr]

        match state:
            case Init(future) if future.done():
                try:
                    req = future.result()
                except StopIteration as ex:
                    play.states[addr] = Terminated(result=ex.value)
                    print(f"actor {addr} terminated during init with value {ex.value}")
                except CancelledError as ex:
                    print(f"actor {addr} cancelled during init")
                    wrap = ActorCancelled(addr)
                    wrap.__cause__ = wrap.__context__ = ex
                    play.states[addr] = Terminated(error=wrap)
                except Exception as ex:
                    play.states[addr] = Terminated(error=ex)
                    print(f"actor {addr} died during init: {ex}")
                else:
                    play.states[addr] = Waiting(request=req)
                    print(f"actor {addr} initialized, pending request {req}")
                return True

            case Waiting(request=req):
                self._handle_request(addr, req, play)
                return True

            case Awaiting(request=req, response_future=fut) if fut.done():
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

                play.states[addr] = Executing(future=exec_future)
                return True

            case Executing(future=fut) if fut.done():
                try:
                    req = fut.result()
                except StopIteration as ex:
                    play.states[addr] = Terminated(result=ex.value)
                    print(f"actor {addr} terminated with value {ex.value}")
                except Exception as ex:
                    play.states[addr] = Terminated(error=ex)
                    print(f"actor {addr} died: {ex}")
                else:
                    play.states[addr] = Waiting(request=req)
                    print(f"actor {addr} now pending request {req}")
                return True

            case Executing(future=fut):
                print(f"actor({addr}) still executing (future {fut})")
                return False

            case Receiving(request=request):
                print(f"actor ({addr}) still in Receiving state for request {request=}")
                return False

            case Terminated(result=result, error=error):
                if play.protagonist and addr == play.protagonist:
                    if error:
                        print(
                            f"protagonist ({addr}) terminated with error"
                        )
                        play.protagonist_result.set_exception(error)
                    else:
                        print(
                            f"protagonist ({addr}) terminated with success"
                        )
                        play.protagonist_result.set_result(result)
                if error:
                    print(f"actor {addr} is terminated with error: {error}")
                else:
                    print(f"actor {addr} is terminated with value {result}")
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
                    case Executing(future=fut) | Init(future=fut):
                        assert future.done()
                        if future is not fut:
                            print(
                                f"Stale event: actor {actor} state has different future {fut}"
                            )
                        self._chain_transitions(actor, play)
                        return
                    case Receiving():
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
                    case Awaiting(request=req, response_future=fut):
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
                if actor not in play.states:
                    print(f"Stale event: actor {actor} gone")
                    return
                actor_state = play.states[actor]
                match actor_state:
                    case Receiving(request=request):
                        sheet = play.actors[actor]
                        msg = sheet.mailbox.get_nowait()
                        resp_future = Future()
                        resp_future.set_result(msg)
                        play.states[actor] = Awaiting(
                            request=request, response_future=resp_future
                        )
                        self._chain_transitions(actor, play)
                    case _:
                        pass
            case Event.RegisterCondition(predicate=pred, projection=proj, future=fut):
                self._play.conditions.append(event)
            case Event.SpawnRequested(script, props, protagonist, result_future):
                address = self._spawn(
                    script=script, props=props, protagonist=protagonist
                )
                result_future.set_result(address)
                self._chain_transitions(address, play)
            case _:
                print(f"Unknown event {event=}")

    def _run_loop(self):
        try:
            assert self._play

            loop_count = itertools.count()
            stop = False
            while not stop:
                cnt = next(loop_count)
                print(f"Running main loop ({cnt})")
                print(f"{sum(1 for s in self._play.states.values() if not isinstance(s, Terminated))} actors on stage")
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

                triggered_conditions = []
                for condition in self._play.conditions:
                    try:
                        if condition.predicate(self._play):
                            print(f"Condition predicate satisified {condition=}")
                            try:
                                result = condition.projection(self._play)
                            except Exception as ex:
                                print(f"Condition projection raised {condition=}: {ex}")
                                condition.future.set_exception(ex)
                            else:
                                print(f"Condition projection successful {condition=}: {result}")
                                condition.future.set_result(result)
                            finally:
                                triggered_conditions.append(condition)
                    except Exception as ex:
                        print(f"Exception from condition predicate {condition.predicate=}: {ex}")
                        continue
                for condition in triggered_conditions:
                    self._play.conditions.remove(condition)

            print(f"Terminating play: {stop=} {self._play.protagonist_result=}")
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

    def _spawn(self, script: Actor, props: tuple, play=None, protagonist=False):
        print(f"Processing spawn request {script=} {props=} {protagonist=}")
        play = play or self._play
        sheet = self._create_actor(script, props)
        play.actors[sheet.address] = sheet
        play.states[sheet.address] = Init(
            future=self._submit_performance(
                sheet.address, sheet.play.send, None
            )
        )
        if protagonist:
            play.protagonist = sheet.address
            play.protagonist_result = Future()
            print(f"Protagonist introduced to play {play.protagonist}")

        return sheet.address

    def spawn(self, script, *props, protagonist=False):
        assert self._thread.is_alive()
        result_future = Future()
        event = Event.SpawnRequested(
            script=script,
            props=props,
            protagonist=protagonist,
            result_future=result_future
        )
        self._events.put(event)
        print(f"Awaiting spawn request response {result_future=}")
        new_address = result_future.result()
        return new_address

    def run(self, protagonist: Actor, *props):
        if not (self._thread and self._thread.is_alive()):
            raise RuntimeError("No running run loop thread!")
        if self._play.protagonist_result is not None:
            raise RuntimeError("A run is already in progress")

        protagonist_address = self.spawn(
            protagonist, *props, protagonist=True
        )

        return self.spotlight(protagonist_address)

    def wait_ensemble(self):
        # wait for all actors to terminate
        # TODO: generic mechanism for waiting conditions
        # & signaling based on actors states
        future = Future()
        self._events.put(
            Event.RegisterCondition(
                predicate=lambda play: all(isinstance(state, Terminated) for state in play.states.values()),
                projection=lambda play: [
                    (addr, ErrorExit(state.error) if state.error else NormalExit(state.result))
                    for addr, state in play.states.items()
                ],
                future=future
            )
        )
        return future.result()

    def spotlight(self, actor: ActorAddr):
        # wait for a specific actor to terminate
        future = Future()
        self._events.put(
            Event.RegisterCondition(
                predicate=lambda play: actor in play.states and isinstance(play.states[actor], Terminated),
                projection=lambda play: (
                    ErrorExit(play.states[actor].error)
                    if play.states[actor].error
                    else NormalExit(play.states[actor].result)
                ),
                future=future
            )
        )
        match future.result():
            case NormalExit(value):
                return value
            case ErrorExit(cause=error):
                raise error

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc, typ, tb):
        if self._thread and self._thread.is_alive():
            self._stop()
            self._thread.join()
        # cancel pending tasks if exception is raised
        # else gracefully complete remaining tasks
        self.executor.shutdown(cancel_futures=bool(exc))

