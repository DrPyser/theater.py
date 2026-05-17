from theatre.interfaces import receive, Actor, Exit, ActorSheet, send, spawn
import threading
import queue
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, CancelledError
import itertools
from collections import deque
from collections.abc import Iterator
from contextvars import copy_context
from typing import NewType, Any
from dataclasses import dataclass
import time


def create_actor_sheet(actor_script, props, addr, mailbox):
    return ActorSheet(
        address=addr,
        script=actor_script,
        play=actor_script(*props),
        props=props,
        mailbox=mailbox(),
        outbox=mailbox(),
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
class Pending:
    """Actor yielded a request, waiting for it to be handled"""

    request: object
    response_future: Future | None = None  # None = not yet handled


@dataclass
class Executing:
    """Request fulfilled, actor executing until next yield"""

    future: Future


@dataclass
class Terminated:
    """Actor finished execution"""

    result: object | None = None
    error: Exception | None = None


ActorState = Init | Pending | Executing | Terminated


class Event:
    pass


@dataclass
class ActorEvent(Event):
    actor: ActorAddr


@dataclass
class RequestCompleted(ActorEvent):
    request: Any
    future: Future


@dataclass
class EndOfScene(ActorEvent):
    future: Future


def has_active_actors(states: dict) -> bool:
    return any(not isinstance(s, Terminated) for s in states.values())

class DestinationNotFound(Exception):
    def __init__(self, destination: ActorAddr):
        self.destination = destination


class ActorCancelled(Exception):
    def __init__(self, actor: ActorAddr):
        self.actor = actor


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
class NormalExit:
    value: Any


class ErrorExit(Exception):
    def __init__(self, cause, context=None):
        self.cause = cause
        self.context = context



@dataclass
class Play:
    states: dict[ActorAddr, ActorState]
    actors: dict[ActorAddr, ActorSheet]
    protagonist: ActorAddr
    exit: ErrorExit | NormalExit | None = None


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

    def __init__(self, executor: Executor, queue_size=1024, clock_tick=0.15):
        self._counter = itertools.count()
        self.queue_size = queue_size
        self.executor = executor
        self.clock_tick = clock_tick
        self._events = queue.Queue()

    def make_addr(self, script, props) -> ActorAddr:
        addr = hash((script, props, next(self._counter)))
        return ActorAddr(addr)

    def __enter__(self):
        return self

    def __exit__(self, exc, typ, tb):
        self.executor.shutdown(cancel_futures=True)

    def _create_actor(self, script, props):
        addr = self.make_addr(script, props)
        mailbox = lambda: queue.Queue(self.queue_size)
        sheet = create_actor_sheet(script, props, addr, mailbox)
        return sheet

    def _handle_request(self, addr, request, play: Play):
        print(f"handling request: actor({addr}), request({request})")
        sheet = play.actors[addr]
        def request_done_callback(fut):
            print(f"Request task completed for actor {addr} {request=} {fut=}")
            self._events.put(RequestCompleted(
                actor=addr,
                request=request,
                future=fut
            ))

        match request:
            case Theatre.exit(value):
                print(f"actor({addr}) terminated with value {value}")
                play.states[addr] = Terminated(
                    result=value
                )

            case spawn(script, props):
                new_sheet = self._create_actor(script, props)
                play.actors[new_sheet.address] = new_sheet
                def init_callback(fut):
                    print(f"actor {new_sheet.address} done initializing {fut=}")
                    self._events.put(EndOfScene(
                        actor=new_sheet.address,
                        future=fut
                    ))
                future = self.executor.submit(
                    new_sheet.play.send, None
                )
                future.add_done_callback(init_callback)
                play.states[new_sheet.address] = Init(
                    future=future
                )
                resp_future = Future()
                resp_future.set_result(new_sheet.address)
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case Theatre.self():
                resp_future = Future()
                resp_future.set_result(addr)
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case send(dest_addr, msg):
                resp_future = Future()
                if destination := play.actors.get(dest_addr):
                    destination.mailbox.put(msg)
                    resp_future.set_result(None)
                else:
                    resp_future.set_exception(DestinationNotFound(dest_addr))
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case receive():
                resp_future = self.executor.submit(sheet.mailbox.get)
                resp_future.add_done_callback(request_done_callback)
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case Theatre.sleep(n):
                def delayed():
                    time.sleep(n)
                resp_future = self.executor.submit(delayed)
                resp_future.add_done_callback(request_done_callback)
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case _:
                print(f"unexpected request {request}")
                future = self.executor.submit(
                    sheet.play.throw, UnsupportedRequest(addr, request)
                )
                def done_callback(fut):
                    print(f"end of scene for actor {addr}, {future=}")
                    self._events.put(EndOfScene(
                        actor=addr,
                        future=fut
                    ))
                future.add_done_callback(done_callback)
                play.states[addr] = Executing(
                    future=future
                )

        return play

    def _process_state(self, addr: ActorAddr, play: Play):
        state = play.states[addr]
        sheet = play.actors[addr]

        match state:
            case Init(future) if future.done():
                try:
                    req = future.result()
                except StopIteration as ex:
                    play.states[addr] = Terminated(result=ex.value)
                    print(
                        f"actor {addr} terminated during init with value {ex.value}"
                    )
                except CancelledError as ex:
                    print(f"actor {addr} cancelled during init")
                    wrap = ActorCancelled(addr)
                    wrap.__cause__ = wrap.__context__ = ex
                    play.states[addr] = Terminated(error=wrap)
                except Exception as ex:
                    play.states[addr] = Terminated(error=ex)
                    print(f"actor {addr} died during init: {ex}")
                else:
                    play.states[addr] = Pending(request=req)
                    print(f"actor {addr} initialized, pending request {req}")

            case Pending(request=req, response_future=None):
                play = self._handle_request(addr, req, play)

            case Pending(request=req, response_future=fut) if (
                fut is not None and fut.done()
            ):
                print(f"actor({addr}) request({req}) response ready")
                if fut.cancelled():
                    print(f"actor({addr}) request({req}) cancelled")
                    exec_future = self.executor.submit(
                        sheet.play.throw, RequestCancelled(req)
                    )
                elif exception := fut.exception():
                    print(f"actor({addr}) request({req}) failed: {exception}")
                    exec_future = self.executor.submit(
                        sheet.play.throw, exception
                    )
                else:
                    print(f"actor({addr}) request({req}) succeeded")
                    exec_future = self.executor.submit(
                        sheet.play.send, fut.result()
                    )

                def done_callback(done_future):
                    print(f"end of scene for actor {addr}, {done_future=}")
                    self._events.put(EndOfScene(
                        actor=addr,
                        future=done_future
                    ))

                exec_future.add_done_callback(done_callback)
                play.states[addr] = Executing(future=exec_future)

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
                    play.states[addr] = Pending(request=req)
                    print(f"actor {addr} now pending request {req}")

            case Executing(future=fut):
                print(f"actor({addr}) still executing (future {fut})")
            case Terminated(result=result, error=error):
                if error:
                    print(f"actor {addr} is terminated with error: {error}")
                    if addr == play.protagonist:
                        print(f"protagonist ({addr}) terminated with error, setting exit error")
                        play.exit = ErrorExit(error)
                else:
                    print(f"actor {addr} is terminated with value {result}")
                    if addr == play.protagonist:
                        print(f"protagonist ({addr}) terminated with success, setting exit value")
                        play.exit = NormalExit(value=result)
                # TODO: handle terminated actors (links, cleanup)
                play.states.pop(addr)
                play.actors.pop(addr)
        return play

    def _chain_transitions(self, actor: ActorAddr, play: Play) -> Play:
        state = play.states[actor]
        print(f"Chaining transitions for actor {actor} from state {state} ")
        while play := self._process_state(actor, play):
            if actor not in play.states:
                print(f"State of actor {actor} disappeared during transition")
                break
            if play.states[actor] == state:
                # reached a blocked state, need to wait for event
                print(f"Reached steady state {state} for actor {actor}")
                break
            print(f"Transitioned actor {actor}: {state} -> {play.states[actor]}")
            state = play.states[actor]
        return play

    def _handle_event(self, event: Event, play: Play) -> Play:
        print(f"Handling event {event}")
        match event:
            case EndOfScene(actor=actor, future=future):
                if actor not in play.states:
                    print(f"Stale event: actor {actor} gone")
                    return play
                actor_state = play.states[actor]
                match actor_state:
                    case Executing(future=fut) | Init(future=fut):
                        assert future.done()
                        if future is not fut:
                            print(f"Stale event: actor {actor} state has different future {fut}")
                        # perform all possible state transitions
                        return self._chain_transitions(actor, play)
                    case state:
                        print(f"Stale event: actor {actor} has unexpected state {state}")
                        return play

            case RequestCompleted(actor=actor, request=request, future=future):
                if actor not in play.states:
                    print(f"Stale event: actor {actor} gone")
                    return play
                actor_state = play.states[actor]
                match actor_state:
                    case Pending(request=req, response_future=fut):
                        assert future.done()
                        if req is not request or fut is not future:
                            print(f"Stale event: actor {actor} state has different future {fut}")
                        return self._chain_transitions(actor, play)
                    case state:
                        print(f"Stale event: actor {actor} has unexpected state {state}")
                        return play
            case _:
                raise NotImplementedError(event)


    def run(self, main_actor, *args):
        main_props = args
        main_sheet = self._create_actor(main_actor, main_props)

        # Explicit state wrappers:
        # Init -> Pending(req) -> Executing -> Terminated
        actors: dict[ActorAddr, ActorSheet] = {main_sheet.address: main_sheet}
        protagonist_init_future = self.executor.submit(main_sheet.play.send, None)
        def init_callback(fut):
            print(f"protagonist done initializing {fut=}")
            self._events.put(EndOfScene(
                actor=main_sheet.address,
                future=fut
            ))
        protagonist_init_future.add_done_callback(init_callback)
        states: dict[ActorAddr, ActorState] = {
            main_sheet.address: Init(
                future=protagonist_init_future
            )
        }

        play = Play(
            states=states,
            actors=actors,
            protagonist=main_sheet.address
        )

        loop_count = itertools.count()
        while states:
            cnt = next(loop_count)
            print(f"Running main loop ({cnt})")
            print(f"{len(states)} actors on stage")
            print(f"{threading.active_count()} active threads")

            events = list(drain(self._events, timeout=self.clock_tick))
            if not events:
                print(f"({cnt}) No events in last cycle ({self.clock_tick}s)")
                continue

            print(f"{len(events)} events to handle")
            for event in events:
                play = self._handle_event(event, play)
                print(f"Handled event {event}")

            # for addr in list(states.keys()):
            #     play = self._process_state(addr, play)
            #     print(f"Processed state of actor {addr=}")

        print(f"Terminating play: {play.exit=}")

        match play.exit:
            case ErrorExit():
                raise play.exit
            case NormalExit(value=value):
                return value
            case None:
                return None

