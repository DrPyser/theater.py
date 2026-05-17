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
    states: list[ActorState]
    actors: list[ActorSheet]
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

    def __init__(self, executor: Executor, queue_size=1024, clock_tick=0.0):
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
        match request:
            case Theatre.exit(value):
                print(f"actor({addr}) terminated with value {value}")
                play.states[addr] = Terminated(
                    result=value
                )

            case spawn(script, props):
                new_sheet = self._create_actor(script, props)
                play.actors[new_sheet.address] = new_sheet
                play.states[new_sheet.address] = Init(
                    future=self.executor.submit(
                        new_sheet.play.send, None
                    )
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
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case Theatre.sleep(n):
                def delayed():
                    time.sleep(n)
                resp_future = self.executor.submit(delayed)
                play.states[addr] = Pending(
                    request=request, response_future=resp_future
                )
            case _:
                print(f"unexpected request {request}")
                play.states[addr] = Executing(
                    future=self.executor.submit(
                        sheet.play.throw, UnsupportedRequest(addr, request)
                    )
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

                def done_callback(fut):
                    print(f"future {fut} done")

                exec_future.add_done_callback(done_callback)
                play.states[addr] = Executing(future=exec_future)

            case Executing(future=fut) if fut.done():
                try:
                    req = fut.result()
                except StopIteration as ex:
                    play.states[addr] = Terminated(result=ex.value)
                    print(f"actor {addr} terminated with value {ex.value}")
                    if addr == play.protagonist:
                        print(
                            f"Main actor terminated, capturing exit value: {ex.value}"
                        )
                        exit_value = ex.value
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

    def run(self, main_actor, *args):
        main_props = args
        main_sheet = self._create_actor(main_actor, main_props)

        # Explicit state wrappers:
        # Init -> Pending(req) -> Executing -> Terminated
        actors: dict[ActorAddr, ActorSheet] = {main_sheet.address: main_sheet}
        states: dict[ActorAddr, ActorState] = {
            main_sheet.address: Init(
                future=self.executor.submit(main_sheet.play.send, None)
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

            # events = drain(self._events, timeout=self.clock_tick)
            # if not events:
            #     print(f"({cnt}) No events in last cycle ({self.clock_tick}s)")
            #     continue

            # for event in events:
            #     states, actors, exit_value, exit_error = self._handle_event(event, states, actors)

            for addr in list(states.keys()):
                play = self._process_state(addr, play)
                print(f"Processed state of actor {addr=}")

        print(f"Terminating play: {play.exit=}")

        match play.exit:
            case ErrorExit():
                raise play.exit
            case NormalExit(value=value):
                return value
            case None:
                return None

