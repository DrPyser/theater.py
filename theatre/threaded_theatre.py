from theatre.interfaces import receive, Actor, Exit, ActorSheet, send, spawn
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, CancelledError
import itertools
from queue import Queue
from collections import deque
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

    def make_addr(self, script, props) -> ActorAddr:
        addr = hash((script, props, next(self._counter)))
        return ActorAddr(addr)

    def __enter__(self):
        return self

    def __exit__(self, exc, typ, tb):
        self.executor.shutdown(cancel_futures=True)

    def _create_actor(self, script, props):
        addr = self.make_addr(script, props)
        mailbox = lambda: Queue(self.queue_size)
        sheet = create_actor_sheet(script, props, addr, mailbox)
        return sheet

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
        exit_value = None
        exit_error = None

        loop_count = itertools.count()
        while states:
            cnt = next(loop_count)
            print(f"Running main loop ({cnt})")
            print(f"{len(states)} actors on stage")
            print(f"{threading.active_count()} active threads")
            for addr in list(states.keys()):
                state = states[addr]
                sheet = actors[addr]

                match state:
                    case Init(future) if future.done():
                        try:
                            req = future.result()
                        except StopIteration as ex:
                            states[addr] = Terminated(result=ex.value)
                            print(
                                f"actor {addr} terminated during init with value {ex.value}"
                            )
                        except CancelledError as ex:
                            print(f"actor {addr} cancelled during init")
                            wrap = ActorCancelled(addr)
                            wrap.__cause__ = wrap.__context__ = ex
                            states[addr] = Terminated(error=wrap)
                        except Exception as ex:
                            states[addr] = Terminated(error=ex)
                            print(f"actor {addr} died during init: {ex}")
                        else:
                            states[addr] = Pending(request=req)
                            print(f"actor {addr} initialized, pending request {req}")

                    case Pending(request=req, response_future=None):
                        print(f"handling request: actor({addr}), request({req})")
                        match req:
                            case Theatre.exit(value):
                                print(f"actor({addr}) terminated with value {value}")
                                states[addr] = Terminated(
                                    result=value
                                )

                            case spawn(script, props):
                                new_sheet = self._create_actor(script, props)
                                actors[new_sheet.address] = new_sheet
                                states[new_sheet.address] = Init(
                                    future=self.executor.submit(
                                        new_sheet.play.send, None
                                    )
                                )
                                resp_future = Future()
                                resp_future.set_result(new_sheet.address)
                                states[addr] = Pending(
                                    request=req, response_future=resp_future
                                )
                            case Theatre.self():
                                resp_future = Future()
                                resp_future.set_result(addr)
                                states[addr] = Pending(
                                    request=req, response_future=resp_future
                                )
                            case send(dest_addr, msg):
                                resp_future = Future()
                                if destination := actors.get(dest_addr):
                                    destination.mailbox.put(msg)
                                    resp_future.set_result(None)
                                else:
                                    resp_future.set_exception(DestinationNotFound(dest_addr))
                                states[addr] = Pending(
                                    request=req, response_future=resp_future
                                )
                            case receive():
                                resp_future = self.executor.submit(sheet.mailbox.get)
                                states[addr] = Pending(
                                    request=req, response_future=resp_future
                                )
                            case Theatre.sleep(n):
                                def delayed():
                                    time.sleep(n)
                                resp_future = self.executor.submit(delayed)
                                states[addr] = Pending(
                                    request=req, response_future=resp_future
                                )
                            case _:
                                print(f"unexpected request {req}")
                                states[addr] = Executing(
                                    future=self.executor.submit(
                                        sheet.play.throw, UnsupportedRequest(addr, req)
                                    )
                                )

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
                        states[addr] = Executing(future=exec_future)

                    case Executing(future=fut) if fut.done():
                        try:
                            req = fut.result()
                        except StopIteration as ex:
                            states[addr] = Terminated(result=ex.value)
                            print(f"actor {addr} terminated with value {ex.value}")
                            if addr == main_sheet.address:
                                print(
                                    f"Main actor terminated, capturing exit value: {ex.value}"
                                )
                                exit_value = ex.value
                        except Exception as ex:
                            states[addr] = Terminated(error=ex)
                            print(f"actor {addr} died: {ex}")
                        else:
                            states[addr] = Pending(request=req)
                            print(f"actor {addr} now pending request {req}")

                    case Executing(future=fut):
                        print(f"actor({addr}) still executing (future {fut})")
                    case Terminated(result=result, error=error):
                        if error:
                            print(f"actor {addr} is terminated with error: {error}")
                            if addr == main_sheet.address:
                                exit_error = error
                        else:
                            print(f"actor {addr} is terminated with value {result}")
                            if addr == main_sheet.address:
                                exit_value = result
                        # TODO: handle terminated actors (links, cleanup)
                        states.pop(addr)
                        actors.pop(addr)
            time.sleep(self.clock_tick)
        print(f"Terminating play: {exit_error=} {exit_value=}")
        if exit_error:
            raise exit_error
        return exit_value


