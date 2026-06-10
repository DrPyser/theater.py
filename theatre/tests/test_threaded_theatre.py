import dataclasses
import queue
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

import pytest

from theatre.interfaces import System
from theatre.threaded_theatre import (
    NormalExit,
    ReceiveTimeout,
    Signal,
    Theatre,
    UnsupportedRequest,
    curtain_call,
    drain,
)

from theatre.context import whoami, get_logger, whoisparent


def test_drain_empty_queue():
    q = queue.Queue()
    assert list(drain(q, timeout=0.0)) == []


def test_drain_non_empty_queue():
    q = queue.Queue()
    q.put_nowait(1)
    assert list(drain(q)) == [1]


def test_drain_multiple_items():
    q = queue.Queue()
    for i in range(10):
        q.put_nowait(i)
    assert list(drain(q)) == list(range(10))


def test_theatre_run():
    def main_actor(*args):
        print(f"Received args {args}")
        yield System.exit()

    with curtain_call(max_idle=1) as theatre:
        theatre.run(main_actor)


def test_theatre_run_get_self():
    def main_actor(*args):
        me = yield System.whoami()
        return me

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(main_actor)
        assert result is not None


def test_theatre_run_actor_self_send():
    def main_actor(*args):
        me = yield System.whoami()
        yield System.send(me, "Hello")
        msg = yield System.receive()
        assert msg == "Hello"

    with curtain_call(max_idle=1) as theatre:
        theatre.run(main_actor)


def test_theatre_run_actor_spawn():
    def sub_actor(parent, *args):
        me = yield System.whoami()
        msg = yield System.receive()
        assert msg == "Hello"
        yield System.send(parent, "mom")

    def main_actor(*args):
        me = yield System.whoami()
        child = yield System.spawn(sub_actor, (me,))
        yield System.send(child, "Hello")
        msg = yield System.receive()
        assert msg == "mom"

    with curtain_call(max_idle=1) as theatre:
        theatre.run(main_actor)


def test_theatre_run_actor_unsupported_request():
    from dataclasses import dataclass

    @dataclass
    class strange_request:
        pass

    def main_actor(*args):
        with pytest.raises(UnsupportedRequest):
            yield strange_request()

    with curtain_call(max_idle=1) as theatre:
        theatre.run(main_actor)


# termination scenarios
def test_theatre_run_actor_terminated():
    main_me = None

    def sub_actor(msg, *args):
        yield System.send(main_me, f"sub_received:{msg}")
        yield System.exit("sub_success")

    def main_actor(*args):
        nonlocal main_me
        main_me = yield System.whoami()
        child = yield System.spawn(sub_actor, ("test",))
        msg = yield System.receive()
        assert msg == "sub_received:test"
        yield System.exit("main_success")

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(main_actor)
        assert result == "main_success"


def test_theatre_run_actor_terminated_with_value():
    def simple_actor(*args):
        yield System.exit(42)

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(simple_actor)
        assert result == 42


def test_theatre_run_actor_terminated_with_error():
    def failing_actor(*args):
        yield System.sleep(0)
        raise Exception("Forgot my lines")
        yield System.exit()

    with curtain_call(max_idle=1) as theatre:
        with pytest.raises(Exception) as ex:
            theatre.run(failing_actor)
        assert ex.value.args == ("Forgot my lines",)


def test_theatre_run_multiple_actors_terminated():
    def worker(name):
        yield System.whoami()
        yield System.exit(f"{name}_done")

    def main_actor(*args):
        yield System.whoami()
        w1 = yield System.spawn(worker, ("w1",))
        w2 = yield System.spawn(worker, ("w2",))
        yield System.exit("all_done")

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(main_actor)
        assert result == "all_done"


def test_send_to_terminated_actor_raises():
    def target_actor(*args):
        yield System.exit("target_done")

    def sender(*args):
        doomed = yield System.spawn(target_actor)
        yield System.sleep(0.005)
        yield System.send(doomed, "test")

    with curtain_call(max_idle=1) as theatre:
        with pytest.raises(Signal.ActorTerminated) as exc:
            result = theatre.run(sender)


def test_send_to_terminated_actor_caught():
    def target_actor(*args):
        yield System.exit("target_done")

    def sender(*args):
        doomed = yield System.spawn(target_actor)
        yield System.sleep(0.001)
        try:
            yield System.send(doomed, "test")
        except Signal.ActorTerminated:
            pass
        yield System.exit("sender_success")

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(sender)
        assert result == "sender_success"


def test_run_actor_dies_during_init():
    def protagonist(*args):
        raise Exception()
        yield System.exit("sender_success")

    with curtain_call(max_idle=1) as theatre:
        with pytest.raises(Exception):
            result = theatre.run(protagonist)


def test_run_actor_returns_during_init():
    def protagonist(*args):
        return 0
        yield System.exit("sender_success")

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(protagonist)
        assert result == 0


def test_cancelled_init():
    from unittest.mock import create_autospec

    mock_executor = create_autospec(ThreadPoolExecutor, instance=True)
    future = Future()
    future.cancel()
    mock_executor.submit.return_value = future

    def main_actor():
        yield System.whoami()

    with Theatre(mock_executor) as theatre:
        with pytest.raises(Signal.INT) as exc:
            theatre.run(main_actor)


# @pytest.mark.skip(reason="cancellation does not work for receive requests right now")
def test_cancelled_request():
    def main_actor(*args):
        try:
            msg = yield System.receive()
        except Signal.INT:
            yield System.exit("cancelled_ok")

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(main_actor)
        theatre.cancel(addr)
        result = theatre.spotlight(addr)
        assert result == "cancelled_ok"


def test_non_blocking_receive():
    def waiter(name, *args):
        msg = yield System.receive()
        yield System.exit(f"{name}:{msg}")

    def sender(*args):
        w1 = yield System.spawn(waiter, ("w1",))
        w2 = yield System.spawn(waiter, ("w2",))
        yield System.sleep(0.001)
        yield System.send(w1, "hello")
        yield System.send(w2, "world")
        yield System.exit("done")

    with curtain_call(
        max_idle=1, executor=ThreadPoolExecutor(max_workers=1)
    ) as theatre:
        result = theatre.run(sender)
        assert result == "done"


def test_theatre_spawn_and_spotlight():
    def waiter():
        msg = yield System.receive()
        return msg

    def replier(address):
        yield System.send(address, "hello")

    with curtain_call(max_idle=1) as theatre:
        waiter_address = theatre.spawn(waiter)
        theatre.spawn(replier, waiter_address)
        result = theatre.spotlight(waiter_address)
        assert result == "hello"


def test_theatre_wait_ensemble():
    def waiter():
        msg = yield System.receive()
        return msg

    def replier(addresses):
        for addr in addresses:
            yield System.send(addr, f"hello {addr}")

    with curtain_call(max_idle=1) as theatre:
        waiters = []
        for i in range(10):
            waiter_address = theatre.spawn(waiter)
            waiters.append(waiter_address)
        replier_addr = theatre.spawn(replier, tuple(waiters))
        results = theatre.wait_ensemble()
        for addr, res in results:
            if addr in waiters:
                assert res == NormalExit(f"hello {addr}")
            else:
                assert addr == replier_addr
                assert res == NormalExit(None)


def test_selective_receive():
    def worker(me):
        yield System.send(me, "ping")
        yield System.send(me, "pong")
        yield System.send(me, "ping")
        yield System.exit("ok")

    def main_actor(*args):
        me = yield System.whoami()
        child = yield System.spawn(worker, (me,))
        msg = yield System.receive(filter=lambda m: m == "pong")
        assert msg == "pong"
        remaining1 = yield System.receive()
        assert remaining1 == "ping"
        remaining2 = yield System.receive()
        assert remaining2 == "ping"
        yield System.exit("ok")

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(main_actor)
        assert result == "ok"


def test_selective_receive_no_match_parks():
    def worker(me):
        msg = yield System.receive(filter=lambda m: m == "special")
        yield System.exit(msg)

    def main_actor(*args):
        me = yield System.whoami()
        child = yield System.spawn(worker, (me,))
        yield System.send(me, "noise")  # noise to main, worker stays parked
        yield System.send(child, "special")  # now worker matches
        msg = yield System.receive()
        assert msg == "noise"
        yield System.exit("done")

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(main_actor)
        assert result == "done"


def test_mailbox_full_in_send_raises():
    def sender(*args):
        doomed = yield System.spawn(filler)
        yield System.send(doomed, "x")
        yield System.send(doomed, "y")
        try:
            yield System.send(doomed, "overflow")
            yield System.receive()
        except Signal.MailboxFull:
            yield System.exit("caught")

    def filler(*args):
        yield System.receive(filter=lambda msg: msg == "z")
        yield System.exit("done")

    with curtain_call(max_idle=1, queue_size=2) as theatre:
        result = theatre.run(sender)
        assert result == "caught"


def test_sigkill():
    def blocker(*args):
        yield System.receive(filter=lambda msg: False)
        yield System.exit("done")

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(blocker)
        theatre.kill(addr)
        with pytest.raises(Signal.KILL) as exc:
            theatre.spotlight(addr)


def test_link_trap_while_receive():
    def link_target(*args):
        yield System.receive()
        yield System.exit("done")

    def linker(*args):
        addr = yield System.spawn(link_target)
        yield System.link(addr)
        yield System.send(addr, "hello")
        try:
            yield System.receive()
        except Signal.ActorTerminated as ex:
            assert ex.actor == addr
            assert ex.cause == NormalExit("done")

    with curtain_call(max_idle=1) as theatre:
        theatre.run(linker)


def test_link_trap_while_sleeping():
    def link_target(*args):
        yield System.receive()
        yield System.exit("done")

    def linker(*args):
        addr = yield System.spawn(link_target)
        yield System.link(addr)
        yield System.send(addr, "hello")
        try:
            yield System.sleep(5)
        except Signal.ActorTerminated as ex:
            assert ex.actor == addr
            assert ex.cause == NormalExit("done")

    with curtain_call(max_idle=1) as theatre:
        theatre.run(linker)


def test_link_trap_after_termination():
    def link_target(*args):
        yield System.receive()
        yield System.exit("done")

    def linker(*args):
        addr = yield System.spawn(link_target)
        yield System.link(addr)
        yield System.send(addr, "hello")

    with curtain_call(max_idle=1) as theatre:
        theatre.run(linker)


def test_spawn_link_trap_while_receiving():
    def link_target(*args):
        msg = yield System.receive()
        print(f"Received message: {msg}")
        yield System.exit("done")

    def linker(*args):
        addr = yield System.spawn_link(link_target)
        yield System.send(addr, "hello")
        try:
            yield System.receive()
        except Signal.ActorTerminated as ex:
            assert ex.actor == addr
            assert ex.cause == NormalExit("done")

    with curtain_call(max_idle=1) as theatre:
        theatre.run(linker)


def test_fire_in_theatre_while_receive():
    def actor(*args):
        while True:
            msg = yield System.receive()

    with pytest.raises(Exception):
        with curtain_call(max_idle=1) as theatre:
            theatre.spawn(actor)
            raise Exception()
    assert threading.active_count() == 1


def test_fire_in_theatre_while_sleep():
    def actor(*args):
        while True:
            msg = yield System.sleep(5)

    with pytest.raises(Exception):
        with curtain_call(max_idle=1) as theatre:
            theatre.spawn(actor)
            raise Exception()
    assert threading.active_count() == 1


def test_receive_with_timeout():
    def actor(*args):
        while True:
            try:
                t = time.time()
                msg = yield System.receive(timeout=0.05)
            except ReceiveTimeout as ex:
                t2 = time.time()
                print(f"{t2} - {t} = {t2 - t}")
                return ex

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(actor)
        assert isinstance(result, ReceiveTimeout)
    assert threading.active_count() == 1


def test_sleep_interrupted_by_sigint():
    def sleeper(*args):
        try:
            yield System.sleep(60)
        except Signal.INT:
            yield System.exit("interrupted")

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(sleeper)
        time.sleep(0.05)
        theatre.cancel(addr)
        result = theatre.spotlight(addr)
        assert result == "interrupted"


def test_sleep_interrupted_by_sigkill():
    def sleeper(*args):
        yield System.sleep(60)

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(sleeper)
        time.sleep(0.05)
        theatre.kill(addr)
        with pytest.raises(Signal.KILL) as exc:
            theatre.spotlight(addr)


def test_signal_all_interrupts_sleep():
    N = 3

    def sleeper(*args):
        try:
            yield System.sleep(60)
        except Signal.INT:
            yield System.exit("interrupted")

    with curtain_call(max_idle=1) as theatre:
        addrs = [theatre.spawn(sleeper) for _ in range(N)]
        time.sleep(0.05)
        theatre.signal_all(Signal.INT())
        results = theatre.wait_ensemble()
        for addr, cause in results:
            if addr in addrs:
                assert isinstance(cause, NormalExit)
                assert cause.value == "interrupted"


def test_receive_timeout_interrupted_by_sigint():
    def actor(*args):
        try:
            msg = yield System.receive(timeout=5)
        except Signal.INT:
            yield System.exit("cancelled")

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(actor)
        time.sleep(0.05)
        theatre.cancel(addr)
        result = theatre.spotlight(addr)
        assert result == "cancelled"


def test_sigint_on_terminated_actor_is_noop():
    def actor(*args):
        yield System.exit("done")

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(actor)
        result = theatre.spotlight(addr)
        assert result == "done"
        theatre.cancel(addr)
        result2 = theatre.spotlight(addr)
        assert result2 == "done"


def test_receive_timeout_cancelled_on_message():
    def receiver(*args):
        msg = yield System.receive(timeout=5)
        yield System.exit(msg)

    def sender(target, *args):
        yield System.send(target, "hello")

    with curtain_call(max_idle=1) as theatre:
        recv = theatre.spawn(receiver)
        theatre.spawn(sender, recv)
        result = theatre.spotlight(recv)
        assert result == "hello"


def test_receive_timeout_with_filter_matches_before_timeout():
    def receiver(*args):
        msg = yield System.receive(filter=lambda m: m == "hello", timeout=5)
        yield System.exit(msg)

    def sender(target, *args):
        yield System.send(target, "hello")

    with curtain_call(max_idle=1) as theatre:
        recv = theatre.spawn(receiver)
        theatre.spawn(sender, recv)
        result = theatre.spotlight(recv)
        assert result == "hello"


def test_receive_timeout_with_filter_no_match_expires():
    def actor(*args):
        try:
            msg = yield System.receive(filter=lambda m: False, timeout=0.05)
        except ReceiveTimeout as ex:
            return ex

    with curtain_call(max_idle=1) as theatre:
        result = theatre.run(actor)
        assert isinstance(result, ReceiveTimeout)
    assert threading.active_count() == 1


def test_stale_receive_timeout_ignored():
    def receiver(*args):
        msg = yield System.receive(timeout=0.3)
        yield System.sleep(0.005)
        yield System.exit(msg)

    def sender(target, *args):
        yield System.send(target, "hello")

    with curtain_call(max_idle=1) as theatre:
        recv = theatre.spawn(receiver)
        theatre.spawn(sender, recv)
        result = theatre.spotlight(recv)
        assert result == "hello"


def test_theatre_send():
    def catcher(i):
        msg = yield System.receive()
        assert msg == i
        return i

    with curtain_call(max_idle=1) as theatre:
        pool = []
        for i in range(10):
            pool.append((i, theatre.spawn(catcher, i)))

        for n, actor in pool:
            theatre.send(actor, n)

        theatre.wait_ensemble()


def test_census_returns_actor_infos():
    def simple(*args):
        yield System.exit(42)

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(simple)
        time.sleep(0.05)
        infos = theatre.census()
        assert len(infos) == 1
        info = infos[0]
        assert info.address == addr
        assert info.script.endswith("simple")
        assert isinstance(info.state_name, str)


def test_census_multiple_actors():
    def worker(name, *args):
        yield System.receive()
        yield System.exit(name)

    with curtain_call(max_idle=1) as theatre:
        a1 = theatre.spawn(worker, "a")
        a2 = theatre.spawn(worker, "b")
        theatre.send(a1, "go")
        theatre.send(a2, "go")
        theatre.wait_ensemble()
        infos = theatre.census()
        assert len(infos) == 2
        for info in infos:
            assert info.state_name == "Terminated"
            assert info.props == (info.exit_cause.value,)


def test_census_frozen():
    def simple(*args):
        yield System.exit(42)

    with curtain_call(max_idle=1) as theatre:
        theatre.spawn(simple)
        time.sleep(0.05)
        infos = theatre.census()
        info = infos[0]
        with pytest.raises((AttributeError, dataclasses.FrozenInstanceError)):
            info.state_name = "mutated"


def test_census_live_actor_has_correct_state():
    def waiter(*args):
        msg = yield System.receive()
        yield System.exit(msg)

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(waiter)
        time.sleep(0.05)
        infos = theatre.census()
        info = next(i for i in infos if i.address == addr)
        assert info.state_name == "Receiving"
        assert info.mailbox_size == 0
        assert info.exit_cause is None


def test_call():
    def blocking_task():
        time.sleep(0.05)
        return 42

    def waiter(*args):
        res = yield System.call(blocking_task)
        yield System.exit(res)

    with curtain_call(max_idle=1) as theatre:
        res = theatre.run(waiter)
        assert res == 42


def test_call_exception():
    def blocking_task():
        time.sleep(0.05)
        raise Exception("Oops")

    def waiter(*args):
        res = yield System.call(blocking_task)
        yield System.exit(res)

    with curtain_call(max_idle=1) as theatre:
        with pytest.raises(Exception):
            theatre.run(waiter)


def test_get_self_from_context():
    def amnesiac():
        self = whoami()
        yield System.exit(self)

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(amnesiac)
        result = theatre.spotlight(addr)
        assert result == addr


def test_log_from_context():
    import logging
    import io

    def amnesiac():
        logger = get_logger()
        buffer = io.StringIO()
        logger.addHandler(logging.StreamHandler(buffer))
        logger.info("Logging from %s", whoami())
        yield System.exit(buffer.getvalue())

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(amnesiac)
        result = theatre.spotlight(addr)
        assert f"Logging from {addr}"


def test_parent_from_context():
    def child():
        yield System.exit(whoisparent())

    def parent():
        child_addr = yield System.spawn(child)
        yield System.exit(child_addr)

    with curtain_call(max_idle=1) as theatre:
        addr = theatre.spawn(parent)
        child_addr = theatre.spotlight(addr)
        result = theatre.spotlight(child_addr)
        assert result == addr
