import pytest
from concurrent.futures import Future, ThreadPoolExecutor
from theatre.threaded_theatre import (
    Theatre,
    receive,
    spawn,
    send,
    DestinationNotFound,
    curtain_call,
    RequestCancelled,
    ActorCancelled,
    UnsupportedRequest,
    drain,
    ErrorExit,
    NormalExit,
    Receiving,
    ActorTerminated,
    MailboxFull,
)
import queue


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
        yield Theatre.exit()

    with curtain_call() as theatre:
        theatre.run(main_actor)


def test_theatre_run_get_self():
    def main_actor(*args):
        me = yield Theatre.self()
        return me

    with curtain_call() as theatre:
        result = theatre.run(main_actor)
        assert result is not None


def test_theatre_run_actor_self_send():
    def main_actor(*args):
        me = yield Theatre.self()
        yield send(me, "Hello")
        msg = yield receive()
        assert msg == "Hello"

    with curtain_call() as theatre:
        theatre.run(main_actor)


def test_theatre_run_actor_spawn():
    def sub_actor(parent, *args):
        me = yield Theatre.self()
        msg = yield receive()
        assert msg == "Hello"
        yield send(parent, "mom")

    def main_actor(*args):
        me = yield Theatre.self()
        child = yield spawn(sub_actor, (me,))
        yield send(child, "Hello")
        msg = yield receive()
        assert msg == "mom"

    with curtain_call() as theatre:
        theatre.run(main_actor)


def test_theatre_run_actor_unsupported_request():
    from dataclasses import dataclass

    @dataclass
    class strange_request:
        pass

    def main_actor(*args):
        with pytest.raises(UnsupportedRequest):
            yield strange_request()

    with curtain_call() as theatre:
        theatre.run(main_actor)


# termination scenarios
def test_theatre_run_actor_terminated():
    main_me = None

    def sub_actor(msg, *args):
        yield send(main_me, f"sub_received:{msg}")
        yield Theatre.exit("sub_success")

    def main_actor(*args):
        nonlocal main_me
        main_me = yield Theatre.self()
        child = yield spawn(sub_actor, ("test",))
        msg = yield receive()
        assert msg == "sub_received:test"
        yield Theatre.exit("main_success")

    with curtain_call() as theatre:
        result = theatre.run(main_actor)
        assert result == "main_success"


def test_theatre_run_actor_terminated_with_value():
    def simple_actor(*args):
        yield Theatre.exit(42)

    with curtain_call() as theatre:
        result = theatre.run(simple_actor)
        assert result == 42


def test_theatre_run_actor_terminated_with_error():
    def failing_actor(*args):
        yield Theatre.sleep(0)
        raise Exception("Forgot my lines")
        yield Theatre.exit()

    with curtain_call() as theatre:
        with pytest.raises(Exception) as ex:
            theatre.run(failing_actor)
        assert ex.value.args == ("Forgot my lines",)


def test_theatre_run_multiple_actors_terminated():
    def worker(name):
        yield Theatre.self()
        yield Theatre.exit(f"{name}_done")

    def main_actor(*args):
        yield Theatre.self()
        w1 = yield spawn(worker, ("w1",))
        w2 = yield spawn(worker, ("w2",))
        yield Theatre.exit("all_done")

    with curtain_call() as theatre:
        result = theatre.run(main_actor)
        assert result == "all_done"


def test_send_to_terminated_actor_raises():
    def target_actor(*args):
        yield Theatre.exit("target_done")

    def sender(*args):
        doomed = yield spawn(target_actor)
        yield Theatre.sleep(0.005)
        yield send(doomed, "test")

    with curtain_call() as theatre:
        with pytest.raises(ActorTerminated) as exc:
            result = theatre.run(sender)


def test_send_to_terminated_actor_caught():
    def target_actor(*args):
        yield Theatre.exit("target_done")

    def sender(*args):
        doomed = yield spawn(target_actor)
        yield Theatre.sleep(0.01)
        try:
            yield send(doomed, "test")
        except ActorTerminated:
            pass
        yield Theatre.exit("sender_success")

    with curtain_call() as theatre:
        result = theatre.run(sender)
        assert result == "sender_success"


def test_run_actor_dies_during_init():
    def protagonist(*args):
        raise Exception()
        yield Theatre.exit("sender_success")

    with curtain_call() as theatre:
        with pytest.raises(Exception):
            result = theatre.run(protagonist)


def test_run_actor_returns_during_init():
    def protagonist(*args):
        return 0
        yield Theatre.exit("sender_success")

    with curtain_call() as theatre:
        result = theatre.run(protagonist)
        assert result == 0


def test_cancelled_init():
    from unittest.mock import create_autospec

    mock_executor = create_autospec(ThreadPoolExecutor, instance=True)
    future = Future()
    future.cancel()
    mock_executor.submit.return_value = future

    def main_actor():
        yield Theatre.self()

    with Theatre(mock_executor) as theatre:
        with pytest.raises(ActorCancelled) as exc:
            theatre.run(main_actor)


@pytest.mark.skip(reason="cancellation does not work for receive requests right now")
def test_cancelled_request():
    def main_actor(*args):
        try:
            msg = yield receive()
        except RequestCancelled as e:
            assert isinstance(e.request, receive)
            yield Theatre.exit("cancelled_ok")

    with curtain_call() as theatre:
        with theatre.spawn(main_actor) as addr:
            theatre.cancel(addr)
        assert result == "cancelled_ok"


def test_non_blocking_receive():
    def waiter(name, *args):
        msg = yield receive()
        yield Theatre.exit(f"{name}:{msg}")

    def sender(*args):
        w1 = yield spawn(waiter, ("w1",))
        w2 = yield spawn(waiter, ("w2",))
        yield Theatre.sleep(0.01)
        yield send(w1, "hello")
        yield send(w2, "world")
        yield Theatre.exit("done")

    with curtain_call(executor=ThreadPoolExecutor(max_workers=1)) as theatre:
        result = theatre.run(sender)
        assert result == "done"


def test_theatre_spawn_and_spotlight():
    def waiter():
        msg = yield receive()
        return msg

    def replier(address):
        yield send(address, "hello")

    with curtain_call() as theatre:
        waiter_address = theatre.spawn(waiter, protagonist=True)
        theatre.spawn(replier, waiter_address)
        result = theatre.spotlight(waiter_address)
        assert result == "hello"


def test_theatre_wait_ensemble():
    def waiter():
        msg = yield receive()
        return msg

    def replier(addresses):
        for addr in addresses:
            yield send(addr, f"hello {addr}")

    with curtain_call() as theatre:
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
        yield send(me, "ping")
        yield send(me, "pong")
        yield send(me, "ping")
        yield Theatre.exit("ok")

    def main_actor(*args):
        me = yield Theatre.self()
        child = yield spawn(worker, (me,))
        msg = yield receive(filter=lambda m: m == "pong")
        assert msg == "pong"
        remaining1 = yield receive()
        assert remaining1 == "ping"
        remaining2 = yield receive()
        assert remaining2 == "ping"
        yield Theatre.exit("ok")

    with curtain_call() as theatre:
        result = theatre.run(main_actor)
        assert result == "ok"


def test_selective_receive_no_match_parks():
    def worker(me):
        msg = yield receive(filter=lambda m: m == "special")
        yield Theatre.exit(msg)

    def main_actor(*args):
        me = yield Theatre.self()
        child = yield spawn(worker, (me,))
        yield send(me, "noise")  # noise to main, worker stays parked
        yield send(child, "special")  # now worker matches
        msg = yield receive()
        assert msg == "noise"
        yield Theatre.exit("done")

    with curtain_call() as theatre:
        result = theatre.run(main_actor)
        assert result == "done"


def test_mailbox_full_in_send_raises():
    def sender(*args):
        doomed = yield spawn(filler)
        yield send(doomed, "x")
        yield send(doomed, "y")
        try:
            yield send(doomed, "overflow")
        except MailboxFull:
            yield Theatre.exit("caught")

    def filler(*args):
        yield receive(filter=lambda msg: msg == "z")
        yield Theatre.exit("done")

    with curtain_call(queue_size=2) as theatre:
        result = theatre.run(sender)
        assert result == "caught"
