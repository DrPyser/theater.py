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
    UnsupportedRequest
)


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

    with curtain_call(clock_tick=0.0) as theatre:
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

    with curtain_call(clock_tick=0.01) as theatre:
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
        with pytest.raises(DestinationNotFound):
            result = theatre.run(sender)


def test_send_to_terminated_actor_caught():
    def target_actor(*args):
        yield Theatre.exit("target_done")

    def sender(*args):
        doomed = yield spawn(target_actor)
        yield Theatre.sleep(0.01)
        try:
            yield send(doomed, "test")
        except DestinationNotFound:
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
        with pytest.raises(ActorCancelled):
            theatre.run(main_actor)


def test_cancelled_request():
    real_executor = ThreadPoolExecutor(max_workers=1)

    class CancellingExecutor:
        def __init__(self):
            self._real = real_executor
            self._call_count = 0

        def submit(self, fn, *args):
            self._call_count += 1
            if self._call_count == 2:
                f = Future()
                f.cancel()
                return f
            return self._real.submit(fn, *args)

        def shutdown(self, cancel_futures=False):
            self._real.shutdown(cancel_futures=cancel_futures)

    def main_actor(*args):
        try:
            msg = yield receive()
        except RequestCancelled as e:
            assert isinstance(e.req, receive)
            yield Theatre.exit("cancelled_ok")

    with Theatre(CancellingExecutor()) as theatre:
        result = theatre.run(main_actor)
        assert result == "cancelled_ok"



# def test_threadpool_exhaustion():
#     def abandonned(*args):
#         msg = yield receive()
#         yield Theatre.exit(msg)

#     def spawner(*args):
#         for i in range(10):
#             yield spawn(abandonned)

#     with curtain_call() as theatre:
#         theatre.run(spawner)
