import pytest
from theatre.threaded_theatre import Theatre, receive, spawn, send


def test_theatre_run():
    def main_actor(*args):
        print(f"Received args {args}")
        yield Theatre.exit()

    with Theatre() as theatre:
        theatre.run(main_actor)



def test_theatre_run_get_self():
    def main_actor(*args):
        me = yield Theatre.self()
        return me

    with Theatre() as theatre:
        result = theatre.run(main_actor)
        assert result is not None


def test_theatre_run_actor_self_send():
    def main_actor(*args):
        me = yield Theatre.self()
        yield send(me, "Hello")
        msg = yield receive()
        assert msg == "Hello"

    with Theatre() as theatre:
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

    with Theatre(clock_tick=0.0) as theatre:
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

    with Theatre() as theatre:
        result = theatre.run(main_actor)
        assert result == "main_success"


def test_theatre_run_actor_terminated_with_value():
    def simple_actor(*args):
        yield Theatre.exit(42)

    with Theatre() as theatre:
        result = theatre.run(simple_actor)
        assert result == 42


def test_theatre_run_actor_terminated_with_error():
    def failing_actor(*args):
        yield Theatre.sleep(0)
        raise Exception("Forgot my lines")
        yield Theatre.exit()

    with Theatre() as theatre:
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

    with Theatre(clock_tick=0.01) as theatre:
        result = theatre.run(main_actor)
        assert result == "all_done"

