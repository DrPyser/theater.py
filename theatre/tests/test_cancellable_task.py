import threading
import pytest
from concurrent.futures import Future
from theatre.threaded_theatre import CancellableTask


def test_cancellable_task_delegates_done():
    fut: Future = Future()
    task = CancellableTask(fut)
    assert not task.done()
    fut.set_result(None)
    assert task.done()


def test_cancellable_task_delegates_result():
    fut: Future = Future()
    task = CancellableTask(fut)
    fut.set_result(42)
    assert task.result() == 42


def test_cancellable_task_delegates_exception():
    fut: Future = Future()
    task = CancellableTask(fut)
    exc = ValueError("test")
    fut.set_exception(exc)
    assert task.exception() is exc
    with pytest.raises(ValueError, match="test"):
        task.result()


def test_cancellable_task_delegates_cancelled():
    fut: Future = Future()
    task = CancellableTask(fut)
    fut.cancel()
    assert task.cancelled()


def test_cancellable_task_cancel_no_interrupt():
    fut: Future = Future()
    task = CancellableTask(fut)
    assert task.cancel()
    assert fut.cancelled()
    assert task.done()


def test_cancellable_task_cancel_with_interrupt():
    fut: Future = Future()
    event = threading.Event()
    task = CancellableTask(fut, interrupt=event)
    assert not event.is_set()
    assert task.cancel()
    assert event.is_set()
    assert fut.cancelled()


def test_cancellable_task_future_property():
    fut: Future = Future()
    task = CancellableTask(fut)
    assert task.future is fut
