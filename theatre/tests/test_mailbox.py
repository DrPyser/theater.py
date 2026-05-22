import pytest
from theatre.threaded_theatre import _NoMatch, MailboxFull, Mailbox


def test_mailbox_basic_append_pop():
    m = Mailbox(maxlen=3)
    m.append("a")
    m.append("b")
    assert m.pop_matching() == "a"
    assert m.pop_matching() == "b"


def test_mailbox_pop_matching_empty_raises():
    m = Mailbox(maxlen=3)
    with pytest.raises(_NoMatch):
        m.pop_matching()


def test_mailbox_pop_matching_with_filter():
    m = Mailbox(maxlen=5)
    m.append("ping")
    m.append("pong")
    m.append("ping")
    assert m.pop_matching(filter_fn=lambda m: m == "pong") == "pong"
    assert list(m) == ["ping", "ping"]


def test_mailbox_pop_matching_filter_no_match_raises():
    m = Mailbox(maxlen=3)
    m.append("ping")
    with pytest.raises(_NoMatch):
        m.pop_matching(filter_fn=lambda m: m == "pong")
    assert len(m) == 1
    assert m.pop_matching() == "ping"


def test_mailbox_full_raises():
    m = Mailbox(maxlen=2)
    m.append("a")
    m.append("b")
    with pytest.raises(MailboxFull):
        m.append("c")
    assert len(m) == 2


def test_mailbox_none_is_valid_message():
    m = Mailbox(maxlen=3)
    m.append(None)
    assert m.pop_matching() is None


def test_mailbox_len_bool_iter():
    m = Mailbox(maxlen=3)
    assert len(m) == 0
    assert not m
    m.append("x")
    assert len(m) == 1
    assert m
    assert list(m) == ["x"]
