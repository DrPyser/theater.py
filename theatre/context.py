from contextvars import Context, ContextVar
from theatre.interfaces import Address
import logging

_SELF_ADDRESS: ContextVar[Address] = ContextVar("_SELF_ADDRESS")
_PARENT_ADDRESS: ContextVar[Address | None] = ContextVar(
    "_PARENT_ADDRESS", default=None
)
_LOGGER: ContextVar[logging.Logger] = ContextVar("_LOGGER")


def whoami() -> Address:
    return _SELF_ADDRESS.get()


def whoisparent() -> Address | None:
    return _PARENT_ADDRESS.get()


def get_logger() -> logging.Logger:
    return _LOGGER.get()
