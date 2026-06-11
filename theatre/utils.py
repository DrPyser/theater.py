from collections.abc import Sequence
import logging


def starts_with(*args):
    return lambda x: (
        isinstance(x, Sequence) and len(args) <= len(x) and args == x[: len(args)]
    )


def log_adapter(record_adapter):
    class _LoggingAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return record_adapter(self.extra, msg, kwargs)

    return _LoggingAdapter

class _ContextFilter(logging.Filter):
    def __init__(self, context):
        super().__init__()
        self.context = context

    def filter(self, record):
        if self.context:
            attrs = " ".join(f"{key}={value}" for key, value in self.context.items())
            record.msg = f"{record.msg} [{attrs}]"
        return True


def log_bind(logger, **context):
    filter = _ContextFilter(context)
    logger.addFilter(filter)
    return logger


@log_adapter
def contextual_adapter(ctx: dict, msg: str, kwargs: dict):
    context = ctx | kwargs.pop("context", {})
    attrs = " ".join(f"{key}={value}" for key, value in context.items())
    return f"{msg} [{attrs}]", kwargs
