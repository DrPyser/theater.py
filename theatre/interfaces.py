from collections.abc import (
    Callable,
    Coroutine,
    Generator,
    Hashable,
    Iterable,
)
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import (
    Any,
    Generic,
    Protocol,
    TypeVar,
)


class Address(Hashable, Protocol):
    pass


AddressingScheme = Callable[..., Address]

MessageT = TypeVar("MessageT")


class Mailbox(Iterable, Protocol):
    def append(self, msg: Any) -> None: ...

    def pop_matching(self, filter_fn: Callable[[Any], bool] | None = None) -> Any: ...


PropsT = TypeVar("PropsT", bound=tuple)
Script = Callable[[PropsT], Coroutine]
SignalT = TypeVar("SignalT")
T = TypeVar("T")
RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")


class Exit(Exception):
    pass


class System:
    @dataclass
    class call:
        fn: Callable
        args: tuple = ()
        kwargs: dict = field(default_factory=dict)

    @dataclass
    class exit:
        value: Any = None

    @dataclass
    class whoami:
        pass

    @dataclass
    class sleep:
        duration: float

    @dataclass
    class send(Generic[T]):
        address: Address
        message: T

    @dataclass
    class receive(Generic[T]):
        filter: Callable[[T], bool] | None = None
        timeout: float | None = None

    @dataclass
    class select(Generic[T]):
        predicates: list[Callable[[T], bool]]

    @dataclass
    class spawn:
        script: Script
        props: tuple = ()

    @dataclass
    class link:
        target: Address

    @dataclass
    class spawn_link:
        script: Script
        props: tuple = ()

    @dataclass
    class kill:
        address: Address
        reason: object | None = None


SystemT = TypeVar("SystemT")
Actor = Generator[SystemT, ResponseT, None]


class Theater(Protocol):
    async def spawn(self, script: Script[PropsT], props: PropsT) -> Address: ...

    async def link(self, from_addr: Address, to_addr: Address): ...

    async def spawn_link(self, script: Script[PropsT], props: PropsT) -> Address: ...

    async def send(self, address: Address, message: T): ...

    async def kill(self, address: Address, signal: SignalT): ...

    def run(self): ...


class ActorContext:
    addr: ContextVar[Address] = ContextVar("addr")
    parent_addr: ContextVar[Address] = ContextVar("parent_addr")
