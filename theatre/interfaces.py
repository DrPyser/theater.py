from typing import (
    Protocol,
    TypeVar,
    Hashable,
    Callable,
    Generic,
    Coroutine,
    Generator,
    Any,
)
from dataclasses import dataclass
from contextvars import Context, ContextVar


class Address(Hashable, Protocol):
    pass


AddressingScheme = Callable[..., Address]

MessageT = TypeVar("MessageT")


class Queue(Protocol[MessageT]):
    def put(self, message: MessageT) -> None: ...
    def get(self) -> MessageT: ...


Inbox = Queue
Outbox = Queue
Mailbox = Queue

PropsT = TypeVar("PropsT")
Script = Callable[[PropsT], Coroutine]
SignalT = TypeVar("SignalT")
T = TypeVar("T")
RequestT = TypeVar("RequestT")
ResponseT = TypeVar("ResponseT")


class Exit(Exception):
    pass


@dataclass
class send(Generic[T]):
    address: Address
    message: T


@dataclass
class receive(Generic[T]):
    filter: Callable[[T], bool] | None = None


@dataclass
class select(Generic[T]):
    predicates: list[Callable[[T], bool]]


@dataclass
class spawn(Generic[PropsT]):
    script: Script
    props: PropsT = ()


@dataclass
class spawn_link(Generic[PropsT]):
    script: Script
    props: PropsT


@dataclass
class kill:
    address: Address
    reason: object | None = None


SystemCall = send[Any] | receive | select[Any] | spawn[Any] | spawn_link[Any] | kill

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


@dataclass
class ActorSheet(Generic[PropsT]):
    address: Address
    script: Script[PropsT]
    play: Coroutine
    props: PropsT
    mailbox: Inbox
    context: Context
