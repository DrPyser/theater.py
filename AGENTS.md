# AGENTS.md - Theatre.py Agent Guidelines

## Project Overview

Theatre is an actor-model concurrency library for Python using `anyio[trio]`. The project structure:

```
theater.py/
├── pyproject.toml          # Python project configuration
├── flake.nix             # Nix dev shell
├── theatre/
│   ├── theatre.py       # Main actor implementation (~445 lines)
│   ├── lib.py           # Supporting utilities (~228 lines)
│   ├── interfaces.py    # Type definitions
│   ├── threadpool.py    # Thread pool executor
│   └── tests.py         # Unit tests (unittest-based)
```

## Build, Lint, and Test Commands

### Running Tests

```bash
# Run all tests
pytest

# Run a single test function
pytest -k test_function_name

# Run with verbose output
pytest -v

# Run with coverage
pytest --cov=theatre --cov-report=term-missing
```

### Python Execution

```bash
# Enter dev shell (Nix)
nix develop

# Or using uv
uv sync
uv run python -m pytest

# Run a specific module
uv run python -m theatre.tests
```

### Formatting and Linting

```bash
# Format code (uv comes with built-in formatting)
uv format

# Type checking (mypy - if installed)
uv run mypy theatre/

# Run linting
uv run ruff check theatre/
```

### Building

```bash
# Build package with hatch
uv build

# Install in editable mode
uv pip install -e .
```

## Code Style Guidelines

### General Principles

- **Conciseness**: Keep responses short (1-3 sentences unless detail needed)
- **No unnecessary preamble/postamble**: Answer directly without explanations
- **Code style mimicry**: Follow existing patterns in the codebase

### Import Conventions

```python
# Standard library first, then third-party
from types import SimpleNamespace
from typing import Set, Coroutine, Any, Callable, TypeVar
from contextvars import Context, ContextVar, copy_context
from concurrent.futures import Future, Executor, CancelledError
import anyio
```

- Use type hints for all function signatures
- Use `from __future__ import annotations` for forward references
- Group: future -> stdlib -> typing -> third-party -> local

### Type Annotations

```python
# Generic types
RequestType = TypeVar("RequestType")
RequestHandler = Callable[[RequestType], Any]

# Class with generics
class IndexedSet(defaultdict):
    def __init__(self, key: Callable[[T], K], *args, **kwargs): ...

# Dataclasses require explicit field types
@dataclass
class ActorRecord:
    address: Address
    state: ActorState
    links: set[Address]
    script: Callable[[Props], Coroutine]
    props: Props
    mailbox: Mailbox
    context: Context
    lock: anyio.Lock
```

### Naming Conventions

- **Classes**: `CamelCase` (e.g., `ActorRecord`, `Theatre`)
- **Functions/methods**: `snake_case` (e.g., `_initialize_actor`)
- **Private methods**: leading underscore (e.g., `_create_actor`)
- **Constants**: `UPPER_SNAKE_CASE`
- **Type variables**: `T`, `K`, `V` or descriptive `RequestType`

### Dataclass Usage

```python
from dataclasses import dataclass

@dataclass
class ActorState:
    coroutine: Coroutine
    exit_condition: Exit
```

### Coroutine Patterns

```python
# Actor scripts yield requests, receive responses
def script():
    while True:
        message = yield Request.Receive()
        # process message

# State machine transitions
def initialize(self):
    try:
        request = self.coroutine.send(None)
    except StopIteration as ex:
        return self.terminate(Exit(ex.value))
    else:
        return _ActorState.Ready(self.coroutine, request)
```

### Error Handling

```python
# Use custom exceptions
class Exit(Exception):
    def __init__(self, value):
        self.value = value

class AbnormalExit(Exit):
    pass

class CollateralExit(Exit):
    def __init__(self, origin: Address):
        self.origin = origin
```

### Pattern Matching (match/case)

```python
match request:
    case Spawn(script, props):
        new_address = await self.spawn(script, props)
        return new_address
    case _:
        continue
```

### Async/Await

```python
async def handle_request(self, request, record):
    async with record.lock:
        result = await self.handle_request(pending_request, record)
        new_state = await anyio.to_thread.run_sync(
            record.context.run,
            state.handle,
            result
        )
```

### Documentation

- **No comments unless explicitly requested**
- Docstrings only for public APIs
- Keep code self-documenting with descriptive names

### Git Workflow

- Commit only when explicitly requested
- Never amend pushed commits
- Use meaningful commit messages (1-2 sentences)

## Known Issues in Codebase

1. **theatre/theatre.py:4**: Missing imports (`dataclass`, `Optional`, `RequestType`, `T`)
2. **theatre/theatre.py:307**: Typo `ator.state` → `actor.state`
3. **theatre/theatre.py:233**: Missing `Mailbox`, `T`, `K` definitions
4. **theatre/theatre.py:358**: Missing `InvalidStateException`
5. **theatre/lib.py:1,26**: Missing imports in lib.py
6. **flake.nix:21-24**: Commented-out build configuration references non-existent `defaultPackage`

## Development Notes

- Uses anyio with trio backend for async execution
- Custom state machine for actor lifecycle (Fresh → Ready → Active → Suspended → Terminated)
- Mailbox-based message passing with actor links
- ContextVar for context isolation
- Thread pool integration via anyio.to_thread.run_sync