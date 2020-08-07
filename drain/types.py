"""
drain.types.py
~~~~~~~~~~~~~~

Contains custom types definitions and utilities
"""
from .record import Record
from typing import (
    Awaitable,
    AsyncIterable,
    Callable,
    Union,
    TypeVar,
)

RecordT = TypeVar("RecordT", bound=Record)

Source = AsyncIterable[RecordT]
Processor = Union[
    Callable[[RecordT], RecordT], Callable[[RecordT], Awaitable[RecordT]]
]
Predicate = Callable[[RecordT], bool]
