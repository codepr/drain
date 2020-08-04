from .record import Record
from typing import (
    AsyncIterable,
    Callable,
    Coroutine,
    Union,
    TypeVar,
)

RecordT = TypeVar("RecordT")

Source = AsyncIterable[Record]
Processor = Union[Callable[[Record], Record], Coroutine[Record, None, Record]]
Predicate = Callable[[Record], bool]
