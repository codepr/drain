from .record import Record
from typing import (
    AsyncIterable,
    Callable,
    Coroutine,
    Union,
    TypeVar,
)

RecordT = TypeVar("RecordT", bound=Record)

Source = AsyncIterable[RecordT]
Processor = Union[
    Callable[[RecordT], RecordT], Coroutine[RecordT, None, RecordT]
]
Predicate = Callable[[RecordT], bool]
