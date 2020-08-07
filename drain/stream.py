"""
drain.stream.py
~~~~~~~~~~~~~~~

Stream datastructure definition and utility methods
"""

from __future__ import annotations
import asyncio
import functools
from .utils import async_reduce, takewhile
from .exceptions import NoObservableSourceError
from .types import Source, Processor, Predicate, RecordT
from typing import (
    Tuple,
    Optional,
    AsyncGenerator,
    Generic,
    List,
)


class Stream(Generic[RecordT]):
    def __init__(
        self,
        observable: Source,
        record_class: RecordT,
        concurrency: int = 1,
        name: str = "",
    ):
        self.observable: Source = observable
        self.ops: List[Processor] = []
        self.name = name
        self.concurrency = concurrency
        self.record_class = record_class

    def __repr__(self) -> str:
        return f"Stream<{self.name}>"

    def __aiter__(self) -> Stream[RecordT]:
        return self

    async def __anext__(self) -> RecordT:
        return await self.results.get()

    def pipe(self, *ops: Processor) -> Stream[RecordT]:
        self.ops.extend(ops)
        return self

    def pipe_filter(self, pred: Predicate) -> Stream[RecordT]:
        self.ops.append(functools.partial(takewhile, pred))
        return self

    async def filterby(self, pred: Predicate) -> AsyncGenerator[RecordT, None]:
        while True:
            data = await self.results.get()
            if pred(data):
                yield data

    async def take(self, size: int) -> AsyncGenerator[List[RecordT], RecordT]:
        while True:
            data = [await self.results.get() for _ in range(size)]
            yield data

    async def enumerate(self) -> AsyncGenerator[Tuple[int, RecordT], None]:
        counter = 0
        while True:
            yield counter, await self.results.get()
            counter += 1

    async def sink(self, op: Optional[Processor] = None) -> None:
        self.results: asyncio.Queue = asyncio.Queue()
        if not self.observable:
            raise NoObservableSourceError("An observable source must be set")
        if op:
            self.ops.append(op)
        for _ in range(self.concurrency):
            asyncio.create_task(self.process_records())

    async def process_records(self) -> None:
        try:
            async for record in self.observable:
                res = await async_reduce(
                    self.ops, self.record_class.loads(record)
                )
                if res:
                    await self.results.put(res)
        except asyncio.CancelledError:
            pass
