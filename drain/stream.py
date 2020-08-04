from __future__ import annotations
import asyncio
import functools
from .record import Record
from .async_reduce import async_reduce, takewhile
from .types import Source, Processor, Predicate, RecordT
from typing import (
    Optional,
    AsyncGenerator,
    Generic,
    List,
    Type,
)


class NoObservableSource(Exception):
    pass


class Stream(Generic[RecordT]):
    def __init__(
        self,
        observable: Source,
        concurrency: int = 1,
        name: str = "",
        record_class: Type[Record] = Record,
    ):
        self.observable: Source = observable
        self.ops: List[Processor] = []
        # self.results: Optional[asyncio.Queue] = None
        self.name = name
        self.concurrency = concurrency
        self.record_class = record_class

    def __repr__(self) -> str:
        return f"Stream<{self.name}>"

    def __aiter__(self) -> Stream[RecordT]:
        return self

    async def __anext__(self) -> Record:
        return await self.results.get()

    def to_stream(self) -> Stream:
        return Stream(self)

    def pipe(self, *ops: Processor) -> Stream:
        self.ops.extend(ops)
        return self

    def pipe_filter(self, pred: Predicate) -> Stream:
        self.ops.append(functools.partial(takewhile, pred))
        return self

    async def filterby(self, pred: Predicate) -> AsyncGenerator[Record, None]:
        while True:
            data = await self.results.get()
            if pred(data):
                yield data

    async def take(self, size: int) -> AsyncGenerator[List[Record], Record]:
        while True:
            data = [await self.results.get() for _ in range(size)]
            yield data

    async def sink(self, op: Optional[Processor] = None) -> None:
        self.results: asyncio.Queue = asyncio.Queue(
            loop=asyncio.get_running_loop()
        )
        if not self.observable:
            raise NoObservableSource("An observable source must be set")
        if op:
            self.ops.append(op)
        for _ in range(self.concurrency):
            asyncio.create_task(self.process_records())

    async def process_records(self) -> None:
        try:
            async for record in self.observable:
                await self.results.put(
                    await async_reduce(
                        self.ops, self.record_class.loads(record)
                    )
                )
        except asyncio.CancelledError:
            pass
