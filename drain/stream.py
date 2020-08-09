"""
drain.stream.py
~~~~~~~~~~~~~~~

Stream datastructure definition and utility methods
"""

from __future__ import annotations
import uuid
import asyncio
import functools
from .utils import async_reduce, takewhile
from .exceptions import NoObservableSourceError
from .types import Source, Processor, Predicate, RecordT
from typing import (
    Type,
    Tuple,
    Optional,
    AsyncGenerator,
    Generic,
    List,
)


class Stream(Generic[RecordT]):

    """Stream[RecordT] structure, an infinite flow of records that offer some
    simple manipulation methods, consumable by multiple async subscribers.
    Concurrency supported and achieved through the asyncio module.

    :type source: Source
    :param source: The source of the records, have to satisfy
                       `AsyncGenerator` methods

    :type record_class: Type[RecordT]
    :param record_class: A class factory to build records once received, have
                         to be a subclass of `Record` class

    :type concurrency: int
    :param concurrency: The concurrency level, define the number of workers to
                        concurrently consume the stream

    :type name: str
    :param name: Stream name, just an identifier
    """

    def __init__(
        self,
        source: Source,
        record_class: Type[RecordT],
        concurrency: int = 1,
        name: str = "",
    ):
        self.source = source
        # A list of manipulations to apply just before the consumption of each
        # new record.
        # Can be a synchronous Callable[[RecordT], RecordT] or an awaitable
        # Coroutine[RecordT, None, RecordT], see types.py
        self.ops: List[Processor] = []
        self.name = name or str(uuid.uuid4())
        self.concurrency = concurrency
        self.record_class = record_class

    def __repr__(self) -> str:
        return f"Stream<{self.name}>"

    def __aiter__(self) -> Stream[RecordT]:
        """
        Async iterator magic method, create nr. of workers based on
        `concurrency` param.

        :raises: NoObservableSourceError, in case of no `source` specified
        """
        if not self.source:
            raise NoObservableSourceError("An observable source must be set")
        if not self.start_event.is_set():
            self.start_event.set()
        return self

    async def __anext__(self) -> RecordT:
        try:
            record = await self.new_records.get()
        except asyncio.CancelledError:
            raise StopAsyncIteration()
        self.new_records.task_done()
        return record

    def pipe(self, *ops: Processor) -> Stream[RecordT]:
        """Add multiple manipulations to apply to each new record before the
        consumption.

        :type ops: Processor
        :param ops: One or more processor functions or coroutine to be applied
                    in a reduction to each new record before consumption
        """
        self.ops.extend(ops)
        return self

    def pipe_filter(self, pred: Predicate) -> Stream[RecordT]:
        """Add a filter function to be applied to each new record before the
        consumption. If the predicate is satisfied the record will be accepted.

        :type pred: Predicate
        :param pred: A (RecordT) -> bool predicate
        """
        self.ops.append(functools.partial(takewhile, pred))
        return self

    def merge(self, *streams: Stream[RecordT]) -> StreamMerge:
        """Merge multiple streams in a single unified stream."""
        return StreamMerge(self, *streams)

    async def filterby(self, pred: Predicate) -> AsyncGenerator[RecordT, None]:
        """Consume the stream by applying a predicate to each new record. Can be
        assumed as `pipe_filter` but in place at each new record.

        :type pred: Predicate
        :param pred: A (RecordT) -> bool predicate
        """
        if not self.start_event.is_set():
            self.start_event.set()
        while True:
            data = await self.new_records.get()
            self.new_records.task_done()
            if pred(data):
                yield data

    async def take(
        self, size: int, timeout: Optional[int] = None
    ) -> AsyncGenerator[List[RecordT], None]:
        """Consume the stream in chunks of defined size, yielding a list only
        after the desired size of records has been reached. A timeout can be
        set to wait for in case there's not enough new records to reach the
        size.

        :type size: int
        :param size: The size of the chunk to retrieve from the stream

        :type timeout: Optional[int]
        :param timeout: The number of seconds to wait for completing a
                        chunk of records
        """
        within_seconds = timeout / size if timeout else None

        async def _read_record(q):
            try:
                record = await asyncio.wait_for(
                    q.get(), timeout=within_seconds
                )
            except asyncio.TimeoutError:
                return None
            else:
                q.task_done()
                return record

        if not self.start_event.is_set():
            self.start_event.set()
        while True:
            records = [
                await _read_record(self.new_records) for _ in range(size)
            ]
            yield [record for record in records if record is not None]

    async def enumerate(self) -> AsyncGenerator[Tuple[int, RecordT], None]:
        """Consume the stream enumerating the records."""
        counter = 0
        if not self.start_event.is_set():
            self.start_event.set()
        while True:
            yield counter, await self.new_records.get()
            self.new_records.task_done()
            counter += 1

    async def sink(self, op: Optional[Processor] = None) -> None:
        """Start processing records and queue them into an asyncio queue.
        Accept an optional last processor to apply to each record.

        :type op: Optional[Processor]
        :param op: Last processor to apply to the chain of manipulations.
        """
        if not self.source:
            raise NoObservableSourceError("An observable source must be set")
        self.start_event: asyncio.Event = asyncio.Event()
        self.new_records: asyncio.Queue = asyncio.Queue()
        if op:
            self.ops.append(op)
        for _ in range(self.concurrency):
            asyncio.create_task(self.process_records())

    async def process_records(self) -> None:
        """Process each new record, applying a reduction with all the specified
        manipulations and putting it into the record queue after, ready to be
        consumed by consumers."""
        try:
            await self.start_event.wait()
            async for record in self.source:
                res = await async_reduce(
                    self.ops, self.record_class.loads(record)
                )
                if res:
                    await self.new_records.put(res)
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass


class StreamMerge:
    """Merge multiple streams into a single unified one

    :type streams: Stream[RecordT]
    :param streams: Vararg of `Stream[RecordT]` type, the streams to be merged
    """

    def __init__(self, *streams: Stream[RecordT]):
        self.streams = [self.consume_stream(stream) for stream in streams]
        self.records: asyncio.Queue = asyncio.Queue()

    def __aiter__(self):
        """Async iterator magic method, create a nr. of workers based on the
        `concurrency` param number.
        """
        loop = asyncio.get_running_loop()
        for stream in self.streams:
            loop.create_task(stream)
        return self

    async def __anext__(self) -> RecordT:
        try:
            record = await self.records.get()
        except asyncio.CancelledError:
            raise StopAsyncIteration()
        self.records.task_done()
        return record

    async def consume_stream(self, stream: Stream[RecordT]) -> None:
        """Start consuming a stream and putting records into an `asyncio.Queue`
        instance.

        :type stream: Stream[RecordT]
        :param stream: A `Stream[RecordT]` object to be consumed and piped
                       into an `asyncio.Queue`.
        """
        try:
            async for record in stream:
                await self.records.put(record)
        except asyncio.CancelledError:
            pass
