"""
drain.app.py
~~~~~~~~~~~~

Main entry point for creating a streaming application and running asynchronous
streams.
"""
import asyncio
from typing import Any, Set, Callable, Awaitable, Tuple, cast, Coroutine
from .types import RecordT, Source
from .stream import Stream


class App:
    """Main controller class, create a streaming application and use it to
    create streams to be consumed, registering sinks to subscribe and consume
    them"""

    def __init__(self):
        self.streams: Set[Stream[RecordT]] = set()
        self.sinks: Set[
            Tuple[Callable[[Stream[RecordT]], Awaitable[Any]], Stream[RecordT]]
        ] = set()

    def stream(
        self,
        source: Source,
        record_class: RecordT,
        concurrency: int = 1,
        name: str = u"",
    ) -> Stream[RecordT]:
        """
        Create and register a new `Stream[RecordT]` datastructure, ready to be
        consumed.

        :type source: Source
        :param source: The source of the records, have to satisfy
                           `AsyncGenerator` methods

        :type record_class: RecordT
        :param record_class: A class factory to build records once received, have
                             to be a subclass of `Record` class

        :type concurrency: int
        :param concurrency: The concurrency level, define the number of workers to
                            concurrently consume the stream

        :type name: str
        :param name: Stream name, just an identifier

        :rtype: Stream[RecordT]
        :return: A `Stream[RecordT]` instance
        """
        stream = Stream(
            source,
            concurrency=concurrency,
            name=name,
            record_class=record_class,
        )
        self.streams.add(stream)
        return stream

    def sink(
        self, stream: Stream[RecordT]
    ) -> Callable[[Callable[[Stream[RecordT]], Awaitable[Any]]], None]:
        """
        Decorator, register a consumer function, which should be an `async def`
        func, of type Callable[[RecordT], Awaitable[Any]].

        :type stream: Stream[RecordT]
        :param stream: A stream to subscribe the consumer to.

        :rtype: Callable[[Callable[[RecordT], Awaitable[Any]]], None]
        :return: A function to register the consumer and the associated stream.
        """

        def add_sink(func):
            self.sinks.add((func, stream))

        return add_sink

    def run(self) -> None:
        """Start all the streams and consumers in an asyncio loop."""
        asyncio.run(self.start_streams())

    async def start_streams(self) -> None:
        """Gather all the registered streams and consumers (sinks)."""
        await asyncio.gather(
            *(
                [stream.sink() for stream in self.streams]
                + [
                    cast(Coroutine[Any, Any, Any], sink(stream))
                    for sink, stream in self.sinks
                ]
            )
        )
