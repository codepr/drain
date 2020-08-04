import asyncio
from .stream import Stream
from .record import Record


class App:
    def __init__(self):
        self.streams = set()
        self.sinks = set()

    def stream(self, source, concurrency=1, name=u"", record_class=Record):
        stream = Stream(
            source,
            concurrency=concurrency,
            name=name,
            record_class=record_class,
        )
        self.streams.add(stream)
        return stream

    def sink(self, stream):
        def add_sink(func):
            self.sinks.add((func, stream))

        return add_sink

    def run(self):
        asyncio.run(self.start_streams())

    async def start_streams(self):
        await asyncio.gather(
            *(
                [stream.sink() for stream in self.streams]
                + [sink(stream) for sink, stream in self.sinks]
            )
        )
