import asyncio
import unittest
import dataclasses
from drain.stream import Stream
from drain.record import Record


@dataclasses.dataclass
class TestRecord(Record):
    value: int


async def source():
    for n in range(10):
        yield TestRecord(n).dumps()
    yield TestRecord(100).dumps()


async def consumer(stream):
    results = []
    async for record in stream:
        if record.value >= 100:
            break
        results.append(record.value)
    return results


async def consumer_take(stream, n):
    async for record in stream.take(n):
        return [r.value for r in record]


async def consumer_filterby(stream, pred):
    results = []
    async for record in stream.filterby(pred):
        if record.value >= 100:
            break
        results.append(record.value)
    return results


async def run_tasks(tasks):
    return await asyncio.gather(*tasks)


class TestStream(unittest.TestCase):
    def test_pipe_stream(self):
        stream = Stream(source(), record_class=TestRecord).pipe(
            lambda x: TestRecord(x.value + 1)
        )
        self.assertEqual(len(stream.ops), 1)
        res = asyncio.run(run_tasks([stream.sink(), consumer(stream)]))[1]
        self.assertEqual(res, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    def test_pipe_filter_stream(self):
        stream = Stream(source(), record_class=TestRecord).pipe_filter(
            lambda x: x.value > 5
        )
        self.assertEqual(len(stream.ops), 1)
        res = asyncio.run(run_tasks([stream.sink(), consumer(stream)]))[1]
        self.assertEqual(res, [6, 7, 8, 9])

    def test_take_stream(self):
        stream = Stream(source(), record_class=TestRecord)
        res = asyncio.run(
            run_tasks([stream.sink(), consumer_take(stream, 3)])
        )[1]
        self.assertEqual(res, [0, 1, 2])

    def test_filterby_stream(self):
        stream = Stream(source(), record_class=TestRecord)
        res = asyncio.run(
            run_tasks(
                [
                    stream.sink(),
                    consumer_filterby(stream, lambda x: x.value % 2 == 0),
                ]
            )
        )[1]
        self.assertEqual(res, [0, 2, 4, 6, 8])
