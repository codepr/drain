import asyncio
import unittest
import dataclasses
from drain.stream import Stream
from drain.record import Record


@dataclasses.dataclass
class TestRecord(Record):
    value: int

    def __hash__(self):
        return self.value


async def source():
    for n in range(10):
        yield TestRecord(n).dumps()
    yield TestRecord(100).dumps()


async def sourceB():
    for n in range(10, 20):
        yield TestRecord(n).dumps()
    yield TestRecord(100).dumps()


async def source_repeated():
    for n in [0, 1, 1, 1, 1, 2, 3, 4, 5, 5, 5, 5, 1, 2, 4]:
        yield TestRecord(n).dumps()
    yield TestRecord(100).dumps()


async def consumer(stream):
    results = []
    async for record in stream:
        if record.value >= 100:
            break
        results.append(record.value)
    return results


async def consumer_merge(streamA, streamB):
    results = []
    async for record in streamA.merge(streamB):
        if len(results) == 21:
            break
        results.append(record.value)
    return results


async def consumer_distinct(stream):
    results = []
    async for record in stream.distinct():
        if len(results) == 6:
            break
        results.append(record.value)
    return results


async def consumer_take(stream, n):
    async for record in stream.take(n):
        return [r.value for r in record]


async def consumer_enumerate(stream):
    results = []
    async for i, record in stream.enumerate():
        if record.value >= 100:
            break
        results.append((i, record.value))
    return results


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

    def test_enumerate_stream(self):
        stream = Stream(source(), record_class=TestRecord)
        res = asyncio.run(
            run_tasks([stream.sink(), consumer_enumerate(stream)])
        )[1]
        self.assertEqual(
            res,
            [
                (0, 0),
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5),
                (6, 6),
                (7, 7),
                (8, 8),
                (9, 9),
            ],
        )

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

    def test_merge_stream(self):
        streamA = Stream(source(), record_class=TestRecord)
        streamB = Stream(sourceB(), record_class=TestRecord)
        res = asyncio.run(
            run_tasks(
                [
                    streamA.sink(),
                    streamB.sink(),
                    consumer_merge(streamA, streamB),
                ]
            )
        )[2]
        self.assertEqual(len(res), 21)
        self.assertEqual(
            set(res),
            {
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                100,
                100,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
            },
        )

    def test_distinct_stream(self):
        stream = Stream(source_repeated(), record_class=TestRecord)
        res = asyncio.run(
            run_tasks([stream.sink(), consumer_distinct(stream)])
        )[1]
        self.assertEqual(len(res), 6)
        self.assertEqual(set(res), {0, 1, 2, 3, 4, 5})
