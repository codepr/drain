import asyncio
import unittest
import drain.stream as stream


async def source():
    for n in range(10):
        yield n


class TestStream(unittest.TestCase):
    def test_pipe(self):
        src = stream.Stream(source())
        res = src.pipe(lambda x: x + 1)
        asyncio.run(res.sink())
        self.assertEqual([7.0], res)
