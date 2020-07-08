import unittest
import streamario.source as source


class FakeStream:
    def __init__(self, size=0):
        self.size = size
        self.pos = 0
        self.data = bytearray(size)

    def read(self, size=None):
        if not size or size > self.size:
            self.pos = self.size
            yield self.data
        else:
            self.pos = size
            yield self.data[:size]
        self.data = self.data[self.pos :]


class TestSource(unittest.TestCase):
    def test_get(self):
        src = source.Source(FakeStream(20))
        data = src.get(15)
        self.assertTrue(len(list(data)), 15)


if __name__ == "__main__":
    unittest.main()
