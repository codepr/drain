import unittest
import dataclasses
from drain.record import Record


@dataclasses.dataclass
class Fake(Record):
    fake_id: int
    name: str
    score: float


class TestRecord(unittest.TestCase):
    def test_loads_dumps_record(self):
        fake = Fake("a1b2", "TestFake", 12.2)
        raw = fake.dumps()
        self.assertEqual(Fake.loads(raw), fake)
