"""
drain.record.py
~~~~~~~~~~~~~~~

Contains simple implementation and utilities for records to be consumed by
from the stream structure
"""
import json
from dataclasses import dataclass


@dataclass
class Record:
    """Simple record base class, only defines serialization methods"""

    def dumps(self):
        return json.dumps(self.__dict__)

    @classmethod
    def loads(cls, raw):
        return cls(**json.loads(raw))
