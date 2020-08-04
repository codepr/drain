import json
from dataclasses import dataclass


@dataclass
class Record:
    def dumps(self):
        return json.dumps(self.__dict__)

    @classmethod
    def loads(cls, raw):
        return cls(**json.loads(raw))
