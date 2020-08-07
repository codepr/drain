Drain
=====

A sewer of streams.

**Goals**

- Fun project, PoC on streaming patterns
- Learn more about python typing module and infinite streams.
- Testing on asyncio concurrency model

**Non-Goals**

- Complete the development
- Grow it out of control (Second-system effect)

## Quickstart

A simple example of a stream with 3 consumers

```python

import random
import asyncio
from dataclasses import dataclass
import drain


# Define a simple record
@dataclass
class IntRecord(drain.Record):
    value: int


# Some simple manipulations (processors)
async def divide_by_two(x):
    return IntRecord(x.value // 2)


async def random_timeout(x):
    await asyncio.sleep(random.randint(0, 3))
    return x


# Create a simple async generator based source
async def source():
    while True:
        yield IntRecord(random.randint(1, 10000)).dumps()


# Create a streaming app
app = drain.App()
int_stream = (
    app.stream(source(), record_class=IntRecord, concurrency=10, name="test")
    .pipe(lambda x: IntRecord(x.value ** 2))
    .pipe(divide_by_two)
    .pipe(random_timeout)
    .pipe_filter(lambda x: x.value > 500000)  # Add a filter here
)

@app.sink(int_stream)
async def strange_consumer(stream):
    async for record in stream.take(10):
        print("strange -> ", record)


@app.sink(int_stream)
async def charm_consumer(stream):
    async for record in stream.filterby(lambda x: x.value > 125):
        print("charm -> ", record)


@app.sink(int_stream)
async def bottom_consumer(s):
    async for record in s:
        print("bottom -> ", record)


# Start the app
if __name__ == "__main__":
    try:
        app.run()
    except KeyboardInterrupt:
        pass
```

## License

WTFPL
