#!/usr/bin/env python
#
# Drain usage example, a simple HTTP log aggregation stats monitor.
#
# Monitor an ever-growing HTTP log in Common Log Format by tailing it (let's a
# tail subprocess handle all the hassles like rename, re-creation during
# log-rotation etc.) and aggregating some stats on a time-basis.
#
# LogStats class uses an event loop to schedule some callbacks that just calculate
# and print some stats like top hits by section or check for the log flow size
# printing an alert based on a threshold.

import os
import sys
import asyncio
import subprocess
from datetime import datetime as dt
from statistics import mean
from collections import defaultdict
from dataclasses import dataclass
import drain


# Path of the log to watch
LOG_PATH = "../logan/access.log"


@dataclass
class LogRecord(drain.Record):
    """
    Define a simple log record dataclass that apply some simple string
    manipulation based on Common Log Format
    """

    row: str

    @property
    def section(self) -> str:
        tokens = self.row.split(" ")
        try:
            section = "/".join([tokens[0], tokens[6].split("/")[1]])
        except IndexError:
            section = ""
        return section


# Create a simple async generator based source
async def source(path):
    """
    Async generator data source, subprocess a tail command on a POSIX shell
    yielding rows as they're read.
    """

    log = subprocess.Popen(
        ["tail", "-f", path],
        encoding="utf-8",
        errors="ignore",
        universal_newlines=True,
        bufsize=1,
        stdout=subprocess.PIPE,
    ).stdout
    try:
        while True:
            line = log.readline()
            yield LogRecord(line).dumps()
    except KeyboardInterrupt:
        pass
    finally:
        log.close()


# Create a streaming app
app = drain.App()
log_stream = app.stream(
    source(LOG_PATH), record_class=LogRecord, concurrency=10, name="test"
)


class LogStats:

    """
    Just a simple stats aggregator for the log stream, compute some basic
    calculations and print results on a time-basis using an asyncio event-loop
    to schedule callbacks.
    """

    class Window:
        """Deadsimple window-list inner class, trim size automatically on insert"""

        def __init__(self, size):
            self.size = size
            self.items = []

        def put(self, item):
            if len(self.items) == self.size:
                self.items.pop(0)
            self.items.append(item)

        def mean(self):
            return mean(self.items)

    def __init__(
        self,
        window_size=120,
        alert_threshold=30,
        stats_every=10,
        mean_hits_every=1,
    ):
        self.window = self.Window(window_size)
        self.alert = False
        self.alert_threshold = alert_threshold
        self.hits = defaultdict(int)
        self.hits_per_second = 0
        self.loop = asyncio.get_running_loop()
        self.stats_every = stats_every
        self.mean_hits_every = mean_hits_every
        self.loop.call_later(stats_every, self.print_hits)
        self.loop.call_later(mean_hits_every, self.mean_hits)

    def hit(self, record):
        self.hits[record.section] += 1
        self.hits_per_second += 1

    def print_hits(self):
        max_hits = max(self.hits, key=self.hits.get)
        min_hits = min(self.hits, key=self.hits.get)
        print(f"Max hits by section: {max_hits} {self.hits[max_hits]}",)
        print(f"Min hits by section: {min_hits} {self.hits[min_hits]}",)
        self.loop.call_later(self.stats_every, self.print_hits)

    def mean_hits(self):
        self.window.put(self.hits_per_second)
        self.hits_per_second = 0
        avg = self.window.mean()
        if not self.alert and avg > self.alert_threshold:
            self.alert = True
            print(
                f"High traffic generated an alert - hits={avg}, "
                f" triggered at {dt.now().strftime('%d/%m/%Y %H:%M:%S')}",
            )
        elif self.alert and avg < self.alert_threshold:
            self.alert = False
            print(
                f"High traffic alert recovered - hits={avg}, "
                f" recovered at {dt.now().strftime('%d/%m/%Y %H:%M:%S')}",
            )
        self.loop.call_later(self.mean_hits_every, self.mean_hits)


@app.sink(log_stream)
async def log_consumer(stream):
    print(f"Watching file: {LOG_PATH}")
    log_stats = LogStats()
    async for record in stream:
        log_stats.hit(record)


if __name__ == "__main__":
    # Check file exists
    if not os.path.isfile(LOG_PATH):
        print(f"Cannot open file {LOG_PATH}: No such file or directory")
        sys.exit(0)
    try:
        app.run()
    except KeyboardInterrupt:
        pass
