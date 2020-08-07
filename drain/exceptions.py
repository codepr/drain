"""
drain.exceptions.py
~~~~~~~~~~~~~~~~~~~

Contains custom errors definitions
"""


class DrainError(Exception):
    """Generic base error """

    ...


class NoObservableSourceError(DrainError):
    """Starting consuming a stream without having set an observable first"""

    ...


class DropRecordError(Exception):
    """A record does not satisfy a given predicate and has to be dropped"""

    ...
