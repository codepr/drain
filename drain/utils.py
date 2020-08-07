"""
drain.utils.py
~~~~~~~~~~~~~~

Contains utility functions
"""
import inspect
from typing import List, Any, Optional, Union, Callable, Awaitable, cast
from .types import Processor, RecordT, Predicate
from .exceptions import DropRecordError


async def async_reduce(
    processors: List[Processor], target: RecordT
) -> Optional[RecordT]:
    """Apply reduction in functools.reduce style to a target object returning
    the result ina `RecordT` object.

    :type processors: List[Processor]
    :param processors: A list of Processor, which can be either a Callable or a
                       Coroutine, see types.py for more info. Each one of these
                       processors are applied to the result of the previous one
                       starting from the target.

    :type target: RecordT
    :param target: A `RecordT` object meant to be manipulated by each processor.

    :rtype: Optional[RecordT]
    :return: A `RecordT` object or None in case of `DropRecordError` exception,
             which can be raised by an unsatisfied predicate `Processor`.

    """
    res = target
    for proc in processors:
        try:
            res = await maybe_async(proc(res))
        except DropRecordError:
            return None
    return res


def takewhile(pred: Predicate, target: RecordT) -> RecordT:
    """Test a target against a predicate, if it pass the test return it,
    otherwise raise a `DropRecordError` exception.

    :type pred: Predicate
    :param pred: A `Predicate` test to be applied to the target, see types.py
                 for more info.

    :type target: RecordT
    :param target: A `RecordT` object mean to be tested against the predicate.

    :rtype: RecordT
    :return: A `RecordT` only if it passes the predicate test.

    :raises: DropRecordError, in case of failure on predicate test.
    """
    if pred(target):
        return target
    raise DropRecordError()


async def maybe_async(fn: Union[Callable[[Any], Any], Awaitable[Any]]) -> Any:
    """
    Just an auxiliary function, accept either a normal callable function or an
    awaitable, acting accordingly based on the type, awaiting in case of an
    awaitable o just returning it if a callable.

    :type fn: Union[Callable[[Any], Any], Awaitable[Any]]
    :param fn: A function, be it an awaitable or a callable.
    """
    if inspect.isawaitable(fn):
        fn = cast(Awaitable[Any], fn)
        return await fn
    return fn
