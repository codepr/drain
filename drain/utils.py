import inspect
from .exceptions import DropRecordError


async def async_reduce(coros, target):
    res = target
    for coro in coros:
        try:
            res = await maybe_async(coro(res))
        except DropRecordError:
            return None
    return res


def takewhile(pred, target):
    if pred(target):
        return target
    raise DropRecordError()


async def maybe_async(obj):
    if inspect.isawaitable(obj):
        return await obj
    return obj
