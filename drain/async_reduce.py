import inspect


class DropRecord(Exception):
    pass


async def async_reduce(coros, target):
    res = target
    for coro in coros:
        try:
            res = await maybe_async(coro(res))
        except DropRecord:
            return None
    return res


def takewhile(pred, target):
    if pred(target):
        return target
    raise DropRecord()


async def maybe_async(obj):
    if inspect.isawaitable(obj):
        return await obj
    return obj
