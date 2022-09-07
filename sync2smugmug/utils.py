import asyncio
import logging
import time

logger = logging.getLogger(__name__)


def cmp(a, b):
    return (a > b) - (a < b)


def timeit(func):
    """
    Timeit function that works for both regular functions and async coroutines
    """
    async def process(func, *args, **params):
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **params)
        else:
            return func(*args, **params)

    async def helper(*args, **params):
        start = time.time()
        result = await process(func, *args, **params)

        elapsed = time.time() - start
        if elapsed > 1:
            logger.info(f"!---- '{func.__name__}' execution time: {elapsed:.2f} sec ----!")

        return result

    return helper
