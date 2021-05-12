import logging
import os
import time
from multiprocessing.pool import Pool
from typing import Optional

logger = logging.getLogger(__name__)

_task_pool: Optional[Pool] = None


def get_task_pool() -> Pool:
    global _task_pool

    if _task_pool is None:
        _task_pool = Pool(10)

    return _task_pool


def cmp(a, b):
    if hasattr(a, 'compare') and hasattr(b, 'compare'):
        return a.compare(b)
    else:
        return (a > b) - (a < b)


def timeit(f):
    def timed(*args, **kwargs):
        start = time.time()
        try:
            return f(*args, **kwargs)
        finally:
            elapsed = time.time() - start
            if elapsed > 1:
                logger.info(f'!---- {f.__name__} execution time: {elapsed:.2f} sec ----!')

    return timed


def scan_tree(path: str):
    """
    Recursively yield DirEntry objects for given directory.
    """
    for entry in os.scandir(path):
        entry: os.DirEntry

        yield entry

        if entry.is_dir(follow_symlinks=False):
            yield from scan_tree(entry.path)  # see below for Python 2.x
