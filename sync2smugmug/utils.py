import logging
import os
import time

logger = logging.getLogger(__name__)


def cmp(a, b):
    if hasattr(a, 'compare'):
        return a.compare(b)
    elif hasattr(b, 'compare'):
        return -b.compare(a)
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
