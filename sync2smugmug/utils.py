import logging
from multiprocessing.pool import Pool
import threading
import time
from typing import List

DEFAULT_TIMEOUT = 30
PROCESSES = 10

logger = logging.getLogger(__name__)
_pool = None


def cmp(a, b):
    if hasattr(a, 'compare') and hasattr(b, 'compare'):
        return a.compare(b)
    else:
        return (a > b) - (a < b)


def the_pool():
    global _pool
    if _pool is None:
        _pool = Pool(processes=PROCESSES)

    return _pool


def wait_for_all_tasks():
    global _pool
    if _pool is None:
        return

    _pool.close()
    _pool.join()


class TaskPool:
    """
    Represents a set of tasks that can be executed concurrently, but need to be monitored (for completion) together.

    A classic example is downloading images for an album. We want to do this as concurrently as possible, but only
    mark the albums meta-data (on disk) when download is fully finished
    """

    def __init__(self, all_done_callback=None):
        self._done: List[bool] = []
        self._errors: List[Exception] = []
        self._all_done_callback = all_done_callback
        self._cond = threading.Condition(threading.Lock())

    def apply_async(self, func, params=(), callback=None):
        with self._cond:
            # Wrap the callback (if provided) with our own for tracking purposes
            idx = len(self._done)
            self._done.append(False)

            def error_callback(err):
                self.mark_done(idx)
                self._errors.append(err)
                logger.exception(err)

            def tracking_callback(value):
                self.mark_done(idx)

                if callback is not None:
                    # Call original callback
                    callback(value)

                if self._all_done_callback is not None and self.all_done():
                    # All are done, callback
                    self._all_done_callback()

            return the_pool().apply_async(func, params,
                                          callback=tracking_callback,
                                          error_callback=error_callback)

    @property
    def errors(self) -> List[Exception]:
        assert self.all_done()
        return self._errors

    def mark_done(self, idx):
        with self._cond:
            self._done[idx] = True

    def all_done(self):
        with self._cond:
            return all(self._done)

    def join(self):
        """
        Wait on all tasks to complete
        """
        while not self.all_done():
            time.sleep(0.2)

        if self._errors:
            raise Exception('Failed to run all tasks')


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
