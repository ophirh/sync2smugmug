import logging
import threading
import time
from typing import List, Optional, Tuple, Callable
from multiprocessing.pool import Pool

logger = logging.getLogger(__name__)


class TaskPool:
    """
    Represents a set of tasks that can be executed concurrently, but need to be monitored (for completion) together.

    A classic example is downloading images for an album. We want to do this as concurrently as possible, but only
    mark the albums meta-data (on disk) when download is fully finished
    """

    PROCESSES = 10

    _pool: Optional[Pool] = None

    def __init__(self, all_done_callback: Optional[Callable[[], None]] = None):
        self._done: List[bool] = []
        self._errors: List[Exception] = []
        self._all_done_callback = all_done_callback
        self._cond = threading.Condition(threading.Lock())

        if self._pool is None:
            self._pool = Pool(processes=self.PROCESSES)

    def apply_async(self, func: Callable, params: Tuple = (), callback: Callable = None):
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

        return self._pool.apply_async(func, params, callback=tracking_callback, error_callback=error_callback)

    @classmethod
    def wait_for_all_tasks(cls):
        """
        Make sure that all tasks from all pool instances are done
        """
        if cls._pool is not None:
            cls._pool.close()
            cls._pool.join()
            cls._pool = None

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
