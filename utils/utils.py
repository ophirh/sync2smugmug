import datetime
import logging
import sys

logger = logging.getLogger(__name__)


class TimeIt(object):
    """
    I changed the time_it function to a class to allow passing a message that will be printed along with the time
    """
    def __init__(self, msg):
        self.msg = msg

    def __call__(self, fn):
        def inner_function(*args, **kwargs):
            then = datetime.datetime.now()
            v = fn(*args, **kwargs)
            now = datetime.datetime.now()
            delta = now - then

            logger.debug('%s runtime: %d seconds' % (self.msg, delta.total_seconds()))

            return v

        return inner_function


class ProgressTracker(object):
    def __init__(self, msg, every=10, total=None):
        """
        :type msg: str
        :type every: int
        :type total: int
        """
        self.msg = msg
        self.count = 0
        self.count_special = 0
        self.every = every
        self.total = total

    def __enter__(self):
        return self

    # noinspection PyShadowingBuiltins,PyUnusedLocal
    def __exit__(self, type, value, traceback):
        sys.stdout.write('\n')
        logger.info("Done %s (%d / %d)" % (self.msg, self.count, self.total))

    def tick(self):
        """
        Reported every time a progress is made. This method will print to the screen a progress indicator ('.') every
        'self.every' events to allow user to visually see progress.
        """
        self.count += 1
        if self.count % self.every == 0:
            # Write progress to the screen
            sys.stdout.write('\r%s - %d%% - %d/%d' % (self.msg, (self.count * 100) / self.total, self.count, self.total))
            sys.stdout.flush()