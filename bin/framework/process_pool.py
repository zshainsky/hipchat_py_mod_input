"""
A wrapper of multiprocessing.pool
"""

import multiprocessing
import logging

import log_files


_LOGGER = logging.getLogger(log_files.ta_frmk)


class ProcessPool(object):
    """
    A simple wrapper of multiprocessing.pool
    """

    def __init__(self, min_size=0, maxtasksperchild=10000):
        if min_size <= 0:
            min_size = multiprocessing.cpu_count()
        self.size = min_size
        self._pool = multiprocessing.Pool(processes=min_size,
                                          maxtasksperchild=maxtasksperchild)
        self._stopped = False

    def tear_down(self):
        """
        Tear down the pool
        """

        if self._stopped:
            return
        self._stopped = True

        self._pool.close()
        self._pool.join()
        _LOGGER.info("ProcessPool stopped.")

    def apply(self, func, args=(), kwargs={}):
        """
        Run the job synchronously
        """

        return self._pool.apply(func, args, kwargs)

    def apply_async(self, func, args=(), kwargs={}, callback=None):
        """
        Run the job asynchronously
        """

        return self._pool.apply_async(func, args, kwargs, callback)
