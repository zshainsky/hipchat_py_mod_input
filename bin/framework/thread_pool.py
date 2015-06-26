"""
A simple thread pool implementation
"""

import threading
import Queue
import multiprocessing
import logging
import traceback
from time import time

import log_files


_LOGGER = logging.getLogger(log_files.ta_frmk)


class ThreadPool(object):
    """
    A simple thread pool implementation
    """

    _high_watermark = 2
    _low_watermark = 0.5
    _resize_window = 120

    def __init__(self, min_size=1, max_size=128,
                 task_queue_size=256, daemon=True):
        assert task_queue_size

        if not min_size or min_size <= 0:
            min_size = multiprocessing.cpu_count()

        if not max_size or max_size <= 0:
            max_size = multiprocessing.cpu_count() * 8

        self.min_size = min_size
        self.max_size = max_size
        self.daemon = daemon

        self.work_queue = Queue.PriorityQueue(task_queue_size)
        self.thrs = []
        for _ in range(min_size):
            thr = threading.Thread(target=self._run)
            self.thrs.append(thr)
        self._last_resize_time = time()
        self._last_size = min_size
        self._lock = threading.Lock()
        self._started = False
        self._stopped = False

    def start(self):
        """
        Start threads in the pool
        """

        with self._lock:
            if self._started:
                return
            self._started = True

            for thr in self.thrs:
                thr.daemon = self.daemon
                thr.start()
            _LOGGER.info("ThreadPool started.")

    def tear_down(self):
        """
        Tear down thread pool
        """

        with self._lock:
            if self._stopped:
                return
            self._stopped = True

            for thr in self.thrs:
                self.work_queue.put(None)

            if not self.daemon:
                _LOGGER.info("Wait for threads to stop.")
                for thr in self.thrs:
                    thr.join()

            _LOGGER.info("ThreadPool stopped.")

    def enqueue_jobs(self, jobs, block=True):
        """
        @jobs: tuple/list-like or generator like object, job shall be callable
        """

        for job in jobs:
            self.work_queue.put(job, block)

    def resize(self, new_size):
        """
        Resize the pool size, spawn or destroy threads if necessary
        """

        if new_size <= 0:
            return

        if self._lock.locked() or self._stopped:
            _LOGGER.info("Try to resize thread pool during the tear "
                         "down process, do nothing")
            return

        with self._lock:
            self._remove_exited_threads_with_lock()
            size = self._last_size
            self._last_size = new_size
            if new_size > size:
                for _ in xrange(new_size - size):
                    thr = threading.Thread(target=self._run)
                    thr.daemon = self.daemon
                    thr.start()
                    self.thrs.append(thr)
            elif new_size < size:
                for _ in xrange(size - new_size):
                    self.work_queue.put(None)
        _LOGGER.info("Finished ThreadPool resizing. New size=%d", new_size)

    def _remove_exited_threads_with_lock(self):
        """
        Join the exited threads last time when resize was called
        """

        joined_thrs = set()
        for thr in self.thrs:
            if not thr.is_alive():
                try:
                    if not thr.daemon:
                        thr.join(timeout=0.5)
                    joined_thrs.add(thr.ident)
                except RuntimeError:
                    pass

        if joined_thrs:
            live_thrs = []
            for thr in self.thrs:
                if thr.ident not in joined_thrs:
                    live_thrs.append(thr)
            self.thrs = live_thrs

    def _do_resize_according_to_loads(self, qsize):
        if (self._last_resize_time and
                time() - self._last_resize_time < self._resize_window):
            return

        thr_size = self._last_size
        if qsize / thr_size > self._high_watermark:
            _LOGGER.info("Reach high watermark")
            if thr_size < self.max_size:
                thr_size = min(thr_size * 2, self.max_size)
                self.resize(thr_size)
        elif qsize / thr_size < self._low_watermark:
            _LOGGER.info("Reach low watermark")
            if thr_size > self.min_size:
                thr_size = max(self.min_size, thr_size / 2)
                self.resize(thr_size)
        self._last_resize_time = time()

    def _run(self):
        """
        Threads callback func, run forever to handle jobs from the job queue
        """

        work_queue = self.work_queue
        while 1:
            _LOGGER.debug("Going to get job")
            job = work_queue.get()
            if job is None:
                break

            if self._stopped:
                work_queue.put(None)

            _LOGGER.debug("Going to exec job")
            try:
                job()
            except Exception:
                _LOGGER.error(traceback.format_exc())
            _LOGGER.debug("Done with exec job")
            qsize = work_queue.qsize()
            _LOGGER.info("Thread work_queue_size=%d", qsize)
            self._do_resize_according_to_loads(qsize)

        _LOGGER.info("Worker thread %s stopped.",
                     threading.current_thread().getName())
