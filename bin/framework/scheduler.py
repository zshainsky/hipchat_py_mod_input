import threading
from heapq import (heapify, heappush, heappop)
from time import time
import random
import logging

import log_files


_LOGGER = logging.getLogger(log_files.ta_frmk)


class Scheduler(object):
    """
    A simple scheduler which schedules the periodic or once event
    """

    max_delay_time = 60

    def __init__(self):
        self._job_heap = []
        self._lock = threading.Lock()

    def get_ready_jobs(self):
        """
        @return: a 2 element tuple. The first element is the next ready
                 duration. The second element is ready jobs list
        """

        now = time()
        ready_jobs = []
        sleep_time = 1

        with self._lock:
            job_heap = self._job_heap
            print "Job Heap in Scheduler:",job_heap
            total_jobs = len(job_heap)
            while job_heap:
                if job_heap[0][0] <= now:
                    job = heappop(job_heap)
                    if job[1].is_alive():
                        ready_jobs.append(job[1])
                        if job[1].get("duration") != 0:
                            # repeated job, calculate next due time and enqueue
                            job[0] = now + job[1].get("duration")
                            heappush(job_heap, job)
                    else:
                        _LOGGER.warn("Removing dead endpoint: %s",
                                     job[1].get("name"))
                else:
                    sleep_time = job_heap[0][0] - now
                    break

        if ready_jobs:
            _LOGGER.info("Get %d ready jobs, next duration is %f, "
                         "and there are %s jobs scheduling",
                         len(ready_jobs), sleep_time, total_jobs)

        ready_jobs.sort(key=lambda job: job.get("priority", 0), reverse=True)
        return (sleep_time, ready_jobs)

    def add_jobs(self, jobs):
        with self._lock:
            now = time()
            job_heap = self._job_heap
            for job in jobs:
                if job is not None:
                    delay_time = random.randrange(0, self.max_delay_time)
                    heappush(job_heap, [now + delay_time, job])

    def update_jobs(self, jobs):
        with self._lock:
            job_heap = self._job_heap
            for njob in jobs:
                for i, job in enumerate(job_heap):
                    if njob == job[1]:
                        job_heap[i][1] = njob
                        break
            heapify(job_heap)

    def remove_jobs(self, jobs):
        with self._lock:
            new_heap = []
            for job in self._job_heap:
                for djob in jobs:
                    if djob == job[1]:
                        _LOGGER.info("Remove job=%s", djob.ident())
                        break
                else:
                    new_heap.append(job)
            heapify(new_heap)
            self._job_heap = new_heap

    def number_of_jobs(self):
        with self._lock:
            return len(self._job_heap)
