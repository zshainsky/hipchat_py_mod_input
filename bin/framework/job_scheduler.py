"""
A simple scheduler which schedules the periodic or once job
"""

import logging
import threading
import Queue
import traceback

import log_files
import scheduler as sched


_LOGGER = logging.getLogger(log_files.ta_frmk)


class JobScheduler(object):

    def __init__(self, job_factory, scheduler=None):
        """
        @scheduler: a scheduler which schedules peroidic events or once events
        @job_factory: an object which creates jobs. Shall implement
        AbstractJobFactory interface and return an callable object. The
        returned job object shall implement dict.get like interface, and shall
        support get("name") => string, get("duration") => int,
        get("priority") => int, and implements is_alive() => boolean interfaces
        """

        self._sched = scheduler
        if scheduler is None:
            self._sched = sched.Scheduler()
        self._job_factory = job_factory
        self._job_thr = threading.Thread(target=self._get_jobs)
        self._wakeup_q = Queue.Queue()
        self._jobs = set()
        self._started = False
        self._stopped = False

    def start(self):
        if self._started:
            return
        self._started = True

        self._job_thr.start()
        self._job_factory.start()
        _LOGGER.info("JobScheduler started.")

    def tear_down(self):
        if self._stopped:
            return
        self._stopped = True

        self._wakeup_q.put(True)
        self._job_thr.join()
        self._job_factory.tear_down()
        _LOGGER.info("JobScheduler stopped.")

    def get_ready_jobs(self):
        return self._sched.get_ready_jobs()

    def number_of_jobs(self):
        return self._sched.number_of_jobs()

    @staticmethod
    def _handle_jobs(jobs, existing_jobs, scheduler):
        for job in jobs:
            if job in existing_jobs:
                if job.get("duration") is None:
                    scheduler.remove_jobs((job,))
                else:
                    scheduler.update_jobs((job,))
            else:
                scheduler.add_jobs((job,))

    def _get_jobs(self):
        job_factory = self._job_factory
        wakeup_q = self._wakeup_q
        scheduler = self._sched
        existing_jobs = self._jobs

        while 1:
            try:
                jobs = job_factory.get_jobs()
            except Exception:
                print "Fialed to get jobs, reason=%s", traceback.format_exc()
                _LOGGER.error("Failed to get jobs, reason=%s",
                              traceback.format_exc())
            else:
                if jobs:
                    self._handle_jobs(jobs, existing_jobs, scheduler)
                    continue

            try:
                done = wakeup_q.get(timeout=1)
            except Queue.Empty:
                pass
            else:
                if done:
                    break
