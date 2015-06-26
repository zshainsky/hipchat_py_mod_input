"""
Create scheduling jobs
"""

import logging
import socket
import os.path as op
import sys

sys.path.append(op.join(op.dirname(op.abspath(__file__)), "framework"))

import state_store as ss
import job_factory as jf
import utils


__all__ = ["HipChatJobFactory"]



#_LOGGER = logging.getLogger("ta_box")


class HipChatCollectionJob(jf.Job):

    def __init__(self, job_id, config, data_collect_func):
        super(HipChatCollectionJob, self).__init__(job_id)
        self._config = config
        self._func = data_collect_func
        if not config.get("host", None):
            config["host"] = socket.gethostname()

    def __call__(self):
        config = self._config
        #_LOGGER.debug("Start collecting from %s.", config["name"])
        print "Start collecting from %s.", config["name"]
        idx = config.get("index", "hipchat")
        host = config["host"]
        event_writer = config["event_writer"]
        while 1:
            done, results = self._func()
            if results:
                events = "".join(("<stream>%s</stream>"
                                  % obj.to_string(idx, host)
                                  for obj in results))
                event_writer.write_events(events)

            if done:
                break

        _LOGGER.debug("End collecting from %s.", self._config["name"])

    def get(self, key, default=None):
        return self._config.get(key, default)


class HipChatJobFactory(jf.JobFactory):

    # Fix Cyclic import issue
    import hipchat_data_loader as hcdl

    def __init__(self, job_source, event_writer):
        super(HipChatJobFactory, self).__init__(job_source, event_writer)
        print "init Job"
        self._rest_to_cls = {
            "users": self.hcdl.HipChatUsers
        }
        print self._rest_to_cls

    def _create_job(self, job):
        appname = utils.get_appname_from_path(op.abspath(__file__))
        job["appname"] = appname
        job["event_writer"] = self._event_writer
        job["state_store"] = ss.StateStore(
            job, appname, use_kv_store=job.get("use_kv_store"))
        hipchat = self._rest_to_cls[job["rest_endpoint"]](job)
        print "Created hipchat job"
        return HipChatCollectionJob(job["rest_endpoint"], job, hipchat.collect_data)
