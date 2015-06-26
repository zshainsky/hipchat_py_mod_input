class JobFactory(object):

    def __init__(self, job_source, event_writer):
        self._job_source = job_source
        self._event_writer = event_writer

    def start(self):
        self._job_source.start()

    def tear_down(self):
        self._job_source.tear_down()

    def get_jobs(self):
        """
        Get jobs from job source
        """

        jobs = self._job_source.get_jobs(timeout=1)
        if jobs:
            return [self._create_job(job) for job in jobs]
        return None

    def _create_job(self, job):
        """
        @job: dict for job definition
        """
        raise NotImplementedError("Derived class shall override _create_job")


class Job(object):

    def __init__(self, job_id):
        self._id = job_id

    def __call__(self):
        raise NotImplementedError("Derived class shall implement call method")

    def is_alive(self):
        return True

    def get(self, key, default=None):
        raise NotImplementedError("Derived class shall implement get method")

    def __eq__(self, other):
        return isinstance(other, Job) and self.ident() == other.ident()

    def __cmp__(self, other):
        if self.ident() > other.ident():
            return 1
        elif self.ident() < other.ident():
            return -1
        return 0

    def ident(self):
        return self._id
