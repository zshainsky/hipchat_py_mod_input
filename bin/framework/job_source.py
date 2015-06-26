class JobSource(object):

    def start(self):
        pass

    def tear_down(self):
        pass

    def get_jobs(self, timeout=0):
        """
        jobs can be from conf files or from stdin or from network ...
        """

        raise NotImplementedError("Derived class shall implement get_jobs")

    def put_jobs(self, jobs):
        pass
