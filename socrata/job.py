from socrata.http import noop
from socrata.resource import Resource

class Job(Resource):
    def is_complete(self):
        """
        Has this job finished or failed
        """
        status = self.attributes['status']
        return (status == 'failure' or status == 'successful')

    def wait_for_finish(self, progress = noop, timeout = None, sleeptime = 1):
        """
        Wait for this dataset to finish transforming and validating. Accepts a progress function
        and a timeout.
        """
        return self._wait_for_finish(
            is_finished = lambda m: m.attributes['finished_at'],
            is_failed = lambda m: m.attributes['status'] == 'failure',
            progress = progress,
            timeout = timeout,
            sleeptime = sleeptime
        )
