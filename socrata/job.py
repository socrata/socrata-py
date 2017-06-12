import time
from socrata.http import noop
from socrata.resource import Resource

class Job(Resource):
    def is_complete(self):
        """
        Has this job finished or failed
        """
        return not (self.attributes['status'] == 'in_progress')

    def wait_for_finish(self, progress = noop):
        """
        Wait for this job to finish applying to the underlying
        dataset
        """
        while not self.is_complete():
            (ok, job) = self.show()
            progress(self)
            if not ok:
                return (ok, job)
            time.sleep(1)
        return (True, self)

