from socrata.http import noop
from socrata.resource import Resource

class Job(Resource):
    def __init__(self, auth, response, parent = None, *args, **kwargs):
        Resource.__init__(self, auth, response, parent, *args, **kwargs)
        if self.submitted_for_approval():
            self.attributes['status'] = 'submitted_for_approval'

    def is_complete(self):
        """
        Has this job finished or failed (or been submitted for approval)
        """
        if self.submitted_for_approval(): return True
        status = self.attributes['status']
        return (status == 'failure' or status == 'successful')

    def wait_for_finish(self, progress = noop, timeout = None, sleeptime = 1):
        """
        Wait for this dataset to finish transforming and validating. Accepts a progress function
        and a timeout.
        """
        if self.submitted_for_approval(): return self

        return self._wait_for_finish(
            is_finished = lambda m: m.attributes['finished_at'],
            is_failed = lambda m: m.attributes['status'] == 'failure',
            progress = progress,
            timeout = timeout,
            sleeptime = sleeptime
        )
    
    def submitted_for_approval(self):
        """
        Has this job entered the approval queue (rather than finishing or failing)
        """
        return self.attributes.get('key', '') == 'approval_submitted'


