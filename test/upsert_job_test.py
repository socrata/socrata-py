from time import sleep
from test.auth import TestCase

class TestUpsertJob(TestCase):
    def test_show_job_until_finished(self):
        rev = self.create_rev()
        input_schema = self.create_input_schema(rev = rev)
        output_schema = self.create_output_schema(input_schema = input_schema)
        job = rev.apply(output_schema = output_schema)
        job = job.wait_for_finish()
        job.attributes['status'] == 'successful'

