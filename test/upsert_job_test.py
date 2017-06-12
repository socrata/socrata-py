from time import sleep
from test.auth import TestCase

class TestUpsertJob(TestCase):
    def test_show_job_until_finished(self):
        rev = self.create_rev()
        input_schema = self.create_input_schema(rev = rev)
        output_schema = self.create_output_schema(input_schema = input_schema)
        (ok, job) = rev.apply(output_schema = output_schema)
        assert ok, job
        done = False
        attempts = 0
        while not done and attempts < 20:
            (ok, job) = job.show()
            attempts += 1
            assert ok, job

            done = job.attributes['status'] == 'successful'
            sleep(0.5)

        assert done, "Polling job never resulted in a successful completion: %s" % job

