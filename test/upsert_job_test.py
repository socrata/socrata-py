from time import sleep
import unittest
from test.auth import create_output_schema

class TestUpsertJob(unittest.TestCase):
    def test_show_job_until_finished(self):
        output_schema = create_output_schema()
        (ok, job) = output_schema.apply()
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

