from time import sleep
import unittest
from test.auth import create_output_schema

class TestUpsertJob(unittest.TestCase):
    def test_show_upsert_job_until_finished(self):
        output_schema = create_output_schema()
        (ok, upsert_job) = output_schema.apply()

        done = False
        attempts = 0
        while not done and attempts < 20:
            (ok, job) = upsert_job.show()
            attempts += 1
            assert ok, "Job poller failed to poll!"

            done = job.attributes['status'] == 'successful'
            print(job.attributes['log'])
            sleep(0.5)

        assert done, "Polling job never resulted in a successful completion: %s" % job

