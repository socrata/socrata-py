import time
import requests
import json
from socrata.resource import Resource
from socrata.upsert_job import UpsertJob
from socrata.http import headers, respond, noop

class OutputSchema(Resource):
    def apply(self):
        uri = self.parent.parent.parent.show_uri + '/apply'
        return self.subresource(UpsertJob, respond(requests.put(
            self.path(uri),
            headers = headers(),
            data = json.dumps({'output_schema_id': self.attributes['id']}),
            auth = self.auth.basic,
            verify = self.auth.verify
        )))

    def is_complete(self):
        return all([column['transform']['completed_at'] for column in self.attributes['output_columns']])

    def wait_for_finish(self, progress = noop):
        while not self.is_complete():
            (ok, me) = self.show()
            progress(self)
            if not ok:
                return (ok, me)
            time.sleep(1)
        return (True, self)
