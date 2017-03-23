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
