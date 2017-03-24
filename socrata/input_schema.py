import json
import requests
from socrata.http import headers, respond, noop
from socrata.resource import Collection, Resource
from socrata.output_schema import OutputSchema

class InputSchema(Resource):
    def transform(self, uri, body):
        result = respond(requests.post(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        ))
        return self.subresource(OutputSchema, result)

    def latest_output(self, uri):
        result = respond(requests.get(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        ))
        return self.subresource(OutputSchema, result)
