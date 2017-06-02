import json
import requests
from socrata.http import noop, post, get
from socrata.resource import Collection, Resource
from socrata.output_schema import OutputSchema

class InputSchema(Resource):
    def transform(self, uri, body):
        return self.subresource(OutputSchema, post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body),
        ))

    def latest_output(self, uri):
        return self.subresource(OutputSchema, get(
            self.path(uri),
            auth = self.auth,
        ))
