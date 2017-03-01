import json
import requests
from src.http import headers, respond, noop
from src.resource import Collection, Resource
from src.output_schema import OutputSchema

class InputSchema(Resource):
    def channel_name(self):
        return "input_schema"

    def transform(self, uri, body, progress = noop):
        result = respond(requests.post(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        ))
        return self.subresource(OutputSchema, result, progress = progress)
