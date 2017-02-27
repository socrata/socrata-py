import json
import requests
from src.http import headers, respond, noop
from src.resource import Collection, Resource
from src.output_schema import OutputSchema

class InputSchema(Resource):

    def transform(self, body, progress = noop):
        # This should come back in the response like everything else
        uri = self.path('/api/update/{fourfour}/{seq}/schema/{is_id}'.format(
            fourfour = self.parent.parent.attributes['fourfour'],
            seq = self.parent.parent.attributes['update_seq'],
            is_id = self.attributes['id']
        ))

        result = respond(requests.post(
            uri,
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        ))
        return self.subresource(OutputSchema, result, progress = progress)
