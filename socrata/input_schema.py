import json
import requests
from socrata.http import noop, post, get
from socrata.resource import Collection, Resource
from socrata.output_schema import OutputSchema

class InputSchema(Resource):
    def transform(self, uri, body):
        """
        Transform this InputSchema into an Output. Returns the
        new OutputSchema. Note that this call is async - the data
        may still be transforming even though the OutputSchema is
        returned. See OutputSchema.wait_for_finish to block until
        the
        """
        return self._subresource(OutputSchema, post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body),
        ))

    def latest_output(self, uri):
        """
        Get the latest (most recently created) OutputSchema
        which descends from this InputSchema
        """
        return self._subresource(OutputSchema, get(
            self.path(uri),
            auth = self.auth,
        ))
