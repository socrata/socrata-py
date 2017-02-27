import json
import requests
from src.http import headers, respond
from src.resource import Resource
from src.input_schema import InputSchema


def noop(*args, **kwargs):
    pass

class Upload(Resource):
    def bytes(self, uri, file_handle, content_type, progress):
        return self.subresource(InputSchema, respond(requests.post(
            self.path(uri),
            headers = headers({'content-type': content_type}),
            auth = self.auth.basic,
            data = self.wrap(file_handle, progress),
            verify = self.auth.verify
        )))

    def csv(self, file_handle, progress = noop):
        return self.bytes(file_handle, "text/csv", progress)

    def wrap(self, iter, progress):
        total = 0
        for chunk in iter:
            b = len(chunk)
            total += b
            progress({'total_bytes': total, 'bytes': b})
            yield chunk
