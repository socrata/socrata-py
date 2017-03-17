import json
import requests
from socrata.http import headers, respond
from socrata.resource import Resource
from socrata.input_schema import InputSchema

def noop(*args, **kwargs):
    pass

class Upload(Resource):
    def channel_name(self):
        return "upload"

    def joined(self):
        return
        def on_new_input_schema(payload, _):
            payload = {
                'resource': payload,
                'links': {
                    'show': self.show_uri() + '/schema/' + str(payload['id'])
                }
            }
            (ok, input_schema) = self.subresource(InputSchema, (True, payload))
            assert ok, "Failed to create InputSchema on Upload"
            for column in input_schema.attributes['output_schemas'][0]['output_columns']:
                transform = column['transform']

                channel = self.socket.channel(
                    "transform_progress:{tid}".format(tid = transform['id']),
                    {}
                )
                def on_progress(payload, _):
                    payload['column'] = column
                    self._row_progress(payload)
                channel.on('max_ptr', on_progress)
                channel.join()

        self.on('insert_input_schema', on_new_input_schema)


    def bytes(self, uri, file_handle, content_type, progress):
        self._row_progress = progress
        return self.subresource(InputSchema, respond(requests.post(
            self.path(uri),
            headers = headers({
                'content-type': content_type
            }),
            auth = self.auth.basic,
            data = file_handle,
            verify = self.auth.verify
        )))

    def csv(self, file_handle, progress = noop):
        return self.bytes(file_handle, "text/csv", progress)
