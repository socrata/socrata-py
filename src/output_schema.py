from src.resource import Resource
from src.upsert_job import UpsertJob
import requests
from src.http import headers, respond, noop

class OutputSchema(Resource):
    def __init__(self, *args, **kwargs):
        super(OutputSchema, self).__init__(*args, **kwargs)
        self._transform_progress = kwargs.get('progress', noop)

    def channel_name(self):
        return "output_schema"

    def joined(self):
        for column in self.attributes['output_columns']:
            transform = column['transform']

            channel = self.socket.channel(
                "transform_progress:{tid}".format(tid = transform['id']),
                {}
            )

            def on_progress(event_type, column):
                def p(event, _):
                    event['column'] = column
                    event['type'] = event_type
                    self._transform_progress(event)
                return p

            channel.on('max_ptr', on_progress('max_ptr', column))
            channel.on('finished', on_progress('finished', column))
            channel.on('errors', on_progress('errors', column))

            channel.join()


    def apply(self, uri):
        return self.subresource(UpsertJob, respond(requests.post(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        )))
