from src.resource import Resource
from src.token import get_token
from occamy import Socket
from src.http import noop

class OutputSchema(Resource):
    def __init__(self, *args, **kwargs):
        super(OutputSchema, self).__init__(*args, **kwargs)
        self.progress = kwargs.get('progress', noop)
        self.bind_progress()

    def bind_progress(self):
        fourfour = self.parent.parent.parent.attributes['fourfour']
        (ok, response) = get_token(self.auth, fourfour)
        assert ok, "Failed to get channel token"

        token = response['token']
        socket = Socket("wss://{domain}/api/update/socket".format(
            domain = self.auth.domain
        ),
        params = {
            'fourfour': fourfour,
            'token': token
        })
        socket.connect()

        for column in self.attributes['output_columns']:
            transform = column['transform']

            channel = socket.channel(
                "transform_progress:{tid}".format(tid = transform['id']),
                {}
            )
            channel.on('max_ptr', self._on_progress('max_ptr', column))
            channel.on('finished', self._on_progress('finished', column))
            channel.on('errors', self._on_progress('errors', column))

            channel.join()

    def _on_progress(self, event_type, column):
        def p(event, _):
            event['column'] = column
            event['type'] = event_type
            self.progress(event)
        return p

