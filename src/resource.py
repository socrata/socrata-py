import pprint
from src.http import headers, respond, noop
import requests

class Collection(object):
    def __init__(self, auth):
        self.auth = auth

    def path(self, fourfour):
        return 'https://{domain}/api/update/{fourfour}'.format(
            domain = self.auth.domain,
            fourfour = fourfour
        )

    def subresource(self, klass, socket, result):
        (ok, res) = result
        if ok:
            return (ok, klass(self.auth, res, socket, self))
        return result

class Resource(object):
    def __init__(self, auth, response, socket, parent = None, *args, **kwargs):
        self.auth = auth
        self.socket = socket
        self.channel = None
        self.on = self.no_channel
        self.on_response(response)
        self.define_operations(self.links)
        self.parent = parent

    def on_response(self, response):
        self.attributes = response['resource']
        self.links = response['links']
        if not self.channel:
            channel = self.join_channel()
            self.bind_channel(channel)

    def no_channel(self):
        raise AttributeError('Not connected to a channel yet.')

    def on_channel(self, channel):
        def on(event, cb):
            channel.on(event, cb)
            return self
        return on

    def joined(self):
        pass

    def bind_channel(self, channel):
        self.on = self.on_channel(channel)
        self.joined()

    def channel_name(self):
        return None

    def join_channel(self):
        name = self.channel_name()
        if name:
            topic = '{name}:{id}'.format(
                name = name,
                id = self.attributes['id']
            )
            channel = self.socket.channel(topic, {})
            channel.join()
            return channel

    def path(self, uri):
        return 'https://{domain}{uri}'.format(
            domain = self.auth.domain,
            uri = uri
        )

    def subresource(self, klass, result, **kwargs):
        (ok, res) = result
        if ok:
            return (ok, klass(self.auth, res, self.socket, self, **kwargs))
        return result

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, pprint.pformat(self.attributes))

    def define_operations(self, links):
        for name, uri in links.items():
            setattr(self, name, self.dispatch(name, uri))
            setattr(self, '%s_uri' % name, lambda: uri)

        setattr(self, 'list_operations', lambda: list(links.keys()))

    def dispatch(self, name, uri):
        old = getattr(self, name, self.noop)
        def f(*args, **kwargs):
            return old(uri, *args, **kwargs)
        return f

    def noop(self, uri, *args, **kwargs):
        raise NotImplementedError("%s is not implemented" % uri)

    # This is just the identity of this resource, so it's easy to abstract
    def show(self, uri):
        (ok, res) = result = respond(requests.get(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        ))
        if ok:
            self.on_response(res)
            return (ok, self)
        return result

