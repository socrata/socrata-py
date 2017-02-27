import pprint
from src.http import headers, respond
import requests

class Collection(object):
    def __init__(self, auth):
        self.auth = auth

    def path(self, fourfour):
        return 'https://{domain}/api/update/{fourfour}'.format(
            domain = self.auth.domain,
            fourfour = fourfour
        )

    def subresource(self, klass, result):
        (ok, res) = result
        if ok:
            return (ok, klass(self.auth, res))
        return result

class Resource(object):
    def __init__(self, auth, response, parent = None, *args, **kwargs):
        self.auth = auth
        self.on_response(response)
        self.define_operations(self.links)
        self.parent = parent

    def on_response(self, response):
        self.attributes = response['resource']
        self.links = response['links']

    def path(self, uri):
        return 'https://{domain}{uri}'.format(
            domain = self.auth.domain,
            uri = uri
        )

    def subresource(self, klass, result, **kwargs):
        (ok, res) = result
        if ok:
            return (ok, klass(self.auth, res, self, **kwargs))
        return result

    def __repr__(self):
        return "Resource(%s)" % pprint.pformat(self.attributes)

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
        return result

