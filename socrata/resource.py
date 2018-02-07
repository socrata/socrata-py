import time
import pprint
from socrata.http import noop, get, TimeoutException
import requests

class Collection(object):
    def __init__(self, auth):
        self.auth = auth

    def _subresources(self, klass, result):
        (ok, resources) = result
        if ok:
            return (ok, [klass(self.auth, res, self) for res in resources])
        return result

    def _subresource(self, klass, result):
        (ok, res) = result
        if ok:
            return (ok, klass(self.auth, res, self))
        return result


def parameterize_links(links, id_name, id_val):
    d = {}
    for name, uri in links.items():
        if type(uri) == str:
            d[name] = uri.replace('{%s}' % id_name, str(id_val))
        else:
            d[name] = parameterize_links(uri, id_name, id_val)
    return d

class ChildResourceSpec(object):
    def __init__(
        self,
        parent,
        child_list_name,
        links_namespace,
        response_namespace,
        child_type,
        link_id_attr
    ):
        self._parent = parent
        self._child_list_name = child_list_name
        self._links_namespace = links_namespace
        self._response_namespace = response_namespace
        self._child_type = child_type
        self._link_id_attr = link_id_attr


    def build_children_from(self, parent_response):
        def build_links(child):

            child_link_templates = self._parent.child_ops[self._links_namespace]
            return parameterize_links(
                child_link_templates,
                self._link_id_attr,
                child['id']
            )

        subresources = []
        # This is the actual list of data in the parent response
        response_list = parent_response['resource'][self._response_namespace]
        for child in response_list:
            child_response = {
                'links': build_links(child),
                'resource': child
            }

            # At this point, this looks identical to the response we receive
            # when looking up the child via the API using a GET request,
            # so we can just create the child resource like we would in that case
            result = (True, child_response)

            (_, subresource) = self._parent._subresource(self._child_type, result)
            subresources.append(subresource)

        return self._child_list_name, subresources


class Resource(object):
    def __init__(self, auth, response, parent = None, *args, **kwargs):
        self.auth = auth
        self._on_response(response)
        self.parent = parent

    @classmethod
    def from_uri(cls, auth, uri):
        path = 'https://{domain}{uri}'.format(
            domain = auth.domain,
            uri = uri
        )
        (ok, resp) = result = get(path, auth = auth)
        if ok:
            return (ok, cls(auth, resp))
        return result


    def _clone(self, result):
        (ok, res) = result
        if ok:
            return (ok, self.__class__(self.auth, res, self.parent))
        return result

    def _on_response(self, response):
        self.attributes = response['resource']
        self.links = response['links']
        self._define_operations(self.links)
        self.define_children(response)

    def define_children(self, response):
        for child_list_name, child_list in [
            spec.build_children_from(response)
            for spec in self.child_specs()
        ]:
            setattr(self, child_list_name, child_list)

    def child_specs(self):
        return []

    def path(self, uri):
        return 'https://{domain}{uri}'.format(
            domain = self.auth.domain,
            uri = uri
        )

    def _subresource(self, klass, result, **kwargs):
        (ok, res) = result
        if ok:
            return (ok, klass(self.auth, res, self, **kwargs))
        return result

    def _subresources(self, klass, result):
        (ok, resources) = result
        if ok:
            return (ok, [klass(self.auth, res, self) for res in resources])
        return result

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, pprint.pformat(self.attributes))

    def _define_operations(self, links):
        self_ops = {name: uri for name, uri in links.items() if type(uri) == str}
        child_ops = {name: d for name, d in links.items() if type(d) == dict}

        for name, uri in self_ops.items():
            setattr(self, name, self._dispatch(name, uri))
            setattr(self, '%s_uri' % name, uri)

        setattr(self, 'available_operations', self_ops.keys())
        setattr(self, 'child_ops', child_ops)

    def list_operations(self):
        """
        Get a list of the operations that you can perform on this
        object. These map directly onto what's returned from the API
        in the `links` section of each resource
        """
        return getattr(self, 'available_operations', [])

    # Yes this is kind of terrifying and bad and magic but i can't
    # think of a less obnoxious way to inject the uri
    def _dispatch(self, name, uri):
        og_method_name = '_' + name
        if not hasattr(self, og_method_name):
            og_method = getattr(self, name, self._noop)
            setattr(self, og_method_name, og_method)

        old = getattr(self, og_method_name, self._noop)
        def f(*args, **kwargs):
            return old(uri, *args, **kwargs)
        return f

    def _noop(self, uri, *args, **kwargs):
        raise NotImplementedError("%s is not implemented" % uri)

    def _mutate(self, result):
        (ok, response) = result
        if ok:
            self._on_response(response)
            return (ok, self)
        return result

    # This is just the identity of this resource, so it's easy to abstract
    def show(self, uri):
        return self._mutate(get(
            self.path(uri),
            auth = self.auth
        ))


    def _wait_for_finish(self, is_finished, is_failed, progress, timeout, sleeptime):
        started = time.time()
        while not is_finished(self):
            current = time.time()
            if timeout and (current - started > timeout):
                raise TimeoutException("Timed out after %s seconds waiting for completion" % timeout)
            (ok, me) = self.show()
            progress(self)
            if not ok:
                return (ok, me)
            if is_failed(self):
                return (False, me)
            time.sleep(sleeptime)
        return (True, self)
