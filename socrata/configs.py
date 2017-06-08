import json
import requests
from socrata.http import get, post, patch, delete
from socrata.resource import Collection, Resource


class Configs(Collection):
    def path(self):
        return 'https://{domain}/api/publishing/v1/config'.format(
            domain = self.auth.domain
        )

    def create(self, name, data_action, parse_options = None, columns = None):
        return self.subresource(Config, post(
            self.path(),
            auth = self.auth,
            data = json.dumps({
                'name': name,
                'data_action': data_action,
                'parse_options': parse_options,
                'columns': columns
            })
        ))

    # ~~ danger ~~ This URL is hardcoded
    def lookup(self, name):
        return self.subresource(Config, get(
            self.path() + '/' + name,
            auth = self.auth
        ))

    def list(self):
        return self.subresources(Config, get(
            self.path(),
            auth = self.auth
        ))

class Config(Resource):
    def delete(self, uri):
        return delete(self.path(uri), auth = self.auth)

    def update(self, uri, data_action = None, parse_options = None, columns = None):
        data_action = data_action or self.attributes['data_action']
        parse_options = parse_options or self.attributes['parse_options']
        columns = columns or self.attributes['columns']

        return self._mutate(patch(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'data_action': data_action,
                'parse_options': parse_options,
                'columns': columns
            })
        ))

    def create_revision(self, uri, fourfour):
        # Because of circular dependencies ;_;
        from socrata.revisions import Revision

        (ok, res) = result = post(
            self.path(uri).format(fourfour = fourfour),
            auth = self.auth
        )
        if not ok:
            return result
        return (ok, Revision(self.auth, res))
