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
        """
        Create a new ImportConfig. See http://docs.socratapublishing.apiary.io/
        ImportConfig section for what is supported in `data_action`, `parse_options`,
        and `columns`.
        """
        return self._subresource(Config, post(
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
        """
        Obtain a single ImportConfig by name
        """
        return self._subresource(Config, get(
            self.path() + '/' + name,
            auth = self.auth
        ))

    def list(self):
        """
        List all the ImportConfigs on this domain
        """
        return self._subresources(Config, get(
            self.path(),
            auth = self.auth
        ))

class Config(Resource):
    def delete(self, uri):
        """
        Delete this ImportConfig. Note that this cannot be undone.
        """
        return delete(self.path(uri), auth = self.auth)

    def update(self, uri, data_action = None, parse_options = None, columns = None):
        """
        Mutate this ImportConfig in place. Subsequent revisions opened against this
        ImportConfig will take on its new value.
        """
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
        """
        Create a new Revision in the context of this ImportConfig.
        Uploads that happen in this Revision will take on the values
        in this Config.
        """
        # Because of circular dependencies ;_;
        from socrata.revisions import Revision

        (ok, res) = result = post(
            self.path(uri).format(fourfour = fourfour),
            auth = self.auth
        )
        if not ok:
            return result
        return (ok, Revision(self.auth, res))
