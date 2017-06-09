import json
import requests
from socrata.http import post, put, delete
from socrata.resource import Collection, Resource
from socrata.uploads import Upload

class Revisions(Collection):
    def path(self, fourfour):
        return 'https://{domain}/api/publishing/v1/revision/{fourfour}'.format(
            domain = self.auth.domain,
            fourfour = fourfour
        )

    def create(self, fourfour):
        """
        Create a revision for the given dataset.
        """
        return self.subresource(Revision, post(
            self.path(fourfour),
            auth = self.auth
        ))

    def create_using_config(self, fourfour, config):
        """
        Create a revision for the given dataset.
        """
        return self.subresource(Revision, post(
            self.path(fourfour),
            auth = self.auth,
            data = json.dumps({
                'config': config.attributes['name']
            })
        ))


class Revision(Resource):
    """
    A revision is a change to a dataset
    """

    def create_upload(self, uri, body):
        """
        Create an upload within this revision
        """
        return self.subresource(Upload, post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        ))

    def discard(self, uri):
        """
        Discard this open revision.
        """
        return delete(self.path(uri), auth = self.auth)


    def metadata(self, uri, meta):
        """
        Set the metadata to be applied to the view
        when this revision is applied
        """
        return self._mutate(put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({'metadata': meta}),
        ))

