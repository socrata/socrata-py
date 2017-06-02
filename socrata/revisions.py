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
        return self.subresource(Revision, post(
            self.path(fourfour),
            auth = self.auth
        ))

class Revision(Resource):
    def create_upload(self, uri, body):
        return self.subresource(Upload, post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        ))

    def discard(self, uri):
        return delete(self.path(uri), auth = self.auth)

    def metadata(self, uri, meta):
        (ok, res) = result = put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({'metadata': meta}),
        )
        if ok:
            self.on_response(res)
            return (ok, self)
        return result

