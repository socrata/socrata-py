import json
import requests
from socrata.http import headers, respond
from socrata.resource import Collection, Resource
from socrata.uploads import Upload

class Revisions(Collection):
    def path(self, fourfour):
        return 'https://{domain}/api/publishing/v1/revision/{fourfour}'.format(
            domain = self.auth.domain,
            fourfour = fourfour
        )

    def create(self, fourfour):
        (ok, revision) = result = self.subresource(Revision, respond(requests.post(
            self.path(fourfour),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        )))

        return result

class Revision(Resource):
    def create_upload(self, uri, body):
        return self.subresource(Upload, respond(requests.post(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        )))

    def discard(self, uri):
        return respond(requests.delete(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        ))

    def metadata(self, uri, meta):
        (ok, res) = result = respond(requests.put(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps({'metadata': meta}),
            verify = self.auth.verify
        ))
        if ok:
            self.on_response(res)
            return (ok, self)
        return result

