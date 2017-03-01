import json
import requests
from src.transport.ws import connect
from src.http import headers, respond
from src.resource import Collection, Resource
from src.uploads import Upload

class Revisions(Collection):
    def create(self, fourfour):
        (ok, revision) = result = self.subresource(Revision, connect(self.auth, fourfour), respond(requests.post(
            self.path(fourfour),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        )))


        return result

class Revision(Resource):
    def channel_name(self):
        return "update"

    def create_upload(self, uri, body):
        return self.subresource(Upload, respond(requests.post(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        )))
