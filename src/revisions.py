import json
import requests
from src.http import headers, respond
from src.resource import Collection, Resource
from src.uploads import Upload

class Revisions(Collection):
    def create(self, fourfour):
        return self.subresource(Revision, respond(requests.post(
            self.path(fourfour),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        )))

class Revision(Resource):
    def create_upload(self, uri, body):
        return self.subresource(Upload, respond(requests.post(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        )))
