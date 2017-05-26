import json
import requests
from socrata.http import headers, respond
from socrata.resource import Resource, Collection
from socrata.input_schema import InputSchema

class Uploads(Collection):
    def create(self, body):
        path = 'https://{domain}/api/publishing/v1/upload'.format(
            domain = self.auth.domain
        )
        return self.subresource(Upload, respond(requests.post(
            path,
            headers = headers(),
            auth = self.auth.basic,
            data = json.dumps(body),
            verify = self.auth.verify
        )))

class Upload(Resource):
    def bytes(self, uri, file_handle, content_type):
        return self.subresource(InputSchema, respond(requests.post(
            self.path(uri),
            headers = headers({
                'content-type': content_type
            }),
            auth = self.auth.basic,
            data = file_handle,
            verify = self.auth.verify
        )))

    def csv(self, file_handle):
        return self.bytes(file_handle, "text/csv")

    def xls(self, file_handle):
        return self.bytes(file_handle, "application/vnd.ms-excel")

    def xlsx(self, file_handle):
        return self.bytes(file_handle, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def tsv(self, file_handle):
        return self.bytes(file_handle, "text/tab-separated-values")


    def add_to_revision(self, uri, revision):
        (ok, res) = result = respond(requests.patch(
            self.path(uri),
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify,
            data = json.dumps({
                'revision': {
                    'fourfour': revision.attributes['fourfour'],
                    'revision_seq': revision.attributes['revision_seq']
                }
            })
        ))
        if ok:
            self.on_response(res)
            return (ok, self)
        return result
