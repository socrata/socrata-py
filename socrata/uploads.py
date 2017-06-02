import json
from socrata.http import post, patch
from socrata.resource import Resource, Collection
from socrata.input_schema import InputSchema

class Uploads(Collection):
    def create(self, body):
        path = 'https://{domain}/api/publishing/v1/upload'.format(
            domain = self.auth.domain
        )
        return self.subresource(Upload, post(
            path,
            auth = self.auth,
            data = json.dumps(body)
        ))

class Upload(Resource):
    def bytes(self, uri, file_handle, content_type):
        return self.subresource(InputSchema, post(
            self.path(uri),
            auth = self.auth,
            data = file_handle,
            headers = {
                'content-type': content_type
            }
        ))

    def csv(self, file_handle):
        return self.bytes(file_handle, "text/csv")

    def xls(self, file_handle):
        return self.bytes(file_handle, "application/vnd.ms-excel")

    def xlsx(self, file_handle):
        return self.bytes(file_handle, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def tsv(self, file_handle):
        return self.bytes(file_handle, "text/tab-separated-values")


    def add_to_revision(self, uri, revision):
        (ok, res) = result = patch(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'revision': {
                    'fourfour': revision.attributes['fourfour'],
                    'revision_seq': revision.attributes['revision_seq']
                }
            })
        )
        if ok:
            self.on_response(res)
            return (ok, self)
        return result
