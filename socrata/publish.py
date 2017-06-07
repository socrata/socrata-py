from socrata.resource import Collection
from socrata.revisions import Revisions
from socrata.uploads import Uploads
from socrata.configs import Configs
from socrata.http import gen_headers, post
import json
import requests

class SocrataException(Exception):
    def __init__(self, message, response):
        super(Exception, self).__init__(message + ':\n' + str(response))
        self.response = response


class Operation(object):
    def __init__(self, publish, **kwargs):
        self.publish = publish
        self.properties = kwargs

    def csv(self, file):
        return self.run(file, lambda upload: upload.csv(file))

    def xls(self, file):
        return self.run(file, lambda upload: upload.xls(file))

    def xlsx(self, file):
        return self.run(file, lambda upload: upload.xlsx(file))

    def tsv(self, file):
        return self.run(file, lambda upload: upload.tsv(file))


class Create(Operation):
    def run(self, file, put_bytes):
        (ok, view) = view_create = self.publish.new(self.properties)
        if not ok:
            raise SocrataException("Failed to create the view", view)

        (ok, rev) = self.publish.revisions.create(view['id'])
        if not ok:
            raise SocrataException("Failed to create the revision", rev)

        (ok, upload) = rev.create_upload({'filename': file.name})
        if not ok:
            raise SocrataException("Failed to create the upload", upload)

        (ok, inp) = put_bytes(upload)
        if not ok:
            raise SocrataException("Failed to upload the file", inp)

        (ok, out) = inp.latest_output()
        if not ok:
            raise SocrataException("Failed to get the parsed dataset")

        (ok, out) = out.wait_for_finish()
        if not ok:
            raise SocrataException("The dataset failed to validate")

        return out


class Publish(Collection):
    def __init__(self, auth):
        super(Publish, self).__init__(auth)
        self.revisions = Revisions(auth)
        self.uploads = Uploads(auth)
        self.configs = Configs(auth)

    def new(self, body):
        path = '{proto}{domain}/api/views'.format(
            proto = self.auth.proto,
            domain = self.auth.domain
        )
        return post(
            path,
            auth = self.auth,
            data = json.dumps(body)
        )

    def delete(self, id):
        path = '{proto}{domain}/api/views/{ff}'.format(
            proto = self.auth.proto,
            domain = self.auth.domain,
            ff = id
        )
        response = requests.delete(
            path,
            headers = gen_headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        )

        if response.status_code in [200, 201, 202]:
            return (True, {})
        else:
            return (False, response)


    def create(self, **kwargs):
        return Create(self, **kwargs)

    # Eventually...
    # def append(self, **kwargs):
    #     return Append(self, **kwargs)

    # def replace(self, **kwargs):
    #     return Replace(self, **kwargs)
