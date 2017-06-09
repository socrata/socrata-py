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
        """
        Create a revision on that view, then upload a file of
        type CSV and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.csv(file))

    def xls(self, file):
        """
        Create a revision on that view, then upload a file of
        type XLS and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.xls(file))

    def xlsx(self, file):
        """
        Create a revision on that view, then upload a file of
        type XLSX and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.xlsx(file))

    def tsv(self, file):
        """
        Create a revision on that view, then upload a file of
        type TSV and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
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

class ConfiguredJob(Operation):
    def run(self, file, put_bytes):
        (ok, rev) = self.publish.revisions.create_using_config(
            self.properties['fourfour'],
            self.properties['config']
        )
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

        (ok, job) = out.apply()
        if not ok:
            raise SocrataException("Failed to apply the change", job)
        return job


class Publish(Collection):
    """
    Top level publishing object.

    All functions making HTTP calls return a result tuple, where the first element in the
    tuple is whether or not the call succeeded, and the second element is the returned
    object if it was a success, or a dictionary containing the error response if the call
    failed. 2xx responses are considered successes. 4xx and 5xx responses are considered failures.
    In the event of a socket hangup, an exception is raised.
    """
    def __init__(self, auth):
        """
        See the `Authorization` class docs for info on how to construct an auth object.
        """
        super(Publish, self).__init__(auth)
        self.revisions = Revisions(auth)
        self.uploads = Uploads(auth)
        self.configs = Configs(auth)

    def new(self, body):
        """
        Create a new Socrata view.
        """
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
        """
        Delete a Socrata view, given its view id
        """
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

    def using_config(self, name, fourfour):
        """
        Not sure yet
        """
        (ok, config) = result = self.configs.lookup(name)
        if not ok:
            raise SocrataException("Failed to lookup config %s" % name, result)
        return ConfiguredJob(self, fourfour = fourfour, config = config)


    def create(self, **kwargs):
        """
        Shortcut to create a dataset. Returns a `Create` object,
        which contains functions which will create a view, upload
        your file, and validate data quality in one step.
        """
        return Create(self, **kwargs)

    # Eventually...
    # def append(self, **kwargs):
    #     return Append(self, **kwargs)

    # def replace(self, **kwargs):
    #     return Replace(self, **kwargs)
