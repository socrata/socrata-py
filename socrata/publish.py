from socrata.resource import Collection
from socrata.uploads import Uploads
from socrata.configs import Configs
from socrata.views import Views
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

    def csv(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type CSV and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.csv(file), *args, **kwargs)

    def xls(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type XLS and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.xls(file), *args, **kwargs)

    def xlsx(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type XLSX and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.xlsx(file), *args, **kwargs)

    def tsv(self, file, *args, **kwargs):
        """
        Create a revision on that view, then upload a file of
        type TSV and wait for validation to complete. Returns
        an `OutputSchema` which, when applied, will by applied
        to the view.
        """
        return self.run(file, lambda upload: upload.tsv(file), *args, **kwargs)

def get_filename(data, filename):
    if (filename is None) and getattr(data, 'name', False):
        filename = data.name
    elif filename is None:
        raise SocrataException("When creating without a file handle, you must include the filename as the second argument")
    return filename


class Create(Operation):
    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)

        (ok, view) = self.publish.views.create(self.properties)
        if not ok:
            raise SocrataException("Failed to create the view", view)

        (ok, rev) = view.revisions.update()
        if not ok:
            raise SocrataException("Failed to create the revision", rev)

        (ok, upload) = rev.create_upload({'filename': filename})
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

        return (view, rev, out)

class Update(Operation):
    def run(self, file, put_bytes):
        raise NotImplemented("nope!")


class ConfiguredJob(Operation):
    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)

        (ok, rev) = self.properties['view'].revisions.create_using_config(
            self.properties['config']
        )
        if not ok:
            raise SocrataException("Failed to create the revision", rev)

        (ok, upload) = rev.create_upload({'filename': filename})
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

        (ok, job) = rev.apply(output_schema = out)
        if not ok:
            raise SocrataException("Failed to apply the change", job)
        return (rev, job)


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
        self.views = Views(auth)
        self.uploads = Uploads(auth)
        self.configs = Configs(auth)

    def using_config(self, config_name, view):
        """
        Not sure yet
        """
        (ok, config) = result = self.configs.lookup(config_name)
        if not ok:
            raise SocrataException("Failed to lookup config %s" % config_name, result)
        return ConfiguredJob(self, view = view, config = config)


    def create(self, **kwargs):
        """
        Shortcut to create a dataset. Returns a `Create` object,
        which contains functions which will create a view, upload
        your file, and validate data quality in one step.
        """
        return Create(self, **kwargs)

    def update(self, **kwargs):
        return Update(self, **kwargs)

    # def replace(self, **kwargs):
    #     return Replace(self, **kwargs)
