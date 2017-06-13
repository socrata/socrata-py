import json
import requests
from socrata.http import post, put, delete
from socrata.resource import Collection, Resource
from socrata.uploads import Upload
from socrata.job import Job
import webbrowser

class Revisions(Collection):
    def __init__(self, view):
        self.auth = view.auth
        self.view = view


    def path(self):
        fourfour = self.view.attributes['id']
        return 'https://{domain}/api/publishing/v1/revision/{fourfour}'.format(
            domain = self.auth.domain,
            fourfour = fourfour
        )

    def _create(self, action_type):
        """
        Create a revision for the given dataset.
        """
        return self._subresource(Revision, post(
            self.path(),
            auth = self.auth,
            data = json.dumps({
                'action': {
                    'type': action_type
                }
            })
        ))

    def replace(self):
        return self._create('replace')

    def update(self):
        return self._create('update')

    def metadata(self):
        return self._create('metadata')


    def create_using_config(self, config):
        """
        Create a revision for the given dataset.
        """
        return self._subresource(Revision, post(
            self.path(),
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
        return self._subresource(Upload, post(
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

    def is_metadata_revision(self):
        """
        Whether or not this revision will only change the dataset's metadata
        """
        return self.attributes['action']['type'] == 'metadata'

    def apply(self, uri, output_schema = None):
        # We ignore any output schemas passed in if this revision only cares
        # about metadata
        if self.is_metadata_revision():
            output_schema = None

        if output_schema:
            (ok, output_schema) = result = output_schema.wait_for_finish()
            if not ok:
                return result

        body = {}

        if output_schema:
            body.update({
                'output_schema_id': output_schema.attributes['id']
            })
        """
        Apply the Revision to the view that it was opened on
        """
        return self._subresource(Job, put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        ))

    def ui_url(self):
        """
        This is the URL to the landing page in the UI for this revision
        """
        return "https://{domain}/d/{fourfour}/revisions/{seq}".format(
            domain = self.auth.domain,
            fourfour = self.attributes["fourfour"],
            seq = self.attributes["revision_seq"]
        )

    def open_in_browser(self):
        """
        Open this revision in your browser, this will open a window
        """
        webbrowser.open(self.ui_url(), new = 2)

