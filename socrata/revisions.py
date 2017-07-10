import json
import requests
from socrata.http import post, put, delete, get
from socrata.resource import Collection, Resource
from socrata.sources import Source
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

    def list(self):
        return self._subresources(Revision, get(
            self.path(),
            auth = self.auth
        ))

    def create_replace_revision(self):
        return self._create('replace')

    def create_update_revision(self):
        return self._create('update')

    def lookup(self, revision_seq):
        return self._subresource(Revision, get(
            self.path() + '/' + str(revision_seq),
            auth = self.auth
        ))


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

    def create_upload(self, filename):
        """
        Create an source within this revision
        """
        return self.create_source({
            'type': 'upload',
            'filename': filename
        })

    def create_source(self, uri, source_type):
        return self._subresource(Source, post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'source_type' : source_type
            })
        ))

    def discard(self, uri):
        """
        Discard this open revision.
        """
        return delete(self.path(uri), auth = self.auth)


    def update(self, uri, meta):
        """
        Set the metadata to be applied to the view
        when this revision is applied
        """
        return self._mutate(put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({'metadata': meta}),
        ))

    def apply(self, uri, output_schema = None):
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
        result = self._subresource(Job, put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        ))

        self.show() # To mutate ourself and get the job to show up in our attrs

        return result

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
