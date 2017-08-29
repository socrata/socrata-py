import json
import requests
from socrata.http import post, put, delete, get
from socrata.resource import Collection, Resource
from socrata.sources import Source
from socrata.job import Job
from socrata.revisions import Revisions
import webbrowser
from socrata.http import gen_headers, post


class CoreResource(Resource):
    def _on_response(self, response):
        self.attributes = response

class Views(Collection):
    def path(self):
        return '{proto}{domain}/api/views'.format(
            proto = self.auth.proto,
            domain = self.auth.domain
        )

    def lookup(self, fourfour):
        """
        Lookup the view by ID

        Args:
        ```
            fourfour (str): The view's identifier, ex: abcd-1234
        ```
        Returns:
        ```
            result (bool, Revision | dict): Returns an API Result; the View or an error response
        ```
        """
        return self._subresource(View, get(
            self.path() + '/' + fourfour,
            auth = self.auth
        ))


class View(CoreResource):
    def __init__(self, *args, **kwargs):
        super(CoreResource, self).__init__(*args, **kwargs)
        self.revisions = Revisions(self.attributes['id'], self.auth)

    def delete(self):
        """
        Delete a Socrata view, given its view id
        """
        path = '{proto}{domain}/api/views/{ff}'.format(
            proto = self.auth.proto,
            domain = self.auth.domain,
            ff = self.attributes['id']
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

    def ui_url(self):
        """
        This is the URL to the landing page in the UI for this revision
        """
        return "https://{domain}/d/{fourfour}".format(
            domain = self.auth.domain,
            fourfour = self.attributes["id"]
        )

    def open_in_browser(self):
        """
        Open this revision in your browser, this will open a window
        """
        webbrowser.open(self.ui_url(), new = 2)
