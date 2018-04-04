import json
import io
import webbrowser
from socrata.http import post, patch, get, noop
from socrata.resource import Resource, Collection, ChildResourceSpec
from socrata.input_schema import InputSchema
from socrata.builders.parse_options import ParseOptionBuilder

class Sources(Collection):
    def path(self):
        return 'https://{domain}/api/publishing/v1/source'.format(
            domain = self.auth.domain
        )

    def lookup(self, source_id):
        """
        Lookup a source

        Args:
        ```
            source_id (int): The id
        ```

        Returns:
        ```
            result (bool, dict | Source): The Source resulting from this API call, or an error
        ```
        """
        return self._subresource(Source, get(
            self.path() + '/' + str(source_id),
            auth = self.auth
        ))

    def create_upload(self, filename):
        """
        Create a new source. Takes a `body` param, which must contain a `filename`
        of the file.

        Args:
        ```
            filename (str): The name of the file you are uploading
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            (ok, upload) = revision.create_upload('foo.csv')
        ```

        """
        return self._subresource(Source, post(
            self.path(),
            auth = self.auth,
            data = json.dumps({
                'source_type' : {
                    'type': 'upload',
                    'filename': filename
                }
            })
        ))

class Source(Resource, ParseOptionBuilder):
    """
    Uploads bytes into the source. Requires content_type argument
    be set correctly for the file handle. It's advised you don't
    use this method directly, instead use one of the csv, xls, xlsx,
    or tsv methods which will correctly set the content_type for you.
    """
    def bytes(self, uri, file_handle, content_type):
        return self._mutate(post(
            self.path(uri),
            auth = self.auth,
            data = file_handle,
            headers = {
                'content-type': content_type
            }
        ))

    def child_specs(self):
        return [
            ChildResourceSpec(
                self,
                'input_schemas',
                'input_schema_links',
                'schemas',
                InputSchema,
                'input_schema_id'
            )
        ]


    def blob(self, file_handle):
        """
        Uploads a Blob dataset. A blob is a file that will not be parsed as a data file,
        ie: an image, video, etc.


        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-blob.jpg', 'rb') as f:
                (ok, upload) = upload.blob(f)
        ```

        """
        source = self
        if self.attributes['parse_options']['parse_source']:
            (ok, cloned) = self.change_parse_option('parse_source').to(False).run()
            assert ok, cloned
            source = cloned

        return source.bytes(file_handle, "application/octet-stream")


    def csv(self, file_handle):
        """
        Upload a CSV, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-file.csv', 'rb') as f:
                (ok, upload) = upload.csv(f)
        ```
        """
        return self.bytes(file_handle, "text/csv")

    def xls(self, file_handle):
        """
        Upload an XLS, returns the new input schema

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-file.xls', 'rb') as f:
                (ok, upload) = upload.xls(f)
        ```
        """
        return self.bytes(file_handle, "application/vnd.ms-excel")

    def xlsx(self, file_handle):
        """
        Upload an XLSX, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-file.xlsx', 'rb') as f:
                (ok, upload) = upload.xlsx(f)
        ```
        """
        return self.bytes(file_handle, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    def tsv(self, file_handle):
        """
        Upload a TSV, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-file.tsv', 'rb') as f:
                (ok, upload) = upload.tsv(f)
        ```
        """
        return self.bytes(file_handle, "text/tab-separated-values")

    def shapefile(self, file_handle):
        """
        Upload a Shapefile, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-shapefile-archive.zip', 'rb') as f:
                (ok, upload) = upload.shapefile(f)
        ```
        """
        return self.bytes(file_handle, "application/zip")

    def kml(self, file_handle):
        """
        Upload a KML file, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-kml-file.kml', 'rb') as f:
                (ok, upload) = upload.kml(f)
        ```
        """
        return self.bytes(file_handle, "application/vnd.google-earth.kml+xml")


    def geojson(self, file_handle):
        """
        Upload a geojson file, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            with open('my-geojson-file.geojson', 'rb') as f:
                (ok, upload) = upload.geojson(f)
        ```
        """
        return self.bytes(file_handle, "application/vnd.geo+json")


    def df(self, dataframe):
        """
        Upload a pandas DataFrame, returns the new source.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, Source | dict): Returns an API Result; the new Source or an error response
        ```

        Examples:
        ```python
            import pandas
            df = pandas.read_csv('test/fixtures/simple.csv')
            (ok, upload) = upload.df(df)
        ```
        """
        s = io.StringIO()
        dataframe.to_csv(s, index=False)
        return self.bytes(bytes(s.getvalue().encode()),"text/csv")

    def add_to_revision(self, uri, revision):
        """
        Associate this Source with the given revision.
        """
        return self._clone(patch(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'revision': {
                    'fourfour': revision.attributes['fourfour'],
                    'revision_seq': revision.attributes['revision_seq']
                }
            })
        ))

    def update(self, uri, body):
        return self._clone(post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        ))

    def show_input_schema(self, uri, input_schema_id):
        (ok, res) = result = get(
            self.path(uri.format(input_schema_id = input_schema_id)),
            auth = self.auth
        )

        if ok:
            return self._subresource(InputSchema, result)
        return result

    def get_latest_input_schema(self):
        return max(self.input_schemas, key = lambda s: s.attributes['id'])

    def wait_for_finish(self, progress = noop, timeout = None, sleeptime = 1):
        """
        Wait for this dataset to finish transforming and validating. Accepts a progress function
        and a timeout.
        """
        return self._wait_for_finish(
            is_finished = lambda m: m.attributes['finished_at'],
            is_failed = lambda m: m.attributes['failed_at'],
            progress = progress,
            timeout = timeout,
            sleeptime = sleeptime
        )

    def ui_url(self):
        """
        This is the URL to the landing page in the UI for the sources

        Returns:
        ```
            url (str): URL you can paste into a browser to view the source UI
        ```
        """
        if not self.parent:
            raise NotImplementedError("UI for revisionless sources is not implemented (yet). Sorry!")

        revision = self.parent
        return revision.ui_url() +  '/sources/{source_id}/preview'.format(
            source_id = self.attributes['id']
        )

    def open_in_browser(self):
        """
        Open this source in your browser, this will open a window
        """
        webbrowser.open(self.ui_url(), new = 2)
