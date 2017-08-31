import json
import io
from socrata.http import post, patch, get
from socrata.resource import Resource, Collection, ChildResourceSpec
from socrata.input_schema import InputSchema
from socrata.builders.parse_options import ParseOptionBuilder

class Sources(Collection):
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
        path = 'https://{domain}/api/publishing/v1/source'.format(
            domain = self.auth.domain
        )
        return self._subresource(Source, post(
            path,
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
        return self._mutate(patch(
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
        return self._mutate(patch(
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

