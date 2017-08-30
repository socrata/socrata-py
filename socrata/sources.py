import json
import io
from copy import copy
from socrata.http import post, patch, get
from socrata.resource import Resource, Collection
from socrata.input_schema import InputSchema

class ParseOptionChange(object):
    def __init__(self, name, source):
        self._name = name
        self._source = source

    def to(self, value):
        self._source.parse_option_changes.append((self._name, value))
        return self._source


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

class Source(Resource):
    def __init__(self, *args, **kwargs):
        super(Source, self).__init__(*args, **kwargs)
        self.parse_option_changes = []
    """
    Uploads bytes into the source. Requires content_type argument
    be set correctly for the file handle. It's advised you don't
    use this method directly, instead use one of the csv, xls, xlsx,
    or tsv methods which will correctly set the content_type for you.
    """
    def bytes(self, uri, file_handle, content_type):
        return self._subresource(InputSchema, post(
            self.path(uri),
            auth = self.auth,
            data = file_handle,
            headers = {
                'content-type': content_type
            }
        ))

    def csv(self, file_handle):
        """
        Upload a CSV, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`
        ```

        Returns:
        ```
            result (bool, InputSchema | dict): Returns an API Result; the new InputSchema or an error response
        ```

        Examples:
        ```python
            with open('my-file.csv', 'rb') as f:
                (ok, input_schema) = upload.csv(f)
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
            result (bool, InputSchema | dict): Returns an API Result; the new InputSchema or an error response
        ```

        Examples:
        ```python
            with open('my-file.xls', 'rb') as f:
                (ok, input_schema) = upload.xls(f)
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
            result (bool, InputSchema | dict): Returns an API Result; the new InputSchema or an error response
        ```

        Examples:
        ```python
            with open('my-file.xlsx', 'rb') as f:
                (ok, input_schema) = upload.xlsx(f)
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
            result (bool, InputSchema | dict): Returns an API Result; the new InputSchema or an error response
        ```

        Examples:
        ```python
            with open('my-file.tsv', 'rb') as f:
                (ok, input_schema) = upload.tsv(f)
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
            result (bool, InputSchema | dict): Returns an API Result; the new InputSchema or an error response
        ```

        Examples:
        ```python
            with open('my-shapefile-archive.zip', 'rb') as f:
                (ok, input_schema) = upload.shapefile(f)
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
            result (bool, InputSchema | dict): Returns an API Result; the new InputSchema or an error response
        ```

        Examples:
        ```python
            import pandas
            df = pandas.read_csv('test/fixtures/simple.csv')
            (ok, input_schema) = upload.df(df)
        ```
        """
        s = io.StringIO()
        dataframe.to_csv(s, index=False)
        return self.bytes(bytes(s.getvalue().encode()),"text/csv")

    def add_to_revision(self, uri, revision):
        """
        Associate this Source with the given revision.
        """
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
            self._on_response(res)
            return (ok, self)
        return result

    def update(self, uri, body):
        (ok, res) = result = patch(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        )
        if ok:
            self._on_response(res)
            return (ok, self)
        return result

    def show_input_schema(self, uri, input_schema_id):
        (ok, res) = result = get(
            self.path(uri.format(input_schema_id = input_schema_id)),
            auth = self.auth
        )

        if ok:
            return self._subresource(InputSchema, result)
        return result

    def latest_input(self):
        return self.show_input_schema(max([s['id'] for s in self.attributes['schemas']]))

    def change_parse_option(self, name):
        """
        Change a parse option on the source.

        If there are not yet bytes uploaded, these parse options will be used
        in order to parse the file.

        If there are already bytes uploaded, this will trigger a re-parsing of
        the file, and consequently a new InputSchema will be created. You can call
        `source.latest_input()` to get the newest one.

        Parse options are:
        header_count (int): the number of rows considered a header
        column_header (int): the one based index of row to use to generate the header
        encoding (string): defaults to guessing the encoding, but it can be explicitly set
        column_separator (string): For CSVs, this defaults to ",", and for TSVs "\t", but you can use a custom separator
        quote_char (string): Character used to quote values that should be escaped. Defaults to "\""

        Args:
        ```
            name (string): One of the options above, ie: "column_separator" or "header_count"
        ```

        Returns:
        ```
            change (ParseOptionChange): implements a `.to(value)` function which you call to set the value
        ```

        For our example, assume we have this dataset

        ```
        This is my cool dataset
        A, B, C
        1, 2, 3
        4, 5, 6
        ```

        We want to say that the first 2 rows are headers, and the second of those 2
        rows should be used to make the column header. We would do that like so:

        Examples:
        ```python
            (ok, source) = source\
            .change_parse_option('header_count').to(2)\
            .change_parse_option('column_header').to(2)\
            .run()
        ```

        """
        return ParseOptionChange(name, self)

    def run(self):
        parse_options = copy(self.attributes['parse_options'])
        parse_options.update({key: value for key, value in self.parse_option_changes})

        return self.update({
            'parse_options': parse_options
        })
