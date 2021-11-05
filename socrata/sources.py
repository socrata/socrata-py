import json
import io
import webbrowser
import types
from time import sleep
from socrata.http import post, put, patch, get, noop, UnexpectedResponseException
from socrata.resource import Resource, Collection, ChildResourceSpec
from socrata.input_schema import InputSchema
from socrata.builders.parse_options import ParseOptionBuilder
from socrata.lazy_pool import LazyThreadPoolExecutor
from threading import Lock
from requests.exceptions import RequestException


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
            Source: Returns the new Source The Source resulting from this API call, or an error
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
            Source: Returns the new Source
        ```

        Examples:
        ```python
            upload = revision.create_upload('foo.csv')
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


class ChunkIterator(object):
    def __init__(self, filelike, chunk_size):
        self._filelike = filelike
        self.lock = Lock()
        self._chunk_size = chunk_size
        self.seq_num = 0
        self.byte_offset = 0

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            read = self._filelike.read(self._chunk_size)
            if not read:
                raise StopIteration

            this_seq = self.seq_num
            this_byte_offset = self.byte_offset
            self.seq_num = self.seq_num + 1
            self.byte_offset = self.byte_offset + len(read)
            return (this_seq, this_byte_offset, self.byte_offset, read)

    def next(self):
        return self.__next__()

class FileLikeGenerator(object):
    def __init__(self, gen):
        self.gen = gen
        self.done = False

    def read(self, how_much):
        if self.done:
            return None

        buf = []
        consumed = 0
        while consumed < how_much:
            try:
                chunk = next(self.gen)
                consumed += len(chunk)
                buf.append(chunk)
            except StopIteration:
                self.done = True
                break

        return b''.join(buf)


class Source(Resource, ParseOptionBuilder):
    def initiate(self, uri, content_type):
        return post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({ 'content_type': content_type })
        )

    def chunk(self, uri, seq_num, byte_offset, bytes):
        return post(
            self.path(uri).format(seq_num=seq_num, byte_offset=byte_offset),
            auth = self.auth,
            data = bytes,
            headers = { 'content-type': 'application/octet-stream' }
        )

    def commit(self, uri, seq_num, byte_offset):
        return post(
            self.path(uri).format(seq_num=seq_num, byte_offset=byte_offset),
            auth = self.auth
        )


    def _chunked_bytes(self, file_or_string_or_bytes_or_generator, content_type, **kwargs):

        if type(file_or_string_or_bytes_or_generator) is str:
            file_handle = io.StringIO(file_or_string_or_bytes_or_generator)
        elif type(file_or_string_or_bytes_or_generator) is bytes:
            file_handle = io.BytesIO(file_or_string_or_bytes_or_generator)
        elif isinstance(file_or_string_or_bytes_or_generator, types.GeneratorType):
            file_handle = FileLikeGenerator(file_or_string_or_bytes_or_generator)
        elif hasattr(file_or_string_or_bytes_or_generator, 'read'):
            file_handle = file_or_string_or_bytes_or_generator
        else:
            raise ValueError("The thing to upload must be a file, string, bytes, or generator which yields bytes")


        init = self.initiate(content_type)
        chunk_size = init['preferred_chunk_size']
        parallelism = init['preferred_upload_parallelism']
        max_retries = kwargs.get('max_retries', 5)
        backoff_seconds = kwargs.get('backoff_seconds', 2)

        def sendit(chunk, attempts = 0):
            (seq_num, byte_offset, end_byte_offset, bytes) = chunk
            try:
                self.chunk(seq_num, byte_offset, bytes)
            except RequestException as e:
                return retry(chunk, e, attempts)
            except UnexpectedResponseException as e:
                if 500 <= e.status <= 599:
                    return retry(chunk, e, attempts)
                else:
                    raise e

            return (seq_num, byte_offset, end_byte_offset)

        def retry(chunk, e, attempts):
            if attempts < max_retries:
                attempts = attempts + 1
                sleep(attempts * attempts * backoff_seconds)
                return sendit(chunk, attempts)
            else:
                raise e


        pool = LazyThreadPoolExecutor(parallelism)
        results = [r for r in pool.map(sendit, ChunkIterator(file_handle, chunk_size))]
        (seq_num, byte_offset, end_byte_offset) = sorted(results, key=lambda x: x[0])[-1]
        self.commit(seq_num, end_byte_offset)
        return self.show()


    """
    Uploads bytes into the source. Requires content_type argument
    be set correctly for the file handle. It's advised you don't
    use this method directly, instead use one of the csv, xls, xlsx,
    or tsv methods which will correctly set the content_type for you.
    """
    def bytes(self, uri, file_handle, content_type, **kwargs):
        # This is just for backwards compat
        self._chunked_bytes(file_handle, content_type, **kwargs)


    def load(self, uri = None):
        """
        Forces the source to load, if it's a view source.

        Returns:
        ```
            Source: Returns the new Source
        ```
        """
        return self._mutate(put(
            self.path(uri or (self.links['show'] + "/load")),
            auth = self.auth,
            data = {},
            headers = {
                'content-type': 'application/json'
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


    def blob(self, file_handle, **kwargs):
        """
        Uploads a Blob dataset. A blob is a file that will not be parsed as a data file,
        ie: an image, video, etc.


        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-blob.jpg', 'rb') as f:
                upload = upload.blob(f)
        ```

        """
        source = self
        if self.attributes['parse_options']['parse_source']:
            source = self.change_parse_option('parse_source').to(False).run()
        return source._chunked_bytes(file_handle, "application/octet-stream", **kwargs)


    def csv(self, file_handle, **kwargs):
        """
        Upload a CSV, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-file.csv', 'rb') as f:
                upload = upload.csv(f)
        ```
        """
        return self._chunked_bytes(file_handle, "text/csv", **kwargs)

    def xls(self, file_handle, **kwargs):
        """
        Upload an XLS, returns the new input schema

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-file.xls', 'rb') as f:
                upload = upload.xls(f)
        ```
        """
        return self._chunked_bytes(file_handle, "application/vnd.ms-excel", **kwargs)

    def xlsx(self, file_handle, **kwargs):
        """
        Upload an XLSX, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-file.xlsx', 'rb') as f:
                upload = upload.xlsx(f)
        ```
        """
        return self._chunked_bytes(file_handle, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", **kwargs)

    def tsv(self, file_handle, **kwargs):
        """
        Upload a TSV, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-file.tsv', 'rb') as f:
                upload = upload.tsv(f)
        ```
        """
        return self._chunked_bytes(file_handle, "text/tab-separated-values", **kwargs)

    def shapefile(self, file_handle, **kwargs):
        """
        Upload a Shapefile, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-shapefile-archive.zip', 'rb') as f:
                upload = upload.shapefile(f)
        ```
        """
        return self._chunked_bytes(file_handle, "application/zip", **kwargs)

    def kml(self, file_handle, **kwargs):
        """
        Upload a KML file, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-kml-file.kml', 'rb') as f:
                upload = upload.kml(f)
        ```
        """
        return self._chunked_bytes(file_handle, "application/vnd.google-earth.kml+xml", **kwargs)


    def geojson(self, file_handle, **kwargs):
        """
        Upload a geojson file, returns the new input schema.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            with open('my-geojson-file.geojson', 'rb') as f:
                upload = upload.geojson(f)
        ```
        """
        return self._chunked_bytes(file_handle, "application/vnd.geo+json", **kwargs)


    def df(self, dataframe, **kwargs):
        """
        Upload a pandas DataFrame, returns the new source.

        Args:
        ```
            file_handle: The file handle, as returned by the python function `open()`

            max_retries (integer): Optional retry limit per chunk in the upload. Defaults to 5.
            backoff_seconds (integer): Optional amount of time to backoff upon a chunk upload failure. Defaults to 2.
        ```

        Returns:
        ```
            Source: Returns the new Source
        ```

        Examples:
        ```python
            import pandas
            df = pandas.read_csv('test/fixtures/simple.csv')
            upload = upload.df(df)
        ```
        """
        s = io.StringIO()
        dataframe.to_csv(s, index=False)
        return self._chunked_bytes(bytes(s.getvalue().encode()),"text/csv", **kwargs)

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
        res = get(
            self.path(uri.format(input_schema_id = input_schema_id)),
            auth = self.auth
        )

        return self._subresource(InputSchema, res)

    def get_latest_input_schema(self):
        self.wait_for_schema()
        return max(self.input_schemas, key = lambda s: s.attributes['id'])

    def wait_for_schema(self, progress = noop, timeout = 43200, sleeptime = 1):
        """
        Wait for this data source to have at least one schema present. Accepts a progress function
        and a timeout.

        Default timeout is 12 hours
        """
        return self._wait_for_finish(
            is_finished = lambda m: len(m.attributes['schemas']) > 0,
            is_failed = lambda m: m.attributes['failed_at'],
            progress = progress,
            timeout = timeout,
            sleeptime = sleeptime
        )

    def wait_for_finish(self, progress = noop, timeout = 43200, sleeptime = 1):
        """
        Wait for this data source to finish transforming and validating. Accepts a progress function
        and a timeout.

        Default timeout is 12 hours
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
