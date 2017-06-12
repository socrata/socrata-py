import time
import json
from socrata.resource import Resource
from socrata.configs import Config
from socrata.http import noop, put, get, post

class TimeoutException(Exception):
    pass

class OutputSchema(Resource):
    def build_config(self, uri, name, data_action):
        """
        Create a new ImportConfig from this OutputSchema. See the API
        docs for what an ImportConfig is and why they're useful
        """
        (ok, res) = result = post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'name': name,
                'data_action': data_action
            })
        )
        if ok:
            return (ok, Config(self.auth, res, None))
        return result


    def any_failed(self):
        """
        Whether or not any transform in this output schema has failed
        """
        return any([column['transform']['failed_at'] for column in self.attributes['output_columns']])

    def wait_for_finish(self, progress = noop, timeout = None):
        """
        Wait for this dataset to finish transforming and validating. Accepts a progress function
        and a timeout.
        """
        started = time.time()
        while not self.attributes['completed_at']:
            current = time.time()
            if timeout and (current - started > timeout):
                raise TimeoutException("Timed out after %s seconds waiting for completion" % timeout)
            (ok, me) = self.show()
            progress(self)
            if not ok:
                return (ok, me)
            if self.any_failed():
                return (False, me)
            time.sleep(1)
        return (True, self)

    def _munge_row(self, row):
        row = row['row']
        columns = sorted(self.attributes['output_columns'], key = lambda oc: oc['position'])
        field_names = [oc['field_name'] for oc in columns]
        return {k: v for k, v in zip(field_names, row)}

    def rows(self, uri, offset = 0, limit = 500):
        """
        Get the rows for this OutputSchema. Acceps `offset` and `limit` params
        for paging through the data.
        """
        ok, resp = get(
            self.path(uri),
            params = {'limit': limit, 'offset': offset},
            auth = self.auth
        )

        if not ok:
            return ok, resp

        rows = resp[1:]

        return (ok, [self._munge_row(row) for row in rows])
