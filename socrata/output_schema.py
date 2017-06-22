import time
import json
from copy import deepcopy
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


    def _get_rows(self, uri, offset, limit):
        ok, resp = get(
            self.path(uri),
            params = {'limit': limit, 'offset': offset},
            auth = self.auth
        )

        if not ok:
            return ok, resp

        rows = resp[1:]

        return (ok, [self._munge_row(row) for row in rows])

    def rows(self, uri, offset = 0, limit = 500):
        """
        Get the rows for this OutputSchema. Acceps `offset` and `limit` params
        for paging through the data.
        """
        return self._get_rows(uri, offset, limit)


    def schema_errors(self, uri, offset = 0, limit = 500):
        """
        Get the errors that resulted in transforming into this output schema.
        Accepts `offset` and `limit` params
        """
        return self._get_rows(uri, offset, limit)

    def schema_errors_csv(self):
        """
        Get the errors that results in transforming into this output schema
        as a CSV stream.

        Note that this returns an (ok, Reponse) tuple, where Reponse
        is a python requests Reponse object
        """
        return get(
            self.path(self.schema_errors_uri),
            auth = self.auth,
            headers = {'accept': 'text/csv', 'content-type': 'text/csv'},
            stream = True
        )

    def validate_row_id(self, uri, field_name):
        print(self.attributes)
        output_column = [oc for oc in self.attributes['output_columns'] if oc['field_name'] == field_name]
        if len(output_column):
            [output_column] = output_column
            transform_id = output_column['transform']['id']

            return get(
                self.path(uri.format(transform_id = transform_id)),
                auth = self.auth
            )
        else:
            return (False, {"reason": "No column with field_name = %s" % field_name})


    def set_row_id(self, field_name = None):
        desired_schema = deepcopy(self.attributes['output_columns'])

        for oc in desired_schema:
            oc['is_primary_key'] = (oc['field_name'] == field_name)

        return self.parent.transform({'output_columns': desired_schema})

