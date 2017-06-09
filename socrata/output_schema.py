import time
import json
from socrata.resource import Resource
from socrata.job import Job
from socrata.configs import Config
from socrata.http import noop, put, get, post

class TimeoutException(Exception):
    pass

class OutputSchema(Resource):
    def apply(self):
        """
        Apply the Revision that this OutputSchema is associated
        with to the View. Returns a Job which will contain progress
        information.
        """
        uri = self.parent.parent.parent.show_uri + '/apply'
        return self.subresource(Job, put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({'output_schema_id': self.attributes['id']})
        ))

    def build_config(self, uri, name, data_action):
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
        return any([column['transform']['failed_at'] for column in self.attributes['output_columns']])

    def wait_for_finish(self, progress = noop, timeout = None):
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

    def munge_row(self, row):
        row = row['row']
        columns = sorted(self.attributes['output_columns'], key = lambda oc: oc['position'])
        field_names = [oc['field_name'] for oc in columns]
        return {k: v for k, v in zip(field_names, row)}

    def rows(self, uri, offset = 0, limit = 500):
        ok, resp = get(
            self.path(uri),
            params = {'limit': limit, 'offset': offset},
            auth = self.auth
        )

        if not ok:
            return ok, resp

        rows = resp[1:]

        return (ok, [self.munge_row(row) for row in rows])
