import time
import requests
import json
from socrata.resource import Resource
from socrata.upsert_job import UpsertJob
from socrata.http import headers, respond, noop

class TimeoutException(Exception):
    pass

class OutputSchema(Resource):
    def apply(self):
        uri = self.parent.parent.parent.show_uri + '/apply'
        return self.subresource(UpsertJob, respond(requests.put(
            self.path(uri),
            headers = headers(),
            data = json.dumps({'output_schema_id': self.attributes['id']}),
            auth = self.auth.basic,
            verify = self.auth.verify
        )))

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
        ok, resp = respond(requests.get(
            self.path(uri),
            params = {'limit': limit, 'offset': offset},
            headers = headers(),
            auth = self.auth.basic,
            verify = self.auth.verify
        ))

        if not ok:
            return ok, resp

        rows = resp[1:]

        return (ok, [self.munge_row(row) for row in rows])
