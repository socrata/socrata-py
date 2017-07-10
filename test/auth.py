import os
from socrata.authorization import Authorization
from socrata.publish import Publish
import logging
import unittest

auth = Authorization(
  os.environ['SOCRATA_DOMAIN'],
  os.environ['SOCRATA_USERNAME'],
  os.environ['SOCRATA_PASSWORD']
)

if auth.domain == 'localhost':
    auth.live_dangerously()

import logging


logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class TestCase(unittest.TestCase):
    def create_rev(self):
        p = Publish(auth)
        (ok, r) = self.view.revisions.create_update_revision()
        assert ok
        self.rev = r
        return r

    def create_input_schema(self, rev = None):
        if not rev:
            rev = self.create_rev()
        (ok, source) = rev.create_upload('foo.csv')
        assert ok
        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = source.csv(f)
            assert ok
            return input_schema

    def create_output_schema(self, input_schema = None):
        if not input_schema:
            input_schema = self.create_input_schema()

        (ok, output_schema) = input_schema.transform({
            'output_columns': [
                {
                    "field_name": "b",
                    "display_name": "b, but as a number",
                    "position": 0,
                    "description": "b but with a bunch of errors",
                    "transform": {
                        "transform_expr": "to_number(b)"
                    }
                }
            ]}
        )
        assert ok
        return output_schema

    def setUp(self):
        self.pub = Publish(auth)
        (ok, v) = self.pub.views.create({'name': 'test-view'})
        assert ok, v
        self.view = v

    def tearDown(self):
        if getattr(self, 'rev', False):
            self.rev.discard()
        self.view.delete()
