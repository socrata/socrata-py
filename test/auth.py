import os
from socrata.authorization import Authorization
from socrata import Socrata
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
        p = Socrata(auth)
        r = self.view.revisions.create_update_revision()
        self.rev = r
        return r

    def create_input_schema(self, rev = None, filename = 'simple.csv'):
        if not rev:
            rev = self.create_rev()
        source = rev.create_upload('foo.csv')
        with open('test/fixtures/%s' % filename, 'rb') as f:
            source = source.csv(f)
            return source.get_latest_input_schema()

    def create_output_schema(self, input_schema = None):
        if not input_schema:
            input_schema = self.create_input_schema()

        output_schema = input_schema.transform({
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
        return output_schema

    def setUp(self):
        self.pub = Socrata(auth)
        rev = self.pub.new({'name': 'test-view'})
        self.rev = rev
        view = self.pub.views.lookup(rev.attributes['fourfour'])
        self.view = view

    def tearDown(self):
        if getattr(self, 'rev', False):
            self.rev.discard()
        self.view.delete()
