import os
from socrata.authorization import Authorization
from socrata import Socrata
import logging
import unittest

# Use cookie auth if SOCRATA_COOKIE is set, otherwise fall back to username/password
if os.environ.get('SOCRATA_COOKIE'):
    from http.cookies import SimpleCookie

    domain = os.environ['SOCRATA_DOMAIN']
    authCookie = os.environ['SOCRATA_COOKIE']

    cookies = SimpleCookie()
    cookie_text = authCookie.strip()
    cookies.load(cookie_text)
    csrf_morsel = cookies.get("socrata-csrf-token")
    if csrf_morsel is None:
        raise RuntimeError(
            "Cookie missing socrata-csrf-token. "
            f"Parsed cookie keys: {list(cookies.keys())}"
        )
    cookieHeader = {
        "Cookie": authCookie,
        "X-CSRF-Token": csrf_morsel.value,
    }

    auth = Authorization(domain, cookies=cookieHeader)
else:
    auth = Authorization(
      os.environ['SOCRATA_DOMAIN'],
      username=os.environ['SOCRATA_USERNAME'],
      password=os.environ['SOCRATA_PASSWORD']
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
