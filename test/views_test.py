import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase


class TestViews(TestCase):
    def test_create_view(self):
        (ok, r) = self.pub.views.create({'name': 'foo'})
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['name'], 'foo')

    def test_lookup_view(self):
        (ok, r) = self.pub.views.create({'name': 'foo'})
        self.assertTrue(ok, r)
        (ok, view) = self.pub.views.lookup(r.attributes['id'])
        self.assertEqual(view.attributes['name'], 'foo')


