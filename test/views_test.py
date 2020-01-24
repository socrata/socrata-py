import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase


class TestViews(TestCase):
    def test_lookup_view(self):
        view = self.pub.views.lookup(self.rev.view_id())
        self.assertEqual(view.attributes['name'], 'test-view')


