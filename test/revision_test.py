import unittest
from socrata.publish import Publish
from socrata.authorization import Authorization
from test.auth import auth, TestCase


class TestPublish(TestCase):
    def test_replace_revision(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['action']['type'], 'replace')

    def test_update_revision(self):
        (ok, r) = self.view.revisions.create_update_revision()
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['action']['type'], 'update')

    def test_list_operations(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        assert 'show' in r.list_operations(), r
        assert 'create_upload' in r.list_operations(), r

    def test_show_revision(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        (ok, rev) = r.show()
        self.assertTrue(ok, rev)

    def test_create_upload(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        (ok, upload) = r.create_upload({'filename': 'foo.csv'})
        self.assertTrue(ok, upload)
