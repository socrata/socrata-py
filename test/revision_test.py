import unittest
from socrata.publish import Publish
from socrata.authorization import Authorization
from test.auth import auth, TestCase


class TestPublish(TestCase):
    def test_replace_revision(self):
        (ok, r) = self.view.revisions.replace()
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['action']['type'], 'replace')

    def test_update_revision(self):
        (ok, r) = self.view.revisions.update()
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['action']['type'], 'update')

    def test_list_operations(self):
        (ok, r) = self.view.revisions.replace()
        assert 'show' in r.list_operations(), r
        assert 'create_upload' in r.list_operations(), r

    def test_show_revision(self):
        (ok, r) = self.view.revisions.replace()
        self.assertTrue(ok)

        (ok, rev) = r.show()
        self.assertTrue(ok, rev)

    def test_create_upload(self):
        (ok, r) = self.view.revisions.replace()
        self.assertTrue(ok)

        (ok, upload) = r.create_upload({'filename': 'foo.csv'})
        self.assertTrue(ok, upload)

    def test_metadata_revision(self):
        (ok, r) = self.view.revisions.metadata()
        self.assertTrue(ok)
        self.assertEqual(r.attributes['action']['type'], 'metadata')

        (ok, rev) = r.metadata({
            'name': 'foo',
            'description': 'bar',
            'category': 'fun'
        })
        self.assertTrue(ok, rev)
        self.assertEqual(rev.attributes['metadata']['name'], 'foo')
        self.assertEqual(rev.attributes['metadata']['description'], 'bar')

    def test_apply_metadata_revision(self):
        (ok, r) = self.view.revisions.metadata()
        self.assertTrue(ok)
        self.assertEqual(r.attributes['action']['type'], 'metadata')

        (ok, rev) = r.metadata({
            'name': 'foo',
            'description': 'bar',
            'category': 'fun'
        })
        self.assertTrue(ok, rev)

        (ok, job) = rev.apply()
        self.assertTrue(ok, job)

        (ok, job) = job.wait_for_finish()
        self.assertTrue(ok, job)
