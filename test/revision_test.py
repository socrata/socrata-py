import unittest
from socrata.publish import Publish
from socrata.authorization import Authorization
from test.auth import auth, fourfour

class TestPublish(unittest.TestCase):
    def test_create_revision(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        self.assertTrue(ok, r)

    def test_list_operations(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        assert 'show' in r.list_operations(), r
        assert 'create_upload' in r.list_operations(), r

    def test_show_revision(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        self.assertTrue(ok)

        (ok, rev) = r.show()
        self.assertTrue(ok, rev)

    def test_create_upload(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        self.assertTrue(ok)

        (ok, upload) = r.create_upload({'filename': 'foo.csv'})
        self.assertTrue(ok, upload)

    def test_metadata_revision(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        self.assertTrue(ok)

        (ok, rev) = r.metadata({
            'name': 'foo',
            'description': 'bar',
            'category': 'fun'
        })
        self.assertTrue(ok, rev)
        self.assertEqual(rev.attributes['metadata']['name'], 'foo')
        self.assertEqual(rev.attributes['metadata']['description'], 'bar')
