import unittest
from src.publish import Publish
from src.authorization import Authorization
from test.auth import auth, fourfour

class TestPublish(unittest.TestCase):
    def test_create_revision(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        self.assertTrue(ok)

    def test_list_operations(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        assert 'show' in r.list_operations()
        assert 'create_upload' in r.list_operations()

    def test_show_revision(self):
        p = Publish(auth)
        (ok, r) = p.revisions.create(fourfour)
        self.assertTrue(ok)

        (ok, rev) = r.show()
        self.assertTrue(ok)
