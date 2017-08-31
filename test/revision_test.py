import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase


class TestSocrata(TestCase):
    def test_create_revision_and_view(self):
        (ok, rev) = self.pub.new({
            'name': 'hi',
            'description': 'foo!',
            'metadata': {
                'lol': 'anything',
                'is': 'allowed here'

            }
        })

        self.assertEqual(rev.attributes['metadata']['name'], 'hi')
        self.assertEqual(rev.attributes['metadata']['description'], 'foo!')
        self.assertEqual(rev.attributes['metadata']['metadata']['lol'], 'anything')
        self.assertEqual(rev.attributes['metadata']['metadata']['is'], 'allowed here')

    def test_replace_revision(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['action']['type'], 'replace')

    def test_update_revision(self):
        (ok, r) = self.view.revisions.create_update_revision()
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['action']['type'], 'update')

    def test_mutate_revision(self):
        (ok, r) = self.view.revisions.create_update_revision()

        (ok, r) = r.update({
            'name': 'new revision name'
        })
        self.assertTrue(ok, r)
        self.assertEqual(r.attributes['metadata']['name'], 'new revision name')

    def test_list_revisions(self):
        (ok, r) = self.view.revisions.create_update_revision()
        assert ok, r
        (ok, r) = self.view.revisions.create_replace_revision()
        assert ok, r

        (ok, revs) = self.view.revisions.list()
        self.assertEqual(len(revs), 3)

    def test_lookup_revision(self):
        (ok, r) = self.view.revisions.create_update_revision()
        assert ok, r
        (ok, l) = self.view.revisions.lookup(1)
        assert ok, l
        self.assertEqual(l.attributes, r.attributes)


    def test_list_operations(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        assert 'show' in r.list_operations(), r
        assert 'create_source' in r.list_operations(), r

    def test_show_revision(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        (ok, rev) = r.show()
        self.assertTrue(ok, rev)

    def test_create_source(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        (ok, source) = r.create_upload('foo.csv')
        self.assertTrue(ok, source)

    def test_list_sources(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        (ok, source) = r.create_upload('foo.csv')
        self.assertTrue(ok, source)

        (ok, sources) = r.list_sources()
        self.assertEqual(len(sources), 1)
        self.assertEqual(source.attributes['id'], sources[0].attributes['id'])