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
            'metadata': {
                'name': 'new revision name'
            }
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

    def test_get_output_schema(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        input_schema = self.create_input_schema(rev = r)

        r.set_output_schema(input_schema.get_latest_output_schema().attributes['id'])

        (ok, output_schema) = r.get_output_schema()
        self.assertTrue(ok)
        self.assertTrue(output_schema != None)

    def test_get_plan(self):
        (ok, r) = self.view.revisions.create_replace_revision()
        self.assertTrue(ok)

        input_schema = self.create_input_schema(rev = r)

        r.set_output_schema(input_schema.get_latest_output_schema().attributes['id'])

        (ok, plan) = r.plan()
        self.assertTrue(ok)
        expected = set(['prepare_draft_for_import', 'set_schema', 'apply_metadata', 'upsert_task', 'set_display_type', 'publish', 'set_permission', 'wait_for_replication'])
        actual   = set([step['type'] for step in plan])
        self.assertTrue(set.issubset(expected, actual))

    def test_create_from_dataset(self):
        with open('test/fixtures/simple.csv', 'rb') as file:
            # boilerplate
            input_schema = self.create_input_schema()
            (ok, job) = self.rev.apply(output_schema = input_schema.get_latest_output_schema())
            (ok, job) = job.wait_for_finish()
            assert ok, job


            (ok, view) = self.pub.views.lookup(self.rev.attributes['fourfour'])

            (ok, rev) = view.revisions.create_replace_revision()
            self.assertTrue(ok, rev)

            (ok, rev) = rev.update({
                'metadata': {
                    'description': 'new dataset description'
                }
            })
            self.assertTrue(ok, rev)

            (ok, source) = rev.source_from_dataset()
            self.assertTrue(ok, source)

            output_schema = source.get_latest_input_schema().get_latest_output_schema()

            (ok, new_output) = output_schema\
                .change_column_metadata('a', 'description').to('meh')\
                .change_column_metadata('b', 'display_name').to('bbbb')\
                .change_column_metadata('c', 'field_name').to('ccc')\
                .run()

            [a, b, c] = new_output.attributes['output_columns']
            self.assertEqual(a['description'], 'meh')
            self.assertEqual(b['display_name'], 'bbbb')
            self.assertEqual(c['field_name'], 'ccc')

            self.assertEqual(rev.attributes['metadata']['description'], 'new dataset description')
