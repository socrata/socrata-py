import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase


class TestSocrata(TestCase):
    def test_create_revision_and_view(self):
        rev = self.pub.new({
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
        r = self.view.revisions.create_replace_revision()
        self.assertEqual(r.attributes['action']['type'], 'replace')

    def test_update_revision(self):
        r = self.view.revisions.create_update_revision()
        self.assertEqual(r.attributes['action']['type'], 'update')

    def test_mutate_revision(self):
        r = self.view.revisions.create_update_revision()

        r = r.update({
            'metadata': {
                'name': 'new revision name'
            }
        })
        self.assertEqual(r.attributes['metadata']['name'], 'new revision name')

    def test_list_revisions(self):
        r = self.view.revisions.create_update_revision()
        r = self.view.revisions.create_replace_revision()
        revs = self.view.revisions.list()
        self.assertEqual(len(revs), 3)

    def test_lookup_revision(self):
        r = self.view.revisions.create_update_revision()
        l = self.view.revisions.lookup(1)
        self.assertEqual(l.attributes, r.attributes)


    def test_list_operations(self):
        r = self.view.revisions.create_replace_revision()
        assert 'show' in r.list_operations(), r
        assert 'create_source' in r.list_operations(), r

    def test_show_revision(self):
        r = self.view.revisions.create_replace_revision()
        rev = r.show()

    def test_create_source(self):
        r = self.view.revisions.create_replace_revision()
        source = r.create_upload('foo.csv')

    def test_list_sources(self):
        r = self.view.revisions.create_replace_revision()
        source = r.create_upload('foo.csv')
        sources = r.list_sources()
        self.assertEqual(len(sources), 1)
        self.assertEqual(source.attributes['id'], sources[0].attributes['id'])

    def test_get_output_schema(self):
        r = self.view.revisions.create_replace_revision()
        input_schema = self.create_input_schema(rev = r)
        r.set_output_schema(input_schema.get_latest_output_schema().attributes['id'])
        output_schema = r.get_output_schema()
        self.assertTrue(output_schema != None)

    def test_get_plan(self):
        r = self.view.revisions.create_replace_revision()
        input_schema = self.create_input_schema(rev = r)

        r.set_output_schema(input_schema.get_latest_output_schema().attributes['id'])

        plan = r.plan()
        expected = set(['prepare_draft_for_import', 'set_schema', 'apply_metadata', 'upsert_task', 'set_display_type', 'publish', 'set_permission', 'wait_for_replication'])
        actual   = set([step['type'] for step in plan])
        self.assertTrue(set.issubset(expected, actual))

    def test_create_from_dataset(self):
        with open('test/fixtures/simple.csv', 'rb') as file:
            # boilerplate
            input_schema = self.create_input_schema()
            job = self.rev.apply(output_schema = input_schema.get_latest_output_schema())
            job = job.wait_for_finish()

            view = self.pub.views.lookup(self.rev.attributes['fourfour'])

            rev = view.revisions.create_replace_revision()

            rev = rev.update({
                'metadata': {
                    'description': 'new dataset description'
                }
            })

            source = rev.source_from_dataset()

            output_schema = source.get_latest_input_schema().get_latest_output_schema()

            new_output = output_schema\
                .change_column_metadata('a', 'description').to('meh')\
                .change_column_metadata('b', 'display_name').to('bbbb')\
                .change_column_metadata('c', 'field_name').to('ccc')\
                .run()

            [a, b, c] = new_output.attributes['output_columns']
            self.assertEqual(a['description'], 'meh')
            self.assertEqual(b['display_name'], 'bbbb')
            self.assertEqual(c['field_name'], 'ccc')

            self.assertEqual(rev.attributes['metadata']['description'], 'new dataset description')
