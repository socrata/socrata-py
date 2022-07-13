import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase
from socrata.http import post, get
from time import sleep
import urllib

def enroll_in_archival_secondary(auth, view):
    # Enroll the dataset in the archival secondary manifest
    params = {
        'id': view.attributes['id'],
        'method': 'enroll'
    }
    post(
        'https://{domain}/api/archival?{q}'.format(
            domain=auth.domain,
            q=urllib.parse.urlencode(params)
        ),
        auth
    )

    def is_enrolled():
        response = get(
            'https://{domain}/api/views/{id}/replication.json'.format(
                domain=auth.domain,
                id=view.attributes['id']
            ),
            auth
        )

        archival = [v['version'] for v in response['secondary_versions'] if v['name'] == 'archival']
        if len(archival) == 1:
            return response['truth_data_version'] == archival[0]
        return False

    while not is_enrolled():
        sleep(1)

class TestSocrata(TestCase):
    def test_create_revision_and_view(self):
        rev = self.pub.new({
            'name': 'socrata-py test_create_revision_and_view',
            'description': 'foo!',
            'metadata': {
                'lol': 'anything',
                'is': 'allowed here'

            }
        })

        try:
            self.assertEqual(rev.attributes['metadata']['name'], 'socrata-py test_create_revision_and_view')
            self.assertEqual(rev.attributes['metadata']['description'], 'foo!')
            self.assertEqual(rev.attributes['metadata']['metadata']['lol'], 'anything')
            self.assertEqual(rev.attributes['metadata']['metadata']['is'], 'allowed here')
        finally:
            # Clean up the view we created inside this method
            view = self.pub.views.lookup(rev.view_id())
            view.delete()

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
        input_schema = self.create_input_schema(rev = r).wait_for_schema()
        r.set_output_schema(input_schema.get_latest_output_schema().attributes['id'])
        input_schema.get_latest_output_schema().wait_for_finish()

        plan = r.plan()
        expected = set(['prepare_draft_for_import', 'set_schema', 'apply_metadata', 'upsert_task', 'set_display_type', 'publish', 'set_permission', 'wait_for_replication'])
        actual   = set([step['type'] for step in plan])
        self.assertTrue(set.issubset(expected, actual))

    def test_create_from_dataset(self):
        with open('test/fixtures/simple.csv', 'rb') as file:
            # boilerplate
            input_schema = self.create_input_schema().wait_for_schema()
            input_schema.get_latest_output_schema().wait_for_finish()

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

    def test_restore_revision(self):
        # Do a revision so we can get it enrolled in archival
        self.rev.apply().wait_for_finish()
        enroll_in_archival_secondary(auth, self.view)


        rev = self.view.revisions.create_replace_revision()
        source = rev.create_upload('simple.csv')
        with open('test/fixtures/simple.csv', 'rb') as file:
            input_schema = source.csv(file)
        input_schema.wait_for_schema().get_latest_input_schema().get_latest_output_schema().wait_for_finish()
        rev.apply().wait_for_finish()
        rev.show()
        restored = rev.restore()

        self.assertEqual(rev.attributes['revision_seq'] + 1, restored.attributes['revision_seq'])
        self.assertEqual(rev.attributes['metadata'], restored.attributes['metadata'])

        source = restored.list_sources()[0]

        source.wait_for_schema()
        output_schema = source.get_latest_input_schema().get_latest_output_schema().wait_for_finish()
        self.assertEqual(output_schema.attributes['total_rows'], 4)
        self.assertEqual(
            set([oc['transform']['transform_expr'] for oc in output_schema.attributes['output_columns']]),
            set(['`a`', '`b`', '`c`'])
        )
