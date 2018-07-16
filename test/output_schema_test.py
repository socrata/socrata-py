import unittest
from socrata import Socrata
from test.auth import auth, TestCase
from time import sleep
import uuid

def create_bad_output_schema(input_schema):
    (ok, output_schema) = input_schema.transform({
        'output_columns': [
            {
                "field_name": "b",
                "display_name": "b, but as a number",
                "position": 0,
                "description": "b but with a bunch of errors",
                "transform": {
                    "transform_expr": "to_number(b)"
                }
            }
        ]}
    )
    assert ok
    return output_schema

def create_good_output_schema(input_schema):
    (ok, output_schema) = input_schema.transform({
        'output_columns': [
            {
                "field_name": "b",
                "display_name": "b",
                "position": 0,
                "description": "b",
                "transform": {
                    "transform_expr": "`b` || 'foo'"
                }
            }
        ]}
    )
    assert ok
    return output_schema


class TestOutputSchema(TestCase):
    def test_get_errors(self):
        output_schema = create_bad_output_schema(self.create_input_schema())
        (ok, output_schema) = output_schema.wait_for_finish()

        (ok, errors) = output_schema.schema_errors()

        for e in errors:
            assert 'error' in e['b']

    def test_get_errors_csv(self):
        output_schema = create_bad_output_schema(self.create_input_schema())
        (ok, output_schema) = output_schema.wait_for_finish()

        (ok, errors) = output_schema.schema_errors_csv()
        assert ok, errors
        out_csv = '\n'.join([str(line) for line in errors.iter_lines()])

        assert 'Unable to convert' in out_csv

    def test_get_rows(self):
        output_schema = create_good_output_schema(self.create_input_schema())
        (ok, output_schema) = output_schema.wait_for_finish()

        (ok, rows) = output_schema.rows()
        self.assertTrue(ok, rows)

        self.assertEqual(rows, [
            {'b': {'ok': 'bfoo'}},
            {'b': {'ok': 'bfoo'}},
            {'b': {'ok': 'bfoo'}},
            {'b': {'ok': 'bfoo'}}
        ])

        (ok, rows) = output_schema.rows(offset = 2, limit = 1)
        self.assertTrue(ok, rows)

        self.assertEqual(rows, [
            {'b': {'ok': 'bfoo'}}
        ])

    def test_build_config(self):
        output_schema = create_good_output_schema(self.create_input_schema())

        (ok, config) = output_schema.build_config(
            "my cool config %s" % str(uuid.uuid4()),
            "replace"
        )

        self.assertTrue(ok, config)

        [single_column] = config.attributes['columns']

        self.assertEqual(single_column['field_name'], 'b')
        self.assertEqual(single_column['display_name'], 'b')
        self.assertEqual(single_column['transform_expr'], "`b` || 'foo'")


    def test_validate_row_id(self):
        input_schema = self.create_input_schema()
        (ok, output_schema) = input_schema.transform({
            'output_columns': [
                {
                    "field_name": "a",
                    "display_name": "a",
                    "position": 0,
                    "description": "a",
                    "transform": {
                        "transform_expr": "`a`"
                    }
                }
            ]}
        )

        (ok, result) = output_schema.validate_row_id('a')

        self.assertEqual(result, {'valid': True})

        (ok, result) = output_schema.validate_row_id('nope')

        self.assertEqual(result, {'reason': 'No column with field_name = nope'})

    def test_set_row_id(self):
        input_schema = self.create_input_schema()

        (ok, output_schema) = input_schema.transform({
            'output_columns': [
                {
                    "field_name": "a",
                    "display_name": "a",
                    "position": 0,
                    "description": "a",
                    "transform": {
                        "transform_expr": "`a`"
                    }
                }
            ]}
        )
        (ok, result) = output_schema.validate_row_id('a')
        assert ok, result

        (ok, output_schema) = output_schema.set_row_id('a')
        assert ok, output_schema

        self.assertEqual(output_schema.attributes['output_columns'][0]['is_primary_key'], True)

    def test_change_columns(self):
        output = self.create_input_schema().get_latest_output_schema()

        (ok, output) = output\
            .change_column_metadata('a', 'field_name').to('aa')\
            .change_column_metadata('b', 'description').to('the description of b')\
            .change_column_metadata('c', 'display_name').to('Column C!')\
            .change_column_transform('c').to('to_number(`c`) + 7')\
            .run()

        assert ok, output

        [aa, b, c] = output.attributes['output_columns']

        self.assertEqual(aa['field_name'], 'aa')
        self.assertEqual(b['description'], 'the description of b')
        self.assertEqual(c['display_name'], 'Column C!')
        self.assertEqual(c['transform']['transform_expr'], 'to_number(`c`) + 7')

    def test_change_column_and_reference(self):
        output = self.create_input_schema().get_latest_output_schema()

        (ok, output) = output\
            .change_column_metadata('a', 'field_name').to('aa')\
            .change_column_metadata('aa', 'description').to('the description of aa')\
            .change_column_metadata('aa', 'display_name').to('COLUMN AA!')\
            .run()

        assert ok, output

        [aa, _b, _c] = output.attributes['output_columns']

        self.assertEqual(aa['field_name'], 'aa')
        self.assertEqual(aa['description'], 'the description of aa')
        self.assertEqual(aa['display_name'], 'COLUMN AA!')

    def test_add_after_delete(self):
        output = self.create_input_schema().get_latest_output_schema()

        (ok, output) = output\
            .drop_column('c')\
            .drop_column('b')\
            .drop_column('a')\
            .add_column('a', 'AA+AA', 'to_number(`a`) + to_number(`a`)', 'this is column a plus a')\
            .change_column_metadata('a', 'display_name').to('COLUMN AA!')\
            .run()

        assert ok, output

        [a] = output.attributes['output_columns']

        self.assertEqual(a['field_name'], 'a')
        self.assertEqual(a['description'], 'this is column a plus a')
        self.assertEqual(a['display_name'], 'COLUMN AA!')

    def test_drop_column(self):
        output = self.create_input_schema().get_latest_output_schema()

        (ok, output) = output\
            .change_column_metadata('a', 'field_name').to('aa')\
            .drop_column('b')\
            .drop_column('c')\
            .run()

        assert ok, output

        [aa] = output.attributes['output_columns']
        self.assertEqual(len(output.attributes['output_columns']), 1)
        self.assertEqual(aa['field_name'], 'aa')

    def test_create_column(self):
        output = self.create_input_schema().get_latest_output_schema()

        (ok, output) = output\
            .change_column_metadata('a', 'field_name').to('aa')\
            .drop_column('b')\
            .drop_column('c')\
            .add_column('aa_aa', 'AA+AA', 'to_number(`a`) + to_number(`a`)', 'this is column a plus a')\
            .run()

        assert ok, output

        (ok, output) = output.wait_for_finish()

        [aa, a_plus_a] = output.attributes['output_columns']
        self.assertEqual(len(output.attributes['output_columns']), 2)
        self.assertEqual(aa['field_name'], 'aa')
        self.assertEqual(a_plus_a['field_name'], 'aa_aa')

        (ok, rows) = output.rows(offset = 0, limit = 4)
        cells = [row.get('aa_aa')['ok'] for row in rows]

        self.assertEqual(cells, [
            '2',
            '4',
            '6',
            '8'
        ])


    def test_geocode_column(self):
        output = self.create_input_schema(filename = 'geo.csv').get_latest_output_schema()
        (ok, output) = output\
            .add_column('geocoded', 'Geocoded', 'geocode(`address`, `city`, `state`, `zip`)', 'geocoded column')\
            .drop_column('address')\
            .drop_column('city')\
            .drop_column('state')\
            .drop_column('zip')\
            .run()

        assert ok, output

        (ok, output) = output.wait_for_finish()
        assert ok, output

        (ok, rows) = output.rows(offset = 0, limit = 4)
        assert ok, rows

        [p0, p1, p2] = [r['geocoded']['ok'] for r in rows]

        self.assertEqual(p0['type'], 'Point')
        self.assertAlmostEqual(p0['coordinates'][0], -122.29939, places = 2)
        self.assertAlmostEqual(p0['coordinates'][1], 47.702105, places = 2)

        self.assertEqual(p1['type'], 'Point')
        self.assertAlmostEqual(p1['coordinates'][0], -77.037458, places = 2)
        self.assertAlmostEqual(p1['coordinates'][1], 38.898771, places = 2)

        self.assertEqual(p2['type'], 'Point')
        self.assertAlmostEqual(p2['coordinates'][0], -122.398373, places = 2)
        self.assertAlmostEqual(p2['coordinates'][1], 47.6762, places = 2)

    def test_change_existing_schema(self):
        # First we'll actually create a dataset

        rev = self.create_rev()
        input_schema = self.create_input_schema(rev = rev)
        output_schema = input_schema.get_latest_output_schema()
        (ok, job) = rev.apply(output_schema = output_schema)

        assert ok, job
        done = False
        attempts = 0
        while not done and attempts < 20:
            (ok, job) = job.show()
            attempts += 1
            assert ok, job

            done = job.attributes['status'] == 'successful'
            sleep(0.5)

        assert done, "Polling job never resulted in a successful completion: %s" % job

        (ok, rev) = self.view.revisions.create_replace_revision()
        assert ok, rev
        self.rev.discard()
        self.rev = rev

        # Ok, we've got a dataset.  Let's create a revision on it and mess with its schema!

        (ok, source) = rev.source_from_dataset()
        assert ok, source

        input_schema = source.get_latest_input_schema()
        output_schema = input_schema.get_latest_output_schema()
        (ok, new_output_schema) = output_schema.add_column('d', 'D', 'a + 5')\
                                               .drop_column('b')\
                                               .drop_column('c')\
                                               .run()
        assert ok, new_output_schema

        (ok, job) = rev.apply(output_schema = new_output_schema)
        assert ok, job

        done = False
        attempts = 0
        while not done and attempts < 20:
            (ok, job) = job.show()
            attempts += 1
            assert ok, job

            done = job.attributes['status'] == 'successful'
            sleep(0.5)

        assert done, "Polling job never resulted in a successful completion: %s" % job

        from socrata.http import get

        (ok, result) = get("https://{domain}/id/{fourfour}".format(domain = auth.domain, fourfour = rev.attributes['fourfour']), auth)
        assert ok, result

        self.assertEqual(result, [{'a': '1', 'd': '6'}, {'a': '2', 'd': '7'}, {'a': '3', 'd': '8'}, {'a': '4', 'd': '9'}])


    def test_add_column_for_wrong_backend(self):
        # First we'll actually create a dataset

        rev = self.create_rev()
        input_schema = self.create_input_schema(rev = rev)
        output_schema = input_schema.get_latest_output_schema()
        (ok, job) = rev.apply(output_schema = output_schema)
        (ok, job) = job.wait_for_finish()
        assert ok, job

        (ok, rev) = self.view.revisions.create_replace_revision()
        assert ok, rev
        self.rev.discard()
        self.rev = rev

        # Ok, we've got a dataset.  Let's create a revision on it and mess with its schema!

        (ok, source) = rev.source_from_dataset()
        assert ok, source

        input_schema = source.get_latest_input_schema()
        output_schema = input_schema.get_latest_output_schema()
        (ok, new_output_schema) = output_schema.add_column('d', 'D', 'make_point(a, c)').run()

        assert not ok

        self.assertEqual(new_output_schema, {
            'message': '{output_schema_id} Cannot attach a schema containing a column of type "point" to this revision',
            'params': {
                'details': {
                    'output_schema_id': [{}]
                },
                'output_schema_id': [
                    'Cannot attach a schema containing a column of type "point" to this revision'
                ],
                'invalid_keys': ['output_schema_id']
            },
            'key': 'validation_failed'
        })
