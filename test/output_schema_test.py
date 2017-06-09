import unittest
from socrata.publish import Publish
from test.auth import auth, fourfour, create_input_schema
import uuid

def create_bad_output_schema():
    input_schema = create_input_schema()

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

def create_good_output_schema():
    input_schema = create_input_schema()

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


class TestOutputSchema(unittest.TestCase):
    def test_create_upsert_job(self):
        output_schema = create_bad_output_schema()
        (ok, upsert_job) = output_schema.apply()
        self.assertTrue(ok, upsert_job)

    def test_get_rows(self):
        output_schema = create_good_output_schema()
        (ok, output_schema) = output_schema.wait_for_finish()

        (ok, rows) = output_schema.rows()
        self.assertTrue(ok, rows)

        self.assertEqual(rows, [
            {'b': {'ok': ' bfoo'}},
            {'b': {'ok': ' bfoo'}},
            {'b': {'ok': ' bfoo'}},
            {'b': {'ok': ' bfoo'}}
        ])

        (ok, rows) = output_schema.rows(offset = 2, limit = 1)
        self.assertTrue(ok, rows)

        self.assertEqual(rows, [
            {'b': {'ok': ' bfoo'}}
        ])

    def test_build_config(self):
        output_schema = create_good_output_schema()

        (ok, config) = output_schema.build_config(
            "my cool config %s" % str(uuid.uuid4()),
            "replace"
        )

        self.assertTrue(ok, config)

        [single_column] = config.attributes['columns']

        self.assertEqual(single_column['field_name'], 'b')
        self.assertEqual(single_column['display_name'], 'b')
        self.assertEqual(single_column['transform_expr'], "`b` || 'foo'")
