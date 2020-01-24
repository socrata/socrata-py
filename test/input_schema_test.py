import unittest
from socrata import Socrata
from test.auth import TestCase

class TestInputSchema(TestCase):
    def test_transform(self):
        input_schema = self.create_input_schema()

        res = input_schema.transform({
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

    def test_show_latest(self):
        input_schema = self.create_input_schema()

        output_schema = input_schema.latest_output()
        self.assertEqual(input_schema.attributes['id'], output_schema.attributes['input_schema_id'])

        nested = input_schema.get_latest_output_schema()

        self.assertEqual(nested.attributes['id'], output_schema.attributes['id'])
