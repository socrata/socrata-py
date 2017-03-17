import unittest
from socrata.publish import Publish
from test.auth import create_input_schema

class TestInputSchema(unittest.TestCase):
    def test_transform(self):
        input_schema = create_input_schema()

        (ok, res) = input_schema.transform({
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

        self.assertTrue(ok)
