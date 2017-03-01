import unittest
from src.publish import Publish
from test.auth import auth, fourfour, create_input_schema

def create_output_schema():
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

class TestOutputSchema(unittest.TestCase):
    def test_create_upsert_job(self):
        output_schema = create_output_schema()
        (ok, upsert_job) = output_schema.apply()
        self.assertTrue(ok)
