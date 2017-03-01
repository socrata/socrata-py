import unittest
from src.publish import Publish
from src.authorization import Authorization
from test.auth import create_rev

class TestUpload(unittest.TestCase):
    def test_create_upload(self):
        rev = create_rev()

        (ok, upload) = rev.create_upload({'filename': "foo.csv"})
        self.assertTrue(ok)
        self.assertEqual(upload.attributes['filename'], 'foo.csv')

        assert 'show' in upload.list_operations()
        assert 'bytes' in upload.list_operations()

    def test_upload_csv(self):
        rev = create_rev()
        (ok, upload) = rev.create_upload({'filename': "foo.csv"})
        assert ok

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = upload.csv(f)
            self.assertTrue(ok)
            self.assertEqual(input_schema.attributes['total_rows'], 4)

            names = [ic['field_name'] for ic in input_schema.attributes['input_columns']]
            self.assertEqual(['a', 'b', 'c'], names)

            assert 'show' in input_schema.list_operations()
