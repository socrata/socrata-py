from socrata.publish import Publish
from socrata.authorization import Authorization
from test.auth import auth, TestCase

class TestUpload(TestCase):
    def test_create_upload(self):
        rev = self.create_rev()

        (ok, upload) = rev.create_upload({'filename': "foo.csv"})
        self.assertTrue(ok)
        self.assertEqual(upload.attributes['filename'], 'foo.csv')

        assert 'show' in upload.list_operations()
        assert 'bytes' in upload.list_operations()

    def test_upload_csv(self):
        rev = self.create_rev()
        (ok, upload) = rev.create_upload({'filename': "foo.csv"})
        assert ok

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = upload.csv(f)
            self.assertTrue(ok)
            self.assertEqual(input_schema.attributes['total_rows'], 4)

            names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            self.assertEqual(['a', 'b', 'c'], names)

            assert 'show' in input_schema.list_operations()

    def test_create_upload_outside_rev(self):
        pub = Publish(auth)

        (ok, upload) = pub.uploads.create({'filename': 'foo.csv'})
        self.assertTrue(ok, upload)
        self.assertEqual(upload.attributes['filename'], 'foo.csv')

        assert 'show' in upload.list_operations()
        assert 'bytes' in upload.list_operations()

    def test_upload_csv_outside_rev(self):
        pub = Publish(auth)

        (ok, upload) = pub.uploads.create({'filename': 'foo.csv'})
        self.assertTrue(ok, upload)

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = upload.csv(f)
            self.assertTrue(ok, input_schema)
            names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            self.assertEqual(['a', 'b', 'c'], names)

    def test_put_upload_in_revision(self):
        pub = Publish(auth)

        (ok, upload) = pub.uploads.create({'filename': 'foo.csv'})
        self.assertTrue(ok, upload)

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = upload.csv(f)
            self.assertTrue(ok, input_schema)

            rev = self.create_rev()

            (ok, upload) = upload.add_to_revision(rev)
            self.assertTrue(ok, upload)

