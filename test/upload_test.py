from socrata.publish import Publish
from socrata.authorization import Authorization
from test.auth import auth, TestCase

class TestSource(TestCase):
    def test_create_source(self):
        rev = self.create_rev()

        (ok, source) = rev.create_upload('foo.csv')
        self.assertTrue(ok)
        self.assertEqual(source.attributes['source_type']['filename'], 'foo.csv')

        assert 'show' in source.list_operations()
        assert 'bytes' in source.list_operations()

    def test_source_csv(self):
        rev = self.create_rev()
        (ok, source) = rev.create_upload('foo.csv')
        assert ok

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = source.csv(f)
            self.assertTrue(ok)
            self.assertEqual(input_schema.attributes['total_rows'], 4)

            names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            self.assertEqual(['a', 'b', 'c'], names)

            assert 'show' in input_schema.list_operations()

    def test_create_source_outside_rev(self):
        pub = Publish(auth)

        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)
        self.assertEqual(source.attributes['source_type']['filename'], 'foo.csv')

        assert 'show' in source.list_operations()
        assert 'bytes' in source.list_operations()

    def test_source_csv_outside_rev(self):
        pub = Publish(auth)

        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = source.csv(f)
            self.assertTrue(ok, input_schema)
            names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            self.assertEqual(['a', 'b', 'c'], names)

    def test_put_source_in_revision(self):
        pub = Publish(auth)

        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, input_schema) = source.csv(f)
            self.assertTrue(ok, input_schema)

            rev = self.create_rev()

            (ok, source) = source.add_to_revision(rev)
            self.assertTrue(ok, source)

