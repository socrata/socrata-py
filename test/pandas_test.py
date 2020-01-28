from socrata import Socrata
from socrata.authorization import Authorization
from auth import auth, TestCase
try:
    import pandas as pd
except ImportError:
    print("Pandas is required for this test")
    exit()
class TestPandas(TestCase):
    def test_create_source(self):
        rev = self.create_rev()

        source = rev.create_upload('foo.csv')
        self.assertEqual(source.attributes['source_type']['filename'], 'foo.csv')

        assert 'show' in source.list_operations()
        assert 'bytes' in source.list_operations()

    def test_source_csv(self):
        rev = self.create_rev()
        source = rev.create_upload('foo.csv')

        df = pd.read_csv('test/fixtures/simple.csv')
        source = source.df(df)
        output_schema = source.get_latest_input_schema().get_latest_output_schema()

        names = sorted([oc['field_name'] for oc in output_schema.attributes['output_columns']])
        self.assertEqual(['a', 'b', 'c'], names)

        assert 'show' in output_schema.list_operations()

    def test_create_source_outside_rev(self):
        pub = Socrata(auth)

        source = pub.sources.create_upload('foo.csv')
        self.assertEqual(source.attributes['source_type']['filename'], 'foo.csv')

        assert 'show' in source.list_operations()
        assert 'bytes' in source.list_operations()

    def test_source_csv_outside_rev(self):
        pub = Socrata(auth)

        source = pub.sources.create_upload('foo.csv')
        df = pd.read_csv('test/fixtures/simple.csv')
        source = source.df(df)
        input_schema = source.get_latest_input_schema()
        names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
        self.assertEqual(['a', 'b', 'c'], names)

    def test_put_source_in_revision(self):
        pub = Socrata(auth)
        source = pub.sources.create_upload('foo.csv')
        df = pd.read_csv('test/fixtures/simple.csv')
        input_schema = source.df(df)
        rev = self.create_rev()
        source = source.add_to_revision(rev)
