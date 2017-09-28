from socrata import Socrata
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

    def test_upload_csv(self):
        rev = self.create_rev()
        (ok, source) = rev.create_upload('foo.csv')
        assert ok

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, source) = source.csv(f)
            self.assertTrue(ok)
            input_schema = source.get_latest_input_schema()
            self.assertEqual(input_schema.attributes['total_rows'], 4)

            names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            self.assertEqual(['a', 'b', 'c'], names)

            assert 'show' in input_schema.list_operations()


    def test_upload_kml(self):
        rev = self.create_rev()
        (ok, source) = rev.create_upload('wards.kml')
        assert ok

        with open('test/fixtures/wards.kml', 'rb') as f:
            (ok, source) = source.kml(f)
            self.assertTrue(ok, source)

            input_schema = source.get_latest_input_schema()

            self.assertEqual(
                set(['ward_phone', 'ward', 'shape_leng', 'shape_area', 'perimeter', 'hall_phone', 'hall_offic', 'edit_date1', 'data_admin', 'class', 'alderman', 'address', 'polygon']),
                set([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            )

    def test_upload_shapefile(self):
        rev = self.create_rev()
        (ok, source) = rev.create_upload('wards.zip')
        assert ok

        with open('test/fixtures/wards.zip', 'rb') as f:
            (ok, source) = source.shapefile(f)
            self.assertTrue(ok, source)

            input_schema = source.get_latest_input_schema()

            self.assertEqual(
                set(['ward_phone', 'ward', 'shape_leng', 'shape_area', 'perimeter', 'hall_phone', 'hall_offic', 'edit_date1', 'data_admin', 'class', 'alderman', 'address', 'the_geom']),
                set([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            )

    def test_upload_geojson(self):
        rev = self.create_rev()
        (ok, source) = rev.create_upload('wards.geojson')
        assert ok

        with open('test/fixtures/wards.geojson', 'rb') as f:
            (ok, source) = source.geojson(f)
            self.assertTrue(ok, source)

            input_schema = source.get_latest_input_schema()

            self.assertEqual(
                set(['ward_phone', 'ward', 'shape_leng', 'shape_area', 'perimeter', 'hall_phone', 'hall_offic', 'edit_date1', 'data_admin', 'class', 'alderman', 'address', 'polygon']),
                set([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            )

    def test_create_from_url(self):
        # Yes, this is a bad idea
        # But the reason this test doesn't make a view on demand is because
        # we blacklist local addresses, which wouldn't allow this test to run against
        # localhost
        url = 'https://cheetah.test-socrata.com/api/views/agi2-jsej/rows.csv?accessType=DOWNLOAD'

        rev = self.create_rev()
        (ok, source) = rev.source_from_url(url)
        self.assertTrue(ok, source)

        (ok, source) = source.show()
        output_schema = source.get_latest_input_schema().get_latest_output_schema()
        output_schema.wait_for_finish()

        actual_columns = set([oc['field_name'] for oc in output_schema.attributes['output_columns']])
        expected_columns = set(['id', 'plausibility', 'incident_occurrence', 'incident_location', 'witness_gibberish', 'blood_alcohol_level'])

        self.assertEqual(actual_columns, expected_columns)

    def test_create_source_outside_rev(self):
        pub = Socrata(auth)

        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)
        self.assertEqual(source.attributes['source_type']['filename'], 'foo.csv')

        assert 'show' in source.list_operations()
        assert 'bytes' in source.list_operations()

    def test_upload_csv_outside_rev(self):
        pub = Socrata(auth)

        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, source) = source.csv(f)
            input_schema = source.get_latest_input_schema()
            self.assertTrue(ok, input_schema)
            names = sorted([ic['field_name'] for ic in input_schema.attributes['input_columns']])
            self.assertEqual(['a', 'b', 'c'], names)

    def test_put_source_in_revision(self):
        pub = Socrata(auth)

        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)

        with open('test/fixtures/simple.csv', 'rb') as f:
            (ok, source) = source.csv(f)
            input_schema = source.get_latest_input_schema()
            self.assertTrue(ok, input_schema)

            rev = self.create_rev()

            (ok, source) = source.add_to_revision(rev)
            self.assertTrue(ok, source)


    def test_source_change_header_rows(self):
        pub = Socrata(auth)
        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)

        (ok, source) = source\
            .change_parse_option('header_count').to(2)\
            .change_parse_option('column_header').to(2)\
            .run()

        self.assertTrue(ok, source)

        po = source.attributes['parse_options']
        self.assertEqual(po['header_count'], 2)
        self.assertEqual(po['column_header'], 2)

    def test_source_change_on_existing_upload(self):
        pub = Socrata(auth)
        (ok, source) = pub.sources.create_upload('foo.csv')
        self.assertTrue(ok, source)

        with open('test/fixtures/skip-header.csv', 'rb') as f:
            (ok, source) = source.csv(f)
            self.assertTrue(ok, source)


        (ok, source) = source\
            .change_parse_option('header_count').to(2)\
            .change_parse_option('column_header').to(2)\
            .run()

        self.assertTrue(ok, source)

        po = source.attributes['parse_options']
        self.assertEqual(po['header_count'], 2)
        self.assertEqual(po['column_header'], 2)

        input_schema = source.get_latest_input_schema()
        self.assertTrue(ok, input_schema)
        (ok, output_schema) = input_schema.latest_output()
        self.assertTrue(ok, output_schema)

        [a, b, c] = output_schema.attributes['output_columns']

        self.assertEqual(a['field_name'], 'a')
        self.assertEqual(b['field_name'], 'b')
        self.assertEqual(c['field_name'], 'c')
