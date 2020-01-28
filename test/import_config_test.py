import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase
import uuid
from socrata.http import UnexpectedResponseException

class ImportConfigTest(TestCase):
    def test_create_config(self):
        name = "some_config %s" % str(uuid.uuid4())
        p = Socrata(auth)
        config = p.configs.create(name, "replace")
        self.assertEqual(config.attributes['name'], name)


    def test_create_config_with_non_defaults(self):
        name = "some_config %s" % str(uuid.uuid4())
        p = Socrata(auth)
        config = p.configs.create(
            name,
            "replace",
            parse_options = {
                "encoding": "utf8",
                "header_count": 2,
                "column_header": 2
            },
            columns = [
                {
                    "field_name": "foo",
                    "display_name": "Foo is the display name",
                    "transform_expr": "to_number(`foo`)"
                }
            ]
        )
        self.assertEqual(config.attributes['name'], name)

        self.assertEqual(config.attributes['parse_options'], {
            "encoding": "utf8",
            "header_count": 2,
            "column_header": 2,
            "quote_char": '"',
            "parse_source": True,
            "column_separator": ",",
            "remove_empty_rows": True,
            "trim_whitespace": True
        })

        self.assertEqual(config.attributes['columns'], [
            {
                "field_name": "foo",
                "display_name": "Foo is the display name",
                "transform_expr": "to_number(`foo`)",
                "format": {},
                "description": "",
                "is_primary_key": None,
                "flags": []
            }
        ])

    def test_list_operations(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        configs = p.configs.list()

        # Assert there's some config on this domain where the
        # name is what we want
        self.assertTrue(any([
            config.attributes['name'] == name
            for config in configs
        ]))

    def test_lookup_config(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        config = p.configs.lookup(name)

        self.assertEqual(config.attributes['name'], name)

    def test_upload_to_config(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        p = Socrata(auth)
        with open('test/fixtures/simple.csv', 'rb') as my_file:
            (rev, job) = p.using_config(name, self.view).csv(my_file)
            self.assertEqual(rev.attributes['action']['type'], 'replace')
            self.assertTrue(job.attributes['created_at'])
            job.wait_for_finish()

    def test_config_not_found(self):
        p = Socrata(auth)

        with open('test/fixtures/simple.csv', 'rb') as my_file:
            with self.assertRaises(UnexpectedResponseException):
                (rev, job) = p.using_config("nope", self.view).csv(my_file)

    def test_source_to_config(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        p = Socrata(auth)
        (rev, job) = p.using_config(name, self.view).csv(
            """a,b,c
                1,2,3
                4,5,6
                7,8,9
            """,
            filename = "abc.csv"
        )
        self.assertEqual(rev.attributes['action']['type'], 'replace')
        self.assertTrue(job.attributes['created_at'])
        job.wait_for_finish()


    def test_show_config(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        config = config.show()

    def test_delete_config(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        _ = config.delete()

        # TODO exception
        with self.assertRaises(UnexpectedResponseException):
            _ = config.show()

    def test_update_config(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        columns = [
            {
                "field_name": "foo",
                "display_name": "Foo is the display name",
                "transform_expr": "to_number(`foo`)",
                "format": {},
                "description": "",
                "is_primary_key": None,
                "flags": []
            }
        ]

        config = config.update({
            'data_action': 'update',
            'columns': columns
        })

        self.assertEqual(config.attributes["data_action"], "update")
        self.assertEqual(config.attributes["columns"], columns)


    def test_update_config_using_builder(self):
        p = Socrata(auth)
        name = "some_config %s" % str(uuid.uuid4())
        config = p.configs.create(name, "replace")

        columns = [
            {
                "field_name": "foo",
                "display_name": "Foo is the display name",
                "transform_expr": "to_number(`foo`)"
            }
        ]

        config = config\
            .change_parse_option('header_count').to(2)\
            .change_parse_option('encoding').to('utf16')\
            .change_parse_option('column_header').to(2)\
            .run()


        parse_options = config.attributes['parse_options']
        self.assertEqual(parse_options['header_count'], 2)
        self.assertEqual(parse_options['column_header'], 2)
        self.assertEqual(parse_options['encoding'], 'utf16')
