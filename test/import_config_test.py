import unittest
from socrata.publish import Publish
from socrata.authorization import Authorization
from test.auth import auth, fourfour
import uuid

class ImportConfigTest(unittest.TestCase):
    def test_create_config(self):
        name = "some_config %s" % str(uuid.uuid4())
        p = Publish(auth)
        (ok, config) = p.configs.create(name, "replace")
        self.assertTrue(ok, config)
        self.assertEqual(config.attributes['name'], name)


    def test_list_configs(self):
        p = Publish(auth)
        name = "some_config %s" % str(uuid.uuid4())
        (ok, config) = p.configs.create(name, "replace")
        self.assertTrue(ok, config)

        (ok, configs) = p.configs.list()

        # Assert there's some config on this domain where the
        # name is what we want
        self.assertTrue(any([
            config.attributes['name'] == name
            for config in configs
        ]))

    def test_lookup_config(self):
        p = Publish(auth)
        name = "some_config %s" % str(uuid.uuid4())
        (ok, config) = p.configs.create(name, "replace")
        self.assertTrue(ok, config)

        (ok, config) = p.configs.lookup(name)

        self.assertTrue(ok, config)
        self.assertEqual(config.attributes['name'], name)

    # Not yet implemented, this is the most important thing to make
    # import configs work so don't mess it up
    # def test_upload_to_config(self):
    #     p = Publish(auth)
    #     name = "some_config %s" % str(uuid.uuid4())
    #     (ok, config) = p.configs.create(name, "replace")
    #     self.assertTrue(ok, config)

    #     p.using_config(name, fourfour)

    def test_show_config(self):
        p = Publish(auth)
        name = "some_config %s" % str(uuid.uuid4())
        (ok, config) = p.configs.create(name, "replace")
        self.assertTrue(ok, config)

        (ok, config) = config.show()
        self.assertTrue(ok, config)

    def test_delete_config(self):
        p = Publish(auth)
        name = "some_config %s" % str(uuid.uuid4())
        (ok, config) = p.configs.create(name, "replace")
        self.assertTrue(ok, config)

        (ok, _) = config.delete()
        self.assertTrue(ok)

        (ok, _) = config.show()
        self.assertFalse(ok)

    def test_update_config(self):
        p = Publish(auth)
        name = "some_config %s" % str(uuid.uuid4())
        (ok, config) = p.configs.create(name, "replace")
        self.assertTrue(ok, config)

        columns = [
            {
                "field_name": "foo",
                "display_name": "Foo is the display name",
                "transform_expr": "to_number(`foo`)"
            }
        ]

        (ok, config) = config.update(
            data_action = "update",
            columns = columns
        )
        self.assertTrue(ok, config)

        self.assertEqual(config.attributes["data_action"], "update")
        self.assertEqual(config.attributes["columns"], columns)

