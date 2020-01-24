import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase

def create(filename, kind):
    def decorator(method):
        def wrapper(slf):
            test_name = str(method.__qualname__)
            pub = Socrata(auth)
            with open('test/fixtures/%s' % filename, 'rb') as file:
                create = pub.create(
                    name = "test for %s" % test_name,
                    description = "a description"
                )
                (revision, output) = getattr(create, kind)(file)
                try:
                    method(slf, output)
                finally:
                    view = pub.views.lookup(revision.view_id())
                    view.delete()
        return wrapper
    return decorator


class CompositionTest(TestCase):

    @create('simple.csv', 'csv')
    def test_create_new_csv(self, output):
        self.assertEqual(output.attributes['error_count'], 0)
        self.assertIsNotNone(output.attributes['completed_at'])


    def test_create_new_csv_from_str(self):
        string = """a,b,c
        1,2,3
        4,5,6
        7,8,9
        """

        (revision, output) = Socrata(auth).create(
            name = "cool dataset",
            description = "a description"
        ).csv(string, filename = "foo.csv")
        try:
            self.assertIsNotNone(output.attributes['completed_at'])
        finally:
            view = Socrata(auth).views.lookup(revision.view_id())
            view.delete()


    @create('simple.xls', 'xls')
    def test_create_new_xls(self, output):
        self.assertEqual(output.attributes['error_count'], 0)
        self.assertIsNotNone(output.attributes['completed_at'])


    @create('simple.xlsx', 'xlsx')
    def test_create_new_xlsx(self, output):
        self.assertEqual(output.attributes['error_count'], 0)
        self.assertIsNotNone(output.attributes['completed_at'])

    @create('simple.tsv', 'tsv')
    def test_create_new_tsv(self, output):
        self.assertEqual(output.attributes['error_count'], 0)
        self.assertIsNotNone(output.attributes['completed_at'])

    @create('zillow.zip', 'shapefile')
    def test_create_new_shapefile(self, output):
        self.assertEqual(output.attributes['error_count'], 0)
        self.assertIsNotNone(output.attributes['completed_at'])
