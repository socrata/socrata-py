import unittest
from socrata import Socrata
from socrata.authorization import Authorization
from test.auth import auth, TestCase

class CompositionTest(TestCase):

    def test_create_new_csv(self):
        with open('test/fixtures/simple.csv', 'rb') as file:
            (revision, output) = Socrata(auth).create(
                name = "cool dataset",
                description = "a description"
            ).csv(file)

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

        self.assertIsNotNone(output.attributes['completed_at'])

    def test_create_new_xls(self):
        with open('test/fixtures/simple.xls', 'rb') as file:
            (revision, output) = Socrata(auth).create(
                name = "cool dataset",
                description = "a description"
            ).xls(file)

            self.assertEqual(output.attributes['error_count'], 0)
            self.assertIsNotNone(output.attributes['completed_at'])


    def test_create_new_xlsx(self):
        with open('test/fixtures/simple.xlsx', 'rb') as file:
            (revision, output) = Socrata(auth).create(
                name = "cool dataset",
                description = "a description"
            ).xlsx(file)

            self.assertEqual(output.attributes['error_count'], 0)
            self.assertIsNotNone(output.attributes['completed_at'])


    def test_create_new_tsv(self):
        with open('test/fixtures/simple.tsv', 'rb') as file:
            (revision, output) = Socrata(auth).create(
                name = "cool dataset",
                description = "a description"
            ).tsv(file)

            self.assertEqual(output.attributes['error_count'], 0)
            self.assertIsNotNone(output.attributes['completed_at'])

    def test_create_new_shapefile(self):
        with open('test/fixtures/zillow.zip', 'rb') as file:
            (revision, output) = Socrata(auth).create(
                name = "zillow",
                description = "a description"
            ).shapefile(file)

            self.assertEqual(output.attributes['error_count'], 0)
            self.assertIsNotNone(output.attributes['completed_at'])
