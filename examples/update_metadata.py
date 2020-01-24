import os
from socrata.authorization import Authorization
from socrata import Socrata
import sys
import time
import argparse

parser = argparse.ArgumentParser(description='Create a dataset.')
parser.add_argument('name', type=str, help='Name of your dataset')
parser.add_argument('csv', type=str, help='Path to the CSV')
parser.add_argument('domain', type=str, help='Your Socrata domain')
parser.add_argument('--username', type=str, help='Your Socrata username', default = os.environ.get('SOCRATA_USERNAME', None))
parser.add_argument('--password', type=str, help='Your Socrata password', default = os.environ.get('SOCRATA_PASSWORD', None))
args = parser.parse_args()

auth = Authorization(
  args.domain,
  args.username,
  args.password
)
auth.live_dangerously()

def create(name, filepath):
    socrata = Socrata(auth)
    with open(filepath, 'rb') as csv_file:
        (initial_rev, output) = socrata.create(
            name = name
        ).csv(csv_file)

        job = initial_rev.apply(output_schema = output)
        job = job.wait_for_finish()

        view = socrata.views.lookup(initial_rev.attributes['fourfour'])
        update(view)


def update(view):
    revision = view.revisions.create_replace_revision()
    source = revision.source_from_dataset()

    output_schema = source.get_latest_input_schema().get_latest_output_schema()

    print(output_schema)
    random_column = output_schema.attributes['output_columns'][0]['field_name']

    new_output = output_schema\
        .change_column_metadata(random_column, 'description').to('this is the updated description')\
        .change_column_metadata(random_column, 'display_name').to('updated display name')\
        .run()


    revision.apply(output_schema = new_output)

    revision.open_in_browser()

create(args.name, args.csv)
