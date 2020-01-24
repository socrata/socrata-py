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

def create(name, filepath):
    socrata = Socrata(auth)
    with open(filepath, 'rb') as csv_file:
        (revision, output) = socrata.create(
            name = name
        ).csv(csv_file)

        job = revision.apply(output_schema = output)
        revision.open_in_browser()

create(args.name, args.csv)
