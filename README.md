# publish-py
experimental sdk for the socrata publishing api

## Installation
This only supports python3.

Installation is available through pip. Using a virtualenv is advised. Install
the package by running

```
pip3 install socrata-publish-py
```

## Documentation
* [API Docs](http://docs.socratapublishing.apiary.io/#)
* [SDK Docs](https://socrata.github.io/publish-py/docs)

## Example
Try the command line example with
```bash
python -m examples.create 'Police Reports' ~/Desktop/catalog.data.gov/Seattle_Real_Time_Fire_911_Calls.csv 'pete-test.test-socrata.com' --username $SOCRATA_USERNAME --password $SOCRATA_PASSWORD
```
## Using

### Boilerplate
```python
# Import some stuff
from socrata.authorization import Authorization
from socrata.publish import Publish
import os

# Boilerplate...
# Make an auth object
auth = Authorization(
  "pete-test.test-socrata.com",
  os.environ['SOCRATA_USERNAME'],
  os.environ['SOCRATA_PASSWORD']
)

```

### Simple usage
To create a dataset with as little code as possible, you can do this:

```python
with open('path/to/my/file.csv', 'rb') as file:
    # Upload the data, validate it
    (revision, output) = Publish(auth).create(
        name = "cool dataset",
        description = "a description"
    ).csv(file)

    # It's wise to make sure you have no type errors, if that's what you expect
    assert output.attributes['error_count'] == 0

    # Publish the dataset - this will make it public and available to make
    # visualizations from
    revision.apply(output_schema = output)
```

Similar to the `csv` method are the `xls`, `xlsx`, and `tsv` methods, which upload
those files.


### Advanced usage

#### Create a revision

```python
# This is our publishing object, using the auth variable from above
publishing = Publish(auth)

(ok, view) = publishing.new({'name': 'cool dataset'})
assert ok, view
# This is our view
fourfour = view['id']

# Make a revision
(ok, rev) = publishing.revisions.create(fourfour)
assert ok

# rev is a Revision object, we can print it
print(rev)
Revision({'created_by': {'display_name': 'rozap',
                'email': 'chris.duranti@socrata.com',
                'user_id': 'tugg-ikce'},
 'fourfour': 'ij46-xpxe',
 'id': 346,
 'inserted_at': '2017-02-27T23:05:08.522796',
 'metadata': None,
 'update_seq': 285,
 'upsert_jobs': []})

# We can also access the attributes of the revision
print(rev.attributes['fourfour'])
'ij46-xpxe'
```

### Make an upload, given a revision
```python
# Using that revision, we can create an upload
(ok, upload) = rev.create_upload({'filename': "foo.csv"})
assert ok

# And print it
print(upload)
Upload({'content_type': None,
 'created_by': {'display_name': 'rozap',
                'email': 'chris.duranti@socrata.com',
                'user_id': 'tugg-ikce'},
 'filename': 'foo.csv',
 'finished_at': None,
 'id': 290,
 'inserted_at': '2017-02-27T23:07:18.309676',
 'schemas': []})
```
### Upload a csv, given an upload
```python
# And using that upload we just created, we can put bytes into it
with open('test/fixtures/simple.csv', 'rb') as f:
    (ok, input_schema) = upload.csv(f)
    assert ok
```

### Transform a file, given an input schema
```python
# Putting bytes into an upload gives us an input schema. We can call `transform` on the
# input schema to get a new output schema with our transforms applied.
(ok, output_schema) = input_schema.transform({
    'output_columns': [
        {
            "field_name": "b",
            "display_name": "b, but as a number",
            "position": 0,
            "description": "b but with a bunch of errors",
            "transform": {
                "transform_expr": "to_number(b)"
            }
        }
    ]})
)

# Wait for the transformation to finish
(ok, output_schema) = output_schema.wait_for_finish()
assert ok, output_schema

# Look at how many validation errors happened while trying to transform our dataset
print(output_schema.attributes['error_count'])
```


### Validating rows
```python
(ok, rows) = output_schema.rows(offset = 0, limit = 20)
asssert ok,

self.assertEqual(rows, [
    {'b': {'ok': ' bfoo'}},
    {'b': {'ok': ' bfoo'}},
    {'b': {'ok': ' bfoo'}},
    {'b': {'ok': ' bfoo'}}
])
```

### Do the upsert!
```python
# Now we have transformed our data into the shape we want, let's do an upsert
(ok, job) = rev.apply(output_schema = output_schema)

# This will complete the upsert behind the scenes. If we want to
# re-fetch the current state of the upsert job, we can do so
(ok, job) = job.show()

# To get the progress
print(job.attributes['log'])
[
    {'details': {'Errors': 0, 'Rows Created': 0, 'Rows Updated': 0, 'By RowIdentifier': 0, 'By SID': 0, 'Rows Deleted': 0}, 'time': '2017-02-28T20:20:59', 'stage': 'upsert_complete'},
    {'details': {'created': 1}, 'time': '2017-02-28T20:20:59', 'stage': 'columns_created'},
    {'details': {'created': 1}, 'time': '2017-02-28T20:20:59', 'stage': 'columns_created'},
    {'details': None, 'time': '2017-02-28T20:20:59', 'stage': 'started'}
]


# So maybe we just want to wait here, printing the progress, until the job is done
job.wait_for_finish(progress = lambda job: sys.stdout.write(str(job.attributes['log'][0]) + "\n"))

# So now if we go look at our original four-four, our data will be there
```


# Development

## Testing
Configuration is set in `test/auth.py` for tests. It reads the domain, username, and password from environment variables. If you want to run the tests, set those environment variables to something that will work.

If I wanted to run the tests against my local instance, I would run:
```bash
SOCRATA_DOMAIN=localhost SOCRATA_USERNAME=$SOCRATA_LOCAL_USER SOCRATA_PASSWORD=$SOCRATA_LOCAL_PASS bin/test
```

## Generating docs
make the docs by running
```bash
make docs
```

## Releasing
release to pypi by bumping the version to something reasonable and running
```
python setup.py sdist upload -r pypi
```
Note you'll need your `.pypirc` file in your home directory. For help, read [this](http://peterdowns.com/posts/first-time-with-pypi.html)
