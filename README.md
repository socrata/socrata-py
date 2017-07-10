# publish-py
experimental sdk for the socrata publishing api

## Installation
This only supports python3.

Installation is available through pip. Using a virtualenv is advised. Install
the package by running

```
pip3 install socrata-publish-py
```

The only hard dependency is `requests` which will be installed via pip. Pandas is not required, but creating a dataset from a Pandas dataframe is supported. See below.


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


#### Create a new Dataset from a csv, tsv, xls or xlsx file
To create a dataset with as little code as possible, you can do this:

```python
with open('cool_dataset.csv', 'rb') as file:
    # Upload + Transform step

    # view is the actual view in the Socrata catalog
    # revision is the *change* to the view in the catalog, which has not yet been applied
    # output is the OutputSchema, which is a change to data which can be applied via the revision
    (view, revision, output) = Publish(auth).create(
        name = "cool dataset",
        description = "a description"
    ).csv(file)


    # Validation results step

    # The data has been validated now, and we can access errors that happened during validation
    assert output.attributes['error_count'] == 0

    # If you want, you can get a csv stream of all the errors
    (ok, errors) = output_schema.schema_errors_csv()
    for line in errors.iter_lines():
        print(line)

    # Update step

    # Publish the dataset - this will make it public and available to make
    # visualizations from
    (ok, job) = revision.apply(output_schema = output)

    # Publishing is async - this will block until all the data
    # is in place and readable
    job.wait_for_finish()

    # This opens a browser window with your new view you just created
    view.open_in_browser()
```

Similar to the `csv` method are the `xls`, `xlsx`, and `tsv` methods, which upload
those files.

#### Create a new Dataset from Pandas
Datasets can also be created from Pandas DataFrames
```python
import pandas as pd
df = pd.read_csv('publish-py/test/fixtures/simple.csv')
# Do various Pandas-y changes and modifications, then...
(view, revision, output) = Publish(auth).create(
    name = "Pandas Dataset",
    description = "Dataset made from a Pandas Dataframe"
).df(df)

# Same code as above to apply the revision.

```

#### Updating a dataset
A Socrata `update` is actually an upsert. Rows are updated or created based on the row identifier. If the row-identifer doesn't exist, all updates are just appends to the dataset.

A `replace` truncates the whole dataset and then inserts the new data.

##### Generating a config and using it to update
```python
# This is how we create our view initially
with open('cool_dataset.csv', 'rb') as file:
    (view, revision, output) = Publish(auth).create(
        name = "cool dataset",
        description = "a description"
    ).csv(file)

# This will build a configuration using the same settings (file parsing and
# data transformation rules) that we used to get our output. The action
# that we will take will be "update", though it could also be "replace"
(ok, config) = output.build_config("cool-dataset-config", "update")

# Now we need to save our configuration name and view id somewhere so we
# can update the view using our config
configuration_name = "cool-dataset-config"
view_id = view.attributes['id']

# Now later, if we want to use that config to update our view, we just need the view and the configuration_name
(ok, view) = publishing.views.lookup(view_id) # View will be the view we are updating with the new data

with open('updated-cool-dataset.csv', 'rb') as my_file:
    (rev, job) = publishing.using_config(configuration_name, view).csv(my_file)
    print(job) # Our update job is now running
```

##### Updating without a config
This isn't advised. Doing an update without a config doesn't ensure that the same settings that your view was created with are used to parse and transform the updated file. This is why we have configs - they freeze settings that a view was once created with and allow them to be reused for updates and replaces.
```python
(ok, view) = publishing.views.lookup('tnir-bc4v')
(ok, rev) = view.revisions.update()
with open('cool_dataset.csv, 'rb') as file:
    (ok, upload) = rev.create_upload(file.name)
    (ok, input_schema) = upload.csv(file)
    (ok, output_schema) = input_schema.latest_output()
    rev.apply(output_schema = output_schema)
```



### Advanced usage

#### Create a revision

```python
# This is our publishing object, using the auth variable from above
publishing = Publish(auth)

(ok, view) = publishing.views.create({'name': 'cool dataset'})
assert ok, view

# Make an `update` revision to that view
(ok, rev) = view.revisions.create_update_revision()
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
(ok, upload) = rev.create_upload('foo.csv')
assert ok

# And print it
print(upload)
Source({'content_type': None,
 'created_by': {'display_name': 'rozap',
                'email': 'chris.duranti@socrata.com',
                'user_id': 'tugg-ikce'},
 'source_type': {
    'filename': 'foo.csv',
    'type': 'upload'
 },
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

# Wait for the transformation to finish
(ok, output_schema) = output_schema.wait_for_finish()
assert ok, output_schema

# Look at how many validation errors happened while trying to transform our dataset
print(output_schema.attributes['error_count'])

# Maybe we want to discard the revision if our file had errors in it. This will prevent the revision from ever being applied
if output_schema.attributes['error_count'] > 0:
    rev.discard()
```


### Validating rows
```python
(ok, rows) = output_schema.rows(offset = 0, limit = 20)
assert ok,

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
job.wait_for_finish(progress = lambda job: print(job.attributes['log']))

# So now if we go look at our original four-four, our data will be there
```


# Development

## Testing
Install test deps by running `pip install -r requirements.txt`. This will install `pdoc` and `pandas` which are required to run the tests.

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
