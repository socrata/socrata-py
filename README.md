# socrata-py
experimental sdk for the socrata data-pipeline api

<!-- toc -->

  * [Installation](#installation)
  * [Documentation](#documentation)
  * [Example](#example)
  * [Using](#using)
    + [Boilerplate](#boilerplate)
    + [Simple usage](#simple-usage)
      - [Create a new Dataset from a csv, tsv, xls or xlsx file](#create-a-new-dataset-from-a-csv-tsv-xls-or-xlsx-file)
      - [Create a new Dataset from Pandas](#create-a-new-dataset-from-pandas)
      - [Updating a dataset](#updating-a-dataset)
      - [Generating a config and using it to update](#generating-a-config-and-using-it-to-update)
  * [Advanced usage](#advanced-usage)
    + [Create a revision](#create-a-revision)
    + [Create an upload](#create-an-upload)
    + [Upload a file](#upload-a-file)
    + [Transforming your data](#transforming-your-data)
    + [Wait for the transformation to finish](#wait-for-the-transformation-to-finish)
    + [Errors in a transformation](#errors-in-a-transformation)
    + [Validating rows](#validating-rows)
    + [Do the upsert!](#do-the-upsert)
- [Development](#development)
  * [Testing](#testing)
  * [Generating docs](#generating-docs)
  * [Releasing](#releasing)

<!-- tocstop -->

## Installation
This only supports python3.

Installation is available through pip. Using a virtualenv is advised. Install
the package by running

```
pip3 install socrata-py
```

The only hard dependency is `requests` which will be installed via pip. Pandas is not required, but creating a dataset from a Pandas dataframe is supported. See below.


## Documentation
* [SDK Docs](https://socrata.github.io/socrata-py/docs)

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
from socrata import Socrata
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
To create a dataset, you can do this:

```python
with open('cool_dataset.csv', 'rb') as file:
    # Upload + Transform step

    # view is the actual view in the Socrata catalog
    # revision is the *change* to the view in the catalog, which has not yet been applied
    # output is the OutputSchema, which is a change to data which can be applied via the revision
    (view, revision, output) = Socrata(auth).create(
        name = "cool dataset",
        description = "a description"
    ).csv(file)

    # Transformation step
    # We want to add some metadata to our column, drop another column, and add a new column which will
    # be filled with values from another column and then transformed
    (ok, output) = output\
        .change_column_metadata('a_column', 'display_name').to('A Column!')\
        .change_column_metadata('a_column', 'description').to('Here is a description of my A Column')\
        .drop_column('b_column')\
        .add_column('a_column_squared', 'A Column, but times itself', 'to_number(`a_column`) * to_number(`a_column`)', 'this is a column squared')\
        .run()


    # Validation of the results step
    (ok, output) = output.wait_for_finish()
    # The data has been validated now, and we can access errors that happened during validation. For example, if one of the cells in `a_column` couldn't be converted to a number in the call to `to_number`, that error would be reflected in this error_count
    assert output.attributes['error_count'] == 0

    # If you want, you can get a csv stream of all the errors
    (ok, errors) = output_schema.schema_errors_csv()
    for line in errors.iter_lines():
        print(line)

    # Update step

    # Apply the revision - this will make it public and available to make
    # visualizations from
    (ok, job) = revision.apply(output_schema = output)

    # Application is async - this will block until all the data
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
df = pd.read_csv('socrata-py/test/fixtures/simple.csv')
# Do various Pandas-y changes and modifications, then...
(view, revision, output) = Socrata(auth).create(
    name = "Pandas Dataset",
    description = "Dataset made from a Pandas Dataframe"
).df(df)

# Same code as above to apply the revision.

```

#### Updating a dataset
A Socrata `update` is actually an upsert. Rows are updated or created based on the row identifier. If the row-identifer doesn't exist, all updates are just appends to the dataset.

A `replace` truncates the whole dataset and then inserts the new data.

#### Generating a config and using it to update
```python
# This is how we create our view initially
with open('cool_dataset.csv', 'rb') as file:
    (view, revision, output) = Socrata(auth).create(
        name = "cool dataset",
        description = "a description"
    ).csv(file)

    revision.apply(output_schema = output)

# This will build a configuration using the same settings (file parsing and
# data transformation rules) that we used to get our output. The action
# that we will take will be "update", though it could also be "replace"
(ok, config) = output.build_config("cool-dataset-config", "update")

# Now we need to save our configuration name and view id somewhere so we
# can update the view using our config
configuration_name = "cool-dataset-config"
view_id = view.attributes['id']

# Now later, if we want to use that config to update our view, we just need the view and the configuration_name
(ok, view) = socrata.views.lookup(view_id) # View will be the view we are updating with the new data

with open('updated-cool-dataset.csv', 'rb') as my_file:
    (rev, job) = socrata.using_config(configuration_name, view).csv(my_file)
    print(job) # Our update job is now running
```


## Advanced usage

### Create a revision

```python
# This is our socrata object, using the auth variable from above
socrata = Socrata(auth)

(ok, view) = socrata.views.create({'name': 'cool dataset'})
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

### Create an upload
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
### Upload a file
```python
# And using that upload we just created, we can put bytes into it
with open('test/fixtures/simple.csv', 'rb') as f:
    (ok, input_schema) = upload.csv(f)
    assert ok
```
### Transforming your data
Transforming data consists of going from input data (data exactly as it appeared in the source)
to output data (data as you want it to appear).

Transformation from input data to output data often has problems. You might, for example, have a column
full of numbers, but one row in that column is actually the value "???" which cannot be transformed into
a number. Rather than failing at each datum which is dirty or wrong, transforming your data allows you to
reconcile these issues.

We might have a dataset called `temps.csv` that looks like
```
date, celsius
8-24-2017, 22
8-25-2017, 20
8-26-2017, 23
8-27-2017, hehe!
8-28-2017,
8-29-2017, 21
```

Suppose we uploaded it in our previous step, like this:

```
with open('temps.csv', 'rb') as f:
    (ok, input_schema) = upload.csv(f)
    assert ok
```

Our `input_schema` is the input data exactly as it appeared in the CSV, with all values of type `string`.

Our `output_schema` is the output data as it was *guessed* by Socrata. Guessing may not always be correct, which is why we have import configs to "lock in" a schema for automation. We can get the `output_schema`
like so:

```
(ok, output_schema) = input_schema.latest_output()
assert ok
```

We can now make changes to the schema, like so

```
(ok, new_output_schema) = output
    # Change the field_name of date to the_date
    .change_column_metadata('date', 'field_name').to('the_date')\
    # Change the description of the celsius column
    .change_column_metadata('celsius', 'description').to('the temperature in celsius')\
    # Change the display name of the celsius column
    .change_column_metadata('celsius', 'display_name').to('Degrees (Celsius)')\
    # Change the transform of the_date column to to_fixed_timestamp(`date`)
    .change_column_transform('the_date').to('to_fixed_timestamp(`date`)')
    # Make the celsius column all numbers
    .change_column_transform('celsius').to('to_number(`celsius`)')
    # Add a new column, which is computed from the `celsius` column
    .add_column('fahrenheit', 'Degrees (Fahrenheit)', '(to_number(`celsius`) * (9 / 5)) + 32', 'the temperature in celsius')
    .run()
```

We can also call `drop_column(celsius)` which will drop the column.

Transforms can be complex SoQL expressions. Available functions are listed [http://docs.socratapublishing.apiary.io/#reference/0/inputschema](here). You can do lots of stuff with them;


For example, you could change all `null` values into errors (which won't be imported) by doing
something like
```
(ok, new_output_schema) = output
    .change_column_transform('celsius').to('coalesce(to_number(`celsius`), error("Celsius was null!"))')
    .run()
```

Or you could add a new column that says if the day was hot or not
```
(ok, new_output_schema) = output
    .add_column('is_hot', 'Was the day hot?', 'to_number(`celsius`) >= 23', '')
    .run()
```

Composing these SoQL functions into expressions will allow you to validate, shape, clean and extend your data to make it more useful to the consumer.

### Wait for the transformation to finish
Transformations are async, so if you want to wait for it to finish, you can do so
```python
(ok, output_schema) = new_output_schema.wait_for_finish()
assert ok, output_schema
```

### Errors in a transformation
Transformations may have had errors, like in the previous example, we can't convert `hehe!` to a number. We can see the count of them like this:
```python
print(output_schema.attributes['error_count'])
```

We can view the detailed errors like this:
```python
(ok, errors) = output_schema.schema_errors()
```

We can get a CSV of the errors like this:
```python
(ok, csv_stream) = output_schema.schema_errors_csv()
```

### Validating rows
We can look at the rows of our schema as well
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
