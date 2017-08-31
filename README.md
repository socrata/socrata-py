# socrata-py
experimental sdk for the socrata data-pipeline api

<!-- toc -->

  * [Installation](#installation)
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
- [Library Docs](#library-docs)
    + [Socrata](#socrata)
      - [create](#create)
      - [new](#new)
      - [using_config](#using_config)
    + [Authorization](#authorization)
      - [live_dangerously](#live_dangerously)
    + [Revisions](#revisions)
      - [create_replace_revision](#create_replace_revision)
      - [create_update_revision](#create_update_revision)
      - [create_using_config](#create_using_config)
      - [list](#list)
      - [lookup](#lookup)
    + [Revision](#revision)
      - [apply](#apply)
      - [create_upload](#create_upload)
      - [discard](#discard)
      - [list_operations](#list_operations)
      - [open_in_browser](#open_in_browser)
      - [ui_url](#ui_url)
      - [update](#update)
    + [Sources](#sources)
      - [create_upload](#create_upload-1)
    + [Source](#source)
      - [add_to_revision](#add_to_revision)
      - [change_parse_option](#change_parse_option)
      - [csv](#csv)
      - [df](#df)
      - [list_operations](#list_operations-1)
      - [shapefile](#shapefile)
      - [tsv](#tsv)
      - [xls](#xls)
      - [xlsx](#xlsx)
    + [Configs](#configs)
      - [create](#create-1)
      - [list](#list-1)
      - [lookup](#lookup-1)
    + [Config](#config)
      - [change_parse_option](#change_parse_option-1)
      - [create_revision](#create_revision)
      - [delete](#delete)
      - [list_operations](#list_operations-2)
      - [update](#update-1)
    + [InputSchema](#inputschema)
      - [get_latest_output_schema](#get_latest_output_schema)
      - [latest_output](#latest_output)
      - [list_operations](#list_operations-3)
      - [transform](#transform)
    + [OutputSchema](#outputschema)
      - [add_column](#add_column)
      - [any_failed](#any_failed)
      - [build_config](#build_config)
      - [change_column_metadata](#change_column_metadata)
      - [change_column_transform](#change_column_transform)
      - [drop_column](#drop_column)
      - [list_operations](#list_operations-4)
      - [rows](#rows)
      - [run](#run)
      - [schema_errors](#schema_errors)
      - [schema_errors_csv](#schema_errors_csv)
      - [set_row_id](#set_row_id)
      - [validate_row_id](#validate_row_id)
      - [wait_for_finish](#wait_for_finish)
    + [Job](#job)
      - [is_complete](#is_complete)
      - [list_operations](#list_operations-5)
      - [wait_for_finish](#wait_for_finish-1)

<!-- tocstop -->

## Installation
This only supports python3.

Installation is available through pip. Using a virtualenv is advised. Install
the package by running

```
pip3 install socrata-py
```

The only hard dependency is `requests` which will be installed via pip. Pandas is not required, but creating a dataset from a Pandas dataframe is supported. See below.


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

    # revision is the *change* to the view in the catalog, which has not yet been applied.
    # output is the OutputSchema, which is a change to data which can be applied via the revision
    (revision, output) = Socrata(auth).create(
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

    # This opens a browser window to your revision, and you will see the progress
    # of the job
    rev.open_in_browser()

    # Application is async - this will block until all the data
    # is in place and readable
    job.wait_for_finish()
```

Similar to the `csv` method are the `xls`, `xlsx`, and `tsv` methods, which upload
those files.

#### Create a new Dataset from Pandas
Datasets can also be created from Pandas DataFrames
```python
import pandas as pd
df = pd.read_csv('socrata-py/test/fixtures/simple.csv')
# Do various Pandas-y changes and modifications, then...
(revision, output) = Socrata(auth).create(
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
    (revision, output) = Socrata(auth).create(
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
view_id = revision.view_id()

# Now later, if we want to use that config to update our view, we just need the view and the configuration_name
socrata = Socrata(auth)
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

# This will make our initial revision, on a view that doesn't yet exist
(ok, rev) = socrata.new({'name': 'cool dataset'})
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
print(rev.attributes['metadata']['name'])
'cool dataset'
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
    (ok, source) = upload.csv(f)
    assert ok
```
### Transforming your data
Transforming data consists of going from input data (data exactly as it appeared in the source)
to output data (data as you want it to appear).

Transformation from input data to output data often has problems. You might, for example, have a column
full of numbers, but one row in that column is actually the value `hehe!` which cannot be transformed into
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

```python
with open('temps.csv', 'rb') as f:
    (ok, source) = upload.csv(f)
    assert ok
    input_schema = source.get_latest_input_schema()
```

Our `input_schema` is the input data exactly as it appeared in the CSV, with all values of type `string`.

Our `output_schema` is the output data as it was *guessed* by Socrata. Guessing may not always be correct, which is why we have import configs to "lock in" a schema for automation. We can get the `output_schema`
like so:

```python
(ok, output_schema) = input_schema.get_latest_output_schema()()
assert ok
```

We can now make changes to the schema, like so

```python
(ok, new_output_schema) = output
    # Change the field_name of date to the_date
    .change_column_metadata('date', 'field_name').to('the_date')\
    # Change the description of the celsius column
    .change_column_metadata('celsius', 'description').to('the temperature in celsius')\
    # Change the display name of the celsius column
    .change_column_metadata('celsius', 'display_name').to('Degrees (Celsius)')\
    # Change the transform of the_date column to to_fixed_timestamp(`date`)
    .change_column_transform('the_date').to('to_fixed_timestamp(`date`)')\
    # Make the celsius column all numbers
    .change_column_transform('celsius').to('to_number(`celsius`)')\
    # Add a new column, which is computed from the `celsius` column
    .add_column('fahrenheit', 'Degrees (Fahrenheit)', '(to_number(`celsius`) * (9 / 5)) + 32', 'the temperature in celsius')\
    .run()
```

`change_column_metadata(column_name, column_attribute)` takes the field name used to
identify the column and the column attribute to change (`field_name`, `display_name`, `description`, `position`)

`add_column(field_name, display_name, transform_expression, description)` will create a new column

We can also call `drop_column(celsius)` which will drop the column.

`.run()` will then make a request and return the new output_schema, or an error if something is invalid.

Transforms can be complex SoQL expressions. Available functions are listed [here](http://docs.socratapublishing.apiary.io/#reference/0/inputschema). You can do lots of stuff with them;


For example, you could change all `null` values into errors (which won't be imported) by doing
something like
```python
(ok, new_output_schema) = output
    .change_column_transform('celsius').to('coalesce(to_number(`celsius`), error("Celsius was null!"))')
    .run()
```

Or you could add a new column that says if the day was hot or not
```python
(ok, new_output_schema) = output
    .add_column('is_hot', 'Was the day hot?', 'to_number(`celsius`) >= 23')
    .run()
```

Or you could geocode a column, given the following CSV
```
address,city,zip,state
10028 Ravenna Ave NE, Seattle, 98128, WA
1600 Pennsylvania Avenue, Washington DC, 20500, DC
6511 32nd Ave NW, Seattle, 98155, WA
```

We could transform our first `output_schema` into a single column dataset, where that
single column is a `Point` of the address

```python
(ok, output) = output\
    .add_column('location', 'Incident Location', 'geocode(`address`, `city`, `state`, `zip`)')\
    .drop_column('address')\
    .drop_column('city')\
    .drop_column('state')\
    .drop_column('zip')\
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

<!-- doc -->
# Library Docs

### [Socrata](https://github.com/socrata/socrata-py/blob/master//socrata/__init__.py#L14)
`ArgSpec(args=['self', 'auth'], varargs=None, keywords=None, defaults=None)`

Top level publishing object.

All functions making HTTP calls return a result tuple, where the first element in the
tuple is whether or not the call succeeded, and the second element is the returned
object if it was a success, or a dictionary containing the error response if the call
failed. 2xx responses are considered successes. 4xx and 5xx responses are considered failures.
In the event of a socket hangup, an exception is raised.

#### [create](https://github.com/socrata/socrata-py/blob/master//socrata/__init__.py#L64)
`ArgSpec(args=['self'], varargs=None, keywords='kwargs', defaults=None)`

Shortcut to create a dataset. Returns a `Create` object,
which contains functions which will create a view, upload
your file, and validate data quality in one step.

To actually place the validated data into a view, you can call .apply()
on the revision
```
(revision, output_schema) Socrata(auth).create(
    name = "cool dataset",
    description = "a description"
).csv(file)

(ok, job) = revision.apply(output_schema = output_schema)
```

Args:
```
   **kwargs: Arbitrary revision metadata values
```

Returns:
```
    result (Revision, OutputSchema): Returns the revision that was created and the OutputSchema created from your uploaded file
```

Examples:
```python
Socrata(auth).create(
    name = "cool dataset",
    description = "a description"
).csv(open('my-file.csv'))
```

#### [new](https://github.com/socrata/socrata-py/blob/master//socrata/__init__.py#L104)
`ArgSpec(args=['self', 'metadata'], varargs=None, keywords=None, defaults=None)`

Create an empty revision, on a view that doesn't exist yet. The
view will be created for you, and the initial revision will be returned.

Args:
```
    metadata (dict): Metadata to apply to the revision
```

Returns:
```
    result (bool, Revision | dict): Returns an API Result; the Revision if it was created or an API Error response
```

Examples:
```python
    (ok, rev) = Socrata(auth).new({
        'name': 'hi',
        'description': 'foo!',
        'metadata': {
            'view': 'metadata',
            'anything': 'is allowed here'

        }
    })
```

#### [using_config](https://github.com/socrata/socrata-py/blob/master//socrata/__init__.py#L33)
`ArgSpec(args=['self', 'config_name', 'view'], varargs=None, keywords=None, defaults=None)`

Update a dataset, using the configuration that you previously
created, and saved the name of. Takes the `config_name` parameter
which uniquely identifies the config, and the `View` object, which can
be obtained from `socrata.views.lookup('view-id42')`

Args:
```
    config_name (str): The config name
    view (View): The view to update
```

Returns:
```
    result (Revision, Job): Returns the Revision and the Job, which is now running
```

Examples:
```
    with open('my-file.csv', 'rb') as my_file:
        (rev, job) = p.using_config(name, view).csv(my_file)
```

### [Authorization](https://github.com/socrata/socrata-py/blob/master//socrata/authorization.py#L3)
`ArgSpec(args=['self', 'domain', 'username', 'password'], varargs=None, keywords=None, defaults=None)`

Manages basic authorization for accessing the socrata API.
This is passed into the `Socrata` object once, which is the entry
point for all operations.

    auth = Authorization(
        "data.seattle.gov",
        os.environ['SOCRATA_USERNAME'],
        os.environ['SOCRATA_PASSWORD']
    )
    publishing = Socrata(auth)

#### [live_dangerously](https://github.com/socrata/socrata-py/blob/master//socrata/authorization.py#L26)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Disable SSL checking. Note that this should *only* be used while developing
against a local Socrata instance.

### [Revisions](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L9)
`ArgSpec(args=['self', 'fourfour', 'auth'], varargs=None, keywords=None, defaults=None)`



#### [create_replace_revision](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L49)
`ArgSpec(args=['self', 'metadata'], varargs=None, keywords=None, defaults=({},))`

Create a revision on the view, which when applied, will replace the data.

Args:
```
    metadata (dict): The metadata to change; these changes will be applied when the revision
        is applied
```
Returns:
```
    result (bool, dict | Revision): The new revision, or an error
```
Examples:
```
    >>> view.revisions.create_replace_revision({'name': 'new dataset name', 'description': 'updated description'})
```

#### [create_update_revision](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L69)
`ArgSpec(args=['self', 'metadata'], varargs=None, keywords=None, defaults=({},))`

Create a revision on the view, which when applied, will update the data
rather than replacing it.

This is an upsert; if there is a rowId defined and you have duplicate ID values,
those rows will be updated. Otherwise they will be appended.

Args:
```
    metadata (dict): The metadata to change; these changes will be applied when the revision is applied
```

Returns:
```
    result (bool, dict | Revision): The new revision, or an error
```

Examples:
```python
    view.revisions.create_update_revision({
        'name': 'new dataset name',
        'description': 'updated description'
    })
```

#### [create_using_config](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L138)
`ArgSpec(args=['self', 'config'], varargs=None, keywords=None, defaults=None)`

Create a revision for the given dataset.

#### [list](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L34)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

List all the revisions on the view

Returns:
```
    result (bool, dict | list[Revision])
```

#### [lookup](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L119)
`ArgSpec(args=['self', 'revision_seq'], varargs=None, keywords=None, defaults=None)`

Lookup a revision within the view based on the sequence number

Args:
```
    revision_seq (int): The sequence number of the revision to lookup
```

Returns:
```
    result (bool, dict | Revision): The Revision resulting from this API call, or an error
```

### [Revision](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L151)
`ArgSpec(args=['self', 'auth', 'response', 'parent'], varargs='args', keywords='kwargs', defaults=(None,))`

A revision is a change to a dataset

#### [apply](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L230)
`ArgSpec(args=['self', 'uri', 'output_schema'], varargs=None, keywords=None, defaults=(None,))`

Apply the Revision to the view that it was opened on

Args:
```
    output_schema (OutputSchema): Optional output schema. If your revision includes
        data changes, this should be included. If it is a metadata only revision,
        then you will not have an output schema, and you do not need to pass anything
        here
```

Returns:
```
    result (bool, dict | Job): Returns the job that is being run to apply the revision
```

Examples:
```
(ok, job) = revision.apply(output_schema = my_output_schema)
```

#### [create_upload](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L156)
`ArgSpec(args=['self', 'filename'], varargs=None, keywords=None, defaults=None)`

Create an upload within this revision

Args:
```
    filename (str): The name of the file to upload
```
Returns:
```
    result (bool, dict | Source): The Source created by this API call, or an error
```

#### [discard](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L189)
`ArgSpec(args=['self', 'uri'], varargs=None, keywords=None, defaults=None)`

Discard this open revision.

Returns:
```
    result (bool, dict | Revision): The closed Revision or an error
```

#### [list_operations](https://github.com/socrata/socrata-py/blob/master//socrata/resource.py#L144)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get a list of the operations that you can perform on this
object. These map directly onto what's returned from the API
in the `links` section of each resource

#### [open_in_browser](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L290)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Open this revision in your browser, this will open a window

#### [ui_url](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L275)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

This is the URL to the landing page in the UI for this revision

Returns:
```
    url (str): URL you can paste into a browser to view the revision UI
```

#### [update](https://github.com/socrata/socrata-py/blob/master//socrata/revisions.py#L201)
`ArgSpec(args=['self', 'uri', 'meta'], varargs=None, keywords=None, defaults=None)`

Set the metadata to be applied to the view
when this revision is applied

Args:
```
    metadata (dict): The changes to make to this revision
```

Returns:
```
    result (bool, dict | Revision): The updated Revision as a result of this API call, or an error
```

Examples:
```python
    (ok, revision) = revision.update({
        'name': 'new name',
        'description': 'new description'
    })
```

### [Sources](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L8)
`ArgSpec(args=['self', 'auth'], varargs=None, keywords=None, defaults=None)`



#### [create_upload](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L9)
`ArgSpec(args=['self', 'filename'], varargs=None, keywords=None, defaults=None)`

Create a new source. Takes a `body` param, which must contain a `filename`
of the file.

Args:
```
    filename (str): The name of the file you are uploading
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    (ok, upload) = revision.create_upload('foo.csv')
```

### [Source](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L44)
`ArgSpec(args=['self', 'auth', 'response', 'parent'], varargs='args', keywords='kwargs', defaults=(None,))`

Uploads bytes into the source. Requires content_type argument
be set correctly for the file handle. It's advised you don't
use this method directly, instead use one of the csv, xls, xlsx,
or tsv methods which will correctly set the content_type for you.

#### [add_to_revision](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L209)
`ArgSpec(args=['self', 'uri', 'revision'], varargs=None, keywords=None, defaults=None)`

Associate this Source with the given revision.

#### [change_parse_option](https://github.com/socrata/socrata-py/blob/master//socrata/builders/parse_options.py#L15)
`ArgSpec(args=['self', 'name'], varargs=None, keywords=None, defaults=None)`

Change a parse option on the source.

If there are not yet bytes uploaded, these parse options will be used
in order to parse the file.

If there are already bytes uploaded, this will trigger a re-parsing of
the file, and consequently a new InputSchema will be created. You can call
`source.latest_input()` to get the newest one.

Parse options are:
header_count (int): the number of rows considered a header
column_header (int): the one based index of row to use to generate the header
encoding (string): defaults to guessing the encoding, but it can be explicitly set
column_separator (string): For CSVs, this defaults to ",", and for TSVs "       ", but you can use a custom separator
quote_char (string): Character used to quote values that should be escaped. Defaults to """

Args:
```
    name (string): One of the options above, ie: "column_separator" or "header_count"
```

Returns:
```
    change (ParseOptionChange): implements a `.to(value)` function which you call to set the value
```

For our example, assume we have this dataset

```
This is my cool dataset
A, B, C
1, 2, 3
4, 5, 6
```

We want to say that the first 2 rows are headers, and the second of those 2
rows should be used to make the column header. We would do that like so:

Examples:
```python
    (ok, source) = source            .change_parse_option('header_count').to(2)            .change_parse_option('column_header').to(2)            .run()
```

#### [csv](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L74)
`ArgSpec(args=['self', 'file_handle'], varargs=None, keywords=None, defaults=None)`

Upload a CSV, returns the new input schema.

Args:
```
    file_handle: The file handle, as returned by the python function `open()`
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    with open('my-file.csv', 'rb') as f:
        (ok, upload) = upload.csv(f)
```

#### [df](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L184)
`ArgSpec(args=['self', 'dataframe'], varargs=None, keywords=None, defaults=None)`

Upload a pandas DataFrame, returns the new source.

Args:
```
    file_handle: The file handle, as returned by the python function `open()`
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    import pandas
    df = pandas.read_csv('test/fixtures/simple.csv')
    (ok, upload) = upload.df(df)
```

#### [list_operations](https://github.com/socrata/socrata-py/blob/master//socrata/resource.py#L144)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get a list of the operations that you can perform on this
object. These map directly onto what's returned from the API
in the `links` section of each resource

#### [shapefile](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L162)
`ArgSpec(args=['self', 'file_handle'], varargs=None, keywords=None, defaults=None)`

Upload a Shapefile, returns the new input schema.

Args:
```
    file_handle: The file handle, as returned by the python function `open()`
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    with open('my-shapefile-archive.zip', 'rb') as f:
        (ok, upload) = upload.shapefile(f)
```

#### [tsv](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L140)
`ArgSpec(args=['self', 'file_handle'], varargs=None, keywords=None, defaults=None)`

Upload a TSV, returns the new input schema.

Args:
```
    file_handle: The file handle, as returned by the python function `open()`
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    with open('my-file.tsv', 'rb') as f:
        (ok, upload) = upload.tsv(f)
```

#### [xls](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L96)
`ArgSpec(args=['self', 'file_handle'], varargs=None, keywords=None, defaults=None)`

Upload an XLS, returns the new input schema

Args:
```
    file_handle: The file handle, as returned by the python function `open()`
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    with open('my-file.xls', 'rb') as f:
        (ok, upload) = upload.xls(f)
```

#### [xlsx](https://github.com/socrata/socrata-py/blob/master//socrata/sources.py#L118)
`ArgSpec(args=['self', 'file_handle'], varargs=None, keywords=None, defaults=None)`

Upload an XLSX, returns the new input schema.

Args:
```
    file_handle: The file handle, as returned by the python function `open()`
```

Returns:
```
    result (bool, Source | dict): Returns an API Result; the new Source or an error response
```

Examples:
```python
    with open('my-file.xlsx', 'rb') as f:
        (ok, upload) = upload.xlsx(f)
```

### [Configs](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L8)
`ArgSpec(args=['self', 'auth'], varargs=None, keywords=None, defaults=None)`



#### [create](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L14)
`ArgSpec(args=['self', 'name', 'data_action', 'parse_options', 'columns'], varargs=None, keywords=None, defaults=(None, None))`

Create a new ImportConfig. See http://docs.socratapublishing.apiary.io/
ImportConfig section for what is supported in `data_action`, `parse_options`,
and `columns`.

#### [list](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L41)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

List all the ImportConfigs on this domain

#### [lookup](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L32)
`ArgSpec(args=['self', 'name'], varargs=None, keywords=None, defaults=None)`

Obtain a single ImportConfig by name

### [Config](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L50)
`ArgSpec(args=['self', 'auth', 'response', 'parent'], varargs='args', keywords='kwargs', defaults=(None,))`



#### [change_parse_option](https://github.com/socrata/socrata-py/blob/master//socrata/builders/parse_options.py#L15)
`ArgSpec(args=['self', 'name'], varargs=None, keywords=None, defaults=None)`

Change a parse option on the source.

If there are not yet bytes uploaded, these parse options will be used
in order to parse the file.

If there are already bytes uploaded, this will trigger a re-parsing of
the file, and consequently a new InputSchema will be created. You can call
`source.latest_input()` to get the newest one.

Parse options are:
header_count (int): the number of rows considered a header
column_header (int): the one based index of row to use to generate the header
encoding (string): defaults to guessing the encoding, but it can be explicitly set
column_separator (string): For CSVs, this defaults to ",", and for TSVs "       ", but you can use a custom separator
quote_char (string): Character used to quote values that should be escaped. Defaults to """

Args:
```
    name (string): One of the options above, ie: "column_separator" or "header_count"
```

Returns:
```
    change (ParseOptionChange): implements a `.to(value)` function which you call to set the value
```

For our example, assume we have this dataset

```
This is my cool dataset
A, B, C
1, 2, 3
4, 5, 6
```

We want to say that the first 2 rows are headers, and the second of those 2
rows should be used to make the column header. We would do that like so:

Examples:
```python
    (ok, source) = source            .change_parse_option('header_count').to(2)            .change_parse_option('column_header').to(2)            .run()
```

#### [create_revision](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L68)
`ArgSpec(args=['self', 'uri', 'fourfour'], varargs=None, keywords=None, defaults=None)`

Create a new Revision in the context of this ImportConfig.
Sources that happen in this Revision will take on the values
in this Config.

#### [delete](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L51)
`ArgSpec(args=['self', 'uri'], varargs=None, keywords=None, defaults=None)`

Delete this ImportConfig. Note that this cannot be undone.

#### [list_operations](https://github.com/socrata/socrata-py/blob/master//socrata/resource.py#L144)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get a list of the operations that you can perform on this
object. These map directly onto what's returned from the API
in the `links` section of each resource

#### [update](https://github.com/socrata/socrata-py/blob/master//socrata/configs.py#L57)
`ArgSpec(args=['self', 'uri', 'body'], varargs=None, keywords=None, defaults=None)`

Mutate this ImportConfig in place. Subsequent revisions opened against this
ImportConfig will take on its new value.

### [InputSchema](https://github.com/socrata/socrata-py/blob/master//socrata/input_schema.py#L7)
`ArgSpec(args=['self', 'auth', 'response', 'parent'], varargs='args', keywords='kwargs', defaults=(None,))`

This represents a schema exactly as it appeared in the source

#### [get_latest_output_schema](https://github.com/socrata/socrata-py/blob/master//socrata/input_schema.py#L38)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Note that this does not make an API request

Returns:
    output_schema (OutputSchema): Returns the latest output schema

#### [latest_output](https://github.com/socrata/socrata-py/blob/master//socrata/input_schema.py#L25)
`ArgSpec(args=['self', 'uri'], varargs=None, keywords=None, defaults=None)`

Get the latest (most recently created) OutputSchema
which descends from this InputSchema

Returns:
    result (bool, OutputSchema | dict): Returns an API Result; the new OutputSchema or an error response

#### [list_operations](https://github.com/socrata/socrata-py/blob/master//socrata/resource.py#L144)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get a list of the operations that you can perform on this
object. These map directly onto what's returned from the API
in the `links` section of each resource

#### [transform](https://github.com/socrata/socrata-py/blob/master//socrata/input_schema.py#L11)
`ArgSpec(args=['self', 'uri', 'body'], varargs=None, keywords=None, defaults=None)`

Transform this InputSchema into an Output. Returns the
new OutputSchema. Note that this call is async - the data
may still be transforming even though the OutputSchema is
returned. See OutputSchema.wait_for_finish to block until
the

### [OutputSchema](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L41)
`ArgSpec(args=['self'], varargs='args', keywords='kwargs', defaults=None)`

This is data as transformed from an InputSchema

#### [add_column](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L198)
`ArgSpec(args=['self', 'field_name', 'display_name', 'transform_expr', 'description'], varargs=None, keywords=None, defaults=(None,))`

Add a column

Args:
```
    field_name (str): The column's field_name, must be unique
    display_name (str): The columns display name
    transform_expr (str): SoQL expression to evaluate and fill the column with data from
    description (str): Optional column description
```

Returns:
```
    output_schema (OutputSchema): Returns self for easy chaining
```

Examples:
```python
(ok, new_output_schema) = output
    # Add a new column, which is computed from the `celsius` column
    .add_column('fahrenheit', 'Degrees (Fahrenheit)', '(to_number(`celsius`) * (9 / 5)) + 32', 'the temperature in celsius')
    # Add a new column, which is computed from the `celsius` column
    .add_column('kelvin', 'Degrees (Kelvin)', '(to_number(`celsius`) + 273.15')
    .run()
```

#### [any_failed](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L69)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Whether or not any transform in this output schema has failed

#### [build_config](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L52)
`ArgSpec(args=['self', 'uri', 'name', 'data_action'], varargs=None, keywords=None, defaults=None)`

Create a new ImportConfig from this OutputSchema. See the API
docs for what an ImportConfig is and why they're useful

#### [change_column_metadata](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L261)
`ArgSpec(args=['self', 'field_name', 'attribute'], varargs=None, keywords=None, defaults=None)`

Change the column metadata. This returns a ColumnChange,
which implements a `.to` function, which takes the new value to change to

Args:
```
    field_name (str): The column to change
    attribute (str): The attribute of the column to change
```

Returns:
```
    change (TransformChange): The transform change, which implements the `.to` function
```

Examples:
```python
    (ok, new_output_schema) = output
        # Change the field_name of date to the_date
        .change_column_metadata('date', 'field_name').to('the_date')
        # Change the description of the celsius column
        .change_column_metadata('celsius', 'description').to('the temperature in celsius')
        # Change the display name of the celsius column
        .change_column_metadata('celsius', 'display_name').to('Degrees (Celsius)')
        .run()
```

#### [change_column_transform](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L291)
`ArgSpec(args=['self', 'field_name'], varargs=None, keywords=None, defaults=None)`

Change the column transform. This returns a TransformChange,
which implements a `.to` function, which takes a transform expression.

Args:
```
    field_name (str): The column to change
```

Returns:
```
    change (TransformChange): The transform change, which implements the `.to` function
```

Examples:
```python
    (ok, new_output_schema) = output
        .change_column_transform('the_date').to('to_fixed_timestamp(`date`)')
        # Make the celsius column all numbers
        .change_column_transform('celsius').to('to_number(`celsius`)')
        # Add a new column, which is computed from the `celsius` column
        .add_column('fahrenheit', 'Degrees (Fahrenheit)', '(to_number(`celsius`) * (9 / 5)) + 32', 'the temperature in celsius')
        .run()
```

#### [drop_column](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L237)
`ArgSpec(args=['self', 'field_name'], varargs=None, keywords=None, defaults=None)`

Drop the column

Args:
```
    field_name (str): The column to drop
```

Returns:
```
    output_schema (OutputSchema): Returns self for easy chaining
```

Examples:
```python
    (ok, new_output_schema) = output
        .drop_column('foo')
        .run()
```

#### [list_operations](https://github.com/socrata/socrata-py/blob/master//socrata/resource.py#L144)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get a list of the operations that you can perform on this
object. These map directly onto what's returned from the API
in the `links` section of each resource

#### [rows](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L115)
`ArgSpec(args=['self', 'uri', 'offset', 'limit'], varargs=None, keywords=None, defaults=(0, 500))`

Get the rows for this OutputSchema. Acceps `offset` and `limit` params
for paging through the data.

#### [run](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L319)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Run all adds, drops, and column changes.


Returns:
```
    result (bool, OutputSchema | dict): Returns an API Result; the new OutputSchema or an error response
```

Examples:
```python
    (ok, new_output_schema) = output
        # Change the field_name of date to the_date
        .change_column_metadata('date', 'field_name').to('the_date')
        # Change the description of the celsius column
        .change_column_metadata('celsius', 'description').to('the temperature in celsius')
        # Change the display name of the celsius column
        .change_column_metadata('celsius', 'display_name').to('Degrees (Celsius)')
        # Change the transform of the_date column to to_fixed_timestamp(`date`)
        .change_column_transform('the_date').to('to_fixed_timestamp(`date`)')
        # Make the celsius column all numbers
        .change_column_transform('celsius').to('to_number(`celsius`)')
        # Add a new column, which is computed from the `celsius` column
        .add_column('fahrenheit', 'Degrees (Fahrenheit)', '(to_number(`celsius`) * (9 / 5)) + 32', 'the temperature in celsius')
        .run()
```

#### [schema_errors](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L123)
`ArgSpec(args=['self', 'uri', 'offset', 'limit'], varargs=None, keywords=None, defaults=(0, 500))`

Get the errors that resulted in transforming into this output schema.
Accepts `offset` and `limit` params

#### [schema_errors_csv](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L130)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get the errors that results in transforming into this output schema
as a CSV stream.

Note that this returns an (ok, Reponse) tuple, where Reponse
is a python requests Reponse object

#### [set_row_id](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L171)
`ArgSpec(args=['self', 'field_name'], varargs=None, keywords=None, defaults=(None,))`

Set the row id. Note you must call `validate_row_id` before doing this.

Args:
```
    field_name (str): The column to set as the row id
```

Returns:
```
    result (bool, OutputSchema | dict): Returns an API Result; the new OutputSchema or an error response
```

Examples:
```python
(ok, new_output_schema) = output.set_row_id('the_id_column')
```

#### [validate_row_id](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L145)
`ArgSpec(args=['self', 'uri', 'field_name'], varargs=None, keywords=None, defaults=None)`

Set the row id. Note you must call `validate_row_id` before doing this.

Args:
```
    field_name (str): The column to validate as the row id
```

Returns:
```
    result (bool, dict): Returns an API Result; where the response says if it can be used as a row id
```

#### [wait_for_finish](https://github.com/socrata/socrata-py/blob/master//socrata/output_schema.py#L75)
`ArgSpec(args=['self', 'progress', 'timeout', 'sleeptime'], varargs=None, keywords=None, defaults=(<function noop at 0x7fc1ccb286a8>, None, 1))`

Wait for this dataset to finish transforming and validating. Accepts a progress function
and a timeout.

### [Job](https://github.com/socrata/socrata-py/blob/master//socrata/job.py#L5)
`ArgSpec(args=['self', 'auth', 'response', 'parent'], varargs='args', keywords='kwargs', defaults=(None,))`



#### [is_complete](https://github.com/socrata/socrata-py/blob/master//socrata/job.py#L6)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Has this job finished or failed

#### [list_operations](https://github.com/socrata/socrata-py/blob/master//socrata/resource.py#L144)
`ArgSpec(args=['self'], varargs=None, keywords=None, defaults=None)`

Get a list of the operations that you can perform on this
object. These map directly onto what's returned from the API
in the `links` section of each resource

#### [wait_for_finish](https://github.com/socrata/socrata-py/blob/master//socrata/job.py#L13)
`ArgSpec(args=['self', 'progress'], varargs=None, keywords=None, defaults=(<function noop at 0x7fc1ccb286a8>,))`

Wait for this job to finish applying to the underlying
dataset
<!-- docstop -->
