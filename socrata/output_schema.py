import time
import json
from copy import deepcopy
from socrata.resource import Resource
from socrata.configs import Config
from socrata.http import noop, put, get, post


class ColumnChange(object):
    def __init__(self, field_name, attribute, output_schema):
        self._field_name = field_name
        self._attribute = attribute
        self._output_schema = output_schema

    def to(self, value):
        def change_fun(col):
            col[self._attribute] = value
            return col

        self._output_schema.column_changes.append((self._field_name, change_fun))

        return self._output_schema

class TransformChange(object):
    def __init__(self, field_name, output_schema):
        self._field_name = field_name
        self._output_schema = output_schema

    def to(self, value):
        def change_fun(col):
            col['transform']['transform_expr'] = value
            return col

        self._output_schema.column_changes.append((self._field_name, change_fun))

        return self._output_schema


class OutputSchema(Resource):
    """
        This is data as transformed from an InputSchema
    """

    def __init__(self, *args, **kwargs):
        super(OutputSchema, self).__init__(*args, **kwargs)
        self.column_changes = []
        self.column_additions = []
        self.column_deletions = []

    def build_config(self, uri, name, data_action):
        """
        Create a new ImportConfig from this OutputSchema. See the API
        docs for what an ImportConfig is and why they're useful
        """
        (ok, res) = result = post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'name': name,
                'data_action': data_action
            })
        )
        if ok:
            return (ok, Config(self.auth, res, None))
        return result

    def any_failed(self):
        """
        Whether or not any transform in this output schema has failed
        """
        return any([column['transform']['failed_at'] for column in self.attributes['output_columns']])

    def wait_for_finish(self, progress = noop, timeout = None, sleeptime = 1):
        """
        Wait for this dataset to finish transforming and validating. Accepts a progress function
        and a timeout.
        """
        return self._wait_for_finish(
            is_finished = lambda m: m.attributes['completed_at'],
            is_failed = lambda m: m.any_failed(),
            progress = progress,
            timeout = timeout,
            sleeptime = sleeptime
        )

    def _munge_row(self, row):
        row = row['row']
        columns = sorted(self.attributes['output_columns'], key = lambda oc: oc['position'])
        field_names = [oc['field_name'] for oc in columns]
        return {k: v for k, v in zip(field_names, row)}


    def _get_rows(self, uri, offset, limit):
        ok, resp = get(
            self.path(uri),
            params = {'limit': limit, 'offset': offset},
            auth = self.auth
        )

        if not ok:
            return ok, resp

        rows = resp[1:]

        return (ok, [self._munge_row(row) for row in rows])

    def rows(self, uri, offset = 0, limit = 500):
        """
        Get the rows for this OutputSchema. Acceps `offset` and `limit` params
        for paging through the data.
        """
        return self._get_rows(uri, offset, limit)


    def schema_errors(self, uri, offset = 0, limit = 500):
        """
        Get the errors that resulted in transforming into this output schema.
        Accepts `offset` and `limit` params
        """
        return self._get_rows(uri, offset, limit)

    def schema_errors_csv(self):
        """
        Get the errors that results in transforming into this output schema
        as a CSV stream.

        Note that this returns an (ok, Reponse) tuple, where Reponse
        is a python requests Reponse object
        """
        return get(
            self.path(self.schema_errors_uri),
            auth = self.auth,
            headers = {'accept': 'text/csv', 'content-type': 'text/csv'},
            stream = True
        )

    def validate_row_id(self, uri, field_name):
        """
        Set the row id. Note you must call `validate_row_id` before doing this.

        Args:
        ```
            field_name (str): The column to validate as the row id
        ```

        Returns:
        ```
            result (bool, dict): Returns an API Result; where the response says if it can be used as a row id
        ```
        """
        output_column = [oc for oc in self.attributes['output_columns'] if oc['field_name'] == field_name]
        if len(output_column):
            [output_column] = output_column
            transform_id = output_column['transform']['id']

            return get(
                self.path(uri.format(transform_id = transform_id)),
                auth = self.auth
            )
        else:
            return (False, {"reason": "No column with field_name = %s" % field_name})

    def set_row_id(self, field_name = None):
        """
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
        """
        desired_schema = deepcopy(self.attributes['output_columns'])

        for oc in desired_schema:
            oc['is_primary_key'] = (oc['field_name'] == field_name)

        return self.parent.transform({'output_columns': desired_schema})


    def add_column(self, field_name, display_name, transform_expr, description = None):
        """
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
        """
        position = len(self.attributes['output_columns']) + len(self.column_additions) - len(self.column_deletions)
        self.column_additions.append({
            'field_name': field_name,
            'display_name': display_name,
            'description': description,
            'position': position,
            'transform': {
                'transform_expr': transform_expr
            }
        })
        return self

    def drop_column(self, field_name):
        """
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
        """
        self.column_deletions.append(field_name)
        return self

    def change_column_metadata(self, field_name, attribute):
        """
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
        """
        return ColumnChange(field_name, attribute, self)

    def change_column_transform(self, field_name):
        """
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
        """
        return TransformChange(field_name, self)

    def run(self):
        """
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
        """
        columns = [
            c for c in deepcopy(self.attributes['output_columns'])
            if not (c['field_name'] in self.column_deletions)
        ] + self.column_additions

        for (to_change, change_fun) in self.column_changes:
            column = [
                column
                for column in columns
                if to_change == column['field_name']
            ]
            if not column:
                raise ValueError('Column `%s` does not exist' % to_change)
            else:
                [column] = column

            new_column = change_fun(column)
            new_column['initial_output_column_id'] = column['id']

            def replace_with(c):
                if c == column:
                    return new_column
                return c

            columns = [replace_with(c) for c in columns]

        columns = sorted(columns, key = lambda x: x['position'])

        for p, c in enumerate(columns):
            c['position'] = p + 1

        desired_schema = {
            'output_columns': columns
        }
        self.column_additions = []
        self.column_deletions = []
        self.column_changes = []
        return self.parent.transform(desired_schema)
