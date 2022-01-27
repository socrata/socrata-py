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

class SortChange(object):
    def __init__(self, output_schema):
        self._output_schema = output_schema
        self._accumulated_columns = []

    def on(self, field_name, ascending = True):
        self._accumulated_columns.append({ 'field_name': field_name, 'ascending': ascending })
        return self

    def end_sort(self):
        self._output_schema.new_sort_by = self._accumulated_columns
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
        self.new_sort_by = None

    def build_config(self, uri, name, data_action):
        """
        Create a new ImportConfig from this OutputSchema. See the API
        docs for what an ImportConfig is and why they're useful
        """
        res = post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'name': name,
                'data_action': data_action
            })
        )
        return Config(self.auth, res, None)

    def any_failed(self):
        """
        This is probably not the function you are looking for.

        This returns whether or not any transform in this output schema has failed. "Failed" in this
        case means an internal error (which is unexpected), not a data error (which is expected). This
        function will wait for processing to complete if it hasn't yet.

        For data errors:

            Tell whether or not there are data errors
                output_schema.any_errors()
            Get the count of data errors
                output_schema.attributes['error_count']
            Get the errors themselves
                output_schema.schema_errors(offset = 0, limit = 20)
        """
        return any([column['transform']['failed_at'] for column in self.attributes['output_columns']])


    def any_errors(self):
        self.wait_for_finish()
        """
        Whether or not any transform returned a data error in this schema. This
        function will wait for processing to complete if it hasn't yet.
        """

        return self.attributes['error_count'] > 0


    def wait_for_finish(self, progress = noop, timeout = 10800, sleeptime = 1):
        """
        Wait for this dataset to finish transforming and validating. Accepts a progress function
        and a timeout.

        Default timeout is 3 hours
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
        resp = get(
            self.path(uri),
            params = {'limit': limit, 'offset': offset},
            auth = self.auth
        )

        rows = resp[1:]
        return [self._munge_row(row) for row in rows]

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

        Note that this returns a Reponse, where Reponse
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
            boolean
        ```
        """
        output_column = [oc for oc in self.attributes['output_columns'] if oc['field_name'] == field_name]
        if len(output_column):
            [output_column] = output_column
            transform_id = output_column['transform']['id']

            return get(
                self.path(uri.format(transform_id = transform_id)),
                auth = self.auth
            )['valid']
        else:
            return False

    def set_row_id(self, field_name = None):
        """
        Set the row id. Note you must call `validate_row_id` before doing this.

        Args:
        ```
            field_name (str): The column to set as the row id
        ```

        Returns:
        ```
            OutputSchema
        ```

        Examples:
        ```python
        new_output_schema = output.set_row_id('the_id_column')
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
        new_output_schema = output
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
            new_output_schema = output
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
            new_output_schema = output
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
            new_output_schema = output
                .change_column_transform('the_date').to('to_fixed_timestamp(`date`)')
                # Make the celsius column all numbers
                .change_column_transform('celsius').to('to_number(`celsius`)')
                # Add a new column, which is computed from the `celsius` column
                .add_column('fahrenheit', 'Degrees (Fahrenheit)', '(to_number(`celsius`) * (9 / 5)) + 32', 'the temperature in celsius')
                .run()
        ```
        """
        return TransformChange(field_name, self)

    def set_sort_by(self):
        """
        Replace the columns used to sort the dataset. This returns a SortChange,
        which implements a `.on` function to add a sort and a `.end_sort` function
        to finish.

        If you do not call this, the OutputSchema will try to preserve any existing
        sorts, which means it will remove sorts on deleted columns or on columns
        whose transforms are changed.

        Returns:
        ```
            change (SortChange): The sort change, which implements the `.on` and `.end_sort` functions
        ```

        Examples:
        ```python
            new_output_schema = output
                .set_sort_by()
                .on('column_one', ascending = True)
                .on('column_two', ascending = False)
                .on('column_three') # ascending = True is the default
                .end_sort()
                .run()
        ```
        """
        return SortChange(self)

    def run(self):
        """
        Run all adds, drops, and column changes.


        Returns:
        ```
            OutputSchema
        ```

        Examples:
        ```python
            new_output_schema = output
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

        if self.new_sort_by is not None:
            sort_bys = deepcopy(self.new_sort_by)
        else:
            sort_bys = [
                sb for sb in deepcopy(self.attributes['sort_bys'])
                if not (sb['field_name'] in self.column_deletions)
            ]

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

            new_column = change_fun(deepcopy(column))

            # if there is an id, this means the column is not brand new
            if 'id' in column:
                new_column['initial_output_column_id'] = column['id']

            def replace_with(c):
                if c == column:
                    return new_column
                return c

            columns = [replace_with(c) for c in columns]

            if self.new_sort_by is None:
                converted_sort_bys = []
                for sort_by in sort_bys:
                    if sort_by['field_name'] == column['field_name']:
                        sort_by['field_name'] = new_column['field_name']
                        # If the transform changed, remove the sort
                        # since we can't be sure it's valid anymore...
                        if column['transform']['transform_expr'] == new_column['transform']['transform_expr']:
                            converted_sort_bys.append(sort_by)
                    else:
                        converted_sort_bys.append(sort_by)
                sort_bys = converted_sort_bys

        columns = sorted(columns, key = lambda x: x['position'])

        for p, c in enumerate(columns):
            c['position'] = p + 1

        if self.new_sort_by is not None:
            # Validate that the new sort doesn't name a column that
            # doesn't exist in the light of all the other changes.
            for sb in sort_bys:
                if not any(column['field_name'] == sb['field_name'] for column in columns):
                    raise ValueError('Column `%s` does not exist to sort on' % sb['field_name'])

        desired_schema = {
            'output_columns': columns,
            'sort_bys': sort_bys
        }
        self.column_additions = []
        self.column_deletions = []
        self.column_changes = []
        self.new_sort_by = None
        return self.parent.transform(desired_schema)
