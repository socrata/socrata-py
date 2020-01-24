from copy import copy

class ParseOptionChange(object):
    def __init__(self, name, obj):
        self._name = name
        self._obj = obj
        self._obj.parse_option_changes = getattr(self._obj, 'parse_option_changes', [])

    def to(self, value):
        self._obj.parse_option_changes.append((self._name, value))
        return self._obj


class ParseOptionBuilder(object):
    def change_parse_option(self, name):
        """
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
        column_separator (string): For CSVs, this defaults to ",", and for TSVs "\t", but you can use a custom separator
        quote_char (string): Character used to quote values that should be escaped. Defaults to "\""

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
            source = source\
            .change_parse_option('header_count').to(2)\
            .change_parse_option('column_header').to(2)\
            .run()
        ```

        """
        return ParseOptionChange(name, self)

    def run(self):
        parse_options = copy(self.attributes['parse_options'])
        parse_options.update({key: value for key, value in self.parse_option_changes})

        self.parse_option_changes = []
        return self.update({
            'parse_options': parse_options
        })
