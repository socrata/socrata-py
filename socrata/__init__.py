from socrata.resource import Collection
from socrata.sources import Sources
from socrata.configs import Configs
from socrata.views import Views
from socrata.revisions import Revisions
from socrata.operations.configured_job import ConfiguredJob
from socrata.operations.create import Create

class Socrata(Collection):
    """
    Top level publishing object.

    All functions making HTTP calls return a result tuple, where the first element in the
    tuple is whether or not the call succeeded, and the second element is the returned
    object if it was a success, or a dictionary containing the error response if the call
    failed. 2xx responses are considered successes. 4xx and 5xx responses are considered failures.
    In the event of a socket hangup, an exception is raised.
    """
    def __init__(self, auth):
        """
        See the `Authorization` class docs for info on how to construct an auth object.
        """
        super(Socrata, self).__init__(auth)
        self.views = Views(auth)
        self.sources = Sources(auth)
        self.configs = Configs(auth)

    def using_config(self, config_name, view):
        """
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
            result (ConfiguredJob): Returns the ConfiguredJob
        ```

        Note:
            Typical usage would be in a context manager block (as demonstrated in the example
            below). In this case, the `ConfiguredJob` is created and immediately launched by way of
            the call to the `ConfiguredJob.csv` method.

        Examples:
        ```
            with open('my-file.csv', 'rb') as my_file:
                (rev, job) = p.using_config(name, view).csv(my_file)
        ```

        """
        config = self.configs.lookup(config_name)
        return ConfiguredJob(self, view=view, config=config)

    def create(self, **kwargs):
        """
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

        job = revision.apply(output_schema = output_schema)
        ```

        Args:
        ```
           **kwargs: Arbitrary revision metadata values
        ```

        Returns:
        ```
            result (Revision, OutputSchema): Returns the revision that was created and the
                OutputSchema created from your uploaded file
        ```

        Examples:
        ```python
        Socrata(auth).create(
            name = "cool dataset",
            description = "a description"
        ).csv(open('my-file.csv'))
        ```

        """
        return Create(self, metadata=kwargs)

    def new(self, metadata, deleted_at = None):
        """
        Create an empty revision, on a view that doesn't exist yet. The
        view will be created for you, and the initial revision will be returned.

        Args:
        ```
            metadata (dict): Metadata to apply to the revision
        ```

        Returns:
        ```
            Revision
        ```

        Examples:
        ```python
            rev = Socrata(auth).new({
                'name': 'hi',
                'description': 'foo!',
                'metadata': {
                    'view': 'metadata',
                    'anything': 'is allowed here'

                }
            })
        ```
        """
        return Revisions.new(self.auth, metadata, deleted_at)


__all__ = [
  "authorization",
  "configs",
  "input_schema",
  "output_schema",
  "revisions",
  "sources",
  "upsert_job"
]
