from socrata.resource import Collection
from socrata.sources import Sources
from socrata.configs import Configs
from socrata.views import Views
from socrata.operations.configured_job import ConfiguredJob
from socrata.operations.create import Create

import json
import requests


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
        """
        (ok, config) = result = self.configs.lookup(config_name)
        if not ok:
            raise SocrataException("Failed to lookup config %s" % config_name, result)
        return ConfiguredJob(self, view = view, config = config)


    def create(self, **kwargs):
        """
        Shortcut to create a dataset. Returns a `Create` object,
        which contains functions which will create a view, source
        your file, and validate data quality in one step.
        """
        return Create(self, **kwargs)


__all__ = [
  "authorization",
  "configs",
  "input_schema",
  "output_schema",
  "revisions",
  "sources",
  "upsert_job"
]
