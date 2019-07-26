import json
import requests
from socrata.http import post, put, delete, get
from socrata.resource import Collection, Resource
from socrata.sources import Source
from socrata.job import Job
import webbrowser

class Revisions(Collection):
    def __init__(self, fourfour, auth):
        self.auth = auth
        self.fourfour = fourfour


    def path(self):
        return 'https://{domain}/api/publishing/v1/revision/{fourfour}'.format(
            domain = self.auth.domain,
            fourfour = self.fourfour
        )

    def _create(self, action_type, metadata, permission):
        body = {
            'metadata': metadata,
            'action': {
                'type': action_type,
                'permission': permission
            }
        }
        return self._subresource(Revision, post(
            self.path(),
            auth = self.auth,
            data = json.dumps(body)
        ))

    def list(self):
        """
        List all the revisions on the view

        Returns:
        ```
            result (bool, dict | list[Revision])
        ```
        """
        return self._subresources(Revision, get(
            self.path(),
            auth = self.auth
        ))


    def create_replace_revision(self, metadata = {}, permission = 'public'):
        """
        Create a revision on the view, which when applied, will replace the data.

        Args:
        ```
            metadata (dict): The metadata to change; these changes will be applied when the revision
                is applied
            permission (string): 'public' or 'private'
        ```
        Returns:
        ```
            result (bool, dict | Revision): The new revision, or an error
        ```
        Examples:
        ```
            >>> view.revisions.create_replace_revision(metadata = {'name': 'new dataset name', 'description': 'updated description'})
        ```
        """
        return self._create('replace', metadata, permission)

    def create_update_revision(self, metadata = {}, permission = 'public'):
        """
        Create a revision on the view, which when applied, will update the data
        rather than replacing it.

        This is an upsert; if there is a rowId defined and you have duplicate ID values,
        those rows will be updated. Otherwise they will be appended.

        Args:
        ```
            metadata (dict): The metadata to change; these changes will be applied when the revision is applied
            permission (string): 'public' or 'private'
        ```

        Returns:
        ```
            result (bool, dict | Revision): The new revision, or an error
        ```

        Examples:
        ```python
            view.revisions.create_update_revision(metadata = {
                'name': 'new dataset name',
                'description': 'updated description'
            })
        ```
        """
        return self._create('update', metadata, permission)

    def create_delete_revision(self, metadata = {}, permission = 'public'):
        """
        Create a revision on the view, which when applied, will delete rows of data.

        This is an upsert; a row id must be set.

        Args:
        ```
            metadata (dict): The metadata to change; these changes will be applied when the revision is applied
            permission (string): 'public' or 'private'
        ```

        Returns:
        ```
            result (bool, dict | Revision): The new revision, or an error
        ```

        Examples:
        ```python
            view.revisions.create_delete_revision(metadata = {
                'name': 'new dataset name',
                'description': 'description'
            })
        ```
        """
        return self._create('delete', metadata, permission)

    @staticmethod
    def new(auth, metadata):
        path = 'https://{domain}/api/publishing/v1/revision'.format(
            domain = auth.domain,
        )

        (ok, response) = result = post(
            path,
            auth = auth,
            data = json.dumps({
                'action': {
                    'type': 'update'
                },
                'metadata': metadata
            })
        )

        if not ok:
            return result

        return (ok, Revision(auth, response))

    def lookup(self, revision_seq):
        """
        Lookup a revision within the view based on the sequence number

        Args:
        ```
            revision_seq (int): The sequence number of the revision to lookup
        ```

        Returns:
        ```
            result (bool, dict | Revision): The Revision resulting from this API call, or an error
        ```
        """
        return self._subresource(Revision, get(
            self.path() + '/' + str(revision_seq),
            auth = self.auth
        ))

    def create_using_config(self, config):
        """
        Create a revision for the given dataset.
        """
        return self._subresource(Revision, post(
            self.path(),
            auth = self.auth,
            data = json.dumps({
                'config': config.attributes['name']
            })
        ))


class Revision(Resource):
    """
    A revision is a change to a dataset
    """

    def create_upload(self, filename, parse_options = {}):
        """
        Create an upload within this revision

        Args:
        ```
            filename (str): The name of the file to upload
        ```
        Returns:
        ```
            result (bool, dict | Source): The Source created by this API call, or an error
        ```
        """
        return self.create_source({
            'type': 'upload',
            'filename': filename
        }, parse_options)

    def source_from_url(self, url, parse_options = {}):
        """
        Create a URL source

        Args:
        ```
            url (str): The URL to create the dataset from
        ```
        Returns:
        ```
            result (bool, dict | Source): The Source created by this API call, or an error
        ```
        """
        return self.create_source({
            'type': 'url',
            'url': url
        }, parse_options)

    def source_from_dataset(self, parse_options = {}):
        """
        Create a dataset source within this revision
        """
        return self.create_source({
            'type': 'view',
            'fourfour': self.view_id()
        }, parse_options)

    def source_from_agent(self, agent_uid, namespace, path, parse_options = {}):
        """
        Create a source from a connection agent in this revision
        """
        return self.create_source({
          'type': 'connection_agent',
          'agent_uid': agent_uid,
          'namespace': namespace,
          'path': path
        }, parse_options)

    def source_as_blob(self, filename, parse_options = {}):
        """
        Create a source from a file that should remain unparsed
        """
        return self.create_upload(filename, parse_options.update({'parse_source': False}))

    def create_source(self, uri, source_type, parse_options = {}):
        return self._subresource(Source, post(
            self.path(uri),
            auth = self.auth,
            data = json.dumps({
                'source_type' : source_type,
                'parse_options' : parse_options
            })
        ))

    def list_sources(self, uri):
        return self._subresources(Source, get(
            self.path(uri),
            auth = self.auth
        ))

    def get_output_schema(self):
        (ok, _) = rev_result = self.show()
        if not ok:
            return rev_result

        output_schema_id = self.attributes['output_schema_id']
        if not output_schema_id:
            return (True, None)

        (ok, sources) = source_result = self.list_sources()
        if not ok:
            return source_result

        output_schemas = [o_s for source in sources for i_s in source.input_schemas for o_s in i_s.output_schemas]
        output_schema = None

        for o_s in output_schemas:
            if o_s.attributes['id'] == output_schema_id:
                output_schema = o_s

        return (True, output_schema)

    def set_output_schema(self, output_schema_id):
        """
        Set the output schema id on the revision. This is what will get applied when
        the revision is applied if no ouput schema is explicitly supplied

        Args:
        ```
            output_schema_id (int): The output schema id
        ```

        Returns:
        ```
            result (bool, dict | Revision): The updated Revision as a result of this API call, or an error
        ```

        Examples:
        ```python
            (ok, revision) = revision.set_output_schema(42)
        ```
        """
        return self.update({'output_schema_id': output_schema_id})


    def discard(self, uri):
        """
        Discard this open revision.

        Returns:
        ```
            result (bool, dict | Revision): The closed Revision or an error
        ```
        """
        return delete(self.path(uri), auth = self.auth)

    def plan(self, uri):
        """
        Return the list of operations this revision will make when it is applied

        Returns:
        ```
            result (bool, dict): The revision plan
        ```
        """
        (ok, resource) = result = get(self.path(uri), auth = self.auth)
        if ok:
            return (ok, resource['resource'])
        return result


    def update(self, uri, body):
        """
        Set the metadata to be applied to the view
        when this revision is applied

        Args:
        ```
            body (dict): The changes to make to this revision
        ```

        Returns:
        ```
            result (bool, dict | Revision): The updated Revision as a result of this API call, or an error
        ```

        Examples:
        ```python
            (ok, revision) = revision.update({
                'metadata': {
                    'name': 'new name',
                    'description': 'new description'
                }
            })
        ```
        """
        return self._mutate(put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body),
        ))

    def apply(self, uri, output_schema = None):
        """
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
        """

        if output_schema:
            if not output_schema.attributes['finished_at']:
                (ok, source) = result = output_schema.parent.parent.show()
                if not ok:
                    return result

                source_type = source.attributes['source_type']
                if source_type['type'] == 'view' and not source_type['loaded']:
                    pass
                else:
                    (ok, output_schema) = result = output_schema.wait_for_finish()
                    if not ok:
                        return result

        body = {}

        if output_schema:
            body.update({
                'output_schema_id': output_schema.attributes['id']
            })

        result = self._subresource(Job, put(
            self.path(uri),
            auth = self.auth,
            data = json.dumps(body)
        ))

        self.show() # To mutate ourself and get the job to show up in our attrs

        return result

    def ui_url(self):
        """
        This is the URL to the landing page in the UI for this revision

        Returns:
        ```
            url (str): URL you can paste into a browser to view the revision UI
        ```
        """
        return "https://{domain}/d/{fourfour}/revisions/{seq}".format(
            domain = self.auth.domain,
            fourfour = self.attributes["fourfour"],
            seq = self.attributes["revision_seq"]
        )

    def open_in_browser(self):
        """
        Open this revision in your browser, this will open a window
        """
        webbrowser.open(self.ui_url(), new = 2)

    def view_id(self):
        return self.attributes["fourfour"]
