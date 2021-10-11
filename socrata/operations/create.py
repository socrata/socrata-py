from socrata.operations.utils import get_filename
from socrata.operations.operation import Operation

class Create(Operation):

    def set_deleted_at(self, date):
        """
        :param date: should have datetime type
        :return: self Create class
        """
        self._deleted_at = date
        return self

    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)
        optional_deleted_at = self._deleted_at if hasattr(self, '_deleted_at') else None
        rev = self.publish.new(self.properties['metadata'], optional_deleted_at)
        source = rev.create_upload(filename)
        source = put_bytes(source)
        output_schema = source.get_latest_input_schema().get_latest_output_schema()
        output_schema = output_schema.wait_for_finish()
        return (rev, output_schema)
