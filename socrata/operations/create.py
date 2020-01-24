from socrata.operations.utils import get_filename
from socrata.operations.operation import Operation

class Create(Operation):
    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)
        rev = self.publish.new(self.properties['metadata'])
        source = rev.create_upload(filename)
        source = put_bytes(source)
        output_schema = source.get_latest_input_schema().get_latest_output_schema()
        output_schema = output_schema.wait_for_finish()
        return (rev, output_schema)
