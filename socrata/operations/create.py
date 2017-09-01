from socrata.operations.utils import get_filename, SocrataException
from socrata.operations.operation import Operation

class Create(Operation):
    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)

        (ok, rev) = self.publish.new(self.properties['metadata'])
        if not ok:
            raise SocrataException("Failed to create the view and revision", view)

        (ok, source) = rev.create_upload(filename)
        if not ok:
            raise SocrataException("Failed to create the upload", source)

        (ok, source) = put_bytes(source)
        if not ok:
            raise SocrataException("Failed to upload the file", source)

        output_schema = source.get_latest_input_schema().get_latest_output_schema()

        (ok, output_schema) = output_schema.wait_for_finish()
        if not ok:
            raise SocrataException("The dataset failed to validate")

        return (rev, output_schema)
