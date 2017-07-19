from socrata.operations.utils import get_filename, SocrataException
from socrata.operations.operation import Operation

class ConfiguredJob(Operation):
    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)

        (ok, rev) = self.properties['view'].revisions.create_using_config(
            self.properties['config']
        )
        if not ok:
            raise SocrataException("Failed to create the revision", rev)

        (ok, source) = rev.create_upload(filename)
        if not ok:
            raise SocrataException("Failed to create the upload", source)

        (ok, inp) = put_bytes(source)
        if not ok:
            raise SocrataException("Failed to upload the file", inp)

        (ok, out) = inp.latest_output()
        if not ok:
            raise SocrataException("Failed to get the parsed dataset")

        (ok, out) = out.wait_for_finish()
        if not ok:
            raise SocrataException("The dataset failed to validate")

        (ok, job) = rev.apply(output_schema = out)
        if not ok:
            raise SocrataException("Failed to apply the change", job)
        return (rev, job)
