from socrata.operations.utils import get_filename, SocrataException
from socrata.operations.operation import Operation

class Create(Operation):
    def run(self, data, put_bytes, filename = None):
        filename = get_filename(data, filename)

        (ok, view) = self.publish.views.create(self.properties)
        if not ok:
            raise SocrataException("Failed to create the view", view)

        (ok, rev) = view.revisions.create_update_revision()
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

        return (view, rev, out)
