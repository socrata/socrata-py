import os
from src.authorization import Authorization
from src.publish import Publish
from tqdm import tqdm
from time import sleep

auth = Authorization(
  "localhost",
  os.environ['SOCRATA_LOCAL_USER'],
  os.environ['SOCRATA_LOCAL_PASS']
)

fourfour = "ij46-xpxe"

path = '/home/chris/Downloads/311_Service_Requests_from_2010_to_Present.csv'
# path = '/home/chris/Downloads/Police_small.csv'

def main():
    p = Publish(auth) #
    (ok, rev) = p.revisions.create(fourfour) #
    assert ok
    (ok, upload) = rev.create_upload({'filename': "311.csv"}) #

    total_size = os.stat(path).st_size
    bar = tqdm(total = total_size, unit='B', desc = 'File Upload', unit_scale=True)
    def upload_progress(p):
        bar.update(p['bytes'])

    with open(path, 'rb') as f:
        (ok, input_schema) = upload.csv(f, progress = upload_progress) #
        assert ok

        input_schema.show()
        bar.close()

        columns = [
            {
                "field_name": "latitude",
                "display_name": "Latitude",
                "position": 0,
                "description": "latitude",
                "transform": {
                    "transform_expr": "`latitude`::text"
                }
            },
            {
                "field_name": "longitude",
                "display_name": "Longitude",
                "position": 1,
                "description": "longitude",
                "transform": {
                    "transform_expr": "`longitude`::text"
                }
            }
        ]

        total = input_schema.attributes['total_rows']
        prevs = {}
        bars = {column['field_name'] : tqdm(total = total, unit = 'row', desc = column['field_name']) for column in columns}

        finished = 0
        def transform_progress(event):
            nonlocal finished
            name = event['column']['field_name']
            bar = bars[name]
            prev = prevs.get(name, 0)
            if event['type'] == 'max_ptr':
                now = event['end_row_offset']
                bar.update(max(now - prev, 0))
                prevs[name] = now
            elif event['type'] == 'finished':
                bar.update(total - prev)
                finished += 1


        (ok, output_schema) = input_schema.transform({
            'output_columns': columns},
            progress = transform_progress
        )

        assert ok, "Failed to transform %s" % output_schema

        while finished < 2:
            sleep(1)

    sleep(1)
    print("\n\nDone\n\n")

if __name__ == '__main__':
    main()
