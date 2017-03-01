import os
from src.authorization import Authorization
from src.publish import Publish
from time import sleep
import sys

def write_progress(progresses):
    def k(wat):
        (label, _c, _t) = wat
        return label
    progresses = sorted(progresses, key = k)
    for _ in progresses:
        sys.stdout.write('\x1b[1A')

    for label, current, total in progresses:
        bar_len = 60
        filled_len = int(round(bar_len * current / float(total)))

        percents = round(100.0 * current / float(total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write('[%s] %s%s | %s (%s / %s)\r\n' % (bar, percents, '%', label, current, total))

    sys.stdout.flush()

def row_count(path):
    with open(path) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

auth = Authorization(
  "localhost",
  os.environ['SOCRATA_LOCAL_USER'],
  os.environ['SOCRATA_LOCAL_PASS']
)

fourfour = "ij46-xpxe"

# path = '/home/chris/Downloads/311_Service_Requests_from_2010_to_Present.csv'
path = '/home/chris/Downloads/Seattle_Real_Time_Fire_911_Calls.csv'

def main():
    p = Publish(auth) #
    (ok, rev) = p.revisions.create(fourfour) #
    assert ok
    (ok, upload) = rev.create_upload({'filename': "311.csv"}) #

    total_row_count = row_count(path)

    def upload_progress(p):
        write_progress(
            [('Rows Uploaded', p['end_row_offset'], total_row_count)]
        )

    with open(path, 'rb') as f:
        print("Starting...\n")
        (ok, input_schema) = upload.csv(f, progress = upload_progress) #
        assert ok

        input_schema.show()

        print("\nStarting transform...")

        columns = [
            {
                "field_name": "latitude",
                "display_name": "Latitude",
                "position": 0,
                "description": "latitude",
                "transform": {
                    "transform_expr": "to_fixed_timestamp(`latitude`)"
                }
            },
            {
                "field_name": "longitude",
                "display_name": "Longitude",
                "position": 1,
                "description": "longitude",
                "transform": {
                    "transform_expr": "to_fixed_timestamp(`longitude`)"
                }
            }
        ]

        for _ in columns:
            print('')

        total = input_schema.attributes['total_rows']
        ps = {c['field_name']: 0 for c in columns}

        finished = 0
        def transform_progress(event):
            nonlocal finished
            name = event['column']['field_name']

            if event['type'] == 'max_ptr':
                now = event['end_row_offset']
                old = ps[name]
                ps[name] = max(now, old)
                write_progress(
                    [(field_name, current, total) for (field_name, current) in ps.items()]
                )
            elif event['type'] == 'finished':
                write_progress(
                    [(field_name, total, total) for (field_name, current) in ps.items()]
                )
                finished += 1

        (ok, output_schema) = input_schema.transform(
            {'output_columns': columns},
            progress = transform_progress
        )

        assert ok, "Failed to transform %s" % output_schema

        while finished < len(columns):
            sleep(1)

    print("\n\nDone!\n\n")

if __name__ == '__main__':
    main()
