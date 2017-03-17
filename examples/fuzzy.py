import os
from os import path
from socrata.authorization import Authorization
from socrata.publish import Publish
from time import sleep
import sys
import csv

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
    with open(path, 'r') as f:
        for i, _ in enumerate(csv.reader(f)):
            pass
    return i

auth = Authorization(
  "pete-test.test-socrata.com",
  os.environ['SOCRATA_USER'],
  os.environ['SOCRATA_PASSWORD']
)

def main():
    p = Publish(auth)

    base = sys.argv[1]
    files = os.listdir(base)

    for filename in files:
        try:
            print("-" * 80)
            file = path.join(base, filename)

            (ok, view) = p.new({
                'name' : 'fuzzy'
            })
            assert ok

            fourfour = view['id']
            (ok, rev) = p.revisions.create(fourfour)
            assert ok

            print(rev.show_uri())

            (ok, upload) = rev.create_upload({'filename': filename})

            total_row_count = row_count(file)

            def upload_progress(p):
                write_progress(
                    [('Rows Uploaded', p['end_row_offset'], total_row_count)]
                )

            with open(file, 'rb') as f:
                print("Starting... %s | %s" % (filename, total_row_count))
                (ok, input_schema) = upload.csv(f) #


                input_schema.show()

            link = "https://pete-test.test-socrata.com/dataset/fuzzy/{fourfour}/updates/{seq}/uploads/{upload}/schemas/{input_schema}/output/{output_schema}".format(
                fourfour = fourfour,
                seq = input_schema.parent.parent.attributes['update_seq'],
                upload = input_schema.parent.attributes['id'],
                input_schema = input_schema.attributes['id'],
                output_schema = input_schema.attributes['output_schemas'][0]['id']
            )
            print(link)
            print("Done with %s %s" % (filename, fourfour))
        except Exception as e:
            print("\n")
            print("=" * 80)
            print ("Failed on %s %s" % (filename, fourfour))
            print(e)
            print("=" * 80)

if __name__ == '__main__':
    main()
