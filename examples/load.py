from multiprocessing.dummy import Pool as ThreadPool
import os
from os import path
from socrata.authorization import Authorization
from socrata.publish import Publish
from time import sleep
import sys
import time
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
    try:
        with open(path, 'r') as f:
            for i, _ in enumerate(csv.reader(f)):
                pass
        return i
    except:
        return -1

# domain = 'pete-test.test-socrata.com'
# auth = Authorization(
#   domain,
#   os.environ['SOCRATA_USER'],
#   os.environ['SOCRATA_PASSWORD']
# )
domain = 'localhost'
auth = Authorization(
  domain,
  os.environ['SOCRATA_LOCAL_USER'],
  os.environ['SOCRATA_LOCAL_PASS']
)


def up(arg):
    p = Publish(auth)
    base, filename = arg
    fourfour = None
    try:
        file = path.join(base, filename)
        (ok, view) = p.new({
            'name' : 'load'
        })
        assert ok

        fourfour = view['id']
        (ok, rev) = p.revisions.create(fourfour)
        assert ok


        (ok, upload) = rev.create_upload({'filename': filename})

        total_row_count = row_count(file)
        print("Start on %s" % filename)
        started = time.time()
        with open(file, 'rb') as f:
            (ok, input_schema) = upload.csv(f) #

            if not ok:
                print("=" * 80)
                print("Failure! %s" % input_schema)
                print("=" * 80)
                return

            input_schema.show()

        link = "https://{domain}/dataset/fuzzy/{fourfour}/updates/{seq}/uploads/{upload}/schemas/{input_schema}/output/{output_schema}".format(
            domain = domain,
            fourfour = fourfour,
            seq = input_schema.parent.parent.attributes['update_seq'],
            upload = input_schema.parent.attributes['id'],
            input_schema = input_schema.attributes['id'],
            output_schema = input_schema.attributes['output_schemas'][0]['id']
        )
        elapsed = time.time() - started
        print("Done with %s %s, %s rps" % (filename, fourfour, total_row_count / elapsed))
    except Exception as e:
        print("\n")
        print("=" * 80)
        print ("Failed on %s %s" % (filename, fourfour))
        print(e)
        print("=" * 80)

def main():
    base = sys.argv[1]
    files = os.listdir(base)

    # files = [files[0] for _ in range(0, 16)]

    pool = ThreadPool(1)
    results = pool.map(up, [(base, filename) for filename in files])



if __name__ == '__main__':
    main()
