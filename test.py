import hashlib
import os
import sys

from prism.task import process_blob

from redis import Redis
from rq import Queue

DIR = os.path.dirname(os.path.realpath(__file__))
BLOB_DIR = os.path.join(DIR, "blobs")
BLOB_SIZE = 1024 * 1024 * 2


def main():
    redis_conn = Redis()
    q = Queue(connection=redis_conn)

    if not os.path.isdir(BLOB_DIR):
        os.mkdir(BLOB_DIR)

    try:
        num_blobs = int(sys.argv[1])
    except IndexError:
        num_blobs = 1

    blobs = []

    for i in range(num_blobs):
        blob_contents = os.urandom(BLOB_SIZE)
        blob_hash = hashlib.sha384(blob_contents).hexdigest()
        blob_path = os.path.join(BLOB_DIR, blob_hash)
        with open(blob_path, 'wb') as f:
            f.write(blob_contents)
        blobs.append(blob_path)

    for blob_path in blobs:
        q.enqueue(process_blob, blob_path)


if __name__ == "__main__":
    sys.exit(main())
