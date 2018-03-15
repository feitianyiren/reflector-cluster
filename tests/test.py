import hashlib
import os
import sys

from prism import config
from prism.protocol.task import process_blobs

from redis import Redis
from rq import Queue

settings = config.get_settings()

BLOB_DIR = os.path.expandvars(settings["blob directory"])
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
        blobs.append(blob_hash)

    q.enqueue(process_blobs, blobs, 1)


if __name__ == "__main__":
    sys.exit(main())
