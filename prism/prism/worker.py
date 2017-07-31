import os
import sys

import prism
from prism.config import get_settings
from prism.task import process_blob

from redis import Redis
from rq import Queue

settings = get_settings()

BLOB_DIR = settings['blob directory']


def main():
    redis_conn = Redis()
    q = Queue(connection=redis_conn)

    if not os.path.isdir(BLOB_DIR):
        os.mkdir(BLOB_DIR)

    blobs = os.listdir(BLOB_DIR)

    for blob_hash in blobs:
        print "Process ", blob_hash
        q.enqueue(process_blob, blob_hash)


if __name__ == "__main__":
    sys.exit(main())
