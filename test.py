import sys

import task

from rq import Queue
from redis import Redis


def main():
    redis_conn = Redis()
    q = Queue(connection=redis_conn)
    q.enqueue(task.process_blob, "/path/to/blob/abcdef0123456789")

if __name__ == "__main__":
    sys.exit(main())
