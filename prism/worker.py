import sys
from redis import Redis
from rq import Connection, Worker
from prism.config import get_settings

settings = get_settings()

def main():
    with Connection(Redis(settings['redis server'])):
        qs = ['default']
        w = Worker(qs)
        w.work()

if __name__ == "__main__":
    sys.exit(main())
