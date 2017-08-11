import os
from rq import Connection, Worker
from redis import Redis
from prism.config import get_settings

settings = get_settings()
blob_dir = settings['blob directory']

redis = Redis(host='localhost')


def cleanup_queues():
    qs = redis.smembers("rq:queues")
    to_keep = {"rq:queue:default", "rq:queue:failed"}
    to_remove = list(qs - to_keep)
    if to_remove:
        redis.srem("rq:queues", to_remove)


def need_burst_workers():
    return len(os.listdir(os.path.expandvars(blob_dir))) > 10


def num_burst_workers_needed():
    return 4  # min(max((len(os.listdir(os.path.expandvars(blob_dir))) / 1000), 5), 1)


def main():
    cleanup_queues()
    print "worker ", os.getpid()

    qs = ['default']

    with Connection(connection=redis):
        worker = Worker(qs)
        q = worker.queues[0]
        job_id = worker.get_current_job_id()
        try:
            worker.work(burst=True)
        except:
            q.requeue(job_id)


if __name__ == '__main__':
    main()
