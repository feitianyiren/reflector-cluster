import os
import time
import logging
from redis import Redis
from rq import Queue
from redis.exceptions import ConnectionError

from prism.storage.storage import ClusterStorage
from prism.config import get_settings

settings = get_settings()
BLOB_DIR = os.path.expandvars(settings['blob directory'])
SETTINGS = get_settings()
HOSTS = SETTINGS['hosts']
NUM_HOSTS = len(HOSTS) - 1

log = logging.getLogger(__name__)
redis_conn = Redis()


def retry_redis(fn):
    def _wrapper(*a, **kw):
        try:
            return fn(*a, **kw)
        except ConnectionError:
            log.error("%s failed, retrying", fn)
            time.sleep(10)
            return fn(*a, **kw)
    return _wrapper


def next_host(db):
    host_info = {}
    for host in HOSTS:
        if ":" in host:
            address, port = host.split(":")
            port = int(port)
        else:
            address, port = host, 5566
        count = db.scard(address)
        host_info["%s:%i" % (address, port)] = count
    for host, blob_count in sorted(host_info.iteritems(), key=lambda x: x[1]):
        address, port = host.split(":")
        return address, int(port), blob_count


def process_blob(blob_hash, client_factory_class):
    import sys
    from rq.timeouts import JobTimeoutException

    log.debug("process blob pid %s", os.getpid())
    blob_path = os.path.join(BLOB_DIR, blob_hash)
    if not os.path.isfile(blob_path):
        log.warning("%s does not exist", blob_path)
        return sys.exit(0)
    redis_conn = Redis()
    host, port, host_blob_count = next_host(redis_conn)
    factory = client_factory_class(ClusterStorage(BLOB_DIR), [blob_hash])
    try:
        from twisted.internet import reactor
        reactor.connectTCP(host, port, factory)
        reactor.run()
        blobs_sent = factory.p.blob_hashes_sent
        if blobs_sent[0] == blob_hash:
            redis_conn.sadd(host, blob_hash)
            redis_conn.sadd("cluster_blobs", blob_hash)
            if os.path.isfile(blob_path):
                os.remove(blob_path)
            log.info("Forwarded %s --> %s, host has %i blobs", blob_hash[:8], host,
                     host_blob_count)
        return sys.exit(0)
    except JobTimeoutException:
        log.error("Failed to forward %s --> %s", blob_hash[:8], host)
        return sys.exit(0)
    except Exception as err:
        log.exception("Job (pid %s) encountered unexpected error")
        return sys.exit(1)


@retry_redis
def enqueue_blob(blob_hash, client_factory_class):
    q = Queue(connection=redis_conn)
    q.enqueue(process_blob, blob_hash, client_factory_class, timeout=60)
