import os
import sys
import time
import logging

from redis import Redis
from rq import Queue
from redis.exceptions import ConnectionError
from rq.timeouts import JobTimeoutException

from twisted.internet import defer

from prism.storage.storage import ClusterStorage
from prism.config import get_settings

settings = get_settings()
BLOB_DIR = os.path.expandvars(settings['blob directory'])
REDIS_ADDRESS = settings['redis server']
SETTINGS = get_settings()
HOSTS = SETTINGS['hosts']
NUM_HOSTS = len(HOSTS) - 1

log = logging.getLogger(__name__)
redis_conn = Redis(REDIS_ADDRESS)


def retry_redis(fn):
    def _wrapper(*a, **kw):
        try:
            return fn(*a, **kw)
        except ConnectionError:
            log.error("%s failed, retrying", fn)
            time.sleep(10)
            return fn(*a, **kw)
    return _wrapper


def next_host():
    host_info = {}
    for host in HOSTS:
        if ":" in host:
            address, port = host.split(":")
            port = int(port)
        else:
            address, port = host, 5566
        count = redis_conn.scard(address)
        host_info["%s:%i" % (address, port)] = count
    for host, blob_count in sorted(host_info.iteritems(), key=lambda x: x[1]):
        address, port = host.split(":")
        return address, int(port), blob_count


def get_blob_path(blob_hash, blob_storage):
    return os.path.join(blob_storage.db_dir, blob_hash)

@defer.inlineCallbacks
def update_sent_blob(blob_hash, host, blob_storage):
    log.info("updating sent blob %s", blob_hash)
    res = yield blob_storage.add_blob_to_host(blob_hash, host)
    blob_path = get_blob_path(blob_hash, blob_storage)
    if os.path.isfile(blob_path):
        log.debug('removing %s', blob_path)
        os.remove(blob_path)

def process_blob(blob_hash, blob_storage, client_factory_class, host_infos):
    host, port, host_blob_count = host_infos
    log.debug("process blob pid %s", os.getpid())
    blob_path = get_blob_path(blob_hash, blob_storage)
    if not os.path.isfile(blob_path):
        log.warning("%s does not exist", blob_path)
        return sys.exit(0)
    factory = client_factory_class(blob_storage, [blob_hash])

    from twisted.internet import reactor
    @defer.inlineCallbacks
    def on_finish(result):
        blobs_sent = factory.p.blob_hashes_sent
        if blobs_sent and blobs_sent[0] == blob_hash:
            log.info("Forwarded %s --> %s, host has %i blobs", blob_hash[:8], host,
                     host_blob_count)
            yield update_sent_blob(blob_hash, host, blob_storage)
        reactor.fireSystemEvent("shutdown")

    factory.on_connection_lost_d.addCallback(on_finish)
    try:
        reactor.connectTCP(host, port, factory)
        reactor.run()
        return sys.exit(0)
    except JobTimeoutException:
        log.error("Failed to forward %s --> %s", blob_hash[:8], host)
        return sys.exit(0)
    except Exception as err:
        log.exception("Job (pid %s) encountered unexpected error")
        return sys.exit(1)


@retry_redis
def enqueue_blob(blob_hash, blob_storage, client_factory_class, host_getter=next_host):
    q = Queue(connection=redis_conn)
    host_infos = host_getter()
    q.enqueue(process_blob, blob_hash, blob_storage, client_factory_class, host_infos, timeout=60)
