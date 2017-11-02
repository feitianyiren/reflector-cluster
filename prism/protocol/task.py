import os
import sys
import time
import logging
import random

from redis import Redis
from rq import Queue
from redis.exceptions import ConnectionError
from rq.timeouts import JobTimeoutException

from twisted.internet import defer, task

from prism.storage.storage import ClusterStorage, get_redis_connection
from prism.config import get_settings

settings = get_settings()
BLOB_DIR = os.path.expandvars(settings['blob directory'])
REDIS_ADDRESS = settings['redis server']
SETTINGS = get_settings()
HOSTS = SETTINGS['hosts']
NUM_HOSTS = len(HOSTS) - 1
TCP_CONNECT_TIMEOUT = 15

log = logging.getLogger(__name__)

def retry_redis(fn):
    def _wrapper(*a, **kw):
        try:
            return fn(*a, **kw)
        except ConnectionError:
            log.error("%s failed, retrying", fn)
            time.sleep(10)
            return fn(*a, **kw)
    return _wrapper


def next_host(redis_conn):
    host_info = {}
    for host in HOSTS:
        if ":" in host:
            address, port = host.split(":")
            port = int(port)
        else:
            address, port = host, 5566
        count = redis_conn.scard(address)
        if count < settings['max blobs']:
            host_info["%s:%i" % (address, port)] = count

    host = random.choice(host_info.keys())
    address, port = host.split(":")
    blob_count = host_info[host]
    return address, int(port), blob_count


def get_blob_path(blob_hash, blob_storage):
    return os.path.join(blob_storage.db_dir, blob_hash)

@defer.inlineCallbacks
def update_sent_blobs(blob_hashes_sent, host, blob_storage):
    for blob_hash in blob_hashes_sent:
        log.info("updating sent blob %s", blob_hash)
        res = yield blob_storage.add_blob_to_host(blob_hash, host)
        blob_path = get_blob_path(blob_hash, blob_storage)
        if os.path.isfile(blob_path):
            log.debug('removing %s', blob_path)
            os.remove(blob_path)

def connect_factory(host, port, factory, blob_storage, hash_to_process):
    from twisted.internet import reactor
    @defer.inlineCallbacks
    def on_finish(result):
        log.info("Finished sending %s", hash_to_process)
        yield update_sent_blobs(factory.p.blob_hashes_sent, host, blob_storage)
        connection.disconnect()
        reactor.fireSystemEvent("shutdown")

    @defer.inlineCallbacks
    def on_error(error):
        log.error("Error when sending %s: %s. Hashes sent %s", hash_to_process, error,
                                                            factory.p.blob_hashes_sent)
        yield update_sent_blobs(factory.p.blob_hashes_sent, host, blob_storage)
        connection.disconnect()
        reactor.fireSystemEvent("shutdown")

    def on_connection_fail(result):
        log.error("Failed to connect to %s:%s", host, port)
        reactor.fireSystemEvent("shutdown")

    def _error(failure):
        log.error("Failed on_connection_lost_d callback: %s", failure)
        reactor.fireSystemEvent("shutdown")

    factory.on_connection_lost_d.addCallbacks(on_finish, on_error)
    factory.on_connection_lost_d.addErrback(_error)

    factory.on_connection_fail_d.addCallback(on_connection_fail)
    try:
        log.debug("Connecting factory to %s:%s", host, port)
        connection = reactor.connectTCP(host, port, factory, timeout=TCP_CONNECT_TIMEOUT)
    except JobTimeoutException:
        log.error("Failed to forward %s --> %s", hash_to_process[:8], host)
        return sys.exit(0)
    except Exception as err:
        log.exception("Job (pid %s) encountered unexpected error")
        return sys.exit(1)

def factory_setup_error(error):
    from twisted.internet import reactor
    log.error("Error when setting up factory:%s",error)
    reactor.fireSystemEvent("shutdown")
    return sys.exit(1)

def process_blob(blob_hash, db_dir, client_factory_class, redis_address, host_infos=None, setup_d=None):
    log.debug("process blob pid %s", os.getpid())
    if host_infos is None:
        host, port, host_blob_count = next_host(get_redis_connection(redis_address))
    else:
        host, port, host_blob_count = host_infos
    blob_storage = ClusterStorage(db_dir, redis_address)

    from twisted.internet import reactor
    if setup_d is not None:
        d = setup_d()
    else:
        d = defer.succeed(True)
    d.addCallback(lambda _: client_factory_class(blob_hash, blob_storage))
    d.addErrback(factory_setup_error)
    d.addCallback(lambda factory: connect_factory(host, port, factory, blob_storage, blob_hash))
    reactor.run()
    return sys.exit(0)


def process_stream(sd_hash, db_dir, client_factory_class, redis_address, host_infos=None, setup_d=None):
    log.debug("processing stream pid %s", os.getpid())
    if host_infos is None:
        host, port, host_blob_count = next_host(get_redis_connection(redis_address))
    else:
        host, port, host_blob_count = host_infos
    blob_storage = ClusterStorage(db_dir, redis_address)
    from twisted.internet import reactor
    if setup_d is not None:
        d = setup_d()
    else:
        d = defer.succeed(True)
    d.addCallback(lambda _: client_factory_class(sd_hash, blob_storage, host))
    d.addErrback(factory_setup_error)
    d.addCallback(lambda factory: connect_factory(host, port, factory, blob_storage, sd_hash))
    reactor.run()
    return sys.exit(0)


@retry_redis
def enqueue_stream(sd_hash, num_blobs_in_stream, db_dir, client_factory_class, redis_address=settings['redis server'],
                   host_infos=None):

    timeout = (num_blobs_in_stream+1)*30
    redis_connection = get_redis_connection(redis_address)
    q = Queue(connection=redis_connection)
    q.enqueue(process_stream, sd_hash, db_dir, client_factory_class, redis_address, host_infos, timeout=timeout)

@retry_redis
def enqueue_blob(blob_hash, db_dir, client_factory_class, redis_address=settings['redis server'], 
                    host_infos=None):

    redis_connection = get_redis_connection(redis_address)
    q = Queue(connection=redis_connection)
    q.enqueue(process_blob, blob_hash, db_dir, client_factory_class, redis_address, host_getter, timeout=60)
