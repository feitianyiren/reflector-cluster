import os
import random
from redis import Redis

from prism.forward import forward_blobs
from prism.config import get_settings

random.seed(None)

SETTINGS = get_settings()
HOSTS = SETTINGS['hosts']
NUM_HOSTS = len(HOSTS) - 1
BLOB_DIR = os.path.expandvars(SETTINGS['blob directory'])


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


def process_blob(blob_hash, remaining):
    redis_conn = Redis()
    blob_path = os.path.join(BLOB_DIR, blob_hash)
    if not os.path.isfile(blob_path):
        raise OSError(blob_hash + " does not exist")

    host, port, host_blob_count = next_host(redis_conn)
    blobs_sent = forward_blobs(BLOB_DIR, host, port, blob_hash)

    if blobs_sent[0] == blob_hash:
        redis_conn.sadd(host, blob_hash)
        redis_conn.sadd("cluster_blobs", blob_hash)
        os.remove(blob_path)
        print "Forwarded %s --> %s, %i remaining, host has %i blobs" % (blob_hash[:8], host,
                                                                        remaining, host_blob_count)
    else:
        print "Failed to forward %s --> %s, %i remaining" % (blob_hash[:8], host, remaining)
