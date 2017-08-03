import os
import random
from redis import Redis

from prism.forward import forward_blobs
from prism.config import get_settings

random.seed(None)

SETTINGS = get_settings()
HOSTS = SETTINGS['hosts']
NUM_HOSTS = len(HOSTS)
BLOB_DIRECTORY = SETTINGS['blob directory']


def host_for_blob(blob_hash):
    host = HOSTS[random.randint(1, NUM_HOSTS)]
    if ":" in host:
        address, port = host.split(":")
    else:
        address, port = host, 5566
    return address, int(port)


def process_blob(blob_hash):
    redis_conn = Redis()
    blob_path = os.path.join(BLOB_DIRECTORY, blob_hash)
    if not os.path.isfile(blob_path):
        raise OSError(blob_hash + " does not exist")

    host, port = host_for_blob(blob_hash)
    blobs_sent = forward_blobs(BLOB_DIRECTORY, host, port, blob_hash)

    if blobs_sent[0] == blob_hash:
        redis_conn.sadd(host, blob_hash)
        redis_conn.sadd("cluster_blobs", blob_hash)
        os.remove(blob_path)
        print "Forwarded %s --> %s" % (blob_hash[:8], host)
    else:
        print "Failed to forward %s --> %s" % (blob_hash[:8], host)
