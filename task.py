import os
import random

random.seed(None)

NUM_HOSTS = 4

HOSTS = {
    1: '127.0.0.1',
    2: '127.0.0.1',
    3: '127.0.0.1',
    4: '127.0.0.1',
}


def host_for_blob(blob_hash):
    # return random.randint(1, NUM_HOSTS)
    return int(int(blob_hash[0], 16) / len(HOSTS)) + 1


def process_blob(blob_path):
    if not os.path.isfile(blob_path):
        raise OSError(blob_path + " does not exist")

    blob_hash = os.path.basename(blob_path)
    host = host_for_blob(blob_hash)
    print "Copying blob %s to host %d (%s)" % (blob_hash[0:8], host, HOSTS[host])

    # copy blob to host

    # check that copy succeeded

    os.remove(blob_path)
