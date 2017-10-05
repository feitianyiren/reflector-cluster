# Use this script redistribute blobs from specified host into
# the rest of the cluster.
#
# The blobs must already be moved from host into prism blob directory
# via rsync before running this script
#
# Use in case host(s) died and we want to redistribute its blobs
# to other hosts
#
# python redistribute_blobs.py [host1, host2, ... ]
#


from prism.storage.storage import ClusterStorage, SD_BLOB_HASHES
from prism.config import get_settings
from prism.protocol.task import enqueue_stream
from prism.protocol.factory import build_prism_stream_client_factory
from twisted.internet import reactor,defer
from redis import Redis

import sys
import os
import subprocess
import time
import json

# this turns on logging
from twisted.python import log
import sys
log.startLogging(sys.stdout)

conf = get_settings()
hosts = conf['hosts']
storage = ClusterStorage()
prism_location = 'test-prism'
start_time = time.time()

@defer.inlineCallbacks
def migrate_entry(blob_hash, from_host):
    # reset blob to be in prism but not sent to host

    out = yield storage.blob_exists(blob_hash)
    if not out:
        raise Exception('blob {} not found in db'.format(blob_hash))

    out = yield storage.get_blob_host(blob_hash)
    if out != from_host:
        raise Exception('blob {} was not on different host {}'.format(blob_hash, out))

    # set blob so that its no longer in a host
    yield storage.delete_blob_from_host(blob_hash)

    # do some checks here to make sure datbase is fine
    exists = yield storage.blob_exists(blob_hash)
    if not exists:
        raise Exception("failed to check, blob does not exist")
    forwarded = yield storage.blob_has_been_forwarded_to_host(blob_hash)
    if forwarded:
        raise Exception("failed to check, blob is forwarded")

@defer.inlineCallbacks
def migrate_sd_hash(sd_hash, from_host):
    if not os.path.isfile(os.path.join(storage.db_dir,sd_hash)):
        raise Exception("sd hash %s not found"%sd_hash)

    yield migrate_entry(sd_hash, from_host)

    blob_hashes = yield storage.db.smembers(sd_hash)

    # copy blobs
    for blob_hash in blob_hashes:
        if not os.path.isfile(os.path.join(storage.db_dir,sd_hash)):
            raise Exception("blob hash %s not found"%blob_hash)
        yield migrate_entry(blob_hash, from_host)

    # launch task to process stream
    enqueue_stream(sd_hash, len(blob_hashes), storage.db_dir, build_prism_stream_client_factory)
    print("completed enqueuing {}".format(sd_hash))
    defer.returnValue({'sd_hash':sd_hash,'blobs':len(blob_hashes)})

def do_migration(sd_hashes):
    def print_final_result(result):
        num_successes = 0
        num_fails = 0
        num_blobs = 0
        for (success,value) in result:
            if success:
                num_successes+=1
                num_blobs += value['blobs']
                print("Success:{}".format(value))
                print('num success:{}, num fail:{}, total:{}'.format(num_successes,num_fails,len(sd_hashes)))
            else:
                num_fails+=1
                print("Fail:{}".format(value))
        time_taken = time.time() - start_time
        sec_per_blob = num_blobs / time_taken
        print("All Finished! Streams: {} Successes:{}, Fails:{}, Blobs moved:{}, Min to finish:{}, Sec per blob:{}".format(
                len(sd_hashes), num_successes, num_fails, num_blobs, time_taken/60, sec_per_blob))
        reactor.stop()

    ds = []
    sem = defer.DeferredSemaphore(4)
    for host, sd_hash in sd_hashes:
        d = sem.run(migrate_sd_hash, sd_hash, host)
        ds.append(d)

    d = defer.DeferredList(ds,consumeErrors=True)
    d.addCallback(print_final_result)
    reactor.run()

def find_host_sd_hashes(hosts):
    sd_hashes = []
    for host in hosts:
        blob_hashes = storage.db.db.smembers(host)
        for blob_hash in blob_hashes:
            if storage.db.db.sismember(SD_BLOB_HASHES, blob_hash):
                sd_hashes.append((host, blob_hash))
    print("{} sd hashes found".format(len(sd_hashes)))
    return sd_hashes

def run(hosts, sd_hash=None):
    if sd_hash is None:
        sd_hashes = find_host_sd_hashes(hosts)
    else:
        sd_hashes = [sd_hash]
    print("{} streams to process".format(len(sd_hashes)))
    do_migration(sd_hashes)



if __name__ == '__main__':
    hosts = sys.argv[1:]
    print("Processing hosts:{}".format(hosts))
    run(hosts)


