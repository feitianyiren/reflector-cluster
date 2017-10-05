"""
check for info in the cluster

USAGE:
python get_cluster_info.py [blob_hash]

RETURNS:
if blob_hash is specified, return info about the blob,
otherwise return info about the cluster
"""

from prism.storage.storage import ClusterStorage, SD_BLOB_HASHES
from prism.config import get_settings

from twisted.internet import reactor,defer
import sys
import datetime
import json

# this turns on logging
from twisted.python import log
import sys
log.startLogging(sys.stdout)

settings = get_settings()

@defer.inlineCallbacks
def check_blob_hash(blob_hash):
    storage = ClusterStorage()
    blob_exists = yield storage.blob_exists(blob_hash)
    if not blob_exists:
        print("Blob does not exist in cluster")
        reactor.stop()
    
    host = yield storage.get_blob_host(blob_hash)
    timestamp = yield storage.get_blob_timestamp(blob_hash)
    length, _, _= yield storage.db.get_blob(blob_hash)
    timestamp = float(timestamp)
    date_time = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    print("HOST:{} (empty if not in host), Time when blob entered cluster:{}, length:{}".format(host, date_time, length))

    is_sd_blob = yield storage.is_sd_blob(blob_hash)
    if is_sd_blob:
        print("Blob is an SD blob")
        # check if its blobs has been forwarded properly to hosts
        blobs = yield storage.db.get_blobs_for_stream(blob_hash)
        print("{} blobs in stream".format(len(blobs)))
        for blob in blobs:
            blob_exists = yield storage.blob_exists(blob)
            if not blob_exists:
                print("found a blob in stream that does not exist in cluster:{}".format(blob))
                continue
            blob_host = yield storage.get_blob_host(blob)
            if blob_host != host:
                print("found a blob in stream that is in a different host:{}".format(blob_host))
            if len(host) == 0:# if not on host check
                blob = yield storage.get_blob(blob)
                if not blob.verified:
                    print("found unverified blob on host:{}".format(blob))
    else:
        print("Blob is not an SD blob")
    reactor.stop()

@defer.inlineCallbacks
def check_cluster_info():
    storage = ClusterStorage()
    sd_blobs = storage.db.db.smembers(SD_BLOB_HASHES)
    print("Num sd hashes:{}".format(len(sd_blobs)))

    for host in settings['hosts']:
        count = storage.db.db.scard(host)
        print("HOST:{}, BLOB Count:{}".format(host, count))

    num_blobs = 0
    for sd_blob in sd_blobs:
        blobs = yield storage.db.get_blobs_for_stream(sd_blob)
        num_blobs += len(blobs)
    print("Num blobs associated with streams:{}".format(num_blobs))

    unforwarded_sd_blobs = yield storage.get_all_unforwarded_sd_blobs()
    print("Num unforwarded sd blobs:{}".format(len(unforwarded_sd_blobs)))
    num_unforwarded_blobs = 0
    for sd_blob in unforwarded_sd_blobs:
        blobs = yield storage.db.get_blobs_for_stream(sd_blob)
        num_unforwarded_blobs += len(blobs)
    print("Num blobs in unforwarded streams:{}".format(num_unforwarded_blobs))
    reactor.stop()



if __name__ == '__main__':
    if len(sys.argv) >1:
        check_blob_hash(sys.argv[1])
    else:
        check_cluster_info()

    reactor.run()

