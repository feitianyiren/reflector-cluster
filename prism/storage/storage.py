import os
import logging

from prism.config import get_settings
from prism.constants import BLOB_HASH_LENGTH
from prism.error import AlreadyStarted, InvalidBlobHashError
from prism.protocol.blob import BlobFile
from prism.storage import txredisapi as redis
from twisted.internet import defer, reactor

log = logging.getLogger(__name__)

conf = get_settings()

# table names
BLOB_HASHES = "blob_hashes"
CLUSTER_BLOBS = "cluster_blobs"

# set of node addresses
CLUSTER_NODE_ADDRESSES = conf['hosts']
MAX_BLOBS_PER_HOST = conf['max blobs']


class ClusterStorage(object):
    def __init__(self, path=None):
        self.db = None
        self.db_dir = path or conf['blob directory']
        if not os.path.isdir(self.db_dir):
            raise OSError("blob storage directory \"%s\" does not exist" % self.db_dir)
        reactor.addSystemEventTrigger("after", "startup", self.start)

    @defer.inlineCallbacks
    def start(self):
        if self.db is None:
            self.db = yield redis.ConnectionPool()
            reactor.addSystemEventTrigger("before", "shutdown", self.stop)
        else:
            raise AlreadyStarted()
        defer.returnValue(None)

    @defer.inlineCallbacks
    def stop(self):
        if self.db:
            yield self.db.disconnect()
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_host_counts(self):
        result = {}
        for addr in CLUSTER_NODE_ADDRESSES:
            result[addr] = yield self.db.scard(addr)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def blob_exists(self, blob_hash):
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        exists = yield self.db.hexists(BLOB_HASHES, blob_hash)
        defer.returnValue(exists)

    @defer.inlineCallbacks
    def blob_in_cluster(self, blob_hash):
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        in_cluster = yield self.db.sismember(CLUSTER_BLOBS, blob_hash)
        defer.returnValue(in_cluster)

    @defer.inlineCallbacks
    def get_blob(self, blob_hash, length=None):
        if length is None:
            known = yield self.blob_exists(blob_hash)
            if known:
                length = yield self.db.hget(BLOB_HASHES, blob_hash)
            else:
                raise InvalidBlobHashError()
        blob = BlobFile(self.db_dir, blob_hash, length)
        defer.returnValue(blob)

    @defer.inlineCallbacks
    def delete(self, blob_hash):
        log.info("Delete %s", blob_hash)
        exists = yield self.blob_exists(blob_hash)
        if exists:
            blob_length = yield self.db.hget(BLOB_HASHES, blob_hash)
            blob = BlobFile(self.db_dir, blob_hash, blob_length)
            yield blob.delete()
            was_deleted = yield self.db.hdel(BLOB_HASHES, blob_hash)
            defer.returnValue(was_deleted)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def completed(self, blob_hash, blob_length):
        exists = yield self.blob_exists(blob_hash)
        if not exists:
            if len(blob_hash) != BLOB_HASH_LENGTH:
                raise InvalidBlobHashError()
            was_set = yield self.db.hset(BLOB_HASHES, blob_hash, blob_length)
            defer.returnValue(was_set)
        else:
            defer.returnValue(True)
