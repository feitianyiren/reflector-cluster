import os
import json
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

    @defer.inlineCallbacks
    def stop(self):
        if self.db:
            yield self.db.disconnect()

    @defer.inlineCallbacks
    def get_host_counts(self):
        result = {}
        for addr in CLUSTER_NODE_ADDRESSES:
            result[addr] = yield self.db.scard(addr)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def blob_exists_locally(self, blob_hash):
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
    def stream_in_cluster(self, sd_hash):
        """
        Return a tuple of (bool) <stream_in_cluster>, (list) <known_needed_blobs>
        """

        in_cluster = yield self.blob_in_cluster(sd_hash)
        needed = []

        if in_cluster:
            blobs_in_stream = yield self.db.smembers(sd_hash)
            for blob_hash in blobs_in_stream:
                blob_in_cluster = yield self.blob_in_cluster(blob_hash)
                if not blob_in_cluster:
                    blob_exists = yield self.blob_exists_locally(blob_hash)
                    if not blob_exists:
                        needed.append(blob_hash)
        else:
            sd_blob = yield self.get_blob(sd_hash)
            if sd_blob.is_validated():
                in_cluster = True
                needed = yield self.determine_missing_local_blobs(sd_blob)

        defer.returnValue((in_cluster, needed))

    @defer.inlineCallbacks
    def determine_missing_local_blobs(self, sd_blob):
        needed = []
        decoded_sd = yield self.load_sd_blob(sd_blob)
        for blob_info in decoded_sd['blobs']:
            if 'blob_hash' in blob_info and 'length' in blob_info:
                blob_hash, blob_len = blob_info['blob_hash'], blob_info['length']
                in_cluster = yield self.blob_in_cluster(blob_hash)
                if not in_cluster:
                    blob = yield self.get_blob(blob_hash, blob_len)
                    if not blob.is_validated():
                        needed.append(blob_hash)
        defer.returnValue(needed)

    @defer.inlineCallbacks
    def load_sd_blob(self, sd_blob):
        with sd_blob.open_for_reading() as sd_file:
            sd_blob_data = sd_file.read()
        decoded_sd_blob = json.loads(sd_blob_data)
        for blob in decoded_sd_blob['blobs']:
            if 'blob_hash' in blob and 'length' in blob:
                blob_hash, blob_len = blob['blob_hash'], blob['length']
                yield self.db.sadd(sd_blob.blob_hash, blob_hash)
        defer.returnValue(decoded_sd_blob)

    @defer.inlineCallbacks
    def get_blob(self, blob_hash, length=None):
        if length is None:
            length = yield self.db.hget(BLOB_HASHES, blob_hash)
        blob = BlobFile(self.db_dir, blob_hash, length)
        defer.returnValue(blob)

    @defer.inlineCallbacks
    def delete(self, blob_hash):
        log.info("Delete %s", blob_hash)
        exists = yield self.blob_exists_locally(blob_hash)
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
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        was_set = yield self.db.hset(BLOB_HASHES, blob_hash, blob_length)
        defer.returnValue(was_set)
