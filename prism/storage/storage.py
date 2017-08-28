import os
import json
import logging
from redis import Redis
from prism.config import get_settings
from prism.constants import BLOB_HASH_LENGTH
from prism.error import InvalidBlobHashError
from prism.protocol.blob import BlobFile
from twisted.internet import defer, threads

log = logging.getLogger(__name__)

conf = get_settings()

# table names
BLOB_HASHES = "blob_hashes"
CLUSTER_BLOBS = "cluster_blobs"

# set of node addresses
CLUSTER_NODE_ADDRESSES = conf['hosts']
MAX_BLOBS_PER_HOST = conf['max blobs']

REDIS_ADDRESS = conf['redis server']


class RedisHelper(object):
    def __init__(self):
        self.db = Redis(REDIS_ADDRESS)

    def hget(self, name, key):
        return threads.deferToThread(self.db.hget, name, key)

    def hset(self, name, key, value):
        return threads.deferToThread(self.db.hset, name, key, value)

    def hdel(self, name, *keys):
        return threads.deferToThread(self.db.hdel, name, *keys)

    def hexists(self, name, key):
        return threads.deferToThread(self.db.hexists, name, key)

    def sismember(self, name, value):
        return threads.deferToThread(self.db.sismember, name, value)

    def smembers(self, name):
        return threads.deferToThread(self.db.smembers, name)

    def sadd(self, name, *values):
        return threads.deferToThread(self.db.sadd, name, *values)


class ClusterStorage(object):
    def __init__(self, path=None):
        self.db = RedisHelper()
        self.db_dir = path or os.path.expandvars(conf['blob directory'])
        if not os.path.isdir(self.db_dir):
            raise OSError("blob storage directory \"%s\" does not exist" % self.db_dir)

    @defer.inlineCallbacks
    def blob_exists(self, blob_hash):
        """True if blob file exists in the cluster"""
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        exists = yield self.db.hexists(BLOB_HASHES, blob_hash)
        defer.returnValue(exists)

    @defer.inlineCallbacks
    def blob_has_been_forwarded_to_host(self, blob_hash):
        """True if the blob has been sent to a host"""
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError(blob_hash)
        sent_to_host = yield self.db.sismember(CLUSTER_BLOBS, blob_hash)
        defer.returnValue(sent_to_host)

    @defer.inlineCallbacks
    def get_needed_blobs_for_stream(self, sd_hash):
        """
        Return a list of known_needed_blobs
        """

        blobs_in_stream = yield self.db.smembers(sd_hash)
        missing_blobs = []

        if blobs_in_stream:
            for blob_hash in blobs_in_stream:
                blob_in_cluster = yield self.blob_has_been_forwarded_to_host(blob_hash)
                if not blob_in_cluster:
                    blob_exists = yield self.blob_exists(blob_hash)
                    if not blob_exists:
                        missing_blobs.append(blob_hash)
        else:
            sd_exists_locally = yield self.blob_exists(sd_hash)
            if sd_exists_locally:
                sd_blob = yield self.get_blob(sd_hash)
                if sd_blob.is_validated():
                    missing_blobs = yield self.determine_missing_local_blobs(sd_blob)
                    defer.returnValue(missing_blobs)
            missing_blobs = None
        defer.returnValue(missing_blobs)

    @defer.inlineCallbacks
    def determine_missing_local_blobs(self, sd_blob):
        needed = []
        decoded_sd = yield self.load_sd_blob(sd_blob)
        for blob_info in decoded_sd['blobs']:
            if 'blob_hash' in blob_info and 'length' in blob_info:
                blob_hash, blob_len = blob_info['blob_hash'], blob_info['length']
                in_cluster = yield self.blob_has_been_forwarded_to_host(blob_hash)
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
        blob_hashes = []
        for blob in decoded_sd_blob['blobs']:
            if 'blob_hash' in blob and 'length' in blob:
                blob_hashes.append(blob['blob_hash'])
        if blob_hashes:
            yield self.db.sadd(sd_blob.blob_hash, *tuple(blob_hashes))
        defer.returnValue(decoded_sd_blob)

    @defer.inlineCallbacks
    def get_blob(self, blob_hash, length=None):
        if length is None:
            length = yield self.db.hget(BLOB_HASHES, blob_hash)
            if length is not None:
                length = int(length)
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
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        was_set = yield self.db.hset(BLOB_HASHES, blob_hash, blob_length)
        defer.returnValue(was_set)
