import os
import json
import logging
import time
from redis import Redis

from lbrynet.blob.blob_file import BlobFile
from lbrynet.core.utils import is_valid_blobhash
from lbrynet import conf
conf.initialize_settings() #needed since BlobFile needs nconf

from twisted.internet import defer, threads

from prism.config import get_settings
from prism.constants import BLOB_HASH_LENGTH
from prism.error import InvalidBlobHashError

log = logging.getLogger(__name__)

conf = get_settings()

# table names
# contains all blob hashes (including SD blob hashes), value is json encoded length, timestamp, host
BLOB_HASHES = "blob_hashes"
# contains blob hases that have been sent to a reflector node
CLUSTER_BLOBS = "cluster_blobs"
# contain all SD blob hashes
SD_BLOB_HASHES = "sd_blob_hashes"
# each sd_blob_hash is its own table, stores blobs is stream
# each host is its own table, stores all blob hashes it has



# set of node addresses
CLUSTER_NODE_ADDRESSES = conf['hosts']
MAX_BLOBS_PER_HOST = conf['max blobs']

REDIS_ADDRESS = conf['redis server']

def get_redis_connection(address):
    if address == 'fake':
        # use fakeredis for testing only
        import fakeredis
        return fakeredis.FakeRedis()
    else:
        return Redis(address)

class RedisHelper(object):
    def __init__(self, redis_address):
        self.db = get_redis_connection(redis_address)
        if redis_address == 'fake':
            # fakeredis is not thread safe
            self.defer_func = defer.execute
        else:
            self.defer_func = threads.deferToThread

    def delete(self, key):
        return self.defer_func(self.db.delete, key)

    def hget(self, name, key):
        return self.defer_func(self.db.hget, name, key)

    def hset(self, name, key, value):
        return self.defer_func(self.db.hset, name, key, value)

    def hdel(self, name, *keys):
        return self.defer_func(self.db.hdel, name, *keys)

    def hexists(self, name, key):
        return self.defer_func(self.db.hexists, name, key)

    def sismember(self, name, value):
        return self.defer_func(self.db.sismember, name, value)

    def smembers(self, name):
        return self.defer_func(self.db.smembers, name)

    def sadd(self, name, *values):
        return self.defer_func(self.db.sadd, name, *values)

    def srem(self, name, *values):
        return self.defer_func(self.db.srem, name, *values)

    def sdiff(self, name, *values):
        return self.defer_func(self.db.sdiff, name, *values)

    @defer.inlineCallbacks
    def is_sd_blob(self, blob_hash):
        out = yield self.sismember(SD_BLOB_HASHES, blob_hash)
        defer.returnValue(out)

    @defer.inlineCallbacks
    def add_blob_to_host(self, blob_hash, host):
        yield self.sadd(host, blob_hash)
        yield self.sadd(CLUSTER_BLOBS, blob_hash)
        length, timestamp, prev_host = yield self.get_blob(blob_hash)
        yield self.set_blob(blob_hash, length, timestamp, host)

    @defer.inlineCallbacks
    def add_sd_blob(self, sd_blob_hash, blob_hashes):
        yield self.sadd(sd_blob_hash, *tuple(blob_hashes))
        yield self.sadd(SD_BLOB_HASHES, sd_blob_hash)

    @defer.inlineCallbacks
    def blob_exists(self, blob_hash):
        exists = yield self.hexists(BLOB_HASHES, blob_hash)
        defer.returnValue(exists)

    @defer.inlineCallbacks
    def blob_has_been_forwarded_to_host(self, blob_hash):
        sent_to_host = yield self.sismember(CLUSTER_BLOBS, blob_hash)
        defer.returnValue(sent_to_host)

    @defer.inlineCallbacks
    def get_all_unforwarded_sd_blobs(self):
        # returns a set of sd_blob hashes that have not been sent to a host
        out = yield self.sdiff(SD_BLOB_HASHES, CLUSTER_BLOBS)
        defer.returnValue(out)

    @defer.inlineCallbacks
    def get_blobs_for_stream(self, sd_hash):
        blobs_in_stream = yield self.smembers(sd_hash)
        defer.returnValue(blobs_in_stream)

    @defer.inlineCallbacks
    def set_blob(self, blob_hash, blob_length, timestamp, host=''):
        blob_val = json.dumps([blob_length, timestamp, host])
        was_set = yield self.hset(BLOB_HASHES, blob_hash, blob_val)
        defer.returnValue(was_set)

    @defer.inlineCallbacks
    def get_blob(self, blob_hash):
        blob_val = yield self.hget(BLOB_HASHES, blob_hash)
        if blob_val is None:
            raise Exception("Blob does not exist")
        try:
            [length, timestamp, host] = json.loads(blob_val)
        except TypeError as e:
            # older blob entries just had length as blob_val
            length = int(blob_val)
            timestamp = 0
            host = ''
        defer.returnValue((length, timestamp, host))

    @defer.inlineCallbacks
    def delete_blob(self, blob_hash):
        was_deleted = yield self.hdel(BLOB_HASHES, blob_hash)
        defer.returnValue(was_deleted)

    @defer.inlineCallbacks
    def delete_blob_from_host(self, blob_hash, host):
        # set blob so that its no longer in a host
        yield self.srem(CLUSTER_BLOBS, blob_hash)
        yield self.srem(host, blob_hash)

    @defer.inlineCallbacks
    def delete_sd_blob(self, blob_hash):
        yield self.srem(SD_BLOB_HASHES, blob_hash)
        yield self.delete(blob_hash)

class ClusterStorage(object):
    def __init__(self, path=None, redis_address=None):
        self.db = RedisHelper(redis_address or conf['redis server'])
        self.db_dir = path or os.path.expandvars(conf['blob directory'])
        if not os.path.isdir(self.db_dir):
            raise OSError("blob storage directory \"%s\" does not exist" % self.db_dir)

    @defer.inlineCallbacks
    def blob_exists(self, blob_hash):
        """True if blob file exists in the cluster"""
        if not is_valid_blobhash(blob_hash):
            raise InvalidBlobHashError()
        exists = yield self.db.blob_exists(blob_hash)
        defer.returnValue(exists)

    @defer.inlineCallbacks
    def blob_has_been_forwarded_to_host(self, blob_hash):
        """True if the blob has been sent to a host"""
        if not is_valid_blobhash(blob_hash):
            raise InvalidBlobHashError(blob_hash)
        sent_to_host = yield self.db.blob_has_been_forwarded_to_host(blob_hash)
        defer.returnValue(sent_to_host)

    @defer.inlineCallbacks
    def add_blob_to_host(self, blob_hash, host):
        yield self.db.add_blob_to_host(blob_hash, host)

    @defer.inlineCallbacks
    def get_blobs_for_stream(self, sd_hash):
        """
        Return a list of blobs that belong to stream with sd_hash
        raise Exception if the sd_hash is unknown
        """
        blobs_in_stream = yield self.db.get_blobs_for_stream(sd_hash)
        if blobs_in_stream is None:
            raise Exception('unknown sd_hash:%s',sd_hash)
        out = []
        for b in blobs_in_stream:
            blob = yield self.get_blob(b)
            out.append(blob)
        defer.returnValue(out)

    @defer.inlineCallbacks
    def get_needed_blobs_for_stream(self, sd_hash):
        """
        Return a list of blob hashes in a stream
        that we do not have
        """

        blobs_in_stream = yield self.db.get_blobs_for_stream(sd_hash)
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
                if sd_blob.verified:
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
                    if not blob.verified:
                        needed.append(blob_hash)
        defer.returnValue(needed)

    @defer.inlineCallbacks
    def load_sd_blob(self, sd_blob):
        sd_file = sd_blob.open_for_reading()
        sd_blob_data = sd_file.read()
        sd_file.close()
        decoded_sd_blob = json.loads(sd_blob_data)
        blob_hashes = []
        for blob in decoded_sd_blob['blobs']:
            if 'blob_hash' in blob and 'length' in blob:
                blob_hashes.append(blob['blob_hash'])
        if blob_hashes:
            yield self.db.add_sd_blob(sd_blob.blob_hash, blob_hashes)
        defer.returnValue(decoded_sd_blob)

    @defer.inlineCallbacks
    def get_all_unforwarded_sd_blobs(self):
        # returns a set of sd_blob hashes that have not been sent to a host
        out = yield self.db.get_all_unforwarded_sd_blobs()
        defer.returnValue(out)

    @defer.inlineCallbacks
    def get_blob(self, blob_hash, length=None):
        if length is None:
            blob_exists = yield self.db.blob_exists(blob_hash)
            if blob_exists:
                length, timestamp, host = yield self.db.get_blob(blob_hash)
        blob = BlobFile(self.db_dir, blob_hash, length)
        defer.returnValue(blob)

    @defer.inlineCallbacks
    def get_blob_host(self, blob_hash):
        # get current host of blob, will be empty string if its not on any
        # host
        length, timestamp, host = yield self.db.get_blob(blob_hash)
        defer.returnValue(host)

    @defer.inlineCallbacks
    def get_blob_timestamp(self, blob_hash):
        # get timestamp of when blob was sent to the cluster
        length, timestamp, host = yield self.db.get_blob(blob_hash)
        defer.returnValue(timestamp)

    @defer.inlineCallbacks
    def is_sd_blob(self, blob_hash):
        # True if blob_hash refers to an sd_blob, False otherwise
        out = yield self.db.is_sd_blob(blob_hash)
        defer.returnValue(out)

    @defer.inlineCallbacks
    def delete(self, blob_hash):
        log.info("Delete %s", blob_hash)
        exists = yield self.blob_exists(blob_hash)
        if exists:
            blob_length, timestamp, host = yield self.db.get_blob(blob_hash)
            if len(host) > 0: # blob is on a host
                raise Exception("Cannot delete blob on a host, use delete_from_host")
            blob = BlobFile(self.db_dir, blob_hash, blob_length)
            yield blob.delete()
            was_deleted = yield self.db.delete_blob(blob_hash)
            is_sd_blob = yield self.is_sd_blob(blob_hash)
            if is_sd_blob:
                yield self.db.delete_sd_blob(blob_hash)

            defer.returnValue(was_deleted)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def delete_blob_from_host(self, blob_hash):
        exists = yield self.blob_exists(blob_hash)
        if not exists:
            raise Exception('blob not found')

        blob_length, timestamp, host = yield self.db.get_blob(blob_hash)
        if len(host) == 0:# blob is not on a host
            raise Exception('blob must be on a host for delete_blob_from_host')
        # this will set host to empty
        yield self.db.set_blob(blob_hash, blob_length, timestamp)
        yield self.db.delete_blob_from_host(blob_hash, host)


    @defer.inlineCallbacks
    def completed(self, blob_hash, blob_length):
        if not is_valid_blobhash(blob_hash):
            raise InvalidBlobHashError()
        timestamp = time.time()
        was_set = yield self.db.set_blob(blob_hash, blob_length, timestamp)
        defer.returnValue(was_set)

    @defer.inlineCallbacks
    def verify_stream_ready_to_forward(self, sd_hash):
        blob_exists = yield self.blob_exists(sd_hash)
        if not blob_exists:
            defer.returnValue(False)

        blob_forwarded = yield self.blob_has_been_forwarded_to_host(sd_hash)
        if blob_forwarded:
            defer.returnValue(False)
        sd_blob = yield self.get_blob(sd_hash)
        if not sd_blob.verified:
            defer.returnValue(False)
        blobs = yield self.get_blobs_for_stream(sd_hash)
        for b in blobs:
            if not b.verified:
                defer.returnValue(False)
        defer.returnValue(True)
