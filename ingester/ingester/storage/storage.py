import logging
import os

from ingester.blob import BlobFile
from ingester.error import AlreadyStarted, InvalidBlobHashError
from ingester.storage import txredisapi as redis
from twisted.internet import defer, reactor

from ingester.constants import BLOB_HASH_LENGTH

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)


BLOB_HASHES = "blob_hashes"


class ClusterStorage(object):
    def __init__(self, db_dir=None):
        self.db = None
        self.db_dir = db_dir or os.path.expanduser("~/reflector_cluster")
        self._blob_cache = {}

    @defer.inlineCallbacks
    def start(self):
        if self.db is None:
            self.db = yield redis.Connection()
            reactor.addSystemEventTrigger("before", "shutdown", self.stop)
        else:
            raise AlreadyStarted()
        defer.returnValue(None)

    @defer.inlineCallbacks
    def stop(self):
        if self.db:
            log.info(self.db)
            yield self.db.disconnect()
        defer.returnValue(None)

    @defer.inlineCallbacks
    def hexists(self, blob_hash):
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        exists = yield self.db.hexists(BLOB_HASHES, blob_hash)
        defer.returnValue(exists)

    @defer.inlineCallbacks
    def hdel(self, blob_hash):
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        was_deleted = yield self.db.hdel(BLOB_HASHES, blob_hash)
        defer.returnValue(was_deleted)

    @defer.inlineCallbacks
    def hset(self, blob_hash, blob_length):
        if len(blob_hash) != BLOB_HASH_LENGTH:
            raise InvalidBlobHashError()
        was_set = yield self.db.hset(BLOB_HASHES, blob_hash, blob_length)
        defer.returnValue(was_set)

    def get_blob(self, blob_hash, length):
        if blob_hash not in self._blob_cache:
            self._blob_cache[blob_hash] = BlobFile(self.db_dir, blob_hash, length)
        return defer.succeed(self._blob_cache[blob_hash])

    @defer.inlineCallbacks
    def delete(self, blob_hash):
        exists = yield self.hexists(blob_hash)
        if exists:
            if blob_hash in self._blob_cache:
                blob = self._blob_cache[blob_hash]
            else:
                blob_length = yield self.db.hget(BLOB_HASHES, blob_hash)
                blob = BlobFile(self.db_dir, blob_hash, blob_length)
            yield blob.delete()
            del blob
            if blob_hash in self._blob_cache:
                del self._blob_cache[blob_hash]
            deleted = yield self.hdel(blob_hash)
            defer.returnValue(deleted)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def completed(self, blob_hash, blob_length):
        exists = yield self.hexists(blob_hash)
        if not exists:
            yield self.hset(blob_hash, blob_length)
        defer.returnValue(True)
