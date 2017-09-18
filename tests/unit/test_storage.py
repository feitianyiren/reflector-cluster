import shutil
import tempfile
from os import path
import time

from twisted.trial import unittest
from twisted.internet import defer

from prism.storage.storage import ClusterStorage
from prism.protocol.blob import BlobFile

class TestClusterStorage(unittest.TestCase):
    def setUp(self):
        self.db_dir = tempfile.mkdtemp()
        self.cs = ClusterStorage(self.db_dir, 'fake')

    def tearDown(self):
        shutil.rmtree(self.db_dir)

    @defer.inlineCallbacks
    def test_redis_helper(self):
        blob_hash = '6ac46ae5445eb2d26ff41739440ac92d240fdade9a34d38f87f5b47154f6edc95f637a1a2cdb3ae60aa2c2ef91533d38'
        blob_length = 10
        timestamp = time.time()
        host ='somehost'
        yield self.cs.db.set_blob(blob_hash, blob_length, timestamp, host)

        out = yield self.cs.db.get_blob(blob_hash)
        self.assertEqual(blob_length, out[0])
        self.assertEqual(timestamp, out[1])
        self.assertEqual(host, out[2])

        sd_blob_hash = '1ac46ae5445eb2d26ff41739440ac92d240fdade9a34d38f87f5b47154f6edc95f637a1a2cdb3ae60aa2c2ef91533d11'
        out = yield self.cs.db.add_sd_blob(sd_blob_hash, [blob_hash])

        out = yield self.cs.db.get_blobs_for_stream(sd_blob_hash)
        self.assertEqual(set([blob_hash]), out)

    @defer.inlineCallbacks
    def test_cluster_storage(self):
        blob_hash='6ac46ae5445eb2d26ff41739440ac92d240fdade9a34d38f87f5b47154f6edc95f637a1a2cdb3ae60aa2c2ef91533d38'
        out = yield self.cs.blob_exists(blob_hash)
        self.assertEqual(False, out)

        out = yield self.cs.completed(blob_hash,10)
        self.assertEqual(1, out)
        out = yield self.cs.blob_exists(blob_hash)
        self.assertEqual(True, out)
        out = yield self.cs.get_blob_host(blob_hash)
        self.assertEqual(0, len(out))
        out = yield self.cs.is_sd_blob(blob_hash)
        self.assertFalse(out)

        out = yield self.cs.delete(blob_hash)
        out = yield self.cs.blob_exists(blob_hash)
        self.assertEqual(False, out)

        # some non existing blob
        blob_hash = '1ac46ae5445eb2d26ff41739440ac92d240fdade9a34d38f87f5b47154f6edc95f637a1a2cdb3ae60aa2c2ef91533d11'
        out = yield self.cs.get_blob(blob_hash)
        self.assertEqual(None, out.length)
        self.assertFalse(out._verified)

if __name__=='__main__':
    unittest.main()
