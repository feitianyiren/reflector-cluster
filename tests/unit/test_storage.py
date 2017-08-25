import shutil
import tempfile
from os import path

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
    def test_basic(self):
        blob_hash='6ac46ae5445eb2d26ff41739440ac92d240fdade9a34d38f87f5b47154f6edc95f637a1a2cdb3ae60aa2c2ef91533d38'
        out = yield self.cs.blob_exists(blob_hash)
        self.assertEqual(False, out)

        out = yield self.cs.completed(blob_hash,10)
        self.assertEqual(1, out)
        out = yield self.cs.blob_exists(blob_hash)
        self.assertEqual(True, out)
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
