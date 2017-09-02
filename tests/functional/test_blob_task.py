from prism.protocol.task import  process_blob, get_blob_path
from prism.protocol.factory import build_prism_blob_client_factory
from prism.storage.storage import ClusterStorage
from prism.protocol.blob import BlobFile

from twisted.internet import defer, task

import unittest
import shutil
import tempfile
import fakeredis
import os
import time
import multiprocessing
import threading
import Queue

# this turns on logging
from twisted.python import log
import sys
log.startLogging(sys.stdout)

sys.path.insert(0, os.path.dirname(__file__))
from test_utils import setup_server, SD_BLOB_HASH, SD_BLOB_CONTENT, BLOB_HASH, BLOB_CONTENT

class TestTask(unittest.TestCase):

    def setUp(self):
        self._setup_server([BLOB_HASH])
        self._setup_client()

    def _setup_server(self, blob_hashes_to_expect):
        # this is where server will receive from client
        self.server_queue = multiprocessing.Queue()
        # this is where client will receive from server
        self.client_queue = multiprocessing.Queue()
        self.server_process = multiprocessing.Process(target=setup_server,
            args=(self.server_queue, self.client_queue, blob_hashes_to_expect))
        self.server_process.start()

    def _setup_client(self):
        # setup client storage
        self.client_db_dir = tempfile.mkdtemp()
        self.client_storage = ClusterStorage(self.client_db_dir, 'fake')

    @defer.inlineCallbacks
    def _setup_client_blobs(self):
        # create blob
        blob_file_name = os.path.join(self.client_db_dir, BLOB_HASH)
        with open(blob_file_name, 'w') as blob_file:
            blob_file.write(BLOB_CONTENT)
        blob_file = BlobFile(self.client_db_dir, BLOB_HASH, len(BLOB_CONTENT))
        out = yield self.client_storage.completed(BLOB_HASH, len(BLOB_CONTENT))

    def tearDown(self):
        shutil.rmtree(self.client_db_dir)
        self.server_process.join()

    @defer.inlineCallbacks
    def _on_finish_blob(self):
        # check client storage state after protocol is finished
        self.blob_exists = yield self.client_storage.blob_exists(BLOB_HASH)
        self.blob_has_been_forwarded = yield self.client_storage.blob_has_been_forwarded_to_host(BLOB_HASH)

    def test_process_blob(self):
        client_factory_class = build_prism_blob_client_factory

        # start client
        from twisted.internet import reactor
        reactor.addSystemEventTrigger('before','shutdown', self._on_finish_blob)
        try:
            process_blob(BLOB_HASH, self.client_db_dir, client_factory_class, 'fake', host_infos=('localhost',5566,0),
                        setup_d = self._setup_client_blobs)
        except SystemExit:
            pass

        # tell server process to stop
        self.server_queue.put('stop')

        # check client variables
        self.assertEqual(1, self.blob_exists)
        self.assertEqual(1, self.blob_has_been_forwarded)
        # file should be removed from client, because it was sent to server
        self.assertFalse(os.path.isfile(get_blob_path(BLOB_HASH, self.client_storage)))

        # check expected variables we should received from server
        server_results = self.client_queue.get()
        self.assertEqual(BLOB_CONTENT, server_results[0]['blob_content'])
        self.assertEqual(1, server_results[0]['blob_exists'])

