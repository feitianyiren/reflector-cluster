from prism.protocol.task import  process_blob, get_blob_path
from prism.protocol.factory import PrismClientFactory, PrismServerFactory
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

BLOB_HASH= '0aceb607d62e5c75468ded32343a2812d69e0f4545c9fd471f2e1f96f0b6769fda58584a88e9c96778372916b9062b0f'
BLOB_CONTENT = "#z{5\xc1\x11U\xb8\xeb'%>\x9b\xa9@\x02\xf4\x8c\xba\x01\xc0\xce\x11\xc2\xb4\xd8\xb5MOo\xcfE"

def get_redis():
    import fakeredis
    return fakeredis.FakeRedis()

def _setup_server(server_queue, client_queue):
    """
    We setup server for receiving blobs here on a process,
    so we can utilize a seperate reactor, and a
    seperate fakeredis instances
    """
    from twisted.internet import reactor
    def _listen_queue():
        # listen for the client to give a stop signal
        # (so that we know we are done)
        try:
            obj = server_queue.get(block=False,timeout=0)
        except Queue.Empty as e:
            pass
        else:
            if obj == 'stop':
                reactor.stop()

    @defer.inlineCallbacks
    def on_finish():
        # put results of things we want to test on the client queue
        # so that it can test using self.assert....
        blob_exists =  yield server_storage.blob_exists(BLOB_HASH)
        blob_content = None
        with open(os.path.join(server_db_dir, BLOB_HASH),'r') as blob_file:
            blob_content = blob_file.read()
        client_queue.put({'blob_content':blob_content,'blob_exists':blob_exists})

    server_db_dir = tempfile.mkdtemp()
    server_storage = ClusterStorage(server_db_dir)
    server_storage.db.db = get_redis()
    server_factory = PrismServerFactory(server_storage)
    port = reactor.listenTCP(5566, server_factory)
    loop = task.LoopingCall(_listen_queue)
    loop.start(0.01)

    reactor.addSystemEventTrigger('before','shutdown', on_finish)
    reactor.run()

    shutil.rmtree(server_db_dir)


class TestTask(unittest.TestCase):

    def _setup_server(self):
        # this is where server will receive from client
        self.server_queue = multiprocessing.Queue()
        # this is where client will receive from server
        self.client_queue = multiprocessing.Queue()
        self.server_process = multiprocessing.Process(target=_setup_server,
            args=(self.server_queue, self.client_queue))
        self.server_process.start()

    def tearDown(self):
        shutil.rmtree(self.client_db_dir)

    @defer.inlineCallbacks
    def _setup_client(self):
        # setup client storage
        self.client_db_dir = tempfile.mkdtemp()
        self.client_storage = ClusterStorage(self.client_db_dir)
        self.client_storage.db.db = get_redis()

         # create blob to send on client
        blob_file = BlobFile(self.client_db_dir, BLOB_HASH, len(BLOB_CONTENT))
        finished_d, write, _ = blob_file.open_for_writing(None)
        write(BLOB_CONTENT)
        yield finished_d
        yield self.client_storage.completed(BLOB_HASH, len(BLOB_CONTENT))

    @defer.inlineCallbacks
    def _on_finish(self):
        # check client storage state after protocol is finished
        # sometimes this hangs and takes a longer time than expected (about 10 to 20 seconds..)
        # not sure why...
        self.blob_exists = yield self.client_storage.blob_exists(BLOB_HASH)
        self.blob_has_been_forwarded = yield self.client_storage.blob_has_been_forwarded_to_host(BLOB_HASH)

    def test_process_blob(self):
        self._setup_server()
        self._setup_client()

        client_factory_class = PrismClientFactory
        expected_file_path = get_blob_path(BLOB_HASH, self.client_storage)
        self.assertTrue(os.path.isfile(expected_file_path))

        # start client
        from twisted.internet import reactor
        reactor.addSystemEventTrigger('before','shutdown', self._on_finish)
        try:
            process_blob(BLOB_HASH, self.client_db_dir, client_factory_class, 'fake', host_infos=('localhost',5566,0))
        except SystemExit:
            pass

        # tell server process to stop
        self.server_queue.put('stop')

        # check client variables
        self.assertEqual(1, self.blob_exists)
        self.assertEqual(1, self.blob_has_been_forwarded)
        # file should be removed from client, because it was sent to server
        self.assertFalse(os.path.isfile(expected_file_path))

        # check expected variables we should received from server
        server_results = self.client_queue.get()
        self.assertEqual(BLOB_CONTENT, server_results['blob_content'])
        self.assertEqual(1, server_results['blob_exists'])


