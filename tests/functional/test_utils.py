from prism.protocol.factory import PrismServerFactory
from prism.storage.storage import ClusterStorage

from twisted.internet import defer,task

import Queue
import tempfile
import shutil
import os

SD_BLOB_HASH = 'c81b73e05e9b2e782a3d6b1cd2b6f3ba7b37da9359641e25b5d5a39fec4f6989d25c815e671be0c1deff62b25f50b5f5'
SD_BLOB_CONTENT ='{"stream_name": "746d7066696c652e747874", "blobs": [{"length": 32, "blob_num": 0, "blob_hash": "0aceb607d62e5c75468ded32343a2812d69e0f4545c9fd471f2e1f96f0b6769fda58584a88e9c96778372916b9062b0f", "iv": "ef8c1e1fdae59e87313d3857377b6ac7"}, {"length": 0, "blob_num": 1, "iv": "b3bd10c6d4456bca61d9e2d165e6fb9a"}], "stream_type": "lbryfile", "key": "9b63ffc9553378e7a6c7c7b175312f05", "suggested_file_name": "746d7066696c652e747874", "stream_hash": "f7a15f0c8ea04d323745c3bbc2c76ea5b55882547a63f93aad1314fb3041c88d537ef5187d8776132f242fa0443d6efe"}'
BLOB_HASH= '0aceb607d62e5c75468ded32343a2812d69e0f4545c9fd471f2e1f96f0b6769fda58584a88e9c96778372916b9062b0f'
BLOB_CONTENT = "#z{5\xc1\x11U\xb8\xeb'%>\x9b\xa9@\x02\xf4\x8c\xba\x01\xc0\xce\x11\xc2\xb4\xd8\xb5MOo\xcfE"


def setup_server(server_queue, client_queue, blob_hashes_to_check):
    """
    We setup server for receiving blobs here on a process,
    so we can utilize a seperate reactor, and a
    seperate fakeredis instances

    server_queue - queue to recieve message from client
    client_queue - queue to send messesage to cliennt
    blob_hashes_to_check - check that we recieved these blob hashes
    """
    from twisted.internet import reactor

    def _listen_queue():
        # listen for the client to give a stop signal
        # (so that we know we are done)
        try:
            obj = server_queue.get(block=False, timeout=0)
        except Queue.Empty as e:
            pass
        else:
            if obj == 'stop':
                reactor.stop()

    @defer.inlineCallbacks
    def on_finish():
        # put results of things we want to test on the client queue
        # so that it can test using self.assert....
        to_client_queue=[]
        for blob_hash in blob_hashes_to_check:
            blob_exists = yield server_storage.blob_exists(blob_hash)
            blob_content = None
            with open(os.path.join(server_db_dir, blob_hash), 'r') as blob_file:
                blob_content = blob_file.read()
            to_client_queue.append({'blob_content': blob_content, 'blob_exists': blob_exists})
        client_queue.put(to_client_queue)

        yield port.stopListening()

    server_db_dir = tempfile.mkdtemp()
    server_storage = ClusterStorage(server_db_dir, 'fake')
    server_factory = PrismServerFactory(server_storage)
    port = reactor.listenTCP(5566, server_factory)
    loop = task.LoopingCall(_listen_queue)
    loop.start(0.01)

    reactor.addSystemEventTrigger('before','shutdown', on_finish)
    reactor.run()

    shutil.rmtree(server_db_dir)


