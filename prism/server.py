import os
import logging
from twisted.internet import defer, reactor
from twisted.application import service

from prism.protocol.factory import build_prism_stream_server_factory
from prism.protocol.factory import build_prism_stream_client_factory
from prism.protocol.task import enqueue_stream
from prism.storage.storage import ClusterStorage
from prism.config import get_settings

settings = get_settings()
log = logging.getLogger(__name__)

LISTEN_ON = settings['listen']


class PrismServer(service.Service):
    def __init__(self, port_num=5566):
        self.port_num = port_num
        self.cluster_storage = ClusterStorage()
        self._port = None

    def startService(self):
        log.info("Starting prism server (pid %i), listening on %s (reactor: %s)", os.getpid(),
                 LISTEN_ON, reactor)
        factory = build_prism_stream_server_factory(self.cluster_storage)
        self._port = reactor.listenTCP(self.port_num, factory, 50, LISTEN_ON)

    def stopService(self):
        return self._port.stopListening()

@defer.inlineCallbacks
def enqueue_on_start():
    cluster_storage = ClusterStorage()
    sd_hashes = yield cluster_storage.get_all_unforwarded_sd_blobs()
    for sd_hash in sd_hashes:
        blobs = yield cluster_storage.get_blobs_for_stream(sd_hash)
        enqueue_stream(sd_hash, len(blobs), cluster_storage.db_dir, build_prism_stream_client_factory)
        log.info("enqueued stream {}".format(sd_hash))

def main():
    prism_server = PrismServer()
    reactor.addSystemEventTrigger("before", "startup", prism_server.startService)
    reactor.addSystemEventTrigger("before", "shutdown", prism_server.stopService)
    if settings['enqueue on startup']:
        d = enqueue_on_start()
    reactor.run()
