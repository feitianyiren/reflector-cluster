import logging
from twisted.internet import defer, reactor

from protocol.factory import ReflectorClusterFactory
from storage.storage import ClusterStorage

log = logging.getLogger(__name__)


class ReflectorClusterServer(object):
    def __init__(self, data_directory=None):
        self.data_directory = data_directory
        self.server_port = None
        self.cluster_storage = ClusterStorage(data_directory)
        self.factory = ReflectorClusterFactory(self.cluster_storage)

    @defer.inlineCallbacks
    def start(self):
        if not self.server_port:
            reactor.addSystemEventTrigger("before", "shutdown", self.stop)
            self.server_port = reactor.listenTCP(5566, self.factory, 50, 'localhost')
            log.info("Listening on %s", self.server_port)
            yield self.cluster_storage.start()
            log.info("Connected to redis server")

    @defer.inlineCallbacks
    def stop(self):
        if self.server_port is not None:
            log.info("Stop listening on %s", self.server_port)
            yield self.server_port.stopListening()
