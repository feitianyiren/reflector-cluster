import os
import logging
from twisted.internet import reactor
from twisted.application import service

from prism.protocol import PrismServerFactory
from prism.storage import ClusterStorage

log = logging.getLogger(__name__)


class PrismServer(service.Service):
    def __init__(self, port_num=5566):
        self.port_num = port_num
        self.cluster_storage = None
        self._port = None

    def startService(self):
        log.info("Starting prism server (pid %i)", os.getpid())
        self.cluster_storage = ClusterStorage()
        self._port = reactor.listenTCP(self.port_num, PrismServerFactory(self.cluster_storage),
                                       50, 'localhost')

    def stopService(self):
        return self._port.stopListening()


def main():
    prism_server = PrismServer()
    reactor.addSystemEventTrigger("before", "startup", prism_server.startService)
    reactor.addSystemEventTrigger("before", "shutdown", prism_server.stopService)
    reactor.run()
