import os
import logging
from twisted.internet import reactor
from twisted.application import service

from prism.protocol import PrismServerFactory
from prism.storage import ClusterStorage
from prism.config import get_settings

log = logging.getLogger(__name__)

settings = get_settings()

LISTEN_ON = settings['listen']


class PrismServer(service.Service):
    def __init__(self, port_num=5566):
        self.port_num = port_num
        self.cluster_storage = ClusterStorage()
        self._port = None

    def startService(self):
        log.info("Starting prism server (pid %i), listening on %s (reactor: %s)", os.getpid(),
                 LISTEN_ON, reactor)
        self._port = reactor.listenTCP(self.port_num, PrismServerFactory(self.cluster_storage),
                                       50, LISTEN_ON)

    def stopService(self):
        return self._port.stopListening()


def main():
    prism_server = PrismServer()
    reactor.addSystemEventTrigger("before", "startup", prism_server.startService)
    reactor.addSystemEventTrigger("before", "shutdown", prism_server.stopService)
    reactor.run()
