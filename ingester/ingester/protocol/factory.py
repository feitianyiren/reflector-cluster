from twisted.internet.protocol import ServerFactory

from ingester.protocol.protocol import ReflectorServerProtocol


class ReflectorClusterFactory(ServerFactory):
    protocol = ReflectorServerProtocol

    def __init__(self, storage):
        self.storage = storage
        self.protocol_version = 1

    def buildProtocol(self, addr):
        return ServerFactory.buildProtocol(self, addr)
