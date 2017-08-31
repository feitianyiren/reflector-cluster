import logging
import os
from twisted.internet.protocol import ServerFactory, ClientFactory

from prism.protocol.server import ReflectorServerProtocol, enqueue_blob
from prism.protocol.client import BlobReflectorClient
from prism.config import get_settings

log = logging.getLogger(__name__)
settings = get_settings()

class PrismServerFactory(ServerFactory):
    """
    A factory for a entry-point reflector server that pushes blobs to a cluster of
    reflector hosts. A prism server listens using the reflector protocol v2 (blobs and sd blobs),
    and pushes to hosts in the cluster via reflector v1 (blob only). The distribution of blobs
    if tracked using a redis database
    """

    protocol = ReflectorServerProtocol

    def __init__(self, storage):
        self.storage = storage
        self.protocol_version = 1
        self.client_factory = PrismClientFactory

    def buildProtocol(self, addr):
        p = self.protocol()
        p.factory = self
        p.addr = addr
        self.p = p
        return p

    def startFactory(self):
        blobs = os.listdir(self.storage.db_dir)
        if blobs:
            log.warning("%i blobs need to be sent from previous run", len(blobs))
            log.warning("queueing %i of them to be sent", len(blobs))
            for i, blob_hash in enumerate(blobs):
                enqueue_blob(blob_hash, self.storage, self.client_factory)
            log.info("queued blobs, starting server")


class PrismClientFactory(ClientFactory):
    protocol = BlobReflectorClient

    def __init__(self, storage, blobs):
        self.storage = storage
        self.blobs = blobs
        self.protocol_version = 1
        self.sent_blobs = False
        self.p = None

    def buildProtocol(self, addr):
        p = self.protocol()
        p.factory = self
        p.addr = addr
        p.blob_hashes_sent = []
        p.blob_storage = self.storage
        p.protocol_version = self.protocol_version
        p.blob_hashes_to_send = self.blobs
        self.p = p
        return p
