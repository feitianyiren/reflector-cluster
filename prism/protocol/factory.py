import logging
import os
import subprocess
from twisted.internet import defer, threads
from twisted.internet.protocol import ServerFactory, ClientFactory

from prism.protocol.server import ReflectorServerProtocol, enqueue_blob
from prism.protocol.client import BlobReflectorClient
from prism.config import get_settings

log = logging.getLogger(__name__)
settings = get_settings()
BLOB_DIR = os.path.expandvars(settings['blob directory'])


class worker_iterator_cls(object):
    def __init__(self, max=settings['workers']):
        self._count = 0
        self._max = max
        self._worker_processes = {}

    def __iter__(self):
        if self._count + 1 <= self._max:
            self._count += 1
        else:
            self._count = 1
        yield "worker-%i" % self._count


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
        self.worker_iterator = worker_iterator_cls()

    def buildProtocol(self, addr):
        p = self.protocol()
        p.factory = self
        p.addr = addr
        p.worker_iterator = self.worker_iterator
        self.p = p
        return p

    @defer.inlineCallbacks
    def startFactory(self):
        yield self.storage.start()
        blobs = os.listdir(BLOB_DIR)
        if blobs:
            log.warning("%i blobs need to be sent from previous run", len(blobs))
            blobs = blobs[:10000]
            log.warning("queueing %i of them to be sent", len(blobs))
            for blob_hash in blobs:
                threads.deferToThread(enqueue_blob, blob_hash, self.client_factory,
                                      self.worker_iterator)
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

    def startFactory(self):
        return self.storage.start()
