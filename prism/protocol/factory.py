import logging
import os
from twisted.internet.protocol import ServerFactory, ClientFactory
from twisted.internet import defer

from prism.protocol.server import ReflectorServerProtocol
from prism.protocol.client import BlobReflectorClient
from prism.protocol.stream_client import StreamReflectorClient
from prism.protocol.task import enqueue_blob, enqueue_stream
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

    def buildProtocol(self, addr):
        p = self.protocol(self.storage, build_prism_stream_client_factory)
        p.factory = self
        p.addr = addr
        self.p = p
        return p

    def startFactory(self):
        pass


class PrismClientFactory(ClientFactory):
    protocol = BlobReflectorClient

    def __init__(self, storage, blobs):
        self.storage = storage
        self.blobs = blobs
        self.protocol_version = 1
        self.sent_blobs = False
        self.p = None
        #this deferred is fired when this protocol is disconnected
        #(when connectionLost() is called)
        self.on_connection_lost_d = defer.Deferred()
        self.on_connection_fail_d = defer.Deferred()

    def clientConnectionFailed(self, connector, reason):
        log.error('Connection failed: %s', reason)
        self.on_connection_fail_d.callback(None)

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

class PrismStreamClientFactory(ClientFactory):
    protocol = StreamReflectorClient

    def __init__(self, storage, sd_blob, blobs):
        self.storage = storage
        self.sd_blob = sd_blob
        self.blobs = blobs
        self.protocol_version = 1
        self.p = None
        #this deferred is fired when this protocol is disconnected
        #(when connectionLost() is called)
        self.on_connection_lost_d = defer.Deferred()
        self.on_connection_fail_d = defer.Deferred()

    def clientConnectionFailed(self, connector, reason):
        log.error('Connection failed: %s', reason)
        self.on_connection_fail_d.callback(None)

    def buildProtocol(self, addr):
        p = self.protocol(self.sd_blob, self.blobs)
        p.factory = self
        p.addr = addr
        p.protocol_version = self.protocol_version
        self.p = p
        return p


def build_prism_stream_server_factory(blob_storage):
    return PrismServerFactory(blob_storage)

@defer.inlineCallbacks
def build_prism_stream_client_factory(sd_hash, blob_storage, host_to_send):
    """
    Build a prism stream client factory

    sd_hash - sd_hash of stream to send
    blob_storage - blob storage class
    host_to_send - host to send to, None if not known yet
    """
    blob_exists = yield blob_storage.blob_exists(sd_hash)
    if not blob_exists:
        raise Exception("blob does not exist in cluster")

    blob_forwarded = yield blob_storage.blob_has_been_forwarded_to_host(sd_hash)
    sd_blob = yield blob_storage.get_blob(sd_hash)
    if blob_forwarded:
        # if sd blob has already been forwarded to some other host,
        # raise exception 
        sd_hash_host = yield blob_storage.get_blob_host(sd_hash)
        if host_to_send !=  sd_hash_host:
            raise Exception("sd blob has been forwarded to some other host:%s", sd_hash_host)     
    else:
        # if sd blob hasn't been forwarded make sure we have it
        if not sd_blob.verified:
            raise Exception("cannot send unverified sd blob")

    blobs = yield blob_storage.get_blobs_for_stream(sd_hash)
    for b in blobs:
        blob_forwarded = yield blob_storage.blob_has_been_forwarded_to_host(b.blob_hash)
        if blob_forwarded: 
            # if blob has been forwarded, make sure its not on some other host
            blob_host = yield blob_storage.get_blob_host(b.blob_hash)
            if host_to_send != blob_host:
                raise Exception("blob has been forwarded to some other host:%s", blob_host)
        else:
            # if blob is not forwarded, make sure we have it
            if not b.verified:
                raise Exception("blob %s is not verified", b.blob_hash)
    defer.returnValue(PrismStreamClientFactory(blob_storage, sd_blob, blobs))

@defer.inlineCallbacks
def build_prism_blob_client_factory(blob_hash, blob_storage):
    """
    Build a prism blob client factory

    blob_hash - blob_hash of stream to send
    blob_storage - blob storage class
    """
    blob_exists = yield blob_storage.blob_exists(blob_hash)
    if not blob_exists:
        raise Exception("blob %s does not exist in cluster"%blob_hash)

    blob_forwarded = yield blob_storage.blob_has_been_forwarded_to_host(blob_hash)
    if blob_forwarded:
        raise Exception("blob has been forwarded")

    blob = yield blob_storage.get_blob(blob_hash)
    if not blob.verified:
        raise Exception("cannot send unverified sd blob")

    defer.returnValue(PrismClientFactory(blob_storage, [blob_hash]))


