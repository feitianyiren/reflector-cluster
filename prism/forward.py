from twisted.internet import reactor

from prism.protocol import PrismClientFactory
from prism.storage import ClusterStorage


def forward_blobs(blob_dir, host, port, *blobs):
    blobs = list(tuple(blobs))
    cluster_storage = ClusterStorage(blob_dir)
    client_factory = PrismClientFactory(cluster_storage, blobs)
    reactor.connectTCP(host, port, client_factory)
    reactor.run()
    return client_factory.p.blob_hashes_sent
