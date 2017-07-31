from twisted.internet import reactor

from prism.protocol import PrismClientFactory
from prism.storage import ClusterStorage


def forward_blobs(blob_dir, host, port, *blobs):
    blobs = list(tuple(blobs))
    cluster_storage = ClusterStorage(blob_dir)
    reactor.connectTCP(host, port, PrismClientFactory(cluster_storage, blobs))
    reactor.run()
