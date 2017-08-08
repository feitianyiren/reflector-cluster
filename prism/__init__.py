import logging
from twisted.internet import selectreactor
selectreactor.install()

log = logging.getLogger()
h = logging.StreamHandler()
log.setLevel(logging.DEBUG)
log.addHandler(h)

log = logging.getLogger("twisted")
h = logging.StreamHandler()
log.setLevel(logging.DEBUG)
log.addHandler(h)
