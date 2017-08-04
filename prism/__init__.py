import logging
from twisted.internet import selectreactor
selectreactor.install()

log = logging.getLogger()
h = logging.StreamHandler()
log.setLevel(logging.INFO)
log.addHandler(h)
