import logging
import os
from logging.handlers import RotatingFileHandler
from twisted.internet import selectreactor
selectreactor.install()

log = logging.getLogger()
h = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s:%(lineno)d: %(message)s")
h.setFormatter(formatter)
log.addHandler(h)
file_h = RotatingFileHandler(os.path.expanduser("~/prism-server.log"))
file_h.setFormatter(formatter)
log.addHandler(file_h)
log.setLevel(logging.INFO)

__version__ = "0.0.1"
