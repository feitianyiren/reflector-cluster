import logging
from twisted.python import log as tx_log
from twisted.internet import selectreactor
selectreactor.install()

log = logging.getLogger()
h = logging.StreamHandler()
log.setLevel(logging.DEBUG)
log.addHandler(h)
observer = tx_log.PythonLoggingObserver(loggerName=__name__)
observer.start()
