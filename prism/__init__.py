import logging
from twisted.python import log as tx_log
from twisted.internet import selectreactor
selectreactor.install()

log = logging.getLogger()
h = logging.StreamHandler()
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s:%(lineno)d: %(message)s"))
log.addHandler(h)
log.setLevel(logging.DEBUG)
observer = tx_log.PythonLoggingObserver(loggerName=__name__)
observer.start()
