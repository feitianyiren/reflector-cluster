import logging

log = logging.getLogger()
h = logging.StreamHandler()
log.setLevel(logging.INFO)
log.addHandler(h)
