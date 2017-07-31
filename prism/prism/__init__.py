import logging

log = logging.getLogger()
h = logging.StreamHandler()
h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s:%(lineno)d: %(message)s"))
log.setLevel(logging.INFO)
log.addHandler(h)
