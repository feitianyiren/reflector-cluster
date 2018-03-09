import os
import yaml
import logging
from logging.handlers import RotatingFileHandler


def init_log(verbose=True):
    log = logging.getLogger()
    if not len(log.handlers):
        h = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s:%(lineno)d: %(message)s")
        h.setFormatter(formatter)
        log.addHandler(h)
        file_h = RotatingFileHandler(os.path.expanduser("~/prism-server.log"))
        file_h.setFormatter(formatter)
        log.addHandler(file_h)
        if verbose:
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO
        log.setLevel(log_level)


def get_settings():
    # initialize settings from .prism.yml
    # and create root logger

    conf_path = os.path.expanduser("~/.prism.yml")

    if os.path.isfile(conf_path):
        with open(conf_path, "r") as conf_file:
            conf_file_data = yaml.safe_load(conf_file.read())
    else:
        conf_file_data = {}

    HOSTS = "hosts"
    MAX_BLOBS_PER_HOST = "max blobs"
    BLOB_DIR = "blob directory"
    LISTEN_ON = "listen"
    WORKERS = "workers"
    REDIS_SERVER = "redis server"
    ENQUEUE_ON_STARTUP = "enqueue on startup"
    VERBOSE = "verbose"

    settings_types = {
        LISTEN_ON: str,
        HOSTS: list,
        MAX_BLOBS_PER_HOST: int,
        BLOB_DIR: str,
        WORKERS: int,
        REDIS_SERVER: str,
        ENQUEUE_ON_STARTUP: bool,
        VERBOSE: bool,
    }

    default_conf = {
        LISTEN_ON: "localhost",
        HOSTS: [
            "jack.lbry.tech",
        ],
        MAX_BLOBS_PER_HOST: 480000, # assuming 1 terabyte disk / 2 mb blobs
        BLOB_DIR: os.path.expanduser("~/.prism"),
        REDIS_SERVER: "localhost",
        ENQUEUE_ON_STARTUP: True,
        VERBOSE: False,
    }

    settings = {}

    for k in default_conf:
        if k in conf_file_data:
            settings[k] = conf_file_data[k]
        else:
            settings[k] = default_conf[k]

    init_log(settings['verbose'])

    return settings



