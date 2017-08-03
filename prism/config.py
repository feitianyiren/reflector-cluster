import os
import yaml


def get_settings():
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

    settings_types = {
        LISTEN_ON: str,
        HOSTS: dict,
        MAX_BLOBS_PER_HOST: int,
        BLOB_DIR: str,
    }

    default_conf = {
        LISTEN_ON: "localhost",
        HOSTS: {
            1: "jack.lbry.tech",
        },
        MAX_BLOBS_PER_HOST: 10,
        BLOB_DIR: os.path.expanduser("~/.prism")
    }

    settings = {}

    for k in default_conf:
        if k in conf_file_data:
            if isinstance(settings_types[k], dict) and isinstance(conf_file_data[k], list):
                settings[k] = {i + 1: conf_file_data[k][i] for i in range(len(conf_file_data[k]))}
            settings[k] = settings_types[k](conf_file_data[k])
        else:
            settings[k] = default_conf[k]
    return settings
