import os
import json
import hashlib
import datetime
from redis import Redis
from prism.config import get_settings


class PrismInspector(object):
    def __init__(self):
        conf = get_settings()
        self.db = Redis(conf['redis server'])
        self.blob_dir = conf['blob directory']

    def get_blob_hashes(self):
        return self.db.smembers("blob_hashes")

    def get_stream_blobs(self, sd_hash):
        return self.db.smembers(sd_hash)

    @property
    def sd_hashes(self):
        return self.db.smembers("sd_blob_hashes")

    def blob_is_pending(self, blob_hash):
        return os.path.isfile(os.path.join(self.blob_dir, blob_hash))

    def pending_blob_is_complete(self, blob_hash, length):
        with open(os.path.join(self.blob_dir, blob_hash)) as blob:
            data = blob.read()
        if length > len(data):
            return False
        elif length < len(data):
            raise Exception("blob is too long: %i vs %i" % (len(data), length))
        digest = hashlib.sha384()
        digest.update(data)
        if blob_hash != digest.hexdigest():
            raise Exception("hash mismatch")
        return True

    def get_blob_summary(self, blob_hash):
        length = ts = host = pending = pending_blob_is_complete = pending_blob_error = None
        info = self.db.hget("blob_hashes", blob_hash)
        if info:
            try:
                length, ts, host = json.loads(info)
                if not host:
                    host = None
                if ts:
                    ts = datetime.datetime.fromtimestamp(ts).isoformat(' ')
                pending = self.blob_is_pending(blob_hash)
                if pending:
                    try:
                        pending_blob_is_complete = self.pending_blob_is_complete(blob_hash, length)
                    except Exception as err:
                        pending_blob_error = err.message
            except:
                pass
        return {
            "pending": pending,
            "pending_blob_is_complete": pending_blob_is_complete,
            "pending_blob_error": pending_blob_error,
            "host": host,
            "blob_hash": blob_hash,
            "length": length,
            "timestamp": ts
        }

    def get_stream_summary(self, sd_hash, full_summary=True):
        sd_summary = self.get_blob_summary(sd_hash)
        blob_summaries = [self.get_blob_summary(blob_hash) for blob_hash in self.get_stream_blobs(sd_hash)]
        result = {
            "some_data_is_hosted": any([s['host'] is not None for s in blob_summaries]),
            "all_data_is_hosted": all([s['host'] is not None for s in blob_summaries]),
            "some_data_is_pending": any([s['pending'] for s in blob_summaries]),
            "all_data_is_pending": all([s['pending'] for s in blob_summaries]),
            "sd_is_hosted": sd_summary['host'] is not None,
            "sd_is_pending": sd_summary['pending'],
            "single_host": sd_summary['host'] is not None and
                           all([s['host'] == sd_summary['host'] for s in blob_summaries]),
        }
        if full_summary:
            result['descriptor'] = sd_summary
            result['data'] = blob_summaries
        return result
