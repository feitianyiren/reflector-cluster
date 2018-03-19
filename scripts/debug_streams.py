import json
import progressbar
from prism.debug import PrismInspector


def main():
    inspector = PrismInspector()
    bar = progressbar.ProgressBar()

    stream_summaries = {}
    m = 1000
    for sd_hash in bar(list(inspector.sd_hashes)[:m]):
        stream_summaries[sd_hash] = inspector.get_stream_summary(sd_hash, True)

    print "finished gathering information"

    should_work = set([sd_hash for sd_hash in stream_summaries if stream_summaries[sd_hash]['single_host']])
    to_check = set(stream_summaries.keys()).difference(should_work)
    print len(should_work), len(to_check)
    to_recover = {}
    for sd_hash in to_check:
        to_recover[sd_hash] = {
            'sd_pending': stream_summaries[sd_hash]['descriptor']['pending_blob_is_complete'],
            'sd_hosted': True if stream_summaries[sd_hash]['descriptor']['host'] else False,
            'hosted_not_pending': len([blob_info for blob_info in stream_summaries[sd_hash]['data']
                                       if blob_info['host'] and not blob_info['pending']]),
            'pending_not_hosted': len([blob_info for blob_info in stream_summaries[sd_hash]['data']
                                       if blob_info['pending_blob_is_complete'] and not blob_info['host']]),
            'incomplete_pending': len([blob_info for blob_info in stream_summaries[sd_hash]['data']
                                       if not blob_info['pending_blob_is_complete'] and blob_info['pending']]),
        }

    print json.dumps(to_recover, indent=2)


if __name__ == "__main__":
    main()
