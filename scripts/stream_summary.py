import sys
import json
from prism.debug import PrismInspector


def main():
    inspector = PrismInspector()
    sd_hashes = sys.argv[1:]
    for sd_hash in sd_hashes:
        print json.dumps(inspector.get_stream_summary(sd_hash, True), indent=2)
    sys.exit(0)


if __name__ == "__main__":
    main()
