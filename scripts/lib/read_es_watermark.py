"""Read one Elasticsearch disk watermark setting from `_cluster/settings` on stdin.

Effective settings live in transient, then persistent, then defaults; the first
one present wins, because that is the order Elasticsearch resolves them in.
Kept as a file rather than an inline heredoc because embedding it in a shell
function makes the quoting unreadable, and this is the kind of parsing that
should be testable on its own.
"""

import json
import sys

KEYS = ("cluster", "routing", "allocation", "disk", "watermark")


def effective(document, name):
    for layer in ("transient", "persistent", "defaults"):
        node = document
        for key in (layer, *KEYS):
            if not isinstance(node, dict):
                node = None
                break
            node = node.get(key)
        if isinstance(node, dict) and node.get(name) is not None:
            return node[name]
    return ""


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("low", "high", "flood_stage"):
        raise SystemExit("usage: read_es_watermark.py low|high|flood_stage < cluster_settings.json")
    print(effective(json.load(sys.stdin), sys.argv[1]))


if __name__ == "__main__":
    main()
