#!/usr/bin/env python3
"""Render the monitored consumer-group allow-list from its single source.

`deploy/observability/single-node/monitored-consumer-groups.txt` is the only
hand-written copy of the allow-list. This script rewrites the three derived
artifacts so they stay byte-identical:

  1. scripts/manage_tuba_monitoring.py  — MONITORED_CONSUMER_GROUPS tuple
  2. deploy/observability/single-node/rules/kafka.yml — the two alert exprs
  3. deploy/observability/single-node/grafana/provisioning/dashboards/
     tuba-single-node.json — the lag panel query

Run without arguments to rewrite in place (idempotent); run with --check to
verify only, exiting 1 and naming the drifted files when any artifact no
longer matches the source. The rewrites are anchored: in the Python file only
the `MONITORED_CONSUMER_GROUPS = "|".join((` ... `))` block is regenerated,
and in kafka.yml / the dashboard JSON only `consumergroup=~"<allow-list>"`
matchers are replaced — surrounding hand-written content is left untouched.
"""

import argparse
import os
import re
import sys


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE = os.path.join(REPO_ROOT, "deploy", "observability", "single-node", "monitored-consumer-groups.txt")
TARGETS = {
    "manage_tuba_monitoring.py": os.path.join(REPO_ROOT, "scripts", "manage_tuba_monitoring.py"),
    "kafka.yml": os.path.join(REPO_ROOT, "deploy", "observability", "single-node", "rules", "kafka.yml"),
    "tuba-single-node.json": os.path.join(REPO_ROOT, "deploy", "observability", "single-node",
                                          "grafana", "provisioning", "dashboards", "tuba-single-node.json"),
}

GENERATED_HEADER = (
    "    # GENERATED from deploy/observability/single-node/monitored-consumer-groups.txt\n"
    "    # by scripts/generate_monitoring_allowlist.py — edit the source, not this block.\n"
)


def load_source(path):
    """Return (entries, body_lines): regex branches and comment/entry body."""
    with open(path, encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    try:
        separator = lines.index("---")
    except ValueError:
        raise RuntimeError("source is missing the '---' header/body separator: " + path)
    body = lines[separator + 1:]
    entries = []
    for line in body:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            entries.append(stripped)
    if not entries:
        raise RuntimeError("source lists no consumer-group branches: " + path)
    return entries, body


def render_python_block(body):
    lines = ['MONITORED_CONSUMER_GROUPS = "|".join((\n', GENERATED_HEADER]
    for line in body:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            lines.append("    " + stripped + "\n")
        else:
            lines.append('    r"%s",\n' % stripped)
    lines.append("))\n")
    return "".join(lines)


def patch_python(content, body):
    pattern = re.compile(r'MONITORED_CONSUMER_GROUPS = "\|"\.join\(\(\n.*?\n\)\)\n', re.DOTALL)
    block = render_python_block(body)
    patched, count = pattern.subn(lambda _: block, content)
    if count != 1:
        raise RuntimeError("expected exactly one MONITORED_CONSUMER_GROUPS block, found %d" % count)
    return patched


def patch_matcher_file(content, alternation, escaped, expected_count, name):
    if escaped:
        pattern = re.compile(r'consumergroup=~\\"tuba-[^\\"]*\\"')
        replacement = 'consumergroup=~\\"%s\\"' % alternation
    else:
        pattern = re.compile(r'consumergroup=~"tuba-[^"]*"')
        replacement = 'consumergroup=~"%s"' % alternation
    patched, count = pattern.subn(lambda _: replacement, content)
    if count != expected_count:
        raise RuntimeError("%s: expected %d consumergroup matchers, found %d" % (name, expected_count, count))
    return patched


def render_all(source_path=SOURCE):
    entries, body = load_source(source_path)
    alternation = "|".join(entries)
    rendered = {}
    with open(TARGETS["manage_tuba_monitoring.py"], encoding="utf-8") as handle:
        rendered["manage_tuba_monitoring.py"] = patch_python(handle.read(), body)
    with open(TARGETS["kafka.yml"], encoding="utf-8") as handle:
        rendered["kafka.yml"] = patch_matcher_file(handle.read(), alternation, escaped=False,
                                                   expected_count=3, name="kafka.yml")
    with open(TARGETS["tuba-single-node.json"], encoding="utf-8") as handle:
        rendered["tuba-single-node.json"] = patch_matcher_file(handle.read(), alternation, escaped=True,
                                                               expected_count=1, name="tuba-single-node.json")
    return rendered


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="verify only; exit 1 naming drifted files, write nothing")
    args = parser.parse_args()
    rendered = render_all()
    drifted = []
    for name, content in rendered.items():
        with open(TARGETS[name], encoding="utf-8") as handle:
            current = handle.read()
        if current == content:
            print("%s: in sync" % name)
            continue
        if args.check:
            drifted.append(name)
            print("%s: DRIFTED (run scripts/generate_monitoring_allowlist.py)" % name)
        else:
            temporary = TARGETS[name] + ".tmp"
            with open(temporary, "w", encoding="utf-8", newline="") as handle:
                handle.write(content)
            os.replace(temporary, TARGETS[name])
            print("%s: rewritten" % name)
    if drifted:
        print("allow-list drift in: " + ", ".join(drifted), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
