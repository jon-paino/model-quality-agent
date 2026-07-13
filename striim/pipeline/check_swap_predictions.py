#!/usr/bin/env python3
"""Scripted swap proof via scored-output snapshots (Week 3).

ModelOp's SWAPPED / ROLLED BACK / REJECTED lines go to the Striim server's
STDOUT (a terminal, in practice flooded by the agents' SysOut ticks), so swap
verification here is behavioral instead: feed the SAME CSV through the
pipeline before and after a model change and compare the predictions for
exactly those rows. Identical inputs through the same ONNX session are
bit-identical, so:

  - a real swap changes (essentially all) predictions;
  - a rollback makes them match the pre-swap snapshot EXACTLY;
  - a rejected candidate leaves them EXACTLY unchanged.

Usage:
  snapshot: after feeding <feed.csv> (a fresh FileReader filename each time,
            same CONTENT each time), capture the latest prediction per feed
            row into a JSON snapshot:
      check_swap_predictions.py snapshot --feed feed.csv --out snapA.json
          [--pred-dir /opt/Striim/UploadedFiles] [--prefix predictions]

  compare:  diff two snapshots, optionally asserting the expectation:
      check_swap_predictions.py compare snapA.json snapB.json \
          [--expect identical|different]
      exit 0 iff the expectation holds (or no --expect given).

The snapshot matcher walks the scored records from the END and keeps the
latest prediction for each feed row (keyed by the 6 input columns), stopping
at the first record that is not from this feed. So take a snapshot right
after each feed of the SAME content, before feeding anything else.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys


def iter_records(text: str):
    """Yield every complete JSON object in a JSONFormatter open-array file (it
    emits '[ {..}, {..},' and only closes ']' on rollover)."""
    dec = json.JSONDecoder()
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i] in " \t\r\n,[]":
            i += 1
        if i >= n:
            break
        try:
            obj, end = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj
        i = end


def all_records(pred_dir: str, prefix: str) -> list:
    recs = []
    for path in sorted(glob.glob(os.path.join(pred_dir, prefix + "*.json"))):
        with open(path) as f:
            recs.extend(iter_records(f.read()))
    return recs


def feed_keys(path: str) -> set:
    keys = set()
    with open(path) as f:
        for row in csv.reader(f):
            if len(row) >= 6:
                keys.add(tuple(x.strip() for x in row[:6]))
    return keys


def rec_key(r: dict) -> tuple:
    return (str(r.get("pickup_geohash", "")).strip(), str(r.get("hour_of_day", "")).strip(),
            str(r.get("day_of_week", "")).strip(), str(r.get("is_weekend", "")).strip(),
            str(r.get("trip_distance", "")).strip(), str(r.get("passenger_count", "")).strip())


def cmd_snapshot(args) -> int:
    fk = feed_keys(args.feed)
    snap = {}
    for r in reversed(all_records(args.pred_dir, args.prefix)):
        k = rec_key(r)
        if k not in fk:
            break
        snap.setdefault("|".join(k), float(r.get("prediction")))
    if not snap:
        print("FAIL: no trailing scored records match the feed "
              "(feed it with a fresh filename and wait for scoring first)")
        return 1
    with open(args.out, "w") as f:
        json.dump(snap, f)
    print(f"snapshot: {len(snap)} feed rows -> {args.out}")
    return 0


def cmd_compare(args) -> int:
    a = json.load(open(args.a))
    b = json.load(open(args.b))
    common = sorted(set(a) & set(b))
    if not common:
        print("FAIL: snapshots share no keys")
        return 1
    diffs = [abs(a[k] - b[k]) for k in common]
    changed = sum(1 for d in diffs if d > 1e-6)
    print(f"common={len(common)} changed={changed} "
          f"mean_abs_diff={sum(diffs) / len(diffs):.4f} max_abs_diff={max(diffs):.4f}")
    if args.expect == "identical":
        ok = changed == 0
    elif args.expect == "different":
        # a real swap moves essentially every prediction; >half is decisive
        ok = changed > len(common) // 2
    else:
        ok = True
    print("RESULT:", "PASS" if ok else "FAIL",
          f"(expected {args.expect})" if args.expect else "")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="Swap proof via scored-output snapshots")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("--feed", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--pred-dir", default="/opt/Striim/UploadedFiles")
    s.add_argument("--prefix", default="predictions")
    c = sub.add_parser("compare")
    c.add_argument("a")
    c.add_argument("b")
    c.add_argument("--expect", choices=["identical", "different"])
    args = ap.parse_args()
    return cmd_snapshot(args) if args.cmd == "snapshot" else cmd_compare(args)


if __name__ == "__main__":
    sys.exit(main())
