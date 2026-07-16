#!/usr/bin/env python3
"""Scripted FCVAE swap proof via scored-output snapshots (F1).

The FCVAE pipeline is STATEFUL (event-time 1h jumping window feeding a KEEP 24
ROWS sliding window per combo_key), so swap proofs use TIME-SHIFTED IDENTICAL
FEEDS (see make_fcvae_feed.py): each phase re-feeds the same base slice with
every timestamp shifted forward by K whole days. Whole-day shifts preserve
per-hour event counts exactly, so every phase presents bit-identical window
vectors to the scorer OP. Identical vectors through the same ONNX session score
bit-identically, therefore:

  - a real swap changes (essentially all) scores;
  - a rollback matches the pre-swap snapshot EXACTLY;
  - a rejected candidate leaves scores EXACTLY unchanged.

Scores are keyed by the integer HOUR OFFSET of window_end from the phase start
(base_start + K days), and each phase's first 23 windows are dropped: they mix
the previous feed's tail rows inside the KEEP 24 ROWS sliding window.

Scored records are JSONFormatter objects with STRING fields combo_key,
window_end ("2025/02/17 00:58:09.000"), is_anomaly, anomaly_score, threshold.
JSONFormatter files are OPEN arrays until rollover, so records are parsed one
at a time with a tolerant raw_decode loop across ALL matching rolled files.

Subcommands:
  snapshot  capture {hour_offset: {score, threshold, is_anomaly}} for a phase.
  compare   diff two snapshots, asserting --expect identical|different.
"""
import datetime as dt
import glob
import json
import os
import sys
import time

import click

# The first 23 windows of a phase mix the previous feed's tail rows in the
# KEEP 24 ROWS sliding window, so they are never comparable across phases.
WARMUP_KEYS = 23

POLL_SEC = 5


def iter_records(text):
    """Yield every complete JSON object in a JSONFormatter open-array file (it
    emits '[ {..}, {..},' and only closes ']' on rollover): parse objects one
    at a time, skip separators, stop at the first incomplete trailing object."""
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


def all_records(pred_dir, prefix):
    """All records across ALL rolled files, in file order (latest last)."""
    recs = []
    for path in sorted(glob.glob(os.path.join(pred_dir, prefix + "*.json"))):
        with open(path) as f:
            recs.extend(iter_records(f.read()))
    return recs


def parse_window_end(s):
    s = str(s).strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def parse_bool(s):
    return str(s).strip().lower() in ("true", "1", "yes", "t")


def collect(pred_dir, prefix, phase_start, phase_end):
    """One phase's scores keyed by hour offset; latest record wins per key."""
    snap = {}
    for r in all_records(pred_dir, prefix):
        we = parse_window_end(r.get("window_end"))
        if we is None or not (phase_start <= we < phase_end):
            continue
        key = int((we - phase_start).total_seconds() // 3600)
        if key < WARMUP_KEYS:
            continue
        try:
            snap[key] = {
                "score": float(str(r.get("anomaly_score")).strip()),
                "threshold": float(str(r.get("threshold")).strip()),
                "is_anomaly": parse_bool(r.get("is_anomaly")),
            }
        except (TypeError, ValueError):
            continue
    return snap


def thr_summary(snap, keys):
    vals = sorted({snap[k]["threshold"] for k in keys})
    if len(vals) == 1:
        return f"{vals[0]:.6g}"
    return f"{vals[0]:.6g}..{vals[-1]:.6g}"


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option("--out", required=True, type=click.Path(dir_okay=False),
              help="Snapshot JSON to write.")
@click.option("--base-start", "base_start", required=True,
              help="Base slice start date, YYYY-MM-DD (first row's date of the base feed).")
@click.option("--shift-days", "shift_days", required=True, type=int,
              help="This phase's whole-day shift K; phase start = base_start + K days.")
@click.option("--days", default=5, show_default=True, type=int,
              help="Phase length in days (the base slice's --days).")
@click.option("--pred-dir", "pred_dir", default="/opt/Striim/UploadedFiles",
              show_default=True, help="Directory holding the scored JSON files.")
@click.option("--prefix", default="fcvae_scored", show_default=True,
              help="Scored file prefix (all <prefix>*.json files are scanned).")
@click.option("--min-count", "min_count", default=1, show_default=True, type=int,
              help="Minimum number of keys required for the snapshot to succeed.")
@click.option("--wait-sec", "wait_sec", default=0, show_default=True, type=int,
              help="Poll every 5s until min-count keys appear; 0 = single attempt.")
def snapshot(out, base_start, shift_days, days, pred_dir, prefix, min_count, wait_sec):
    """Capture one phase's scores, keyed by hour offset from the phase start."""
    try:
        base = dt.datetime.strptime(base_start, "%Y-%m-%d")
    except ValueError:
        raise click.BadParameter(f"--base-start must be YYYY-MM-DD, got {base_start!r}")
    phase_start = base + dt.timedelta(days=shift_days)
    phase_end = phase_start + dt.timedelta(days=days)

    deadline = time.time() + wait_sec
    while True:
        snap = collect(pred_dir, prefix, phase_start, phase_end)
        if len(snap) >= min_count:
            break
        if time.time() >= deadline:
            click.echo(f"FAIL: only {len(snap)} keys (< min-count {min_count}) in "
                       f"[{phase_start.isoformat()}, {phase_end.isoformat()}) "
                       f"under {pred_dir}/{prefix}*.json")
            sys.exit(1)
        time.sleep(POLL_SEC)

    ordered = {str(k): snap[k] for k in sorted(snap)}
    with open(out, "w") as f:
        json.dump(ordered, f, indent=2)
    scores = [v["score"] for v in snap.values()]
    click.echo(f"SNAPSHOT keys={len(snap)} phase_start={phase_start.isoformat()} "
               f"score_range=[{min(scores):.6g}..{max(scores):.6g}]")


@cli.command()
@click.argument("a", type=click.Path(exists=True, dir_okay=False))
@click.argument("b", type=click.Path(exists=True, dir_okay=False))
@click.option("--expect", type=click.Choice(["identical", "different"]),
              help="Assertion; omitted = informational (exit 0 unless too few common keys).")
@click.option("--min-common", "min_common", default=50, show_default=True, type=int,
              help="Minimum common keys required for a meaningful comparison.")
def compare(a, b, expect, min_common):
    """Diff two snapshots; scores must match BITWISE for `identical`."""
    with open(a) as f:
        snap_a = json.load(f)
    with open(b) as f:
        snap_b = json.load(f)
    common = sorted(set(snap_a) & set(snap_b), key=int)
    if len(common) < min_common:
        click.echo(f"COMPARE common={len(common)} verdict=FAIL "
                   f"(fewer than --min-common {min_common} common keys)")
        sys.exit(1)

    # Exact float comparison: JSON round-trips floats losslessly (repr-based),
    # so equality here is bitwise equality of the scored outputs.
    changed = sum(1 for k in common if snap_a[k]["score"] != snap_b[k]["score"])
    max_abs = max(abs(snap_a[k]["score"] - snap_b[k]["score"]) for k in common)
    thr_a = thr_summary(snap_a, common)
    thr_b = thr_summary(snap_b, common)

    if expect == "identical":
        ok = all(snap_a[k]["score"] == snap_b[k]["score"]
                 and snap_a[k]["threshold"] == snap_b[k]["threshold"] for k in common)
    elif expect == "different":
        # A real swap moves essentially every score; require a strict majority.
        ok = changed * 2 > len(common)
    else:
        ok = True

    click.echo(f"COMPARE common={len(common)} changed={changed} max_abs_diff={max_abs:.6g} "
               f"thresholdA={thr_a} thresholdB={thr_b} expect={expect or 'none'} "
               f"verdict={'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    cli()
