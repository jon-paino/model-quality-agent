#!/usr/bin/env python3
"""Assert the agent's reported cumulative counters against the labeled feed.

Phase 1b acceptance check for the Quality Monitoring Agent. Given the ground
truth written by make_miss_feed.py (<feed>.truth.json) and the agent's emitted
health assessment, this asserts the EXACT cumulative totals:

    reported faults      == M   (the forced feature-store misses)
    reported events_seen == N   (total rows fed)
    derived miss rate     = M / N within tolerance

It reads the cumulative totals, NOT the per-tick windowed rate, so it does not
have to fight the tick window: those totals are monotonic and stable once the
feed has fully drained.

PRECONDITION (enforced by reading, not assumed): the counters are cumulative
across an OP instance's life, so this must run against a FRESH deploy of
qualitydemo.FareInference fed ONLY the miss file. If events_seen != N, the most
likely cause is a stale deployment that was already fed other rows; redeploy and
re-feed rather than papering over it.

Usage:
    .venv/bin/python striim/pipeline/check_miss_acceptance.py \
        --truth /opt/Striim/UploadedFiles/pipeline_trips_miss1.csv.truth.json
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from pathlib import Path


def iter_complete_records(text: str):
    """Yield every complete JSON object in a JSONFormatter file. The FileWriter
    emits an OPEN array ('[ {...}, {...},') and only closes the ']' on rollover,
    so the currently-open file is not valid JSON as a whole. We decode objects
    one at a time and stop at the first incomplete trailing one."""
    decoder = json.JSONDecoder()
    i, n = 0, len(text)
    while i < n:
        # Skip array/object separators and whitespace between records.
        while i < n and text[i] in " \t\r\n,[]":
            i += 1
        if i >= n:
            break
        try:
            obj, end = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            break  # trailing partial record (file still being written)
        yield obj
        i = end


def latest_assessment(health_dir: str, prefix: str) -> tuple[dict, str]:
    """Across all health files (closed and currently-open), find the record with
    the greatest tick_ts and return its embedded assessment object."""
    pattern = os.path.join(health_dir, prefix + "*.json")
    files = glob.glob(pattern)
    if not files:
        raise SystemExit(f"no health files matching {pattern}; has the agent ticked?")
    best = None
    best_file = None
    for f in files:
        for rec in iter_complete_records(Path(f).read_text()):
            try:
                ts = int(rec["tick_ts"])
            except (KeyError, ValueError, TypeError):
                continue
            if best is None or ts > best[0]:
                best = (ts, rec)
                best_file = f
    if best is None:
        raise SystemExit("no complete assessment records found; has the agent ticked?")
    return json.loads(best[1]["assessment_json"]), best_file


def find_signal(assessment: dict, name: str) -> dict:
    for sig in assessment.get("signals", []):
        if sig.get("name") == name:
            return sig
    raise SystemExit(f"signal '{name}' not found in assessment "
                     f"(signals: {[s.get('name') for s in assessment.get('signals', [])]})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", required=True, help="<feed>.truth.json from make_miss_feed.py")
    ap.add_argument("--health-dir", default="/opt/Striim/UploadedFiles")
    ap.add_argument("--health-prefix", default="health_assessments.")
    ap.add_argument("--signal", default="feature_miss_rate")
    ap.add_argument("--tolerance", type=float, default=1e-6)
    args = ap.parse_args()

    truth = json.loads(Path(args.truth).read_text())
    n, m = truth["n"], truth["m"]
    expected_rate = truth["expected_feature_miss_rate"]

    assessment, health_file = latest_assessment(args.health_dir, args.health_prefix)
    sig = find_signal(assessment, args.signal)

    events_seen = sig.get("events_seen")
    faults = sig.get("faults")
    if events_seen is None or faults is None:
        raise SystemExit(f"signal '{args.signal}' carries no cumulative totals "
                         f"(events_seen/faults); got: {sig}")
    observed_rate = (faults / events_seen) if events_seen else 0.0

    print(f"health file : {health_file}")
    print(f"verdict     : {assessment.get('verdict')} "
          f"(tick_ts {assessment.get('tick_ts')})")
    print(f"signal      : {args.signal}  state={sig.get('state')}  "
          f"observed={sig.get('observed')!r}")
    print("")
    print(f"{'metric':<18}{'expected':>12}{'observed':>12}{'result':>10}")
    print("-" * 52)

    checks = [
        ("events_seen", n, events_seen, events_seen == n),
        ("faults", m, faults, faults == m),
        ("miss_rate", round(expected_rate, 6), round(observed_rate, 6),
         abs(observed_rate - expected_rate) <= args.tolerance),
    ]
    all_ok = True
    for label, exp, obs, ok in checks:
        all_ok &= ok
        print(f"{label:<18}{str(exp):>12}{str(obs):>12}{('PASS' if ok else 'FAIL'):>10}")

    print("")
    if all_ok:
        print(f"ACCEPTANCE PASSED: faults == M == {m} and events_seen == N == {n} "
              f"(exact); miss rate {observed_rate:.4f} == {expected_rate:.4f}.")
        sys.exit(0)
    print("ACCEPTANCE FAILED: see the table above. If events_seen != N, redeploy "
          "fresh and feed ONLY the miss file (counters are cumulative). Otherwise "
          "treat the mismatch as a real bug (double-count / denominator / window).")
    sys.exit(1)


if __name__ == "__main__":
    main()
