#!/usr/bin/env python3
"""Week 1 acceptance: the Quality Agent produces a valid verdict over mon/REST with
JMX out of the loop.

PRECONDITION (documented, not asserted): deploy qualitydemo.FareInference with the
HealthAgent configured `HealthSource: 'MON_REST'` (see inference_pipeline.tql), and
feed it. This harness then reads the LATEST emitted health assessment (the
HealthFileSink JSONFormatter output) and asserts the Week-1 bar:

  1. a verdict is emitted (GREEN/YELLOW/RED/UNKNOWN);
  2. app_status is non-UNKNOWN  -- sourced from mon/REST, since JMX is not read on
     the MON_REST path;
  3. at least one freshness signal (source_freshness* / target_write_age*) is
     non-UNKNOWN -- also from mon/REST;
  4. no signal-ABSENCE forces RED: a RED verdict must be backed by >=1 FAIL signal
     (UNKNOWN signals never gate), and the circuit breaker is consistent with the
     verdict (open only on RED, or YELLOW when TreatYellowAsHealthy=false);
  5. STRETCH (warn-only): an ML signal (feature_miss_rate / nan_score_rate) is being
     computed from the stream this tick (detail carries `[source=stream]`). This is
     timing-dependent (the stream path is active only while scored events flow; it
     falls back to the OpCounter MBean when idle), so it is reported, not required.

Exit 0 iff 1-4 hold. Mirrors the tolerant open-array decode in check_miss_acceptance.py.

Usage:
    .venv/bin/python striim/pipeline/check_monrest_acceptance.py \
        [--health-dir /opt/Striim/UploadedFiles] [--prefix health_assessments]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys


def iter_records(text: str):
    """Yield every complete JSON object in a JSONFormatter open-array file (it emits
    '[ {..}, {..},' and only closes ']' on rollover, so the newest file is not valid
    JSON as a whole)."""
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


def tick_of(rec: dict) -> int:
    try:
        return int(str(rec.get("tick_ts", "0")))
    except (TypeError, ValueError):
        return 0


def latest_assessment(health_dir: str, prefix: str):
    best = None
    for path in sorted(glob.glob(os.path.join(health_dir, prefix + "*.json"))):
        with open(path) as f:
            text = f.read()
        for rec in iter_records(text):
            if best is None or tick_of(rec) >= tick_of(best):
                best = rec
    if best is None:
        return None
    raw = best.get("assessment_json") or best.get("assessment")
    return json.loads(raw) if isinstance(raw, str) else raw


def main() -> int:
    ap = argparse.ArgumentParser(description="Week 1 mon/REST acceptance check")
    ap.add_argument("--health-dir", default="/opt/Striim/UploadedFiles")
    ap.add_argument("--prefix", default="health_assessments")
    args = ap.parse_args()

    a = latest_assessment(args.health_dir, args.prefix)
    if a is None:
        print("FAIL: no health assessment records found in", args.health_dir,
              "\n(deploy with HealthSource=MON_REST, feed, wait a tick, then re-run.)")
        return 1

    signals = a.get("signals", [])
    state = {s.get("name", ""): s.get("state") for s in signals}
    verdict = a.get("verdict")
    breaker = a.get("ops_circuit_breaker_open")

    def known(st):
        return st not in (None, "UNKNOWN")

    checks = []

    # 1. a verdict is emitted
    checks.append(("verdict emitted", verdict in ("GREEN", "YELLOW", "RED", "UNKNOWN"),
                   f"verdict={verdict}"))

    # 2. app_status non-UNKNOWN (from mon/REST)
    checks.append(("app_status from mon/REST (non-UNKNOWN)", known(state.get("app_status")),
                   f"app_status={state.get('app_status')}"))

    # 3. >=1 freshness signal non-UNKNOWN (from mon/REST)
    fresh = [n for n, st in state.items()
             if (n.startswith("source_freshness") or n.startswith("target_write_age")) and known(st)]
    checks.append((">=1 freshness signal non-UNKNOWN", len(fresh) > 0,
                   f"{len(fresh)} fresh: {fresh[:3]}"))

    # 4. no signal-absence forces RED; breaker consistent with verdict
    fails = [n for n, st in state.items() if st == "FAIL"]
    red_backed = (verdict != "RED") or (len(fails) >= 1)
    checks.append(("RED only if a signal FAILs (UNKNOWN never gates)", red_backed,
                   f"verdict={verdict}, FAILs={fails}"))
    breaker_ok = (bool(breaker) is False) or (verdict == "RED") or (verdict == "YELLOW")
    checks.append(("circuit breaker consistent with verdict", breaker_ok,
                   f"breaker_open={breaker}, verdict={verdict}"))

    # 5. STRETCH: ML signal from the stream this tick (warn-only)
    ml_stream = any("[source=stream]" in (s.get("detail") or "")
                    for s in signals if s.get("name") in ("feature_miss_rate", "nan_score_rate"))

    print(f"assessment tick_ts={a.get('tick_ts')} verdict={verdict} "
          f"ops_healthy={a.get('ops_healthy')} breaker_open={breaker}")
    ok = True
    for name, passed, detail in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  ({detail})")
        ok = ok and passed
    print(f"  [{'yes ' if ml_stream else 'n/a '}] STRETCH: ML signal computed from the stream this tick "
          f"(timing-dependent, not required)")

    print("\nACCEPTANCE:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
