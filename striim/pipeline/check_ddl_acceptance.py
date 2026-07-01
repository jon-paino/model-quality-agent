#!/usr/bin/env python3
"""Assert the agent's schema_evolution signal against a known number of DDLs.

Layer 2 Phase 1 acceptance check for the Quality Monitoring Agent. After issuing
exactly N DDL statements (see striim/mysql/run_ddl_acceptance.sh), this asserts,
off the agent's emitted health assessments:

    1. cumulative DDL total rose by EXACTLY N
         (max observed schema_evolution.ddls_total) - baseline == N
    2. the alert FIRED and accounted for every DDL: summing the per-tick deltas of
       the ticks where schema_evolution went WARN == N
    3. (sanity) a WARN tick exists and the verdict on it was YELLOW (Phase 1 posture:
       alert + verdict, breaker stays closed)

It reads the CUMULATIVE total (monotonic, stable) for the count, and the WARN
deltas for the alert, so it does not have to race the 30s tick window: the spike
WARN(s) and the cumulative total both survive in the rolled health files.

This mirrors check_miss_acceptance.py (same tolerant parse of the JSONFormatter
OPEN-array rollover). The DDL count is cumulative per SOURCE run and resets when the
source app restarts, so run against a FRESH deploy (baseline 0) or pass the
pre-test ddls_total via --baseline.

Usage:
    striim/mysql/run_ddl_acceptance.sh 3
    # wait ~30-60s for an agent tick
    .venv/bin/python striim/pipeline/check_ddl_acceptance.py --expected 3
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path

DELTA_RE = re.compile(r"^\+(\d+)\s+this tick")


def iter_complete_records(text: str):
    """Yield every complete JSON object in a JSONFormatter file. The FileWriter
    emits an OPEN array ('[ {...}, {...},') and only closes the ']' on rollover,
    so the currently-open file is not valid JSON as a whole. Decode objects one at
    a time and stop at the first incomplete trailing one."""
    decoder = json.JSONDecoder()
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i] in " \t\r\n,[]":
            i += 1
        if i >= n:
            break
        try:
            obj, end = decoder.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj
        i = end


def all_schema_ticks(health_dir: str, prefix: str) -> list[dict]:
    """Every tick's schema_evolution signal (deduped by tick_ts, time-ordered).
    Returns dicts: {tick_ts, verdict, state, delta, ddls_total, last_ddl}."""
    pattern = os.path.join(health_dir, prefix + "*.json")
    files = glob.glob(pattern)
    if not files:
        raise SystemExit(f"no health files matching {pattern}; has the agent ticked?")
    by_ts: dict[int, dict] = {}
    for f in files:
        for rec in iter_complete_records(Path(f).read_text()):
            try:
                ts = int(rec["tick_ts"])
            except (KeyError, ValueError, TypeError):
                continue
            try:
                assessment = json.loads(rec["assessment_json"])
            except (KeyError, json.JSONDecodeError):
                continue
            sig = next((s for s in assessment.get("signals", [])
                        if s.get("name") == "schema_evolution"), None)
            if sig is None:
                continue
            mobj = DELTA_RE.match(str(sig.get("observed", "")))
            by_ts[ts] = {
                "tick_ts": ts,
                "verdict": assessment.get("verdict"),
                "state": sig.get("state"),
                "delta": int(mobj.group(1)) if mobj else 0,
                "ddls_total": sig.get("ddls_total"),
                "last_ddl": sig.get("last_ddl"),
            }
    if not by_ts:
        raise SystemExit("no schema_evolution signal found in any assessment; "
                         "is this the MySQL-CDC pipeline with the Layer 2 agent?")
    return [by_ts[ts] for ts in sorted(by_ts)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--expected", type=int, required=True, help="N DDLs issued")
    ap.add_argument("--baseline", type=int, default=0,
                    help="ddls_total before the test (0 on a fresh deploy)")
    ap.add_argument("--health-dir", default="/opt/Striim/UploadedFiles")
    ap.add_argument("--health-prefix", default="health_assessments.")
    args = ap.parse_args()

    n = args.expected
    ticks = all_schema_ticks(args.health_dir, args.health_prefix)

    totals = [t["ddls_total"] for t in ticks if t["ddls_total"] is not None]
    max_total = max(totals) if totals else None
    cumulative_delta = (max_total - args.baseline) if max_total is not None else None

    # Only WARN ticks whose cumulative total is past the baseline, so the test is
    # repeatable against a long-running source without a fresh redeploy (the DDL
    # counter is cumulative per source run; earlier tests' WARNs sit below baseline).
    warn_ticks = [t for t in ticks if t["state"] == "WARN"
                  and (t["ddls_total"] is None or t["ddls_total"] > args.baseline)]
    warn_delta_sum = sum(t["delta"] for t in warn_ticks)
    warn_verdicts_ok = all(t["verdict"] == "YELLOW" for t in warn_ticks) and bool(warn_ticks)
    last_ddl = next((t["last_ddl"] for t in reversed(ticks) if t["last_ddl"] not in (None, "n/a")), None)

    print(f"health dir  : {args.health_dir}/{args.health_prefix}*")
    print(f"ticks seen  : {len(ticks)}  (WARN ticks: {len(warn_ticks)})")
    print(f"last DDL    : {last_ddl!r}")
    print(f"WARN ticks  : " + ", ".join(
        f"tick={t['tick_ts']} +{t['delta']} ({t['verdict']})" for t in warn_ticks) or "(none)")
    print("")
    print(f"{'metric':<26}{'expected':>10}{'observed':>10}{'result':>8}")
    print("-" * 54)

    checks = [
        ("cumulative_ddls_delta", n, cumulative_delta, cumulative_delta == n),
        ("sum_of_WARN_deltas", n, warn_delta_sum, warn_delta_sum == n),
        ("WARN_verdict_is_YELLOW", "yes", "yes" if warn_verdicts_ok else "no", warn_verdicts_ok),
    ]
    all_ok = True
    for label, exp, obs, ok in checks:
        all_ok &= ok
        print(f"{label:<26}{str(exp):>10}{str(obs):>10}{('PASS' if ok else 'FAIL'):>8}")

    print("")
    if all_ok:
        print(f"ACCEPTANCE PASSED: cumulative DDL total rose by exactly {n}, the "
              f"schema_evolution signal WARNed for all {n} (sum of WARN deltas == {n}), "
              f"and each WARN tick's verdict was YELLOW (breaker stayed closed).")
        sys.exit(0)
    print("ACCEPTANCE FAILED: see the table. If cumulative_ddls_delta != N, the most "
          "likely cause is a non-fresh source (counter cumulative; pass --baseline) or "
          "the agent missed a tick. Treat a real mismatch as a bug (miscount / window / "
          "the DDL-disposition sum in perceive()).")
    sys.exit(1)


if __name__ == "__main__":
    main()
