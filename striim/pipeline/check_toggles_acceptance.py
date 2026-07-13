#!/usr/bin/env python3
"""Week 2 acceptance: the standalone Quality Monitor (quality_monitor.tql) senses a
target app cross-app over mon/REST, and EnabledSignals gates the assessed signal set.

PRECONDITION (documented, not asserted): the MONITORED app (default
qualitydemo.FareInference) is deployed and running, and qualitymon.QualityMonitorApp
is deployed from striim/quality-agent/quality_monitor.tql with an EnabledSignals list
matching --enabled / --disabled below. This harness reads the LATEST assessment the
standalone monitor wrote (the QualityFileSink JSONFormatter output) and asserts:

  1. the standalone app emitted a verdict for the target app (the distinct file
     prefix proves it came from the standalone monitor, not the in-app HealthAgent);
  2. every ENABLED signal family is present (UNKNOWN allowed: backpressure and
     discarded_events legitimately read UNKNOWN over mon/REST);
  3. every DISABLED family is ABSENT from signals[] and contributes nothing to the
     verdict: fail_count / warn_count / verdict are reproduced exactly from the
     present signals by the agent's rollup rule, and the assessment's
     disabled_signals field matches --disabled;
  4. cross-app sensing works: app_status and >=1 freshness signal are non-UNKNOWN
     (both can only come from mon/REST here);
  5. rollup integrity: RED only with >=1 FAIL; breaker consistent with the verdict.

Exit 0 iff 1-5 hold. Mirrors the tolerant open-array decode in
check_monrest_acceptance.py / check_miss_acceptance.py.

Usage:
    .venv/bin/python striim/pipeline/check_toggles_acceptance.py \
        [--health-dir /opt/Striim/UploadedFiles] [--prefix quality_monitor_assessments] \
        [--target-app qualitydemo.FareInference] \
        [--enabled app_status,source_freshness,...] [--disabled feature_miss_rate,...] \
        [--after-ms 0]

The negative check is a second parameterized invocation: redeploy the monitor with
lag_end2end removed from EnabledSignals, then run with BOTH lists adjusted:
    --enabled app_status,source_freshness,target_write_age,backpressure,discarded_events,node_memory,node_cpu \
    --disabled feature_miss_rate,nan_score_rate,schema_evolution,lag_end2end \
    --after-ms <epoch ms captured just before the redeploy>
(--enabled must drop the family too, or check 2 will demand a signal the agent
correctly no longer emits. --after-ms guards against stale records from the
previous config; also delete quality_monitor_assessments*.json while the monitor
is undeployed.)
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

DEFAULT_ENABLED = ("app_status,source_freshness,target_write_age,lag_end2end,"
                   "backpressure,discarded_events,node_memory,node_cpu")
DEFAULT_DISABLED = "feature_miss_rate,nan_score_rate,schema_evolution"


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


def latest_assessment(health_dir: str, prefix: str, after_ms: int):
    best = None
    for path in sorted(glob.glob(os.path.join(health_dir, prefix + "*.json"))):
        with open(path) as f:
            text = f.read()
        for rec in iter_records(text):
            if tick_of(rec) < after_ms:
                continue
            if best is None or tick_of(rec) >= tick_of(best):
                best = rec
    if best is None:
        return None
    raw = best.get("assessment_json") or best.get("assessment")
    return json.loads(raw) if isinstance(raw, str) else raw


def matches(name: str, token: str) -> bool:
    """The Java-side toggle contract: a family token gates the exact name, its
    per-component instances (token[Component]), and its transport-specific
    variants (token_suffix, e.g. node_memory -> node_memory_free)."""
    n, t = name.lower(), token.strip().lower()
    return n == t or n.startswith(t + "[") or n.startswith(t + "_")


def rollup(signals: list) -> tuple:
    """Reproduce ModelQualityAgent.assess()'s rollup over the present signals."""
    fail = sum(1 for s in signals if s.get("state") == "FAIL")
    warn = sum(1 for s in signals if s.get("state") == "WARN")
    any_known = any(s.get("state") != "UNKNOWN" for s in signals)
    if fail > 0:
        verdict = "RED"
    elif warn > 0:
        verdict = "YELLOW"
    elif any_known:
        verdict = "GREEN"
    else:
        verdict = "UNKNOWN"
    return verdict, fail, warn


def main() -> int:
    ap = argparse.ArgumentParser(description="Week 2 EnabledSignals + standalone monitor acceptance")
    ap.add_argument("--health-dir", default="/opt/Striim/UploadedFiles")
    ap.add_argument("--prefix", default="quality_monitor_assessments")
    ap.add_argument("--target-app", default="qualitydemo.FareInference")
    ap.add_argument("--enabled", default=DEFAULT_ENABLED)
    ap.add_argument("--disabled", default=DEFAULT_DISABLED)
    ap.add_argument("--after-ms", type=int, default=0,
                    help="ignore records with tick_ts older than this epoch-ms")
    args = ap.parse_args()

    enabled = [t.strip().lower() for t in args.enabled.split(",") if t.strip()]
    disabled = [t.strip().lower() for t in args.disabled.split(",") if t.strip()]

    a = latest_assessment(args.health_dir, args.prefix, args.after_ms)
    if a is None:
        print("FAIL: no assessment records found in", args.health_dir,
              f"(prefix={args.prefix}, after_ms={args.after_ms})",
              "\n(deploy quality_monitor.tql, wait 2+ ticks, then re-run.)")
        return 1

    signals = a.get("signals", [])
    state = {s.get("name", ""): s.get("state") for s in signals}
    verdict = a.get("verdict")
    breaker = a.get("ops_circuit_breaker_open")

    def known(st):
        return st not in (None, "UNKNOWN")

    checks = []

    # 1. the standalone app emitted a verdict for the target app
    checks.append(("standalone monitor emitted a verdict for the target app",
                   a.get("target_app") == args.target_app
                   and verdict in ("GREEN", "YELLOW", "RED", "UNKNOWN"),
                   f"target_app={a.get('target_app')}, verdict={verdict}"))

    # 2. every ENABLED family present (UNKNOWN allowed)
    missing = [t for t in enabled if not any(matches(n, t) for n in state)]
    checks.append(("every enabled signal family present", not missing,
                   f"missing={missing}" if missing else f"{len(enabled)} families present"))

    # 3a. every DISABLED family absent from signals[]
    leaked = [n for n in state if any(matches(n, t) for t in disabled)]
    checks.append(("every disabled signal family absent", not leaked,
                   f"leaked={leaked}" if leaked else f"{len(disabled)} families absent"))

    # 3b. counts and verdict are reproduced exactly from the present signals
    r_verdict, r_fail, r_warn = rollup(signals)
    counts_ok = (r_fail == int(a.get("fail_count", -1))
                 and r_warn == int(a.get("warn_count", -1))
                 and r_verdict == verdict)
    checks.append(("verdict/counts reproduced from present signals only", counts_ok,
                   f"recomputed={r_verdict}/{r_fail}f/{r_warn}w, "
                   f"emitted={verdict}/{a.get('fail_count')}f/{a.get('warn_count')}w"))

    # 3c. disabled_signals field states the policy
    ds = a.get("disabled_signals", [])
    ds_ok = set(ds) == set(disabled) if disabled else not ds
    checks.append(("assessment disabled_signals matches the expected policy", ds_ok,
                   f"disabled_signals={ds}"))

    # 4. cross-app sensing over mon/REST
    checks.append(("app_status from mon/REST (non-UNKNOWN)", known(state.get("app_status")),
                   f"app_status={state.get('app_status')}"))
    fresh = [n for n, st in state.items()
             if (n.startswith("source_freshness") or n.startswith("target_write_age")) and known(st)]
    checks.append((">=1 freshness signal non-UNKNOWN", len(fresh) > 0,
                   f"{len(fresh)} fresh: {fresh[:3]}"))

    # 5. rollup integrity
    fails = [n for n, st in state.items() if st == "FAIL"]
    checks.append(("RED only if a signal FAILs (UNKNOWN never gates)",
                   (verdict != "RED") or (len(fails) >= 1),
                   f"verdict={verdict}, FAILs={fails}"))
    breaker_ok = (bool(breaker) is False) or (verdict == "RED") or (verdict == "YELLOW")
    checks.append(("circuit breaker consistent with verdict", breaker_ok,
                   f"breaker_open={breaker}, verdict={verdict}"))

    print(f"assessment tick_ts={a.get('tick_ts')} target_app={a.get('target_app')} "
          f"verdict={verdict} ops_healthy={a.get('ops_healthy')} breaker_open={breaker}")
    ok = True
    for name, passed, detail in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  ({detail})")
        ok = ok and passed

    print("\nACCEPTANCE:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
