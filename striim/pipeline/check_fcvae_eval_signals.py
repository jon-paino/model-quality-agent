#!/usr/bin/env python3
"""Assert properties of the FCVAE monitor's emitted health assessments (F3).

Reads the JSONFormatter open-array records the fcvaemon FileWriter writes
(fcvae_health_assessments*.json; each record is the FormatFcvaeHealth
projection whose assessment_json string is the typed truth), takes the LATEST
assessment with tick_ts strictly after --after-ms, and checks the requested
assertions against it.

The whole assertion set is POLLED until it passes or --wait-sec expires: an
assessment tick may lag a just-written eval file by one TickIntervalSec, so
"latest record" alone is racy while poll-until-satisfied is not. On timeout
the last failing checks are printed and the exit code is 1.

Assertions (all optional, combined):
  --eval-file PATH            every non-UNKNOWN metric-family signal's `value`
                              (and `baseline` where present) must match the
                              eval file within 1e-9:
                                model_precision -> eval.point_adjusted.precision
                                model_recall    -> eval.point_adjusted.recall
                                model_f1        -> eval.point_adjusted.f1
                                anomaly_rate    -> scoring.anomaly_rate
  --expect-state NAME=STATE   exact signal name has exactly this state
                              (repeatable), e.g. 'model_f1[Penny_All]=FAIL'
  --expect-nonunknown CSV     every listed signal name is present and not
                              UNKNOWN
  --expect-unknown-families CSV
                              every present instance of each family is UNKNOWN
                              and at least one instance per family is present
                              (UNKNOWN must never read as a silent PASS)
  --expect-absent-families CSV
                              no signal of these families appears at all (the
                              EnabledSignals-disabled case) and each family is
                              listed in disabled_signals
  --expect-verdict V          assessment verdict equals V
  --require-coherent-rollup   recompute the verdict from signals[] (RED iff any
                              FAIL, else YELLOW iff any WARN, else GREEN if any
                              known else UNKNOWN) and require it, fail_count,
                              warn_count, ops_healthy and the breaker boolean
                              to be mutually consistent (TreatYellowAsHealthy
                              default true: ops_healthy == verdict != RED)
"""
import datetime as dt
import glob
import json
import os
import sys
import time

import click

POLL_SEC = 3
METRIC_FAMILIES = ("model_precision", "model_recall", "model_f1", "anomaly_rate")
TOL = 1e-9


def iter_records(text):
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


def latest_assessment(pred_dir, prefix, after_ms):
    """(tick_ts, assessment dict) of the newest record after after_ms, or None."""
    best_ts = None
    best = None
    for path in sorted(glob.glob(os.path.join(pred_dir, prefix + "*.json"))):
        try:
            with open(path) as f:
                text = f.read()
        except OSError:
            continue
        for r in iter_records(text):
            try:
                ts = int(str(r.get("tick_ts")).strip())
            except (TypeError, ValueError):
                continue
            if ts <= after_ms:
                continue
            if best_ts is None or ts >= best_ts:
                best_ts = ts
                best = r
    if best is None:
        return None
    raw = best.get("assessment_json") or best.get("assessment")
    try:
        assessment = raw if isinstance(raw, dict) else json.loads(raw)
    except (TypeError, ValueError):
        return None
    return best_ts, assessment


def family_of(name):
    base = name.split("[", 1)[0]
    return base if base in METRIC_FAMILIES else None


def combo_of(name):
    if "[" in name and name.endswith("]"):
        return name.split("[", 1)[1][:-1]
    return None


def close(a, b):
    try:
        return abs(float(a) - float(b)) <= TOL
    except (TypeError, ValueError):
        return False


def check_eval_file(assessment, eval_path):
    """Non-UNKNOWN metric signals must match the eval file within 1e-9."""
    fails = []
    try:
        with open(eval_path) as f:
            ev = json.load(f)
    except (OSError, ValueError) as exc:
        return [f"eval-file: cannot read {eval_path}: {exc}"]
    combos = ev.get("combos", {})
    checked = 0
    for sig in assessment.get("signals", []):
        fam = family_of(str(sig.get("name", "")))
        if fam is None or sig.get("state") == "UNKNOWN":
            continue
        combo = combo_of(str(sig.get("name", "")))
        block = combos.get(combo)
        if block is None:
            fails.append(f"eval-file: {sig['name']}: combo {combo} not in eval file")
            continue
        if fam == "anomaly_rate":
            expected = (block.get("scoring") or {}).get("anomaly_rate")
        else:
            key = fam.split("_", 1)[1]  # precision | recall | f1
            expected = ((block.get("eval") or {}).get("point_adjusted") or {}).get(key)
        if not close(sig.get("value"), expected):
            fails.append(f"eval-file: {sig['name']}: value {sig.get('value')} != "
                         f"eval file {expected}")
            continue
        if fam != "anomaly_rate":
            base = (block.get("baseline") or {}).get("pa_" + fam.split("_", 1)[1])
            if base is not None and not close(sig.get("baseline"), base):
                fails.append(f"eval-file: {sig['name']}: baseline {sig.get('baseline')}"
                             f" != manifest {base}")
                continue
        checked += 1
    if checked == 0:
        fails.append("eval-file: no non-UNKNOWN metric signals to compare")
    return fails


def check_rollup(assessment):
    fails = []
    states = [s.get("state") for s in assessment.get("signals", [])]
    n_fail = states.count("FAIL")
    n_warn = states.count("WARN")
    any_known = any(s != "UNKNOWN" for s in states)
    if n_fail > 0:
        expect = "RED"
    elif n_warn > 0:
        expect = "YELLOW"
    elif any_known:
        expect = "GREEN"
    else:
        expect = "UNKNOWN"
    if assessment.get("verdict") != expect:
        fails.append(f"rollup: verdict {assessment.get('verdict')} != recomputed {expect}")
    if assessment.get("fail_count") != n_fail:
        fails.append(f"rollup: fail_count {assessment.get('fail_count')} != {n_fail}")
    if assessment.get("warn_count") != n_warn:
        fails.append(f"rollup: warn_count {assessment.get('warn_count')} != {n_warn}")
    # TreatYellowAsHealthy default true: unhealthy iff RED; breaker = !healthy.
    expect_healthy = assessment.get("verdict") != "RED"
    if bool(assessment.get("ops_healthy")) != expect_healthy:
        fails.append(f"rollup: ops_healthy {assessment.get('ops_healthy')} != {expect_healthy}")
    if bool(assessment.get("ops_circuit_breaker_open")) == bool(assessment.get("ops_healthy")):
        fails.append("rollup: breaker must be the negation of ops_healthy")
    return fails


def run_checks(assessment, eval_file, expect_state, expect_nonunknown,
               expect_unknown_families, expect_absent_families, expect_verdict,
               require_coherent_rollup):
    fails = []
    signals = {str(s.get("name")): s for s in assessment.get("signals", [])}

    if eval_file:
        fails += check_eval_file(assessment, eval_file)

    for spec in expect_state:
        name, _, state = spec.partition("=")
        sig = signals.get(name)
        if sig is None:
            fails.append(f"state: {name} absent from signals[]")
        elif sig.get("state") != state:
            fails.append(f"state: {name} is {sig.get('state')}, expected {state}")

    for name in expect_nonunknown:
        sig = signals.get(name)
        if sig is None:
            fails.append(f"nonunknown: {name} absent from signals[]")
        elif sig.get("state") == "UNKNOWN":
            fails.append(f"nonunknown: {name} is UNKNOWN ({sig.get('detail')})")

    for fam in expect_unknown_families:
        members = [s for n, s in signals.items() if n == fam or n.startswith(fam + "[")]
        if not members:
            fails.append(f"unknown-family: no {fam} signal present at all")
            continue
        for s in members:
            if s.get("state") != "UNKNOWN":
                fails.append(f"unknown-family: {s.get('name')} is {s.get('state')},"
                             " expected UNKNOWN")

    disabled = assessment.get("disabled_signals", [])
    for fam in expect_absent_families:
        members = [n for n in signals if n == fam or n.startswith(fam + "[")]
        if members:
            fails.append(f"absent-family: {fam} present as {members}")
        if fam not in disabled:
            fails.append(f"absent-family: {fam} not listed in disabled_signals {disabled}")

    if expect_verdict and assessment.get("verdict") != expect_verdict:
        fails.append(f"verdict: {assessment.get('verdict')} != expected {expect_verdict}")

    if require_coherent_rollup:
        fails += check_rollup(assessment)

    return fails


@click.command(help=__doc__)
@click.option("--pred-dir", "pred_dir", default="/opt/Striim/UploadedFiles",
              show_default=True, help="Directory of assessment JSON files.")
@click.option("--prefix", default="fcvae_health_assessments", show_default=True,
              help="Assessment file prefix.")
@click.option("--after-ms", "after_ms", default=0, show_default=True, type=int,
              help="Only consider assessments with tick_ts strictly after this epoch ms.")
@click.option("--wait-sec", "wait_sec", default=90, show_default=True, type=int,
              help="Poll until the assertion set passes or this deadline expires.")
@click.option("--eval-file", "eval_file", default=None,
              type=click.Path(dir_okay=False),
              help="eval_metrics.json to match signal values against (1e-9).")
@click.option("--expect-state", "expect_state", multiple=True,
              help="NAME=STATE exact assertion, repeatable.")
@click.option("--expect-nonunknown", "expect_nonunknown", default="",
              help="CSV of signal names that must be present and not UNKNOWN.")
@click.option("--expect-unknown-families", "expect_unknown_families", default="",
              help="CSV of families whose every present instance must be UNKNOWN.")
@click.option("--expect-absent-families", "expect_absent_families", default="",
              help="CSV of families that must be absent and listed in disabled_signals.")
@click.option("--expect-verdict", "expect_verdict", default=None,
              type=click.Choice(["GREEN", "YELLOW", "RED", "UNKNOWN"]),
              help="Required assessment verdict.")
@click.option("--require-coherent-rollup", "require_coherent_rollup", is_flag=True,
              default=False, help="Recompute the rollup from signals[] and require consistency.")
def cli(pred_dir, prefix, after_ms, wait_sec, eval_file, expect_state,
        expect_nonunknown, expect_unknown_families, expect_absent_families,
        expect_verdict, require_coherent_rollup):
    nonunknown = [s.strip() for s in expect_nonunknown.split(",") if s.strip()]
    unknown_fams = [s.strip() for s in expect_unknown_families.split(",") if s.strip()]
    absent_fams = [s.strip() for s in expect_absent_families.split(",") if s.strip()]

    deadline = time.time() + wait_sec
    last_fails = ["no assessment with tick_ts > --after-ms found yet"]
    last_ts = None
    while True:
        found = latest_assessment(pred_dir, prefix, after_ms)
        if found is not None:
            last_ts, assessment = found
            last_fails = run_checks(assessment, eval_file, expect_state, nonunknown,
                                    unknown_fams, absent_fams, expect_verdict,
                                    require_coherent_rollup)
            if not last_fails:
                tick = dt.datetime.fromtimestamp(last_ts / 1000.0).isoformat()
                click.echo(f"CHECK tick_ts={last_ts} ({tick}) verdict="
                           f"{assessment.get('verdict')} signals="
                           f"{len(assessment.get('signals', []))} verdict=PASS")
                sys.exit(0)
        if time.time() >= deadline:
            break
        time.sleep(POLL_SEC)

    click.echo(f"CHECK verdict=FAIL (tick_ts={last_ts}) after {wait_sec}s:")
    for f in last_fails:
        click.echo(f"  - {f}")
    sys.exit(1)


if __name__ == "__main__":
    cli()
