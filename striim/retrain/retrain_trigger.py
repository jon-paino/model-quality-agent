#!/usr/bin/env python3
"""Week 4 retrain trigger: schedule, enough-new-data, or (F4) metric
degradation, closing the loop.

Host-side and separate from the quality agent by design (the 6-week plan keeps
the Week 4 trigger out of the agent; Week 5's agent recommendation becomes
simply another reason to retrain through this same path). The trigger senses
through files the pipeline already writes and owns:

  READS  <health-dir>/<prefix>*.json  a quality agent's per-tick assessments
         (ops gate: ops_healthy + breaker; data condition: the cumulative
         events_seen the ML signals carry; F4 metric condition: the state of
         one model-quality signal like model_f1[Penny_All])
  READS  model.manifest.json          the durable record of the last publish
         (schedule condition reference; survives restarts)
  RUNS   run_trainer.sh               the one-shot trainer container wrapper
         (taxi; run_fcvae_trainer.sh for the FCVAE combos)
  WRITES its own state/lock/audit log under striim/retrain/state/ ONLY
         (never into the directory ModelOp and the FileReader watch)

Gate order per evaluation: load state -> single-flight lock -> assessment
exists and is fresh -> ops gate (never train against an unhealthy pipeline) ->
cooldown -> conditions (schedule OR new-events OR metric_degradation) -> fire.

F4 ops-gate nuance: a model-quality FAIL drives the FCVAE monitor's verdict
RED (ops_healthy false, breaker open) BY DESIGN, and that is precisely the
moment the metric condition must fire. So when --metric-signal is set, the
ops gate is recomputed over the PLATFORM signals only (everything except the
model_precision/model_recall/model_f1/anomaly_rate families): a platform FAIL
(app down, source stale, node sick) still refuses, while model-quality
failures are the condition, not the gate. Without --metric-signal the gate is
byte-identical to the Week 4 behavior.

Exit codes (check mode; each refusal is distinctly assertable):
  0  fired and the trainer succeeded (or --dry-run reached a would-fire)
  2  refusal: no/stale assessment, ops unhealthy or breaker open,
     events_seen unavailable while the data condition is enabled, or the
     metric signal absent while the metric condition is enabled
  3  not due: cooldown active, no condition met (an UNKNOWN metric signal
     NEVER fires), or the rebaseline tick
  4  lock held by a live process
  5  trainer exited nonzero or timed out
  6  post-fire anomaly (trainer exited 0 but the manifest is unreadable)

Usage:
  retrain_trigger.py check [opts]              one-shot (cron-friendly)
  retrain_trigger.py watch --poll-sec N [opts] the same evaluation in a loop

The audit log (one JSONL record per evaluation) carries a `reason` field
(schedule | new_events | metric_degradation, '+'-joined when several are due);
Week 5 adds `requested` as a further condition source. No approval logic
lives here.

When the metric condition fires, the degraded combo parsed from the signal
name (model_f1[Penny_All] -> Penny_All) is exported to the trainer as
MQA_FCVAE_MODEL, so run_fcvae_trainer.sh retrains exactly the degraded model.
"""
from __future__ import annotations

import argparse
import json
import glob
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
STATE_DIR = SCRIPT_DIR / "state"
SCHEMA_VERSION = 1
STALE_LOCK_SEC = 60


# ---------------------------------------------------------------------------
# assessment reading (tolerant open-array decoder, shared house pattern)
# ---------------------------------------------------------------------------

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


def tick_of(rec: dict) -> int:
    try:
        return int(str(rec.get("tick_ts", "0")))
    except (TypeError, ValueError):
        return 0


def latest_assessment(health_dir: str, prefix: str):
    best = None
    for path in sorted(glob.glob(os.path.join(health_dir, prefix + "*.json"))):
        try:
            with open(path) as f:
                text = f.read()
        except OSError:
            continue   # pruned between glob and open; skip, do not die
        for rec in iter_records(text):
            if best is None or tick_of(rec) >= tick_of(best):
                best = rec
    if best is None:
        return None
    raw = best.get("assessment_json") or best.get("assessment")
    return json.loads(raw) if isinstance(raw, str) else raw


def matches(name: str, token: str) -> bool:
    """The signal-family token rule shared with the acceptance harnesses."""
    n, t = name.lower(), token.strip().lower()
    return n == t or n.startswith(t + "[") or n.startswith(t + "_")


# F4: the model-quality signal families the FCVAE monitor emits. They are the
# metric CONDITION's vocabulary and are excluded from the platform-only ops
# gate (see the module docstring).
METRIC_FAMILIES = ("model_precision", "model_recall", "model_f1", "anomaly_rate")


def is_metric_signal(name: str) -> bool:
    return name.split("[", 1)[0] in METRIC_FAMILIES


def find_signal(assessment: dict, name: str):
    """The signal dict with this EXACT name, or None."""
    for s in assessment.get("signals", []):
        if s.get("name") == name:
            return s
    return None


def combo_of(name: str):
    """model_f1[Penny_All] -> Penny_All; None for un-bracketed names."""
    if "[" in name and name.endswith("]"):
        return name.split("[", 1)[1][:-1]
    return None


def events_seen_of(assessment: dict, signal_prefs: list) -> tuple:
    """Return (signal_name, events_seen) from the first preference token with
    a matching signal carrying events_seen; (None, None) if none do."""
    signals = assessment.get("signals", [])
    for token in signal_prefs:
        best = None
        for s in signals:
            if matches(s.get("name", ""), token) and s.get("events_seen") is not None:
                v = int(s["events_seen"])
                if best is None or v > best:
                    best = v
        if best is not None:
            return token, best
    return None, None


# ---------------------------------------------------------------------------
# state / manifest / audit log
# ---------------------------------------------------------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_utc(iso: str):
    try:
        return datetime.fromisoformat(iso).timestamp()
    except (TypeError, ValueError):
        return None


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] state file corrupt ({e}); treating as fresh")
        return {"_corrupt": True}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {k: v for k, v in state.items() if not k.startswith("_")}
    state["schema_version"] = SCHEMA_VERSION
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(state, indent=2) + "\n")
    os.replace(tmp, path)


def read_manifest(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def append_log(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# single-flight lock
# ---------------------------------------------------------------------------

def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def acquire_lock(path: Path):
    """Return an acquired-lock token (the path) or None if held. Breaks a lock
    only when its recorded pid is dead AND the file is older than
    STALE_LOCK_SEC (guards a just-created lock not yet written). The break is
    an atomic RENAME-steal: exactly one breaker wins the rename; a loser gets
    FileNotFoundError and backs off, so two processes can never both acquire."""
    path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in (1, 2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w") as f:
                f.write(json.dumps({"pid": os.getpid(), "acquired_utc": now_utc_iso()}))
            return path
        except FileExistsError:
            try:
                holder = json.loads(path.read_text())
                holder_pid = int(holder.get("pid", -1))
            except (OSError, json.JSONDecodeError, ValueError):
                holder_pid = -1
            try:
                age = time.time() - path.stat().st_mtime
            except OSError:
                age = 0.0
            if attempt == 1 and not pid_alive(holder_pid) and age > STALE_LOCK_SEC:
                stale = path.with_name(f"{path.name}.stale.{os.getpid()}")
                try:
                    os.rename(path, stale)   # atomic steal: one winner
                except OSError:
                    return None              # someone else stole or re-acquired first
                print(f"[WARN] broke stale lock (pid {holder_pid} dead, {age:.0f}s old)")
                stale.unlink(missing_ok=True)
                continue
            return None
    return None


def release_lock(path: Path) -> None:
    """Unlink the lock only if this process still owns it (a stale-lock steal
    by another process must not be clobbered)."""
    try:
        holder = json.loads(path.read_text())
        if int(holder.get("pid", -1)) == os.getpid():
            path.unlink()
    except (OSError, json.JSONDecodeError, ValueError):
        pass


# ---------------------------------------------------------------------------
# one evaluation
# ---------------------------------------------------------------------------

def evaluate(args, mode: str) -> int:
    lock = acquire_lock(args.lock)
    if lock is None:
        print(f"[FAIL] lock held ({args.lock}); another evaluation or training run owns it")
        return 4

    try:
        return _evaluate_locked(args, mode)
    finally:
        release_lock(lock)


def _evaluate_locked(args, mode: str) -> int:
    # State is read INSIDE the lock so a concurrent evaluation's writes are
    # never overwritten from a stale load.
    state = load_state(args.state)
    now_ms = int(time.time() * 1000)
    gates = {"min_new_events": args.min_new_events, "interval_sec": args.interval_sec}

    def refuse(outcome: str, msg: str, code: int) -> int:
        print(f"[FAIL] {msg}")
        append_log(args.log, {"ts": now_utc_iso(), "mode": mode, "outcome": outcome,
                             "reason": None, "gates": gates})
        return code

    # Gate: assessment presence + freshness.
    a = latest_assessment(args.health_dir, args.prefix)
    if a is None:
        return refuse("refused_no_assessment",
                      f"no assessments under {args.health_dir}/{args.prefix}*", 2)
    tick_ts = int(a.get("tick_ts", 0))
    age_sec = (now_ms - tick_ts) / 1000.0
    gates.update({"assessment_tick_ts": tick_ts, "assessment_age_sec": round(age_sec, 1)})
    if age_sec > args.max_assessment_age_sec:
        return refuse("refused_stale_assessment",
                      f"latest assessment is {age_sec:.0f}s old "
                      f"(> {args.max_assessment_age_sec}s); pipeline health unknown", 2)
    print(f"[PASS] assessment fresh ({age_sec:.0f}s old, verdict={a.get('verdict')})")

    # Gate: ops (never train against an unhealthy pipeline).
    ops_healthy = a.get("ops_healthy")
    breaker_open = a.get("ops_circuit_breaker_open")
    gates.update({"ops_healthy": ops_healthy, "breaker_open": breaker_open})
    metric_mode = bool(args.metric_signal)
    if metric_mode:
        # F4 platform-only recompute: a model-quality FAIL is the metric
        # CONDITION (verdict RED by design), so only the platform signals may
        # refuse here. The assessment's own ops booleans still ride in gates
        # for the audit trail.
        platform_fail = [str(s.get("name")) for s in a.get("signals", [])
                         if not is_metric_signal(str(s.get("name", "")))
                         and s.get("state") == "FAIL"]
        gates.update({"ops_gate": "platform_only",
                      "ops_platform_fail_signals": platform_fail})
        if platform_fail:
            return refuse("refused_ops",
                          "ops gate (platform-only): FAILing platform signal(s) "
                          f"{platform_fail}; retraining is not authorized over a "
                          "sick pipeline", 2)
        print("[PASS] ops gate (platform-only: no platform signal FAILing)")
    else:
        if ops_healthy is not True or breaker_open is not False:
            return refuse("refused_ops",
                          f"ops gate: ops_healthy={ops_healthy} breaker_open={breaker_open}; "
                          "retraining is not authorized while ops are unhealthy", 2)
        print("[PASS] ops gate (healthy, breaker closed)")

    # Gate: cooldown (applies to every real fire, success or failure).
    last_fire = parse_utc(state.get("last_fire_utc", ""))
    cooldown_ok = last_fire is None or (time.time() - last_fire) >= args.cooldown_sec
    gates["cooldown_ok"] = cooldown_ok
    if not cooldown_ok:
        remaining = args.cooldown_sec - (time.time() - last_fire)
        print(f"[FAIL] cooldown active ({remaining:.0f}s remaining)")
        append_log(args.log, {"ts": now_utc_iso(), "mode": mode, "outcome": "not_due",
                              "reason": None, "gates": gates})
        return 3

    manifest = read_manifest(args.manifest)

    # Condition: schedule.
    schedule_due = False
    if args.interval_sec > 0:
        ref = parse_utc(manifest.get("created_utc", "")) \
            or parse_utc(state.get("last_success_utc", ""))
        gates["last_retrain_utc"] = manifest.get("created_utc") or state.get("last_success_utc")
        # Bootstrap: no manifest and no history means nothing was ever
        # published by this loop, so a scheduled trigger is due.
        schedule_due = ref is None or (time.time() - ref) >= args.interval_sec
    gates["schedule_due"] = schedule_due

    # Condition: new events (with the rebaseline rule: a counter reset can
    # only delay a data-fire, never cause one).
    data_due = False
    rebaselined = False
    if args.min_new_events > 0:
        signal, events = events_seen_of(a, args.signal)
        gates.update({"signal": signal, "events_seen": events})
        if events is None:
            return refuse("refused_no_events_signal",
                          "data condition enabled but no signal carries events_seen "
                          f"(prefs={args.signal}); is --prefix pointing at the in-app "
                          "agent's assessments?", 2)
        baseline = state.get("events_baseline")
        gates["events_baseline"] = baseline
        if baseline is None or events < int(baseline):
            rebaselined = baseline is not None
            state.update({"signal": signal, "events_baseline": events,
                          "baseline_set_utc": now_utc_iso()})
            if not args.dry_run:
                save_state(args.state, state)
            print(f"[INFO] events baseline {'rebaselined' if rebaselined else 'set'} "
                  f"to {events} (no data-fire on this tick)")
        else:
            delta = events - int(baseline)
            data_due = delta >= args.min_new_events
            print(f"[{'PASS' if data_due else 'FAIL'}] new events: {delta} "
                  f"(threshold {args.min_new_events})")
    gates["data_due"] = data_due

    # Condition (F4): metric degradation. Fires when the chosen model-quality
    # signal reads one of the --metric-fire-on states; an UNKNOWN metric
    # (missing labels, stale eval file) NEVER fires. When the whole eval FILE
    # is unavailable the monitor collapses the per-combo instances into ONE
    # base-name UNKNOWN signal (model_f1, not model_f1[Penny_All]), so an
    # absent exact name falls back to the base family signal; only when
    # NEITHER exists is it a configuration error that refuses loudly.
    metric_due = False
    if metric_mode:
        sig = find_signal(a, args.metric_signal)
        if sig is None:
            base = args.metric_signal.split("[", 1)[0]
            sig = find_signal(a, base)
            if sig is not None:
                print(f"[INFO] '{args.metric_signal}' absent; using the family-level "
                      f"signal '{base}' (file-level state)")
        if sig is None:
            return refuse("refused_no_metric_signal",
                          f"metric condition enabled but signal '{args.metric_signal}' "
                          "is absent from the assessment (is the FCVAE monitor deployed "
                          "with the metric families enabled, and --prefix pointing at "
                          "its sink?)", 2)
        metric_state = str(sig.get("state", "UNKNOWN")).upper()
        gates.update({"metric_signal": args.metric_signal,
                      "metric_state": metric_state,
                      "metric_value": sig.get("value")})
        if metric_state == "UNKNOWN":
            print(f"[FAIL] metric condition: {args.metric_signal} is UNKNOWN "
                  f"({sig.get('detail')}); UNKNOWN never fires")
        else:
            metric_due = metric_state in args.metric_fire_on
            print(f"[{'PASS' if metric_due else 'FAIL'}] metric condition: "
                  f"{args.metric_signal}={metric_state} "
                  f"(fires on {','.join(sorted(args.metric_fire_on))})")
    gates["metric_due"] = metric_due

    if not schedule_due and not data_due and not metric_due:
        outcome = "rebaselined" if rebaselined else "not_due"
        print("[not due] no condition met")
        append_log(args.log, {"ts": now_utc_iso(), "mode": mode, "outcome": outcome,
                              "reason": None, "gates": gates})
        return 3

    reason = "+".join([r for r, due in (("schedule", schedule_due),
                                        ("new_events", data_due),
                                        ("metric_degradation", metric_due)) if due])
    version_before = manifest.get("model_version")
    created_before = manifest.get("created_utc")

    if args.dry_run:
        print(f"WOULD FIRE reason={reason} (dry run; no state written, no trainer run)")
        append_log(args.log, {"ts": now_utc_iso(), "mode": mode,
                              "outcome": "dry_run_would_fire", "reason": reason,
                              "gates": gates, "model_version_before": version_before})
        return 0

    # Fire. Phase 1: make the cooldown durable BEFORE the trainer launches.
    events_at_fire = gates.get("events_seen")
    state.update({"last_fire_utc": now_utc_iso(), "last_exit_code": None,
                  "last_reason": reason, "events_seen_at_fire": events_at_fire,
                  "model_version_before": version_before})
    save_state(args.state, state)

    print(f"FIRED reason={reason}; running {args.trainer}")
    # F4: hand the degraded combo to the trainer wrapper so exactly the
    # degraded model retrains (run_fcvae_trainer.sh reads MQA_FCVAE_MODEL).
    # An explicit MQA_FCVAE_MODEL in the environment wins over the derived
    # one; taxi invocations (no metric fire) inherit the environment as-is.
    trainer_env = None
    if metric_due:
        degraded_combo = combo_of(args.metric_signal)
        if degraded_combo and "MQA_FCVAE_MODEL" not in os.environ:
            trainer_env = dict(os.environ, MQA_FCVAE_MODEL=degraded_combo)
            print(f"[INFO] degraded combo {degraded_combo} -> MQA_FCVAE_MODEL")
    t0 = time.time()
    try:
        proc = subprocess.run([str(args.trainer)], timeout=args.trainer_timeout_sec,
                              env=trainer_env)
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        exit_code = -1
        print(f"[FAIL] trainer timed out after {args.trainer_timeout_sec}s")
        # The timeout killed only the wrapper; the container runs under dockerd
        # and would otherwise finish LATER and publish a model the audit trail
        # recorded as failed. Kill it by its deterministic name (best effort).
        name = os.environ.get("MQA_TRAINER_NAME", "mqa-trainer-run")
        docker = os.environ.get("MQA_DOCKER", "docker")
        try:
            subprocess.run([docker, "kill", name], timeout=30,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"[INFO] killed container {name}")
        except (subprocess.TimeoutExpired, OSError):
            print(f"[WARN] could not kill container {name}; it may still publish")
    except OSError as e:
        # Missing/non-executable trainer must flow into the failure outcome
        # (state + audit + exit 5), not an uncaught traceback that skips the
        # log and, in watch mode, kills the loop.
        exit_code = -1
        print(f"[FAIL] trainer could not be launched: {e}")
    duration = round(time.time() - t0, 1)

    after = read_manifest(args.manifest)
    version_after = after.get("model_version")
    created_after = after.get("created_utc")
    if exit_code == 0 and (version_after is None or created_after == created_before):
        # publish writes a fresh created_utc on EVERY publish, identical bytes
        # included; an unchanged stamp means nothing was published where we
        # are looking (wrong --manifest / wrong output volume).
        outcome, rc = "fired_no_manifest", 6
        print("[FAIL] trainer exited 0 but no fresh manifest appeared at "
              f"{args.manifest} (broken publish contract or wrong output path)")
    elif exit_code == 0 and version_after == version_before:
        outcome, rc = "fired_identical", 0
        print(f"[PASS] retrained, but bytes identical to {version_before} "
              "(seeded training on unchanged data; ModelOp skips by sha)")
    elif exit_code == 0:
        outcome, rc = "fired_published", 0
        print(f"[PASS] published {version_after} (was {version_before}); "
              "ModelOp swaps within one batch")
    else:
        outcome, rc = "fired_trainer_failed", 5
        print(f"[FAIL] trainer exited {exit_code}")

    # Phase 2: outcome write. On success the data window resets; on failure
    # the baseline is kept so the retry after cooldown does not need fresh
    # traffic. A persistence failure here is a post-fire anomaly (exit 6),
    # never an uncaught traceback: the fire already happened.
    try:
        state.update({"last_exit_code": exit_code, "model_version_after": version_after})
        if rc == 0:
            state["last_success_utc"] = now_utc_iso()
            if events_at_fire is not None:
                state["events_baseline"] = events_at_fire
                state["baseline_set_utc"] = now_utc_iso()
        save_state(args.state, state)
        append_log(args.log, {"ts": now_utc_iso(), "mode": mode, "outcome": outcome,
                              "reason": reason, "gates": gates,
                              "trainer": {"exit_code": exit_code, "duration_sec": duration},
                              "model_version_before": version_before,
                              "model_version_after": version_after})
    except OSError as e:
        print(f"[FAIL] post-fire state/log write failed: {e}")
        return 6
    return rc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def add_shared_options(p: argparse.ArgumentParser) -> None:
    p.add_argument("--health-dir", default="/opt/Striim/UploadedFiles")
    p.add_argument("--prefix", default="health_assessments",
                   help="the IN-APP agent's sink (the standalone monitor has ML "
                        "signals off and carries no events_seen)")
    p.add_argument("--manifest", type=Path,
                   default=Path("/opt/Striim/UploadedFiles/model.manifest.json"))
    p.add_argument("--state", type=Path, default=STATE_DIR / "retrain_trigger_state.json")
    p.add_argument("--log", type=Path, default=STATE_DIR / "retrain_trigger_log.jsonl")
    p.add_argument("--lock", type=Path, default=STATE_DIR / "retrain_trigger.lock")
    p.add_argument("--interval-sec", type=int, default=0,
                   help="schedule condition; 0 disables")
    p.add_argument("--min-new-events", type=int, default=0,
                   help="new-events condition; 0 disables")
    p.add_argument("--max-assessment-age-sec", type=int, default=120)
    p.add_argument("--cooldown-sec", type=int, default=3600)
    p.add_argument("--trainer", type=Path, default=SCRIPT_DIR / "run_trainer.sh")
    p.add_argument("--trainer-timeout-sec", type=int, default=3600)
    p.add_argument("--signal", default="nan_score_rate,feature_miss_rate",
                   help="ordered preference of signals whose events_seen measures "
                        "new data (nan_score_rate counts rows that reached ModelOp)")
    p.add_argument("--metric-signal", default="",
                   help="F4 metric_degradation condition: the EXACT model-quality "
                        "signal name from the FCVAE monitor's assessments (e.g. "
                        "'model_f1[Penny_All]'); empty disables the condition and "
                        "keeps the Week 4 behavior byte-identical")
    p.add_argument("--metric-fire-on", default="WARN,FAIL",
                   help="CSV of signal states that satisfy the metric condition "
                        "(UNKNOWN never fires regardless)")
    p.add_argument("--dry-run", action="store_true",
                   help="evaluate all gates but write no state and run no trainer")


def main() -> int:
    ap = argparse.ArgumentParser(description="Week 4 retrain trigger")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="one-shot evaluation (cron-friendly)")
    add_shared_options(c)
    w = sub.add_parser("watch", help="loop the evaluation")
    add_shared_options(w)
    w.add_argument("--poll-sec", type=int, default=60)
    args = ap.parse_args()
    args.signal = [t.strip() for t in args.signal.split(",") if t.strip()]
    args.metric_signal = args.metric_signal.strip()
    args.metric_fire_on = {t.strip().upper() for t in args.metric_fire_on.split(",")
                           if t.strip()}
    # UNKNOWN can never be a fire state: missing/stale eval data must not
    # trigger training no matter how the option is spelled.
    args.metric_fire_on.discard("UNKNOWN")

    if args.cmd == "check":
        return evaluate(args, "check")
    while True:
        # One bad evaluation (transient fs error, pruned assessment file mid
        # read) must not kill the loop; check mode still tracebacks loudly.
        try:
            rc = evaluate(args, "watch")
        except Exception as e:
            rc = 1
            print(f"[WARN] evaluation crashed ({e.__class__.__name__}: {e}); "
                  "loop continues")
        print(f"[watch] evaluation done (code {rc}); next in {args.poll_sec}s")
        time.sleep(args.poll_sec)


if __name__ == "__main__":
    sys.exit(main())
