# Retrain trigger (Week 4 + FCVAE F4)

Host-side, separate from the quality agent by design (the 6-week plan's Week 4 decision;
Week 5's agent recommendation becomes a further reason to retrain through this same path).
The trigger reads a quality agent's health assessments (ops gate + the cumulative
`events_seen` the ML signals carry + F4's model-quality signal states) and the published
`model.manifest.json` (durable record of the last retrain), and fires the trainer wrapper:
`run_trainer.sh` (the Week 3 taxi `mqa-trainer` container) or `run_fcvae_trainer.sh`
(the F4 `mqa-fcvae-trainer` container, one FCVAE combo per run). It writes only under
`striim/retrain/state/` (gitignored), never into the directories the scorer OPs watch.

## Usage

One-shot (cron-friendly; exits 0 only on a fire):

    python3 striim/retrain/retrain_trigger.py check --interval-sec 86400 --min-new-events 10000

Loop mode for demos:

    python3 striim/retrain/retrain_trigger.py watch --poll-sec 60 --min-new-events 1000

Example crontab line (hourly check, daily schedule + row-count):

    0 * * * * cd <repo> && python3 striim/retrain/retrain_trigger.py check \
        --interval-sec 86400 --min-new-events 10000 >> striim/retrain/state/cron.out 2>&1

Manual training runs go through `run_trainer.sh` (env-overridable image/cpus/data/out). Do
not run it by hand while a `watch` loop is live: the files cannot tear (publish writes
atomically) but the trigger's recorded `model_version_after` could then describe your manual
run's model.

## Gate order and exit codes

Evaluation: load state -> single-flight lock -> assessment exists and is fresh
(`--max-assessment-age-sec`, default 120) -> ops gate (`ops_healthy` true AND breaker
closed; retraining is never authorized against an unhealthy pipeline) -> cooldown
(`--cooldown-sec`, default 3600; applies after failures too, so a broken trainer cannot
hot-loop) -> conditions -> fire.

Conditions (fire if any): SCHEDULE due when `now - last_retrain >= --interval-sec`, where
last_retrain is the manifest's `created_utc` (falls back to state, then bootstrap = due).
DATA due when `events_seen - baseline >= --min-new-events`; the baseline is set on first
sight and REBASELINED whenever the counter goes backwards (an OP redeploy resets it), with
no data-fire allowed on the rebaseline tick, so a counter reset can only delay a fire, never
cause one. METRIC (F4) due when the `--metric-signal` (an EXACT signal name from the FCVAE
monitor's assessments, e.g. `model_f1[Penny_All]`) reads one of the `--metric-fire-on`
states (default `WARN,FAIL`); an UNKNOWN metric (no labels dropped, stale eval file) NEVER
fires, and an ABSENT signal refuses loudly (exit 2, misconfiguration).

F4 ops-gate nuance: a model-quality FAIL drives the FCVAE monitor's verdict RED by design,
which is precisely when the metric condition must fire. So with `--metric-signal` set, the
ops gate is recomputed over the PLATFORM signals only (everything except the
model_precision/model_recall/model_f1/anomaly_rate families): a platform FAIL still refuses;
model-quality failures are the condition, not the gate. Without `--metric-signal` the gate
is byte-identical to Week 4.

When the metric condition fires, the degraded combo parsed from the signal name is exported
to the trainer as `MQA_FCVAE_MODEL`, so `run_fcvae_trainer.sh` retrains exactly the degraded
model (an explicit `MQA_FCVAE_MODEL` in the environment wins). Example FCVAE invocation:

    python3 striim/retrain/retrain_trigger.py check \
        --prefix fcvae_health_assessments \
        --manifest /opt/Striim/fcvae-models/Penny_All/model.manifest.json \
        --metric-signal 'model_f1[Penny_All]' \
        --trainer striim/retrain/run_fcvae_trainer.sh --trainer-timeout-sec 2400 \
        --state striim/retrain/state/fcvae_trigger_state.json \
        --log striim/retrain/state/fcvae_trigger_log.jsonl \
        --lock striim/retrain/state/fcvae_trigger.lock

(Set `MQA_TRAINER_NAME=mqa-fcvae-trainer-run` in the trigger's environment so its timeout
kill targets the fcvae container, not the taxi one.)

Exit codes: 0 fired (or `--dry-run` would-fire); 2 refusal (no/stale assessment, ops
unhealthy, events_seen unavailable, metric signal absent); 3 not due (cooldown, conditions
unmet incl. UNKNOWN metric, rebaseline); 4 lock held; 5 trainer failed/timed out; 6 trainer
exited 0 but the manifest is unreadable.

`run_fcvae_trainer.sh` failure modes are distinctly assertable: exit 7 = docker daemon
unreachable (Docker Desktop stopped), exit 8 = image missing (build it:
`cd python && docker build -f Dockerfile.fcvae -t mqa-fcvae-trainer .`), exit 2 = source CSV
missing, exit 3 = container succeeded but the manifest is unreadable.

Every evaluation appends one record to the audit log (outcome, reason, gates snapshot,
trainer exit/duration, model versions before/after). The `reason` vocabulary is
`schedule | new_events | metric_degradation` ('+'-joined when several are due); Week 5 adds
`requested`.

## Acceptance runbook (Week 4 bar)

Bar: "A trigger puts a fresh model live within one swap cycle, the scored output changes,
and a rollback returns to the previous model." Prereqs: FareInference + in-app agent
deployed and fed, `docker build -t mqa-trainer python/` done, feeds flowing.

Conventions: `HD=/opt/Striim/UploadedFiles`, `S=$(mktemp -d)`, repo root as cwd. Generate
ONE feed and copy the same bytes under fresh names per feed (FileReader tracks names; the
snapshot matcher needs identical content):

    python/.venv/bin/python striim/pipeline/make_trip_feed.py --rows 200 --out $S/feed.csv

Phase A, the positive loop:

    # A1 seed a known-different live model (seeded retraining on unchanged data can be
    #    byte-identical, which ModelOp correctly skips; the v2 fixture predicts in the
    #    thousands, unmistakable)
    cp striim/model-op/test/artifacts/swap_good_v2.onnx $HD/.model.onnx.tmp
    mv $HD/.model.onnx.tmp $HD/model.onnx
    cp $S/feed.csv $HD/pipeline_trips_rt1.csv    # wait ~30 s for scoring
    python3 striim/pipeline/check_swap_predictions.py snapshot --feed $S/feed.csv --out $S/base.json

    # A2 first check sets the events baseline: EXPECT exit 3, no trainer run
    python3 striim/retrain/retrain_trigger.py check --state $S/state.json --log $S/log.jsonl \
        --lock $S/lock --min-new-events 100 --cooldown-sec 0

    # A3 flow new data, then the same check FIRES: EXPECT exit 0, container runs (minutes)
    cp $S/feed.csv $HD/pipeline_trips_rt2.csv    # wait ~2 agent ticks (60 s)
    python3 striim/retrain/retrain_trigger.py check --state $S/state.json --log $S/log.jsonl \
        --lock $S/lock --min-new-events 100 --cooldown-sec 0

    # A4 swap proof: EXPECT compare PASS (different)
    cp $S/feed.csv $HD/pipeline_trips_rt3.csv    # wait ~30 s
    python3 striim/pipeline/check_swap_predictions.py snapshot --feed $S/feed.csv --out $S/new.json
    python3 striim/pipeline/check_swap_predictions.py compare $S/base.json $S/new.json --expect different

    # A5 rollback proof: EXPECT compare PASS (identical to the pre-retrain baseline)
    printf 'rollback\n' > $HD/model.control      # wait ~30 s
    cp $S/feed.csv $HD/pipeline_trips_rt4.csv    # wait ~30 s
    python3 striim/pipeline/check_swap_predictions.py snapshot --feed $S/feed.csv --out $S/back.json
    python3 striim/pipeline/check_swap_predictions.py compare $S/base.json $S/back.json --expect identical
    rm $HD/model.control
    # re-promote the trained bytes later with: touch $HD/model.onnx
    # (ModelOp's mtime gate precedes its sha compare)

Phase B, negative gates: all use `--dry-run` plus crafted assessment fixtures in scratch
dirs, so even a gate-order bug cannot launch docker. Each must exit nonzero (or 0 only for
an explicit would-fire) with no `fired_*` record in the log: breaker-open fixture (exit 2),
stale assessment with `--max-assessment-age-sec 1` (exit 2), empty health dir (exit 2),
lock held by a live pid (exit 4), cooldown active (exit 3), condition unmet (exit 3),
rebaseline after a counter reset (exit 3, outcome `rebaselined`, and the follow-up check
does not fire).
