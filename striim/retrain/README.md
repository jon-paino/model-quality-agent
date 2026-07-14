# Retrain trigger (Week 4)

Host-side, separate from the quality agent by design (the 6-week plan's Week 4 decision;
Week 5's agent recommendation becomes a second reason to retrain through this same path).
The trigger reads the in-app agent's health assessments (ops gate + the cumulative
`events_seen` the ML signals carry) and the published `model.manifest.json` (durable record
of the last retrain), and fires `run_trainer.sh`, the canonical one-shot run of the Week 3
`mqa-trainer` container. It writes only under `striim/retrain/state/` (gitignored), never
into the directory ModelOp watches.

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

Conditions (fire if either): SCHEDULE due when `now - last_retrain >= --interval-sec`, where
last_retrain is the manifest's `created_utc` (falls back to state, then bootstrap = due).
DATA due when `events_seen - baseline >= --min-new-events`; the baseline is set on first
sight and REBASELINED whenever the counter goes backwards (an OP redeploy resets it), with
no data-fire allowed on the rebaseline tick, so a counter reset can only delay a fire, never
cause one.

Exit codes: 0 fired (or `--dry-run` would-fire); 2 refusal (no/stale assessment, ops
unhealthy, events_seen unavailable); 3 not due (cooldown, conditions unmet, rebaseline);
4 lock held; 5 trainer failed/timed out; 6 trainer exited 0 but the manifest is unreadable.

Every evaluation appends one record to `state/retrain_trigger_log.jsonl` (outcome, reason,
gates snapshot, trainer exit/duration, model versions before/after). The `reason` field is
the Week 5 seam.

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
