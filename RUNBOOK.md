# RUNBOOK: running the model-quality-agent on your own Striim instance

This walks a fresh machine from zero to both live demos (the NYC-taxi pipeline and the
Fiserv-style FCVAE anomaly pipeline), explains WHAT HAPPENS at every step, and ends with the
scripted acceptance suites that prove each capability. Companion doc: [ARCHITECTURE.md](ARCHITECTURE.md)
explains how every component works and how they relate; this file is the ordered instruction set.

Everything below was verified live on Striim 5.2.0.4 / macOS / OpenJDK 11. Newer Striim
releases mostly work the same; the known drift points are called out inline.

---

## 0. Prerequisites

| What | Why |
|---|---|
| Striim 5.2.x installed at `/opt/Striim` (`$STRIIM_HOME`), licensed, single node | everything deploys onto it; poms resolve Striim jars via `-DSTRIIM_HOME` (default `/opt/Striim`) |
| OpenJDK 11 + Maven | the Open Processors (OPs) are Java, built as shaded `.scm` modules |
| [uv](https://docs.astral.sh/uv/) + Python 3.12 | the `python/` project (training, export, publish, Feast, harness tooling) |
| Docker Desktop (running) | the one-shot trainer containers (`mqa-trainer`, `mqa-fcvae-trainer`) |
| The sibling repo `fcvae-anomaly-detection` checked out NEXT TO this repo | the FCVAE labeled source CSV (`data/synthetic_transactions.csv`, ~473 MB) and the prebuilt model checkpoints live there. EXTERNAL BY DESIGN: they are large model/data artifacts, not code of this repo. Override the location with `FCVAE_REPO` |
| (taxi only) the processed trips parquet | `model/data/processed/trips_cleaned.parquet`, also external; needed to train/publish the taxi model and build Feast-hitting feeds |

Ports: Striim web/REST `9080`; taxi Feast `6566`; FCVAE Feast `6567`.

Know these two identity facts about your cluster before anything else:

- **Cluster name** = `WAClusterName` in `$STRIIM_HOME/conf/startUp.properties` (NOT the
  `$USER`-derived name the interactive console prompt suggests). Every `console.sh` call
  below takes it as `-c <name>`.
- **Admin password**: the scripts never store it; they read the `STRIIM_ADMIN_PW`
  environment variable. Export it once per shell.

```bash
export STRIIM_ADMIN_PW='<your admin password>'
export STRIIM_CLUSTER='<your WAClusterName>'    # scripts default to the author's; override
```

## 1. One-time platform setup

1. **Start Striim with captured stdout** (OP diagnostics print there; the acceptance proofs
   never depend on it, but debugging does):

   ```bash
   nohup /opt/Striim/bin/server.sh > /tmp/striim_server.log 2>&1 &
   # ready when REST auth answers:
   curl -s -X POST http://localhost:9080/security/authenticate \
        --data-urlencode 'username=admin' --data-urlencode "password=$STRIIM_ADMIN_PW"
   ```

2. **Create the secrets vault** (the monitors auth to Striim's own REST API through a
   vault-resolved password; a vault cannot be named `vault`, a TQL reserved word). Do this
   over REST, not `console.sh -f` (the latter hangs on `CREATE VAULT`):

   ```bash
   TOK=$(curl -s -X POST 'http://localhost:9080/security/authenticate' \
         --data-urlencode 'username=admin' --data-urlencode "password=$STRIIM_ADMIN_PW" \
       | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
   t() { curl -s -X POST 'http://localhost:9080/api/v2/tungsten' \
          -H "authorization: STRIIM-TOKEN $TOK" -H 'content-type: text/plain' --data "$1"; echo; }
   t 'CREATE NAMESPACE qualitydemo;'
   t 'CREATE VAULT qualitydemo.secrets;'
   t "WRITE INTO qualitydemo.secrets ( vaultKey: \"striim_admin_pw\", vaultValue: \"$STRIIM_ADMIN_PW\" );"
   ```

   What happens: every monitor TQL references `[qualitydemo.secrets.striim_admin_pw]`;
   at deploy the agent resolves it via Striim's VaultAPI and uses it for its mon/REST calls.
   An unresolvable reference is NOT a deploy failure — the agent auths with an empty
   password and every mon/REST signal reads UNKNOWN (check `logs/striim.command.log` for
   "password is incorrect" spam if you see all-UNKNOWN platform signals).

3. **(FCVAE only) WAEUdf jar**: `WAEUdf-5.2.0.jar` must be in `/opt/Striim/lib` (the
   fcvae TQL uses its `createWAEvent` UDF). Restart Striim after adding jars to `lib/`.

4. **(taxi CDC variant only) MySQL JDBC driver**: `mysql-connector-java-8.0.30.jar` into
   `/opt/Striim/lib` + restart (see CLAUDE.md's MySQL section; the CSV variant needs none
   of this).

5. **(optional, self-managed only) JMX health beans**: the agent's default transport is
   mon/REST (SaaS-safe, no setup). Only if you want `HealthSource: 'JMX'` follow the
   two-edit procedure in CLAUDE.md ("Enabling JMX").

## 2. Python environment

```bash
cd python
uv sync --extra training   # taxi stack (XGBoost, sklearn<1.6, geohash)
# OR
uv sync --extra fcvae      # FCVAE stack (torch 2.11, sklearn>=1.6)
```

What happens: the two extras are DECLARED CONFLICTING (incompatible sklearn floors);
`uv sync --extra <one>` swaps the whole stack in place. Everything host-side that the
harnesses run works under either (numpy/click/feast/onnxruntime are base deps). Intel-Mac
note: `onnxruntime` is forked by platform marker so Intel Macs resolve `<1.24` (the last
release with Intel-mac wheels); Apple-Silicon and Linux resolve the latest. If `uv sync`
ever refuses on wheels, you are on an unusual platform — check the markers in
`python/pyproject.toml`.

House rule used everywhere below: run repo python via `uv` from `python/`:

```bash
py() { (cd python && uv run python "$@"); }
```

## 3. The taxi pipeline (Scenario A)

Which TQL is which (asked often):

| File | What it is |
|---|---|
| `striim/pipeline/inference_pipeline.tql` | THE taxi demo app (`qualitydemo.FareInference`): CSV FileReader -> FeatureOp (Feast enrich) -> ModelOp (ONNX score + hot-swap) -> JSON sink, plus the IN-APP quality agent |
| `striim/pipeline/inference_pipeline_mysql.tql` | same app on a MySQL CDC source (adds the `schema_evolution` DDL signal; needs `striim/mysql/` Docker setup) |
| `striim/quality-agent/quality_monitor.tql` | standalone cross-app monitor template (`qualitymon.QualityMonitorApp`): senses ANY app over mon/REST |

Bring-up order (each numbered step's effect in parentheses):

```bash
# 1 build + stage the three OP modules (creates UploadedFiles/<Op>.scm)
(cd striim/feature-op && ./build.sh)
(cd striim/model-op && ./build.sh)
(cd striim/quality-agent && ./build.sh)

# 2 load the modules (registers the OP templates in the Global namespace)
#   via the same REST t() helper as above:
t "LOAD OPEN PROCESSOR 'UploadedFiles/FeatureOp.scm';"
t "LOAD OPEN PROCESSOR 'UploadedFiles/ModelOp.scm';"
t "LOAD OPEN PROCESSOR 'UploadedFiles/ModelQualityAgent.scm';"

# 3 taxi Feast online store on 6566 (FeatureOp reads it per event)
#   needs the external parquet staged; see python/model/feature_repo
# 4 publish the taxi model (gated: signature + golden parity; writes
#   UploadedFiles/model.onnx + model.manifest.json ATOMICALLY, manifest first)
py -m model.publish run

# 5 deploy the app + the standalone monitor
/opt/Striim/bin/console.sh -c "$STRIIM_CLUSTER" -u admin -p "$STRIIM_ADMIN_PW" \
    -f striim/pipeline/inference_pipeline.tql          # note: append quit; if you edit it
/opt/Striim/bin/console.sh -c "$STRIIM_CLUSTER" -u admin -p "$STRIIM_ADMIN_PW" \
    -f striim/quality-agent/quality_monitor.tql

# 6 feed it (FileReader tracks names: every feed needs a FRESH filename)
py striim/pipeline/make_trip_feed.py --rows 200 --out /tmp/feed.csv
cp /tmp/feed.csv /opt/Striim/UploadedFiles/pipeline_trips_run1.csv
```

What happens end to end: FeatureOp looks each event's geohash up in Feast (misses DROP the
event: never score against silently-defaulted features); ModelOp assembles the 15-feature
vector, scores it through an in-JVM ONNX session, stamps the prediction into `data[]` (so
the JSON formatter can see it) and health counters into `userdata` (so the agent can); the
in-app agent ticks every 30 s, senses the app over mon/REST, and appends one assessment
record to `UploadedFiles/health_assessments.json`. ModelOp watches `model.onnx`'s mtime:
publishing a new model hot-swaps it within one batch, `model.control` containing `rollback`
reverts to the previous session (bit-exact, proven by
`striim/pipeline/check_swap_predictions.py snapshot/compare`).

Taxi retraining loop:

```bash
docker build -t mqa-trainer python/          # one-time image build
python3 striim/retrain/retrain_trigger.py check --interval-sec 86400 --min-new-events 10000
```

What happens: the trigger reads the newest assessment (gates: freshness, ops health +
breaker, cooldown, single-flight lock), then fires `run_trainer.sh` when the schedule or
new-events condition is due; the container trains, exports, and publishes through the same
gates; ModelOp swaps. Exit codes are the contract (0 fired, 2 refusal, 3 not due, 4 lock,
5 trainer failed, 6 post-fire anomaly). See `striim/retrain/README.md`.

## 4. The FCVAE pipeline

| File | What it is |
|---|---|
| `striim/pipeline/fcvae_inference.tql` | the FCVAE app (`fcvaedemo.FcvaeInference`): one CSV source fanning into two mirrored chains (`Penny_All` pooled sub-dollar series, `Accel_CMP` combo series), each = hourly window -> 24-row sliding window -> FCVAEParamsOp (Feast per-combo params) -> FCVAEOnnxScorer (NLL + decision + hot-swap) -> JSON sink. DO NOT deploy raw: use `deploy_fcvae.sh` |
| `striim/quality-agent/fcvae_monitor.tql` | the FCVAE monitor TEMPLATE (`fcvaemon.FcvaeMonitor`), `@TOKENS@` rendered by `deploy_fcvae_monitor.sh`. DO NOT deploy raw |

Bring-up:

```bash
# 1 the FCVAE Feast instance (per-combo scoring params, port 6567; the taxi
#   one on 6566 is never touched)
py -m fcvae.feast_setup apply
(cd python && nohup uv run python -m fcvae.feast_setup serve --port 6567 \
    > /tmp/fcvae_feast.log 2>&1 &)

# 2 export the prebuilt checkpoints from the sibling repo to ONNX artifacts
#   (parity-gated; ~1 min each) and publish them as the live models
py -m fcvae.onnx_export export --model Penny_All \
    --model-dir ../fcvae-anomaly-detection/models/fcvae/Penny_All \
    --out-dir fcvae/artifacts/Penny_All_prebuilt
py -m fcvae.onnx_export export --model Accel_CMP \
    --model-dir ../fcvae-anomaly-detection/models/fcvae/Accel_CMP \
    --out-dir fcvae/artifacts/Accel_CMP_prebuilt
py -m fcvae.publish run --model Penny_All  --artifacts fcvae/artifacts/Penny_All_prebuilt \
    --out /opt/Striim/fcvae-models/Penny_All --require-feast
py -m fcvae.publish run --model Accel_CMP --artifacts fcvae/artifacts/Accel_CMP_prebuilt \
    --out /opt/Striim/fcvae-models/Accel_CMP --require-feast

# 3 deploy the pipeline (idempotent: teardown, rebuild both scms, LOAD, TQL, poll RUNNING)
striim/pipeline/deploy_fcvae.sh

# 4 deploy the monitor. FIRST TIME (or after any agent-code change) use the
#   module-reload cycle, which briefly redeploys the taxi apps sharing the scm:
striim/pipeline/deploy_fcvae_monitor.sh --reload-module
#   ... later redeploys (changed thresholds/signals) are app-only:
FCVAE_MON_ENABLED_SIGNALS='...' striim/pipeline/deploy_fcvae_monitor.sh

# 5 feed it: 5-day slices of the labeled source, time-shifted so each feed is
#   new in event time but bit-identical per hour (the swap-proof protocol)
py striim/pipeline/make_fcvae_feed.py prepare --start-date 2025-02-25 --days 5 \
    --out striim/pipeline/feeds/penny_test_5d.csv        # TEST range: has real anomalies
py striim/pipeline/make_fcvae_feed.py emit --base striim/pipeline/feeds/penny_test_5d.csv \
    --shift-days 0 --out /tmp/fcvae_swap_test/penny_feed_s0.csv.tmp
mv /tmp/fcvae_swap_test/penny_feed_s0.csv.tmp /tmp/fcvae_swap_test/penny_feed_s0.csv

# 6 ground-truth eval: generate labels for the slice (in prod these come from
#   case dispositions), then join them against the scored output
py striim/pipeline/make_fcvae_labels.py emit --start-date 2025-02-25 --days 5 \
    --shift-days 0 --out /opt/Striim/UploadedFiles/ground_truth/fcvae_labels_s0.csv
py striim/pipeline/eval_labels.py run     # -> UploadedFiles/fcvae_eval/eval_metrics.json
```

What happens: the scorer flags a window when its last-point NLL score is BELOW the
threshold (lower NLL = more anomalous; decisions are inverted relative to intuition).
`eval_labels.py` joins labels to scored windows on `(combo_key, hour)`, computes raw and
point-adjusted precision/recall/F1 plus a labels-free anomaly rate, embeds each combo's
published manifest baseline, and writes `eval_metrics.json` atomically. The monitor turns
that file into per-combo signals (`model_f1[Penny_All]`...) judged RELATIVE to each model's
own accepted baseline; a stale or missing file reads UNKNOWN, never a false PASS.

Quality-triggered retraining (the F4 loop):

```bash
(cd python && docker build -f Dockerfile.fcvae -t mqa-fcvae-trainer .)   # one-time
MQA_TRAINER_NAME=mqa-fcvae-trainer-run \
python3 striim/retrain/retrain_trigger.py check \
    --prefix fcvae_health_assessments \
    --manifest /opt/Striim/fcvae-models/Penny_All/model.manifest.json \
    --metric-signal 'model_f1[Penny_All]' \
    --trainer striim/retrain/run_fcvae_trainer.sh --trainer-timeout-sec 2400 \
    --state striim/retrain/state/fcvae_trigger_state.json \
    --log striim/retrain/state/fcvae_trigger_log.jsonl \
    --lock striim/retrain/state/fcvae_trigger.lock
```

What happens: the trigger fires when `model_f1[Penny_All]` reads WARN/FAIL (an UNKNOWN
metric never fires); the ops gate is recomputed over PLATFORM signals only, since a
model-quality FAIL turns the verdict RED by design (that is the condition, not the gate);
the degraded combo is passed to the container as `MQA_FCVAE_MODEL`; the container trains
(minutes), exports, publishes through six gates; the scorer hot-swaps; the retrained
`model_version` automatically invalidates any stale Feast row via the skew guard. Docker
down reads as the wrapper's exit 7, missing image as exit 8, with actionable messages.

## 5. The proof suites (run these to trust your install)

Each harness is self-contained (fresh teardown, deterministic assertions, PASS/FAIL
summary, nonzero exit on any failure). Expected wall-clock on a laptop in parentheses.

| Harness | Proves | Time |
|---|---|---|
| `python/fcvae/test/run_f0_acceptance.sh` | vendored FCVAE trains/exports; single-file ONNX bit-exact vs the sibling repo; ORT-Java loads the FFT graph (~5 min) |
| `striim/pipeline/run_f1_acceptance.sh` | scorer hot-swap: bad-signature rejections bit-exact, swap changes 96/96 scores, rollback bit-exact, touch re-promotion (~15 min) |
| `striim/pipeline/run_f2_acceptance.sh` | Feast per-combo params: threshold flip with bit-identical scores, version skew guard, Feast-down fallback, restore; per-combo isolation (~15 min) |
| `striim/pipeline/run_f3_acceptance.sh` | ground-truth eval: pinned exact P/R/F1 + independent recomputation, monitor metric signals, staleness/toggle negatives, drift-to-RED and exact recovery (~20 min) |
| `striim/pipeline/run_f4_acceptance.sh` | the full quality-triggered retrain loop incl. all negatives and the docker error modes (~20 min) |
| `striim/pipeline/check_monrest_acceptance.py`, `check_toggles_acceptance.py`, `striim/mysql/run_ddl_acceptance.sh` | taxi-side: mon/REST signal sourcing, EnabledSignals toggles, CDC schema-evolution signal |

All need `STRIIM_ADMIN_PW` exported; F3/F4 assume the F3-era agent module is LOADED (the
F3 harness's deploy step does the shared-module reload itself; F4 accepts
`F4_RELOAD_MODULE=1` if you skipped F3).

## 6. Demo script (the acts, condensed)

1. **Health verdicts**: `quality_monitor.tql` pointed at any app; show GREEN -> stop the
   feed -> freshness WARN/FAIL -> YELLOW/RED with per-signal rationale.
2. **Toggles**: redeploy the monitor with a family removed from `EnabledSignals`; it
   vanishes from `signals[]` and appears in `disabled_signals` (policy, not amnesia).
3. **Gated publish + rejection + rollback** (either pipeline): publish a good model (swap
   proven by prediction diff), attempt a bad-signature model (bit-exact refusal), rollback
   via the control file (bit-exact revert).
4. **Feast-only flip** (FCVAE): push a new threshold; every decision changes while scores
   stay bit-identical; the poisoned-version and Feast-down cases fall back per event.
5. **Ground-truth eval** (FCVAE): drop a label file, run `eval_labels.py`, watch
   `model_f1[Penny_All]` go non-UNKNOWN with values matching the eval file.
6. **Quality-triggered retrain** (FCVAE): flip the threshold to degrade F1 -> monitor RED ->
   trigger fires `metric_degradation` -> container retrains the degraded combo -> swap +
   recovery against the new model's own baseline. (This is `run_f4_acceptance.sh` phases
   C-F, runnable live in ~10 minutes.)

## 7. Troubleshooting (the expensive lessons, condensed)

- **OP code changes not taking effect**: NEVER `UNLOAD` a module while apps still use it —
  on 5.2.0.4 that pins the old class bytes for the server lifetime (later UNLOAD/LOADs
  report Success and still serve stale classes; even restart recovery reproduces them while
  the apps exist). Iterate with: drop the apps using the OP, UNLOAD, LOAD, redeploy; if
  already pinned: drop apps, UNLOAD, RESTART Striim, LOAD, redeploy.
  `deploy_fcvae_monitor.sh --reload-module` automates the safe order and sha-verifies the
  module cache (`$STRIIM_HOME/.striim/OpenProcessor/`) after LOAD.
- **Killing processes on the Feast port**: always `lsof -ti tcp:6567 -sTCP:LISTEN`. A bare
  `lsof -ti tcp:6567` also matches the STRIIM SERVER's client sockets (the params OP keeps
  HTTP keep-alives) and killing that list kills Striim.
- **FileReader**: tracks files by NAME; re-feeding needs a fresh filename; write feeds to a
  `.tmp` name then `mv` (never let it see a partial file); `positionByEOF: false` reads
  matching files from the start on (re)deploy.
- **FileWriter filenames must EMBED `.json`** (`x.json` rolls to `x.00.json`; a bare `x`
  rolls extensionless and every `*.json` glob misses it). JSONFormatter files are OPEN
  arrays until they roll: parse them tolerantly (every checker here does).
- **Feast transients**: one failed lookup per ~96-request burst against the local server is
  NORMAL (the OP retries once; per-event config fallback is the designed degradation).
  Never assert uniform-100% on a Feast-served phase.
- **Docker failure modes** (fcvae trainer wrapper): exit 7 = daemon down, exit 8 = image
  missing, 2 = data missing, 3 = broken publish contract.
- **Transient node CPU FAILs**: scoring bursts can peg `node_cpu_pct` for a tick; anything
  asserting on verdicts must poll for the settled state, not sample one tick.
- **After a Striim restart**: re-establish the live model identity (fresh publish) before
  trusting prediction comparisons; app-recovery races make old baselines unreliable.
