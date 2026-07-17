# FCVAE Model-Quality Demo — Runbook

Manual, console-first setup of the FCVAE anomaly-detection pipeline and its
quality-monitoring loop on **any local Striim installation**. Every phase is
independent and done by hand: you upload modules through the web UI, paste TQL
into the console, and start Feast / the trainer in their own terminals. No
deploy scripts, no REST tokens, no cluster names, no assumptions about where
Striim is installed. Companion doc: [ARCHITECTURE.md](ARCHITECTURE.md) (how the
components work); the taxi-demo flow lives there and in the scripted appendix.

Conventions used throughout:

- `$REPO` = wherever you cloned this repository.
- **Console** = the Tungsten console in the Striim web UI (`http://localhost:9080`,
  log in as `admin`). Statements are pasted there unless a command block starts
  with `$` (then it is a shell command).
- The pipeline reads/writes two **fixed data directories**: `/opt/Striim/fcvae-models`
  (model bundles) and `/opt/Striim/UploadedFiles` (scored output). These are plain
  data paths baked into the TQL — they work identically whether or not your Striim
  is actually installed at `/opt/Striim` (Phase 0 creates them).

---

## Phase 0 — Prerequisites (once per machine)

| Requirement | Check / how to get it |
|---|---|
| Striim 5.2.x running locally, web UI at `localhost:9080`, admin login | `open http://localhost:9080` |
| **WAEUdf** jar in your Striim install's `lib/` (SE add-on; the pipeline's `createWAEvent` CQs need it) | `ls <your-striim-install>/lib \| grep -i wae` — if missing, get `WAEUdf-5.2.0.jar` from the SE Confluence page (webaction.atlassian.net page 2318336001), drop it in `lib/`, **restart Striim** |
| [uv](https://docs.astral.sh/uv/) + the Python env | `cd $REPO/python && uv sync` — base deps only; **no `--extra` is needed to run anything in this runbook** (torch is only used inside the Docker trainer) |
| Docker (Phase 8 only) | `docker info` |
| The fixed data directories | see below |

```bash
$ sudo mkdir -p /opt/Striim/fcvae-models /opt/Striim/UploadedFiles
$ sudo chown "$(whoami)" /opt/Striim/fcvae-models /opt/Striim/UploadedFiles
$ mkdir -p /tmp/fcvae_swap_test          # the feed directory the pipeline watches
```

Both directories must be writable by **whoever runs the Striim server** too
(the pipeline's FileWriters create the scored JSON in `UploadedFiles`). On a
Mac dev box where you start Striim yourself, the chown above is enough; if
Striim runs as a service user (packaged Linux installs), chown to that user
instead, or `chmod 775` with a shared group (`777` is fine for a throwaway
demo box).

> Apple Silicon note: if `uv sync` claims you are on an Intel mac
> (`macosx_*_x86_64`), your uv or Python is an x86 binary under Rosetta
> (usually Migration Assistant residue). Reinstall uv natively
> (`curl -LsSf https://astral.sh/uv/install.sh | sh`), `rm -rf python/.venv`,
> `uv python install 3.12`, and re-sync.

---

## Phase 1 — Load the OP modules

The three compiled modules are **committed in this repo** — no Maven, no JDK,
no version flags (built against the 5.2.0.4 SDK; the OP interface is stable
across 5.2.x — if your Striim rejects them, rebuild per Appendix A):

| File in repo | Module |
|---|---|
| `striim/fcvae-scorer/FCVAEOnnxScorer.scm` (31 MB) | ONNX anomaly scorer with hot-swap |
| `striim/fcvae-params-op/FCVAEParamsOp.scm` (287 KB) | per-combo Feast params lookup |
| `striim/quality-agent/ModelQualityAgent.scm` (40 KB) | quality monitor (Phase 7) |

1. In the web UI open the **Files** page and upload all three files. They land
   in the running server's `UploadedFiles` area regardless of where Striim is
   installed. (Filesystem alternative: `cp` them into
   `<your-striim-install>/UploadedFiles/`.)
2. In the console:

```sql
LOAD OPEN PROCESSOR 'UploadedFiles/FCVAEOnnxScorer.scm';
LOAD OPEN PROCESSOR 'UploadedFiles/FCVAEParamsOp.scm';
LOAD OPEN PROCESSOR 'UploadedFiles/ModelQualityAgent.scm';
```

Each must return Success.

> ⚠️ If this server ever ran an **older FCVAE demo** (an existing
> `FCVAEOnnxScorer.scm` from another repo, e.g. in a `modules/` directory):
> loaded module bytes are pinned per module *name*, and an UNLOAD issued while
> any app still uses the module silently pins the old classes until the server
> restarts. Clean path: run the Phase 9 teardown, move the old `.scm` file out
> of the way, **restart Striim**, then LOAD the new ones.

---

## Phase 2 — Start Feast (its own terminal)

```bash
$ cd $REPO/python
$ uv run python -m fcvae.feast_setup apply                 # registers the feature view (once)
$ uv run python -m fcvae.feast_setup serve --port 6567     # leave running
```

Port 6567 (the taxi demo owns 6566). Optional but standard: without it the
pipeline still works — the params OP falls back to each model's swap-paired
config (`fcvae_params_used=config_fallback` in the output instead of `feast`).

> Never sweep port 6567 with a bare `lsof` kill — the OP's client sockets show
> up too and you will kill Striim. Use `lsof -ti tcp:6567 -sTCP:LISTEN`.

---

## Phase 3 — Stage the models

The deployable ONNX bundles are committed (`python/fcvae/artifacts/*_prebuilt/`).
Publish them to the directories the scorer watches — `publish` gate-checks the
artifacts (signature, golden-set parity) and pushes the scoring params to Feast:

```bash
$ cd $REPO/python
$ uv run python -m fcvae.publish run --model Penny_All \
    --artifacts fcvae/artifacts/Penny_All_prebuilt  --out /opt/Striim/fcvae-models/Penny_All
$ uv run python -m fcvae.publish run --model Accel_CMP \
    --artifacts fcvae/artifacts/Accel_CMP_prebuilt --out /opt/Striim/fcvae-models/Accel_CMP
```

Expected: `PASS PUBLISH gates` then `published sha256:660f8547a307` (Penny_All)
and `sha256:9adb70b4194c` (Accel_CMP). If Feast is down the push logs a WARN
and publish still succeeds (add `--require-feast` to make it strict).

---

## Phase 4 — Deploy the pipeline (console)

1. **Re-runs only**: run the Phase 9 teardown first (on a first run every
   teardown statement fails harmlessly — skipping is fine).
2. Open `$REPO/striim/pipeline/fcvae_inference.tql`, copy the **entire file
   except the final `quit;` line**, and paste it into the console. The file
   contains its own `CREATE NAMESPACE fcvaedemo;` … `DEPLOY` … `START`, so one
   paste creates, deploys, and starts `fcvaedemo.FcvaeInference`.
3. Confirm it is running (poll a few times; deployment takes seconds):

```sql
mon fcvaedemo.FcvaeInference;
```

`statusChange` must show `RUNNING`. If a mid-paste statement failed, tear down
(Phase 9) and re-paste — don't patch a half-built app forward.

---

## Phase 5 — Feed it

The 5-day demo feed is committed. Copy it in **after** the app is RUNNING
(FileReader may skip files that predate app start), using the `.tmp`+`mv`
rename so a partial copy is never read:

```bash
$ cp $REPO/striim/pipeline/feeds/penny_feed_s7.csv /tmp/fcvae_swap_test/penny_feed_s7.csv.tmp
$ mv /tmp/fcvae_swap_test/penny_feed_s7.csv.tmp /tmp/fcvae_swap_test/penny_feed_s7.csv
```

780,693 events over 120 event-hours (2025-01-13 → 01-17), all-normal traffic.

> **Feeding again later**: FileReader tracks files by *name*, and the windows
> by *event time* — a second feed needs a fresh filename AND non-overlapping
> event time. Generate one from the committed base, then copy it in with the
> same `.tmp`+`mv` pattern:
> ```bash
> $ cd $REPO/python && uv run python ../striim/pipeline/make_fcvae_feed.py emit \
>     --base ../striim/pipeline/feeds/penny_base_5d.csv --shift-days 14 \
>     --out /tmp/penny_feed_s14.csv
> $ cp /tmp/penny_feed_s14.csv /tmp/fcvae_swap_test/penny_feed_s14.csv.tmp
> $ mv /tmp/fcvae_swap_test/penny_feed_s14.csv.tmp /tmp/fcvae_swap_test/penny_feed_s14.csv
> ```

---

## Phase 6 — Verify

Console:

```sql
mon fcvaedemo.TxnFileSource;     -- input/output climbing to ~780,693
mon fcvaedemo.PennyOnnxProc;     -- output reaching 96 scored windows
mon fcvaedemo.AccelOnnxProc;     -- output reaching 96 scored windows
```

Files (scoring output; the first window only closes after 24 event-hours fill,
then the count settles over a few minutes):

```bash
$ ls -l /opt/Striim/UploadedFiles/fcvae_scored*.json /opt/Striim/UploadedFiles/fcvae_accel*.json
$ tail -c 600 "$(ls -t /opt/Striim/UploadedFiles/fcvae_scored*.json | head -1)"
```

Each record carries `combo_key, window_end, is_anomaly, anomaly_score,
threshold` (+ `fcvae_params_used` = `feast` or `config_fallback`). Definitive
check — polls until all 96 windows are scored:

```bash
$ cd $REPO/python && uv run python ../striim/pipeline/check_fcvae_swap.py snapshot \
    --base-start 2025-01-06 --shift-days 7 --min-count 96 --wait-sec 300 \
    --out /tmp/fcvae_s7_snapshot.json
```

> The newest `*.json` output file is an **open JSON array** (the closing `]`
> is only written when the file rolls) — don't feed it to a strict JSON parser.

---

## Phase 7 — Quality monitor (optional, separate app)

1. `ModelQualityAgent.scm` is already loaded (Phase 1).
2. Open `$REPO/striim/quality-agent/fcvae_monitor_manual.tql` — a fully
   rendered monitor app (no templates, no scripts). **Edit one line**: set
   `MonRestPassword` to your admin password (plaintext works; the vault
   alternative is documented inline).
3. Paste the file (minus the final `quit;`) into the console. It creates,
   deploys, and starts `fcvaemon.FcvaeMonitor`.
4. One health assessment every 30 s lands in the server's own
   `UploadedFiles/fcvae_health_assessments*.json` and in SysOut.

Expected on a fresh setup: verdict GREEN with the platform signals populated;
the four model-metric families (`model_precision/recall/f1`, `anomaly_rate`)
read **UNKNOWN** until an eval file exists — that is by design (never a false
PASS). To light them up, run the ground-truth eval:

```bash
# feed the anomaly-bearing TEST slice (fresh name, non-overlapping event time)
$ cd $REPO/python
$ uv run python ../striim/pipeline/make_fcvae_feed.py emit \
    --base ../striim/pipeline/feeds/penny_test_5d.csv --shift-days 7 \
    --out /tmp/penny_feed_test_s7.csv
$ cp /tmp/penny_feed_test_s7.csv /tmp/fcvae_swap_test/penny_feed_test_s7.csv.tmp
$ mv /tmp/fcvae_swap_test/penny_feed_test_s7.csv.tmp /tmp/fcvae_swap_test/penny_feed_test_s7.csv
# emit operator labels from the same committed slice (NOT the default --source,
# which points at the author's machine)
$ mkdir -p /opt/Striim/UploadedFiles/ground_truth
$ uv run python ../striim/pipeline/make_fcvae_labels.py emit \
    --start-date 2025-02-25 --days 5 --shift-days 7 \
    --source ../striim/pipeline/feeds/penny_test_5d.csv \
    --out /opt/Striim/UploadedFiles/ground_truth/labels_s7.csv
# join labels to scored output -> eval_metrics.json (defaults match the monitor)
$ uv run python ../striim/pipeline/eval_labels.py run
```

Within a tick or two the metric families go live (PASS/WARN/FAIL relative to
each combo's published baseline). A single `run` keeps them live for 15 minutes
(`EvalMaxAgeSec: 900` — a stale eval file deliberately reverts the families to
UNKNOWN, since evaluator liveness *is* the staleness signal). For a continuously
fresh demo, keep the evaluator running in its own terminal instead:
`uv run python ../striim/pipeline/eval_labels.py watch --poll-sec 30`.

---

## Phase 8 — Retrain in Docker (optional, its own terminal)

Build the trainer image (once):

```bash
$ cd $REPO/python && docker build -f Dockerfile.fcvae -t mqa-fcvae-trainer .
```

**Training data.** The trainer needs a labeled transactions CSV with a
`split` column. Two options:

- **Full quality**: the original ~496 MB `synthetic_transactions.csv`
  (external — lives in the `fcvae-anomaly-detection` repo; ask the author).
- **Demo-grade, fully self-contained**: derive one from the committed 5-day
  slice. The slice is 100% `split=train`, which the trainer cannot use as-is
  (empty validation split crashes epoch 1), so rewrite the split by day first:

```bash
$ cd $REPO/python && uv run python - <<'EOF'
import pandas as pd
df = pd.read_csv('../striim/pipeline/feeds/penny_base_5d.csv', parse_dates=['timestamp'])
day = df['timestamp'].dt.strftime('%Y-%m-%d')
df['split'] = 'train'
df.loc[day == '2025-01-09', 'split'] = 'val'
df.loc[day == '2025-01-10', 'split'] = 'test'
df.to_csv('/tmp/fcvae_train_demo.csv', index=False)
print(df['split'].value_counts().to_dict())
EOF
```

  (~97 total windows, ~70 of them training, vs ~1400 in the full dataset —
  fine for demonstrating the retrain → hot-swap loop, not for model quality.)

Run the trainer (train → ONNX export with parity gate → gated publish straight
into the watched model dir):

```bash
$ docker run --rm --name mqa-fcvae-trainer-run --cpus 4 \
    -v /tmp/fcvae_train_demo.csv:/work/data/synthetic_transactions.csv:ro \
    -v /opt/Striim/fcvae-models:/out \
    -e FCVAE_MODEL=Penny_All \
    mqa-fcvae-trainer
```

- Swap `-e FCVAE_MODEL=Accel_CMP` to retrain the other combo; add
  `-e FCVAE_EPOCHS=N` to shorten a demo run.
- macOS Docker Desktop: `/opt/Striim` must be added to Settings → Resources →
  File sharing, or the `/out` mount is denied.
- A full Penny train is ~1 min in-container on the full dataset.

**What happens next, automatically**: the scorer notices the new
`model.onnx` mtime and hot-swaps weights+threshold together, zero downtime.
The Feast rows still carry the *old* model's version, so the skew guard makes
the scorer use the swap-paired config (correct immediately). Re-engage Feast
for the new model, and/or restore the original model:

```bash
$ cd $REPO/python
$ uv run python -m fcvae.feast_setup push --model Penny_All      # re-point Feast at the new sha
# restore the committed original at any time (hot-swaps back):
$ uv run python -m fcvae.publish run --model Penny_All \
    --artifacts fcvae/artifacts/Penny_All_prebuilt --out /opt/Striim/fcvae-models/Penny_All
```

---

## Phase 9 — Teardown / clean slate

Paste into the console **in this order** (apps before namespaces before
UNLOADs — an UNLOAD while an app still uses the module pins the old bytes
until a restart). Every statement fails harmlessly if the object doesn't exist:

```sql
STOP APPLICATION fcvaedemo.FcvaeInference;
UNDEPLOY APPLICATION fcvaedemo.FcvaeInference;
DROP APPLICATION fcvaedemo.FcvaeInference CASCADE;

STOP APPLICATION fcvaemon.FcvaeMonitor;
UNDEPLOY APPLICATION fcvaemon.FcvaeMonitor;
DROP APPLICATION fcvaemon.FcvaeMonitor CASCADE;

use admin;
DROP NAMESPACE fcvaedemo CASCADE;
DROP NAMESPACE fcvaemon CASCADE;

UNLOAD OPEN PROCESSOR 'UploadedFiles/FCVAEOnnxScorer.scm';
UNLOAD OPEN PROCESSOR 'UploadedFiles/FCVAEParamsOp.scm';
UNLOAD OPEN PROCESSOR 'UploadedFiles/ModelQualityAgent.scm';
```

Shell-side leftovers, if you want a truly clean machine:

```bash
$ rm -rf /tmp/fcvae_swap_test /opt/Striim/fcvae-models/* \
      /opt/Striim/UploadedFiles/fcvae_scored*.json /opt/Striim/UploadedFiles/fcvae_accel*.json \
      /opt/Striim/UploadedFiles/fcvae_eval /opt/Striim/UploadedFiles/ground_truth
```

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Paste fails at a `PennyToWaevent`/`AccelToWaevent` CQ | WAEUdf jar missing from `lib/` — Phase 0; restart Striim after adding |
| App deploys but scorer OPs fail to start | Model bundles missing — Phase 3 must run before Phase 4 |
| `LOAD` succeeds but the OP behaves like an old version | Stale module bytes pinned (an UNLOAD happened while apps used it). Teardown, **restart Striim**, LOAD, redeploy |
| Events fed but `mon` shows `input: 0` | Poll again (read-burst propagation lag); or the file predates app start / reuses a filename — re-feed with a fresh name |
| Scored output stops at fewer windows than expected | The last event-hour never closes in-feed (by design), and the first 23 are warm-up |
| `fcvae_params_used: config_fallback` everywhere | Feast down (Phase 2) or version skew after a retrain — `feast_setup push --model <X>` |
| One transient Feast 500 per feed burst | Normal (server restart edge); the OP retries |
| `LIST OPENPROCESSORS;` doesn't show a loaded module | It lists *instances*, not modules, on 5.2.0.4 — deploy an app that uses it to see it |
| uv says Intel mac on Apple Silicon | Rosetta toolchain — see Phase 0 note |

---

## Appendix A — Rebuilding the modules from source

Only needed if you change OP code or your Striim version rejects the committed
modules. Requires JDK 11 + Maven and a local Striim install (the poms resolve
system-scoped Striim jars from it):

```bash
$ cd $REPO/striim/fcvae-scorer     # same for fcvae-params-op, quality-agent
$ mvn clean package -DSTRIIM_HOME=<your-striim-install> -DSTRIIM_VERSION=<your version, e.g. 5.2.0.4>
$ cp target/FCVAEOnnxScorer.jar <anywhere>/FCVAEOnnxScorer.scm   # then upload + LOAD as in Phase 1
```

`<your version>` must match `ls <install>/lib/Platform-*.jar`. The taxi-demo
OPs (`model-op`, `feature-op`) build the same way.

## Appendix B — Scripted deploy (author machine) and the taxi demo

The pre-rewrite automated flow still exists for the acceptance harnesses:
`striim/pipeline/deploy_fcvae.sh` (env: `STRIIM_ADMIN_PW`, `STRIIM_CLUSTER`,
`STRIIM_HOME`, `STRIIM_BASE_URL`) and `striim/pipeline/deploy_fcvae_monitor.sh`
(renders the `fcvae_monitor.tql` template — the manual variant used in Phase 7
is its pre-rendered equivalent). The acceptance suites
(`striim/pipeline/run_f*_acceptance.sh`) drive everything end to end and
require the uv `fcvae` extra plus, for some phases, the external source CSV.

The NYC-taxi demo (`qualitydemo.FareInference`: FeatureOp → ModelOp with the
in-app agent) is documented in [ARCHITECTURE.md](ARCHITECTURE.md); its wiring
follows the same manual pattern (build/upload/LOAD the `model-op`, `feature-op`,
`quality-agent` modules, Feast on 6566, paste
`striim/pipeline/inference_pipeline.tql`).
