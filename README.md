# model-quality-agent

A standalone, drag-and-drop **quality management + ML-retraining** component for Striim
pipelines (the "Striim Health Monitor agent"). It monitors the health of any running
pipeline; for ML pipelines it extends into ground-truth accuracy tracking, automated
retraining, and production-safe model hot-swap. Everything is proven live by scripted,
deterministic acceptance suites.

**Start here:**

- [RUNBOOK.md](RUNBOOK.md) — run everything locally on your own Striim instance, step by
  step, with what-happens-at-each-step notes, the demo script, and the troubleshooting
  lessons.
- [ARCHITECTURE.md](ARCHITECTURE.md) — how each component works and relates to the others:
  every OP property, the mon/REST surface the agent consumes, how the accuracy metrics are
  derived and used, and how the sha-256 checksum chain establishes model validity.

## The story (what this demonstrates)

One perceive-assess-act loop, instantiated twice:

1. **Sense**: an Open Processor agent reads platform health over Striim's own mon/REST API
   (SaaS-safe; JMX optional for self-managed), ML fault counters off the scored stream, and
   ground-truth accuracy metrics off an operator-fed evaluation file.
2. **Assess**: every signal is judged against an explicit, per-signal policy
   (PASS/WARN/FAIL/UNKNOWN, with UNKNOWN never masquerading as healthy), rolled up to a
   GREEN/YELLOW/RED verdict with a natural-language rationale, and an ops circuit breaker.
3. **Act**: assessments are emitted as structured events; a host-side trigger retrains when
   a schedule elapses, enough new data arrives, or **model quality degrades against the
   model's own accepted baseline** (precision/recall/F1 from operator ground truth); a
   one-shot Docker container trains, exports to ONNX, and publishes through six validity
   gates; the scorer hot-swaps the model within one batch, with sha-256 identity, history,
   and control-file rollback.

Two pipelines exercise the loop end to end:

- **NYC-taxi fare regression** (`qualitydemo.FareInference`): FeatureOp (Feast feature
  enrichment) + ModelOp (XGBoost-as-ONNX + hot-swap) + the in-app agent, plus a standalone
  cross-app monitor and a MySQL CDC variant with upstream schema-evolution (DDL) detection.
- **Fiserv-style FCVAE anomaly detection** (`fcvaedemo.FcvaeInference`): two per-combo
  scoring chains (pooled sub-dollar `Penny_All`, network/type `Accel_CMP`) over hourly
  transaction-count windows, per-combo scoring parameters served from Feast behind a
  model-version skew guard, ground-truth precision/recall/F1 evaluation
  (point-adjusted, the anomaly-detection literature's standard), baseline-relative quality
  signals, and the quality-triggered retrain loop
  (drift -> RED -> `metric_degradation` fire -> in-container retrain of exactly the
  degraded combo -> gated publish -> hot-swap -> recovery judged against the new model's
  own baseline).

Every capability has a scripted proof: `run_f0_acceptance.sh` (vendored model + ONNX
parity) through `run_f4_acceptance.sh` (the full quality-triggered retrain drill), plus
the taxi-side mon/REST, toggles, DDL, and swap checks. See RUNBOOK.md section 5.

## Provenance

Extracted from the `paypal-demo` repo (the NYC-taxi fare-inference demo) on 2026-07-01,
taking only the transitive closure of the quality-agent initiative; the FCVAE adaptation
(2026-07-16, branch `fcvae-adaptation`) vendored the model library from the sibling
`fcvae-anomaly-detection` repo (commit 9575ba5) and adapted the whole quality architecture
to it. Large data/model artifacts stay EXTERNAL by design: the labeled FCVAE source CSV and
prebuilt checkpoints live in the sibling repo (paths env-overridable via `FCVAE_REPO`), the
taxi processed parquet alongside; nothing in this repo requires them except training,
publishing, and the live demos.

## Layout

```
striim/                 Striim Open Processors (Java, WAEvent pass-through) + Maven builds
  quality-agent/        ModelQualityAgent (health + model-quality signals) + monitor TQL templates
                        (quality_monitor.tql taxi, fcvae_monitor.tql FCVAE)
  model-op/             ModelOp   (taxi ONNX scoring + hot-swap) + swap-test harness
  feature-op/           FeatureOp (taxi Feast feature enrichment)
  fcvae-scorer/         FCVAEOnnxScorer (FCVAE NLL scoring + hot-swap + skew guard)
  fcvae-params-op/      FCVAEParamsOp  (FCVAE per-combo Feast scoring params)
  pipeline/             the TQLs + deploy scripts + feed/label/eval tooling + acceptance harnesses
  retrain/              retrain_trigger.py + trainer wrappers (taxi + fcvae) + README
  mysql/                MySQL 8.0 CDC setup for the schema-evolution signal
python/                 uv project: taxi `model` package + vendored `fcvae` package,
                        trainer Dockerfiles (Dockerfile taxi, Dockerfile.fcvae)
reference-OPs/          field-team sample OPs (ground truth for OP patterns)
CLAUDE.md               Striim OP development guide + every hard-won platform gotcha (local-only)
docs/                   plans, briefs, backlog (local-only by design)
```

## Build and run

Short version (the RUNBOOK has the full ordered path):

```bash
# Java OPs (need a local Striim at $STRIIM_HOME; system-scoped Maven deps)
cd striim/<op-dir> && STRIIM_HOME=/opt/Striim ./build.sh   # stages UploadedFiles/<Op>.scm

# Python (uv; the two extras are mutually exclusive stacks)
cd python && uv sync --extra training    # taxi (XGBoost)
cd python && uv sync --extra fcvae       # FCVAE (torch)

# Trainer images
docker build -t mqa-trainer python/
(cd python && docker build -f Dockerfile.fcvae -t mqa-fcvae-trainer .)

# Deploy + prove
striim/pipeline/deploy_fcvae.sh                      # the FCVAE pipeline (idempotent)
striim/pipeline/deploy_fcvae_monitor.sh --reload-module
striim/pipeline/run_f3_acceptance.sh                 # ground-truth eval proof (35 steps)
striim/pipeline/run_f4_acceptance.sh                 # quality-triggered retrain proof (21 steps)
```
