# model-quality-agent

A standalone, drag-and-drop **quality management + ML-retraining** component for Striim pipelines
(the "Striim Health Monitor agent"). It monitors the health of any running pipeline, and for ML
pipelines it extends into automated retraining and safe model hot-swap.

The architecture is one three-stage loop with a swappable final step:

1. **Collect** platform + pipeline health (moving from JMX to mon-command + REST for SaaS).
2. **Assess** each signal against a configurable policy, producing a GREEN/YELLOW/RED verdict with a
   per-signal reason.
3. **Act** — for a generic pipeline, suggest a fix; for an ML pipeline, retrain and hot-swap the model.

See `docs/quality_manager_ml_retrain_6week_plan.md` for the full 6-week scope and week-by-week sequence,
and `docs/session-prompt.md` for the extraction brief this repo was created from.

## Provenance

Extracted from the `paypal-demo` repo (the NYC-taxi fare-inference demo) on 2026-07-01, taking only the
transitive closure of the quality-agent initiative. The source work lived on two feature branches,
`feature/quality-agent-layer1-health` (operational health) and `feature/quality-agent-layer2-schema`
(schema-evolution); the Layer 2 branch fully contains Layer 1 (verified: `merge-base == layer1 tip`, no
unique Layer 1 commits), so everything here comes from the Layer 2 tree. The real XGBoost training and
ONNX-export scripts under `python/model/` were recovered from `paypal-demo` git history (they had been
deleted in a "repo clean up"); `features.py` is the pre-cleanup superset that matches the shipped
`model.onnx`. Large binaries (the built `.scm`, `model.onnx`) and the raw/processed parquet data were
left behind — the OPs are rebuilt from source and the models re-exported from `train.py`/`export_onnx.py`.

## Layout

```
striim/                 Striim Open Processors (Java, WAEvent pass-through) + Maven builds
  quality-agent/        ModelQualityAgent  (the monitoring agent; Layer 1 + Layer 2 Phase 1 schema signal)
                        quality_monitor.tql: drag-and-drop standalone cross-app monitor (mon/REST, EnabledSignals)
    StriimWatcher/      field-team mon/REST collector — TRANSPORT REFERENCE for the Week 1 JMX->mon/REST pivot
  model-op/             ModelOp   (ONNX scoring + production-safe model hot-swap) + swap-test harness (test/)
  feature-op/           FeatureOp (Feast feature enrichment)
  pipeline/             inference_pipeline.tql (CSV) + _mysql.tql (CDC) + feed/acceptance harnesses
  mysql/                MySQL 8.0 CDC setup (Docker, init.sql, my.cnf) for the schema-evolution signal
python/                 Scenario-A Python (uv project): feature engineering, Feast, XGBoost training, ONNX export
  model/                the `model` package (imports use `from .config`; run via `python -m model.<cmd>`)
reference-OPs/          field-team sample OPs (RestCaller, App-CVS, App-ModelOP) — CLAUDE.md ground truth
docs/                   6-week plan + the extraction brief
CLAUDE.md               Open Processor patterns, the WAEvent pass-through contract, build/load lifecycle
```

## Build & run

### Java Open Processors (require a local Striim install)

The three OPs use `system`-scoped Maven dependencies that resolve against a local Striim install at
`$STRIIM_HOME` (default `/opt/Striim`), so a fresh clone **cannot** `mvn package` without Striim present.
`ModelOp` additionally bundles `onnxruntime` (its `.scm` is ~41 MB). To build and stage an OP:

```bash
cd striim/model-op        # or feature-op, quality-agent
STRIIM_HOME=/opt/Striim ./build.sh   # mvn clean package -> target/<Op>.jar, staged to UploadedFiles/<Op>.scm
```

Then in the Striim console: `LOAD OPEN PROCESSOR 'UploadedFiles/<Op>.scm';` and deploy a pipeline from
`striim/pipeline/`. See `CLAUDE.md` for the full load/unload lifecycle and the JMX-enablement notes.

### Python (uv)

```bash
cd python
uv venv && uv sync --extra training --extra dev   # base = runtime + swap-test; training = recovered Scenario-A
uv run python -m model.feast_writer show <geohash>
```

The swap-test artifact generator needs only the base env:

```bash
uv run python ../striim/model-op/test/make_swap_artifacts.py --out-dir ../striim/model-op/test/artifacts
```

## Status (vs the 6-week plan)

Already in place from the extracted work: the full inference pipeline (FeatureOp -> ModelOp -> scored
output), the Layer 1 operational-health agent with an explainable per-signal verdict, the Layer 2 Phase 1
schema-evolution (DDL) signal, and **ModelOp's production-safe hot-swap** (signature gate, atomic swap,
SHA-256 hash, control-file rollback). Week 1's load-bearing work — moving health collection off JMX onto
mon/REST and the ML signals onto the data stream — is the next task; see `PLAN.md` (added at the start of
Week 1) and the reconciliation in `docs/`.
