# ARCHITECTURE: how every component works and how they fit together

Companion to [RUNBOOK.md](RUNBOOK.md) (the ordered instruction set). This file explains the
system: each component's job, its exact interfaces (properties, REST surface, file
contracts), how the accuracy metrics are derived and used, and how the sha-256 checksum
chain establishes model validity end to end.

Math notation is LaTeX; all thresholds and defaults are quoted from code.

---

## 1. System overview

One loop, two instantiations (NYC-taxi regression, Fiserv-style FCVAE anomaly detection):

```
                         SENSE                    ASSESS                    ACT
                 +--------------------+   +---------------------+   +------------------+
  Striim         |  ModelQualityAgent |   | per-signal policy   |   | assessment file  |
  pipeline  ---> |  (mon/REST + eval  |-->| PASS/WARN/FAIL/UNK  |-->| + verdict + ops  |
  (OPs below)    |   file + counters) |   | -> GREEN/YELLOW/RED |   |   circuit breaker|
                 +--------------------+   +---------------------+   +---------+--------+
                                                                              |
                 +--------------------+   +---------------------+            v
  model    <---  |  scorer hot-swap   |<--| trainer container   |<--- retrain_trigger.py
  artifacts      |  (sha-256 gated)   |   | (train->export->    |     (schedule | new data
                 +--------------------+   |  six publish gates) |      | metric_degradation)
                                          +---------------------+
```

The FCVAE scoring pipeline (`fcvaedemo.FcvaeInference`):

```
FileReader(csv) -> ParseTxn CQ -+-> Penny filter (amount<1) -> 1h window -> 24-row window -+
                                |                                                          |
                                +-> Accel_CMP filter --------> 1h window -> 24-row window -+
                                                                                           v
                     FCVAEParamsOp (Feast 6567: per-combo scaler/threshold/version -> userdata)
                                                                                           v
                     FCVAEOnnxScorer (normalize, ONNX NLL, decide, hot-swap machinery)
                                                                                           v
                     FormatScored CQ -> FileWriter (fcvae_scored*.json / fcvae_accel*.json)
```

The taxi pipeline (`qualitydemo.FareInference`) is the same shape with FeatureOp (Feast
6566 feature enrichment) + ModelOp (XGBoost-as-ONNX regression + hot-swap) and an IN-APP
agent branch; the FCVAE monitor (`fcvaemon.FcvaeMonitor`) is standalone and senses the
scoring app purely over mon/REST.

Design invariants that hold everywhere:

- **WAEvent pass-through**: OPs read positional `data[]`, enrich `userdata` for inter-OP
  handoff, and append to `data[]` for anything a formatter must see (formatters cannot read
  userdata).
- **UNKNOWN is never a false PASS**: missing beans, unreachable REST, stale eval files,
  unlabeled combos all read UNKNOWN with a reason, and UNKNOWN never opens the breaker.
- **Everything proven behaviorally**: acceptance harnesses assert on output files and REST
  state, never on stdout.

## 2. The quality agent (ModelQualityAgent)

An OP whose primary input is a TIMER, not the stream: every `TickIntervalSec` it runs
perceive -> assess -> act and emits ONE assessment event (the timer thread is the sole
`send()` caller). `run()` only aggregates ML counters stamped on scored events (taxi
in-app variant).

**Perceive** (transport per `HealthSource`):

- `MON_REST` (default, SaaS-safe): authenticates at `POST /security/authenticate` (form
  `username`/`password`, response field `token`), then posts console commands to
  `POST /api/v2/tungsten` with header `authorization: STRIIM-TOKEN <token>`
  (re-auth once on 401/403; each call bounded by `MonRestTimeoutSec` with
  `MonRestMaxRetryNum` retries off the tick thread). Commands and the exact fields parsed:

  | Command | Parsed fields |
  |---|---|
  | `mon <ns>.<app>;` | `statusChange`; `applicationComponents[].entityType` (SOURCE/TARGET), `.fullName`, `.latestActivity` ("yyyy-MM-dd HH:mm:ss" -> epoch ms) |
  | `report lee;` | `lagEndToEnd` per row (seconds, e.g. "0.006" or "0.008 sec" -> ms) |
  | `mon;` | `striimClusterNodes[0].cpuRate` ("16%"), `.freeMemory` ("3.31Gb"/"512Mb"/"2Tb" -> GB) |

  Backpressure and discarded_events have no mon/REST field: they stay UNKNOWN by design.
- `JMX` (self-managed clusters): the in-JVM `com.striim.metrics` health beans (see
  CLAUDE.md for enablement).
- Regardless of transport: OP-counter MBeans
  (`com.striim.metrics:name=OpCounters.<ns>.*,type=OpMetrics`, attributes `CounterName`,
  `EventsSeen`, `Faults`, `Component`), the CDC DDL rollup (schema_evolution), and the F3
  eval-metrics file (section 4).

**Assess**: each signal family is gated by `EnabledSignals` (disabled families are OMITTED
from `signals[]` and recorded once in `disabled_signals`; policy, not amnesia). The 15
canonical families:

```
app_status, source_freshness, target_write_age, lag_end2end, backpressure,
discarded_events, node_memory, node_cpu, feature_miss_rate, nan_score_rate,
schema_evolution, model_precision, model_recall, model_f1, anomaly_rate
```

Rollup: RED if any FAIL, else YELLOW if any WARN, else GREEN if anything was known, else
UNKNOWN. `ops_healthy` = verdict is GREEN, UNKNOWN, or (YELLOW and `TreatYellowAsHealthy`);
the circuit breaker is its negation and gates retraining downstream.

**Act**: one WAEvent per tick with the 10-slot positional contract (projected by the
FormatHealth CQs into `health_assessments*.json` / `fcvae_health_assessments*.json`):

| data[] | field | | data[] | field |
|---|---|---|---|---|
| 0 | tick_ts (epoch ms) | | 5 | signal_count |
| 1 | verdict | | 6 | fail_count |
| 2 | ops_healthy | | 7 | warn_count |
| 3 | ops_circuit_breaker_open | | 8 | assessment_json (the typed truth) |
| 4 | rationale (NL) | | 9 | target_app |

Each entry of `assessment_json.signals[]` carries `name`, `observed`, `threshold`, `state`,
`detail`, plus family extras: `events_seen`/`faults` (OP-counter families), `ddls_total`/
`last_ddl` (schema_evolution), and on the model-quality families `combo`, `value`,
`raw_value`, `baseline`, `baseline_model_version`, `tp`, `fp`, `fn`, `n_joined`,
`n_labels`, `coverage`, `n_windows`, `latest_window_end`, `eval_age_sec`.

Full property table (name | type | default | meaning):

| Property | Type | Default | Meaning |
|---|---|---|---|
| TargetNamespace / TargetAppName | String | qualitydemo / FareInference | the monitored app |
| TickIntervalSec | Integer | 30 | perceive/assess/act cadence |
| HealthSource | String | MON_REST | MON_REST or JMX transport |
| MonRestBaseUrl / MonRestUser / MonRestPassword | String/String/Password | localhost:9080 / admin / "" | REST endpoint + credentials (password vault-resolvable, `[ns.vault.key]`) |
| MonRestTimeoutSec / MonRestMaxRetryNum | Integer | 5 / 2 | per-call bound + retries |
| HealthyStatus / FailStatuses | String | RUNNING / HALT,HALTED,TERMINATED,CRASH,CRASHED,EXCEPTION | app_status policy (anything else = WARN) |
| SourceFreshnessWarnSec/FailSec | Integer | 60 / 300 | source last-event age thresholds |
| TargetWriteWarnSec/FailSec | Integer | 60 / 300 | target last-write age thresholds |
| LagWarnMs / LagFailMs | Integer | 5000 / 30000 | end-to-end lag thresholds |
| MemoryWarnPct/FailPct, CpuWarnPct/FailPct | Integer | 85/95, 85/95 | node ceilings (JMX used-%) |
| MemoryFreeWarnGb/FailGb | Integer | 2 / 1 | node free-memory floor (mon/REST) |
| DiscardedWarnDelta/FailDelta | Integer | 1 / 100 | per-tick discarded-event deltas |
| FeatureMissRateWarnPct/FailPct, NanScoreRateWarnPct/FailPct | Integer | 5/20, 5/20 | windowed ML fault-rate ceilings |
| MlMetricsSource | String | STREAM | scored-stream userdata counters, MBean fallback |
| DdlWarnDelta | Integer | 1 | schema_evolution per-tick DDL delta (capped at WARN) |
| EvalMetricsFile | String | "" | F3: eval_metrics.json path; EMPTY = the four metric families are omitted entirely |
| EvalMaxAgeSec | Integer | 900 | eval staleness ceiling (older = UNKNOWN) |
| EvalCombos | String | "" | expected combo keys (absent combo = per-combo UNKNOWN); case-preserved |
| MetricWarnBelowBaselinePct / MetricFailBelowBaselinePct | Integer | 80 / 50 | RELATIVE floors, section 4 |
| AnomalyRateWarnPct / AnomalyRateFailPct | Integer | 40 / 70 | absolute anomaly-rate ceilings |
| BackpressureIsFail | Boolean | false | escalate backpressure WARN -> FAIL |
| TreatYellowAsHealthy | Boolean | true | YELLOW counts as ops-healthy |
| EnabledSignals | String | ALL | CSV of families to assess |
| EnableLogging | Boolean | true | verbose stdout |

## 3. The scoring OPs

**FCVAEOnnxScorer** (`ModelDir` default `/opt/Striim/fcvae-models/Penny_All`, `ControlFile`
default `<ModelDir>/fcvae.control`; positional inputs `ComboKeyIndex`=0, `ValuesIndex`=1,
`WindowEndIndex`=4). Scores a 24-value hourly window: standardize with the scaler, run the
ONNX session (`input` float32 [-1,1,24] -> `nll` float32 [-1,24]), take the LAST-point NLL,
and decide

$$\text{is\_anomaly} \iff \text{nll}_{-1} < \text{last\_point\_threshold}$$

(lower NLL = more anomalous: decisions are inverted relative to intuition). Appends
`data[5..7]` = is_anomaly, anomaly_score, threshold; the FormatScored CQs project
`{combo_key, window_end, is_anomaly, anomaly_score, threshold}` to the JSON sinks.

**FCVAEParamsOp** (Feast on 6567, `MaxRetryNum` 1 with 150 ms backoff, LENIENT: a miss
never drops the event). Posts `POST <FeastUrl>/get-online-features` for feature view
`fcvae_scoring_params_v1` keyed by `combo_key`, and stamps only PRESENT fields into
userdata: `fcvae_scaler_mean`, `fcvae_scaler_scale`, `fcvae_last_point_threshold`,
`fcvae_normal_score_mean/std` (nullable), `fcvae_model_version`, plus
`fcvae_params_source=feast`.

**The version skew guard** (in the scorer): Feast-served params are applied IFF the scaler
pair and threshold are present and finite (scale nonzero) AND `fcvae_model_version` equals
the LIVE model handle's version EXACTLY; otherwise the swap-paired `model_config.json`
values apply. Every scored event stamps `fcvae_params_used = feast | config_fallback`.
Publish updates the model files and Feast non-atomically; version equality pins params to
the exact weights they were fitted against, which is also why a freshly retrained model
automatically ignores stale (or maliciously flipped) Feast rows.

**ModelOp / FeatureOp** (taxi): same patterns one generation earlier: FeatureOp enriches
userdata with 10 Feast features (STRICT: a miss drops the event), ModelOp assembles the
15-feature vector (5 from `data[]` via `EventFeatureIndices`, 10 from userdata; missing =
NaN, which the TreeEnsemble treats as missing), scores, appends the prediction, and stamps
the ML health counters (`feature_miss_*`, `nan_score_*`) the in-app agent aggregates.

## 4. Accuracy metrics: from operator labels to a retrain decision

The full derivation chain (every stage is a file you can inspect):

1. **Labels**: operators drop CSVs (`combo_key,window_end,true_label`; `window_end`
   hour-floored ISO `YYYY-MM-DDTHH:00:00`) into `UploadedFiles/ground_truth/`. The demo
   generates them from the synthetic source's known labels (`make_fcvae_labels.py`
   reproduces the training pipeline's hourly rules exactly: Penny = max(penny_is_anomaly)
   over sub-dollar txns per hour, combos = max(is_anomaly) over the combo's txns; empty
   hours are 0).
2. **Join** (`eval_labels.py`): scored records and labels join on
   `(combo_key, floor_hour(window_end))`; later label files win per key; duplicate scored
   records last-write-wins.
3. **Raw last-point metrics** over the joined pairs:
   $$P = \frac{tp}{tp+fp} \qquad R = \frac{tp}{tp+fn} \qquad F_1 = \frac{2PR}{P+R}$$
   (0 conventions on empty denominators).
4. **Point-adjusted metrics** (the DONUT/FCVAE-paper standard, via the canonical
   `fcvae.metrics.point_adjusted_f1`): if ANY point inside a contiguous ground-truth
   anomaly segment is detected, the WHOLE segment counts detected; false positives are
   flagged points outside all segments. Joined hours are split into consecutive runs with
   an all-negative sentinel between runs, so a segment never spans an unscored gap.
5. **Labels-free anomaly rate**: fraction of flagged windows over the trailing
   `rate_window` (96) HOURS of event time (time-based, so warm-up-contaminated windows
   never pollute it).
6. **eval_metrics.json** (atomic write; top-level `computed_utc`): per combo
   `{scoring: {anomaly_rate, n_windows, ...}, eval: {n_labels, n_joined, coverage,
   gap_count, raw{...}, point_adjusted{...}} | null, baseline: {model_version, pa_f1,
   pa_precision, pa_recall, ...}}` where `baseline` is read from the combo's LIVE published
   manifest on every run (so it self-updates after every retrain).
7. **Signals** (the agent): the P/R/F1 families gate the POINT-ADJUSTED values as floors
   RELATIVE to the published baseline $b$:
   $$\text{FAIL} \iff v < b \cdot \frac{\text{MetricFailBelowBaselinePct}}{100} \qquad
     \text{WARN} \iff v < b \cdot \frac{\text{MetricWarnBelowBaselinePct}}{100}$$
   Relative floors are what keep a low-baseline model (Accel_CMP shipped at PA-F1 0.4595)
   from being born-FAIL under a one-size floor, and they re-anchor automatically when a
   retrain publishes a new manifest. `anomaly_rate` stays an ABSOLUTE ceiling (a spike is
   drift regardless of baseline). Stale/missing eval file, unlabeled combo, missing
   baseline: UNKNOWN with the reason in `detail`.
8. **Retrain condition** (`retrain_trigger.py --metric-signal 'model_f1[Penny_All]'`):
   fires on WARN/FAIL of that signal (`--metric-fire-on`), never on UNKNOWN; with the
   metric condition armed the ops gate is recomputed over PLATFORM signals only (a
   model-quality FAIL turns the verdict RED by design and is the condition, not the gate);
   the degraded combo (from the signal name) is exported to the trainer as
   `MQA_FCVAE_MODEL`. Audit reason: `metric_degradation`.

## 5. The sha-256 model-validity chain

The same 12-hex-prefixed SHA-256 of the `model.onnx` BYTES (`model_version =
"sha256:<12hex>"`) threads through every stage, so "which model is this" is always
answerable and enforceable:

1. **Publish** (`fcvae.publish run`) computes the full `onnx_sha256` + short
   `model_version` and refuses to write anything unless SIX gates pass: artifact presence;
   ONNX signature + runtime load; config sanity (finite threshold/scaler, scale nonzero);
   non-empty golden set; golden-set parity re-verified torch-free (decision agreement
   $= 1.0$, mean$|\Delta\text{nll}| \le 10^{-3}$, max$|\Delta| \le 1.0$; measured FFT
   models have tight mean but large max, which is why the taxi $10^{-4}$ max-abs gate was
   wrong here); metrics.json carries a finite PA-F1. Files then land ATOMICALLY in
   dependency order: manifest, config, golden set, `model.onnx` LAST (the swap trigger).
2. **Swap** (the scorer, per batch): an mtime change triggers a candidate read; identical
   sha short-circuits (a re-copy of the same bytes is not a swap); a differing sha must
   pass the signature + config validation or the candidate is REJECTED and the live session
   kept. Model history holds the last 3 handles; `fcvae.control` `rollback` (or
   `rollback:<sha12>`) restores a prior (weights, scaler, threshold) triple atomically.
3. **Serving-parameter integrity**: Feast rows carry `model_version`; the skew guard
   (section 3) applies them only when they match the live sha exactly.
4. **Monitoring**: each scored event's userdata carries `model_sha256`/`model_version`;
   the manifest's `model_version` + fresh-on-every-publish `created_utc` are what the
   retrain trigger verifies post-fire (a fire whose manifest stamp did not advance is the
   distinct `fired_no_manifest` anomaly, exit 6).
5. **Acceptance**: every harness pins the expected `model_version` before trusting any
   comparison, and the deterministic-decision proofs (bit-identical scores across
   time-shifted feeds) rest on that identity.

## 6. The retrain loop

`striim/retrain/retrain_trigger.py` (host-side, cron-friendly; full option/exit-code
reference in `striim/retrain/README.md`): per evaluation it takes a single-flight lock,
requires a FRESH assessment, applies the ops gate (whole-verdict for taxi; platform-only
when `--metric-signal` is set), applies the cooldown, then fires if ANY condition is due:
schedule (`--interval-sec` vs the manifest's `created_utc`), new data
(`--min-new-events` vs the `events_seen` baseline, with the rebaseline rule so counter
resets can only delay a fire), or metric degradation (section 4.8). Trainer wrappers:

- `run_trainer.sh` -> `mqa-trainer` (taxi: features -> train -> export -> publish).
- `run_fcvae_trainer.sh` -> `mqa-fcvae-trainer` (one FCVAE combo per run:
  `fcvae.train fit` -> `fcvae.onnx_export export` -> `fcvae.publish run` with the same six
  gates; the in-container Feast push is skipped by design, see section 3's skew guard).
  Distinct failure modes: exit 7 docker daemon unreachable, 8 image missing, 2 source CSV
  missing, 3 broken publish contract.

Post-fire the trigger verifies the manifest advanced and audits one JSONL record per
evaluation (outcome, reason, full gates snapshot, model versions before/after).

## 7. File contracts quick reference

| File | Writer | Reader | Shape |
|---|---|---|---|
| `UploadedFiles/fcvae_scored*.json`, `fcvae_accel*.json` | FileWriter (JSONFormatter OPEN arrays) | eval_labels.py, check_fcvae_swap.py | `{combo_key, window_end "%Y/%m/%d %H:%M:%S.%f", is_anomaly, anomaly_score, threshold}` all strings |
| `UploadedFiles/ground_truth/*.csv` | operator / make_fcvae_labels.py | eval_labels.py | `combo_key,window_end,true_label` (hour-floored ISO) |
| `UploadedFiles/fcvae_eval/eval_metrics.json` | eval_labels.py (atomic) | ModelQualityAgent | section 4.6 |
| `UploadedFiles/fcvae_health_assessments*.json` | fcvaemon FileWriter | retrain_trigger.py, checkers | section 2 (10 columns + assessment_json) |
| `/opt/Striim/fcvae-models/<combo>/model.manifest.json` | fcvae.publish | scorer (identity), eval_labels (baseline), trigger (created_utc) | schema_version, model_version, onnx_sha256, created_utc, scaler, thresholds, eval, parity |
| `striim/retrain/state/*.json(l)` | retrain_trigger.py | operators / F5 audits | state + audit JSONL (gitignored) |

External-by-design dependencies: the sibling `fcvae-anomaly-detection` repo (labeled source
CSV + prebuilt checkpoints) and the taxi processed parquet. They are data/model artifacts,
not code; every path to them is env-overridable (`FCVAE_REPO`, `FCVAE_DATA_CSV`,
`MODEL_DATA_PROCESSED`).
