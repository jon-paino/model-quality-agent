#!/usr/bin/env bash
# run_f4_acceptance.sh -- F4 acceptance: the metric_degradation retrain
# condition closes the loop, live end to end.
#
# NEEDS: STRIIM_ADMIN_PW; docker running with the mqa-fcvae-trainer image
# buildable (built here if missing); the F3-era ModelQualityAgent module
# LOADED (run run_f3_acceptance.sh once first, or F4_RELOAD_MODULE=1 to run
# the shared-scm reload cycle). Takes roughly 25 minutes: three feeds plus a
# FULL in-container Penny retrain (15 epochs, early stopping, CPU).
#
# The story, step by step:
#   A baseline    TEST-slice feed + labels + eval -> monitor GREEN; the
#                 trigger (metric condition on model_f1[Penny_All]) reads
#                 not_due (exit 3): a healthy model never fires.
#   B unknown     eval file removed -> metric UNKNOWN -> trigger not_due
#                 (exit 3): UNKNOWN NEVER fires; eval re-run restores.
#   C drift       F2/F3 Feast flip (Penny threshold 0.0) + feed + eval ->
#                 model_f1[Penny_All] FAIL, verdict RED.
#   E fire        trigger check -> exit 0: audit reason=metric_degradation,
#                 the container retrains EXACTLY Penny (Accel manifest
#                 byte-unchanged), publish's six gates pass, Penny's
#                 model_version changes and created_utc advances. The
#                 retrained version also invalidates the poisoned Feast row
#                 via the scorer's skew guard, healing the flip by
#                 construction.
#   F recovery    host re-pushes fresh Feast params for the new version;
#                 feed + snapshot proves the swap (scores differ from
#                 baseline, threshold = the retrained manifest's fitted
#                 threshold); labels + eval -> the baseline block
#                 self-updates to the retrained manifest, metric signals
#                 judged non-FAIL against the model's OWN accepted baseline,
#                 verdict leaves RED; trigger reads not_due again.
#   G ops block   the scored app is STOPPED -> app_status FAIL -> the
#                 platform-only ops gate refuses (exit 2) even with the
#                 metric machinery armed; app restarted after.
#   H restore     prebuilt Penny re-published (+Feast push), cluster back in
#                 the F3 state.
#   Plus: wrapper error-handling units (docker daemon down -> exit 7, image
#   missing -> exit 8; the manager-requested failure modes) and a taxi
#   regression (trigger WITHOUT --metric-signal behaves exactly as Week 4:
#   no metric keys in the audit gates).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PY_DIR="${REPO_ROOT}/python"
FCVAE_REPO="${FCVAE_REPO:-$(dirname "${REPO_ROOT}")/fcvae-anomaly-detection}"
RETRAIN_DIR="${REPO_ROOT}/striim/retrain"

SLICE_DAYS=5
TEST_START="2025-02-25"
EXPECT_KEYS_PENNY=96
MODEL_DIR_PENNY="/opt/Striim/fcvae-models/Penny_All"
MODEL_DIR_ACCEL="/opt/Striim/fcvae-models/Accel_CMP"
PRED_DIR="/opt/Striim/UploadedFiles"
FEEDS="${REPO_ROOT}/striim/pipeline/feeds"
SNAP_DIR="${SNAP_DIR:-/tmp/fcvae_f4_snapshots}"
FEED_DIR="/tmp/fcvae_swap_test"
GT_DIR="/opt/Striim/UploadedFiles/ground_truth"
EVAL_DIR="/opt/Striim/UploadedFiles/fcvae_eval"
EVAL_FILE="${EVAL_DIR}/eval_metrics.json"
TEST_FEED="${FEEDS}/penny_test_5d.csv"
ART_PREBUILT="${PY_DIR}/fcvae/artifacts/Penny_All_prebuilt"
ART_ACCEL_PREBUILT="${PY_DIR}/fcvae/artifacts/Accel_CMP_prebuilt"
MAKE_FEED="${SCRIPT_DIR}/make_fcvae_feed.py"
MAKE_LABELS="${SCRIPT_DIR}/make_fcvae_labels.py"
EVAL_LABELS="${SCRIPT_DIR}/eval_labels.py"
CHECK_SWAP="${SCRIPT_DIR}/check_fcvae_swap.py"
CHECK_SIGNALS="${SCRIPT_DIR}/check_fcvae_eval_signals.py"
DEPLOY_SH="${SCRIPT_DIR}/deploy_fcvae.sh"
DEPLOY_MON_SH="${SCRIPT_DIR}/deploy_fcvae_monitor.sh"
TRIGGER="${RETRAIN_DIR}/retrain_trigger.py"
FCVAE_TRAINER="${RETRAIN_DIR}/run_fcvae_trainer.sh"

FEAST_PORT=6567
FEAST_LOG="/tmp/fcvae_feast.log"
FEAST_PID_FILE="/tmp/fcvae_feast.pid"

PENNY_THRESH="-21.3322"
ACCEL_THRESH="-0.2975213015079498"
PENNY_SHA="sha256:660f8547a307"
PENNY_RATE="0.13541666666666666"

# F4 trigger wiring: fcvae-named state files (never the taxi trigger's), the
# fcvae monitor's assessment prefix, the degraded-model manifest, and the
# fcvae trainer wrapper. Cooldown is tiny so consecutive drill checks are
# never cooldown-blocked; conditions other than the metric stay disabled.
TRIG_STATE="${RETRAIN_DIR}/state/fcvae_trigger_state.json"
TRIG_LOG="${RETRAIN_DIR}/state/fcvae_trigger_log.jsonl"
TRIG_LOCK="${RETRAIN_DIR}/state/fcvae_trigger.lock"
METRIC_SIGNAL="model_f1[Penny_All]"
trigger() {
  MQA_TRAINER_NAME=mqa-fcvae-trainer-run \
  python3 "${TRIGGER}" check \
      --health-dir "${PRED_DIR}" --prefix fcvae_health_assessments \
      --manifest "${MODEL_DIR_PENNY}/model.manifest.json" \
      --metric-signal "${METRIC_SIGNAL}" \
      --trainer "${FCVAE_TRAINER}" --trainer-timeout-sec 2400 \
      --state "${TRIG_STATE}" --log "${TRIG_LOG}" --lock "${TRIG_LOCK}" \
      --cooldown-sec 5 "$@"
}

export FCVAE_MON_TICK_SEC="${FCVAE_MON_TICK_SEC:-15}"
export FCVAE_MON_METRIC_WARN_PCT="${FCVAE_MON_METRIC_WARN_PCT:-70}"

if [ -z "${STRIIM_ADMIN_PW:-}" ]; then
  echo "ERROR: STRIIM_ADMIN_PW must be set" >&2
  exit 2
fi

PASS_COUNT=0
FAIL_COUNT=0
FAILED_STEPS=""

step() {
  local name="$1"; shift
  echo ""
  echo "STEP: ${name}"
  if "$@"; then
    echo "PASS: ${name}"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "FAIL: ${name}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_STEPS="${FAILED_STEPS} ${name}"
  fi
}

summary() {
  echo ""
  echo "==== F4 acceptance summary ===="
  echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}"
  if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo "Failed steps:${FAILED_STEPS}"
  else
    echo "All steps passed."
  fi
}

py() { (cd "${PY_DIR}" && uv run python "$@"); }
now_ms() { echo "$(( $(date +%s) * 1000 ))"; }

# ---- Feast lifecycle (identical to F2/F3; -sTCP:LISTEN is LOAD-BEARING) ----
kill_feast() {
  if [ -f "${FEAST_PID_FILE}" ]; then
    kill "$(cat "${FEAST_PID_FILE}")" 2>/dev/null || true
    rm -f "${FEAST_PID_FILE}"
  fi
  pkill -f 'fcvae.feast_setup serve' 2>/dev/null || true
  pkill -f 'feast -c .*fcvae/feature_repo serve' 2>/dev/null || true
  local pids
  pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  [ -n "${pids}" ] && kill ${pids} 2>/dev/null
  local deadline=$(( $(date +%s) + 10 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
    [ -z "${pids}" ] && return 0
    sleep 1
  done
  pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  [ -n "${pids}" ] && kill -9 ${pids} 2>/dev/null
  return 0
}

served_threshold() {
  local resp
  resp="$(curl -s -X POST "http://127.0.0.1:${FEAST_PORT}/get-online-features" \
    -H 'content-type: application/json' \
    -d "{\"features\":[\"fcvae_scoring_params_v1:last_point_threshold\"],\"entities\":{\"combo_key\":[\"$1\"]},\"full_feature_names\":false}")" || return 1
  py - "$resp" <<'PY'
import sys, json
d = json.loads(sys.argv[1])
idx = d["metadata"]["feature_names"].index("last_point_threshold")
print(d["results"][idx]["values"][0])
PY
}

verify_served() {
  local got
  got="$(served_threshold "$1")" || { echo "  served_threshold read failed"; return 1; }
  py - "$got" "$2" <<'PY'
import sys
got, exp = float(sys.argv[1]), float(sys.argv[2])
ok = abs(got - exp) < 1e-9
print(f"  feast serves threshold={got} expected={exp} match={ok}")
sys.exit(0 if ok else 1)
PY
}

wait_feast_ready() {
  local deadline=$(( $(date +%s) + 90 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    curl -s -o /dev/null "http://127.0.0.1:${FEAST_PORT}/health" && { echo "  feast up"; return 0; }
    sleep 2
  done
  echo "  feast server did not answer within 90s"; tail -20 "${FEAST_LOG}" 2>/dev/null
  return 1
}

start_feast() {
  kill_feast
  rm -f "${FEAST_LOG}"
  (cd "${PY_DIR}" && nohup uv run python -m fcvae.feast_setup serve --port "${FEAST_PORT}" \
      > "${FEAST_LOG}" 2>&1 & echo $! > "${FEAST_PID_FILE}")
  wait_feast_ready
}

trap kill_feast EXIT

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------

pre_image() {
  if docker image inspect mqa-fcvae-trainer > /dev/null 2>&1; then
    echo "  mqa-fcvae-trainer image present"
    return 0
  fi
  echo "  building mqa-fcvae-trainer (first run; torch pull takes a while)"
  (cd "${PY_DIR}" && docker build -f Dockerfile.fcvae -t mqa-fcvae-trainer .)
}

pre_prebuilt_export() {
  [ -f "${ART_PREBUILT}/model.onnx" ] && [ -f "${ART_PREBUILT}/metrics.json" ] && return 0
  py -m fcvae.onnx_export export --model Penny_All \
      --model-dir "${FCVAE_REPO}/models/fcvae/Penny_All" --out-dir "${ART_PREBUILT}"
}

pre_accel_prebuilt_export() {
  [ -f "${ART_ACCEL_PREBUILT}/model.onnx" ] && [ -f "${ART_ACCEL_PREBUILT}/metrics.json" ] && return 0
  py -m fcvae.onnx_export export --model Accel_CMP \
      --model-dir "${FCVAE_REPO}/models/fcvae/Accel_CMP" --out-dir "${ART_ACCEL_PREBUILT}"
}

pre_test_feed() {
  if [ ! -f "${TEST_FEED}" ]; then
    py "${MAKE_FEED}" prepare --start-date "${TEST_START}" --days "${SLICE_DAYS}" \
        --out "${TEST_FEED}" || return 1
  fi
  [ -f "${TEST_FEED}" ]
}

wrapper_docker_down() {
  MQA_DOCKER=/usr/bin/false "${FCVAE_TRAINER}"; [ $? -eq 7 ]
}

wrapper_image_missing() {
  MQA_FCVAE_IMAGE=definitely-not-built "${FCVAE_TRAINER}"; [ $? -eq 8 ]
}

live_reset() {
  pkill -f 'eval_labels.py watch' 2>/dev/null || true
  rm -f "${PRED_DIR}"/fcvae_scored* "${PRED_DIR}"/fcvae_accel* \
        "${PRED_DIR}"/fcvae_health_assessments*
  rm -rf "${FEED_DIR}"
  mkdir -p "${FEED_DIR}" "${GT_DIR}" "${EVAL_DIR}"
  rm -f "${GT_DIR}"/*.csv "${EVAL_FILE}"
  rm -f "${TRIG_STATE}" "${TRIG_LOG}" "${TRIG_LOCK}"
  local mdir
  for mdir in "${MODEL_DIR_PENNY}" "${MODEL_DIR_ACCEL}"; do
    mkdir -p "${mdir}"
    rm -f "${mdir}/model.onnx.data" "${mdir}/fcvae.control"
  done
  py -m fcvae.publish run --model Penny_All --artifacts "${ART_PREBUILT}" \
      --out "${MODEL_DIR_PENNY}" --require-feast || return 1
  py -m fcvae.publish run --model Accel_CMP --artifacts "${ART_ACCEL_PREBUILT}" \
      --out "${MODEL_DIR_ACCEL}" --require-feast || return 1
  verify_served Penny_All "${PENNY_THRESH}" || return 1
  verify_served Accel_CMP "${ACCEL_THRESH}"
}

run_deploy() { "${DEPLOY_SH}"; }

run_deploy_monitor() {
  if [ "${F4_RELOAD_MODULE:-0}" = "1" ]; then
    "${DEPLOY_MON_SH}" --reload-module
  else
    "${DEPLOY_MON_SH}"
  fi
}

# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

feed_shift() {
  local shift_days="$1"
  local feed="${FEED_DIR}/penny_feed_s${shift_days}.csv"
  echo "  feeding shift=${shift_days} -> ${feed}"
  py "${MAKE_FEED}" emit --base "${TEST_FEED}" --shift-days "${shift_days}" \
      --out "${feed}.tmp" || return 1
  mv "${feed}.tmp" "${feed}"
}

snap_penny() {
  py "${CHECK_SWAP}" snapshot --base-start "${TEST_START}" --shift-days "$1" \
      --min-count "${EXPECT_KEYS_PENNY}" --wait-sec 300 --out "$2"
}

drop_labels() {
  rm -f "${GT_DIR}"/*.csv
  py "${MAKE_LABELS}" emit --start-date "${TEST_START}" --days "${SLICE_DAYS}" \
      --shift-days "$1" --out "${GT_DIR}/fcvae_labels_s$1.csv"
}

run_eval() { py "${EVAL_LABELS}" run --out "${EVAL_FILE}"; }

check_signals() { py "${CHECK_SIGNALS}" "$@"; }

push_params() { py -m fcvae.feast_setup push "$@"; }

# Assert the trigger's exit code and the last audit record's outcome (and
# optionally reason / metric_state): the audit log is the durable behavioral
# record, never stdout. Transient PLATFORM refusals (exit 2) are retried for
# up to 4 minutes: scoring bursts legitimately peg node_cpu_pct to FAIL for a
# tick or two, and the platform-only ops gate is SUPPOSED to refuse during
# them; the drill's claim is about the settled state, so it polls through the
# wobble instead of sampling one instant. A refusal that persists past the
# deadline still fails the step.
assert_trigger() {  # $1 expected exit, $2 expected outcome, [$3 expected reason], [$4 expected metric_state]
  local want_rc="$1" want_outcome="$2" want_reason="${3:-}" want_state="${4:-}"
  local deadline=$(( $(date +%s) + 240 ))
  local rc
  while true; do
    trigger
    rc=$?
    if [ "${rc}" -eq "${want_rc}" ]; then
      break
    fi
    if [ "${rc}" -eq 2 ] && [ "${want_rc}" -ne 2 ] && [ "$(date +%s)" -lt "${deadline}" ]; then
      echo "  transient platform refusal (exit 2); retrying in 15s"
      sleep 15
      continue
    fi
    echo "  trigger exit ${rc}, expected ${want_rc}"
    return 1
  done
  python3 - "${TRIG_LOG}" "${want_outcome}" "${want_reason}" "${want_state}" <<'PY'
import sys, json
path, outcome, reason, state = sys.argv[1:5]
last = None
with open(path) as f:
    for line in f:
        line = line.strip()
        if line:
            last = json.loads(line)
ok = last is not None and last.get("outcome") == outcome
msgs = [f"outcome={last.get('outcome') if last else None} (want {outcome})"]
if ok and reason:
    ok = last.get("reason") == reason
    msgs.append(f"reason={last.get('reason')} (want {reason})")
if ok and state:
    ok = last.get("gates", {}).get("metric_state") == state
    msgs.append(f"metric_state={last.get('gates', {}).get('metric_state')} (want {state})")
print("  audit: " + "; ".join(msgs))
sys.exit(0 if ok else 1)
PY
}

manifest_field() {  # $1 manifest, $2 field
  python3 -c "import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$1" "$2"
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

phaseA_baseline() {
  feed_shift 0 || return 1
  snap_penny 0 "${SNAP_DIR}/snapA.json" || return 1
  py "${CHECK_SWAP}" assert-snap "${SNAP_DIR}/snapA.json" \
      --expect-threshold "${PENNY_THRESH}" --expect-anomaly-rate "${PENNY_RATE}" || return 1
  drop_labels 0 || return 1
  run_eval || return 1
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-state "${METRIC_SIGNAL}=PASS" --require-coherent-rollup || return 1
  assert_trigger 3 not_due "" PASS
}

phaseB_unknown_never_fires() {
  rm -f "${EVAL_FILE}"
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-unknown-families model_f1 --require-coherent-rollup || return 1
  assert_trigger 3 not_due "" UNKNOWN || return 1
  run_eval
}

phaseC_drift_red() {
  push_params --model Penny_All --threshold 0.0 || return 1
  verify_served Penny_All 0.0 || return 1
  feed_shift 7 || return 1
  snap_penny 7 "${SNAP_DIR}/snapC.json" || return 1
  drop_labels 7 || return 1
  run_eval || return 1
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-state "${METRIC_SIGNAL}=FAIL" --expect-verdict RED \
      --require-coherent-rollup
}

ACCEL_SHA_BEFORE=""
phaseE_fire() {
  ACCEL_SHA_BEFORE="$(shasum "${MODEL_DIR_ACCEL}/model.manifest.json" | cut -d' ' -f1)"
  local version_before created_before
  version_before="$(manifest_field "${MODEL_DIR_PENNY}/model.manifest.json" model_version)"
  created_before="$(manifest_field "${MODEL_DIR_PENNY}/model.manifest.json" created_utc)"
  echo "  before: ${version_before} created ${created_before}"

  # Same transient-platform-refusal retry as assert_trigger: a scoring burst
  # can peg node_cpu_pct to FAIL for a tick; the fire claim is about the
  # settled state.
  local deadline=$(( $(date +%s) + 240 ))
  local rc
  while true; do
    trigger
    rc=$?
    [ "${rc}" -eq 0 ] && break
    if [ "${rc}" -eq 2 ] && [ "$(date +%s)" -lt "${deadline}" ]; then
      echo "  transient platform refusal (exit 2); retrying in 15s"
      sleep 15
      continue
    fi
    echo "  trigger exit ${rc}, expected 0 (fire + publish)"
    return 1
  done

  # Audit: fired_published with reason metric_degradation.
  python3 - "${TRIG_LOG}" <<'PY' || return 1
import sys, json
last = None
with open(sys.argv[1]) as f:
    for line in f:
        if line.strip():
            last = json.loads(line)
ok = (last.get("outcome") == "fired_published"
      and last.get("reason") == "metric_degradation"
      and last.get("model_version_after") not in (None, last.get("model_version_before")))
print(f"  audit: outcome={last.get('outcome')} reason={last.get('reason')} "
      f"{last.get('model_version_before')} -> {last.get('model_version_after')}")
sys.exit(0 if ok else 1)
PY

  # Behavioral combo isolation: Penny's manifest changed, Accel's is
  # byte-identical (the degraded combo and ONLY it retrained).
  local version_after created_after accel_after
  version_after="$(manifest_field "${MODEL_DIR_PENNY}/model.manifest.json" model_version)"
  created_after="$(manifest_field "${MODEL_DIR_PENNY}/model.manifest.json" created_utc)"
  accel_after="$(shasum "${MODEL_DIR_ACCEL}/model.manifest.json" | cut -d' ' -f1)"
  echo "  after:  ${version_after} created ${created_after}"
  [ "${version_after}" != "${version_before}" ] || { echo "  penny version unchanged"; return 1; }
  [ "${created_after}" != "${created_before}" ] || { echo "  penny created_utc unchanged"; return 1; }
  [ "${accel_after}" = "${ACCEL_SHA_BEFORE}" ] || { echo "  ACCEL manifest changed"; return 1; }
  echo "  combo isolation: Accel manifest untouched"
}

NEW_THRESH=""
phaseF_recovery() {
  # Re-engage the Feast path for the retrained version (in-container push is
  # skipped by design; this mirrors what publish does when Feast is local).
  push_params --model Penny_All --published-dir "${MODEL_DIR_PENNY}" || return 1
  NEW_THRESH="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['thresholds']['last_point_threshold'])" \
      "${MODEL_DIR_PENNY}/model_config.json")"
  echo "  retrained threshold: ${NEW_THRESH}"
  verify_served Penny_All "${NEW_THRESH}" || return 1

  feed_shift 14 || return 1
  snap_penny 14 "${SNAP_DIR}/snapF.json" || return 1
  # Swap proof: a different model scores differently; the live threshold is
  # the retrained fitted one (tolerance for the designed per-event fallback,
  # which lands on the SAME swap-paired value).
  py "${CHECK_SWAP}" compare "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snapF.json" \
      --scores-only --expect different || return 1
  py "${CHECK_SWAP}" assert-snap "${SNAP_DIR}/snapF.json" \
      --expect-threshold "${NEW_THRESH}" || return 1

  drop_labels 14 || return 1
  run_eval || return 1
  # The eval baseline self-updated to the retrained manifest: the new model is
  # judged against its OWN accepted numbers, so nothing may read FAIL and the
  # verdict must leave RED. (Values are not pinned: full torch training is not
  # bit-reproducible.)
  python3 - "${EVAL_FILE}" <<'PY' || return 1
import sys, json
doc = json.load(open(sys.argv[1]))
p = doc["combos"]["Penny_All"]
base = p["baseline"]
ok = base and base.get("pa_f1") is not None
print(f"  eval baseline now {base.get('model_version')} pa_f1={base.get('pa_f1')}")
print(f"  slice pa_f1={p['eval']['point_adjusted']['f1']:.4f} "
      f"anomaly_rate={p['scoring']['anomaly_rate']:.4f}")
sys.exit(0 if ok else 1)
PY
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-nonunknown "${METRIC_SIGNAL},anomaly_rate[Penny_All]" \
      --require-coherent-rollup
}

phaseF_no_fail_states() {
  local marker
  marker="$(now_ms)"
  py "${CHECK_SIGNALS}" --after-ms "${marker}" --wait-sec 90 \
      --require-coherent-rollup || return 1
  # Recovery claim, polled for the SETTLED state (a scoring burst can peg
  # node_cpu_pct FAIL for a tick, turning the verdict RED for reasons that
  # have nothing to do with model quality): within the deadline there must be
  # a tick where NO metric-family signal FAILs AND the verdict is not RED.
  python3 - "${PRED_DIR}" "${marker}" <<'PY'
import sys, glob, json, os, time
pred_dir, after = sys.argv[1], int(sys.argv[2])
FAMS = ("model_precision", "model_recall", "model_f1", "anomaly_rate")
def iter_records(text):
    dec = json.JSONDecoder(); i = 0
    while i < len(text):
        while i < len(text) and text[i] in " \t\r\n,[]": i += 1
        if i >= len(text): break
        try: obj, i = dec.raw_decode(text, i)
        except json.JSONDecodeError: break
        yield obj
deadline = time.time() + 180
last = None
while time.time() < deadline:
    best_ts, best = None, None
    for path in sorted(glob.glob(os.path.join(pred_dir, "fcvae_health_assessments*.json"))):
        for r in iter_records(open(path).read()):
            try: ts = int(str(r.get("tick_ts")).strip())
            except (TypeError, ValueError): continue
            if ts > after and (best_ts is None or ts >= best_ts):
                best_ts, best = ts, r
    if best is not None:
        a = json.loads(best["assessment_json"])
        metric_fails = [s["name"] for s in a["signals"]
                        if s["name"].split("[")[0] in FAMS and s["state"] == "FAIL"]
        penny = {s["name"]: s["state"] for s in a["signals"]
                 if s["name"] in ("model_f1[Penny_All]", "anomaly_rate[Penny_All]")}
        last = f"verdict={a['verdict']} penny={penny} metric_fails={metric_fails}"
        if not metric_fails and a["verdict"] != "RED":
            print(f"  settled: {last}")
            sys.exit(0)
    time.sleep(5)
print(f"  never settled within 180s: {last}")
sys.exit(1)
PY
}

phaseF_trigger_not_due() {
  assert_trigger 3 not_due
}

phaseG_ops_block() {
  # Induce a genuine PLATFORM FAIL without touching the scored app: redeploy
  # the monitor with a 2-second source-freshness FAIL ceiling; the pipeline
  # has been idle since the last feed, so source_freshness[TxnFileSource]
  # reads FAIL on the first tick. The platform-only ops gate must refuse
  # (exit 2) even though the metric machinery is armed. (A STOPped app would
  # NOT work here: STOPPED is not in FailStatuses, so app_status only WARNs.)
  local marker rc=0
  marker="$(now_ms)"
  FCVAE_MON_SRC_WARN_SEC=1 FCVAE_MON_SRC_FAIL_SEC=2 "${DEPLOY_MON_SH}" > /dev/null || return 1
  check_signals --after-ms "${marker}" --wait-sec 120 \
      --expect-state "source_freshness[fcvaedemo.TxnFileSource]=FAIL" || rc=1
  if [ "${rc}" -eq 0 ]; then
    assert_trigger 2 refused_ops || rc=1
  fi
  # restore the normal freshness policy
  marker="$(now_ms)"
  "${DEPLOY_MON_SH}" > /dev/null || return 1
  check_signals --after-ms "${marker}" --wait-sec 120 --require-coherent-rollup || rc=1
  return ${rc}
}

phaseH_restore_prebuilt() {
  py -m fcvae.publish run --model Penny_All --artifacts "${ART_PREBUILT}" \
      --out "${MODEL_DIR_PENNY}" --require-feast || return 1
  verify_served Penny_All "${PENNY_THRESH}" || return 1
  local v
  v="$(manifest_field "${MODEL_DIR_PENNY}/model.manifest.json" model_version)"
  [ "${v}" = "${PENNY_SHA}" ] && echo "  prebuilt restored (${v})"
}

taxi_regression() {
  # No --metric-signal: the audit gates must carry NO metric keys and the ops
  # gate must be the legacy whole-assessment form (Week 4 behavior).
  local tlog=/tmp/f4_taxi_trigger_log.jsonl
  rm -f "${tlog}" /tmp/f4_taxi_state.json /tmp/f4_taxi.lock
  python3 "${TRIGGER}" check \
      --state /tmp/f4_taxi_state.json --log "${tlog}" --lock /tmp/f4_taxi.lock \
      --cooldown-sec 5
  local rc=$?
  case "${rc}" in 2|3) ;; *) echo "  taxi trigger exit ${rc}, expected 2 or 3"; return 1 ;; esac
  python3 - "${tlog}" <<'PY'
import sys, json
last = None
with open(sys.argv[1]) as f:
    for line in f:
        if line.strip():
            last = json.loads(line)
gates = last.get("gates", {})
metric_keys = [k for k in gates if k.startswith("metric") or k.startswith("ops_gate")]
print(f"  outcome={last.get('outcome')} metric/platform gate keys present: {metric_keys}")
sys.exit(0 if not metric_keys else 1)
PY
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

rm -rf "${SNAP_DIR}"
mkdir -p "${SNAP_DIR}"
echo "F4 acceptance: SNAP_DIR=${SNAP_DIR}  metric signal=${METRIC_SIGNAL}"
echo "trainer=${FCVAE_TRAINER} (full train; timeout 2400s)"

step "pre-image" pre_image
step "pre-prebuilt-export" pre_prebuilt_export
step "pre-accel-prebuilt-export" pre_accel_prebuilt_export
step "pre-test-feed" pre_test_feed
step "wrapper-docker-down-exit7" wrapper_docker_down
step "wrapper-image-missing-exit8" wrapper_image_missing
step "feast-apply" py -m fcvae.feast_setup apply
step "feast-serve" start_feast

FAILS_BEFORE="${FAIL_COUNT}"
step "live-reset-publish-both" live_reset
step "deploy-pipeline" run_deploy
step "deploy-monitor" run_deploy_monitor
if [ "${FAIL_COUNT}" -gt "${FAILS_BEFORE}" ]; then
  echo ""
  echo "reset/deploy failed: skipping the live phases"
  summary
  exit 1
fi

step "A-baseline-green-not-due" phaseA_baseline
step "B-unknown-never-fires" phaseB_unknown_never_fires
step "C-drift-metric-fail-red" phaseC_drift_red
step "E-fire-metric-degradation" phaseE_fire
step "F-feast-repush-swap-proof" phaseF_recovery
step "F-metrics-recovered-no-fail" phaseF_no_fail_states
step "F-trigger-not-due-again" phaseF_trigger_not_due
step "G-platform-red-blocks" phaseG_ops_block
step "H-restore-prebuilt" phaseH_restore_prebuilt
step "taxi-trigger-regression" taxi_regression

summary
[ "${FAIL_COUNT}" -eq 0 ]
