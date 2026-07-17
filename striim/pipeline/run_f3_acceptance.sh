#!/usr/bin/env bash
# run_f3_acceptance.sh -- full F3 acceptance: ground-truth eval + agent metric
# signals, proven live end to end.
#
# NEEDS: STRIIM_ADMIN_PW in the environment. Takes roughly 15 minutes: three
# feeds (baseline, drift, recovery), each waiting up to 300s for the scored
# windows, plus monitor ticks (FCVAE_MON_TICK_SEC=15 here).
#
# What it proves, phase by phase (after preconditions, feast, live reset +
# publish of BOTH prebuilts, pipeline deploy, and the monitor deploy with the
# shared-scm reload):
#   born-UNKNOWN  no eval file -> all four metric families UNKNOWN from the
#                 first tick (never a false PASS).
#   A baseline    TEST-range feed (real labeled anomalies), labels dropped,
#                 eval run: P/R/F1 EXACTLY equal the pinned expectations
#                 (f3_expected_eval.json; deterministic decisions), an
#                 independent stdlib recomputation cross-checks the evaluator,
#                 and the monitor's 8 metric signal instances go non-UNKNOWN
#                 with values matching the eval file, verdict GREEN. This is
#                 also the born-RED fix proof: Accel (baseline PA-F1 0.4595)
#                 PASSes against its OWN published baseline.
#   B staleness   aged computed_utc -> UNKNOWN; absent file -> UNKNOWN;
#                 re-run eval -> recovery. UNKNOWN never counts as PASS.
#   C toggles     EnabledSignals minus model_precision -> family omitted from
#                 signals[] and listed in disabled_signals; restore brings it
#                 back (values still matching the eval file).
#   D drift       Feast-only Penny threshold flip to 0.0 (F2 machinery), feed,
#                 eval: anomaly_rate ~84% FAIL (>= 70 ceiling; see
#                 PENNY_DRIFT_RATE), precision collapses FAIL, verdict RED;
#                 Accel untouched (per-combo isolation, exact pinned equality).
#   E recovery    flip back, feed, eval: metrics EXACTLY equal the Phase A
#                 pins again, verdict GREEN.
#
# Event-time isolation: the TEST base starts 2025-02-25 (source day 51) and
# uses shifts 0/7/14 (step 7 > slice 5, mutually disjoint); the latest F2
# event time is 2025-02-08, so F3 records can never key-collide with F2
# residue even without the wipe (which happens anyway).
#
# PINS: f3_expected_eval.json is generated ONCE at dev time (see
# pin_f3_expected.py; regenerate ONLY when the models or the base slice
# legitimately change). PENNY_RATE / ACCEL_RATE below are the same pinned
# anomaly rates asserted on the raw snapshots.
#
# METRIC KNOBS: this harness runs the monitor with MetricWarnBelowBaselinePct
# 70 (not the shipped default 80). Measured at pin time: Penny's 5-day-slice
# PA precision is 0.7143 vs its full-test baseline 0.9333 (ratio 0.765), a
# healthy model that would WARN at 80% purely from small-slice noise (2 penny
# segments on the slice vs the full 10-day test range). The drift phase
# collapses the ratio to ~0.11, so FAIL at 50% remains decisive.
#
# Overrides: FCVAE_REPO, SNAP_DIR, F3_SKIP_MODULE_RELOAD=1 (re-runs: skip the
# taxi-touching scm reload and do an app-only monitor deploy).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PY_DIR="${REPO_ROOT}/python"
FCVAE_REPO="${FCVAE_REPO:-$(dirname "${REPO_ROOT}")/fcvae-anomaly-detection}"

SLICE_DAYS=5
TEST_START="2025-02-25"        # source day 51: first day of the labeled TEST range
EXPECT_KEYS_PENNY=96           # 5*24 minus 23 warm-up minus the never-closing tail hour
EXPECT_KEYS_ACCEL=94           # ditto minus Accel_CMP's two EMPTY hours (offsets 113, 114)
MODEL_DIR_PENNY="/opt/Striim/fcvae-models/Penny_All"
MODEL_DIR_ACCEL="/opt/Striim/fcvae-models/Accel_CMP"
PRED_DIR="/opt/Striim/UploadedFiles"
FEEDS="${REPO_ROOT}/striim/pipeline/feeds"
SNAP_DIR="${SNAP_DIR:-/tmp/fcvae_f3_snapshots}"
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
PIN_TOOL="${SCRIPT_DIR}/pin_f3_expected.py"
EXPECTED_PINS="${SCRIPT_DIR}/f3_expected_eval.json"
DEPLOY_SH="${SCRIPT_DIR}/deploy_fcvae.sh"
DEPLOY_MON_SH="${SCRIPT_DIR}/deploy_fcvae_monitor.sh"

FEAST_PORT=6567
FEAST_LOG="/tmp/fcvae_feast.log"
FEAST_PID_FILE="/tmp/fcvae_feast.pid"

# ---- pinned constants (dev-time; see the PINS note in the header) ----
PENNY_THRESH="-21.3322"
ACCEL_THRESH="-0.2975213015079498"
PENNY_SHA="sha256:660f8547a307"
ACCEL_SHA="sha256:9adb70b4194c"
TEST_BASE_SHA256="fd42059f3fd0a4bbec4b1c85702bd523c1a205d7d7abff57eb1d3f0e6848e5b7"
PENNY_RATE="0.13541666666666666"   # pinned snapshot anomaly rate, baseline phases
ACCEL_RATE="0.2872340425531915"
# Drift phase (threshold flipped to 0.0): the TEST slice's Penny scores range
# up to +0.479 (the anomaly windows push some NLLs positive), so 15/96 windows
# score >= 0 and stay unflagged: the deterministic drift rate is 0.84375, NOT
# the ~1.0 seen on the all-normal TRAIN slice in F2. Still far above the 70%
# FAIL ceiling, which is what the drift story needs.
PENNY_DRIFT_RATE="0.84375"

# monitor knobs for this harness (fast ticks; WARN ratio 70, see header)
export FCVAE_MON_TICK_SEC="${FCVAE_MON_TICK_SEC:-15}"
export FCVAE_MON_EVAL_MAX_AGE_SEC="${FCVAE_MON_EVAL_MAX_AGE_SEC:-900}"
export FCVAE_MON_METRIC_WARN_PCT="${FCVAE_MON_METRIC_WARN_PCT:-70}"

ALL_METRIC_FAMILIES="model_precision,model_recall,model_f1,anomaly_rate"
ALL_8_INSTANCES="model_precision[Penny_All],model_precision[Accel_CMP],model_recall[Penny_All],model_recall[Accel_CMP],model_f1[Penny_All],model_f1[Accel_CMP],anomaly_rate[Penny_All],anomaly_rate[Accel_CMP]"

if [ -z "${STRIIM_ADMIN_PW:-}" ]; then
  echo "ERROR: STRIIM_ADMIN_PW must be set" >&2
  exit 2
fi

PASS_COUNT=0
FAIL_COUNT=0
FAILED_STEPS=""
BASE_START=""

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
  echo "==== F3 acceptance summary ===="
  echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}"
  if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo "Failed steps:${FAILED_STEPS}"
  else
    echo "All steps passed."
  fi
}

py() { (cd "${PY_DIR}" && uv run python "$@"); }

now_ms() { echo "$(( $(date +%s) * 1000 ))"; }

# ---------------------------------------------------------------------------
# Feast lifecycle (identical to the F2 harness; -sTCP:LISTEN is LOAD-BEARING)
# ---------------------------------------------------------------------------

kill_feast() {
  if [ -f "${FEAST_PID_FILE}" ]; then
    kill "$(cat "${FEAST_PID_FILE}")" 2>/dev/null || true
    rm -f "${FEAST_PID_FILE}"
  fi
  pkill -f 'fcvae.feast_setup serve' 2>/dev/null || true
  pkill -f 'feast -c .*fcvae/feature_repo serve' 2>/dev/null || true
  local pids
  pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "${pids}" ]; then
    kill ${pids} 2>/dev/null || true
  fi
  local deadline
  deadline=$(( $(date +%s) + 10 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
    if [ -z "${pids}" ]; then
      return 0
    fi
    sleep 1
  done
  pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "${pids}" ]; then
    kill -9 ${pids} 2>/dev/null || true
    sleep 1
  fi
  return 0
}

served_threshold() {  # $1 = combo_key
  local resp
  resp="$(curl -s -X POST "http://127.0.0.1:${FEAST_PORT}/get-online-features" \
    -H 'content-type: application/json' \
    -d "{\"features\":[\"fcvae_scoring_params_v1:last_point_threshold\"],\"entities\":{\"combo_key\":[\"$1\"]},\"full_feature_names\":false}")" || return 1
  py - "$resp" <<'PY'
import sys, json
d = json.loads(sys.argv[1])
names = d["metadata"]["feature_names"]
idx = names.index("last_point_threshold")
print(d["results"][idx]["values"][0])
PY
}

verify_served() {  # $1 = combo_key, $2 = expected threshold
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
  local deadline
  deadline=$(( $(date +%s) + 90 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if curl -s -o /dev/null "http://127.0.0.1:${FEAST_PORT}/health"; then
      echo "  feast server answering on ${FEAST_PORT}"
      return 0
    fi
    sleep 2
  done
  echo "  feast server did not answer on ${FEAST_PORT} within 90s (log tail):"
  tail -20 "${FEAST_LOG}" 2>/dev/null || true
  return 1
}

start_feast() {
  kill_feast
  rm -f "${FEAST_LOG}"
  (cd "${PY_DIR}" && nohup uv run python -m fcvae.feast_setup serve --port "${FEAST_PORT}" \
      > "${FEAST_LOG}" 2>&1 & echo $! > "${FEAST_PID_FILE}")
  echo "  feast serve launched (pid $(cat "${FEAST_PID_FILE}"), log ${FEAST_LOG})"
  wait_feast_ready
}

trap kill_feast EXIT

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------

pre_prebuilt_export() {
  if [ -f "${ART_PREBUILT}/model.onnx" ] && [ -f "${ART_PREBUILT}/metrics.json" ]; then
    echo "  penny prebuilt artifacts present, skipping export"
    return 0
  fi
  py -m fcvae.onnx_export export --model Penny_All \
      --model-dir "${FCVAE_REPO}/models/fcvae/Penny_All" \
      --out-dir "${ART_PREBUILT}"
}

pre_accel_prebuilt_export() {
  if [ -f "${ART_ACCEL_PREBUILT}/model.onnx" ] && [ -f "${ART_ACCEL_PREBUILT}/metrics.json" ]; then
    echo "  accel prebuilt artifacts present, skipping export"
    return 0
  fi
  py -m fcvae.onnx_export export --model Accel_CMP \
      --model-dir "${FCVAE_REPO}/models/fcvae/Accel_CMP" \
      --out-dir "${ART_ACCEL_PREBUILT}"
}

pre_test_feed() {
  if [ ! -f "${TEST_FEED}" ]; then
    py "${MAKE_FEED}" prepare --start-date "${TEST_START}" --days "${SLICE_DAYS}" \
        --out "${TEST_FEED}" || return 1
  fi
  # Labels AND decisions both derive from this slice: pin its bytes so source
  # CSV drift is caught here, not as a mystifying metric mismatch downstream.
  local sha
  sha="$(shasum -a 256 "${TEST_FEED}" | cut -d' ' -f1)"
  if [ "${sha}" != "${TEST_BASE_SHA256}" ]; then
    echo "  TEST base sha ${sha} != pinned ${TEST_BASE_SHA256} (source CSV drift?)"
    return 1
  fi
  echo "  TEST base present, sha pinned OK"
}

derive_base_start() {
  BASE_START="$(sed -n '2p' "${TEST_FEED}" | cut -d',' -f1 | cut -c1-10)"
  if [ "${BASE_START}" != "${TEST_START}" ]; then
    echo "  BASE_START='${BASE_START}' != expected ${TEST_START}"
    return 1
  fi
  echo "  BASE_START=${BASE_START}"
}

feast_apply() { py -m fcvae.feast_setup apply; }

# ---------------------------------------------------------------------------
# Live reset + publish + deploys
# ---------------------------------------------------------------------------

live_reset() {
  pkill -f 'eval_labels.py watch' 2>/dev/null || true
  rm -f "${PRED_DIR}"/fcvae_scored* "${PRED_DIR}"/fcvae_accel* \
        "${PRED_DIR}"/fcvae_health_assessments*
  rm -rf "${FEED_DIR}"
  mkdir -p "${FEED_DIR}"
  mkdir -p "${GT_DIR}" "${EVAL_DIR}"
  rm -f "${GT_DIR}"/*.csv "${EVAL_FILE}"
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

assert_model_identity() {
  py - "${MODEL_DIR_PENNY}/model.manifest.json" "${PENNY_SHA}" \
       "${MODEL_DIR_ACCEL}/model.manifest.json" "${ACCEL_SHA}" <<'PY'
import sys, json
ok = True
for path, pinned in ((sys.argv[1], sys.argv[2]), (sys.argv[3], sys.argv[4])):
    v = json.load(open(path))["model_version"]
    print(f"  {path}: model_version={v} pinned={pinned} match={v == pinned}")
    ok = ok and v == pinned
sys.exit(0 if ok else 1)
PY
}

run_deploy() { "${DEPLOY_SH}"; }

run_deploy_monitor() {
  if [ "${F3_SKIP_MODULE_RELOAD:-0}" = "1" ]; then
    echo "  F3_SKIP_MODULE_RELOAD=1: app-only monitor deploy"
    "${DEPLOY_MON_SH}"
  else
    "${DEPLOY_MON_SH}" --reload-module
  fi
}

# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

feed_shift() {  # $1 = shift days
  local shift_days="$1"
  local feed="${FEED_DIR}/penny_feed_s${shift_days}.csv"
  echo "  feeding shift=${shift_days} -> ${feed}"
  py "${MAKE_FEED}" emit --base "${TEST_FEED}" --shift-days "${shift_days}" \
      --out "${feed}.tmp" || return 1
  mv "${feed}.tmp" "${feed}"
}

snap_penny() {  # $1 = shift days, $2 = snapshot file
  py "${CHECK_SWAP}" snapshot --base-start "${BASE_START}" --shift-days "$1" \
      --min-count "${EXPECT_KEYS_PENNY}" --wait-sec 300 --out "$2"
}

snap_accel() {  # $1 = shift days, $2 = snapshot file
  py "${CHECK_SWAP}" snapshot --base-start "${BASE_START}" --shift-days "$1" \
      --prefix fcvae_accel --min-count "${EXPECT_KEYS_ACCEL}" --wait-sec 300 --out "$2"
}

compare_snaps() { py "${CHECK_SWAP}" compare "$1" "$2" --expect "$3"; }

assert_snap() { py "${CHECK_SWAP}" assert-snap "$@"; }

push_params() { py -m fcvae.feast_setup push "$@"; }

drop_labels() {  # $1 = shift days
  rm -f "${GT_DIR}"/*.csv
  py "${MAKE_LABELS}" emit --start-date "${TEST_START}" --days "${SLICE_DAYS}" \
      --shift-days "$1" --out "${GT_DIR}/fcvae_labels_s$1.csv"
}

run_eval() { py "${EVAL_LABELS}" run --out "${EVAL_FILE}"; }

check_signals() { py "${CHECK_SIGNALS}" "$@"; }

pin_compare() { py "${PIN_TOOL}" compare --eval "${EVAL_FILE}" --expected "${EXPECTED_PINS}" "$@"; }

# ---------------------------------------------------------------------------
# Phase steps
# ---------------------------------------------------------------------------

born_unknown() {
  # No eval file exists yet: every metric family must read UNKNOWN (as ONE
  # base-name signal per family, since the whole FILE is unavailable).
  check_signals --after-ms "${MON_DEPLOY_MS}" --wait-sec 90 \
      --expect-unknown-families "${ALL_METRIC_FAMILIES}" \
      --require-coherent-rollup
}

phaseA_pinned_eval() {
  if [ ! -f "${EXPECTED_PINS}" ]; then
    echo "  PINS MISSING: ${EXPECTED_PINS}"
    echo "  dev-time pinning: py ${PIN_TOOL} extract --eval ${EVAL_FILE} --out ${EXPECTED_PINS}"
    return 1
  fi
  pin_compare
}

phaseA_crosscheck() {
  # Independent recomputation: fresh stdlib-only parse + join + raw P/R/F1 +
  # segment detection. No eval_labels import, no fcvae import: agreement means
  # the evaluator's numbers are not self-confirming.
  py - "${PRED_DIR}" "${GT_DIR}" "${EVAL_FILE}" <<'PY'
import sys, glob, os, json, datetime as dt
pred_dir, gt_dir, eval_path = sys.argv[1], sys.argv[2], sys.argv[3]

def records(path):
    dec = json.JSONDecoder()
    text = open(path).read()
    i = 0
    while i < len(text):
        while i < len(text) and text[i] in " \t\r\n,[]":
            i += 1
        if i >= len(text):
            break
        try:
            obj, i = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        yield obj

scored = {}
for prefix in ("fcvae_scored", "fcvae_accel"):
    for path in sorted(glob.glob(os.path.join(pred_dir, prefix + "*.json"))):
        for r in records(path):
            we = str(r.get("window_end", "")).strip()
            try:
                t = dt.datetime.strptime(we, "%Y/%m/%d %H:%M:%S.%f")
            except ValueError:
                try:
                    t = dt.datetime.strptime(we, "%Y/%m/%d %H:%M:%S")
                except ValueError:
                    continue
            hour = t.replace(minute=0, second=0, microsecond=0)
            combo = str(r.get("combo_key", "")).strip()
            dec_flag = str(r.get("is_anomaly", "")).strip().lower() in ("true", "1", "yes", "t")
            scored.setdefault(combo, {})[hour] = dec_flag

labels = {}
import csv
for path in sorted(glob.glob(os.path.join(gt_dir, "*.csv")), key=os.path.basename):
    for row in csv.DictReader(open(path)):
        hour = dt.datetime.fromisoformat(row["window_end"]).replace(minute=0, second=0, microsecond=0)
        labels.setdefault(row["combo_key"], {})[hour] = int(row["true_label"])

ev = json.load(open(eval_path))
ok = True
for combo, truth in sorted(labels.items()):
    dec = scored.get(combo, {})
    joined = sorted(set(dec) & set(truth))
    tp = sum(1 for h in joined if dec[h] and truth[h])
    fp = sum(1 for h in joined if dec[h] and not truth[h])
    fn = sum(1 for h in joined if not dec[h] and truth[h])
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * p * r / (p + r) if p + r else 0.0
    # segment detection over LABEL hours in time order (label set is gapless
    # per combo, so contiguity in label order is contiguity in time)
    hours = sorted(truth)
    segs, cur = [], None
    for h in hours:
        if truth[h]:
            if cur is None:
                cur = [h]
            else:
                cur.append(h)
        elif cur is not None:
            segs.append(cur); cur = None
    if cur is not None:
        segs.append(cur)
    tp_seg = sum(1 for seg in segs if any(dec.get(h, False) for h in seg))
    got = ev["combos"][combo]["eval"]
    exp_raw = {"precision": p, "recall": r, "f1": f1, "tp": tp, "fp": fp, "fn": fn}
    raw_ok = got["raw"] == exp_raw
    seg_ok = (got["point_adjusted"]["tp_segments"] == tp_seg
              and got["point_adjusted"]["total_segments"] == len(segs)
              and got["point_adjusted"]["fn_segments"] == len(segs) - tp_seg)
    join_ok = got["n_joined"] == len(joined)
    print(f"  {combo}: raw={'OK' if raw_ok else 'MISMATCH ' + str((exp_raw, got['raw']))}"
          f" segments={'OK' if seg_ok else 'MISMATCH'} n_joined={'OK' if join_ok else 'MISMATCH'}")
    ok = ok and raw_ok and seg_ok and join_ok
sys.exit(0 if ok else 1)
PY
}

stale_eval() {
  # Age computed_utc past EvalMaxAgeSec (atomic rewrite, same publish rule).
  py - "${EVAL_FILE}" "${FCVAE_MON_EVAL_MAX_AGE_SEC}" <<'PY'
import sys, json, os, tempfile, datetime as dt
path, max_age = sys.argv[1], int(sys.argv[2])
doc = json.load(open(path))
old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=2 * max_age)
doc["computed_utc"] = old.isoformat(timespec="seconds")
fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
with os.fdopen(fd, "w") as f:
    json.dump(doc, f)
os.chmod(tmp, 0o644)
os.replace(tmp, path)
print(f"  computed_utc aged to {doc['computed_utc']}")
PY
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-unknown-families "${ALL_METRIC_FAMILIES}" \
      --require-coherent-rollup
}

absent_eval() {
  rm -f "${EVAL_FILE}"
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-unknown-families "${ALL_METRIC_FAMILIES}" \
      --require-coherent-rollup
}

staleness_recovery() {
  run_eval || return 1
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --eval-file "${EVAL_FILE}" \
      --expect-nonunknown "${ALL_8_INSTANCES}" \
      --require-coherent-rollup
}

toggle_off() {
  local marker
  marker="$(now_ms)"
  FCVAE_MON_ENABLED_SIGNALS="app_status,source_freshness,target_write_age,lag_end2end,backpressure,discarded_events,node_memory,node_cpu,model_recall,model_f1,anomaly_rate" \
      "${DEPLOY_MON_SH}" || return 1
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --expect-absent-families model_precision \
      --expect-nonunknown "model_recall[Penny_All],model_f1[Penny_All],anomaly_rate[Penny_All]" \
      --require-coherent-rollup
}

toggle_restore() {
  local marker
  marker="$(now_ms)"
  "${DEPLOY_MON_SH}" || return 1
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --eval-file "${EVAL_FILE}" \
      --expect-nonunknown "${ALL_8_INSTANCES}" \
      --require-coherent-rollup
}

drift_flip() {
  push_params --model Penny_All --threshold 0.0 || return 1
  verify_served Penny_All 0.0
}

drift_feed_snap() {
  feed_shift 7 || return 1
  snap_penny 7 "${SNAP_DIR}/snapD.json" || return 1
  # served (0.0) != config (-21.3322), so the one-per-96 transient fallback is
  # the designed degradation: pinned rate with tolerance, never exact.
  assert_snap "${SNAP_DIR}/snapD.json" \
      --expect-threshold 0.0 --expect-anomaly-rate "${PENNY_DRIFT_RATE}" --tolerance 0.05
}

drift_accel_isolation() {
  snap_accel 7 "${SNAP_DIR}/snapD_acc.json" || return 1
  # Accel's served value equals its config, so full identity is exact.
  compare_snaps "${SNAP_DIR}/snapA_acc.json" "${SNAP_DIR}/snapD_acc.json" identical
}

drift_eval() {
  drop_labels 7 || return 1
  run_eval || return 1
  # Penny band assertions (transient-tolerant); Accel exactly at its pins.
  drift_penny_bands || return 1
  pin_compare --combos Accel_CMP
}

drift_penny_bands() {
  py - "${EVAL_FILE}" <<'PY'
import sys, json
ev = json.load(open(sys.argv[1]))["combos"]["Penny_All"]
rate = ev["scoring"]["anomaly_rate"]
pa = ev["eval"]["point_adjusted"]
checks = [
    # measured drift rate is 0.84375 (see PENNY_DRIFT_RATE); the band floor
    # sits above the 70% FAIL ceiling with room for the <=5% transient wobble
    ("anomaly_rate >= 0.75", rate >= 0.75),
    ("pa recall == 1.0", pa["recall"] == 1.0),
    ("pa precision <= 0.2", pa["precision"] <= 0.2),
    ("pa f1 <= 0.35", pa["f1"] <= 0.35),
]
ok = True
for name, passed in checks:
    print(f"  Penny_All {name}: {'OK' if passed else 'BAD'}"
          f" (rate={rate:.4f} p={pa['precision']:.4f} r={pa['recall']:.4f} f1={pa['f1']:.4f})")
    ok = ok and passed
sys.exit(0 if ok else 1)
PY
}

drift_signals_red() {
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 90 \
      --eval-file "${EVAL_FILE}" \
      --expect-state "anomaly_rate[Penny_All]=FAIL" \
      --expect-state "model_precision[Penny_All]=FAIL" \
      --expect-state "model_f1[Penny_All]=FAIL" \
      --expect-verdict RED \
      --require-coherent-rollup
}

recover_flip() {
  # push the exact published defaults back
  push_params --model Penny_All || return 1
  verify_served Penny_All "${PENNY_THRESH}"
}

recover_feed_snap() {
  feed_shift 14 || return 1
  snap_penny 14 "${SNAP_DIR}/snapE.json" || return 1
  # served == config again: full identity to the baseline snapshot is exact
  compare_snaps "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snapE.json" identical || return 1
  assert_snap "${SNAP_DIR}/snapE.json" \
      --expect-threshold "${PENNY_THRESH}" --expect-anomaly-rate "${PENNY_RATE}"
}

recover_eval() {
  drop_labels 14 || return 1
  run_eval || return 1
  pin_compare
}

recover_signals_green() {
  local marker
  marker="$(now_ms)"
  check_signals --after-ms "${marker}" --wait-sec 120 \
      --eval-file "${EVAL_FILE}" \
      --expect-nonunknown "${ALL_8_INSTANCES}" \
      --expect-verdict GREEN \
      --require-coherent-rollup
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

rm -rf "${SNAP_DIR}"
mkdir -p "${SNAP_DIR}"
echo "F3 acceptance: SNAP_DIR=${SNAP_DIR}  FCVAE_REPO=${FCVAE_REPO}"
echo "TEST_START=${TEST_START}  EXPECT_KEYS penny=${EXPECT_KEYS_PENNY} accel=${EXPECT_KEYS_ACCEL}"
echo "monitor: tick=${FCVAE_MON_TICK_SEC}s eval_max_age=${FCVAE_MON_EVAL_MAX_AGE_SEC}s"

step "pre-prebuilt-export" pre_prebuilt_export
step "pre-accel-prebuilt-export" pre_accel_prebuilt_export
step "pre-test-feed-sha-pinned" pre_test_feed
step "derive-base-start" derive_base_start

step "feast-apply" feast_apply
step "feast-serve" start_feast

FAILS_BEFORE="${FAIL_COUNT}"
step "live-reset-publish-both" live_reset
step "assert-model-identity" assert_model_identity
step "deploy-pipeline" run_deploy
MON_DEPLOY_MS="$(now_ms)"
step "deploy-monitor" run_deploy_monitor
if [ "${FAIL_COUNT}" -gt "${FAILS_BEFORE}" ]; then
  echo ""
  echo "reset/deploy failed: skipping the live phases (they would only stall on waits)"
  summary
  exit 1
fi

step "born-unknown" born_unknown

# Phase A: baseline eval on the TEST slice
step "A-feed-s0" feed_shift 0
step "A-snap-penny" snap_penny 0 "${SNAP_DIR}/snapA.json"
step "A-assert-penny" assert_snap "${SNAP_DIR}/snapA.json" \
    --expect-threshold "${PENNY_THRESH}" --expect-anomaly-rate "${PENNY_RATE}"
step "A-snap-accel" snap_accel 0 "${SNAP_DIR}/snapA_acc.json"
step "A-assert-accel" assert_snap "${SNAP_DIR}/snapA_acc.json" \
    --expect-threshold "${ACCEL_THRESH}" --expect-anomaly-rate "${ACCEL_RATE}"
step "A-labels-s0" drop_labels 0
step "A-eval-run" run_eval
step "A-eval-pinned" phaseA_pinned_eval
step "A-eval-crosscheck" phaseA_crosscheck
MARKER_A="$(now_ms)"
step "A-signals-green" check_signals --after-ms "${MARKER_A}" --wait-sec 90 \
    --eval-file "${EVAL_FILE}" \
    --expect-nonunknown "${ALL_8_INSTANCES}" \
    --expect-verdict GREEN \
    --require-coherent-rollup

# Phase B: staleness negatives
step "B-stale-unknown" stale_eval
step "B-absent-unknown" absent_eval
step "B-recovery" staleness_recovery

# Phase C: EnabledSignals toggle regression
step "C-toggle-off" toggle_off
step "C-toggle-restore" toggle_restore

# Phase D: drift dress rehearsal (F4 preview)
step "D-drift-flip" drift_flip
step "D-feed-snap" drift_feed_snap
step "D-accel-isolation" drift_accel_isolation
step "D-eval" drift_eval
step "D-signals-red" drift_signals_red

# Phase E: recovery
step "E-recover-flip" recover_flip
step "E-feed-snap" recover_feed_snap
step "E-eval-pinned" recover_eval
step "E-signals-green" recover_signals_green

summary
[ "${FAIL_COUNT}" -eq 0 ]
