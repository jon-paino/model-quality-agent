#!/usr/bin/env bash
# run_f2_acceptance.sh -- full F2 acceptance for the FCVAE Feast params path.
#
# NEEDS: STRIIM_ADMIN_PW in the environment (deploy_fcvae.sh uses it for REST
# auth and console.sh). Takes roughly 15 minutes: it feeds the pipeline FIVE
# times (one feed per phase, both chains score every feed) and waits up to
# 300s for the 96 scored windows of each chain per feed.
#
# This harness OWNS the second Feast instance on 127.0.0.1:6567: it kills any
# stale listener at start, launches `python -m fcvae.feast_setup serve` under
# nohup (pidfile /tmp/fcvae_feast.pid, log /tmp/fcvae_feast.log), kills it for
# the feast-down phase, restarts it for the restore phase, and ALWAYS kills it
# on EXIT.
#
# Phase plan (after preconditions, feast apply + serve, live reset + publish
# of BOTH prebuilt models, and deploy):
#   1. shift 0  : baselines. Penny snapA: threshold -21.3322, anomaly rate 0.
#                 Accel snapA_acc: threshold -0.2975213015079498 (its anomaly
#                 rate is unknown, so not asserted).
#   2. shift 7  : Feast-only flip (push Penny_All threshold 0.0, real model
#                 version). Penny scores stay bit-identical to snapA while the
#                 threshold reads 0.0 and the anomaly rate flips to 1.0: the
#                 decision params moved WITHOUT touching the model. Accel is
#                 FULLY identical to snapA_acc (per-combo isolation).
#   3. shift 14 : version guard (push threshold 0.0 with model-version
#                 sha256:deadbeefdead). Penny falls back to the swap-paired
#                 model_config: threshold -21.3322, rate 0, scores identical,
#                 despite Feast being up and poisoned.
#   4. shift 21 : Feast down (re-arm a good-version threshold 0.0 push, THEN
#                 kill the server). Fallback again beats a down-but-armed
#                 Feast: threshold -21.3322, rate 0, scores identical.
#   5. shift 28 : restore (restart the server, push threshold -21.0, a value
#                 distinguishable from BOTH 0.0 and the -21.3322 default;
#                 decisions unchanged since scores max out around -0.23).
#                 Scores identical, threshold -21.0, rate 0: the Feast path
#                 re-engaged. Finally push the exact defaults back (no feed).
#
# Any model copies here use PLAIN cp (never cp -p): the destination mtime must
# ADVANCE for the scorer's mtime gate to notice a changed file. (F2 publishes
# via fcvae.publish, which honors the same rule internally.)
#
# Overrides: FCVAE_REPO (sibling fcvae-anomaly-detection checkout), SNAP_DIR.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PY_DIR="${REPO_ROOT}/python"
FCVAE_REPO="${FCVAE_REPO:-$(dirname "${REPO_ROOT}")/fcvae-anomaly-detection}"

SLICE_DAYS=5                 # the base feed spans 5 days of hourly data
EXPECT_KEYS=96               # scored windows per chain per feed (SLICE_DAYS*24 minus 24h warm-up)
MODEL_DIR_PENNY="/opt/Striim/fcvae-models/Penny_All"
MODEL_DIR_ACCEL="/opt/Striim/fcvae-models/Accel_CMP"
PRED_DIR="/opt/Striim/UploadedFiles"
FEEDS="${REPO_ROOT}/striim/pipeline/feeds"
SNAP_DIR="${SNAP_DIR:-/tmp/fcvae_f2_snapshots}"
FEED_DIR="/tmp/fcvae_swap_test"
BASE_FEED="${FEEDS}/penny_base_5d.csv"
ART_TOY="${PY_DIR}/fcvae/artifacts/Penny_All"
ART_PREBUILT="${PY_DIR}/fcvae/artifacts/Penny_All_prebuilt"
ART_ACCEL_PREBUILT="${PY_DIR}/fcvae/artifacts/Accel_CMP_prebuilt"
MAKE_FEED="${REPO_ROOT}/striim/pipeline/make_fcvae_feed.py"
CHECK_SWAP="${REPO_ROOT}/striim/pipeline/check_fcvae_swap.py"
DEPLOY_SH="${REPO_ROOT}/striim/pipeline/deploy_fcvae.sh"

# The second Feast instance (this harness owns its lifecycle)
FEAST_PORT=6567
FEAST_LOG="/tmp/fcvae_feast.log"
FEAST_PID_FILE="/tmp/fcvae_feast.pid"

# Prebuilt expectations (pinned): the Penny_All last_point_threshold and the
# Accel_CMP last_point_threshold baked into model_config.json by publish.
PENNY_THRESH="-21.3322"
ACCEL_THRESH="-0.2975213015079498"

if [ -z "${STRIIM_ADMIN_PW:-}" ]; then
  echo "ERROR: STRIIM_ADMIN_PW must be set (deploy_fcvae.sh needs it)" >&2
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
  echo "==== F2 acceptance summary ===="
  echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}"
  if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo "Failed steps:${FAILED_STEPS}"
  else
    echo "All steps passed."
  fi
}

# All repo python runs through uv with cwd python/ (house rule; the fcvae CLIs
# also resolve relative paths against the shell cwd).
py() { (cd "${PY_DIR}" && uv run python "$@"); }

# ---------------------------------------------------------------------------
# Feast server lifecycle (owned entirely by this harness)
# ---------------------------------------------------------------------------

kill_feast() {
  # pidfile first ($! recorded at launch is the `uv run` wrapper) ...
  if [ -f "${FEAST_PID_FILE}" ]; then
    kill "$(cat "${FEAST_PID_FILE}")" 2>/dev/null || true
    rm -f "${FEAST_PID_FILE}"
  fi
  # ... then the module invocation. NOTE: feast_setup serve execvp's into a
  # `feast -c <repo> serve` process, so match BOTH cmdline forms (path-scoped
  # to OUR repo so the taxi feast instance on 6566 is never touched) ...
  pkill -f 'fcvae.feast_setup serve' 2>/dev/null || true
  pkill -f 'feast -c .*fcvae/feature_repo serve' 2>/dev/null || true
  # ... then any LISTENER still holding the port (stale from an older run).
  # -sTCP:LISTEN is LOAD-BEARING: a bare lsof -ti tcp:<port> also matches
  # processes with CLIENT sockets to the port, including the Striim JVM whose
  # params OP keeps HTTP connections to Feast; killing those killed the whole
  # Striim server in the first live run.
  local pids
  pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "${pids}" ]; then
    kill ${pids} 2>/dev/null || true
  fi
  # The feast-down phase needs the LISTENER actually gone before feeding
  # (lingering client sockets in FIN_WAIT are irrelevant).
  local deadline
  deadline=$(( $(date +%s) + 10 ))
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
    if [ -z "${pids}" ]; then
      return 0
    fi
    sleep 1
  done
  # last resort for a hung listener
  pids="$(lsof -ti "tcp:${FEAST_PORT}" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "${pids}" ]; then
    kill -9 ${pids} 2>/dev/null || true
    sleep 1
  fi
  return 0
}

# Read the threshold the feature server is ACTUALLY serving for a combo, so a
# stale serve is caught at push time, not as a downstream assertion failure.
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

# ALWAYS reap the server, even on abort (the trap preserves the exit status).
trap kill_feast EXIT

# ---------------------------------------------------------------------------
# a. Preconditions (build-if-missing)
# ---------------------------------------------------------------------------

pre_toy_train() {
  if [ -f "${ART_TOY}/model.pt" ]; then
    echo "  ${ART_TOY}/model.pt present, skipping toy train"
    return 0
  fi
  py -m fcvae.train fit --model Penny_All --epochs 2 --device cpu
}

pre_toy_export() {
  if [ -f "${ART_TOY}/model.onnx" ] && [ -f "${ART_TOY}/metrics.json" ]; then
    echo "  toy model.onnx + metrics.json present, skipping export"
    return 0
  fi
  py -m fcvae.onnx_export export --model Penny_All
}

pre_prebuilt_export() {
  if [ -f "${ART_PREBUILT}/model.onnx" ] && [ -f "${ART_PREBUILT}/metrics.json" ]; then
    echo "  penny prebuilt model.onnx + metrics.json present, skipping export"
    return 0
  fi
  py -m fcvae.onnx_export export --model Penny_All \
      --model-dir "${FCVAE_REPO}/models/fcvae/Penny_All" \
      --out-dir "${ART_PREBUILT}"
}

pre_accel_prebuilt_export() {
  if [ -f "${ART_ACCEL_PREBUILT}/model.onnx" ] && [ -f "${ART_ACCEL_PREBUILT}/metrics.json" ]; then
    echo "  accel prebuilt model.onnx + metrics.json present, skipping export"
    return 0
  fi
  py -m fcvae.onnx_export export --model Accel_CMP \
      --model-dir "${FCVAE_REPO}/models/fcvae/Accel_CMP" \
      --out-dir "${ART_ACCEL_PREBUILT}"
}

pre_base_feed() {
  if [ -f "${BASE_FEED}" ]; then
    echo "  base feed present: ${BASE_FEED}"
  else
    py "${MAKE_FEED}" prepare --days "${SLICE_DAYS}" --out "${BASE_FEED}" || return 1
  fi
  [ -f "${BASE_FEED}" ] || { echo "  base feed still missing: ${BASE_FEED}"; return 1; }
}

derive_base_start() {
  # BASE_START = date of the FIRST DATA ROW of the base feed (line 2; line 1 is
  # the header). check_fcvae_swap.py combines it with --shift-days to key windows.
  BASE_START="$(sed -n '2p' "${BASE_FEED}" | cut -d',' -f1 | cut -c1-10)"
  if [ -z "${BASE_START}" ]; then
    echo "  could not read the first data row of ${BASE_FEED}"
    return 1
  fi
  echo "  BASE_START=${BASE_START}"
}

# ---------------------------------------------------------------------------
# b. Feast setup (registry apply + feature server on 6567)
# ---------------------------------------------------------------------------

feast_apply() { py -m fcvae.feast_setup apply; }

# ---------------------------------------------------------------------------
# c. Live reset + publish BOTH prebuilt models
# ---------------------------------------------------------------------------

live_reset() {
  # no .json suffix in the globs: also catches extensionless files from runs
  # before the filename fix
  rm -f "${PRED_DIR}"/fcvae_scored* "${PRED_DIR}"/fcvae_accel*
  rm -rf "${FEED_DIR}"
  mkdir -p "${FEED_DIR}"
  local mdir
  for mdir in "${MODEL_DIR_PENNY}" "${MODEL_DIR_ACCEL}"; do
    mkdir -p "${mdir}"
    # stale split-file sidecar and control file poison a fresh run: an orphan
    # model.onnx.data makes ORT resolve old external weights, and a leftover
    # fcvae.control can trigger a spurious rollback
    rm -f "${mdir}/model.onnx.data" "${mdir}/fcvae.control"
  done
  # PREBUILT models are the baseline live models. F2 publish ALSO pushes each
  # model's scoring params to the running Feast, so the server must be up;
  # --require-feast makes a failed push fail THIS step instead of surfacing as
  # a confusing phase-2 assertion failure.
  py -m fcvae.publish run --model Penny_All --artifacts "${ART_PREBUILT}" \
      --out "${MODEL_DIR_PENNY}" --require-feast || return 1
  py -m fcvae.publish run --model Accel_CMP --artifacts "${ART_ACCEL_PREBUILT}" \
      --out "${MODEL_DIR_ACCEL}" --require-feast
}

run_deploy() { "${DEPLOY_SH}"; }

# ---------------------------------------------------------------------------
# e. Phase helpers: one feed per phase, snapshot each chain by prefix
# ---------------------------------------------------------------------------

feed_shift() {  # $1 = shift days
  local shift_days="$1"
  local feed="${FEED_DIR}/penny_feed_s${shift_days}.csv"
  echo "  feeding shift=${shift_days} -> ${feed}"
  # write to a temp name then mv into place, so the FileReader never sees a
  # partial file (the .tmp suffix does not match the penny_feed*.csv wildcard)
  py "${MAKE_FEED}" emit --base "${BASE_FEED}" --shift-days "${shift_days}" \
      --out "${feed}.tmp" || return 1
  mv "${feed}.tmp" "${feed}"
}

snap_penny() {  # $1 = shift days, $2 = snapshot file
  py "${CHECK_SWAP}" snapshot --base-start "${BASE_START}" --shift-days "$1" \
      --min-count "${EXPECT_KEYS}" --wait-sec 300 --out "$2"
}

snap_accel() {  # $1 = shift days, $2 = snapshot file
  py "${CHECK_SWAP}" snapshot --base-start "${BASE_START}" --shift-days "$1" \
      --prefix fcvae_accel --min-count "${EXPECT_KEYS}" --wait-sec 300 --out "$2"
}

compare_snaps() {  # $1 = snapshot a, $2 = snapshot b, $3 = identical|different
  py "${CHECK_SWAP}" compare "$1" "$2" --expect "$3"
}

compare_scores_only() {  # $1 = snapshot a, $2 = snapshot b, $3 = identical|different
  py "${CHECK_SWAP}" compare "$1" "$2" --scores-only --expect "$3"
}

assert_snap() {  # $1 = snapshot, then assert-snap flags
  py "${CHECK_SWAP}" assert-snap "$@"
}

push_params() {  # push Feast scoring params (defaults from the published dir)
  py -m fcvae.feast_setup push "$@"
}

# ---------------------------------------------------------------------------
# f. Phases
# ---------------------------------------------------------------------------

phase1_baselines() {
  feed_shift 0 || return 1
  snap_penny 0 "${SNAP_DIR}/snapA.json" || return 1
  assert_snap "${SNAP_DIR}/snapA.json" \
      --expect-threshold "${PENNY_THRESH}" --expect-anomaly-rate 0 || return 1
  snap_accel 0 "${SNAP_DIR}/snapA_acc.json" || return 1
  # the accel prebuilt's anomaly rate on this slice is unknown: threshold only
  assert_snap "${SNAP_DIR}/snapA_acc.json" --expect-threshold "${ACCEL_THRESH}"
}

phase2_feast_flip() {
  echo "  action: Feast-ONLY flip of the Penny decision threshold to 0.0"
  echo "  (defaults keep the REAL model_version, so the scorer accepts the row)"
  push_params --model Penny_All --threshold 0.0 || return 1
  verify_served Penny_All 0.0 || return 1
  feed_shift 7 || return 1
  snap_penny 7 "${SNAP_DIR}/snap2.json" || return 1
  # same model, same scores; only the Feast-served decision params moved
  compare_scores_only "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snap2.json" identical || return 1
  # tolerance 0.05: one-in-96 per-event fallback under a transient Feast
  # hiccup is the DESIGNED degradation, not a failure (the OP retries once)
  assert_snap "${SNAP_DIR}/snap2.json" \
      --expect-threshold 0.0 --expect-anomaly-rate 1.0 --tolerance 0.05 || return 1
  # per-combo isolation: the untouched Accel combo is FULLY identical
  # (threshold still ${ACCEL_THRESH})
  snap_accel 7 "${SNAP_DIR}/snap2_acc.json" || return 1
  compare_snaps "${SNAP_DIR}/snapA_acc.json" "${SNAP_DIR}/snap2_acc.json" identical
}

phase3_version_guard() {
  echo "  action: push threshold 0.0 with a POISONED model_version"
  push_params --model Penny_All --threshold 0.0 \
      --model-version sha256:deadbeefdead || return 1
  verify_served Penny_All 0.0 || return 1
  feed_shift 14 || return 1
  snap_penny 14 "${SNAP_DIR}/snap3.json" || return 1
  compare_scores_only "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snap3.json" identical || return 1
  # fallback to model_config DESPITE Feast being up and answering
  assert_snap "${SNAP_DIR}/snap3.json" \
      --expect-threshold "${PENNY_THRESH}" --expect-anomaly-rate 0
}

phase4_feast_down() {
  echo "  action: re-arm a GOOD-version threshold 0.0 push, THEN kill the server"
  push_params --model Penny_All --threshold 0.0 || return 1
  verify_served Penny_All 0.0 || return 1
  kill_feast || return 1
  echo "  feast server killed (port ${FEAST_PORT} closed)"
  feed_shift 21 || return 1
  snap_penny 21 "${SNAP_DIR}/snap4.json" || return 1
  # fallback beats a down-but-armed Feast
  assert_snap "${SNAP_DIR}/snap4.json" \
      --expect-threshold "${PENNY_THRESH}" --expect-anomaly-rate 0 || return 1
  compare_scores_only "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snap4.json" identical
}

phase5_restore() {
  echo "  action: restart the server, push a DISTINGUISHABLE live threshold -21.0"
  echo "  (decisions unchanged, scores max out around -0.23, but the threshold"
  echo "   field proves the Feast path re-engaged)"
  start_feast || return 1
  push_params --model Penny_All --threshold -21.0 || return 1
  verify_served Penny_All -21.0 || return 1
  feed_shift 28 || return 1
  snap_penny 28 "${SNAP_DIR}/snap5.json" || return 1
  compare_scores_only "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snap5.json" identical || return 1
  assert_snap "${SNAP_DIR}/snap5.json" \
      --expect-threshold -21.0 --expect-anomaly-rate 0 --tolerance 0.05 || return 1
  # leave the store exactly as published (defaults from the published dir)
  echo "  action: push the exact published defaults back (no feed)"
  push_params --model Penny_All
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

rm -rf "${SNAP_DIR}"
mkdir -p "${SNAP_DIR}"
echo "F2 acceptance: SNAP_DIR=${SNAP_DIR}  FCVAE_REPO=${FCVAE_REPO}"
echo "SLICE_DAYS=${SLICE_DAYS}  EXPECT_KEYS=${EXPECT_KEYS}  FEAST_PORT=${FEAST_PORT}"

step "pre-toy-train" pre_toy_train
step "pre-toy-export" pre_toy_export
step "pre-prebuilt-export" pre_prebuilt_export
step "pre-accel-prebuilt-export" pre_accel_prebuilt_export
step "pre-base-feed" pre_base_feed
step "derive-base-start" derive_base_start

step "feast-apply" feast_apply
# restart-fresh: start_feast kills any stale pidfile/process/listener first
step "feast-serve" start_feast

FAILS_BEFORE="${FAIL_COUNT}"
step "live-reset-publish-both" live_reset
step "deploy-pipeline" run_deploy
if [ "${FAIL_COUNT}" -gt "${FAILS_BEFORE}" ]; then
  echo ""
  echo "reset/deploy failed: skipping the live phases (they would only stall on waits)"
  summary
  exit 1
fi

step "phase1-baselines" phase1_baselines
step "phase2-feast-only-flip" phase2_feast_flip
step "phase3-version-guard" phase3_version_guard
step "phase4-feast-down-fallback" phase4_feast_down
step "phase5-restore-feast-path" phase5_restore

summary
[ "${FAIL_COUNT}" -eq 0 ]
