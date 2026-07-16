#!/usr/bin/env bash
# run_f1_acceptance.sh -- full F1 acceptance for the FCVAE hot-swap pipeline.
#
# NEEDS: STRIIM_ADMIN_PW in the environment (deploy_fcvae.sh uses it for REST
# auth and console.sh). Takes roughly 15-25 minutes: it feeds the pipeline
# SEVEN times (one feed per phase) and waits up to 300s for the 96 scored
# windows of each feed.
#
# Phase plan (after host-only publish-refusal proofs and a live reset + deploy):
#   1. shift 0  : baseline snapshot on the PREBUILT model (threshold -21.3322)
#   2. shift 7  : bad input NAME candidate  -> REJECTED, output identical to snapA
#   3. shift 14 : bad input SHAPE candidate -> REJECTED, output identical to snapA
#   4. shift 21 : bad input DTYPE candidate -> REJECTED, output identical to snapA
#   5. shift 28 : good swap to the TOY model (different weights AND threshold)
#                 -> output differs from snapA, threshold differs (config pairs
#                 with model)
#   6. shift 35 : rollback via fcvae.control -> output identical to snapA
#                 (session AND threshold restored)
#   7. shift 42 : touch model.onnx (disk still holds toy bytes) -> mtime gate
#                 fires, sha differs from live prebuilt, toy re-promoted ->
#                 output identical to snapB (fresh session over the same bytes
#                 is bit-exact on CPU)
#
# All model copies use PLAIN cp (never cp -p): the destination mtime must
# ADVANCE for the scorer's mtime gate to notice the file changed.
#
# Overrides: FCVAE_REPO (sibling fcvae-anomaly-detection checkout), SNAP_DIR.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PY_DIR="${REPO_ROOT}/python"
FCVAE_REPO="${FCVAE_REPO:-$(dirname "${REPO_ROOT}")/fcvae-anomaly-detection}"

SLICE_DAYS=5                 # the base feed spans 5 days of hourly data
EXPECT_KEYS=96               # scored windows per feed (SLICE_DAYS*24 hours minus the 24h warm-up)
MODEL_DIR="/opt/Striim/fcvae-models/Penny_All"
PRED_DIR="/opt/Striim/UploadedFiles"
FEEDS="${REPO_ROOT}/striim/pipeline/feeds"
TEST_ART="${REPO_ROOT}/striim/fcvae-scorer/test/artifacts"
SNAP_DIR="${SNAP_DIR:-/tmp/fcvae_f1_snapshots}"
FEED_DIR="/tmp/fcvae_swap_test"
BASE_FEED="${FEEDS}/penny_base_5d.csv"
ART_TOY="${PY_DIR}/fcvae/artifacts/Penny_All"
ART_PREBUILT="${PY_DIR}/fcvae/artifacts/Penny_All_prebuilt"
MAKE_FEED="${REPO_ROOT}/striim/pipeline/make_fcvae_feed.py"
CHECK_SWAP="${REPO_ROOT}/striim/pipeline/check_fcvae_swap.py"
MAKE_BAD="${REPO_ROOT}/striim/fcvae-scorer/test/make_bad_models.py"
DEPLOY_SH="${REPO_ROOT}/striim/pipeline/deploy_fcvae.sh"

if [ -z "${STRIIM_ADMIN_PW:-}" ]; then
  echo "ERROR: STRIIM_ADMIN_PW must be set (deploy_fcvae.sh needs it)" >&2
  exit 2
fi

PASS_COUNT=0
FAIL_COUNT=0
FAILED_STEPS=""
BASE_START=""
THRESH_A=""
THRESH_B=""

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
  echo "==== F1 acceptance summary ===="
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
    echo "  prebuilt model.onnx + metrics.json present, skipping export"
    return 0
  fi
  py -m fcvae.onnx_export export --model Penny_All \
      --model-dir "${FCVAE_REPO}/models/fcvae/Penny_All" \
      --out-dir "${ART_PREBUILT}"
}

pre_bad_models() {
  if [ -f "${TEST_ART}/bad_input_name.onnx" ] && [ -f "${TEST_ART}/bad_shape.onnx" ] \
      && [ -f "${TEST_ART}/bad_dtype.onnx" ]; then
    echo "  bad-model artifacts present, skipping"
    return 0
  fi
  py "${MAKE_BAD}" --out-dir "${TEST_ART}"
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
# b. Publish refusal proofs (host-only, no cluster contact)
# ---------------------------------------------------------------------------

refusal_empty_artifacts() {
  local out="${SNAP_DIR}/refuse_out_empty"
  local art="${SNAP_DIR}/empty_artifacts"
  rm -rf "${out}" "${art}"
  mkdir -p "${out}" "${art}"
  local rc=0
  py -m fcvae.publish run --model Penny_All --artifacts "${art}" --out "${out}" || rc=$?
  if [ "${rc}" -ne 1 ]; then
    echo "  expected REFUSED exit 1 on an EMPTY artifacts dir, got exit ${rc}"
    return 1
  fi
  if [ -n "$(ls -A "${out}")" ]; then
    echo "  out dir NOT empty after the refusal:"
    ls -l "${out}"
    return 1
  fi
  echo "  REFUSED (exit 1) and the fresh out dir stayed empty"
}

refusal_garbage_onnx() {
  local out="${SNAP_DIR}/refuse_out_garbage"
  local art="${SNAP_DIR}/garbage_artifacts"
  rm -rf "${out}" "${art}"
  mkdir -p "${out}" "${art}"
  # real artifact set, then a garbage model.onnx on top (plain cp, never cp -p)
  cp "${ART_PREBUILT}/"* "${art}/" || return 1
  printf 'this is not an onnx model\n' > "${art}/model.onnx"
  local rc=0
  py -m fcvae.publish run --model Penny_All --artifacts "${art}" --out "${out}" || rc=$?
  if [ "${rc}" -ne 1 ]; then
    echo "  expected REFUSED exit 1 on a GARBAGE model.onnx, got exit ${rc}"
    return 1
  fi
  if [ -n "$(ls -A "${out}")" ]; then
    echo "  out dir NOT empty after the refusal:"
    ls -l "${out}"
    return 1
  fi
  echo "  REFUSED (exit 1) and the fresh out dir stayed empty"
}

# ---------------------------------------------------------------------------
# c. Live reset + baseline publish, d. deploy
# ---------------------------------------------------------------------------

live_reset() {
  # no .json suffix in the glob: also catches extensionless files from runs
  # before the filename fix
  rm -f "${PRED_DIR}"/fcvae_scored*
  rm -rf "${FEED_DIR}"
  mkdir -p "${FEED_DIR}"
  mkdir -p "${MODEL_DIR}"
  # stale split-file sidecar and control file poison a fresh run: an orphan
  # model.onnx.data makes ORT resolve old external weights, and a leftover
  # fcvae.control can trigger a rollback before phase 6
  rm -f "${MODEL_DIR}/model.onnx.data" "${MODEL_DIR}/fcvae.control"
  # PREBUILT is the baseline live model
  py -m fcvae.publish run --model Penny_All --artifacts "${ART_PREBUILT}" --out "${MODEL_DIR}"
}

run_deploy() { "${DEPLOY_SH}"; }

# ---------------------------------------------------------------------------
# e. Phase helper: emit a shifted feed, then snapshot the scored output
# ---------------------------------------------------------------------------

feed_and_snapshot() {  # $1 = shift days, $2 = snapshot file
  local shift_days="$1" snap="$2"
  local feed="${FEED_DIR}/penny_feed_s${shift_days}.csv"
  echo "  feeding shift=${shift_days} -> ${feed}"
  # write to a temp name then mv into place, so the FileReader never sees a
  # partial file (the .tmp suffix does not match the penny_feed*.csv wildcard)
  py "${MAKE_FEED}" emit --base "${BASE_FEED}" --shift-days "${shift_days}" \
      --out "${feed}.tmp" || return 1
  mv "${feed}.tmp" "${feed}" || return 1
  py "${CHECK_SWAP}" snapshot --base-start "${BASE_START}" --shift-days "${shift_days}" \
      --min-count "${EXPECT_KEYS}" --wait-sec 300 --out "${snap}"
}

compare_snaps() {  # $1 = snapshot a, $2 = snapshot b, $3 = identical|different
  py "${CHECK_SWAP}" compare "$1" "$2" --expect "$3"
}

# Threshold of the LATEST scored record (data[7] projected by PennyFormatScored),
# decoded tolerantly from the JSONFormatter open-array files.
latest_threshold() {
  py - <<PY
import glob, json
dec = json.JSONDecoder()
recs = []
for path in sorted(glob.glob("${PRED_DIR}/fcvae_scored*.json")):
    text = open(path).read()
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i] in " \t\r\n,[]":
            i += 1
        if i >= n:
            break
        try:
            obj, end = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            break
        recs.append(obj)
        i = end
print(recs[-1].get("threshold", "") if recs else "")
PY
}

threshold_matches() {  # $1 = observed, $2 = expected
  py - "$1" "$2" <<'PY'
import sys
obs, exp = float(sys.argv[1]), float(sys.argv[2])
ok = abs(obs - exp) < 1e-6
print(f"  threshold observed={obs} expected={exp} match={ok}")
sys.exit(0 if ok else 1)
PY
}

thresholds_differ() {  # $1 = threshold A, $2 = threshold B
  py - "$1" "$2" <<'PY'
import sys
a, b = float(sys.argv[1]), float(sys.argv[2])
ok = abs(a - b) > 1e-9
print(f"  thresholdA={a} thresholdB={b} differ={ok}")
sys.exit(0 if ok else 1)
PY
}

# ---------------------------------------------------------------------------
# f. Phases
# ---------------------------------------------------------------------------

phase1_baseline() {
  feed_and_snapshot 0 "${SNAP_DIR}/snapA.json" || return 1
  THRESH_A="$(latest_threshold)"
  if [ -z "${THRESH_A}" ]; then
    echo "  no threshold found in the scored output"
    return 1
  fi
  echo "  baseline threshold: ${THRESH_A}"
  threshold_matches "${THRESH_A}" "-21.3322"
}

reject_phase() {  # $1 = bad artifact, $2 = shift days, $3 = snapshot tag
  echo "  action: copy ${TEST_ART}/$1 over ${MODEL_DIR}/model.onnx (expect REJECTED)"
  # plain cp (never cp -p): the destination mtime must advance for the gate to fire
  cp "${TEST_ART}/$1" "${MODEL_DIR}/model.onnx" || return 1
  feed_and_snapshot "$2" "${SNAP_DIR}/snap_$3.json" || return 1
  compare_snaps "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snap_$3.json" identical
}

phase5_good_swap() {
  echo "  action: publish the TOY model (different weights AND threshold) as live"
  py -m fcvae.publish run --model Penny_All --artifacts "${ART_TOY}" \
      --out "${MODEL_DIR}" || return 1
  feed_and_snapshot 28 "${SNAP_DIR}/snapB.json" || return 1
  compare_snaps "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snapB.json" different || return 1
  THRESH_B="$(latest_threshold)"
  if [ -z "${THRESH_B}" ]; then
    echo "  no threshold found in the scored output"
    return 1
  fi
  echo "  post-swap threshold: ${THRESH_B}"
  # config-pairs-with-model proof: the swap changed the DECISION THRESHOLD too
  thresholds_differ "${THRESH_A}" "${THRESH_B}"
}

phase6_rollback() {
  echo "  action: write 'rollback' to ${MODEL_DIR}/fcvae.control"
  printf 'rollback\n' > "${MODEL_DIR}/fcvae.control" || return 1
  feed_and_snapshot 35 "${SNAP_DIR}/snap_rollback.json" || return 1
  compare_snaps "${SNAP_DIR}/snapA.json" "${SNAP_DIR}/snap_rollback.json" identical || return 1
  # session AND threshold restored
  local t
  t="$(latest_threshold)"
  echo "  post-rollback threshold: ${t}"
  threshold_matches "${t}" "${THRESH_A}"
}

phase7_touch_repromote() {
  echo "  action: touch ${MODEL_DIR}/model.onnx (disk still holds the TOY bytes)"
  # the mtime gate fires before the sha compare, the sha differs from the live
  # prebuilt session, so the toy is re-promoted; a fresh session over the same
  # bytes is bit-exact on CPU, so the output must equal snapB exactly
  touch "${MODEL_DIR}/model.onnx" || return 1
  feed_and_snapshot 42 "${SNAP_DIR}/snap_repromote.json" || return 1
  compare_snaps "${SNAP_DIR}/snapB.json" "${SNAP_DIR}/snap_repromote.json" identical
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

rm -rf "${SNAP_DIR}"
mkdir -p "${SNAP_DIR}"
echo "F1 acceptance: SNAP_DIR=${SNAP_DIR}  FCVAE_REPO=${FCVAE_REPO}"
echo "SLICE_DAYS=${SLICE_DAYS}  EXPECT_KEYS=${EXPECT_KEYS}"

step "pre-toy-train" pre_toy_train
step "pre-toy-export" pre_toy_export
step "pre-prebuilt-export" pre_prebuilt_export
step "pre-bad-models" pre_bad_models
step "pre-base-feed" pre_base_feed
step "derive-base-start" derive_base_start

step "refusal-empty-artifacts" refusal_empty_artifacts
step "refusal-garbage-onnx" refusal_garbage_onnx

step "live-reset-publish-prebuilt" live_reset

FAILS_BEFORE="${FAIL_COUNT}"
step "deploy-pipeline" run_deploy
if [ "${FAIL_COUNT}" -gt "${FAILS_BEFORE}" ]; then
  echo ""
  echo "deploy failed: skipping the live phases (they would only stall on waits)"
  summary
  exit 1
fi

step "phase1-baseline-snapshot" phase1_baseline
step "phase2-reject-bad-input-name" reject_phase bad_input_name.onnx 7 p2_bad_name
step "phase3-reject-bad-shape" reject_phase bad_shape.onnx 14 p3_bad_shape
step "phase4-reject-bad-dtype" reject_phase bad_dtype.onnx 21 p4_bad_dtype
step "phase5-good-swap-toy" phase5_good_swap
step "phase6-rollback-control-file" phase6_rollback
step "phase7-touch-repromotion" phase7_touch_repromote

summary
[ "${FAIL_COUNT}" -eq 0 ]
