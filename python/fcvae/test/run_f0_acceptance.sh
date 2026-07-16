#!/usr/bin/env bash
# F0 acceptance harness for the FCVAE adaptation (python/fcvae + JVM scorer probe).
#
# NOTE: the full run takes several minutes. Each `train fit` loads the sibling
# repo's transactions CSV (~1 min per load); the 2-epoch toy trains themselves
# are short.
#
# Override: FCVAE_REPO points at the sibling fcvae-anomaly-detection checkout
# (default: sibling directory of this repo).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PY_DIR="${REPO_ROOT}/python"
FCVAE_REPO="${FCVAE_REPO:-$(dirname "${REPO_ROOT}")/fcvae-anomaly-detection}"
STRIIM_LIB="/opt/Striim/lib"

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

uv_sync() { (cd "${PY_DIR}" && uv sync --extra fcvae); }

import_smoke() {
  (cd "${PY_DIR}" && uv run python -c \
"import fcvae.model, fcvae.train, fcvae.onnx_export
from fcvae.compat import install_pickle_shims
install_pickle_shims()")
}

train_toy() {
  (cd "${PY_DIR}" && uv run python -m fcvae.train fit --model "$1" --epochs 2 --device cpu)
}

export_toy() {
  (cd "${PY_DIR}" && uv run python -m fcvae.onnx_export export --model Penny_All)
}

export_prebuilt() {
  (cd "${PY_DIR}" && uv run python -m fcvae.onnx_export export --model Penny_All \
      --model-dir "${FCVAE_REPO}/models/fcvae/Penny_All" \
      --out-dir "${PREBUILT_DIR}")
}

recheck_prebuilt() {
  (cd "${PY_DIR}" && uv run python -m fcvae.onnx_export check \
      --onnx-path "${PREBUILT_DIR}/model.onnx" \
      --golden-path "${PREBUILT_DIR}/golden_windows.jsonl")
}

single_file_onnx() {
  if [ -e "${PREBUILT_DIR}/model.onnx.data" ]; then
    echo "  ${PREBUILT_DIR}/model.onnx.data exists: the export is not single-file"
    return 1
  fi
  echo "  no model.onnx.data next to ${PREBUILT_DIR}/model.onnx (single-file export)"
}

jvm_load_test() {
  "${REPO_ROOT}/striim/fcvae-scorer/test/run_load_test.sh" \
      "${PREBUILT_DIR}/model.onnx" "${PREBUILT_DIR}/golden_windows.jsonl"
}

waeudf_jar() {
  if ls "${STRIIM_LIB}" 2>/dev/null | grep -qi waeudf; then
    ls -l "${STRIIM_LIB}" | grep -i waeudf
  else
    echo "  WAEUdf jar NOT found in ${STRIIM_LIB}."
    echo "  The F1 TQL (createWAEvent UDF) cannot compile without it: install the"
    echo "  SE add-on jar into ${STRIIM_LIB} and restart Striim."
    return 1
  fi
}

inventory() {
  echo "  Informational only: stale sibling-repo leftovers, to be replaced in F1."
  if [ -e /opt/Striim/UploadedFiles/FCVAEOnnxScorer.scm ]; then
    ls -l /opt/Striim/UploadedFiles/FCVAEOnnxScorer.scm
  else
    echo "  /opt/Striim/UploadedFiles/FCVAEOnnxScorer.scm: not present"
  fi
  if [ -d /opt/Striim/fcvae-models ]; then
    ls -l /opt/Striim/fcvae-models/
  else
    echo "  /opt/Striim/fcvae-models/: not present"
  fi
  return 0
}

step "uv-sync-extra-fcvae" uv_sync

# The CLI resolves relative paths against the shell cwd, so use absolute paths
# everywhere; ask the package where its artifacts root is (honors FCVAE_ARTIFACTS).
ARTIFACTS_DIR="$( (cd "${PY_DIR}" && uv run python -c \
    'from fcvae.config import ARTIFACTS; print(ARTIFACTS)') 2>/dev/null || true )"
[ -n "${ARTIFACTS_DIR}" ] || ARTIFACTS_DIR="${PY_DIR}/fcvae/artifacts"
PREBUILT_DIR="${ARTIFACTS_DIR}/Penny_All_prebuilt"

step "import-smoke" import_smoke
step "train-toy-Penny_All" train_toy Penny_All
step "train-toy-Accel_CMP" train_toy Accel_CMP
step "export-toy-Penny_All" export_toy
step "export-prebuilt-Penny_All" export_prebuilt
step "recheck-prebuilt-golden" recheck_prebuilt
step "single-file-onnx" single_file_onnx
step "jvm-load-test" jvm_load_test
step "waeudf-jar-present" waeudf_jar
step "inventory-stale-leftovers" inventory

echo ""
echo "==== F0 acceptance summary ===="
echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}"
if [ "${FAIL_COUNT}" -gt 0 ]; then
  echo "Failed steps:${FAILED_STEPS}"
  exit 1
fi
echo "All steps passed."
