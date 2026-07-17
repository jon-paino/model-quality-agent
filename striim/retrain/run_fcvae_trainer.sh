#!/usr/bin/env bash
# run_fcvae_trainer.sh -- canonical one-shot run of the mqa-fcvae-trainer
# container (train -> export -> gated publish for ONE combo), the FCVAE
# sibling of run_trainer.sh. The F4 retrain trigger invokes this via
# --trainer; the degraded combo arrives as MQA_FCVAE_MODEL (exported by the
# trigger from the fired metric signal's name).
#
# Env overrides (all defaulted):
#   MQA_FCVAE_MODEL     combo to retrain             (default Penny_All)
#   MQA_FCVAE_IMAGE     image name                   (default mqa-fcvae-trainer)
#   MQA_FCVAE_DATA      labeled source CSV, ro mount (default sibling repo's
#                       data/synthetic_transactions.csv)
#   MQA_FCVAE_OUT       published-models root, rw    (default /opt/Striim/fcvae-models;
#                       the container publishes to /out/$MODEL, the dir the
#                       scorer OP watches)
#   MQA_FCVAE_EPOCHS    optional epoch override      (default '' = full train)
#   MQA_TRAINER_CPUS    --cpus cap                   (default 4)
#   MQA_TRAINER_NAME    deterministic container name (default mqa-fcvae-trainer-run;
#                       the trigger's timeout kill targets the SAME env var, so
#                       set it identically on the trigger invocation)
#   MQA_DOCKER          docker binary                (default docker)
#
# Exit codes (each failure mode distinctly assertable; the docker-daemon and
# missing-image prechecks exist so a dead Docker Desktop reads as a clear
# refusal, not a cryptic 'docker: Cannot connect' buried in trainer output):
#   0  container ran and the fresh manifest is readable
#   2  input data CSV missing
#   3  container exited 0 but the manifest is unreadable (broken publish
#      contract or wrong output volume)
#   7  docker daemon unreachable
#   8  trainer image missing (build it: cd python && docker build -f
#      Dockerfile.fcvae -t mqa-fcvae-trainer .)
#   else the container's own nonzero exit code passes through
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

MODEL="${MQA_FCVAE_MODEL:-Penny_All}"
IMAGE="${MQA_FCVAE_IMAGE:-mqa-fcvae-trainer}"
DATA="${MQA_FCVAE_DATA:-$(dirname "${REPO_ROOT}")/fcvae-anomaly-detection/data/synthetic_transactions.csv}"
OUT="${MQA_FCVAE_OUT:-/opt/Striim/fcvae-models}"
EPOCHS="${MQA_FCVAE_EPOCHS:-}"
CPUS="${MQA_TRAINER_CPUS:-4}"
NAME="${MQA_TRAINER_NAME:-mqa-fcvae-trainer-run}"
DOCKER="${MQA_DOCKER:-docker}"

# Precheck: docker daemon reachable (a stopped Docker Desktop is an expected
# operational state and deserves its own exit code + message).
if ! "${DOCKER}" info > /dev/null 2>&1; then
  echo "ERROR: docker daemon unreachable via '${DOCKER}' (is Docker running?)" >&2
  exit 7
fi

# Precheck: image present (never silently attempt a registry pull).
if ! "${DOCKER}" image inspect "${IMAGE}" > /dev/null 2>&1; then
  echo "ERROR: image '${IMAGE}' not found; build it first:" >&2
  echo "       cd ${REPO_ROOT}/python && docker build -f Dockerfile.fcvae -t ${IMAGE} ." >&2
  exit 8
fi

if [ ! -f "${DATA}" ]; then
  echo "ERROR: source CSV not found: ${DATA}" >&2
  exit 2
fi

# A leftover same-name container (e.g. from a previous timed-out run whose
# kill raced) would make docker run fail on the name; remove it first.
if "${DOCKER}" container inspect "${NAME}" > /dev/null 2>&1; then
  echo "WARN: removing leftover container ${NAME}" >&2
  "${DOCKER}" rm -f "${NAME}" > /dev/null 2>&1 || true
fi

echo "[run_fcvae_trainer] model=${MODEL} image=${IMAGE} cpus=${CPUS} name=${NAME}"
echo "[run_fcvae_trainer] data=${DATA}"
echo "[run_fcvae_trainer] out=${OUT} (container publishes to /out/${MODEL})"

"${DOCKER}" run --rm --name "${NAME}" --cpus "${CPUS}" \
    -v "${DATA}":/work/data/synthetic_transactions.csv:ro \
    -v "${OUT}":/out \
    -e FCVAE_MODEL="${MODEL}" \
    ${EPOCHS:+-e FCVAE_EPOCHS="${EPOCHS}"} \
    "${IMAGE}"
RC=$?
if [ "${RC}" -ne 0 ]; then
  echo "ERROR: trainer container exited ${RC}" >&2
  exit "${RC}"
fi

# The publish contract: a fresh, readable manifest beside the model.
MANIFEST="${OUT}/${MODEL}/model.manifest.json"
python3 - "${MANIFEST}" <<'PY'
import sys, json
try:
    m = json.load(open(sys.argv[1]))
    print(f"[run_fcvae_trainer] published {m.get('model_version')} "
          f"created_utc={m.get('created_utc')}")
except Exception as exc:
    print(f"ERROR: container exited 0 but manifest unreadable at {sys.argv[1]}: {exc}",
          file=sys.stderr)
    sys.exit(3)
PY
exit $?
