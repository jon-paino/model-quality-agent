#!/bin/sh
# Canonical invocation of the mqa-trainer container (Week 3). One entry point
# for manual runs, the retrain trigger, and the Week 5 bridge. No lock, no
# state, no retries: concurrency is the trigger's job.
#
# Env overrides (defaults match the Week 3 contract):
#   MQA_TRAINER_IMAGE  image name                 (default mqa-trainer)
#   MQA_TRAINER_CPUS   --cpus cap                 (default 4)
#   MQA_TRAINER_DATA   cleaned trips parquet      (default <repo>/model/data/processed/trips_cleaned.parquet)
#   MQA_TRAINER_OUT    the volume ModelOp watches (default /opt/Striim/UploadedFiles)
#   MQA_DOCKER         docker binary              (default docker)
#
# Exit codes: the container's own code passes through untouched (0 = published);
# 2 = input data missing; 3 = container exited 0 but the manifest is unreadable
# (broken publish contract).
set -u

REPO=$(cd "$(dirname "$0")/../.." && pwd)
IMAGE="${MQA_TRAINER_IMAGE:-mqa-trainer}"
CPUS="${MQA_TRAINER_CPUS:-4}"
DATA="${MQA_TRAINER_DATA:-$REPO/model/data/processed/trips_cleaned.parquet}"
OUT="${MQA_TRAINER_OUT:-/opt/Striim/UploadedFiles}"
DOCKER="${MQA_DOCKER:-docker}"
# Deterministic container name: lets the trigger `docker kill` it on timeout,
# and docker itself refuses a second concurrent run of the same name (an extra
# single-flight backstop under the trigger's lock).
NAME="${MQA_TRAINER_NAME:-mqa-trainer-run}"

if [ ! -f "$DATA" ]; then
    echo "run_trainer: no input data at $DATA" >&2
    echo "  (set MQA_TRAINER_DATA or stage the cleaned trips parquet)" >&2
    exit 2
fi

"$DOCKER" run --rm --name "$NAME" --cpus "$CPUS" \
    -v "$DATA":/work/data/processed/trips_cleaned.parquet:ro \
    -v "$OUT":/out \
    "$IMAGE"
rc=$?
if [ "$rc" -ne 0 ]; then
    exit "$rc"
fi

python3 - "$OUT/model.manifest.json" <<'EOF'
import json, sys
try:
    m = json.load(open(sys.argv[1]))
    print(f"published model_version={m['model_version']} created_utc={m['created_utc']}")
except Exception as e:
    print(f"FAIL: container exited 0 but no readable manifest ({e})")
    raise SystemExit(3)
EOF
exit $?
