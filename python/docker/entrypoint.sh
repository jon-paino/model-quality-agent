#!/bin/sh
# MQA training container entrypoint: the one-shot closure. Exits non-zero on
# the first failure. Publish is the ONLY step that writes to the output
# volume ($MODEL_PUBLISH_OUT) and it refuses, writing nothing, unless the
# signature and golden-set parity gates pass.
set -e

if [ ! -f "${MODEL_DATA_PROCESSED}/trips_cleaned.parquet" ]; then
    echo "trainer: no input data. Mount the cleaned trips parquet, e.g." >&2
    echo "  -v <repo>/model/data/processed/trips_cleaned.parquet:${MODEL_DATA_PROCESSED}/trips_cleaned.parquet:ro" >&2
    exit 2
fi

echo "[trainer] features build"
python -m model.features build
echo "[trainer] train fit"
python -m model.train fit
echo "[trainer] export onnx (parity gate)"
python -m model.export_onnx export
echo "[trainer] publish -> ${MODEL_PUBLISH_OUT}"
python -m model.publish run
echo "[trainer] done"
