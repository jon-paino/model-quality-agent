#!/bin/sh
# One-shot FCVAE trainer closure: train -> ONNX export (golden freeze + parity
# gate) -> gated publish, for ONE combo model per run (FCVAE_MODEL; the F4
# trigger exports the degraded combo as MQA_FCVAE_MODEL and the wrapper maps
# it here). set -e: the first failing step aborts the container with its exit
# code, and publish's six gates mean a bad candidate never reaches /out.
#
# Env (defaults baked into the image):
#   FCVAE_MODEL      combo to train (Penny_All | Accel_CMP | ...)
#   FCVAE_DATA_CSV   labeled source transactions CSV (mount read-only)
#   FCVAE_ARTIFACTS  in-container training scratch
#   FCVAE_EPOCHS     optional override; empty = the CLI default (15, early
#                    stopping patience 3)
#
# The post-publish Feast push is SKIPPED in-container by design (no
# --require-feast; the host's 127.0.0.1:6567 is unreachable from here, the
# push logs a WARN and publish still succeeds). The scorer's model_version
# skew guard makes the freshly-published model correct immediately via the
# swap-paired config; the host re-pushes params afterwards to re-engage the
# Feast path (see run_f4_acceptance.sh).
set -e

: "${FCVAE_MODEL:=Penny_All}"

if [ ! -f "$FCVAE_DATA_CSV" ]; then
  echo "[fcvae-trainer] ERROR: source CSV not found at $FCVAE_DATA_CSV" >&2
  echo "[fcvae-trainer]        mount it: -v <synthetic_transactions.csv>:$FCVAE_DATA_CSV:ro" >&2
  exit 2
fi

echo "[fcvae-trainer] train fit --model $FCVAE_MODEL --device cpu ${FCVAE_EPOCHS:+--epochs $FCVAE_EPOCHS}"
python -m fcvae.train fit --model "$FCVAE_MODEL" --device cpu ${FCVAE_EPOCHS:+--epochs "$FCVAE_EPOCHS"}

echo "[fcvae-trainer] onnx export (golden freeze + parity gate)"
python -m fcvae.onnx_export export --model "$FCVAE_MODEL"

echo "[fcvae-trainer] publish -> /out/$FCVAE_MODEL (six gates)"
python -m fcvae.publish run --model "$FCVAE_MODEL" --out "/out/$FCVAE_MODEL"

echo "[fcvae-trainer] done"
