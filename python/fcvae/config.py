"""FCVAE adaptation constants and paths. Single source of truth for every
fcvae.* code path, mirroring model/config.py idioms: env overrides are
empty-safe (`os.environ.get(X) or default`) and evaluated at import time.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1].parent  # model-quality-agent/
FCVAE_ROOT = Path(__file__).resolve().parent            # python/fcvae/

# The sibling FCVAE repo. Data (496 MB labeled CSV) and prebuilt checkpoints
# stay there; this repo references them by path the way the taxi pipeline
# originally referenced the trips parquet.
SIBLING_REPO = Path(os.environ.get("FCVAE_REPO")
                    or (REPO_ROOT.parent / "fcvae-anomaly-detection"))
DATA_CSV = Path(os.environ.get("FCVAE_DATA_CSV")
                or (SIBLING_REPO / "data" / "synthetic_transactions.csv"))
PREBUILT_MODELS = Path(os.environ.get("FCVAE_PREBUILT_MODELS")
                       or (SIBLING_REPO / "models" / "fcvae"))

# Training/export outputs (gitignored). One subdir per model name.
ARTIFACTS = Path(os.environ.get("FCVAE_ARTIFACTS") or (FCVAE_ROOT / "artifacts"))

# Window contract: 24 hourly counts, position [-1] is the scored hour.
WINDOW_SIZE = 24

# The source repo's training seed (data generator and baseline CLI both use 42).
SEED = 42

# Model names: 1 pooled penny-carding model + 4 (network_type, transaction_type)
# combo models. First scope of the adaptation: Penny_All + Accel_CMP.
PENNY_MODEL = "Penny_All"
COMBO_MODELS = {
    "Accel_CMP": ("Accel", "CMP"),
    "Accel_nopin": ("Accel", "no-pin"),
    "Star_CMP": ("Star", "CMP"),
    "Star_nopin": ("Star", "no-pin"),
}
MODEL_NAMES = [PENNY_MODEL, *COMBO_MODELS]

# ONNX contract (what the scorer OP's signature gate will mirror in F1):
#   input  "input" float32 [batch, 1, 24]  ->  output "nll" float32 [batch, 24]
# Scores are log-density shaped: LOWER (more negative) = MORE anomalous, and
# the decision is `last_point_nll < last_point_threshold`.
ONNX_INPUT_NAME = "input"
ONNX_OUTPUT_NAME = "nll"
ONNX_OPSET = 18

GOLDEN_NAME = "golden_windows.jsonl"
MODEL_CONFIG_NAME = "model_config.json"
