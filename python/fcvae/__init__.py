"""FCVAE adaptation package: the Fiserv-style anomaly model (frequency-enhanced
conditional VAE over 24-point hourly transaction-count windows), vendored from
the sibling fcvae-anomaly-detection repo (branch onnx-hybrid-passthrough,
commit 9575ba5) and wrapped in this repo's CLI/config idioms.

Library modules (model, preprocess, scorer, train_lib, training, utils,
metrics) are 1:1 vendored copies with only import rewrites; CLI modules
(train, onnx_export) are new, mirroring `python -m model.*`.
"""

__version__ = "0.1.0"
