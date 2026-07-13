"""Publish the trained model to the volume ModelOp watches (Week 3 Phase 3b).

Takes the artifacts that `model.train fit` and `model.export_onnx export`
produced and publishes the deployable pair -- `model.onnx` plus a
`model.manifest.json` sidecar -- to a target directory (default: the artifacts
dir itself; for a live deploy, point `--out` at Striim's UploadedFiles, the
path ModelOp watches).

The manifest is the audit trail for the Week 5 recommend-then-approve bridge
and the future ModelOp `expectedContract()` sidecar seam. ModelOp itself does
NOT read it today (its identity is the SHA-256 of the model bytes; its gate is
the ONNX signature), so publishing the manifest is purely additive.

Publish REFUSES (non-zero exit, nothing written) unless every gate passes:
  1. all five input artifacts exist (model.onnx, model.json, metrics.json,
     feature_manifest.json, golden_set.jsonl);
  2. the ONNX signature matches ModelOp's gate: input `input` float32
     [-1, n_features], output `variable` float32 [-1, 1];
  3. the golden-set parity check passes (same tolerance as the export gate).

Write order is manifest first, model last, each via temp-then-rename: the
model file is the artifact ModelOp reacts to, so by the time a swap triggers
the manifest describing it is already in place.

Entry point: `uv run python -m model.publish run [--out DIR]`.
The default `--out` honors the MODEL_PUBLISH_OUT env var (the 3c container
sets it to the mounted UploadedFiles volume).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import click
import onnxruntime as ort
import xgboost

from .config import ARTIFACTS, DATA_PROCESSED
from .export_onnx import ONNX_ABS_TOL, ONNX_INPUT_NAME, parity_check
from .features import read_manifest

MANIFEST_NAME = "model.manifest.json"
MODEL_NAME = "model.onnx"
ONNX_OUTPUT_NAME = "variable"   # what convert_xgboost names the regressor output
SCHEMA_VERSION = 1


def _refuse(msg: str) -> None:
    click.echo(f"REFUSED: {msg}")
    sys.exit(1)


def _dims(shape: list) -> list:
    """Normalize an onnxruntime shape (None / symbolic names for dynamic dims)
    to ModelOp's convention: -1 for dynamic, ints for fixed."""
    return [d if isinstance(d, int) else -1 for d in shape]


def _check_signature(onnx_path: Path, n_features: int) -> dict:
    """Mirror ModelOp.validateSignature(): names, dtype, rank, fixed dims.
    Returns the {input, output} manifest fragment on success; refuses on any
    mismatch so a doomed candidate never reaches ModelOp's gate."""
    try:
        sess = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    except Exception as e:  # a file ModelOp could not even open
        _refuse(f"model.onnx does not load as an ONNX session: {e}")
    i, o = sess.get_inputs()[0], sess.get_outputs()[0]
    idims, odims = _dims(i.shape), _dims(o.shape)
    problems = []
    if i.name != ONNX_INPUT_NAME:
        problems.append(f"input name {i.name!r} != {ONNX_INPUT_NAME!r}")
    if i.type != "tensor(float)":
        problems.append(f"input dtype {i.type} != tensor(float)")
    if len(idims) != 2 or idims[1] != n_features:
        problems.append(f"input shape {idims} != [-1, {n_features}]")
    if o.name != ONNX_OUTPUT_NAME:
        problems.append(f"output name {o.name!r} != {ONNX_OUTPUT_NAME!r}")
    if o.type != "tensor(float)":
        problems.append(f"output dtype {o.type} != tensor(float)")
    if len(odims) != 2 or odims[1] != 1:
        problems.append(f"output shape {odims} != [-1, 1]")
    if problems:
        _refuse("signature would be rejected by ModelOp: " + "; ".join(problems))
    return {
        "input": {"name": i.name, "dtype": "float32", "shape": idims},
        "output": {"name": o.name, "dtype": "float32", "shape": odims},
    }


def _write_tmp(path: Path, data: bytes) -> Path:
    """Write `data` fully to a UNIQUE temp file in the target's directory
    (same filesystem, so the later os.replace is atomic; unique name, so
    concurrent publishers cannot truncate each other's temp)."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.chmod(tmp, 0o644)   # mkstemp creates 0600; the published file must be readable
    return Path(tmp)


@click.group()
def cli() -> None:
    """Publish model.onnx + model.manifest.json to the ModelOp volume."""


@cli.command("run")
@click.option("--artifacts", "artifacts_dir", type=click.Path(path_type=Path),
              default=ARTIFACTS, show_default=True,
              help="Directory holding the train/export artifacts.")
@click.option("--out", "out_dir", type=click.Path(path_type=Path),
              default=lambda: Path(os.environ.get("MODEL_PUBLISH_OUT") or str(ARTIFACTS)),
              help="Publish target directory (the path ModelOp watches for a "
                   "live deploy). Default: $MODEL_PUBLISH_OUT or the artifacts dir.")
def cmd_run(artifacts_dir: Path, out_dir: Path) -> None:
    """Gate (artifacts present, signature, parity), then publish atomically."""
    onnx_path = artifacts_dir / MODEL_NAME
    model_path = artifacts_dir / "model.json"
    metrics_path = artifacts_dir / "metrics.json"
    feat_manifest_path = artifacts_dir / "feature_manifest.json"
    golden_path = artifacts_dir / "golden_set.jsonl"

    # Gate 1: all inputs present.
    missing = [p.name for p in (onnx_path, model_path, metrics_path,
                                feat_manifest_path, golden_path) if not p.exists()]
    if missing:
        _refuse(f"missing artifacts in {artifacts_dir}: {', '.join(missing)} "
                "(run `model.train fit` and `model.export_onnx export` first)")

    feat_manifest = json.loads(feat_manifest_path.read_text())
    feature_order = feat_manifest["feature_order"]
    metrics = json.loads(metrics_path.read_text())

    # Gate 2: the exact signature ModelOp validates.
    io_fragment = _check_signature(onnx_path, len(feature_order))

    # Gate 3: golden-set parity, same bound as the export gate. parity_check
    # scores in the CANONICAL feature order (features.read_manifest(), the
    # single train/serve contract), so the order this manifest publishes must
    # BE that order, exactly; and a golden set with zero records would pass
    # the tolerance vacuously, so an empty file is a refusal, not a pass.
    if feature_order != read_manifest()["feature_order"]:
        _refuse("feature_order in the artifacts manifest differs from the "
                "canonical contract (features.read_manifest()); parity would "
                "not verify the order being published")
    with open(golden_path) as f:
        n_golden = sum(1 for line in f if line.strip())
    if n_golden == 0:
        _refuse("golden_set.jsonl has no records; parity would pass vacuously "
                "and the model would ship unverified")
    click.echo("=== publish gates: signature OK; running golden-set parity ===")
    max_err = parity_check(onnx_path=onnx_path, model_path=model_path,
                           golden_path=golden_path)
    if max_err > ONNX_ABS_TOL:
        _refuse(f"parity max abs error {max_err:.2e} exceeds {ONNX_ABS_TOL:.0e}; "
                "not publishing a model that drifted from its booster")

    # Read the model bytes ONCE; the sha, the manifest, and the published file
    # all describe these exact bytes (a concurrent re-export between separate
    # reads could otherwise ship bytes the manifest does not match).
    model_bytes = (artifacts_dir / MODEL_NAME).read_bytes()
    onnx_sha = hashlib.sha256(model_bytes).hexdigest()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "model_version": f"sha256:{onnx_sha[:12]}",   # ModelOp's version identity
        "onnx_sha256": onnx_sha,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model_file": MODEL_NAME,
        **io_fragment,
        "feature_order": feature_order,
        "feature_contract": {
            "target": feat_manifest.get("target"),
            "geohash_precision": feat_manifest.get("geohash_precision"),
            "short_window_min": feat_manifest.get("short_window_min"),
            "long_window_min": feat_manifest.get("long_window_min"),
        },
        "training": {
            "rows": {"train": metrics.get("n_train"), "val": metrics.get("n_val"),
                     "test": metrics.get("n_test")},
            "best_iteration": metrics.get("best_iteration"),
            "params": metrics.get("params"),
            "params_source": ("best_params.json"
                              if (artifacts_dir / "best_params.json").exists()
                              else "defaults"),
            "xgboost_version": xgboost.__version__,
        },
        "metrics": metrics.get("xgb_test"),
        "parity": {"checked": n_golden, "max_abs_err": max_err,
                   "tol": ONNX_ABS_TOL, "pass": True},
        # Week 3 decision 1 (docs/PLAN-week3.md): training reads the file
        # source behind the Feast offline store, not get_historical_features().
        "data_source": {
            "type": "file",
            "trips": str(DATA_PROCESSED / "trips_cleaned.parquet"),
            "features": str(DATA_PROCESSED / "trip_features.parquet"),
            "note": "file source behind the Feast offline store (pragmatic "
                    "Week 3 scope; literal offline retrieval deferred)",
        },
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    # Two-phase: write BOTH temp files fully first (so a failed model write,
    # e.g. ENOSPC, cannot leave a fresh manifest describing a model that never
    # arrived), then rename manifest first and model last: the model file is
    # the swap trigger, so the manifest describing it is in place before
    # ModelOp reacts.
    manifest_tmp = _write_tmp(out_dir / MANIFEST_NAME,
                              (json.dumps(manifest, indent=2) + "\n").encode())
    try:
        model_tmp = _write_tmp(out_dir / MODEL_NAME, model_bytes)
    except BaseException:
        manifest_tmp.unlink(missing_ok=True)
        raise
    os.replace(manifest_tmp, out_dir / MANIFEST_NAME)
    click.echo(f"[write] {out_dir / MANIFEST_NAME}")
    os.replace(model_tmp, out_dir / MODEL_NAME)
    click.echo(f"[write] {out_dir / MODEL_NAME}  (sha256:{onnx_sha[:12]})")
    click.echo(f"published {manifest['model_version']} "
               f"(parity max abs err {max_err:.2e}, {n_golden} golden vectors)")


if __name__ == "__main__":
    cli()
