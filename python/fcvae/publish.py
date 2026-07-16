"""Publish an exported FCVAE candidate to the volume the scorer OP watches (F1).

Takes the artifacts `fcvae.onnx_export export` produced (model.onnx,
model_config.json, golden_windows.jsonl, metrics.json) and publishes the
deployable set, plus a model.manifest.json sidecar, to a target directory
(default: the artifacts dir itself; for a live deploy, point `--out` at the
path the scorer OP watches).

Mirrors model/publish.py from the taxi pipeline: torch-free and data-free
(the candidate-quality evaluation was computed at export time and is only
RECORDED here, never recomputed), refuse-don't-publish gates, and an atomic
temp-then-rename write with the model file last since it is the OP's swap
trigger.

Publish REFUSES (exit 1, nothing written) unless every gate passes:
  1. all four input artifacts exist;
  2. the ONNX signature matches the scorer OP's gate (input `input` float32
     [-1, 1, 24] -> output `nll` float32 [-1, 24]) AND the file loads as an
     onnxruntime session;
  3. model_config.json is sane: thresholds.last_point_threshold a finite
     number, scaler.mean/scale finite with scale != 0 (mirrors the Java
     validateConfig gate);
  4. the golden set is non-empty;
  5. golden-set parity passes the PUBLISH gates (stricter than the F0 export
     gates, see the constants below);
  6. metrics.json parses and carries eval.point_adjusted_last_point.f1.

Entry point: `uv run python -m fcvae.publish run --model <name>`.
The default `--out` honors the FCVAE_PUBLISH_OUT env var, else the artifacts
dir (taxi convention).
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import click
import onnx
import onnxruntime as ort

from .config import (ARTIFACTS, GOLDEN_NAME, MODEL_CONFIG_NAME, MODEL_NAMES, ONNX_INPUT_NAME,
                     ONNX_OUTPUT_NAME, WINDOW_SIZE)
from .onnx_export import METRICS_NAME, check_onnx_signature, run_parity

MANIFEST_NAME = "model.manifest.json"
MODEL_FILE_NAME = "model.onnx"
SCHEMA_VERSION = 1

# PUBLISH parity gates, EMPIRICAL (F0 measurements on the prebuilt Penny_All
# export: max_abs ~1e-1 concentrated in one saturated golden window, mean_abs
# ~1e-4, decision agreement exactly 1.0). Tighter than the provisional F0
# export gates on agreement (any decision flip refuses) and mean_abs; max_abs
# keeps the F0 headroom for the known single-window outlier.
PUBLISH_AGREEMENT_MIN = 1.0
PUBLISH_MEAN_ABS_TOL = 1e-3
PUBLISH_MAX_ABS_TOL = 1.0


def _refuse(msg: str) -> None:
    click.echo(f"REFUSED: {msg}")
    sys.exit(1)


def _finite_number(v) -> bool:
    """A real, finite JSON number (bool is an int in Python; exclude it)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _write_tmp(path: Path, data: bytes) -> Path:
    """Write `data` fully to a UNIQUE temp file in the target's directory
    (same filesystem, so the later os.replace is atomic; unique name, so
    concurrent publishers cannot truncate each other's temp). Cleans up its
    own temp on a failed write."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.chmod(tmp, 0o644)   # mkstemp creates 0600; the published file must be readable
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return Path(tmp)


def publish(model_name: str, artifacts_dir: Path, out_dir: Path) -> None:
    """Gate the artifacts, build the manifest, publish atomically to out_dir."""
    onnx_path = artifacts_dir / MODEL_FILE_NAME
    config_path = artifacts_dir / MODEL_CONFIG_NAME
    golden_path = artifacts_dir / GOLDEN_NAME
    metrics_path = artifacts_dir / METRICS_NAME

    # Gate 1: all inputs present.
    missing = [p.name for p in (onnx_path, config_path, golden_path, metrics_path)
               if not p.exists()]
    if missing:
        _refuse(f"missing artifacts in {artifacts_dir}: {', '.join(missing)} "
                "(run `fcvae.onnx_export export` first)")

    # Gate 2: the exact signature the scorer OP validates, then an actual
    # onnxruntime session load (a file the OP could not even open must never
    # reach its volume).
    try:
        onnx_model = onnx.load(str(onnx_path))
        check_onnx_signature(onnx_model, window=WINDOW_SIZE)
    except Exception as e:
        _refuse(f"ONNX signature gate failed: {e}")
    try:
        ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    except Exception as e:
        _refuse(f"model.onnx does not load as an onnxruntime session: {e}")

    # Gate 3: config sanity, mirroring the Java validateConfig gate. The OP
    # normalizes with (x - mean) / scale and decides nll < last_point_threshold,
    # so a missing/NaN/inf threshold or a zero scale is a doomed deploy.
    config_bytes = config_path.read_bytes()
    try:
        model_config = json.loads(config_bytes)
    except (ValueError, UnicodeDecodeError) as e:
        _refuse(f"{MODEL_CONFIG_NAME} does not parse as JSON: {e}")
    thresholds = model_config.get("thresholds") or {}
    last_point = thresholds.get("last_point_threshold")
    if not _finite_number(last_point):
        _refuse(f"thresholds.last_point_threshold is not a finite number: {last_point!r}")
    scaler = model_config.get("scaler") or {}
    mean, scale = scaler.get("mean"), scaler.get("scale")
    if not _finite_number(mean) or not _finite_number(scale) or scale == 0:
        _refuse("scaler.mean/scale invalid (need finite numbers, scale != 0): "
                f"mean={mean!r} scale={scale!r}")

    # Gate 4: golden set non-empty. An empty file would pass parity vacuously
    # and the model would ship unverified.
    golden_bytes = golden_path.read_bytes()
    golden_lines = [ln for ln in golden_bytes.decode("utf-8").splitlines() if ln.strip()]
    n_golden = len(golden_lines)
    if n_golden == 0:
        _refuse(f"{GOLDEN_NAME} has no records; parity would pass vacuously "
                "and the model would ship unverified")

    # Gate 5: golden-set parity under the PUBLISH gates (torch-free rerun).
    records = [json.loads(ln) for ln in golden_lines]
    click.echo("=== publish gates: presence, signature, config, golden OK; "
               "running golden-set parity ===")
    parity_metrics, ok = run_parity(
        onnx_path, records,
        max_abs_tol=PUBLISH_MAX_ABS_TOL, mean_abs_tol=PUBLISH_MEAN_ABS_TOL,
        agreement_min=PUBLISH_AGREEMENT_MIN,
        gates_label="PUBLISH gates (empirical, F0 measurements)")
    if not ok:
        _refuse("golden-set parity failed the publish gates; not publishing a "
                "model that drifted from its frozen reference")

    # Gate 6: the export-time candidate evaluation exists and is usable.
    try:
        metrics = json.loads(metrics_path.read_text())
    except (ValueError, UnicodeDecodeError) as e:
        _refuse(f"{METRICS_NAME} does not parse as JSON: {e}")
    eval_obj = metrics.get("eval") or {}
    pa_f1 = (eval_obj.get("point_adjusted_last_point") or {}).get("f1")
    if not _finite_number(pa_f1):
        _refuse(f"{METRICS_NAME} lacks a numeric eval.point_adjusted_last_point.f1 "
                "(re-run `fcvae.onnx_export export`)")

    # Read the model bytes ONCE; the sha, the manifest, and the published file
    # all describe these exact bytes (a concurrent re-export between separate
    # reads could otherwise ship bytes the manifest does not match).
    model_bytes = onnx_path.read_bytes()
    onnx_sha = hashlib.sha256(model_bytes).hexdigest()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "model_version": f"sha256:{onnx_sha[:12]}",   # the OP's version identity
        "onnx_sha256": onnx_sha,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model_file": MODEL_FILE_NAME,
        "model_name": model_name,
        "input": {"name": ONNX_INPUT_NAME, "dtype": "float32", "shape": [-1, 1, WINDOW_SIZE]},
        "output": {"name": ONNX_OUTPUT_NAME, "dtype": "float32", "shape": [-1, WINDOW_SIZE]},
        # Copied verbatim from model_config.json: the serve-side contract.
        "scaler": model_config.get("scaler"),
        "thresholds": model_config.get("thresholds"),
        # Recorded verbatim from metrics.json: the export-time evaluation.
        "eval": eval_obj,
        "parity": {
            "checked": parity_metrics["n"],
            "max_abs_err": parity_metrics["max_abs_diff"],
            "mean_abs_err": parity_metrics["mean_abs_diff"],
            "decision_agreement": parity_metrics["decision_agreement"],
            "gates": {"agreement_min": PUBLISH_AGREEMENT_MIN,
                      "mean_abs_tol": PUBLISH_MEAN_ABS_TOL,
                      "max_abs_tol": PUBLISH_MAX_ABS_TOL},
            "pass": True,
        },
        "data_source": {"type": "file", "csv": eval_obj.get("data_csv")},
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    # Two-phase publish: write ALL temp files fully first (a failed write,
    # e.g. ENOSPC, must not leave a fresh manifest describing a model that
    # never arrived), then rename in dependency order:
    #   model.manifest.json FIRST, model_config.json SECOND, the golden set
    #   third, model.onnx LAST. The model file is the OP's swap trigger, so
    #   by the time its mtime fires the config the OP needs is already in
    #   place and the manifest/golden set describing it are readable (golden
    #   before model keeps the published dir self-verifying via
    #   `onnx_export check`).
    targets = [
        (out_dir / MANIFEST_NAME, (json.dumps(manifest, indent=2) + "\n").encode()),
        (out_dir / MODEL_CONFIG_NAME, config_bytes),
        (out_dir / GOLDEN_NAME, golden_bytes),
        (out_dir / MODEL_FILE_NAME, model_bytes),
    ]
    temps = []
    try:
        for path, data in targets:
            temps.append((_write_tmp(path, data), path))
    except BaseException:
        for tmp, _ in temps:
            tmp.unlink(missing_ok=True)
        raise
    for tmp, path in temps:
        os.replace(tmp, path)
        click.echo(f"[write] {path}")
    click.echo(f"published {manifest['model_version']} model={model_name} "
               f"(parity max abs err {parity_metrics['max_abs_diff']:.2e}, "
               f"{n_golden} golden windows, pa_f1={pa_f1:.4f})")


@click.group()
def cli() -> None:
    """Publish model.onnx + config + golden set + manifest to the scorer OP volume."""


@cli.command("run")
@click.option("--model", "model_name", type=click.Choice(MODEL_NAMES), required=True,
              help="Which model's artifacts to publish.")
@click.option("--artifacts", "artifacts_dir", type=click.Path(path_type=Path), default=None,
              help="Directory holding the export artifacts [default: ARTIFACTS/<model>].")
@click.option("--out", "out_dir", type=click.Path(path_type=Path),
              default=lambda: os.environ.get("FCVAE_PUBLISH_OUT") or None,
              help="Publish target directory (the path the scorer OP watches for a "
                   "live deploy). Default: $FCVAE_PUBLISH_OUT or the artifacts dir.")
def cmd_run(model_name: str, artifacts_dir: Path, out_dir: Path) -> None:
    """Gate (presence, signature, config, golden, parity, eval), then publish atomically."""
    if artifacts_dir is None:
        artifacts_dir = ARTIFACTS / model_name
    if out_dir is None:   # no --out and no FCVAE_PUBLISH_OUT: the artifacts dir
        out_dir = artifacts_dir
    publish(model_name, artifacts_dir, out_dir)


if __name__ == "__main__":
    cli()
