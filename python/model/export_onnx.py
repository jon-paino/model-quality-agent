"""Export the trained XGBoost booster to ONNX for the Striim Model OP.

The Model OP scores with ONNX Runtime (Java), not xgboost4j, so the booster
in `artifacts/model.json` must be converted to `artifacts/model.onnx`. The
ONNX graph takes a single `[N, 15]` float tensor; the column order is the
manifest `feature_order` (the OP contract). ONNX carries no feature names, so
the Model OP is responsible for assembling its `[15]` vector in exactly that
order.

After conversion this module runs a parity self-check: every vector in
`golden_set.jsonl` is scored through ONNX Runtime and compared both to the
frozen `expected_prediction` (produced by the XGBoost booster at train time)
and to a fresh XGBoost prediction. XGBoost -> ONNX conversion can drift
slightly from float accumulation; if the max absolute error exceeds the
golden tolerance the check fails loudly rather than absorbing it.

Entry point: `uv run python -m model.export_onnx export`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Iterable

import click
import numpy as np
import onnxruntime as ort
import xgboost as xgb
from onnxmltools.convert import convert_xgboost
from onnxmltools.convert.common.data_types import FloatTensorType

from .config import ARTIFACTS
from .features import read_manifest
from .train import GOLDEN_PATH, MODEL_PATH

ONNX_PATH = ARTIFACTS / "model.onnx"
ONNX_INPUT_NAME = "input"

# Cross-runtime tolerance. oracle.py uses 1e-5 for its XGBoost-vs-XGBoost
# golden test; scoring the same ensemble through ONNX Runtime instead of
# native XGBoost adds float32 accumulation noise across the 694 trees
# (observed floor ~2e-5). 1e-4 keeps ~5x headroom over that floor while still
# catching any real conversion regression (e.g. a truncated ensemble drifts
# by ~5e-2). Phase 4 cross-validation of the Striim ONNX output uses the
# same bound.
ONNX_ABS_TOL = 1e-4


def export(model_path: Path = MODEL_PATH, onnx_path: Path = ONNX_PATH) -> Path:
    """Convert the XGBoost booster at `model_path` to ONNX at `onnx_path`."""
    if not model_path.exists():
        raise FileNotFoundError(f"Run `model.train fit` first ({model_path})")

    manifest = read_manifest()
    n_features = len(manifest["feature_order"])

    # Load through the sklearn wrapper so onnxmltools sees an unambiguous
    # regressor (raw-Booster conversion has to infer regression vs
    # classification from the saved objective; the wrapper makes it explicit).
    reg = xgb.XGBRegressor()
    reg.load_model(str(model_path))
    booster = reg.get_booster()
    if booster.num_features() != n_features:
        raise RuntimeError(
            f"booster has {booster.num_features()} features, "
            f"manifest feature_order has {n_features}"
        )

    # onnxmltools requires the booster's embedded feature names to follow the
    # `f%d` pattern; ours carries the real manifest names (the xgboost4j
    # contract). ONNX scoring is purely positional, so rename the in-memory
    # booster to `f0..fN`. The on-disk model.json is not touched.
    booster.feature_names = [f"f{i}" for i in range(n_features)]

    # onnxmltools truncates the ensemble to `best_iteration + 1` trees
    # (XGBoost.py:55-56). But this project's golden set, oracle.py, and the
    # reported R^2 / MAE metrics all score with the native Booster.predict(),
    # which uses every tree in the file. Clear the best_iteration attribute on
    # the in-memory booster so the full ensemble is converted and the ONNX
    # model matches the deployed contract. The on-disk model.json is unchanged.
    n_trees_file = booster.num_boosted_rounds()
    booster.set_attr(best_iteration=None)

    onnx_model = convert_xgboost(
        reg,
        initial_types=[(ONNX_INPUT_NAME, FloatTensorType([None, n_features]))],
    )

    # Guard: every tree must have survived the conversion.
    treg = next(n for n in onnx_model.graph.node if n.op_type == "TreeEnsembleRegressor")
    treeids = next(a for a in treg.attribute if a.name == "nodes_treeids").ints
    n_trees_onnx = len(set(treeids))
    if n_trees_onnx != n_trees_file:
        raise RuntimeError(
            f"ONNX graph has {n_trees_onnx} trees but the booster has "
            f"{n_trees_file}; the ensemble was truncated during conversion"
        )

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with open(onnx_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    click.echo(
        f"[write] {onnx_path}  "
        f"({n_features}-feature input, {n_trees_onnx} trees)"
    )
    return onnx_path


def _golden_records(path: Path = GOLDEN_PATH) -> Iterable[dict]:
    with open(path) as f:
        for line in f:
            yield json.loads(line)


def parity_check(onnx_path: Path = ONNX_PATH, model_path: Path = MODEL_PATH,
                 golden_path: Path = GOLDEN_PATH) -> float:
    """Score every golden-set vector through ONNX Runtime and report the max
    absolute error against the frozen XGBoost predictions. Returns the max
    error measured against `expected_prediction`.
    """
    feature_order = read_manifest()["feature_order"]

    sess = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    input_name = sess.get_inputs()[0].name

    # Score with XGBoost directly too, to separate genuine ONNX drift from a
    # stale golden set.
    booster = xgb.Booster()
    booster.load_model(str(model_path))

    n = 0
    n_fail = 0
    max_err_golden = 0.0
    max_err_xgb = 0.0
    for rec in _golden_records(golden_path):
        n += 1
        fv = rec["feature_vector"]
        X = np.array([[fv[name] for name in feature_order]], dtype=np.float32)

        onnx_out = np.asarray(sess.run(None, {input_name: X})[0]).reshape(-1)
        onnx_pred = float(onnx_out[0])

        d = xgb.DMatrix(X, feature_names=feature_order)
        xgb_pred = float(booster.predict(d)[0])

        err_golden = abs(onnx_pred - rec["expected_prediction"])
        err_xgb = abs(onnx_pred - xgb_pred)
        max_err_golden = max(max_err_golden, err_golden)
        max_err_xgb = max(max_err_xgb, err_xgb)
        if err_golden > ONNX_ABS_TOL:
            n_fail += 1
            click.echo(
                f"  FAIL  geohash={rec['geohash']}  "
                f"expected={rec['expected_prediction']:.6f}  "
                f"onnx={onnx_pred:.6f}  err={err_golden:.2e}"
            )

    click.echo(
        f"  checked={n}  failed={n_fail}  "
        f"max_abs_err(vs golden)={max_err_golden:.2e}  "
        f"max_abs_err(vs xgboost)={max_err_xgb:.2e}  tol={ONNX_ABS_TOL:.0e}"
    )
    return max_err_golden


@click.group()
def cli() -> None:
    """Export the XGBoost booster to ONNX for the Striim Model OP."""


_MODEL_OPTION = click.option("--model-path", type=click.Path(path_type=Path),
                             default=MODEL_PATH, show_default=True,
                             help="XGBoost booster .json file to convert.")
_ONNX_OPTION = click.option("--onnx-path", type=click.Path(path_type=Path),
                            default=ONNX_PATH, show_default=True,
                            help="Where to write the converted .onnx file.")
_GOLDEN_OPTION = click.option("--golden-path", type=click.Path(path_type=Path),
                              default=GOLDEN_PATH, show_default=True,
                              help="Golden-set JSONL for the parity check.")


@cli.command("export")
@_MODEL_OPTION
@_ONNX_OPTION
@_GOLDEN_OPTION
def cmd_export(model_path: Path, onnx_path: Path, golden_path: Path) -> None:
    """Convert <model>.json -> <onnx>.onnx, then run the golden-set parity check."""
    export(model_path=model_path, onnx_path=onnx_path)
    click.echo("=== ONNX parity check (golden set) ===")
    max_err = parity_check(onnx_path=onnx_path, model_path=model_path,
                           golden_path=golden_path)
    if max_err > ONNX_ABS_TOL:
        click.echo(
            f"\nFAIL: ONNX vs golden max abs error {max_err:.2e} exceeds "
            f"tolerance {ONNX_ABS_TOL:.0e}.\nThis is larger than float32 "
            "accumulation noise and indicates a real conversion regression "
            "(check the converted tree count and base_score)."
        )
        sys.exit(1)
    click.echo(f"ONNX export OK (max abs error {max_err:.2e} < {ONNX_ABS_TOL:.0e})")


@cli.command("check")
@_MODEL_OPTION
@_ONNX_OPTION
@_GOLDEN_OPTION
def cmd_check(model_path: Path, onnx_path: Path, golden_path: Path) -> None:
    """Re-run the parity check against an existing .onnx."""
    if not onnx_path.exists():
        raise FileNotFoundError(f"Run `model.export_onnx export` first ({onnx_path})")
    parity_check(onnx_path=onnx_path, model_path=model_path, golden_path=golden_path)


if __name__ == "__main__":
    cli()
