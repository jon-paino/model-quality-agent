"""FCVAE ONNX export, golden-set freezing, and ONNX Runtime parity harness.

FCVAEInferenceWrapper is vendored verbatim from fcvae-anomaly-detection
src/onnx_export.py (commit 9575ba5); the export / companion-json / parity flow
absorbs code/5_export_onnx.py from the same commit, with the source's silent
random-window fallback replaced by hard errors and the PyTorch reference NLL
frozen into golden_windows.jsonl so `check` re-verifies torch-free.

Entry points:
    uv run python -m fcvae.onnx_export export --model Penny_All
    uv run python -m fcvae.onnx_export check --onnx-path ... --golden-path ...
"""

from __future__ import annotations

import json
import pickle
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import click
import numpy as np

from .compat import install_pickle_shims
from .config import (ARTIFACTS, COMBO_MODELS, DATA_CSV, GOLDEN_NAME, MODEL_CONFIG_NAME,
                     MODEL_NAMES, ONNX_INPUT_NAME, ONNX_OPSET, ONNX_OUTPUT_NAME, PENNY_MODEL,
                     SEED, WINDOW_SIZE)
from .metrics import point_adjusted_f1

if TYPE_CHECKING:
    from .model import FCVAE

# torch backs only the export path. `check` must stay runnable torch-free (the F1
# publish gate re-verifies with json/numpy/onnxruntime alone), so a missing torch
# disables the wrapper/export without breaking the module import.
try:
    import torch
    import torch.nn as nn
except ModuleNotFoundError:
    torch = None
    nn = types.SimpleNamespace(Module=object)

# PROVISIONAL F0 gates, carried over from the source repo's parity harness.
MAX_ABS_TOL = 1.0
MEAN_ABS_TOL = 1e-2
AGREEMENT_MIN = 0.999

# Candidate-quality evaluation written at export time; publish only records it
# (so publish stays torch-free and data-free).
METRICS_NAME = "metrics.json"


class FCVAEInferenceWrapper(nn.Module):
    """Deterministic inference-only wrapper for ONNX export.

    Replaces reparameterization sampling with z = mu (posterior mean),
    collapsing the n_samples averaging loop to a single forward pass.
    """

    def __init__(self, model: FCVAE):
        super().__init__()
        self.model = model

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Deterministic forward pass returning per-point NLL scores.

        Args:
            x: Input tensor (B, 1, W)

        Returns:
            nll: Per-point negative log-likelihood (B, W).
                 Lower (more negative) = more anomalous.
        """
        condition = self.model.get_condition(x)
        mu, var = self.model.encode(torch.cat((x, condition), dim=2))
        z = mu  # deterministic — no reparameterization sampling
        mu_x, var_x = self.model.decode(
            torch.cat((z, condition.squeeze(1)), dim=1)
        )
        nll = -0.5 * (torch.log(var_x) + (x - mu_x) ** 2 / var_x)
        return nll.squeeze(1)


def _check_tensor(value_info, name: str, dims: list, role: str) -> None:
    import onnx

    problems = []
    if value_info.name != name:
        problems.append(f"name {value_info.name!r} != {name!r}")
    ttype = value_info.type.tensor_type
    if ttype.elem_type != onnx.TensorProto.FLOAT:
        problems.append(f"elem_type {ttype.elem_type} != FLOAT (float32)")
    actual = ttype.shape.dim
    if len(actual) != len(dims):
        problems.append(f"rank {len(actual)} != {len(dims)}")
    else:
        for i, want in enumerate(dims):
            fixed = actual[i].WhichOneof("value") == "dim_value"
            if want is None:
                if fixed:
                    problems.append(f"dim[{i}] fixed at {actual[i].dim_value}, expected dynamic")
            elif not fixed or actual[i].dim_value != want:
                got = actual[i].dim_value if fixed else (actual[i].dim_param or "?")
                problems.append(f"dim[{i}] {got} != {want}")
    if problems:
        raise RuntimeError(f"ONNX {role} signature mismatch: " + "; ".join(problems))


def check_onnx_signature(onnx_model, window: int = WINDOW_SIZE) -> None:
    """Assert the graph matches the config contract: input ONNX_INPUT_NAME float32
    [dynamic, 1, window] -> output ONNX_OUTPUT_NAME float32 [dynamic, window].

    This is the seam the F1 publish gate reuses as its signature gate.
    """
    graph = onnx_model.graph
    init_names = {init.name for init in graph.initializer}
    inputs = [i for i in graph.input if i.name not in init_names]
    if len(inputs) != 1 or len(graph.output) != 1:
        raise RuntimeError(
            f"Expected exactly 1 graph input and 1 output, "
            f"got {len(inputs)} inputs / {len(graph.output)} outputs"
        )
    _check_tensor(inputs[0], ONNX_INPUT_NAME, [None, 1, window], "input")
    _check_tensor(graph.output[0], ONNX_OUTPUT_NAME, [None, window], "output")


def export_to_onnx(model, out_path: Path, window: int = WINDOW_SIZE,
                   opset_version: int = ONNX_OPSET) -> Path:
    """Export an FCVAE model to a single-file, contract-checked .onnx."""
    import onnx

    model.eval()
    wrapper = FCVAEInferenceWrapper(model).eval()
    dummy = torch.randn(1, 1, window)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    torch.onnx.export(
        wrapper,
        (dummy,),
        str(out_path),
        input_names=[ONNX_INPUT_NAME],
        output_names=[ONNX_OUTPUT_NAME],
        dynamic_axes={ONNX_INPUT_NAME: {0: "batch"}, ONNX_OUTPUT_NAME: {0: "batch"}},
        opset_version=opset_version,
        # torch 2.11 defaults dynamo=True AND external_data=True, which splits the
        # weights into a model.onnx.data sidecar: exactly the multi-file layout the
        # single-file ModelOp/publish contract forbids. Pin both explicitly.
        dynamo=True,
        external_data=False,
    )

    sidecar = Path(str(out_path) + ".data")
    if sidecar.exists():
        raise RuntimeError(
            f"External-data sidecar exists despite external_data=False: {sidecar}; "
            "the deployment contract is a single .onnx file"
        )

    onnx_model = onnx.load(str(out_path))
    onnx.checker.check_model(onnx_model)
    check_onnx_signature(onnx_model, window=window)
    return out_path


def load_checkpoint(model_dir: Path, device):
    """Load a trained FCVAE from model_dir/model.pt in eval mode.

    Ported from code/5_export_onnx.py load_model: config-in-checkpoint with an
    FCVAEConfig() fallback for raw state_dicts.
    """
    install_pickle_shims()
    import torch
    from .model import FCVAE, FCVAEConfig

    ckpt_path = Path(model_dir) / "model.pt"
    if not ckpt_path.exists():
        raise FileNotFoundError(f"model.pt not found in {model_dir}")
    checkpoint = torch.load(ckpt_path, map_location=device, weights_only=False)

    if "config" in checkpoint or "model_config" in checkpoint:
        config = checkpoint.get("config") or checkpoint.get("model_config")
        model = FCVAE(config).to(device)
        model.load_state_dict(checkpoint["model_state_dict"])
    else:
        config = FCVAEConfig()
        model = FCVAE(config).to(device)
        model.load_state_dict(checkpoint)

    model.eval()
    return model, config


def load_threshold(model_dir: Path) -> float:
    """Last-point decision threshold from scorer.pkl, source semantics:
    last_point_threshold falling back to point_threshold. Hard error otherwise."""
    install_pickle_shims()
    from .scorer import FCVAEScorer

    scorer_path = Path(model_dir) / "scorer.pkl"
    if not scorer_path.exists():
        raise FileNotFoundError(
            f"scorer.pkl not found in {model_dir}; the golden set needs its decision threshold"
        )
    scorer = FCVAEScorer.load(scorer_path)
    threshold = scorer.last_point_threshold or scorer.point_threshold
    if threshold is None:
        raise RuntimeError(
            f"{scorer_path} has neither last_point_threshold nor point_threshold set"
        )
    return float(threshold)


def _load_test_split(model_name: str, model_dir: Path, data_csv: Path):
    """FULL test split for a model, normalized with the model's scaler.

    Extracted from build_golden_set so cmd_export can evaluate candidate
    quality over the whole split while the golden subsample is derived from
    the very same arrays. No silent random fallback: a missing CSV, missing
    scaler, or empty test split is a hard error.

    Returns (hourly_df, test_windows, normalized_windows, test_labels).
    """
    install_pickle_shims()
    from .preprocess import (create_sliding_windows, create_splits, load_combo_data,
                             load_penny_data)

    data_csv = Path(data_csv)
    if not data_csv.exists():
        raise FileNotFoundError(f"Data CSV not found: {data_csv}")
    scaler_path = Path(model_dir) / "scaler.pkl"
    if not scaler_path.exists():
        raise FileNotFoundError(f"scaler.pkl not found in {model_dir}")
    with open(scaler_path, "rb") as f:
        scaler = pickle.load(f)

    if model_name == PENNY_MODEL:
        hourly_df = load_penny_data(data_csv)
    else:
        hourly_df = load_combo_data(data_csv)[COMBO_MODELS[model_name]]

    windows, labels, timestamps = create_sliding_windows(hourly_df)
    splits = create_splits(windows, labels, timestamps, hourly_df)
    test_windows, test_labels = splits["test"]
    if len(test_windows) == 0:
        raise RuntimeError(
            f"Empty test split for {model_name}: cannot freeze golden windows "
            f"(check {data_csv} split column / date coverage)"
        )

    flat = test_windows.flatten().reshape(-1, 1)
    normalized = scaler.transform(flat).reshape(test_windows.shape)
    return hourly_df, test_windows, normalized, test_labels


def _golden_subsample(test_windows: np.ndarray, normalized: np.ndarray,
                      test_labels: np.ndarray, n_golden: int, seed: int):
    """Deterministic golden subsample of the full test split. Same RNG stream
    and selection as the original build_golden_set, so a re-export reproduces
    the frozen golden JSONL byte-for-byte given the same checkpoint."""
    if len(normalized) > n_golden:
        indices = np.random.default_rng(seed).choice(len(normalized), n_golden, replace=False)
    else:
        indices = np.arange(len(normalized))
    return indices, test_windows[indices], normalized[indices], test_labels[indices]


def build_golden_set(model_name: str, model_dir: Path, data_csv: Path,
                     n_golden: int, seed: int):
    """TEST-split windows for the golden set, normalized with the model's scaler.

    Unlike the source's load_test_windows there is NO silent random fallback:
    a missing CSV, missing scaler, or empty test split is a hard error.

    Returns (indices, raw_windows, normalized_windows, labels); indices are
    positions in the test-split window array.
    """
    _, test_windows, normalized, test_labels = _load_test_split(model_name, model_dir, data_csv)
    return _golden_subsample(test_windows, normalized, test_labels, n_golden, seed)


def freeze_expectations(model, normalized: np.ndarray) -> np.ndarray:
    """PyTorch reference NLL (N, W) over the normalized windows; torch.no_grad,
    CPU model expected for a deterministic reference."""
    wrapper = FCVAEInferenceWrapper(model).eval()
    x = torch.FloatTensor(normalized).unsqueeze(1)
    with torch.no_grad():
        return wrapper(x).cpu().numpy()


def write_golden_set(path: Path, model_name: str, indices, raw, normalized,
                     expected_nll, threshold: float, labels) -> None:
    with open(path, "w") as f:
        for i, idx in enumerate(indices):
            last_nll = float(expected_nll[i, -1])
            rec = {
                "model_name": model_name,
                "index": int(idx),
                "raw_values": [float(v) for v in raw[i]],
                "normalized_values": [float(v) for v in normalized[i]],
                "expected_nll": [float(v) for v in expected_nll[i]],
                "expected_last_point_nll": last_nll,
                "threshold": threshold,
                "expected_is_anomaly": bool(last_nll < threshold),
                "last_point_label": int(labels[i, -1]),
            }
            f.write(json.dumps(rec) + "\n")
    click.echo(f"[write] {path}  ({len(indices)} golden windows)")


def export_companion_json(model_dir: Path, model_name: str, config, out_dir: Path) -> None:
    """Write MODEL_CONFIG_NAME with scaler params, thresholds, and ONNX metadata.

    Ported from code/5_export_onnx.py export_companion_json (same keys), writing
    to out_dir instead of model_dir.
    """
    install_pickle_shims()
    from .scorer import FCVAEScorer

    result = {
        "model_name": model_name,
        "onnx": {
            "opset_version": ONNX_OPSET,
            "window_size": config.window,
            "input_name": ONNX_INPUT_NAME,
            "output_name": ONNX_OUTPUT_NAME,
            "input_shape": ["batch", 1, config.window],
            "output_shape": ["batch", config.window],
        },
    }

    scaler_path = Path(model_dir) / "scaler.pkl"
    if scaler_path.exists():
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)
        result["scaler"] = {
            "type": "StandardScaler",
            "mean": float(scaler.mean_[0]),
            "scale": float(scaler.scale_[0]),
        }

    scorer_path = Path(model_dir) / "scorer.pkl"
    if scorer_path.exists():
        scorer = FCVAEScorer.load(scorer_path)
        result["thresholds"] = {
            "last_point_threshold": scorer.last_point_threshold,
            "point_threshold": scorer.point_threshold,
            "window_threshold": scorer.window_threshold,
        }
        result["scorer"] = {
            "normal_score_mean": scorer.normal_score_mean,
            "normal_score_std": scorer.normal_score_std,
        }

    json_path = Path(out_dir) / MODEL_CONFIG_NAME
    with open(json_path, "w") as f:
        json.dump(result, f, indent=2)
    click.echo(f"[write] {json_path}")


def load_golden(path: Path) -> list[dict]:
    with open(path) as f:
        records = [json.loads(line) for line in f if line.strip()]
    if not records:
        raise RuntimeError(f"No golden records in {path}")
    return records


def run_parity(onnx_path: Path, records: list[dict], max_abs_tol: float = MAX_ABS_TOL,
               mean_abs_tol: float = MEAN_ABS_TOL,
               agreement_min: float = AGREEMENT_MIN,
               gates_label: str = "PROVISIONAL F0 gates (source repo)") -> tuple[dict, bool]:
    """ONNX Runtime vs the frozen golden expectations. Torch-free (json/numpy/
    onnxruntime only) so the F1 publish gate can rerun it without the training
    stack. Returns (metrics, gates_passed)."""
    import onnxruntime as ort

    sess = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    x = np.asarray([r["normalized_values"] for r in records], dtype=np.float32)[:, np.newaxis, :]
    expected = np.asarray([r["expected_nll"] for r in records], dtype=np.float64)
    thresholds = np.asarray([r["threshold"] for r in records], dtype=np.float64)
    expected_decisions = np.asarray([r["expected_is_anomaly"] for r in records], dtype=bool)

    ort_scores = sess.run([ONNX_OUTPUT_NAME], {ONNX_INPUT_NAME: x})[0].astype(np.float64)
    abs_diff = np.abs(ort_scores - expected)
    agreement = float(np.mean((ort_scores[:, -1] < thresholds) == expected_decisions))

    single = sess.run([ONNX_OUTPUT_NAME], {ONNX_INPUT_NAME: x[:1]})[0].astype(np.float64)
    single_max = float(np.abs(single[0] - expected[0]).max())

    metrics = {
        "model_name": records[0]["model_name"],
        "n": len(records),
        "max_abs_diff": float(abs_diff.max()),
        "mean_abs_diff": float(abs_diff.mean()),
        "decision_agreement": agreement,
        "single_window_max_abs": single_max,
    }

    click.echo(
        f"PARITY model={metrics['model_name']} n={metrics['n']} "
        f"max_abs={metrics['max_abs_diff']:.3e} mean_abs={metrics['mean_abs_diff']:.3e} "
        f"agreement={agreement:.4f} single_window_max_abs={single_max:.3e}"
    )

    failures = []
    if agreement < agreement_min:
        failures.append(f"agreement {agreement:.4f} < {agreement_min}")
    if metrics["max_abs_diff"] > max_abs_tol:
        failures.append(f"max_abs {metrics['max_abs_diff']:.3e} > {max_abs_tol}")
    if metrics["mean_abs_diff"] > mean_abs_tol:
        failures.append(f"mean_abs {metrics['mean_abs_diff']:.3e} > {mean_abs_tol}")

    gates = (f"{gates_label}: agreement >= {agreement_min}, "
             f"max_abs <= {max_abs_tol}, mean_abs <= {mean_abs_tol}")
    if failures:
        click.echo(f"FAIL {gates}")
        for msg in failures:
            click.echo(f"  {msg}")
    else:
        click.echo(f"PASS {gates}")
    return metrics, not failures


def evaluate_candidate(onnx_path: Path, normalized_test: np.ndarray, test_labels: np.ndarray,
                       threshold: float, hourly_df, data_csv: Path,
                       batch_size: int = 256) -> dict:
    """Candidate-quality evaluation over the FULL test split, scored through
    onnxruntime (CPU EP, batched). Runs at export time and lands in
    metrics.json, so publish stays torch-free and data-free: it only records
    this object into the manifest.

    Decision series: preds = nll[:, -1] < threshold vs ground truth
    test_labels[:, -1]. The test windows are stride-1 and time-ordered, so the
    last-point series IS the hourly series and point_adjusted_f1 over it is
    the DONUT/FCVAE-paper segment metric on the test hours.

    Returns the metrics.json "eval" object.
    """
    import onnxruntime as ort

    sess = ort.InferenceSession(str(onnx_path), providers=["CPUExecutionProvider"])
    x = np.asarray(normalized_test, dtype=np.float32)[:, np.newaxis, :]
    chunks = [sess.run([ONNX_OUTPUT_NAME], {ONNX_INPUT_NAME: x[i:i + batch_size]})[0]
              for i in range(0, len(x), batch_size)]
    nll = np.concatenate(chunks, axis=0).astype(np.float64)

    preds = nll[:, -1] < threshold
    gt = np.asarray(test_labels)[:, -1].astype(bool)
    tp = int(np.sum(preds & gt))
    fp = int(np.sum(preds & ~gt))
    fn = int(np.sum(~preds & gt))
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    if "split" in hourly_df.columns:
        test_hours = hourly_df.loc[hourly_df["split"] == "test", "hour_bucket"]
    else:
        # Day-based fallback boundary, mirroring preprocess.create_splits
        # (test starts at hour 50 * 24 when the CSV has no split column).
        test_hours = hourly_df["hour_bucket"].iloc[50 * 24:]

    return {
        "split": "test",
        "n_windows": int(len(x)),
        "n_anomalous_last_points": int(gt.sum()),
        "threshold": float(threshold),
        "raw_last_point": {"precision": precision, "recall": recall, "f1": f1,
                           "tp": tp, "fp": fp, "fn": fn},
        "point_adjusted_last_point": point_adjusted_f1(preds, gt),
        "scored_with": "onnxruntime",
        "data_csv": str(data_csv),
        "data_span": {"start": test_hours.min().isoformat(),
                      "end": test_hours.max().isoformat()},
    }


@click.group()
def cli() -> None:
    """Export FCVAE checkpoints to ONNX and verify ORT parity vs a frozen golden set."""


@cli.command("export")
@click.option("--model", "model_name", type=click.Choice(MODEL_NAMES), required=True,
              help="Which model's artifacts to export.")
@click.option("--model-dir", type=click.Path(path_type=Path), default=None,
              help="Dir holding model.pt/scaler.pkl/scorer.pkl [default: ARTIFACTS/<model>; "
                   "may point at the sibling repo's prebuilt dir].")
@click.option("--out-dir", type=click.Path(path_type=Path), default=None,
              help="Where model.onnx, model_config.json, and the golden set land "
                   "[default: ARTIFACTS/<model>].")
@click.option("--data-csv", type=click.Path(path_type=Path), default=DATA_CSV,
              show_default=True, help="Labeled transactions CSV for the golden test split.")
@click.option("--n-golden", type=int, default=100, show_default=True,
              help="Golden windows to freeze (subsampled from the test split).")
@click.option("--seed", type=int, default=SEED, show_default=True,
              help="RNG seed for the golden subsample.")
def cmd_export(model_name: str, model_dir: Path, out_dir: Path, data_csv: Path,
               n_golden: int, seed: int) -> None:
    """Load a checkpoint, export single-file ONNX, freeze the golden set, run parity."""
    import torch

    model_dir = model_dir or ARTIFACTS / model_name
    out_dir = out_dir or ARTIFACTS / model_name
    out_dir.mkdir(parents=True, exist_ok=True)

    model, cfg = load_checkpoint(model_dir, torch.device("cpu"))
    onnx_path = export_to_onnx(model, out_dir / "model.onnx", window=cfg.window)
    click.echo(f"[write] {onnx_path}")
    export_companion_json(model_dir, model_name, cfg, out_dir)

    threshold = load_threshold(model_dir)
    hourly_df, test_windows, normalized_full, test_labels = _load_test_split(
        model_name, model_dir, data_csv)
    indices, raw, normalized, labels = _golden_subsample(
        test_windows, normalized_full, test_labels, n_golden, seed)
    expected = freeze_expectations(model, normalized)
    golden_path = out_dir / GOLDEN_NAME
    write_golden_set(golden_path, model_name, indices, raw, normalized, expected,
                     threshold, labels)

    _, ok = run_parity(onnx_path, load_golden(golden_path))
    if not ok:
        sys.exit(1)

    eval_obj = evaluate_candidate(onnx_path, normalized_full, test_labels, threshold,
                                  hourly_df, data_csv)
    metrics = {
        "model_name": model_name,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "eval": eval_obj,
    }
    metrics_path = out_dir / METRICS_NAME
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
        f.write("\n")
    click.echo(f"[write] {metrics_path}")
    click.echo(
        f"EVAL model={model_name} n={eval_obj['n_windows']} "
        f"raw_f1={eval_obj['raw_last_point']['f1']:.4f} "
        f"pa_f1={eval_obj['point_adjusted_last_point']['f1']:.4f} "
        f"threshold={eval_obj['threshold']}"
    )


@cli.command("check")
@click.option("--onnx-path", type=click.Path(path_type=Path), required=True,
              help="Exported model.onnx to re-verify.")
@click.option("--golden-path", type=click.Path(path_type=Path), required=True,
              help="Frozen golden_windows.jsonl to verify against.")
@click.option("--max-abs", type=float, default=MAX_ABS_TOL, show_default=True,
              help="Max absolute NLL diff tolerance.")
@click.option("--mean-abs", type=float, default=MEAN_ABS_TOL, show_default=True,
              help="Mean absolute NLL diff tolerance.")
def cmd_check(onnx_path: Path, golden_path: Path, max_abs: float, mean_abs: float) -> None:
    """ORT-only re-verification of an existing export (torch-free)."""
    if not onnx_path.exists():
        raise FileNotFoundError(f"ONNX file not found: {onnx_path}")
    _, ok = run_parity(onnx_path, load_golden(golden_path),
                       max_abs_tol=max_abs, mean_abs_tol=mean_abs)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    cli()
