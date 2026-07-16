"""Training CLI for the vendored FCVAE package.

Absorbs fcvae-anomaly-detection code/1_train_model.py (commit 9575ba5) into
this repo's click idiom, adding the single-model selection (--model) the
source script lacked (its --mode trained Penny_All or all 4 combos per run).
optimize_threshold_f1 is ported verbatim from that script since it does not
exist in the vendored src/ modules.

Usage:
    uv run python -m fcvae.train fit --model Penny_All
    uv run python -m fcvae.train fit --model Accel_CMP --epochs 30 --lr 5e-4
"""

import logging
import random
import warnings
from pathlib import Path

import click
import numpy as np
import torch
from torch.utils.data import DataLoader

from .config import ARTIFACTS, COMBO_MODELS, DATA_CSV, MODEL_NAMES, PENNY_MODEL, SEED, WINDOW_SIZE
from .model import FCVAE, FCVAEConfig
from .preprocess import (
    create_dataloaders,
    create_sliding_windows,
    create_splits,
    load_combo_data,
    load_penny_data,
    normalize,
)
from .scorer import FCVAEScorer, FCVAEScorerConfig
from .train_lib import AugmentConfig
from .training import TrainingConfig, save_training_artifacts, train_model
from .utils import auto_device

# Suppress PyTorch FFT resize deprecation warnings
warnings.filterwarnings("ignore", message=".*output with one or more elements was resized.*")

logger = logging.getLogger(__name__)


def optimize_threshold_f1(
    model: FCVAE,
    scorer: FCVAEScorer,
    val_loader: DataLoader,
    device: torch.device,
    beta: float = 1.0,
) -> dict:
    """Optimize threshold using F1 score on validation set with real anomaly labels.

    Calibrates three thresholds:
    - All-position point threshold
    - Last-point threshold (paper-faithful, position [-1] only)
    - Window-level threshold
    """
    # Collect all validation labels
    all_labels = []
    for batch in val_loader:
        _, labels, _ = batch
        all_labels.append(labels.numpy())
    all_labels = np.concatenate(all_labels)

    # Score all windows
    all_point_scores, _ = scorer.score_batch(model, val_loader, device)

    # --- All-position threshold ---
    flat_scores = all_point_scores.flatten()
    flat_labels = all_labels.flatten().astype(int)

    normal_scores = flat_scores[flat_labels == 0]
    anomaly_scores = flat_scores[flat_labels == 1]

    logger.debug(
        f"  Validation: {len(normal_scores)} normal points, "
        f"{len(anomaly_scores)} anomaly points"
    )

    if len(anomaly_scores) == 0:
        logger.warning("  No anomaly points in validation -- falling back to percentile threshold")
        scorer.set_threshold(flat_scores, method="percentile", percentile=5.0)
        scorer.set_last_point_threshold(all_point_scores, method="percentile", percentile=5.0)
        return {"method": "percentile_fallback", "reason": "no_anomalies"}

    optimal_threshold, metrics = scorer.find_optimal_threshold(
        normal_scores=normal_scores,
        anomaly_scores=anomaly_scores,
        method="f1_max",
        beta=beta,
    )
    scorer.point_threshold = optimal_threshold

    # --- Last-point threshold (paper-faithful) ---
    lp_scores = all_point_scores[:, -1]
    lp_labels = all_labels[:, -1].astype(int)

    lp_normal = lp_scores[lp_labels == 0]
    lp_anomaly = lp_scores[lp_labels == 1]

    logger.debug(f"  Last-point: {len(lp_normal)} normal, {len(lp_anomaly)} anomaly")

    if len(lp_anomaly) > 0:
        lp_threshold, lp_metrics = scorer.find_optimal_last_point_threshold(
            normal_scores=lp_normal,
            anomaly_scores=lp_anomaly,
            method="f1_max",
            beta=beta,
        )
        scorer.last_point_threshold = lp_threshold
        metrics["last_point_threshold"] = lp_threshold
        metrics["last_point_f1"] = lp_metrics.get("f1", 0)
        logger.debug(
            f"  Last-point threshold: {lp_threshold:.4f} "
            f"(vs all-position: {optimal_threshold:.4f})"
        )
    else:
        scorer.set_last_point_threshold(all_point_scores, method="percentile", percentile=5.0)
        metrics["last_point_threshold"] = scorer.last_point_threshold
        metrics["last_point_method"] = "percentile_fallback"

    # --- Window threshold ---
    window_scores = all_point_scores.mean(axis=1)
    window_labels = (all_labels.sum(axis=1) > 0).astype(int)

    normal_window_scores = window_scores[window_labels == 0]
    anomaly_window_scores = window_scores[window_labels == 1]

    if len(anomaly_window_scores) > 0:
        window_threshold, window_metrics = scorer.find_optimal_threshold(
            normal_scores=normal_window_scores,
            anomaly_scores=anomaly_window_scores,
            method="f1_max",
            beta=beta,
        )
        scorer.window_threshold = window_threshold
        metrics["window_threshold"] = window_threshold
        metrics["window_f1"] = window_metrics.get("f1", 0)
    else:
        scorer.set_window_threshold(normal_window_scores, method="percentile", percentile=5.0)
        metrics["window_threshold"] = scorer.window_threshold

    logger.debug(
        f"  Thresholds: all-position={scorer.point_threshold:.4f}, "
        f"last-point={scorer.last_point_threshold:.4f}, "
        f"window={scorer.window_threshold}"
    )

    return metrics


def fit(
    model_name: str,
    data_csv: Path = DATA_CSV,
    out_dir: Path = ARTIFACTS,
    window_size: int = WINDOW_SIZE,
    stride: int = 1,
    latent_dim: int = 4,
    epochs: int = 15,
    lr: float = 1e-3,
    patience: int = 3,
    batch_size: int = 64,
    grad_clip: float = 2.0,
    kl_warmup_epochs: int = 5,
    score_mode: str = "single_pass",
    seed: int = SEED,
    pool_train_val: bool = False,
    augmentation: bool = False,
    device: str | None = None,
    skip_save: bool = False,
) -> dict:
    """Train one FCVAE model end-to-end: data -> train -> calibrate -> evaluate -> save."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)

    dev = auto_device(device)
    save_dir = Path(out_dir) / model_name
    aug_label = "augmentation ON" if augmentation else "no augmentation"

    print("\n" + "=" * 60)
    print(f"FCVAE TRAINING -- {model_name}")
    print(f"Output directory: {save_dir}")
    print(f"Latent dim: {latent_dim}   LR: {lr}   Epochs: {epochs} ({aug_label})")
    print(f"Device: {dev}")
    print("=" * 60)

    print("\n" + "-" * 40)
    print(f"Preprocessing transaction data for {model_name}")
    print("-" * 40)

    if model_name == PENNY_MODEL:
        hourly_df = load_penny_data(data_csv)
    else:
        combo_key = COMBO_MODELS[model_name]
        hourly_df = load_combo_data(data_csv)[combo_key]

    windows, labels, timestamps = create_sliding_windows(
        hourly_df, window_size=window_size, stride=stride
    )
    splits = create_splits(windows, labels, timestamps, hourly_df)

    # Pool train+val if requested
    if pool_train_val:
        train_w, train_l = splits["train"]
        val_w, val_l = splits["val"]
        if len(train_w) > 0 and len(val_w) > 0:
            pooled_w = np.concatenate([train_w, val_w])
            pooled_l = np.concatenate([train_l, val_l])
            splits["train"] = (pooled_w, pooled_l)
            print(f"  Pooled train+val: {len(train_w)} + {len(val_w)} = {len(pooled_w)} windows")

    normalized, scaler = normalize(splits)
    loaders = create_dataloaders(normalized, batch_size=batch_size)

    print(f"\n{'=' * 60}")
    print(f"TRAINING: {model_name}")
    print(f"{'=' * 60}")

    for split_name in ["train", "val", "test"]:
        if loaders.get(split_name) is not None:
            print(f"  {split_name}: {len(loaders[split_name].dataset)} windows")

    # Initialize model
    print("\n--- Initializing FCVAE model ---")
    model_config = FCVAEConfig(window=window_size, latent_dim=latent_dim)
    model = FCVAE(model_config).to(dev)

    num_params = sum(p.numel() for p in model.parameters())
    print(f"  Model parameters: {num_params:,}")
    print(f"  Latent dim: {latent_dim}, Window: {window_size}")

    scorer_config = FCVAEScorerConfig(score_mode=score_mode)
    scorer = FCVAEScorer(config=scorer_config)

    augment_config = AugmentConfig() if augmentation else None

    if augmentation:
        print(f"  Augmentation: point={augment_config.point_ano_rate}, "
              f"segment={augment_config.seg_ano_rate}, "
              f"missing={augment_config.missing_data_rate}")

    # Train using shared helper
    print("\n--- Training ---")
    training_config = TrainingConfig(
        epochs=epochs,
        learning_rate=lr,
        patience=patience,
        kl_warmup_epochs=kl_warmup_epochs,
        grad_clip=grad_clip,
        batch_size=batch_size,
        augmentation=augmentation,
    )

    model, history = train_model(
        model, loaders["train"], loaders["val"], dev,
        config=training_config,
        augment_config=augment_config,
    )

    # Calibrate threshold (F1-max)
    # Try validation set first; if it has no anomalies, fall back to test set
    # for threshold calibration (same approach as the prebuilt models).
    print("\n--- Calibrating threshold (F1-max) ---")
    scorer.fit(model, loaders["val"], dev)

    cal_loader = loaders["val"]
    cal_label = "validation"
    if loaders.get("val") is not None:
        # Check if val has any anomaly labels
        val_has_anomalies = False
        for batch in loaders["val"]:
            _, batch_labels, _ = batch
            if batch_labels.sum() > 0:
                val_has_anomalies = True
                break
        if not val_has_anomalies and loaders.get("test") is not None:
            cal_loader = loaders["test"]
            cal_label = "test"
            logger.info("Validation has no anomalies -- calibrating threshold on test set")
            print("  Validation has no anomalies -- calibrating on test set")

    metrics = optimize_threshold_f1(model, scorer, cal_loader, dev)
    print(f"  Calibrated on: {cal_label}")
    print(f"  Method: {metrics.get('method', 'f1_max')}")
    print(f"  Last-point threshold: {scorer.last_point_threshold:.4f}")
    if "last_point_f1" in metrics:
        print(f"  Last-point F1: {metrics['last_point_f1']:.4f}")

    # Evaluate on test set
    print("\n--- Test set evaluation ---")
    test_last_point_f1 = float("nan")
    if loaders.get("test") is not None:
        test_point_scores, _ = scorer.score_batch(model, loaders["test"], dev)

        all_test_labels = []
        for batch in loaders["test"]:
            _, labels_batch, _ = batch
            all_test_labels.append(labels_batch.numpy())
        all_test_labels = np.concatenate(all_test_labels)

        lp_scores = test_point_scores[:, -1]
        lp_labels = all_test_labels[:, -1].astype(int)

        if scorer.last_point_threshold is not None:
            lp_preds = lp_scores < scorer.last_point_threshold
            tp = np.sum(lp_preds & lp_labels.astype(bool))
            fp = np.sum(lp_preds & ~lp_labels.astype(bool))
            fn = np.sum(~lp_preds & lp_labels.astype(bool))

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
            test_last_point_f1 = float(f1)

            print(f"  Test last-point: P={precision:.3f}, R={recall:.3f}, F1={f1:.3f}")
            print(f"  TP={tp}, FP={fp}, FN={fn}")

    if not skip_save:
        print("\n--- Saving artifacts ---")
        save_training_artifacts(save_dir, model, model_config, scaler, scorer, history)

    print(f"\n  {model_name} TRAINING COMPLETE")

    best_epoch = history["best_epoch"]
    best_val_loss = history["val_loss"][best_epoch - 1] if best_epoch > 0 else float("nan")
    lp_threshold = scorer.last_point_threshold
    lp_threshold = float(lp_threshold) if lp_threshold is not None else float("nan")
    saved = "skipped" if skip_save else str(save_dir)
    print(
        f"TRAIN model={model_name} device={dev} best_epoch={best_epoch} "
        f"best_val_loss={best_val_loss:.6f} last_point_threshold={lp_threshold:.4f} "
        f"test_last_point_f1={test_last_point_f1:.4f} saved={saved}"
    )

    return {
        "model": model_name,
        "device": str(dev),
        "best_epoch": best_epoch,
        "best_val_loss": best_val_loss,
        "last_point_threshold": lp_threshold,
        "test_last_point_f1": test_last_point_f1,
        "calibrated_on": cal_label,
        "saved": saved,
        "threshold_metrics": metrics,
    }


@click.group()
def cli() -> None:
    """Training CLI: `fit` trains one FCVAE model end-to-end."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    for mod in ("fcvae.model", "fcvae.scorer", "fcvae.preprocess",
                "fcvae.train_lib", "fcvae.training"):
        logging.getLogger(mod).setLevel(logging.WARNING)


@cli.command("fit")
@click.option("--model", "model_name", type=click.Choice(MODEL_NAMES), required=True,
              help="Which model to train (Penny_All or one combo).")
@click.option("--data-csv", type=click.Path(path_type=Path), default=DATA_CSV,
              show_default=True, help="Path to synthetic_transactions.csv.")
@click.option("--out-dir", type=click.Path(path_type=Path), default=ARTIFACTS,
              show_default=True, help="Artifacts root; outputs land in <out-dir>/<model>/.")
@click.option("--window-size", type=int, default=WINDOW_SIZE, show_default=True)
@click.option("--stride", type=int, default=1, show_default=True)
@click.option("--latent-dim", type=int, default=4, show_default=True)
@click.option("--epochs", type=int, default=15, show_default=True)
@click.option("--lr", type=float, default=1e-3, show_default=True)
@click.option("--patience", type=int, default=3, show_default=True)
@click.option("--batch-size", type=int, default=64, show_default=True)
@click.option("--grad-clip", type=float, default=2.0, show_default=True)
@click.option("--kl-warmup-epochs", type=int, default=5, show_default=True)
@click.option("--score-mode", type=click.Choice(["single_pass", "mcmc"]),
              default="single_pass", show_default=True)
@click.option("--seed", type=int, default=SEED, show_default=True)
@click.option("--pool-train-val", is_flag=True, default=False,
              help="Pool train+val splits for training.")
# Clean boolean pair replacing the source's confusing pattern (a store_true
# --no-augmentation defaulting to True, overridden by a second --augmentation
# flag). Same effective baseline default: augmentation off.
@click.option("--augmentation/--no-augmentation", default=False, show_default=True,
              help="Enable training-time data augmentation.")
@click.option("--device", type=click.Choice(["cuda", "mps", "cpu"]), default=None,
              help="Force device; default auto-selects (cuda > mps > cpu).")
@click.option("--skip-save", is_flag=True, default=False,
              help="Skip writing model/scaler/scorer/history artifacts.")
def cmd_fit(model_name: str, data_csv: Path, out_dir: Path, window_size: int, stride: int,
            latent_dim: int, epochs: int, lr: float, patience: int, batch_size: int,
            grad_clip: float, kl_warmup_epochs: int, score_mode: str, seed: int,
            pool_train_val: bool, augmentation: bool, device: str | None,
            skip_save: bool) -> None:
    fit(
        model_name=model_name,
        data_csv=data_csv,
        out_dir=out_dir,
        window_size=window_size,
        stride=stride,
        latent_dim=latent_dim,
        epochs=epochs,
        lr=lr,
        patience=patience,
        batch_size=batch_size,
        grad_clip=grad_clip,
        kl_warmup_epochs=kl_warmup_epochs,
        score_mode=score_mode,
        seed=seed,
        pool_train_val=pool_train_val,
        augmentation=augmentation,
        device=device,
        skip_save=skip_save,
    )


if __name__ == "__main__":
    cli()
