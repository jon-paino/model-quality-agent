"""Train XGBoost on the engineered feature table and save the artifacts.

Two training variants are supported:

  - v1 (Phase 1, the deployed model):
      train: Jan 1  .. Jan 17
      val:   Jan 18 .. Jan 24    (early-stop)
      test:  Jan 25 .. Jan 31
      input: trips_cleaned.parquet (January only)

  - v2 (Phase 6, the monthly-retrain simulation):
      train: Jan 1  .. Jan 24
      val:   Jan 25 .. Jan 31    (early-stop)
      test:  Feb 1  .. Feb 7
      input: trips_cleaned.parquet + trips_cleaned_2015-02.parquet, concat
             (the test slice's dynamic features look back into late January;
             the writer must see the whole history)

Both variants share hyperparameters (v2 reuses v1's tuned `best_params.json`).
v1 writes to model.json / metrics.json / golden_set.jsonl; v2 writes to
model-v2.json / metrics-v2.json / golden_set_v2.jsonl. The v2 run also adds a
`v1_on_feb_holdout` block to metrics-v2.json so the demo can quote the v1 vs
v2 MAE side-by-side on the same Feb 1-7 slice.

The booster has `feature_names` set BEFORE `save_model`, since the downstream
Model OP reads feature names from the booster file (`booster.getFeatureNames()`)
and matches incoming columns by NAME, not position. The manifest must agree
with the booster's embedded names.

Three CLI subcommands:
  - `tune`: run Optuna trials over the param search space (v1 split only)
  - `fit`:  train the final model (`--variant v1` or `--variant v2`)
"""

from __future__ import annotations

import json
import math

import click
import numpy as np
import optuna
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from .config import ARTIFACTS, DATA_PROCESSED, SEED, TARGET
from .data import cleaned_parquet_path
from .features import (
    FEATURES_PARQUET,
    MODEL_FEATURES,
    build as build_features,
    build_manifest,
    write_manifest,
)

# Phase 1 (v1) artifact paths.
MODEL_PATH = ARTIFACTS / "model.json"
METRICS_PATH = ARTIFACTS / "metrics.json"
GOLDEN_PATH = ARTIFACTS / "golden_set.jsonl"
BEST_PARAMS_PATH = ARTIFACTS / "best_params.json"

# Phase 6 (v2) artifact paths.
MODEL_V2_PATH = ARTIFACTS / "model-v2.json"
METRICS_V2_PATH = ARTIFACTS / "metrics-v2.json"
GOLDEN_V2_PATH = ARTIFACTS / "golden_set_v2.jsonl"
FEATURES_V2_PARQUET = DATA_PROCESSED / "trip_features_v2.parquet"

# v2 split boundaries: train < val_start <= val < test_start <= test < test_end.
V2_VAL_START = pd.Timestamp("2015-01-25")
V2_TEST_START = pd.Timestamp("2015-02-01")
V2_TEST_END = pd.Timestamp("2015-02-08")

# Shared base. Objective is squared-error since the target is a continuous
# dollar value (`fare_amount`), not a count.
BASE_PARAMS = {
    "objective": "reg:squarederror",
    "eval_metric": ["rmse", "mae"],
    "tree_method": "hist",
    "seed": SEED,
}

# Hand-set defaults used when no tuned params exist yet.
DEFAULT_PARAMS = {
    **BASE_PARAMS,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "min_child_weight": 5,
    "reg_alpha": 0.0,
    "reg_lambda": 1.0,
}


def _time_split_v1(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = df[df["pickup_ts"] < pd.Timestamp("2015-01-18")]
    val = df[
        (df["pickup_ts"] >= pd.Timestamp("2015-01-18"))
        & (df["pickup_ts"] < pd.Timestamp("2015-01-25"))
    ]
    test = df[df["pickup_ts"] >= pd.Timestamp("2015-01-25")]
    return train, val, test


def _time_split_v2(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = df[df["pickup_ts"] < V2_VAL_START]
    val = df[(df["pickup_ts"] >= V2_VAL_START) & (df["pickup_ts"] < V2_TEST_START)]
    test = df[(df["pickup_ts"] >= V2_TEST_START) & (df["pickup_ts"] < V2_TEST_END)]
    return train, val, test


# Backwards-compatible alias for any caller that imported _time_split.
_time_split = _time_split_v1


def _make_dmatrices(df: pd.DataFrame, variant: str = "v1"):
    splitter = _time_split_v2 if variant == "v2" else _time_split_v1
    train_df, val_df, test_df = splitter(df)

    def make(part):
        return xgb.DMatrix(
            part[MODEL_FEATURES].astype("float32").to_numpy(),
            label=part[TARGET].astype("float32").to_numpy(),
            feature_names=MODEL_FEATURES,
        )

    return train_df, val_df, test_df, make(train_df), make(val_df), make(test_df)


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "rmse": float(math.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def _baseline_linear_distance(X_train: pd.DataFrame, X_eval: pd.DataFrame) -> np.ndarray:
    """OLS of fare on distance fit from the train set, evaluated on X_eval.
    Captures the 'fare = base + per_mile * distance' intuition that any
    learned model has to beat.
    """
    d_train = X_train["trip_distance"].to_numpy()
    y_train = X_train[TARGET].to_numpy()
    # closed-form simple linear regression
    a, b = np.polyfit(d_train, y_train, deg=1)
    return (a * X_eval["trip_distance"].to_numpy() + b).astype("float64")


# ---------------------------------------------------------------------------
# Optuna tuning
# ---------------------------------------------------------------------------


def tune(n_trials: int = 40) -> dict:
    """Run Optuna trials. Optimize val MAE with early-stop inside each trial.
    Persists best params + metadata to `artifacts/best_params.json`.
    """
    if not FEATURES_PARQUET.exists():
        raise FileNotFoundError(f"Run `model.features build` first ({FEATURES_PARQUET})")
    df = pd.read_parquet(FEATURES_PARQUET)
    _, _, _, dtrain, dval, _ = _make_dmatrices(df)
    y_val = df[
        (df["pickup_ts"] >= pd.Timestamp("2015-01-18"))
        & (df["pickup_ts"] < pd.Timestamp("2015-01-25"))
    ][TARGET].to_numpy()

    def objective(trial: optuna.Trial) -> float:
        params = {
            **BASE_PARAMS,
            "max_depth": trial.suggest_int("max_depth", 4, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 50),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True),
        }
        booster = xgb.train(
            params, dtrain, num_boost_round=800,
            evals=[(dval, "val")],
            early_stopping_rounds=50, verbose_eval=False,
        )
        pred = booster.predict(dval)
        # Store best_iteration alongside the trial for use at fit time
        trial.set_user_attr("best_iteration", int(booster.best_iteration))
        return float(mean_absolute_error(y_val, pred))

    sampler = optuna.samplers.TPESampler(seed=SEED)
    study = optuna.create_study(direction="minimize", sampler=sampler)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best = study.best_trial
    payload = {
        "best_value_val_mae": float(best.value),
        "best_params": best.params,
        "best_iteration": int(best.user_attrs.get("best_iteration", 0)),
        "n_trials": n_trials,
        "objective": BASE_PARAMS["objective"],
    }
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with open(BEST_PARAMS_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    click.echo(f"\n[best] val_mae={best.value:.4f}  params={best.params}")
    click.echo(f"[write] {BEST_PARAMS_PATH}")
    return payload


# ---------------------------------------------------------------------------
# Final fit
# ---------------------------------------------------------------------------


def _ensure_v2_features() -> pd.DataFrame:
    """Build (or load) the v2 feature table.

    v2 needs features over the concatenated Jan + Feb cleaned parquets so the
    Feb 1-7 test slice has dynamic features that look back into late January.
    Cached at FEATURES_V2_PARQUET.
    """
    if FEATURES_V2_PARQUET.exists():
        click.echo(f"[features-v2] using cached {FEATURES_V2_PARQUET}")
        return pd.read_parquet(FEATURES_V2_PARQUET)

    jan_path = cleaned_parquet_path(2015, 1)
    feb_path = cleaned_parquet_path(2015, 2)
    if not jan_path.exists():
        raise FileNotFoundError(f"Run `model.data clean --year 2015 --month 1` first ({jan_path})")
    if not feb_path.exists():
        raise FileNotFoundError(f"Run `model.data clean --year 2015 --month 2` first ({feb_path})")

    click.echo(f"[features-v2] concat {jan_path.name} + {feb_path.name}")
    jan = pd.read_parquet(jan_path)
    feb = pd.read_parquet(feb_path)
    combined = pd.concat([jan, feb], ignore_index=True).sort_values("pickup_ts").reset_index(drop=True)
    click.echo(f"[features-v2] combined rows: {len(combined):,}")
    return build_features(trips=combined, output_path=FEATURES_V2_PARQUET)


def fit(variant: str = "v1") -> dict[str, dict[str, float]]:
    """Train the final model. `variant='v2'` triggers the Phase 6 retrain."""
    if variant not in {"v1", "v2"}:
        raise ValueError(f"variant must be 'v1' or 'v2', got {variant!r}")

    if variant == "v2":
        df = _ensure_v2_features()
    else:
        if not FEATURES_PARQUET.exists():
            raise FileNotFoundError(f"Run `model.features build` first ({FEATURES_PARQUET})")
        df = pd.read_parquet(FEATURES_PARQUET)

    train_df, val_df, test_df, dtrain, dval, dtest = _make_dmatrices(df, variant=variant)
    click.echo(
        f"[split-{variant}] train={len(train_df):,}  val={len(val_df):,}  test={len(test_df):,}"
    )

    # Load tuned params if available; otherwise fall back to hand-set defaults.
    if BEST_PARAMS_PATH.exists():
        with open(BEST_PARAMS_PATH) as f:
            tuned = json.load(f)
        params = {**BASE_PARAMS, **tuned["best_params"]}
        click.echo(f"[params] using tuned ({BEST_PARAMS_PATH}): val_mae={tuned['best_value_val_mae']:.4f}")
    else:
        params = dict(DEFAULT_PARAMS)
        click.echo("[params] using hand-set defaults (no best_params.json found)")

    booster = xgb.train(
        params, dtrain, num_boost_round=1500,
        evals=[(dtrain, "train"), (dval, "val")],
        early_stopping_rounds=50, verbose_eval=100,
    )

    pred_test = booster.predict(dtest)
    y_test = test_df[TARGET].astype("float32").to_numpy()
    pred_baseline_median = np.full_like(y_test, float(np.median(train_df[TARGET])))
    pred_baseline_linear = _baseline_linear_distance(train_df, test_df)

    metrics = {
        "xgb_test": _metrics(y_test, pred_test),
        "baseline_median_test": _metrics(y_test, pred_baseline_median),
        "baseline_linear_distance_test": _metrics(y_test, pred_baseline_linear),
        "n_train": len(train_df),
        "n_val": len(val_df),
        "n_test": len(test_df),
        "best_iteration": int(booster.best_iteration),
        "params": {k: v for k, v in params.items() if k not in {"eval_metric"}},
        "feature_importance_gain": booster.get_score(importance_type="gain"),
    }

    # Verify feature names embedded in booster match the manifest.
    embedded = booster.feature_names
    if embedded != MODEL_FEATURES:
        raise RuntimeError(
            f"Booster feature names do not match MODEL_FEATURES.\n"
            f"  embedded: {embedded}\n  expected: {MODEL_FEATURES}"
        )

    # Variant-aware output paths.
    if variant == "v2":
        model_out = MODEL_V2_PATH
        metrics_out = METRICS_V2_PATH
        golden_out = GOLDEN_V2_PATH
        # Head-to-head: score v1 on the v2 test slice (Feb 1-7) so the
        # metrics file carries both numbers for the demo narrative.
        if MODEL_PATH.exists():
            v1_booster = xgb.Booster()
            v1_booster.load_model(str(MODEL_PATH))
            pred_v1 = v1_booster.predict(dtest)
            metrics["v1_on_feb_holdout"] = _metrics(y_test, pred_v1)
            click.echo(
                f"[v1_on_feb_holdout] mae={metrics['v1_on_feb_holdout']['mae']:.3f}  "
                f"rmse={metrics['v1_on_feb_holdout']['rmse']:.3f}  "
                f"r2={metrics['v1_on_feb_holdout']['r2']:.3f}"
            )
        else:
            click.echo(f"[note] {MODEL_PATH} missing; skipping v1_on_feb_holdout")
    else:
        model_out = MODEL_PATH
        metrics_out = METRICS_PATH
        golden_out = GOLDEN_PATH

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(model_out))
    click.echo(f"[write] {model_out}")

    with open(metrics_out, "w") as f:
        json.dump(metrics, f, indent=2, default=float)
    click.echo(f"[write] {metrics_out}")

    # The feature manifest is variant-independent (same 15 features, same
    # order). Only refresh it on the v1 path; v2 reuses what v1 wrote.
    if variant == "v1":
        write_manifest(build_manifest())
        click.echo("[write] feature_manifest.json")

    _write_golden_set(test_df, booster, n=50, path=golden_out)

    click.echo(f"\n=== Test metrics ({variant}) ===")
    for name, m in metrics.items():
        if isinstance(m, dict) and {"rmse", "mae", "r2"} <= set(m.keys()):
            click.echo(f"  {name:25s} rmse={m['rmse']:.3f}  mae={m['mae']:.3f}  r2={m['r2']:.3f}")
    return metrics


def _write_golden_set(test_df: pd.DataFrame, booster: xgb.Booster, n: int = 50,
                      path = GOLDEN_PATH) -> None:
    """Sample n rows from the test set and freeze their feature vectors and
    XGBoost predictions for downstream cross-validation against Striim.
    """
    rng = np.random.default_rng(SEED)
    sample = test_df.sample(n=n, random_state=rng.integers(0, 2**31 - 1)).reset_index(drop=True)
    X = sample[MODEL_FEATURES].astype("float32").to_numpy()
    d = xgb.DMatrix(X, feature_names=MODEL_FEATURES)
    preds = booster.predict(d)

    with open(path, "w") as f:
        for i, row in sample.iterrows():
            rec = {
                "geohash": row["pickup_geohash"],
                "observation_time": pd.Timestamp(row["pickup_ts"]).isoformat(),
                "feature_vector": {name: float(row[name]) for name in MODEL_FEATURES},
                "expected_prediction": float(preds[i]),
            }
            f.write(json.dumps(rec) + "\n")
    click.echo(f"[write] {path}  ({n} records)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """Training CLI: `tune` (Optuna) then `fit` (final training)."""


@cli.command("tune")
@click.option("--trials", default=40, type=int, help="Number of Optuna trials")
def cmd_tune(trials: int) -> None:
    tune(n_trials=trials)


@cli.command("fit")
@click.option("--variant", type=click.Choice(["v1", "v2"]), default="v1",
              show_default=True, help="v1: Phase 1 Jan-only model. "
              "v2: Phase 6 retrain on Jan 1-24 train, Jan 25-31 val, Feb 1-7 test.")
def cmd_fit(variant: str) -> None:
    fit(variant=variant)


if __name__ == "__main__":
    cli()
