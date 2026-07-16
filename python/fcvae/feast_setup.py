"""Feast feature store driver for the FCVAE scoring params (F2).

The second Feast instance on this box (the taxi repo owns 6566): a local file
registry + sqlite online store under `fcvae/feature_repo/data/`, served over
HTTP on 127.0.0.1:6567. One row per model combo (entity `combo_key`) carries
the swap-safe scoring params the FCVAEParamsOp stamps into userdata: scaler
mean/scale, last_point_threshold, the scorer's normal-score stats (may be
null), and the model_version string the scorer OP matches against its live
handle before trusting the params.

Commands (`uv run python -m fcvae.feast_setup <cmd>`):

  apply                     seed the offline parquet if missing, `feast apply`
  serve [--host H] [--port P]
                            foreground exec of the Feast HTTP feature server
                            (callers background it)
  push --model <name> [--published-dir DIR] [--threshold F]
       [--model-version S] [--scaler-mean F] [--scaler-scale F]
                            push one combo's params to the online store;
                            defaults read from the published dir's
                            model_config.json + model.manifest.json
"""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import click
import pandas as pd

from .config import FEATURE_REPO, MODEL_NAMES

FEAST_DATA_DIR = FEATURE_REPO / "data"
SEED_PARQUET = FEAST_DATA_DIR / "params_seed.parquet"

FEATURE_VIEW = "fcvae_scoring_params_v1"
PUBLISHED_ROOT = Path("/opt/Striim/fcvae-models")

# Userdata-facing param fields, in the params OP's FeatureRefs order.
PARAM_FIELDS = [
    "scaler_mean",
    "scaler_scale",
    "last_point_threshold",
    "normal_score_mean",
    "normal_score_std",
    "model_version",
]


def build_seed() -> Path:
    """Write the tiny offline seed parquet the FileSource points at.

    `feast apply` reads the batch source to infer the entity column's dtype
    (the taxi repo satisfies this by building its real offline parquet before
    apply; this repo's offline side is vestigial, so a one-row placeholder
    does the same job). The seed is never materialized: all real rows go
    straight to the online store via `write_to_online_store`.
    """
    FEAST_DATA_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "combo_key": ["__seed__"],
            "scaler_mean": [0.0],
            "scaler_scale": [1.0],
            "last_point_threshold": [0.0],
            "normal_score_mean": [0.0],
            "normal_score_std": [1.0],
            "model_version": ["seed"],
            "event_timestamp": [pd.Timestamp("2020-01-01", tz="UTC")],
        }
    )
    df.to_parquet(SEED_PARQUET, index=False)
    click.echo(f"[write] {SEED_PARQUET}")
    return SEED_PARQUET


def feast_apply() -> None:
    """Ensure the seed parquet exists, then register the repo definitions."""
    if not SEED_PARQUET.exists():
        build_seed()
    click.echo(f"[feast apply] repo={FEATURE_REPO}")
    subprocess.check_call(["feast", "apply"], cwd=FEATURE_REPO)


def read_published_params(published_dir: Path) -> dict:
    """Default param row from a published dir's model_config.json + manifest."""
    config_path = published_dir / "model_config.json"
    manifest_path = published_dir / "model.manifest.json"
    for path in (config_path, manifest_path):
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found (is {published_dir} a published model dir?)")
    config = json.loads(config_path.read_text())
    manifest = json.loads(manifest_path.read_text())
    scaler = config.get("scaler") or {}
    thresholds = config.get("thresholds") or {}
    scorer = config.get("scorer") or {}
    return {
        "scaler_mean": scaler.get("mean"),
        "scaler_scale": scaler.get("scale"),
        "last_point_threshold": thresholds.get("last_point_threshold"),
        "normal_score_mean": scorer.get("normal_score_mean"),
        "normal_score_std": scorer.get("normal_score_std"),
        "model_version": manifest.get("model_version"),
    }


def push_params(model_name: str, published_dir: Path, overrides: dict) -> dict:
    """Push one combo's scoring params to the online store; return the row.

    Defaults come from `published_dir`; `overrides` (keyed by PARAM_FIELDS
    names) replace individual fields. Missing normal stats stay None and are
    written as Feast nulls, which the params OP treats as absent (it stamps
    only PRESENT, non-null values).
    """
    from feast import FeatureStore

    row = read_published_params(published_dir)
    for key, value in overrides.items():
        if key not in PARAM_FIELDS:
            raise KeyError(f"unknown param field override: {key}")
        row[key] = value

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "combo_key": [model_name],
            **{name: [row[name]] for name in PARAM_FIELDS},
            "event_timestamp": [pd.Timestamp(now)],
        }
    )
    fs = FeatureStore(repo_path=str(FEATURE_REPO))
    fs.write_to_online_store(FEATURE_VIEW, df=df)
    return {
        "combo_key": model_name,
        **row,
        "event_timestamp": now.isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """FCVAE scoring-params Feast driver (repo apply, HTTP server, pushes)."""


@cli.command("apply")
def cmd_apply() -> None:
    """Seed the offline parquet if missing, then `feast apply`."""
    feast_apply()


@cli.command("serve")
@click.option("--host", default="127.0.0.1", show_default=True, help="Bind host.")
@click.option("--port", type=int, default=6567, show_default=True, help="Bind port.")
def cmd_serve(host: str, port: int) -> None:
    """Run the Feast HTTP feature server in the foreground (exec)."""
    args = ["feast", "-c", str(FEATURE_REPO), "serve", "--host", host, "--port", str(port)]
    click.echo("[exec] " + " ".join(args))
    os.execvp(args[0], args)


@cli.command("push")
@click.option("--model", "model_name", type=click.Choice(MODEL_NAMES), required=True,
              help="Combo key to push params for.")
@click.option("--published-dir", "published_dir", type=click.Path(path_type=Path), default=None,
              help="Published model dir [default: /opt/Striim/fcvae-models/<model>].")
@click.option("--threshold", type=float, default=None,
              help="Override last_point_threshold.")
@click.option("--model-version", default=None, help="Override model_version.")
@click.option("--scaler-mean", type=float, default=None, help="Override scaler_mean.")
@click.option("--scaler-scale", type=float, default=None, help="Override scaler_scale.")
def cmd_push(model_name: str, published_dir: Path | None, threshold: float | None,
             model_version: str | None, scaler_mean: float | None,
             scaler_scale: float | None) -> None:
    """Push one combo's scoring params to the online store."""
    if published_dir is None:
        published_dir = PUBLISHED_ROOT / model_name
    overrides = {
        "last_point_threshold": threshold,
        "model_version": model_version,
        "scaler_mean": scaler_mean,
        "scaler_scale": scaler_scale,
    }
    overrides = {k: v for k, v in overrides.items() if v is not None}
    pushed = push_params(model_name, Path(published_dir), overrides)
    click.echo("[feast push] " + json.dumps(pushed))


if __name__ == "__main__":
    cli()
