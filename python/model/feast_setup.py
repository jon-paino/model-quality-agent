"""Feast feature store setup driver.

Orchestrates:
  - Building the offline batch source (`feature_repo/data/cell_features.parquet`)
    from the train-time feature table.
  - `feast apply` to register entities and views from feature_repo/definitions.py
  - Materializing the offline data into the online (SQLite) store.
  - Smoke-testing online lookup via the Python SDK.

The streaming writer will push fresh aggregates against
`cell_dynamic_features_push` to keep the online store current.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone

import click
import pandas as pd

from .config import FEATURE_REPO
from .features import DYNAMIC_FEATURES, FEATURES_PARQUET

FEATURE_REPO_DIR = FEATURE_REPO
FEAST_DATA_DIR = FEATURE_REPO_DIR / "data"
OFFLINE_PARQUET = FEAST_DATA_DIR / "cell_features.parquet"


def build_offline() -> Path:
    """Build the Feast offline batch parquet from the train-time feature table.

    Schema: [geohash, observation_ts, created_ts, *DYNAMIC_FEATURES]. One row
    per trip — Feast will use observation_ts as the event-time column and
    materialize the LATEST row per geohash into the online store.
    """
    if not FEATURES_PARQUET.exists():
        raise FileNotFoundError(
            f"Run `nyc_inference.features build` first ({FEATURES_PARQUET})"
        )

    df = pd.read_parquet(FEATURES_PARQUET)
    out = df[["pickup_geohash", "pickup_ts", *DYNAMIC_FEATURES]].rename(
        columns={"pickup_geohash": "geohash", "pickup_ts": "observation_ts"}
    )
    # Feast requires a tz-aware datetime for the timestamp column
    out["observation_ts"] = pd.to_datetime(out["observation_ts"], utc=True)
    out["created_ts"] = pd.Timestamp.utcnow().tz_convert("UTC")

    FEAST_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OFFLINE_PARQUET, index=False)
    click.echo(f"[write] {OFFLINE_PARQUET}  ({len(out):,} rows)")
    return OFFLINE_PARQUET


def feast_apply() -> None:
    click.echo("[feast apply]")
    subprocess.check_call(["feast", "apply"], cwd=FEATURE_REPO_DIR)


def materialize(start: datetime | None = None, end: datetime | None = None) -> None:
    """Materialize offline data into the online store.

    Uses the full-range `feast materialize <start> <end>` because our
    observation_ts is historical (Jan 2015) and `materialize-incremental`
    would default the start to "now" — yielding 0 rows for historical data.
    """
    ts = pd.read_parquet(OFFLINE_PARQUET, columns=["observation_ts"])["observation_ts"]
    if start is None:
        # One second before earliest observation_ts so the range is inclusive.
        start = (pd.to_datetime(ts).min() - pd.Timedelta(seconds=1)).to_pydatetime()
    if end is None:
        end = (pd.to_datetime(ts).max() + pd.Timedelta(seconds=1)).to_pydatetime()
    start_str = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    click.echo(f"[feast materialize] {start_str} .. {end_str}")
    subprocess.check_call(
        ["feast", "materialize", start_str, end_str],
        cwd=FEATURE_REPO_DIR,
    )


def smoke_test(geohash: str | None = None) -> dict:
    """Look up a geohash from the online store via the Feast Python SDK."""
    from feast import FeatureStore

    fs = FeatureStore(repo_path=str(FEATURE_REPO_DIR))
    if geohash is None:
        df = pd.read_parquet(OFFLINE_PARQUET, columns=["geohash"])
        geohash = df["geohash"].value_counts().idxmax()
        click.echo(f"[smoke] busiest geohash = {geohash}")
    feature_refs = [f"cell_dynamic_features_v1:{n}" for n in DYNAMIC_FEATURES]
    res = fs.get_online_features(
        features=feature_refs,
        entity_rows=[{"geohash": geohash}],
    ).to_dict()
    click.echo("[smoke] online lookup result:")
    for k, v in res.items():
        click.echo(f"  {k}: {v}")
    return res


@click.group()
def cli() -> None:
    """Feast feature store driver."""


@cli.command("build_offline")
def cmd_build_offline() -> None:
    build_offline()


@cli.command("apply")
def cmd_apply() -> None:
    feast_apply()


@cli.command("materialize")
def cmd_materialize() -> None:
    materialize()


@cli.command("smoke")
@click.option("--geohash", default=None, help="geohash to look up; default = busiest")
def cmd_smoke(geohash: str | None) -> None:
    smoke_test(geohash)


@cli.command("init")
def cmd_init() -> None:
    """Full init: build offline parquet, feast apply, materialize, smoke test."""
    build_offline()
    feast_apply()
    materialize()
    smoke_test()


if __name__ == "__main__":
    cli()
