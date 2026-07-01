"""Feast online-store push writer.

The out-of-band component that keeps the Feast online store fresh. The Striim
inference path is read-only; this writer is the other half of the
architecture -- it computes the dynamic per-cell features and pushes them so
the FeatureOp's lookups return current values.

Faithful by construction: it reuses `features.compute_dynamic_features`
directly. To get cell G's features at time T it appends a synthetic probe
row `(T, G, ...)` to the trip history and reads that row's computed result
back. Because `compute_dynamic_features` uses exclusive backward windows, the
probe sees only real prior trips and never itself -- so there is zero
train/serve skew, including the daily/weekly lag features, with nothing
reimplemented. Cells are independent, so a single-cell request only processes
that cell's trips.

Commands (`uv run python -m model.feast_writer <cmd>`):

  refresh [--cells N] [--at TS]   recompute and push current features for
                                  every cell (or the busiest N)
  surge <geohash> [--trips N]     inject N synthetic recent trips into one
                                  cell, recompute and push -- the demo beat
  show <geohash>                  print a cell's current online values
"""

from __future__ import annotations

import click
import pandas as pd

from .config import FEATURE_REPO
from .data import CLEANED_PARQUET
from .features import DYNAMIC_FEATURES, compute_dynamic_features

# The PushSource and FeatureView names registered in feature_repo/definitions.py.
PUSH_SOURCE = "cell_dynamic_features_push"
FEATURE_VIEW = "cell_dynamic_features_v1"

# Features that move most visibly under a traffic surge (for before/after output).
SURGE_VISIBLE = [
    "trip_count_last_10min",
    "trip_count_last_1hr",
    "trip_count_last_4hr",
    "trip_count_delta_10min",
    "avg_fare_last_1hr",
]


def _load_trips() -> pd.DataFrame:
    """Load the cleaned trip history, sorted by pickup time."""
    if not CLEANED_PARQUET.exists():
        raise FileNotFoundError(f"Run `model.data clean` first ({CLEANED_PARQUET})")
    trips = pd.read_parquet(
        CLEANED_PARQUET,
        columns=["pickup_ts", "pickup_geohash", "dropoff_zone_id", "fare_amount"],
    )
    return trips.sort_values("pickup_ts").reset_index(drop=True)


def _demo_clock(trips: pd.DataFrame) -> pd.Timestamp:
    """Default 'current time': one minute past the last trip, so every push
    wins the online store's latest-observation-wins race for every cell.
    """
    return pd.Timestamp(trips["pickup_ts"].max()) + pd.Timedelta(minutes=1)


def compute_current_features(
    trips: pd.DataFrame,
    observation_time: pd.Timestamp,
    geohashes: list[str] | None = None,
) -> pd.DataFrame:
    """Dynamic features as of `observation_time`, one row per geohash.

    Appends a probe row at `observation_time` for each cell and reuses
    `compute_dynamic_features`. When `geohashes` is given, the history is
    filtered to those cells first -- cells are independent, so the result is
    identical and much faster. Returns columns [pickup_geohash, *DYNAMIC_FEATURES].
    """
    t = pd.Timestamp(observation_time)
    hist = trips[trips["pickup_ts"] < t]
    if geohashes is not None:
        hist = hist[hist["pickup_geohash"].isin(geohashes)]
        cells = list(geohashes)
    else:
        cells = sorted(hist["pickup_geohash"].unique())

    hist = hist.assign(_is_probe=False)
    probes = pd.DataFrame(
        {
            "pickup_ts": t,
            "pickup_geohash": cells,
            "dropoff_zone_id": -1,  # dummy: the probe is excluded from its own window
            "fare_amount": 0.0,     # dummy: same
            "_is_probe": True,
        }
    )
    combined = (
        pd.concat([hist, probes], ignore_index=True)
        .sort_values("pickup_ts", kind="stable")
        .reset_index(drop=True)
    )
    dyn = compute_dynamic_features(combined)
    out = combined.join(dyn)
    probe_rows = out.loc[out["_is_probe"], ["pickup_geohash", *DYNAMIC_FEATURES]]
    return probe_rows.reset_index(drop=True)


def push_features(features: pd.DataFrame, observation_time: pd.Timestamp) -> int:
    """Push a (geohash + 10 dynamic feature) frame to the Feast online store."""
    from feast import FeatureStore
    from feast.data_source import PushMode

    df = features.rename(columns={"pickup_geohash": "geohash"}).copy()
    df["observation_ts"] = pd.Timestamp(observation_time, tz="UTC")
    df["created_ts"] = pd.Timestamp.now(tz="UTC")

    fs = FeatureStore(repo_path=str(FEATURE_REPO))
    fs.push(PUSH_SOURCE, df, to=PushMode.ONLINE)
    return len(df)


def surge_cell(
    trips: pd.DataFrame,
    geohash: str,
    n_trips: int,
    observation_time: pd.Timestamp,
    window_min: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute a cell's features before and after injecting `n_trips`
    synthetic trips into the `window_min` minutes before `observation_time`.

    The synthetic trips carry the cell's current 1hr average fare and its
    modal dropoff zone, so the surge is pure traffic volume rather than a
    fare or destination shift. Returns (before, after) one-row frames.
    """
    t = pd.Timestamp(observation_time)
    cell_hist = trips[trips["pickup_geohash"] == geohash]
    if cell_hist.empty:
        raise ValueError(f"no trip history for geohash '{geohash}'")

    before = compute_current_features(trips, t, [geohash])

    base_fare = float(before.iloc[0]["avg_fare_last_1hr"]) or 10.0
    modal_zone = int(cell_hist["dropoff_zone_id"].mode().iloc[0])
    # n_trips timestamps evenly spaced inside (t - window_min, t).
    step = pd.Timedelta(minutes=window_min) / (n_trips + 1)
    surge_trips = pd.DataFrame(
        {
            "pickup_ts": [t - pd.Timedelta(minutes=window_min) + step * (i + 1)
                          for i in range(n_trips)],
            "pickup_geohash": geohash,
            "dropoff_zone_id": modal_zone,
            "fare_amount": base_fare,
        }
    )
    surged = pd.concat([trips, surge_trips], ignore_index=True)
    after = compute_current_features(surged, t, [geohash])
    return before, after


def _online_values(geohash: str) -> dict[str, float]:
    """Read a cell's current online feature values via the Feast SDK."""
    from feast import FeatureStore

    fs = FeatureStore(repo_path=str(FEATURE_REPO))
    refs = [f"{FEATURE_VIEW}:{n}" for n in DYNAMIC_FEATURES]
    res = fs.get_online_features(features=refs, entity_rows=[{"geohash": geohash}]).to_dict()
    return {k: v[0] for k, v in res.items() if k != "geohash"}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """Feast online-store push writer."""


@cli.command("refresh")
@click.option("--cells", type=int, default=None,
              help="Refresh only the busiest N cells; default is all cells.")
@click.option("--at", default=None,
              help="Observation time (ISO); default is last trip + 1 minute.")
def cmd_refresh(cells: int | None, at: str | None) -> None:
    """Recompute and push current dynamic features to the online store."""
    trips = _load_trips()
    clock = pd.Timestamp(at) if at else _demo_clock(trips)

    geohashes = None
    if cells is not None:
        geohashes = list(trips["pickup_geohash"].value_counts().head(cells).index)

    click.echo(f"[refresh] observation_time={clock}  "
               f"cells={'all' if geohashes is None else len(geohashes)}")
    features = compute_current_features(trips, clock, geohashes)
    n = push_features(features, clock)
    click.echo(f"[push] {n} cell(s) -> Feast online store ({PUSH_SOURCE})")


@cli.command("surge")
@click.argument("geohash")
@click.option("--trips", "n_trips", type=int, default=20,
              help="Number of synthetic trips to inject (default 20).")
@click.option("--window", type=int, default=10,
              help="Minutes before the observation time to spread them over.")
@click.option("--at", default=None,
              help="Observation time (ISO); default is last trip + 1 minute.")
def cmd_surge(geohash: str, n_trips: int, window: int, at: str | None) -> None:
    """Inject synthetic recent trips into a cell, recompute, and push."""
    trips = _load_trips()
    clock = pd.Timestamp(at) if at else _demo_clock(trips)

    click.echo(f"[surge] geohash={geohash}  +{n_trips} trips over the last "
               f"{window} min  observation_time={clock}")
    before, after = surge_cell(trips, geohash, n_trips, clock, window)

    b, a = before.iloc[0], after.iloc[0]
    click.echo(f"\n  {'feature':32} {'before':>12} {'after':>12}")
    click.echo(f"  {'-' * 32} {'-' * 12} {'-' * 12}")
    for name in SURGE_VISIBLE:
        click.echo(f"  {name:32} {b[name]:12.3f} {a[name]:12.3f}")

    n = push_features(after, clock)
    click.echo(f"\n[push] {n} cell(s) -> Feast online store ({PUSH_SOURCE})")


@cli.command("show")
@click.argument("geohash")
def cmd_show(geohash: str) -> None:
    """Print a cell's current online feature values."""
    values = _online_values(geohash)
    click.echo(f"[online] geohash={geohash}")
    for name in DYNAMIC_FEATURES:
        click.echo(f"  {name:32} {values.get(name)}")


if __name__ == "__main__":
    cli()
