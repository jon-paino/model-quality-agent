"""Feature engineering — single source of truth for both training and serving.

Two families:

1. Static per-event features computed from (geohash, observation_time) at serve
   time. No store lookup needed: `hour_of_day`, `day_of_week`.

2. Dynamic per-cell features looked up from Feast at serve time, computed
   at train time over the trip history using exclusive backward windows:

     - trip_count_last_10min / 1hr / 4hr / 24hr
     - avg_fare_last_1hr
     - unique_dropoff_zones_last_1hr
     - cell_active_minutes_last_1hr
     - trip_count_delta_10min
     - trip_count_same_hour_yesterday / last_week

Target: `fare_amount` (the metered trip fare). Per-trip features dominate
the prediction; the dynamic cell features act as enrichment and are the
proof point that the feature store is wired in correctly.

Train/serve skew note: at training time we observe each trip as the
"observation event" and compute features over the strict-past window
ending just before its timestamp. The streaming writer must produce
identical aggregates against the same window boundary semantics.

This module also owns the feature manifest (the ordered train/serve
contract). Geohash helpers live in geo.py.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict

import click
import numpy as np
import pandas as pd

from .config import (
    ARTIFACTS,
    DATA_PROCESSED,
    GEOHASH_PRECISION,
    LONG_WINDOW_MIN,
    SHORT_WINDOW_MIN,
    TARGET,
)
from .data import CLEANED_PARQUET

FEATURES_PARQUET = DATA_PROCESSED / "trip_features.parquet"
MANIFEST_PATH = ARTIFACTS / "feature_manifest.json"

# Dynamic feature names live in the Feast online store (looked up at serve time).
DYNAMIC_FEATURES = [
    # short-/long-window counts and aggregates
    "trip_count_last_10min",
    "trip_count_last_1hr",
    "trip_count_last_4hr",
    "trip_count_last_24hr",
    "avg_fare_last_1hr",
    "unique_dropoff_zones_last_1hr",
    "cell_active_minutes_last_1hr",
    # rate of change in the very recent past
    "trip_count_delta_10min",
    # daily / weekly seasonality lags
    "trip_count_same_hour_yesterday",
    "trip_count_same_hour_last_week",
]

# Per-event features carried by the incoming trip event itself (no store
# lookup needed at serve time). These dominate the fare prediction.
EVENT_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "trip_distance",
    "passenger_count",
]

# The exact ordered feature list the model consumes. Order is the OP contract.
MODEL_FEATURES = EVENT_FEATURES + DYNAMIC_FEATURES


def compute_event_features(trips: pd.DataFrame) -> pd.DataFrame:
    """Per-event features built from the incoming trip record.

    Pulls timestamp-derived calendar features and the trip-level fields
    (`trip_distance`, `passenger_count`) directly. At serve time these come
    from the inference event, not from Feast.
    """
    ts = trips["pickup_ts"]
    dow = ts.dt.dayofweek.astype("int32")
    return pd.DataFrame(
        {
            "hour_of_day": ts.dt.hour.astype("int32"),
            "day_of_week": dow,
            "is_weekend": (dow >= 5).astype("int32"),
            "trip_distance": trips["trip_distance"].astype("float32"),
            "passenger_count": trips["passenger_count"].astype("int32"),
        },
        index=trips.index,
    )


def compute_dynamic_features(trips: pd.DataFrame) -> pd.DataFrame:
    """Compute the dynamic per-cell features for each trip's observation_time.

    Input: cleaned trips with columns ['pickup_ts', 'pickup_geohash',
    'dropoff_zone_id', 'fare_amount']. Must be sorted by 'pickup_ts'.

    Output: DataFrame with the same index as `trips`, plus DYNAMIC_FEATURES.

    The window for each row is `(t - window, t)` — strictly past, exclusive
    of the current row (so the current event itself never appears in its own
    feature aggregate).
    """
    if not trips["pickup_ts"].is_monotonic_increasing:
        raise ValueError("trips must be sorted by pickup_ts ascending")

    out = pd.DataFrame(index=trips.index)

    # Compute count + fare aggregates via per-cell sliding deques. This
    # avoids the groupby.rolling reindexing pitfall (where the result is
    # ordered by group then time, not by original row order).
    short_count, _ = _rolling_count_sum(trips, SHORT_WINDOW_MIN)
    long_count, long_sum = _rolling_count_sum(trips, LONG_WINDOW_MIN)
    count_4hr, _ = _rolling_count_sum(trips, 4 * 60)
    count_24hr, _ = _rolling_count_sum(trips, 24 * 60)
    # "10-20 min ago" count = (1hr count) is too coarse; use a 20min total
    # minus the most recent 10min to get the prior 10-min slice.
    count_20min, _ = _rolling_count_sum(trips, 2 * SHORT_WINDOW_MIN)
    prior_10min = count_20min - short_count
    # Rate of change: how much busier is the LAST 10 min vs the 10 min before it?
    # Positive => accelerating. Negative => decelerating.
    delta_10 = (short_count - prior_10min).astype("float32")

    out["trip_count_last_10min"] = short_count
    out["trip_count_last_1hr"] = long_count
    out["trip_count_last_4hr"] = count_4hr
    out["trip_count_last_24hr"] = count_24hr
    with np.errstate(divide="ignore", invalid="ignore"):
        avg = np.where(long_count > 0, long_sum / np.maximum(long_count, 1.0), 0.0)
    out["avg_fare_last_1hr"] = avg.astype("float32")
    out["unique_dropoff_zones_last_1hr"] = _rolling_distinct_count(
        trips, key_col="dropoff_zone_id", window_min=LONG_WINDOW_MIN
    )
    out["cell_active_minutes_last_1hr"] = _rolling_distinct_count(
        trips,
        key_col=trips["pickup_ts"].dt.floor("min").astype("int64").rename("_minute_bucket"),
        window_min=LONG_WINDOW_MIN,
    )
    out["trip_count_delta_10min"] = delta_10

    # Daily / weekly seasonality lags.
    out["trip_count_same_hour_yesterday"] = _lag_hour_count(trips, lag_hours=24)
    out["trip_count_same_hour_last_week"] = _lag_hour_count(trips, lag_hours=24 * 7)

    return out


def _rolling_count_sum(
    trips: pd.DataFrame, window_min: int
) -> tuple[np.ndarray, np.ndarray]:
    """Exclusive per-cell rolling count and fare sum.

    Returns two float32 arrays aligned to trips.index, length N. For each row
    i, the value is the count (or fare sum) of events in the same geohash
    cell with timestamps strictly less than `pickup_ts[i]` and at least
    `pickup_ts[i] - window_min`. The current event itself is never counted.
    """
    from collections import defaultdict, deque

    geohash_arr = trips["pickup_geohash"].to_numpy()
    ts_arr = trips["pickup_ts"].to_numpy()
    fare_arr = trips["fare_amount"].astype("float32").to_numpy()
    n = len(trips)

    window_ns = np.timedelta64(int(window_min * 60 * 1_000_000_000), "ns")

    cnt_out = np.zeros(n, dtype="float32")
    sum_out = np.zeros(n, dtype="float32")

    cell_buf: dict[str, deque] = defaultdict(deque)
    cell_sum: dict[str, float] = defaultdict(float)

    for i in range(n):
        g = geohash_arr[i]
        t = ts_arr[i]
        buf = cell_buf[g]

        # Evict events at or before (t - window): we want strictly within
        # (t - window, t). Boundary semantics: the window is open at both ends
        # at the past boundary, open at the present (current event excluded).
        cutoff = t - window_ns
        while buf and buf[0][0] <= cutoff:
            _, f_old = buf.popleft()
            cell_sum[g] -= f_old

        cnt_out[i] = len(buf)
        sum_out[i] = cell_sum[g]

        buf.append((t, fare_arr[i]))
        cell_sum[g] += float(fare_arr[i])

    return cnt_out, sum_out


def _lag_hour_count(trips: pd.DataFrame, lag_hours: int) -> np.ndarray:
    """Per-cell count of trips during the (calendar) hour exactly `lag_hours`
    before the observation event's hour.

    For each row i with timestamp t in cell g:
      - target_hour = floor_to_hour(t) - lag_hours
      - value = number of trips in cell g whose pickup_ts falls inside that hour
    Returns a float32 array aligned to trips.index. Zero when the lookback
    hour is outside the dataset (cold-start; nothing to do but zero-impute).
    """
    geohash_arr = trips["pickup_geohash"].to_numpy()
    # Floor each pickup_ts to the hour, expressed as integer hours since epoch.
    # Resolution-independent: datetime64[h] handles ns / us / ms / s sources.
    hour_arr = trips["pickup_ts"].values.astype("datetime64[h]").astype("int64")
    n = len(trips)

    # First pass: count trips per (cell, hour) using full-history aggregation.
    # The lookup at row i uses (g, hour_arr[i] - lag_hours), which references
    # the past, so the order of accumulation does not change values.
    from collections import defaultdict

    cell_hour_count: dict[tuple[str, int], int] = defaultdict(int)
    for i in range(n):
        cell_hour_count[(geohash_arr[i], int(hour_arr[i]))] += 1

    out = np.zeros(n, dtype="float32")
    for i in range(n):
        out[i] = cell_hour_count.get((geohash_arr[i], int(hour_arr[i]) - lag_hours), 0)
    return out


def _rolling_distinct_count(
    trips: pd.DataFrame, key_col, window_min: int
) -> pd.Series:
    """Exclusive rolling distinct count per geohash over a time window.

    `key_col` can be a column name (string) or a Series. The output is aligned
    to trips.index, dtype float32, with 0 for the very first event in a cell.
    """
    if isinstance(key_col, str):
        keys_full = trips[key_col]
    else:
        keys_full = key_col.reset_index(drop=True)
        keys_full.index = trips.index

    geohash_full = trips["pickup_geohash"]
    ts_full = trips["pickup_ts"]

    window_delta = pd.Timedelta(minutes=window_min)
    out = np.zeros(len(trips), dtype="float32")

    # Per-cell sliding window of (ts, key) deque
    from collections import deque, defaultdict

    cell_buf: dict[str, deque] = defaultdict(deque)
    cell_counts: dict[str, dict] = defaultdict(lambda: defaultdict(int))

    geohash_arr = geohash_full.to_numpy()
    ts_arr = ts_full.to_numpy()
    key_arr = keys_full.to_numpy()

    for i in range(len(trips)):
        g = geohash_arr[i]
        t = ts_arr[i]
        k = key_arr[i]

        buf = cell_buf[g]
        cnt = cell_counts[g]

        # Evict events older than (t - window)
        cutoff = t - np.timedelta64(window_delta.value, "ns")
        while buf and buf[0][0] <= cutoff:
            _, old_k = buf.popleft()
            cnt[old_k] -= 1
            if cnt[old_k] == 0:
                del cnt[old_k]

        # The window is (t - window, t) — strictly less than t (exclusive of current event)
        # so we count distinct keys present in buf BEFORE adding the current event.
        # However, if multiple events share the same timestamp `t`, we want to
        # treat them as not-yet-arrived for this row's count. We enforce that
        # by appending AFTER reading the count.
        out[i] = len(cnt)

        buf.append((t, k))
        cnt[k] += 1

    return pd.Series(out, index=trips.index, dtype="float32")


def build(trips: pd.DataFrame | None = None,
          output_path: Path | None = None) -> pd.DataFrame:
    """Build the train-time feature table from the cleaned trips.

    Output columns: pickup_ts, pickup_geohash, pickup_zone_id, EVENT_FEATURES,
    DYNAMIC_FEATURES, TARGET (`fare_amount`).

    With no arguments, reads `CLEANED_PARQUET` and writes `FEATURES_PARQUET`
    (the Phase 1 default). Pass `trips` and/or `output_path` to override.
    """
    if trips is None:
        if not CLEANED_PARQUET.exists():
            raise FileNotFoundError(f"Run `model.data clean` first ({CLEANED_PARQUET})")
        trips = pd.read_parquet(CLEANED_PARQUET)
    trips = trips.sort_values("pickup_ts").reset_index(drop=True)
    click.echo(f"[features] loaded {len(trips):,} trips")

    event = compute_event_features(trips)
    click.echo("[features] event features computed")
    dyn = compute_dynamic_features(trips)
    click.echo("[features] dynamic features computed")
    y = trips[TARGET].astype("float32").rename(TARGET)
    click.echo(f"[features] target = {TARGET}")

    out = pd.concat(
        [
            trips[["pickup_ts", "pickup_geohash", "pickup_zone_id"]].reset_index(drop=True),
            event.reset_index(drop=True),
            dyn.reset_index(drop=True),
            y.to_frame().reset_index(drop=True),
        ],
        axis=1,
    )

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    dest = output_path if output_path is not None else FEATURES_PARQUET
    out.to_parquet(dest, index=False)
    click.echo(f"[write] {dest}  ({len(out):,} rows)")
    return out


# ---------------------------------------------------------------------------
# Feature manifest — the train/serve contract
# ---------------------------------------------------------------------------


class Manifest(TypedDict):
    version: str
    target: str
    geohash_precision: int
    short_window_min: int
    long_window_min: int
    feature_order: list[str]
    event_features: list[str]
    dynamic_features: list[str]
    feature_dtypes: dict[str, str]


def build_manifest() -> Manifest:
    return Manifest(
        version="2.0",
        target=TARGET,
        geohash_precision=GEOHASH_PRECISION,
        short_window_min=SHORT_WINDOW_MIN,
        long_window_min=LONG_WINDOW_MIN,
        feature_order=list(MODEL_FEATURES),
        event_features=list(EVENT_FEATURES),
        dynamic_features=list(DYNAMIC_FEATURES),
        feature_dtypes={name: "float32" for name in MODEL_FEATURES},
    )


def write_manifest(manifest: Manifest, path: Path = MANIFEST_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def read_manifest(path: Path = MANIFEST_PATH) -> Manifest:
    with open(path) as f:
        return json.load(f)


@click.group()
def cli() -> None:
    """Feature engineering."""


@cli.command("build")
def cmd_build() -> None:
    build()


@cli.command("info")
def cmd_info() -> None:
    if not FEATURES_PARQUET.exists():
        click.echo("No feature table yet. Run: features build")
        return
    df = pd.read_parquet(FEATURES_PARQUET)
    click.echo(f"Rows: {len(df):,}")
    click.echo(f"Unique geohashes: {df['pickup_geohash'].nunique()}")
    click.echo(f"Target stats:\n{df[TARGET].describe()}")
    click.echo("\nSample:")
    click.echo(df.head().to_string())


if __name__ == "__main__":
    cli()
