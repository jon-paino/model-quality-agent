#!/usr/bin/env python3
"""Generate a LABELED feed that forces a known number of FeatureOp misses.

This is the Phase 1b acceptance-test feed for the Quality Monitoring Agent's
feature_miss_rate signal. It emits N total rows of which exactly M use geohashes
that are guaranteed NOT PRESENT in the Feast online store, so FeatureOp drops
them as feature-store misses. The remaining N - M rows use geohashes confirmed
PRESENT, so they flow through cleanly.

Ground truth is verified against the RUNNING Feast server, not guessed: every
candidate geohash (present pool and miss pool) is queried with the same 10
feature refs FeatureOp uses, and classified exactly as FeatureOp classifies it
(a geohash is a miss if ANY of the 10 features is non-PRESENT). The expected
feature_miss_rate is therefore $M / N$ exactly.

The feed is deterministic (fixed seed). Alongside the CSV it writes a
<out>.truth.json sidecar (n, m, expected_miss_rate, the miss geohashes) that
check_miss_acceptance.py reads to assert the agent's reported cumulative totals.

IMPORTANT: the agent's counters are cumulative and monotonic, so the exact
assertion events_seen == N holds only on a FRESH deploy fed ONLY this file.
See check_miss_acceptance.py and the procedure it prints.

Usage:
    .venv/bin/python striim/pipeline/make_miss_feed.py \
        --rows 200 --misses 40 \
        --out /opt/Striim/UploadedFiles/pipeline_trips_miss1.csv
"""
from __future__ import annotations

import argparse
import json
import random
import urllib.request
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
PARQUET = REPO / "model" / "data" / "processed" / "trips_cleaned.parquet"
COLUMNS = ["pickup_geohash", "hour_of_day", "day_of_week",
           "is_weekend", "trip_distance", "passenger_count"]

# The 10 dynamic feature names FeatureOp requests, in feature_manifest order
# (mirrors FeatureOp.DYNAMIC_FEATURES). A present geohash must return PRESENT
# for ALL of these; a miss geohash returns non-PRESENT for at least one.
DYNAMIC_FEATURES = [
    "trip_count_last_10min", "trip_count_last_1hr", "trip_count_last_4hr",
    "trip_count_last_24hr", "avg_fare_last_1hr", "unique_dropoff_zones_last_1hr",
    "cell_active_minutes_last_1hr", "trip_count_delta_10min",
    "trip_count_same_hour_yesterday", "trip_count_same_hour_last_week",
]

# Geohash base32 alphabet (excludes a, i, l, o). A string over this alphabet is
# a syntactically valid geohash; whether it is materialized is what we verify.
GEOHASH_ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz"


def feast_present(geohashes: list[str], feast_url: str, feature_view: str,
                  timeout: int = 10) -> dict[str, bool]:
    """Classify each geohash exactly as FeatureOp does: PRESENT iff Feast returns
    status PRESENT for ALL 10 features. Returns {geohash: is_present}."""
    if not geohashes:
        return {}
    refs = [f"{feature_view}:{name}" for name in DYNAMIC_FEATURES]
    body = json.dumps({"features": refs, "entities": {"geohash": geohashes}}).encode()
    req = urllib.request.Request(
        feast_url.rstrip("/") + "/get-online-features",
        data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode())

    names = payload["metadata"]["feature_names"]
    results = payload["results"]
    # Feature columns are every result whose name is not the "geohash" entity.
    feature_cols = [i for i, nm in enumerate(names) if nm != "geohash"]
    out: dict[str, bool] = {}
    for j, gh in enumerate(geohashes):
        out[gh] = all(results[i]["statuses"][j] == "PRESENT" for i in feature_cols)
    return out


def build_present_pool(df: pd.DataFrame, need: int, feast_url: str,
                       feature_view: str, rng: random.Random) -> pd.DataFrame:
    """Sample rows whose geohash is confirmed PRESENT, so the present pool can
    never contribute an unexpected miss."""
    distinct = list(pd.unique(df["pickup_geohash"].astype(str)))
    present_map = feast_present(distinct, feast_url, feature_view)
    present_ghs = {gh for gh, ok in present_map.items() if ok}
    if not present_ghs:
        raise SystemExit("no PRESENT geohashes found in Feast; is it materialized?")
    pool = df[df["pickup_geohash"].astype(str).isin(present_ghs)]
    if len(pool) < need:
        # Sample with replacement only if the materialized pool is smaller than
        # the requested present-row count (rare for large N).
        return pool.sample(n=need, replace=True, random_state=rng.randint(0, 2**31 - 1))
    return pool.sample(n=need, random_state=rng.randint(0, 2**31 - 1)).reset_index(drop=True)


def make_miss_geohashes(count: int, present_ghs: set[str], feast_url: str,
                        feature_view: str, rng: random.Random) -> list[str]:
    """Generate distinct valid-but-unmaterialized geohashes, each VERIFIED
    non-PRESENT against Feast (so each forces a real FeatureOp miss)."""
    found: list[str] = []
    tried: set[str] = set()
    while len(found) < count:
        # Generate a batch of candidates, then verify in one Feast call.
        batch = []
        while len(batch) < max(count * 2, 16):
            cand = "".join(rng.choice(GEOHASH_ALPHABET) for _ in range(6))
            if cand in present_ghs or cand in tried:
                continue
            tried.add(cand)
            batch.append(cand)
        present_map = feast_present(batch, feast_url, feature_view)
        for gh in batch:
            if not present_map.get(gh, False):  # non-PRESENT -> a guaranteed miss
                found.append(gh)
                if len(found) == count:
                    break
    return found


def derive(df: pd.DataFrame) -> pd.DataFrame:
    """Derive the 6 positional feed columns exactly as make_trip_feed.py does."""
    ts = pd.to_datetime(df["pickup_ts"])
    dow = ts.dt.dayofweek.astype("int32")
    return pd.DataFrame({
        "pickup_geohash": df["pickup_geohash"].astype(str).values,
        "hour_of_day": ts.dt.hour.astype("int32").values,
        "day_of_week": dow.values,
        "is_weekend": (dow >= 5).astype("int32").values,
        "trip_distance": df["trip_distance"].astype(float).values,
        "passenger_count": df["passenger_count"].astype("int32").values,
    })


def build(rows: int, misses: int, seed: int, feast_url: str,
          feature_view: str) -> tuple[pd.DataFrame, dict]:
    if misses > rows:
        raise SystemExit(f"--misses ({misses}) cannot exceed --rows ({rows})")
    rng = random.Random(seed)
    df = pd.read_parquet(PARQUET)

    n_present = rows - misses
    present_rows = derive(build_present_pool(df, n_present, feast_url, feature_view, rng))

    # Miss rows: take real rows for plausible event features, then overwrite the
    # geohash with a verified-absent one so the ONLY drop cause is the Feast miss.
    present_ghs = set(present_rows["pickup_geohash"])
    miss_ghs = make_miss_geohashes(misses, present_ghs, feast_url, feature_view, rng)
    miss_src = derive(df.sample(n=misses, replace=(len(df) < misses),
                                random_state=rng.randint(0, 2**31 - 1)).reset_index(drop=True))
    miss_src["pickup_geohash"] = miss_ghs

    feed = pd.concat([present_rows, miss_src], ignore_index=True)
    feed = feed.sample(frac=1.0, random_state=seed).reset_index(drop=True)

    truth = {
        "n": int(rows),
        "m": int(misses),
        "expected_feature_miss_rate": misses / rows,
        "miss_geohashes": miss_ghs,
        "seed": seed,
        "note": "feature_store_miss counter: faults == m, events_seen == n on a "
                "FRESH deploy fed only this file (counters are cumulative).",
    }
    return feed[COLUMNS], truth


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200, help="N total rows")
    ap.add_argument("--misses", type=int, default=40, help="M rows forced to miss")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--feast-url", default="http://127.0.0.1:6566")
    ap.add_argument("--feature-view", default="cell_dynamic_features_v1")
    ap.add_argument("--out", required=True, help="output CSV path")
    args = ap.parse_args()

    feed, truth = build(args.rows, args.misses, args.seed,
                        args.feast_url, args.feature_view)
    dest = Path(args.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    feed.to_csv(dest, index=False)
    truth_path = dest.with_suffix(dest.suffix + ".truth.json")
    truth_path.write_text(json.dumps(truth, indent=2))

    print(f"wrote {len(feed)} rows to {dest}")
    print(f"ground truth: N={truth['n']} M={truth['m']} "
          f"expected feature_miss_rate = {truth['m']}/{truth['n']} = "
          f"{truth['expected_feature_miss_rate']:.4f}")
    print(f"miss geohashes (verified non-PRESENT): {truth['miss_geohashes']}")
    print(f"wrote ground truth to {truth_path}")


if __name__ == "__main__":
    main()
