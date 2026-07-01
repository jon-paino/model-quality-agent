#!/usr/bin/env python3
"""Generate a CSV trip feed for the inference pipeline from the cleaned parquet.

The pipeline's FileReader + DSVParser reads columns positionally; the column
ORDER is the contract (see inference_pipeline.tql):
    data[0] = pickup_geohash
    data[1] = hour_of_day      (pickup_ts.hour)
    data[2] = day_of_week      (pickup_ts.dayofweek, 0 = Monday)
    data[3] = is_weekend       (1 if day_of_week >= 5 else 0)
    data[4] = trip_distance
    data[5] = passenger_count
hour_of_day / day_of_week / is_weekend are derived exactly as model/features.py
derives them, so the feed matches what the model was trained on.

Usage:
    .venv/bin/python striim/pipeline/make_trip_feed.py \
        --rows 500 --out /opt/Striim/UploadedFiles/pipeline_trips_qd1.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
PARQUET = REPO / "model" / "data" / "processed" / "trips_cleaned.parquet"
COLUMNS = ["pickup_geohash", "hour_of_day", "day_of_week",
           "is_weekend", "trip_distance", "passenger_count"]


def build(rows: int, seed: int) -> pd.DataFrame:
    df = pd.read_parquet(PARQUET)
    if rows and rows < len(df):
        df = df.sample(n=rows, random_state=seed).reset_index(drop=True)
    ts = pd.to_datetime(df["pickup_ts"])
    dow = ts.dt.dayofweek.astype("int32")
    out = pd.DataFrame({
        "pickup_geohash": df["pickup_geohash"].astype(str),
        "hour_of_day": ts.dt.hour.astype("int32"),
        "day_of_week": dow,
        "is_weekend": (dow >= 5).astype("int32"),
        "trip_distance": df["trip_distance"].astype(float),
        "passenger_count": df["passenger_count"].astype("int32"),
    })
    return out[COLUMNS]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500, help="0 = all rows")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", required=True, help="output CSV path")
    args = ap.parse_args()

    out = build(args.rows, args.seed)
    dest = Path(args.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(dest, index=False)
    print(f"wrote {len(out)} rows to {dest}")


if __name__ == "__main__":
    main()
