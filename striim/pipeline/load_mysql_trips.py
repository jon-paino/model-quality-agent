#!/usr/bin/env python3
"""Load trip rows into the MySQL `trips` table that feeds the Striim CDC source.

This is the MySQL-CDC counterpart of make_trip_feed.py (which wrote a CSV for the
FileReader source). It reuses make_trip_feed.build() so the 6 feature columns are
derived IDENTICALLY (hour_of_day / day_of_week / is_weekend from pickup_ts, exactly
as model/features.py does), preserving the positional contract the OPs read:

    trips.pickup_geohash  -> data[0]
    trips.hour_of_day     -> data[1]
    trips.day_of_week     -> data[2]
    trips.is_weekend      -> data[3]
    trips.trip_distance   -> data[4]
    trips.passenger_count -> data[5]
    trips.trip_id (PK)    -> data[6]   (auto_increment, not loaded here)

The MySQLReader CDC source captures each INSERT from the binlog and streams it as a
Global.WAEvent into TripRawStream, so loading rows here is how the inference pipeline
is fed (the analogue of copying a CSV into UploadedFiles for the FileReader).

Usage:
    .venv/bin/python striim/pipeline/load_mysql_trips.py --rows 500
    .venv/bin/python striim/pipeline/load_mysql_trips.py --rows 200 --trickle --delay 0.2
    .venv/bin/python striim/pipeline/load_mysql_trips.py --truncate --rows 0   # all rows
"""
from __future__ import annotations

import argparse
import time

from sqlalchemy import create_engine, text

# Same directory as make_trip_feed.py, so a plain import resolves when run as a script.
from make_trip_feed import build

TABLE = "trips"


def engine_for(host: str, port: int, user: str, password: str, database: str):
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    # future=True keeps 2.0-style semantics; pool_pre_ping survives container restarts.
    return create_engine(url, pool_pre_ping=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500, help="0 = all rows")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    # Write as root: the 'striim' CDC user is intentionally read-only (SELECT +
    # REPLICATION only). Loading data and running DDL is a separate, privileged role.
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="rootpw")
    ap.add_argument("--database", default="qualitydemo")
    ap.add_argument("--truncate", action="store_true", help="empty the table first")
    ap.add_argument("--trickle", action="store_true",
                    help="insert row-by-row with --delay between rows (live-demo feed)")
    ap.add_argument("--delay", type=float, default=0.1, help="seconds between trickle rows")
    ap.add_argument("--chunksize", type=int, default=1000, help="bulk insert chunk size")
    args = ap.parse_args()

    df = build(args.rows, args.seed)
    eng = engine_for(args.host, args.port, args.user, args.password, args.database)

    if args.truncate:
        with eng.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {TABLE}"))
        print(f"truncated {TABLE}")

    if args.trickle:
        # One INSERT per row so each lands as its own binlog event, for a steady
        # CDC stream during a live demo.
        cols = ", ".join(df.columns)
        placeholders = ", ".join(f":{c}" for c in df.columns)
        stmt = text(f"INSERT INTO {TABLE} ({cols}) VALUES ({placeholders})")
        n = 0
        for rec in df.to_dict(orient="records"):
            with eng.begin() as conn:
                conn.execute(stmt, rec)
            n += 1
            if n % 25 == 0:
                print(f"  trickled {n}/{len(df)}")
            time.sleep(args.delay)
        print(f"trickled {n} rows into {TABLE}")
    else:
        df.to_sql(TABLE, eng, if_exists="append", index=False, chunksize=args.chunksize)
        print(f"loaded {len(df)} rows into {TABLE}")

    with eng.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    print(f"{TABLE} now has {total} rows")


if __name__ == "__main__":
    main()
