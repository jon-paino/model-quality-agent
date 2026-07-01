"""Path constants for the cleaned trip parquets used at serve time.

The original data acquisition pipeline (TLC raw parquet -> zone-centroid
geohash -> stratified sample -> cleaned parquet) lived here through Phase 5
and is preserved in git history. The demo runtime consumes only the two
cleaned parquets it produces, so this module is now just the path layer
shared by `feast_writer`, `feast_streamer`, and `features`.
"""

from __future__ import annotations

from pathlib import Path

from .config import DATA_PROCESSED

DEFAULT_YEAR = 2015
DEFAULT_MONTH = 1


def cleaned_parquet_path(year: int = DEFAULT_YEAR, month: int = DEFAULT_MONTH) -> Path:
    """Absolute path to the cleaned trip parquet for `(year, month)`.

    The Phase 1 default `(2015, 1)` resolves to the legacy name
    `trips_cleaned.parquet`; other months get an explicit `_YYYY-MM` suffix.
    """
    if (year, month) == (DEFAULT_YEAR, DEFAULT_MONTH):
        return DATA_PROCESSED / "trips_cleaned.parquet"
    return DATA_PROCESSED / f"trips_cleaned_{year:04d}-{month:02d}.parquet"


CLEANED_PARQUET = cleaned_parquet_path()
