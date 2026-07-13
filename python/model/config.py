"""Project-wide constants. Everything that defines the train/serve contract
lives here so all code paths import a single source of truth.
"""

import os
from pathlib import Path

# Paths (everything under model/ except the top-level repo root).
# MODEL_DATA_PROCESSED / MODEL_ARTIFACTS env vars override the two locations
# the training container must relocate (input data mount, artifact scratch);
# unset or empty means the in-repo default, so host runs are unchanged.
REPO_ROOT = Path(__file__).resolve().parents[1].parent
MODEL_ROOT = Path(__file__).resolve().parent
DATA_RAW = MODEL_ROOT / "data" / "raw"
DATA_PROCESSED = Path(os.environ.get("MODEL_DATA_PROCESSED")
                      or (MODEL_ROOT / "data" / "processed"))
ARTIFACTS = Path(os.environ.get("MODEL_ARTIFACTS") or (MODEL_ROOT / "artifacts"))
FEATURE_REPO = MODEL_ROOT / "feature_repo"

# NYC bounding box (lon_min, lon_max, lat_min, lat_max). Loose so we keep
# legitimate outer-borough trips but drop GPS errors landing in the Atlantic
# or somewhere upstate.
NYC_LON = (-74.30, -73.70)
NYC_LAT = (40.50, 41.00)

# Geohash precision 6 (~1.2 km x 0.6 km). Manhattan is ~21 km long, so
# precision 6 yields ~17 cells across its length — granular enough for
# differentiated cell behavior without sparsity.
GEOHASH_PRECISION = 6

# Window definitions. These EXACT values are the train/serve skew contract;
# the streaming writer that feeds Feast must use identical windows.
SHORT_WINDOW_MIN = 10
LONG_WINDOW_MIN = 60

# Sample cap (rows after cleaning) — keeps training and Feast materialization fast.
SAMPLE_ROWS = 250_000

# Random seed used everywhere
SEED = 17

# Target column name. fare_amount is the trip's metered fare (excluding tip,
# tolls, surcharges). Per-trip distance and time-of-day dominate the
# prediction; the dynamic cell features (looked up via Feast) act as
# enrichment and are the proof point that the feature store is wired in.
TARGET = "fare_amount"
