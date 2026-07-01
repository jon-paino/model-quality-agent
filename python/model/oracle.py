"""End-to-end Python validation harness.

Two self-tests:

1. **Model golden-set test.** For each row in `artifacts/golden_set.jsonl`,
   feed the captured `feature_vector` directly to the XGBoost booster and
   confirm the prediction matches `expected_prediction` within $10^{-5}$.
   This tests model file integrity and feature-name binding — independent
   of Feast.

2. **End-to-end Feast lookup test.** Pick `(lat, lon, ts)` triples, geohash
   them, hit the Feast HTTP `/get-online-features` endpoint, assemble the
   model input by name, score with XGBoost, and sanity-check the prediction.
   This is the oracle the Striim pipeline output is cross-validated against
   (same Feast state, same model file = same prediction).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import click
import numpy as np
import pandas as pd
import requests
import xgboost as xgb

from .config import TARGET
from .features import (
    EVENT_FEATURES,
    MANIFEST_PATH,
    compute_event_features,
    read_manifest,
)
from .geo import encode_geohash
from .train import GOLDEN_PATH, MODEL_PATH

DEFAULT_FEAST_URL = "http://127.0.0.1:6566"
ABS_TOL = 1e-5


class Oracle:
    """Coords -> geohash -> Feast lookup -> XGBoost prediction.

    Encapsulates the model file, the feature manifest, and the Feast HTTP
    client so callers can keep them around between requests.
    """

    def __init__(
        self,
        feast_url: str = DEFAULT_FEAST_URL,
        model_path: Path = MODEL_PATH,
        manifest_path: Path = MANIFEST_PATH,
    ):
        self.feast_url = feast_url
        self.manifest = read_manifest(manifest_path)
        self.booster = xgb.Booster()
        self.booster.load_model(str(model_path))
        if self.booster.feature_names != self.manifest["feature_order"]:
            raise RuntimeError(
                "booster feature_names disagree with manifest feature_order:\n"
                f"  booster:  {self.booster.feature_names}\n"
                f"  manifest: {self.manifest['feature_order']}"
            )
        # Pre-build the Feast feature reference list (with view prefix)
        self._feast_refs = [
            f"cell_dynamic_features_v1:{n}" for n in self.manifest["dynamic_features"]
        ]

    def lookup_dynamic(self, geohash: str) -> dict[str, float]:
        """Hit Feast HTTP for this geohash's dynamic features. Returns a name
        -> value dict. Raises if Feast returns a non-PRESENT status.
        """
        body = {"features": self._feast_refs, "entities": {"geohash": [geohash]}}
        resp = requests.post(
            f"{self.feast_url}/get-online-features",
            json=body,
            timeout=5.0,
        )
        resp.raise_for_status()
        payload = resp.json()

        names = payload["metadata"]["feature_names"]
        results = payload["results"]
        out: dict[str, float] = {}
        for i, name in enumerate(names):
            if name == "geohash":
                continue
            status = results[i]["statuses"][0]
            if status != "PRESENT":
                raise RuntimeError(
                    f"feast lookup non-PRESENT for {geohash} feature={name}: {status}"
                )
            out[name] = float(results[i]["values"][0])
        # Sanity: all expected dynamic features must be present
        missing = set(self.manifest["dynamic_features"]) - set(out.keys())
        if missing:
            raise RuntimeError(f"feast response missing features: {missing}")
        return out

    def predict_from_vector(self, feature_vector: dict[str, float]) -> float:
        """Score a fully-formed feature dict. The Phase 4 cross-validation
        will use the same call shape against the Striim output.
        """
        ordered = [float(feature_vector[name]) for name in self.manifest["feature_order"]]
        X = np.array(ordered, dtype="float32").reshape(1, -1)
        d = xgb.DMatrix(X, feature_names=self.manifest["feature_order"])
        return float(self.booster.predict(d)[0])

    def predict_from_request(
        self,
        lat: float,
        lon: float,
        ts: datetime,
        trip_distance: float,
        passenger_count: int,
    ) -> tuple[str, dict[str, float], float]:
        """Full path: geohash the coord, look up Feast, assemble event +
        dynamic features, and predict. Returns
        (geohash, ordered feature_vector, prediction).
        """
        geohash = encode_geohash(lat, lon, precision=self.manifest["geohash_precision"])
        dynamic = self.lookup_dynamic(geohash)
        ts_pd = pd.Timestamp(ts)
        # Build a single-row DataFrame so we can reuse compute_event_features.
        trip_row = pd.DataFrame(
            {
                "pickup_ts": [ts_pd],
                "trip_distance": [trip_distance],
                "passenger_count": [passenger_count],
            }
        )
        event_df = compute_event_features(trip_row).iloc[0]
        feature_vector = {
            **{k: float(event_df[k]) for k in EVENT_FEATURES},
            **dynamic,
        }
        pred = self.predict_from_vector(feature_vector)
        return geohash, feature_vector, pred


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _golden_records() -> Iterable[dict]:
    with open(GOLDEN_PATH) as f:
        for line in f:
            yield json.loads(line)


@click.group()
def cli() -> None:
    """Oracle: validation harness for the offline stack."""


@cli.command("validate")
@click.option("--feast-url", default=DEFAULT_FEAST_URL,
              help="Feast HTTP serving base URL")
def cmd_validate(feast_url: str) -> None:
    """Run both self-tests. Exits non-zero on any failure."""
    oracle = Oracle(feast_url=feast_url)

    # Test 1: model golden-set
    click.echo("=== test 1: model golden-set (no Feast) ===")
    n = 0
    n_fail = 0
    max_abs_err = 0.0
    for rec in _golden_records():
        n += 1
        pred = oracle.predict_from_vector(rec["feature_vector"])
        err = abs(pred - rec["expected_prediction"])
        max_abs_err = max(max_abs_err, err)
        if err > ABS_TOL:
            n_fail += 1
            click.echo(
                f"  FAIL  geohash={rec['geohash']}  expected={rec['expected_prediction']:.6f}  "
                f"got={pred:.6f}  err={err:.2e}"
            )
    click.echo(f"  checked={n}  failed={n_fail}  max_abs_err={max_abs_err:.2e}")
    if n_fail > 0:
        sys.exit(1)

    # Test 2: end-to-end via Feast (synthetic trip event)
    click.echo("\n=== test 2: end-to-end via Feast HTTP ===")
    try:
        geohashes = list({rec["geohash"] for rec in _golden_records()})[:5]
        ts = pd.Timestamp("2015-01-31T23:00:00")
        for gh in geohashes:
            dynamic = oracle.lookup_dynamic(gh)
            # Synthetic event: a 2-mile, 1-passenger trip starting at ts.
            trip_row = pd.DataFrame(
                {
                    "pickup_ts": [ts],
                    "trip_distance": [2.0],
                    "passenger_count": [1],
                }
            )
            event = compute_event_features(trip_row).iloc[0]
            fv = {
                **{k: float(event[k]) for k in EVENT_FEATURES},
                **dynamic,
            }
            pred = oracle.predict_from_vector(fv)
            click.echo(f"  cell={gh}  pred_fare=${pred:.2f}  (1hr_count={dynamic['trip_count_last_1hr']:.0f})")
        click.echo("  end-to-end OK")
    except requests.exceptions.ConnectionError:
        click.echo(
            "  SKIP: Feast server is not running. Start it with:\n"
            "        cd model/feature_repo && uv run feast serve --host 127.0.0.1 --port 6566"
        )
        sys.exit(2)


@cli.command("predict")
@click.argument("lat", type=float)
@click.argument("lon", type=float)
@click.argument("ts")
@click.argument("trip_distance", type=float)
@click.option("--passengers", type=int, default=1)
@click.option("--feast-url", default=DEFAULT_FEAST_URL)
def cmd_predict(
    lat: float, lon: float, ts: str, trip_distance: float,
    passengers: int, feast_url: str,
) -> None:
    """One-shot prediction.

    Example: oracle.py predict 40.75 -73.98 2015-01-31T23:00:00 2.5 --passengers 2
    """
    oracle = Oracle(feast_url=feast_url)
    gh, fv, pred = oracle.predict_from_request(
        lat, lon, pd.Timestamp(ts).to_pydatetime(),
        trip_distance=trip_distance, passenger_count=passengers,
    )
    click.echo(json.dumps(
        {"geohash": gh, "feature_vector": fv, "prediction": pred, TARGET: pred},
        indent=2,
    ))


if __name__ == "__main__":
    cli()
