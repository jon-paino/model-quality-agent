"""Feast entity and FeatureView definitions for the FCVAE scoring params (F2).

Run `feast apply` from this directory (or `python -m fcvae.feast_setup apply`,
which also seeds the offline parquet) to register these in the registry.

One row per model combo (entity `combo_key`, e.g. "Penny_All", "Accel_CMP"),
carrying the swap-safe scoring params the FCVAEParamsOp reads over HTTP and
stamps into userdata: the scaler mean/scale, the last-point decision
threshold, the scorer's normal-score stats (may be null), and the
model_version string ("sha256:<12hex>") the scorer OP matches against its
live handle before trusting the params.

The offline batch source is a one-row placeholder parquet
(`data/params_seed.parquet`, written by `fcvae.feast_setup apply`); it exists
so `feast apply` can infer the entity column's dtype, mirroring the taxi repo
where the offline parquet is built before apply. It is never materialized:
all real rows arrive via `FeatureStore.write_to_online_store`.
"""

from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float64, String

HERE = Path(__file__).resolve().parent

combo_key = Entity(
    name="combo_key",
    join_keys=["combo_key"],
    description="FCVAE model combo (e.g. Penny_All, Accel_CMP)",
)

params_batch_source = FileSource(
    name="fcvae_scoring_params_batch",
    path=str(HERE / "data" / "params_seed.parquet"),
    timestamp_field="event_timestamp",
)

fcvae_scoring_params_v1 = FeatureView(
    name="fcvae_scoring_params_v1",
    entities=[combo_key],
    ttl=timedelta(days=365),
    schema=[
        Field(name="scaler_mean", dtype=Float64),
        Field(name="scaler_scale", dtype=Float64),
        Field(name="last_point_threshold", dtype=Float64),
        Field(name="normal_score_mean", dtype=Float64),
        Field(name="normal_score_std", dtype=Float64),
        Field(name="model_version", dtype=String),
    ],
    online=True,
    source=params_batch_source,
    description="Per-combo FCVAE scoring params (scaler, threshold, "
                "normal-score stats, model version)",
)
