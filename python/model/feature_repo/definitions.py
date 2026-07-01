"""Feast entity, FeatureView, and PushSource definitions.

Run `feast apply` (from this directory) to register these in the Feast
registry. The offline batch source is `data/cell_features.parquet`,
produced by `model.feast_setup build_offline`.

Materialization is offline-only for the initial bulk push of train-time
aggregates. The streaming writer pushes fresh aggregates against the same
PushSource at runtime, keeping the online store current.
"""

from datetime import timedelta
from pathlib import Path

from feast import Entity, FeatureView, Field, FileSource, PushSource
from feast.types import Float32

HERE = Path(__file__).resolve().parent

geohash = Entity(
    name="geohash",
    join_keys=["geohash"],
    description="Pickup geohash cell (precision 6)",
)

cell_batch_source = FileSource(
    name="cell_dynamic_features_batch",
    path=str(HERE / "data" / "cell_features.parquet"),
    timestamp_field="observation_ts",
    created_timestamp_column="created_ts",
)

cell_push_source = PushSource(
    name="cell_dynamic_features_push",
    batch_source=cell_batch_source,
)

cell_dynamic_features_v1 = FeatureView(
    name="cell_dynamic_features_v1",
    entities=[geohash],
    ttl=timedelta(days=365),
    schema=[
        Field(name="trip_count_last_10min", dtype=Float32),
        Field(name="trip_count_last_1hr", dtype=Float32),
        Field(name="trip_count_last_4hr", dtype=Float32),
        Field(name="trip_count_last_24hr", dtype=Float32),
        Field(name="avg_fare_last_1hr", dtype=Float32),
        Field(name="unique_dropoff_zones_last_1hr", dtype=Float32),
        Field(name="cell_active_minutes_last_1hr", dtype=Float32),
        Field(name="trip_count_delta_10min", dtype=Float32),
        Field(name="trip_count_same_hour_yesterday", dtype=Float32),
        Field(name="trip_count_same_hour_last_week", dtype=Float32),
    ],
    online=True,
    source=cell_push_source,
    description="Velocity / aggregate features per geohash cell",
)
