"""Streaming feature writer for the live inference demo (Phase 5).

Replays the held-out test slice (Jan 25-31, 2015) as a CDC-style event
stream. Maintains running per-cell aggregate state in this process
(warm-started from train+val), pushes a fresh feature vector to Feast every
B events, and hands the same B events to the Striim pipeline by writing a
feed CSV into UploadedFiles. The pipeline scores; the streamer polls
predictions*.json to confirm the batch landed before pushing the next.

The order within a batch is "score first, then update":

  1. Identify the cells touched by the batch.
  2. Compute their pre-batch state (strict-past aggregate at batch start)
     via feast_writer.compute_current_features, using the writer's
     running history.
  3. fs.push(... to=ONLINE) -- one REST call carrying M <= B cell vectors.
  4. Write the batch's B events to a Striim feed CSV; FileReader picks it
     up via the pipeline_trips*.csv wildcard.
  5. Wait until predictions*.json has grown by B records.
  6. Ingest the batch into the writer's running history for the NEXT batch.

Pre-batch ordering preserves train/serve consistency: events are scored
against the same strict-past windows the model was trained on, and the
writer remains the single source of truth for the per-cell state that
Feast publishes.

The state implementation is intentionally simple: the writer keeps a
growing DataFrame and recomputes touched cells' current features per batch
via the project's single `features.compute_dynamic_features` (through
`feast_writer.compute_current_features` and its probe-row trick). A future
iteration could swap in an incremental deque-based aggregator behind the
same interface; the public-facing behavior would not change.

CLI (`uv run python -m model.feast_streamer stream`):

  --limit N       cap on total test events to replay (default 300)
  --batch B       events per micro-batch (default 15)
  --feed-dir D    where to drop Striim feed CSVs (default UploadedFiles)
  --output-dir D  where Striim writes predictions*.json (default UploadedFiles)
  --timeout S     per-batch wait timeout in seconds (default 60)
"""

from __future__ import annotations

import glob
import json
import time
from pathlib import Path

import click
import pandas as pd

from .data import CLEANED_PARQUET
from .feast_writer import compute_current_features, push_features
from .features import compute_event_features

# The chronological split boundary the model was trained against
# (train+val < TEST_START, test >= TEST_START). Mirrors model/train.py.
# Used as the default `--anchor` so the Phase 5 invocation is unchanged.
TEST_START = pd.Timestamp("2015-01-25T00:00:00")

# Phase 6 default anchor for the hot-swap demo: Feb 1, with the Feb cleaned
# parquet supplied via `--stream-parquet`. All of January becomes warm-start
# history, the first ~300 Feb events become the stream.
DEFAULT_FEB_ANCHOR = pd.Timestamp("2015-02-01T00:00:00")

DEFAULT_FEED_DIR = "/opt/Striim/UploadedFiles"
DEFAULT_OUTPUT_DIR = "/opt/Striim/UploadedFiles"
DEFAULT_BATCH = 15
DEFAULT_LIMIT = 300

# Between batches we sleep this long. The lower bound is "Striim's FeatureOp
# has finished its lookups for the previous batch before we overwrite Feast
# with the next batch's state". Empirically Striim processes 15 events in
# well under a second (15 HTTP lookups + 15 ONNX scores), so 1.0s carries
# comfortable headroom.
DEFAULT_SLEEP_SECS = 1.0

# At end of run we wait for the FileWriter to close its active output file.
# Empirically Striim's JSONFormatter + FileWriter holds the active file open
# and rolls it on a ~60-second cadence, so after the last batch we may need
# to wait up to a minute before the trailing predictions become readable.
# Poll modestly, exit early when the target is reached.
FLUSH_MAX_SECS = 90.0
FLUSH_POLL_SECS = 2.0


def _read_predictions(output_dir: str) -> list[dict]:
    """Read all prediction records from Striim's JSONFormatter output.

    Quiet version of crossval._load_predictions: silently skips files that
    are still being written (the rollover stub Striim has opened but not yet
    closed). Returns an empty list if no readable files exist yet. Suitable
    for the streamer's per-poll loop where chatty skipped-file lines would
    drown the output.
    """
    p = Path(output_dir)
    out: list[dict] = []
    for f in sorted(glob.glob(str(p / "predictions*.json"))):
        try:
            text = Path(f).read_text().strip()
        except OSError:
            continue
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            out.extend(d for d in data if isinstance(d, dict))
        elif isinstance(data, dict):
            out.append(data)
    return out


def _count_predictions(output_dir: str) -> int:
    return len(_read_predictions(output_dir))


def _wait_for_flush(output_dir: str, target: int) -> int:
    """Poll until prediction count reaches `target` or `FLUSH_MAX_SECS` elapse.

    The Striim FileWriter rolls files on a ~60-second cadence, so after the
    last batch the active predictions file may take up to a minute to close.
    We poll modestly and exit early when the target is met.
    """
    start = time.time()
    last_print = 0.0
    while True:
        current = _count_predictions(output_dir)
        if current >= target:
            return current
        if time.time() - start > FLUSH_MAX_SECS:
            return current
        if time.time() - last_print > 15.0:
            click.echo(f"  ... visible {current}/{target}")
            last_print = time.time()
        time.sleep(FLUSH_POLL_SECS)


def _write_feed(events: pd.DataFrame, path: Path) -> None:
    """Write a Striim feed CSV: pickup_geohash + the 5 event features.

    Column order is the FileReader/DSVParser positional contract
    (data[0] = pickup_geohash, data[1..5] = event features in manifest order).
    Cast to float so the CSV format matches trips.csv exactly.
    """
    event_df = compute_event_features(events).reset_index(drop=True)
    feed = pd.DataFrame({
        "pickup_geohash": events["pickup_geohash"].reset_index(drop=True),
        "hour_of_day": event_df["hour_of_day"].astype(float),
        "day_of_week": event_df["day_of_week"].astype(float),
        "is_weekend": event_df["is_weekend"].astype(float),
        "trip_distance": event_df["trip_distance"].astype(float),
        "passenger_count": event_df["passenger_count"].astype(float),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    feed.to_csv(path, index=False)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """Streaming feature writer for the live inference demo."""


@cli.command("stream")
@click.option("--limit", default=DEFAULT_LIMIT, type=int,
              help="Max test events to replay (default 300; full slice is ~50k).")
@click.option("--batch", default=DEFAULT_BATCH, type=int,
              help="Events per micro-batch (default 15).")
@click.option("--feed-dir", default=DEFAULT_FEED_DIR,
              help="Where to drop Striim feed CSVs.")
@click.option("--output-dir", default=DEFAULT_OUTPUT_DIR,
              help="Where Striim's FileWriter writes predictions*.json.")
@click.option("--sleep", "sleep_secs", default=DEFAULT_SLEEP_SECS, type=float,
              help="Seconds to sleep between batches; lets Striim's FeatureOp "
                   "finish its lookups for batch N before the writer pushes "
                   "batch N+1's state (default 1.0).")
@click.option("--anchor", default=str(TEST_START.isoformat()), show_default=True,
              help="Stream-boundary timestamp; events with pickup_ts < anchor "
                   "warm-start the writer's history, events >= anchor stream.")
@click.option("--stream-parquet", "stream_parquet", default=None,
              type=click.Path(),
              help="Extra cleaned parquet concatenated onto the default Jan "
                   "parquet (used for the Phase 6 hot-swap demo: "
                   "trips_cleaned_2015-02.parquet with --anchor 2015-02-01).")
@click.option("--swap-after", "swap_after", default=None, type=int,
              help="Pause after this many micro-batches and prompt the "
                   "operator to swap the model file. The streamer then "
                   "resumes; the two halves' MAEs are reported separately.")
def cmd_stream(limit: int, batch: int, feed_dir: str, output_dir: str,
               sleep_secs: float, anchor: str, stream_parquet: str | None,
               swap_after: int | None) -> None:
    """Replay the test slice through the writer and Striim pipeline.

    Prerequisites: Feast is serving on 127.0.0.1:6566; the Striim
    inference.FareInference app is deployed and running and is configured to
    write predictions to OUTPUT-DIR. Feed CSV names match the FileReader
    wildcard `pipeline_trips*.csv`.
    """
    anchor_ts = pd.Timestamp(anchor)

    # Build the trip table. With --stream-parquet, concat the Phase 1 Jan
    # parquet (CLEANED_PARQUET) with the extra month so the writer's history
    # can reach back across the month boundary for dynamic features.
    click.echo(f"[streamer] loading {CLEANED_PARQUET}")
    trips = pd.read_parquet(CLEANED_PARQUET)
    if stream_parquet is not None:
        click.echo(f"[streamer] concat {stream_parquet}")
        extra = pd.read_parquet(stream_parquet)
        trips = pd.concat([trips, extra], ignore_index=True)
    trips = trips.sort_values("pickup_ts").reset_index(drop=True)

    train_val = trips[trips["pickup_ts"] < anchor_ts].copy()
    test = (trips[trips["pickup_ts"] >= anchor_ts]
            .sort_values("pickup_ts")
            .reset_index(drop=True)
            .head(limit))

    n_batches = (len(test) + batch - 1) // batch
    click.echo(f"[streamer] anchor: {anchor_ts.isoformat()}")
    click.echo(f"[streamer] warm-start history: {len(train_val):,} pre-anchor rows")
    click.echo(f"[streamer] replaying {len(test):,} post-anchor events in "
               f"{n_batches} batches of {batch}")
    if swap_after is not None:
        if not 0 < swap_after < n_batches:
            raise click.BadParameter(
                f"--swap-after must be in (0, {n_batches}); got {swap_after}")
        click.echo(f"[streamer] will pause for model swap after batch {swap_after}")

    feed_dir_p = Path(feed_dir)
    history = train_val
    baseline = _count_predictions(output_dir)
    click.echo(f"[streamer] baseline prediction count in {output_dir}: {baseline}\n")

    # Track the batch index at which the swap happened so the MAE reporter
    # can split the stream into v1 vs v2 halves. None if --swap-after was
    # not used (single-slice MAE only).
    swap_batch_idx: int | None = None

    t0 = time.time()
    for batch_idx in range(n_batches):
        # Gap between batches: let Striim's FeatureOp finish reading Feast
        # for the previous batch before we overwrite the relevant cells.
        # Striim's FileWriter is not on this critical path; it flushes async.
        if batch_idx > 0:
            time.sleep(sleep_secs)

        chunk = test.iloc[batch_idx * batch:(batch_idx + 1) * batch].copy()
        cells = chunk["pickup_geohash"].unique().tolist()
        t_batch = pd.Timestamp(chunk["pickup_ts"].min())

        # 1. Pre-batch state for the cells touched by this batch.
        features = compute_current_features(history, t_batch, cells)
        push_features(features, t_batch)

        # 2. Hand events to Striim. A fresh filename per batch so the
        #    FileReader (which tracks files by name) picks each one up.
        feed_path = feed_dir_p / f"pipeline_trips_stream_{batch_idx:06d}.csv"
        _write_feed(chunk, feed_path)

        # 3. Ingest into the writer's running history for the next batch.
        history = pd.concat([history, chunk], ignore_index=True)

        if batch_idx == 0 or (batch_idx + 1) % 5 == 0 or batch_idx == n_batches - 1:
            click.echo(f"  batch {batch_idx + 1:3d}/{n_batches}  "
                       f"t={t_batch}  cells={len(cells):2d}  "
                       f"pushed={len(features):2d}")

        # 4. If a swap point was requested, pause after this batch and prompt
        #    the operator. The swap point sits BETWEEN this batch and the
        #    next so the boundary in `predictions*.json` is unambiguous.
        if swap_after is not None and (batch_idx + 1) == swap_after:
            events_so_far = (batch_idx + 1) * batch
            click.echo()
            click.echo(f"[streamer] PAUSED after batch {swap_after}/{n_batches} "
                       f"({events_so_far} events scored with current model).")
            click.echo(f"[streamer] To swap: cp model/artifacts/model-v2.onnx "
                       f"/opt/Striim/UploadedFiles/model.onnx")
            click.echo(f"[streamer] Watch /opt/Striim/logs/ for the "
                       f"'ModelOp: hot-reloaded ...' line.")
            click.prompt("[streamer] Press ENTER when ready to continue",
                         default="", show_default=False)
            swap_batch_idx = batch_idx + 1
            click.echo()

    push_elapsed = time.time() - t0
    click.echo(f"\n[streamer] pushed {n_batches} batches in {push_elapsed:.1f}s "
               f"({len(test) / push_elapsed:.1f} events/s)")

    # The FileWriter holds the active predictions.NN.json open and only
    # closes it on idle (~10-15s) or on a roll triggered by the next batch.
    # After the last batch, wait for the idle flush.
    click.echo(f"[streamer] waiting for Striim to flush final predictions...")
    target = baseline + len(test)
    observed = _wait_for_flush(output_dir, target)
    click.echo(f"[streamer] visible predictions: {observed} (expected {target})")
    elapsed = time.time() - t0
    click.echo(f"[streamer] total wall time: {elapsed:.1f}s")

    # Closing metric: compare the pipeline's predictions against the events'
    # actual fare_amount. If a swap point was used, report two MAEs: the
    # events scored before the swap (under the original model) and the events
    # scored after (under the swapped-in model).
    final = _read_predictions(output_dir)
    new_records = final[baseline:baseline + len(test)]
    if len(new_records) != len(test):
        click.echo(f"[mae] cannot pair predictions to events: "
                   f"got {len(new_records)} new records, expected {len(test)}")
        return

    pred = pd.Series([float(r["prediction"]) for r in new_records]).to_numpy()
    actual = test["fare_amount"].astype(float).to_numpy()

    if swap_batch_idx is None:
        mae = float(abs(pred - actual).mean())
        click.echo(f"[mae] {len(new_records)} events: pipeline MAE = ${mae:.3f}  "
                   f"(Phase 1 offline test MAE: $1.283)")
        return

    split = swap_batch_idx * batch
    mae_pre = float(abs(pred[:split] - actual[:split]).mean())
    mae_post = float(abs(pred[split:] - actual[split:]).mean())
    click.echo(f"[mae] pre-swap  (events 1..{split:3d}): pipeline MAE = ${mae_pre:.3f}")
    click.echo(f"[mae] post-swap (events {split+1:3d}..{len(test):3d}): pipeline MAE = ${mae_post:.3f}")
    click.echo(f"[mae] delta = ${mae_post - mae_pre:+.3f}  "
               f"(negative = post-swap model is better)")


if __name__ == "__main__":
    cli()
