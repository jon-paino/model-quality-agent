"""Row-for-row cross-validation of the Striim inference pipeline.

Phase 4 validation. Replays the 50 frozen records in
`artifacts/golden_set.jsonl` through the live Striim pipeline
(`inference.FareInference`) and checks each pipeline prediction against the
record's `expected_prediction` (the XGBoost score captured at train time).

The pipeline does a live Feast lookup, so Feast must return each golden
record's exact frozen dynamic vector. Golden records share geohashes (the same
cell at different observation times) but the Feast online store holds one
vector per key, so each record is remapped to a unique synthetic geohash
`cvNNNN`. The model never uses the geohash as a feature, only as the Feast key,
so the remap changes no prediction. All 50 distinct vectors are pushed at once
and the whole set runs through the pipeline in a single batch.

Tolerance is $10^{-4}$: the pipeline scores with ONNX Runtime, and cross-runtime
float32 drift from native XGBoost is ~$2 \\times 10^{-5}$ (established in
export_onnx.py). This is looser than oracle.py's $10^{-5}$ XGBoost-vs-XGBoost
bound and matches export_onnx.ONNX_ABS_TOL and ModelOp's GOLDEN_TOL.

Workflow (`uv run python -m model.crossval <cmd>`):

  prep            push the 50 golden dynamic vectors to Feast under synthetic
                  geohashes and write the pipeline input CSV
  check [PATH]    parse the pipeline's JSON output and compare every prediction
                  to expected_prediction within tolerance

Between the two, the operator copies the CSV into Striim and runs the pipeline
(`prep` prints the exact commands).
"""

from __future__ import annotations

import glob
import json
import os
import sys
from pathlib import Path
from typing import Iterator

import click
import pandas as pd

from .feast_writer import push_features
from .features import DYNAMIC_FEATURES, EVENT_FEATURES
from .train import GOLDEN_PATH

# The generated pipeline input feed. Copied into Striim's UploadedFiles under a
# name matching the pipeline FileReader wildcard `pipeline_trips*.csv`.
REPO_ROOT = Path(__file__).resolve().parent.parent
CROSSVAL_CSV = REPO_ROOT / "striim" / "pipeline" / "crossval_trips.csv"

# Where Striim's FileWriter writes the pipeline's JSON output.
DEFAULT_OUTPUT_DIR = "/opt/Striim/UploadedFiles"

# Cross-runtime tolerance. The pipeline scores through ONNX Runtime; drift from
# the native-XGBoost golden predictions is ~2e-5 float32 accumulation noise
# (see export_onnx.ONNX_ABS_TOL, which carries the full rationale). 1e-4 keeps
# headroom over that floor while still catching a real regression.
ABS_TOL = 1e-4


def _synth_geohash(i: int) -> str:
    """Synthetic geohash for golden record `i`: 6 chars, all in the base32
    geohash alphabet, and disjoint from the real cells (dr5r*, dr72*).
    """
    return f"cv{i:04d}"


def _golden_records() -> Iterator[dict]:
    with open(GOLDEN_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _load_predictions(path: str) -> list[dict]:
    """Load prediction records from Striim's JSONFormatter output.

    `path` is a single JSON file or a directory; for a directory every
    `predictions*.json` is read. Each file is a JSON array of objects; rollover
    stub files that do not parse as JSON are skipped.
    """
    p = Path(path)
    files = sorted(glob.glob(str(p / "predictions*.json"))) if p.is_dir() else [str(p)]
    if not files:
        raise FileNotFoundError(f"no predictions*.json found under {path}")

    out: list[dict] = []
    for f in files:
        text = Path(f).read_text().strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            click.echo(f"  (skipped unparseable {os.path.basename(f)})")
            continue
        if isinstance(data, list):
            out.extend(d for d in data if isinstance(d, dict))
        elif isinstance(data, dict):
            out.append(data)
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """Cross-validate the Striim pipeline against the golden set."""


@cli.command("prep")
def cmd_prep() -> None:
    """Push the 50 golden dynamic vectors to Feast and write the input CSV.

    Each golden record is remapped to a unique synthetic geohash so the online
    store can hold all 50 point-in-time vectors at once. Pushes are dated `now`,
    so the fresh synthetic keys carry zero age against the FeatureView TTL.
    """
    records = list(_golden_records())
    n = len(records)
    if n == 0:
        raise click.ClickException(f"no golden records in {GOLDEN_PATH}")
    click.echo(f"[crossval] {n} golden records from {GOLDEN_PATH.name}")

    # Feast push frame: one row per record, keyed by its synthetic geohash,
    # carrying that record's 10 frozen dynamic features.
    feat_rows = [
        {"pickup_geohash": _synth_geohash(i),
         **{name: rec["feature_vector"][name] for name in DYNAMIC_FEATURES}}
        for i, rec in enumerate(records)
    ]
    feat_df = pd.DataFrame(feat_rows, columns=["pickup_geohash", *DYNAMIC_FEATURES])
    pushed = push_features(feat_df, pd.Timestamp.now())
    click.echo(f"[push] {pushed} synthetic cell(s) -> Feast online store")

    # Pipeline input CSV: one trip per record, same synthetic key, carrying the
    # record's 5 event features. Column order is the pipeline contract
    # (data[0] = geohash, data[1..5] = event features in manifest order).
    trip_rows = [
        {"pickup_geohash": _synth_geohash(i),
         **{name: rec["feature_vector"][name] for name in EVENT_FEATURES}}
        for i, rec in enumerate(records)
    ]
    trip_df = pd.DataFrame(trip_rows, columns=["pickup_geohash", *EVENT_FEATURES])
    CROSSVAL_CSV.parent.mkdir(parents=True, exist_ok=True)
    trip_df.to_csv(CROSSVAL_CSV, index=False)
    click.echo(f"[write] {CROSSVAL_CSV.relative_to(REPO_ROOT)}  ({len(trip_df)} trips)")

    click.echo(
        "\nNext:\n"
        "  1. Feast must be serving on 127.0.0.1:6566.\n"
        f"  2. Copy the feed in (fresh filename so FileReader picks it up):\n"
        f"       cp {CROSSVAL_CSV} {DEFAULT_OUTPUT_DIR}/pipeline_trips_crossval.csv\n"
        "  3. Wait for the pipeline to score all 50 events, then run:\n"
        "       uv run python -m model.crossval check"
    )


@cli.command("check")
@click.argument("output", required=False)
def cmd_check(output: str | None) -> None:
    """Compare the pipeline's predictions to the golden set within tolerance.

    OUTPUT is the Striim JSON output file or its directory; defaults to
    /opt/Striim/UploadedFiles (scans predictions*.json). Exits non-zero on any
    out-of-tolerance prediction or any unmatched golden record.
    """
    records = list(_golden_records())
    expected = {_synth_geohash(i): rec for i, rec in enumerate(records)}

    path = output or DEFAULT_OUTPUT_DIR
    rows = _load_predictions(path)

    # Keep only the synthetic crossval cells. Dedupe (last wins) so a stale
    # earlier run in the same output file does not double-count.
    seen: dict[str, float] = {}
    for row in rows:
        gh = row.get("pickup_geohash", "")
        if gh in expected:
            seen[gh] = float(row["prediction"])
    click.echo(f"[crossval] matched {len(seen)} / {len(expected)} synthetic "
               f"cells in {path}")

    missing = sorted(set(expected) - set(seen))
    if missing:
        shown = ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else "")
        click.echo(f"  WARNING: no pipeline prediction for {len(missing)} "
                   f"cell(s): {shown}")

    n = n_fail = 0
    max_err = 0.0
    for gh in sorted(seen):
        n += 1
        pred = seen[gh]
        exp = expected[gh]["expected_prediction"]
        err = abs(pred - exp)
        max_err = max(max_err, err)
        if err > ABS_TOL:
            n_fail += 1
            click.echo(
                f"  FAIL  {gh} (golden #{int(gh[2:])}, {expected[gh]['geohash']})  "
                f"expected={exp:.6f}  pipeline={pred:.6f}  err={err:.2e}"
            )

    click.echo(f"  checked={n}  failed={n_fail}  max_abs_err={max_err:.2e}  "
               f"tol={ABS_TOL:.0e}")
    if n_fail or missing or n != len(expected):
        sys.exit(1)
    click.echo("crossval OK: every pipeline prediction matches the golden set")


if __name__ == "__main__":
    cli()
