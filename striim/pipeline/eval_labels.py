#!/usr/bin/env python3
"""Host-side label-join evaluator for the FCVAE pipeline (F3).

Joins operator-dropped ground-truth label files against the scored JSON
outputs and writes eval_metrics.json atomically. The ModelQualityAgent's
model_precision / model_recall / model_f1 / anomaly_rate signal families read
that file (staleness-gated on computed_utc), and F4's metric_degradation
retrain condition consumes those signals.

Label files (CSV, header combo_key,window_end,true_label) live in a drop
directory; window_end is the hour-floored ISO form YYYY-MM-DDTHH:00:00. In
production these come from case dispositions; the demo generates them with
make_fcvae_labels.py from the synthetic source's known labels.

Join key: (combo_key, floor_hour(window_end)). The scored window_end is the
first-txn-in-hour timestamp of the window's last hour, so flooring both sides
to the hour is the natural join.

Per combo the eval section reports raw last-point P/R/F1 (+ tp/fp/fn) and the
DONUT-style point-adjusted P/R/F1 via the canonical fcvae.metrics
implementation (the same code that produced the published manifests' baseline
numbers). Joined hours are partitioned into runs of consecutive hours with one
all-negative sentinel between runs, so a ground-truth segment never spans an
unscored gap (conservative on recall; gap_count reports the boundaries).

The scoring section is labels-independent so anomaly_rate stays live with no
labels dropped: the anomaly rate over the trailing --rate-window HOURS of
event time ending at the combo's latest scored window. A time window (not a
count window) keeps the result deterministic when a combo has empty hours:
the trailing 96 hours of a fresh 5-day feed are exactly the clean post-warm-up
windows, whether or not earlier contaminated keys exist in the pool.

Each combo also embeds the published manifest's accepted eval numbers as
`baseline` (from <models-dir>/<combo>/model.manifest.json), which is what the
agent gates degradation against (relative floors), and it self-updates after
every retrain because this file is re-read on every run.

Decision semantics are already applied upstream: the scored record's
is_anomaly field IS the model decision (anomaly = score < threshold).

Subcommands:
  run    one evaluation pass (cron-friendly). Exit 0 = file written (including
         the no-labels case), 2 = no scored records at all or output
         unwritable.
  watch  run forever every --poll-sec, each pass isolated (a transient never
         kills the loop); every pass rewrites the file so computed_utc
         advances (evaluator liveness IS the agent's staleness signal).

Invocation (the uv env provides numpy + the editable fcvae package):
  (cd python && uv run python ../striim/pipeline/eval_labels.py run ...)
"""
import csv
import datetime as dt
import glob
import json
import os
import sys
import tempfile
import time
import traceback

import click

# Run under the python/ uv env for numpy. The fcvae package root is added
# explicitly (relative to this file) so script-mode invocation never depends
# on cwd or on the editable-install .pth being processed; check_fcvae_swap
# lives beside this file (sys.path[0] in script mode, added for module mode).
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (_HERE, os.path.abspath(os.path.join(_HERE, "..", "..", "python"))):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import check_fcvae_swap as cfs
from fcvae.metrics import point_adjusted_f1

DEFAULT_PRED_DIR = "/opt/Striim/UploadedFiles"
DEFAULT_PREFIXES = "fcvae_scored,fcvae_accel"
DEFAULT_LABELS_DIR = "/opt/Striim/UploadedFiles/ground_truth"
DEFAULT_OUT = "/opt/Striim/UploadedFiles/fcvae_eval/eval_metrics.json"
DEFAULT_MODELS_DIR = "/opt/Striim/fcvae-models"


def floor_hour(t):
    return t.replace(minute=0, second=0, microsecond=0)


def load_scored(pred_dir, prefixes):
    """{combo: {hour_dt: bool_decision}}, last record wins per (combo, hour)
    in sorted-glob file order (the check_fcvae_swap collect() rule)."""
    scored = {}
    for prefix in prefixes:
        for r in cfs.all_records(pred_dir, prefix):
            combo = str(r.get("combo_key", "")).strip()
            we = cfs.parse_window_end(r.get("window_end"))
            if not combo or we is None:
                continue
            scored.setdefault(combo, {})[floor_hour(we)] = cfs.parse_bool(
                r.get("is_anomaly"))
    return scored


def load_labels(labels_dir):
    """({combo: {hour_dt: 0/1}}, [basenames], bad_row_count).

    Deterministic dedup: files in lexicographic basename order, rows in file
    order, LAST WRITE WINS per (combo, hour). Malformed rows are counted and
    skipped, never fatal.
    """
    labels = {}
    files = sorted(glob.glob(os.path.join(labels_dir, "*.csv")),
                   key=os.path.basename)
    bad_rows = 0
    for path in files:
        try:
            with open(path, newline="") as f:
                for row in csv.DictReader(f):
                    try:
                        combo = str(row["combo_key"]).strip()
                        hour = floor_hour(dt.datetime.fromisoformat(
                            str(row["window_end"]).strip()))
                        lab = int(str(row["true_label"]).strip())
                        if not combo or lab not in (0, 1):
                            raise ValueError(row)
                    except (KeyError, TypeError, ValueError):
                        bad_rows += 1
                        continue
                    labels.setdefault(combo, {})[hour] = lab
        except OSError:
            bad_rows += 1
    return labels, [os.path.basename(p) for p in files], bad_rows


def consecutive_runs(hours):
    """Partition sorted hour datetimes into maximal runs of consecutive hours."""
    runs = []
    one_hour = dt.timedelta(hours=1)
    for h in hours:
        if runs and h - runs[-1][-1] == one_hour:
            runs[-1].append(h)
        else:
            runs.append([h])
    return runs


def raw_metrics(pairs):
    """Pointwise last-point P/R/F1 over (decision, truth) pairs."""
    tp = sum(1 for d, t in pairs if d and t)
    fp = sum(1 for d, t in pairs if d and not t)
    fn = sum(1 for d, t in pairs if not d and t)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) > 0 else 0.0)
    return {"precision": precision, "recall": recall, "f1": f1,
            "tp": tp, "fp": fp, "fn": fn}


def eval_combo(decisions, truth):
    """The per-combo eval section from joined {hour: decision} / {hour: label}."""
    joined_hours = sorted(set(decisions) & set(truth))
    label_hours = sorted(truth)
    section = {
        "n_labels": len(truth),
        "n_joined": len(joined_hours),
        "coverage": len(joined_hours) / len(truth) if truth else 0.0,
        "gap_count": 0,
        "label_span": {"start": label_hours[0].strftime("%Y-%m-%dT%H:%M:%S"),
                       "end": label_hours[-1].strftime("%Y-%m-%dT%H:%M:%S")}
                      if label_hours else None,
        "raw": None,
        "point_adjusted": None,
    }
    if not joined_hours:
        return section

    runs = consecutive_runs(joined_hours)
    section["gap_count"] = len(runs) - 1

    # Sentinel-joined series: one (False, False) element between runs breaks
    # ground-truth segment contiguity across unscored hours and contributes
    # nothing to tp/fp/fn.
    preds, gts = [], []
    for i, run in enumerate(runs):
        if i:
            preds.append(False)
            gts.append(False)
        for h in run:
            preds.append(bool(decisions[h]))
            gts.append(bool(truth[h]))

    pa = point_adjusted_f1(preds, gts)
    section["point_adjusted"] = {k: pa[k] for k in (
        "precision", "recall", "f1", "tp", "fp", "fn",
        "tp_segments", "fn_segments", "total_segments")}
    section["raw"] = raw_metrics([(decisions[h], truth[h]) for h in joined_hours])
    return section


def scoring_combo(decisions, rate_window):
    """The labels-independent scoring section from {hour: decision}."""
    hours = sorted(decisions)
    latest = hours[-1]
    window_start = latest - dt.timedelta(hours=rate_window - 1)
    recent = [h for h in hours if h >= window_start]
    return {
        "anomaly_rate": sum(1 for h in recent if decisions[h]) / len(recent),
        "n_windows": len(recent),
        "rate_window_hours": rate_window,
        "n_scored_total": len(hours),
        "latest_window_end": latest.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def baseline_combo(models_dir, combo):
    """Published accepted baseline from the combo's manifest, or None."""
    path = os.path.join(models_dir, combo, "model.manifest.json")
    try:
        with open(path) as f:
            manifest = json.load(f)
        pa = manifest["eval"]["point_adjusted_last_point"]
        raw = manifest.get("eval", {}).get("raw_last_point", {})
        return {
            "model_version": manifest.get("model_version"),
            "pa_f1": float(pa["f1"]),
            "pa_precision": float(pa["precision"]),
            "pa_recall": float(pa["recall"]),
            "raw_f1": float(raw["f1"]) if isinstance(
                raw.get("f1"), (int, float)) else None,
            "manifest_path": path,
        }
    except (OSError, ValueError, KeyError, TypeError):
        return None


def evaluate(pred_dir, prefixes, labels_dir, rate_window, models_dir, combos):
    scored = load_scored(pred_dir, prefixes)
    labels, label_files, bad_rows = load_labels(labels_dir)

    combo_keys = sorted(set(scored) | set(labels))
    if combos:
        combo_keys = [c for c in combo_keys if c in combos]

    result = {
        "schema_version": 1,
        "computed_utc": dt.datetime.now(dt.timezone.utc).isoformat(
            timespec="seconds"),
        "inputs": {"pred_dir": pred_dir, "prefixes": prefixes,
                   "labels_dir": labels_dir, "label_files": label_files,
                   "bad_label_rows": bad_rows},
        "combos": {},
    }
    for combo in combo_keys:
        decisions = scored.get(combo)
        truth = labels.get(combo)
        result["combos"][combo] = {
            "scoring": scoring_combo(decisions, rate_window) if decisions else None,
            "eval": eval_combo(decisions or {}, truth) if truth else None,
            "baseline": baseline_combo(models_dir, combo) if models_dir else None,
        }
    return result, sum(len(v) for v in scored.values())


def write_atomic(out, result):
    out_dir = os.path.dirname(os.path.abspath(out)) or "."
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".eval_", suffix=".tmp", dir=out_dir)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(result, f, indent=2)
        os.chmod(tmp, 0o644)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def run_once(pred_dir, prefixes, labels_dir, out, rate_window, models_dir,
             combos, require_scored=True):
    result, n_scored = evaluate(pred_dir, prefixes, labels_dir, rate_window,
                                models_dir, combos)
    if require_scored and n_scored == 0:
        click.echo(f"eval: no scored records under {pred_dir} for prefixes "
                   f"{prefixes}; nothing written")
        return 2
    try:
        write_atomic(out, result)
    except OSError as exc:
        click.echo(f"eval: cannot write {out}: {exc}")
        return 2
    summary = []
    for combo, block in result["combos"].items():
        ev = block["eval"]
        sc = block["scoring"]
        rate = f"rate={sc['anomaly_rate']:.4f}/{sc['n_windows']}w" if sc else "rate=NA"
        pa = ev and ev["point_adjusted"]
        summary.append(
            f"{combo}[{rate} "
            + (f"paF1={pa['f1']:.4f} joined={ev['n_joined']}/{ev['n_labels']}"
               if pa else "eval=NA") + "]")
    click.echo(f"eval: wrote {out} combos={len(result['combos'])} "
               f"{' '.join(summary)}")
    return 0


def common_options(fn):
    for deco in reversed([
        click.option("--pred-dir", "pred_dir", default=DEFAULT_PRED_DIR,
                     show_default=True, help="Directory of scored JSON files."),
        click.option("--prefixes", default=DEFAULT_PREFIXES, show_default=True,
                     help="CSV of scored file prefixes to pool."),
        click.option("--labels-dir", "labels_dir", default=DEFAULT_LABELS_DIR,
                     show_default=True, help="Ground-truth label drop directory."),
        click.option("--out", default=DEFAULT_OUT, show_default=True,
                     type=click.Path(dir_okay=False), help="eval_metrics.json path."),
        click.option("--rate-window", "rate_window", default=96, show_default=True,
                     type=int, help="Trailing hours for the anomaly_rate window."),
        click.option("--models-dir", "models_dir", default=DEFAULT_MODELS_DIR,
                     show_default=True,
                     help="Published model bundles root ('' disables baseline embedding)."),
        click.option("--combos", default="", show_default=True,
                     help="CSV restriction of combo keys (default: all seen)."),
    ]):
        fn = deco(fn)
    return fn


def parse_common(prefixes, models_dir, combos):
    prefix_list = [p.strip() for p in prefixes.split(",") if p.strip()]
    combo_set = {c.strip() for c in combos.split(",") if c.strip()} or None
    return prefix_list, (models_dir or None), combo_set


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@common_options
def run(pred_dir, prefixes, labels_dir, out, rate_window, models_dir, combos):
    """One evaluation pass; exit 0 written, 2 nothing to evaluate/unwritable."""
    prefix_list, models_dir, combo_set = parse_common(prefixes, models_dir, combos)
    sys.exit(run_once(pred_dir, prefix_list, labels_dir, out, rate_window,
                      models_dir, combo_set))


@cli.command()
@common_options
@click.option("--poll-sec", "poll_sec", default=30, show_default=True, type=int,
              help="Seconds between evaluation passes.")
def watch(pred_dir, prefixes, labels_dir, out, rate_window, models_dir, combos,
          poll_sec):
    """Evaluate forever; each pass isolated, computed_utc advances per pass."""
    prefix_list, models_dir, combo_set = parse_common(prefixes, models_dir, combos)
    click.echo(f"watch: every {poll_sec}s -> {out}")
    while True:
        try:
            run_once(pred_dir, prefix_list, labels_dir, out, rate_window,
                     models_dir, combo_set, require_scored=False)
        except Exception:
            traceback.print_exc()
        time.sleep(poll_sec)


if __name__ == "__main__":
    cli()
