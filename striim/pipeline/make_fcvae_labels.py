#!/usr/bin/env python3
"""Generate per-hour ground-truth label files for the FCVAE eval (F3).

Reproduces python/fcvae/preprocess.py's hourly series rules EXACTLY, restricted
to a base slice, so the emitted labels are the canonical ground truth for the
scored windows a shifted feed of that slice produces:

  Penny_All  rows with amount < 1.00; hourly label = max(penny_is_anomaly)
             (the source CSV has the column; preprocess falls back to
             is_anomaly only when it is absent).
  Accel_CMP  rows with network_type == 'Accel' and transaction_type == 'CMP';
             hourly label = max(is_anomaly).

Hours with no matching rows get label 0 (preprocess's reindex-fill-0 rule).
The per-hour max over a filtered subset is local to the hour, so slicing the
source commutes with the pandas pipeline.

Emitted hour range is [--warmup-hours, days*24 - 1 - --drop-tail-hours]
(defaults: offsets 23..118 of a 5-day slice, 96 rows per combo):
  - the first 23 hours are excluded because those scored windows mix the
    previous feed's tail inside the KEEP 24 ROWS sliding window (the same
    warm-up rule check_fcvae_swap.py applies), so they must never join;
  - the final hour is excluded because its jumping window only closes when a
    LATER feed's first event arrives, so it is never scored in-feed.

window_end is written in SHIFTED time (start + shift-days + offset hours) as
hour-floored ISO YYYY-MM-DDTHH:00:00, the eval_labels.py join key, so a label
file joins the shifted feed's scored output directly.

Output CSV: combo_key,window_end,true_label (atomic: .tmp + os.replace).
"""
import csv
import datetime as dt
import os
import tempfile

import click

from make_fcvae_feed import DEFAULT_SOURCE

# Explicit registry: combo_key -> row filter + label column. preprocess.py's
# combo naming (f"{network}_{txn.replace('-', '')}") is lossy, so combos are
# registered here rather than parsed from the key.
COMBO_RULES = {
    "Penny_All": {
        "match": lambda net, txn, amount: amount < 1.00,
        "label_col": "penny_is_anomaly",
    },
    "Accel_CMP": {
        "match": lambda net, txn, amount: net == "Accel" and txn == "CMP",
        "label_col": "is_anomaly",
    },
}


def build_hourly_labels(source, start, days, combos):
    """Stream the source once; return {combo: [0/1] * days*24} hourly labels."""
    n_hours = days * 24
    labels = {c: [0] * n_hours for c in combos}
    start_iso = start.isoformat()
    end_iso = (start + dt.timedelta(days=days)).isoformat()
    with open(source, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        idx = {name: i for i, name in enumerate(header)}
        for combo in combos:
            col = COMBO_RULES[combo]["label_col"]
            if col not in idx:
                raise click.ClickException(
                    f"source {source} has no '{col}' column (header: {header})")
        i_ts, i_net, i_txn = idx["timestamp"], idx["network_type"], idx["transaction_type"]
        i_amt = idx["amount"]
        for row in reader:
            ts = row[i_ts]
            date = ts[:10]
            if date < start_iso:
                continue
            if date >= end_iso:
                break  # source rows are in event-time order
            # floor('h') for fixed-width ISO timestamps is pure string slicing.
            hour = (dt.date.fromisoformat(date) - start).days * 24 + int(ts[11:13])
            net, txn, amount = row[i_net], row[i_txn], float(row[i_amt])
            for combo in combos:
                rule = COMBO_RULES[combo]
                if rule["match"](net, txn, amount):
                    lab = int(row[idx[rule["label_col"]]])
                    if lab > labels[combo][hour]:
                        labels[combo][hour] = lab
    return labels


def segments(lab):
    """Contiguous 1-runs as (start, end) inclusive, mirroring fcvae.metrics."""
    out, seg_start = [], None
    for i, v in enumerate(lab):
        if v and seg_start is None:
            seg_start = i
        elif not v and seg_start is not None:
            out.append((seg_start, i - 1))
            seg_start = None
    if seg_start is not None:
        out.append((seg_start, len(lab) - 1))
    return out


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option("--start-date", "start_date", required=True,
              help="Base slice start date, YYYY-MM-DD (the feed's UNSHIFTED start).")
@click.option("--days", default=5, show_default=True, type=int,
              help="Base slice length in days.")
@click.option("--shift-days", "shift_days", required=True, type=int,
              help="The feed's whole-day shift K; labels are written in shifted time.")
@click.option("--source", default=DEFAULT_SOURCE, show_default=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Labeled source transactions CSV (streamed).")
@click.option("--combos", default="Penny_All,Accel_CMP", show_default=True,
              help="CSV of combo keys to emit (must be registered).")
@click.option("--warmup-hours", "warmup_hours", default=23, show_default=True, type=int,
              help="Leading hours to exclude (never-comparable warm-up windows).")
@click.option("--drop-tail-hours", "drop_tail_hours", default=1, show_default=True, type=int,
              help="Trailing hours to exclude (windows that never close in-feed).")
@click.option("--out", required=True, type=click.Path(dir_okay=False),
              help="Output label CSV (combo_key,window_end,true_label).")
def emit(start_date, days, shift_days, source, combos, warmup_hours,
         drop_tail_hours, out):
    """Emit hourly ground-truth labels for one shifted feed, atomically."""
    try:
        start = dt.date.fromisoformat(start_date)
    except ValueError:
        raise click.BadParameter(f"--start-date must be YYYY-MM-DD, got {start_date!r}")
    combo_list = [c.strip() for c in combos.split(",") if c.strip()]
    unknown = [c for c in combo_list if c not in COMBO_RULES]
    if unknown:
        raise click.ClickException(
            f"unregistered combo(s) {unknown}; known: {sorted(COMBO_RULES)}")

    labels = build_hourly_labels(source, start, days, combo_list)

    first = warmup_hours
    last = days * 24 - 1 - drop_tail_hours
    if not (0 <= first <= last):
        raise click.ClickException(
            f"empty hour range [{first}, {last}] for days={days}")

    shifted_start = dt.datetime.combine(start + dt.timedelta(days=shift_days),
                                        dt.time())
    out_dir = os.path.dirname(os.path.abspath(out)) or "."
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".labels_", suffix=".tmp", dir=out_dir)
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["combo_key", "window_end", "true_label"])
            for combo in combo_list:
                lab = labels[combo]
                for h in range(first, last + 1):
                    we = shifted_start + dt.timedelta(hours=h)
                    writer.writerow([combo, we.strftime("%Y-%m-%dT%H:00:00"), lab[h]])
        os.chmod(tmp, 0o644)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

    for combo in combo_list:
        window = labels[combo][first:last + 1]
        segs = segments(window)
        click.echo(f"emit: {combo} rows={len(window)} anomalous_hours={sum(window)} "
                   f"segments={len(segs)} {segs} -> {out}")


if __name__ == "__main__":
    cli()
