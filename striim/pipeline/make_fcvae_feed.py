#!/usr/bin/env python3
"""Build time-shifted identical feeds for the FCVAE swap-proof protocol.

The FCVAE pipeline is STATEFUL (event-time 1h jumping window feeding a
KEEP 24 ROWS sliding window partitioned by combo_key), so swap proofs cannot
re-feed the same file: FileReader would skip it, and even with a fresh name the
window state would differ. Instead each proof phase re-feeds the SAME base
slice with all timestamps shifted forward by a whole number of days.

Whole-day shifts preserve per-hour event counts exactly, so every phase
presents bit-identical 24-hour window vectors to the scorer OP. Downstream TQL
reads only the first 19 chars of the timestamp (SLEFT(..., 19)), so the shift
is PURE STRING date arithmetic: the first 10 chars (the date) are replaced
with date+K days and every other byte of the line is left untouched. K=0
therefore reproduces the base file byte-for-byte.

Subcommands:
  prepare  slice N days out of the (496 MB) source CSV, streaming. Default
           start is the first row's date (the all-normal TRAIN range); pass
           --start-date to cut a labeled range instead (F3 uses 2025-02-25,
           source day 51, the start of the anomaly-bearing TEST range).
  emit     rewrite a base slice with dates shifted forward by K whole days.
"""
import datetime as dt
import os

import click

DEFAULT_SOURCE = ("/Users/jonpaino/Documents/Striim/fcvae-anomaly-detection/"
                  "data/synthetic_transactions.csv")


def _ensure_parent(path):
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option("--source", default=DEFAULT_SOURCE, show_default=True,
              type=click.Path(exists=True, dir_okay=False),
              help="Source transactions CSV (streamed, never loaded whole).")
@click.option("--days", default=5, show_default=True, type=int,
              help="Number of days to keep, starting at the slice start.")
@click.option("--start-date", "start_date", default=None,
              help="Slice start date, YYYY-MM-DD (default: the first data row's date).")
@click.option("--out", required=True, type=click.Path(dir_okay=False),
              help="Output base slice CSV.")
def prepare(source, days, out, start_date):
    """Slice N days of the source into a base feed, streaming."""
    _ensure_parent(out)
    kept = 0
    scanned = 0
    start_b = None
    cutoff_b = None
    if start_date is not None:
        try:
            start = dt.date.fromisoformat(start_date)
        except ValueError:
            raise click.BadParameter(f"--start-date must be YYYY-MM-DD, got {start_date!r}")
        start_b = start.isoformat().encode("ascii")
        cutoff_b = (start + dt.timedelta(days=days)).isoformat().encode("ascii")
    with open(source, "rb") as fin, open(out, "wb") as fout:
        header = fin.readline()
        fout.write(header)
        for line in fin:
            scanned += 1
            if len(line) < 19:
                continue
            if cutoff_b is None:
                start = dt.date.fromisoformat(line[:10].decode("ascii"))
                start_b = line[:10]
                cutoff_b = (start + dt.timedelta(days=days)).isoformat().encode("ascii")
            # ISO dates compare correctly as bytes; keep [start, start + days).
            if start_b <= line[:10] < cutoff_b:
                fout.write(line)
                kept += 1
            elif line[:10] >= cutoff_b:
                # The source is written in event-time order, so nothing past the
                # cutoff can match; stop instead of scanning the remaining file.
                break
    click.echo(f"prepare: kept {kept} of {scanned} data rows "
               f"({days} days from {start_b.decode('ascii') if start_b else '?'}) -> {out}")


@cli.command()
@click.option("--base", required=True, type=click.Path(exists=True, dir_okay=False),
              help="Base slice CSV produced by `prepare`.")
@click.option("--shift-days", "shift_days", required=True, type=int,
              help="Whole days to add to every row's date (K=0 copies byte-identically).")
@click.option("--out", required=True, type=click.Path(dir_okay=False),
              help="Output shifted feed CSV.")
def emit(base, shift_days, out):
    """Rewrite each data row's date (chars 0..9) to date+K days, byte-identical otherwise."""
    _ensure_parent(out)
    delta = dt.timedelta(days=shift_days)
    cache = {}
    rows = 0
    with open(base, "rb") as fin, open(out, "wb") as fout:
        fout.write(fin.readline())  # header, verbatim
        for line in fin:
            if len(line) < 19:
                fout.write(line)
                continue
            key = line[:10]
            shifted = cache.get(key)
            if shifted is None:
                shifted = ((dt.date.fromisoformat(key.decode("ascii")) + delta)
                           .isoformat().encode("ascii"))
                cache[key] = shifted
            fout.write(shifted + line[10:])
            rows += 1
    click.echo(f"emit: wrote {rows} rows shifted by {shift_days} day(s) -> {out}")


if __name__ == "__main__":
    cli()
