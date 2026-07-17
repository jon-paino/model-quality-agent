#!/usr/bin/env python3
"""Pin / compare the F3 expected eval metrics (deterministic-decision proof).

Scoring is deterministic (same model bytes + same feed = same decisions), so
the eval metrics for a fixed base slice are pinned ONCE at dev time and every
later run must reproduce them EXACTLY (float equality; JSON round-trips are
repr-lossless). `extract` and `compare` share the same extraction, so the pin
and the assertion can never drift apart.

Only SHIFT-INDEPENDENT fields are pinned: raw / point_adjusted metric blocks,
join counts, coverage, gap_count, the trailing-window anomaly rate, and the
published baseline identity. Shift-dependent fields (label_span,
latest_window_end, n_scored_total, computed_utc, inputs) are excluded, which
is what lets the recovery phase assert exact equality with the baseline pins.

Subcommands:
  extract  --eval eval_metrics.json --out f3_expected_eval.json  (pin time)
  compare  --eval eval_metrics.json --expected f3_expected_eval.json
           [--combos Penny_All,Accel_CMP]                        (assert time)
"""
import json
import sys

import click


def extract_pins(eval_doc):
    pins = {}
    for combo, block in sorted(eval_doc.get("combos", {}).items()):
        scoring = block.get("scoring") or {}
        ev = block.get("eval") or {}
        baseline = block.get("baseline") or {}
        pins[combo] = {
            "scoring": {
                "anomaly_rate": scoring.get("anomaly_rate"),
                "n_windows": scoring.get("n_windows"),
            },
            "eval": {
                "n_labels": ev.get("n_labels"),
                "n_joined": ev.get("n_joined"),
                "coverage": ev.get("coverage"),
                "gap_count": ev.get("gap_count"),
                "raw": ev.get("raw"),
                "point_adjusted": ev.get("point_adjusted"),
            },
            "baseline": {
                "model_version": baseline.get("model_version"),
                "pa_precision": baseline.get("pa_precision"),
                "pa_recall": baseline.get("pa_recall"),
                "pa_f1": baseline.get("pa_f1"),
            },
        }
    return pins


def diff(path, expected, actual, out):
    if isinstance(expected, dict) and isinstance(actual, dict):
        for k in sorted(set(expected) | set(actual)):
            diff(f"{path}.{k}", expected.get(k), actual.get(k), out)
        return
    if expected != actual:
        out.append(f"{path}: expected {expected!r}, got {actual!r}")


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option("--eval", "eval_path", required=True,
              type=click.Path(exists=True, dir_okay=False))
@click.option("--out", required=True, type=click.Path(dir_okay=False))
def extract(eval_path, out):
    """Write the pinned expectation file from a produced eval_metrics.json."""
    with open(eval_path) as f:
        pins = extract_pins(json.load(f))
    with open(out, "w") as f:
        json.dump(pins, f, indent=2, sort_keys=True)
        f.write("\n")
    click.echo(f"PINNED {len(pins)} combo(s) -> {out}")


@cli.command()
@click.option("--eval", "eval_path", required=True,
              type=click.Path(exists=True, dir_okay=False))
@click.option("--expected", required=True,
              type=click.Path(exists=True, dir_okay=False))
@click.option("--combos", default="", show_default=True,
              help="CSV restriction (default: every pinned combo).")
def compare(eval_path, expected, combos):
    """Exact-equality comparison of the eval file against the pins."""
    with open(eval_path) as f:
        actual = extract_pins(json.load(f))
    with open(expected) as f:
        pins = json.load(f)
    restrict = {c.strip() for c in combos.split(",") if c.strip()}
    fails = []
    for combo, pin in sorted(pins.items()):
        if restrict and combo not in restrict:
            continue
        if combo not in actual:
            fails.append(f"{combo}: absent from {eval_path}")
            continue
        diff(combo, pin, actual[combo], fails)
    if fails:
        click.echo(f"PIN-COMPARE verdict=FAIL ({len(fails)} mismatch(es)):")
        for f_ in fails:
            click.echo(f"  - {f_}")
        sys.exit(1)
    checked = sorted(restrict) if restrict else sorted(pins)
    click.echo(f"PIN-COMPARE combos={','.join(checked)} verdict=PASS (exact)")


if __name__ == "__main__":
    cli()
