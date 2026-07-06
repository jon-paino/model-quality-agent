#!/usr/bin/env python3
"""Task 0 (throwaway): mon/REST reachability probe for the Quality Monitoring Agent.

Week 1 moves ModelQualityAgent off JMX and onto Striim's mon-command + REST API,
which is the only SaaS-safe transport. Before writing the Java transport, this
probe confirms against the TARGET cluster that:

  (b) per-component freshness has a usable field (`latestActivity`), what its
      datetime format is, and whether it advances -- or whether we fall back to a
      DML-count-delta freshness proxy;
  (c) which signals have a clean mon/REST source. Per the platform lead, lag and
      the DML/DDL counts come from `mon <source>;` / `mon <target>;`; discarded
      events and backpressure may have no clean field (expected UNKNOWN).

It is pure HTTP (no Striim build, no OP context needed). The separate OpCounter-
beans-on-SaaS check (Task 0b) does need OP context and is not covered here.

Auth follows Striim's official rest-api-samples (v2/python/auth_token.py):
  POST <base>/security/authenticate  (form-encoded username+password)
    -> JSON response, token in the field literally named "token"
  POST <base>/api/v2/tungsten  (header `authorization: STRIIM-TOKEN <token>`,
    content-type text/plain, body = the raw command e.g. "mon <app>;")
On Striim Cloud a console-issued API token (More > API > Copy) also works: pass it
with --token to skip username/password auth.

Stdlib only (urllib) to match the other pipeline harnesses and avoid the training
extra. The probe NEVER prints the password; prefer --password-env over --password.

Usage:
    # self-managed / local
    python striim/quality-agent/task0/mon_rest_probe.py \
        --base-url http://localhost:9080 \
        --user admin --password-env STRIIM_PW \
        --app qualitydemo.FareInference \
        --out /tmp/task0

    # SaaS (https, self-signed) with a console token
    python striim/quality-agent/task0/mon_rest_probe.py \
        --base-url https://<cluster>:9081 --insecure \
        --token "$STRIIM_API_TOKEN" \
        --app qualitydemo.FareInference \
        --source qualitydemo.TripSource --target qualitydemo.FileTarget \
        --out /tmp/task0
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Key-name substrings whose scalar values are worth surfacing from the raw JSON.
# Deliberately broad: this is diagnostic, so over-report rather than miss a field
# whose exact name differs from what the mapping table assumed.
INTEREST_SUBSTRINGS = (
    "activity", "lag", "lee",           # freshness + lag
    "ddl", "insert", "update", "delete", "dml", "operation",  # DML/DDL counts
    "status", "entitytype", "fullname", "name", "type",       # identity/status
    "memory", "cpu", "rate", "disk",    # node health
    "discard", "stream", "full", "backpressure",  # discarded / backpressure
    "time", "fresh",                    # timestamps / freshness variants
)


def authenticate(base: str, user: str, password: str, ctx: ssl.SSLContext | None,
                 timeout: float) -> str:
    """POST /security/authenticate, form-encoded; return the JSON `token` field."""
    url = base.rstrip("/") + "/security/authenticate"
    data = urllib.parse.urlencode({"username": user, "password": password}).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded", "charset": "utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        raise SystemExit(f"AUTH FAILED: HTTP {e.code} at {url} (wrong user/password?)\n{detail}")
    except urllib.error.URLError as e:
        raise SystemExit(f"AUTH FAILED: cannot reach {url}: {e.reason}\n"
                         f"(is --base-url right? for https self-signed add --insecure)")
    try:
        token = json.loads(body)["token"]
    except (json.JSONDecodeError, KeyError, TypeError):
        raise SystemExit(f"AUTH FAILED: response had no JSON `token` field:\n{body[:300]}")
    return token


def run_command(base: str, token: str, command: str, ctx: ssl.SSLContext | None,
                timeout: float) -> tuple[int, str]:
    """POST the raw command to /api/v2/tungsten with the STRIIM-TOKEN header."""
    url = base.rstrip("/") + "/api/v2/tungsten"
    req = urllib.request.Request(
        url, data=command.encode("utf-8"), method="POST",
        headers={"authorization": "STRIIM-TOKEN " + token, "content-type": "text/plain"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.getcode(), resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except urllib.error.URLError as e:
        return 0, f"<URLError: {e.reason}>"


def scan_scalars(obj, path: str = ""):
    """Yield (path, key, value) for every scalar whose key matches an interest
    substring, walking dicts and lists recursively."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            here = f"{path}.{k}" if path else str(k)
            if isinstance(v, (dict, list)):
                yield from scan_scalars(v, here)
            elif any(s in str(k).lower() for s in INTEREST_SUBSTRINGS):
                yield here, k, v
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from scan_scalars(v, f"{path}[{i}]")


def discover_components(mon_app_obj) -> list[tuple[str, str]]:
    """Best-effort: pull (name, entityType) pairs so we know which SOURCE/TARGET
    components to issue `mon <name>;` for. Defensive: unknown shape -> []."""
    found: list[tuple[str, str]] = []
    for path, key, value in scan_scalars(mon_app_obj):
        if str(key).lower() == "entitytype" and isinstance(value, str):
            # look for a sibling name at the same parent path
            parent = path.rsplit(".", 1)[0]
            name = None
            for p2, k2, v2 in scan_scalars(mon_app_obj):
                if p2.startswith(parent) and str(k2).lower() in ("fullname", "name"):
                    name = v2
                    break
            found.append((str(name), value))
    # de-dup preserving order
    seen, out = set(), []
    for n, t in found:
        if (n, t) not in seen:
            seen.add((n, t))
            out.append((n, t))
    return out


def dump(out_dir: Path, label: str, status: int, text: str) -> None:
    safe = label.replace(";", "").replace(" ", "_").replace("/", "_").replace(".", "_")
    (out_dir / f"{safe}.raw.json").write_text(text)
    print(f"\n=== {label!r}  (HTTP {status}, {len(text)} bytes) ===")
    parsed = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        print("  (response is not JSON; raw dumped to file)")
        print("  " + text[:200].replace("\n", " "))
        return
    hits = list(scan_scalars(parsed))
    if not hits:
        print("  (no fields of interest matched)")
    for path, key, value in hits[:80]:
        sval = str(value)
        if len(sval) > 80:
            sval = sval[:77] + "..."
        print(f"  {path} = {sval}")
    if len(hits) > 80:
        print(f"  ... {len(hits) - 80} more (see {label!r} raw file)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Task 0 mon/REST reachability probe")
    ap.add_argument("--base-url", required=True, help="e.g. http://localhost:9080 or https://<cluster>:9081")
    ap.add_argument("--user", help="admin username (omit if using --token)")
    ap.add_argument("--password", help="admin password (prefer --password-env)")
    ap.add_argument("--password-env", help="name of env var holding the password")
    ap.add_argument("--token", help="pre-issued API token (Striim Cloud console); skips auth")
    ap.add_argument("--app", required=True, help="target app fullName, e.g. qualitydemo.FareInference")
    ap.add_argument("--source", action="append", default=[], help="source component fullName (repeatable)")
    ap.add_argument("--target", action="append", default=[], help="target component fullName (repeatable)")
    ap.add_argument("--insecure", action="store_true", help="skip TLS cert verification (self-signed SaaS)")
    ap.add_argument("--timeout", type=float, default=10.0, help="per-request timeout seconds")
    ap.add_argument("--out", default="/tmp/task0", help="dir for raw JSON dumps")
    args = ap.parse_args()

    ctx = ssl._create_unverified_context() if args.insecure else None
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.token:
        token = args.token
        print("Using pre-issued API token (skipping username/password auth).")
    else:
        pw = args.password
        if args.password_env:
            pw = os.environ.get(args.password_env)
        if not args.user or pw is None:
            ap.error("provide --token, or --user with --password/--password-env")
        token = authenticate(args.base_url, args.user, pw, ctx, args.timeout)
        print(f"Auth OK: token acquired ({len(token)} chars).")

    # Cluster-wide + app: app status, node memory/cpu, component enumeration.
    commands = [("mon;", "mon_all"), (f"mon {args.app};", "mon_app")]
    st, app_text = run_command(args.base_url, token, f"mon {args.app};", ctx, args.timeout)
    st_all, all_text = run_command(args.base_url, token, "mon;", ctx, args.timeout)
    dump(out_dir, "mon;", st_all, all_text)
    dump(out_dir, f"mon {args.app};", st, app_text)

    # Discover SOURCE/TARGET components if not given explicitly.
    sources, targets = list(args.source), list(args.target)
    if not sources or not targets:
        try:
            for name, etype in discover_components(json.loads(app_text)):
                if etype.upper() == "SOURCE" and name not in sources:
                    sources.append(name)
                elif etype.upper() == "TARGET" and name not in targets:
                    targets.append(name)
        except json.JSONDecodeError:
            pass
    if sources or targets:
        print(f"\nComponents to probe -> sources={sources} targets={targets}")
    else:
        print("\nNo SOURCE/TARGET components discovered; pass --source/--target explicitly "
              "after reading mon_app raw output.")

    # Per-component detail: DML/DDL counts, lag, latestActivity (items b + c).
    for name in sources + targets:
        st_c, text_c = run_command(args.base_url, token, f"mon {name};", ctx, args.timeout)
        dump(out_dir, f"mon {name};", st_c, text_c)

    # Lag confirmation via report lee.
    for cmd in ("report lee;", "report lee+;"):
        st_l, text_l = run_command(args.base_url, token, cmd, ctx, args.timeout)
        dump(out_dir, cmd, st_l, text_l)

    print(f"\nRaw responses written to {out_dir}/.  Review for: freshness field + format "
          f"(item b), lag/DML/DDL fields and discarded/backpressure (item c).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
