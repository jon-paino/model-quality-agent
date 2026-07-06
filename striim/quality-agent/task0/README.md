# Task 0 (throwaway): mon/REST reachability probe

Confirms, against the **target** cluster, that the SaaS-safe mon/REST transport can supply the
Week-1 signals before we write the Java transport. Pure HTTP; no Striim build, no OP context.
Delete this dir once the signal mapping is finalized.

`mon_rest_probe.py` authenticates (per Striim's official `rest-api-samples/v2/python/auth_token.py`:
`POST /security/authenticate` form-encoded -> JSON `token`), then issues `mon;`, `mon <app>;`,
`mon <source>;`, `mon <target>;`, `report lee;` / `report lee+;` via `POST /api/v2/tungsten`
(`authorization: STRIIM-TOKEN <token>`). It dumps each raw JSON response and prints a scan of the
fields that matter for the two open questions:

- **(b) freshness:** does each SOURCE/TARGET carry `latestActivity`, in what datetime format, and
  does it advance between runs? If not, we fall back to a DML-count-delta freshness proxy.
- **(c) coverage:** lag and DML/DDL counts from `mon <source>;` / `mon <target>;` (expected present);
  discarded-events and backpressure (expected UNKNOWN, i.e. no clean field).

## Handoff needed to run it (pause point 1)

1. `--base-url` for the target cluster (e.g. `https://<cluster>:9081`; add `--insecure` for self-signed TLS).
2. Auth: either an admin `--user` + password (via `--password-env STRIIM_PW`, from the cluster Vault), or a
   Striim Cloud console API token via `--token` (More > API > Copy). A plaintext value is acceptable for this
   throwaway probe only.
3. `--app` = the target app fullName (e.g. `qualitydemo.FareInference`). Optionally `--source`/`--target`
   component fullNames; otherwise the probe discovers them from `mon <app>;`.
4. Whether I run it (needs network reachability to the cluster from here) or you run it and share the
   `--out` dump dir.

Example:

    python striim/quality-agent/task0/mon_rest_probe.py \
        --base-url https://<cluster>:9081 --insecure \
        --user admin --password-env STRIIM_PW \
        --app qualitydemo.FareInference --out /tmp/task0

The self-test (`py_compile` + a synthetic-payload field scan) already passed locally; only the live
cluster run remains, which is what this handoff unblocks.
