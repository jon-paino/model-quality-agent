# ModelOp -- ONNX scoring Open Processor

`ModelOp` is a WAEvent pass-through Open Processor that scores each event with an
ONNX fare-prediction model through ORT-Java. It assembles a 15-feature vector
(5 event features read positionally from `data[]`, 10 dynamic features read from
`userdata` where the upstream `FeatureOp` merged them), scores it, and writes the
prediction to both `userdata["prediction"]` and an appended `data[]` column. A
missing feature is scored as `Float.NaN` (the ONNX TreeEnsembleRegressor treats
NaN as missing, matching XGBoost).

Source: [src/main/java/com/striim/demo/ModelOp.java](src/main/java/com/striim/demo/ModelOp.java).
Build/stage: [build.sh](build.sh) (`mvn clean package` -> shaded `ModelOp.scm` ->
`$STRIIM_HOME/UploadedFiles/`). Wired into `qualitydemo.FareInference` as `ModelProc`
in [../pipeline/inference_pipeline.tql](../pipeline/inference_pipeline.tql).

## Production-safe model swap

We are committed to **Scenario A**: training and ONNX conversion run in a separate
Python program OUTSIDE the JVM; a finished `model.onnx` lands on the watched volume
(`UploadedFiles/model.onnx`) and ModelOp's only job is to deploy that artifact
safely at runtime. ModelOp does no framework-to-ONNX conversion.

The swap is driven by `maybeSwapModel()`, called once per Striim batch. The active
model lives behind an `AtomicReference<ModelHandle>`; a bounded history (last
`HISTORY_CAP = 3`) of recent good models is retained for rollback.

### State machine (per batch)

```
                    model.onnx mtime advanced?   control file mtime advanced?
                                 |                          |
            no change ->  return (2 stat() calls, the common case)
                                 |                          |
                                 |                  rollback command? --> revert to a
                                 |                                        prior good model
                                 v
                       SHA-256 the model file
                                 |
              hash == live hash? --yes--> skip reload (mtime noise, identical bytes)
                                 | no
                                 v
                    build candidate OrtSession
                                 |
              signature matches the live contract?  (Mechanic 1)
                 |  no                              |  yes
                 v                                   v
        REJECT: close candidate,          flip AtomicReference to candidate,  (Mechanic 2)
        keep live model,                  retain in bounded history;
        rejection counter++,              old sessions close only on eviction
        log the failing field            (always AFTER the flip)
                                                     |
                                                     v
                                   log identity (path, sha256, version, ms)  (Mechanic 3)
```

### The three mechanics

1. **Signature validation (pre-swap gate).** A candidate's input/output contract --
   names, tensor rank, dtype, and fixed dims (a `-1` dim is dynamic, so batch size
   may vary) -- must match the live model's. On any mismatch the candidate is
   REJECTED, closed, and the pipeline keeps scoring on the last good model; the
   exact failing field is logged. This is a structural gate, kept separate from the
   per-event `Float.NaN` leniency. `expectedContract()` is a seam where a future
   training-pipeline sidecar manifest can override the live-derived contract to
   allow an intentional schema change; v1 uses the live session.

2. **Atomic swap.** The reference is flipped to the validated candidate FIRST; old
   sessions are closed only when they age out of the bounded history, never before
   the flip. Each event captures its `ModelHandle` once and finishes scoring on the
   session it started with. Swaps happen at batch boundaries on the single `run()`
   thread, so flip-then-close is sufficient and no timed drain is needed (one would
   only be required if scoring moved onto a worker pool).

3. **Content hashing, identity, rollback.** A SHA-256 of the model file is the
   identity; an identical re-copy (mtime bumped, bytes unchanged) is skipped, so a
   touch never triggers a needless rebuild. The version is `sha256:<first 12 hex>`.
   The live identity (`model_path`, `model_sha256`, `model_version`,
   `model_loaded_at`) is written onto each event's `userdata`. A control file
   (`ControlFile`, default `UploadedFiles/model.control`) holding `rollback` reverts
   to the most recent prior good model, or `rollback:<sha12>` to a specific one in
   history; the target session is already open, so the revert is immediate.

### Relevant properties

| Property              | Default                       | Purpose                                    |
|-----------------------|-------------------------------|--------------------------------------------|
| `ModelFile`           | `UploadedFiles/model.onnx`    | the watched ONNX artifact                  |
| `ControlFile`         | `UploadedFiles/model.control` | rollback control file (`rollback[:<sha12>]`)|
| `EventFeatureIndices` | `1,2,3,4,5`                   | `data[]` positions of the 5 event features |
| `EnableLogging`       | `false`                       | verbose `ModelOp:` logs (swap lines always print) |

## Testing the swap

The harness lives in [test/](test). It needs the running `qualitydemo.FareInference`
app (with the rebuilt `ModelOp.scm` loaded), the live `model.onnx` staged, and Feast
serving so events actually score.

```bash
# 1. Rebuild + stage the OP, then in the Striim console:
#      UNLOAD OPEN PROCESSOR 'UploadedFiles/ModelOp.scm';
#      LOAD   OPEN PROCESSOR 'UploadedFiles/ModelOp.scm';
#    and restart the app. (Do not overwrite a loaded .scm in place.)
cd striim/model-op && ./build.sh

# 2. Emit the four ONNX test artifacts (valid + 3 bad-signature variants):
../../.venv/bin/python test/make_swap_artifacts.py

# 3. Drive all three mechanics against the live app and print PASS/FAIL:
test/run_swap_acceptance.sh
```

`make_swap_artifacts.py` writes to `test/artifacts/`:

| Artifact             | Contract                              | Expected ModelOp behavior          |
|----------------------|---------------------------------------|------------------------------------|
| `swap_good_v2.onnx`  | `input` f32 `[-1,15]` / `variable` f32 `[-1,1]` | SWAPS (and is the rollback target) |
| `swap_bad_name.onnx` | input renamed `features`              | REJECTED (input names mismatch)    |
| `swap_bad_shape.onnx`| input `[-1,12]`                       | REJECTED (input shape mismatch)    |
| `swap_bad_dtype.onnx`| input float64                         | REJECTED (input dtype mismatch)    |

All four are valid loadable ONNX sessions, so a rejection comes from the signature
gate, not a build failure. `swap_good_v2` predicts in the thousands (W = 2*index,
b = 50), far outside the real fare range, so a successful swap is unmistakable in the
scored output.

`run_swap_acceptance.sh` greps the ModelOp swap log lines (`REJECTED` /
`SWAPPED to` / `skip reload` / `ROLLED BACK`), backs up the live model first, and
restores it (and removes the control file) on exit.

> Log source: those lines go to the Striim server's **stdout**, NOT
> `$STRIIM_HOME/logs/striim.server.log` (that file carries none of them). The
> script auto-detects the server JVM's stdout sink (fd 1 -- works whether the
> server was launched with stdout redirected to a file, `nohup.out`, or a
> background task) and greps it for PASS/FAIL. If the server's stdout is a live
> **terminal** (no file to grep), the script falls back to **WATCH mode**: it
> still drives every step and tells you exactly what to look for, pausing after
> each so you confirm the line in the server terminal by eye. To verify
> automatically instead, relaunch the server teed to a file
> (`/opt/Striim/bin/server.sh 2>&1 | tee /opt/Striim/server.out`) and pass
> `LOG=/opt/Striim/server.out`.

Tunables: `STRIIM_HOME`, `LOG`, `PY`, `ROWS`, `WAIT`.

> Note: the harness mutates `$STRIIM_HOME/UploadedFiles/model.onnx` on the running
> pipeline. Run it against a demo/dev cluster, not production. It restores the
> original model on exit, including on Ctrl-C.
