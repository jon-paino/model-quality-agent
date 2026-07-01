#!/usr/bin/env bash
# run_swap_acceptance.sh -- exercise ModelOp's three production-safe swap mechanics
# against the RUNNING qualitydemo.FareInference app.
#
# It orchestrates the test either way; how it VERIFIES depends on the log source:
#   - LOG mode: if the Striim server's stdout is a greppable file (auto-detected, or
#     passed as LOG=...), it greps each window and prints PASS/FAIL automatically.
#   - WATCH mode: if the server's stdout is a live terminal (no file to grep), it
#     prints exactly what to look for in that terminal and pauses after each step so
#     YOU confirm the SWAPPED / REJECTED / skip reload / ROLLED BACK lines by eye.
#
# Prereqs:
#   - FareInference deployed and STARTED with the rebuilt ModelOp.scm loaded.
#   - The live model staged at $STRIIM_HOME/UploadedFiles/model.onnx (the real fare
#     model; its contract input=input f32[-1,15], output=variable f32[-1,1] is what
#     the signature gate validates candidates against).
#   - Feast serving (so FeatureOp enriches and ModelOp actually scores the feed).
#   - ModelProc EnableLogging:true (the 'skip reload' line is gated by it; the
#     SWAPPED / REJECTED / ROLLED BACK lines always print).
#
# It backs up the live model, drives the sequence, then RESTORES the live model and
# removes the control file. Run from anywhere:
#
#   striim/model-op/test/run_swap_acceptance.sh
#
# Tunables (env): STRIIM_HOME, LOG, PY, ROWS, WAIT.
set -uo pipefail

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
UP="$STRIIM_HOME/UploadedFiles"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
PY="${PY:-$REPO/.venv/bin/python}"
ART="$HERE/artifacts"
ROWS="${ROWS:-200}"
WAIT="${WAIT:-20}"
MODEL="$UP/model.onnx"
CONTROL="$UP/model.control"
BACKUP="$ART/_live_model_backup.onnx"

# ModelOp's swap lines (SWAPPED / REJECTED / skip reload / ROLLED BACK) go to the
# Striim server's STDOUT, NOT $STRIIM_HOME/logs/striim.server.log. Resolve the sink:
#   1. explicit LOG=... override,
#   2. auto-detect fd 1 of the running Striim JVM if it is a regular file,
#   3. otherwise WATCH mode (stdout is a terminal; you verify the lines by eye).
detect_server_stdout(){
  local pid target
  pid="$(ps ax -o pid=,command= 2>/dev/null \
         | grep -i 'java' | grep -iE 'striim|webaction|Platform|Bootstrap' \
         | grep -v grep | awk '{print $1}' | head -1)"
  [ -z "$pid" ] && return 1
  target="$(lsof -p "$pid" 2>/dev/null \
            | awk '$4 ~ /^1[wu]?$/ && $5 == "REG" { print $9; exit }')"
  [ -n "$target" ] && [ -f "$target" ] && { printf '%s\n' "$target"; return 0; }
  return 1
}

WATCH=0
if [ -n "${LOG:-}" ]; then
  echo "using LOG override: $LOG"
  [ -f "$LOG" ] || { echo "LOG not found: $LOG"; exit 2; }
elif LOG="$(detect_server_stdout)"; then
  echo "auto-detected Striim server stdout: $LOG"
else
  WATCH=1; LOG=""
  cat <<'BANNER'

  ----------------------------------------------------------------------------
  WATCH MODE: the Striim server's stdout is a terminal, not a greppable file.
  This run will DRIVE every step and tell you what to look for, pausing after
  each so you can confirm the line in the terminal where the server is running.
  (To verify automatically instead, relaunch the server teed to a file:
     /opt/Striim/bin/server.sh 2>&1 | tee /opt/Striim/server.out
   then re-run as:  LOG=/opt/Striim/server.out  ...run_swap_acceptance.sh)
  ----------------------------------------------------------------------------
BANNER
fi

pass=0; fail=0; feed_n=0; winpos=0; WIN=""
ok(){ echo "  PASS: $1"; pass=$((pass + 1)); }
no(){ echo "  FAIL: $1"; fail=$((fail + 1)); }
logpos(){ wc -c < "$LOG" 2>/dev/null | tr -d ' ' || echo 0; }
since(){ tail -c "+$(( ${1} + 1 ))" "$LOG" 2>/dev/null; }

# Window helpers: in LOG mode capture the log offset before an action and the text
# after; in WATCH mode they are no-ops.
win_start(){ [ "$WATCH" = 0 ] && winpos="$(logpos)"; return 0; }
win_grab(){ [ "$WATCH" = 0 ] && WIN="$(since "$winpos")"; return 0; }

# Assertions. LOG mode greps the captured window and tallies; WATCH mode just
# announces the expectation (you confirm by eye at the pause).
want(){    # $1 human desc, $2 grep pattern
  if [ "$WATCH" = 1 ]; then echo "   EXPECT in terminal:   $1"; return; fi
  if echo "$WIN" | grep -qa "$2"; then ok "$1"; else no "$1 (pattern: '$2')"; fi
}
want_not(){ # $1 human desc, $2 grep pattern
  if [ "$WATCH" = 1 ]; then echo "   EXPECT (must NOT see): $1"; return; fi
  if echo "$WIN" | grep -qa "$2"; then no "$1"; else ok "$1"; fi
}
watch_pause(){
  [ "$WATCH" = 1 ] || return 0
  printf "   ... confirm the above in the server terminal, then press ENTER (Ctrl-C to abort): "
  read -r _ || true
}

feed(){
  # Re-feed so a batch runs (when ModelOp checks for a swap). positionByEOF:false
  # re-reads every pipeline_trips*.csv, so clear stale feeds and use a fresh suffix.
  feed_n=$((feed_n + 1))
  rm -f "$UP"/pipeline_trips*.csv
  "$PY" "$REPO/striim/pipeline/make_trip_feed.py" --rows "$ROWS" \
        --out "$UP/pipeline_trips_swap_${feed_n}.csv" >/dev/null
}

restore(){
  echo
  echo "== teardown: restore live model, remove control file =="
  [ -f "$BACKUP" ] && cp "$BACKUP" "$MODEL" && echo "  restored $MODEL from backup"
  rm -f "$CONTROL" "$UP"/pipeline_trips_swap_*.csv
}
trap restore EXIT

[ -f "$MODEL" ] || { echo "live model not found: $MODEL"; exit 2; }
[ -x "$PY" ]    || { echo "python not found: $PY (set PY=...)"; exit 2; }

echo
echo "== generate ONNX test artifacts =="
"$PY" "$HERE/make_swap_artifacts.py" --out-dir "$ART"
cp "$MODEL" "$BACKUP"
echo "  backed up live model -> $BACKUP"

# -- Mechanic 1: signature validation. Each bad candidate must be REJECTED with the
#    field that failed named, and NO swap must occur (scoring continues on v1). ------
reject_case(){ # $1 = artifact, $2 = expected failure substring, $3 = label
  echo
  echo "== Mechanic 1: reject $3 =="
  echo "   action: copy $1 onto model.onnx, feed $ROWS rows, wait ${WAIT}s"
  win_start
  cp "$ART/$1" "$MODEL"
  feed
  sleep "$WAIT"
  win_grab
  want     "$3 REJECTED ($2)"                 "REJECTED.*$2"
  want_not "$3 did NOT swap (kept live model)" "SWAPPED to"
  watch_pause
}
reject_case swap_bad_name.onnx  "names mismatch" "wrong input name"
reject_case swap_bad_shape.onnx "shape mismatch" "wrong input shape"
reject_case swap_bad_dtype.onnx "dtype mismatch" "wrong input dtype"

# -- Mechanic 2 + valid swap: the matching candidate must SWAP. ---------------------
echo
echo "== Mechanic 2: valid candidate swaps =="
echo "   action: copy swap_good_v2.onnx onto model.onnx, feed, wait ${WAIT}s"
echo "   (good_v2 predicts ~thousands, so predictions.json fares will spike too)"
win_start
cp "$ART/swap_good_v2.onnx" "$MODEL"
feed
sleep "$WAIT"
win_grab
want "valid candidate SWAPPED in (ModelOp: SWAPPED to sha256:...)" "SWAPPED to sha256:"
watch_pause

# -- Mechanic 3a: identical bytes re-copied -> skipped (no rebuild). ----------------
echo
echo "== Mechanic 3: identical re-copy is skipped (hash gate) =="
echo "   action: re-copy the SAME swap_good_v2.onnx (mtime bumps, bytes identical)"
win_start
cp "$ART/swap_good_v2.onnx" "$MODEL"
feed
sleep "$WAIT"
win_grab
want     "identical re-copy SKIPPED (ModelOp: ... skip reload)" "skip reload"
want_not "identical re-copy did NOT rebuild/swap"               "SWAPPED to"
watch_pause

# -- Mechanic 3b: rollback to the prior good model via the control file. ------------
echo
echo "== Mechanic 3: rollback via control file =="
echo "   action: write 'rollback' to model.control, feed, wait ${WAIT}s"
echo "   (reverts good_v2 -> the live model; predictions.json fares return to normal)"
win_start
echo "rollback" > "$CONTROL"
feed
sleep "$WAIT"
win_grab
want "ROLLED BACK to the prior model (ModelOp: ROLLED BACK from ... to ...)" "ROLLED BACK"
watch_pause

echo
if [ "$WATCH" = 1 ]; then
  echo "================ swap acceptance: WATCH mode complete ================"
  echo "Manual verification: confirm each EXPECT line above appeared in the server"
  echo "terminal. The live model has been restored. (predictions.json also shows the"
  echo "fare spike on the swap and the return to normal on rollback.)"
else
  echo "================ swap acceptance: $pass passed, $fail failed ================"
fi
[ "$WATCH" = 1 ] && exit 0
[ "$fail" -eq 0 ]
