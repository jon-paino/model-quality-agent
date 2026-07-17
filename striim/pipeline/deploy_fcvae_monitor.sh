#!/usr/bin/env bash
# deploy_fcvae_monitor.sh -- deploy the standalone FCVAE quality monitor
# (fcvaemon.FcvaeMonitor) from the striim/quality-agent/fcvae_monitor.tql
# template, rendering its @TOKENS@ from env-overridable defaults.
#
# Modes:
#   default          app-only redeploy: tear down fcvaemon (app + namespace,
#                    nothing else lives there), render, deploy, poll RUNNING.
#                    Requires ModelQualityAgent.scm to already be LOADED with
#                    the F3 signal families (run --reload-module once first).
#   --reload-module  the SHARED-SCM cycle (the one place F3 touches taxi
#                    infrastructure): stop/undeploy/drop every app using
#                    ModelQualityAgent.scm (qualitymon.QualityMonitorApp and
#                    qualitydemo.FareInference), UNLOAD, rebuild + stage +
#                    LOAD the scm, restore the taxi apps to RUNNING, then do
#                    the app-only fcvaemon deploy. fcvaedemo.FcvaeInference
#                    does not use this scm and is untouched throughout.
#
# --reload-module ordering rationale (see CLAUDE.md module reload cycle):
#   - compile gate runs BEFORE any teardown (a broken build must not leave the
#     cluster torn down), and build.sh (which overwrites the staged scm) runs
#     only AFTER the UNLOAD.
#   - both taxi pipeline TQLs contain CREATE NAMESPACE qualitydemo; (which
#     FAILS and ABORTS console.sh -f when the namespace survives, and it MUST
#     survive: the qualitydemo.secrets vault lives there) and neither ends in
#     quit;. So FareInference is restored from a GENERATED temp copy with the
#     namespace line stripped and quit; appended.
#   - both taxi TQL variants name their source TripSource, so the live variant
#     is detected via DESCRIBE SOURCE (adapter FileReader vs MysqlReader)
#     BEFORE teardown.
#   - qualitymon's own TQL recreates its namespace and ends in quit;, so it
#     redeploys as-is after its namespace is dropped.
#
# Environment:
#   STRIIM_ADMIN_PW  (REQUIRED) admin password for REST auth and console.sh
#   STRIIM_HOME      default /opt/Striim
#   STRIIM_BASE_URL  default http://localhost:9080
#   STRIIM_CLUSTER   default JonPaino
#   Template knobs (defaults in one block below): FCVAE_MON_ENABLED_SIGNALS,
#   FCVAE_MON_TICK_SEC, FCVAE_MON_EVAL_FILE, FCVAE_MON_EVAL_MAX_AGE_SEC,
#   FCVAE_MON_EVAL_COMBOS, FCVAE_MON_METRIC_WARN_PCT, FCVAE_MON_METRIC_FAIL_PCT,
#   FCVAE_MON_RATE_WARN_PCT, FCVAE_MON_RATE_FAIL_PCT, FCVAE_MON_SRC_WARN_SEC,
#   FCVAE_MON_SRC_FAIL_SEC, FCVAE_MON_TGT_WARN_SEC, FCVAE_MON_TGT_FAIL_SEC
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
BASE_URL="${STRIIM_BASE_URL:-http://localhost:9080}"
CLUSTER="${STRIIM_CLUSTER:-JonPaino}"
QA_DIR="${SCRIPT_DIR}/../quality-agent"
TEMPLATE="${QA_DIR}/fcvae_monitor.tql"
QUALITYMON_TQL="${QA_DIR}/quality_monitor.tql"
TAXI_FILE_TQL="${SCRIPT_DIR}/inference_pipeline.tql"
TAXI_MYSQL_TQL="${SCRIPT_DIR}/inference_pipeline_mysql.tql"
SCM_AGENT="UploadedFiles/ModelQualityAgent.scm"
MON_APP="fcvaemon.FcvaeMonitor"

# ---- template knobs (env-overridable) ----
ENABLED_SIGNALS="${FCVAE_MON_ENABLED_SIGNALS:-app_status,source_freshness,target_write_age,lag_end2end,backpressure,discarded_events,node_memory,node_cpu,model_precision,model_recall,model_f1,anomaly_rate}"
TICK_SEC="${FCVAE_MON_TICK_SEC:-30}"
EVAL_FILE="${FCVAE_MON_EVAL_FILE:-/opt/Striim/UploadedFiles/fcvae_eval/eval_metrics.json}"
EVAL_MAX_AGE_SEC="${FCVAE_MON_EVAL_MAX_AGE_SEC:-900}"
EVAL_COMBOS="${FCVAE_MON_EVAL_COMBOS:-Penny_All,Accel_CMP}"
METRIC_WARN_PCT="${FCVAE_MON_METRIC_WARN_PCT:-80}"
METRIC_FAIL_PCT="${FCVAE_MON_METRIC_FAIL_PCT:-50}"
RATE_WARN_PCT="${FCVAE_MON_RATE_WARN_PCT:-40}"
RATE_FAIL_PCT="${FCVAE_MON_RATE_FAIL_PCT:-70}"
SRC_WARN_SEC="${FCVAE_MON_SRC_WARN_SEC:-900}"
SRC_FAIL_SEC="${FCVAE_MON_SRC_FAIL_SEC:-3600}"
TGT_WARN_SEC="${FCVAE_MON_TGT_WARN_SEC:-900}"
TGT_FAIL_SEC="${FCVAE_MON_TGT_FAIL_SEC:-3600}"

RELOAD_MODULE=0
if [ "${1:-}" = "--reload-module" ]; then
  RELOAD_MODULE=1
fi

if [ -z "${STRIIM_ADMIN_PW:-}" ]; then
  echo "ERROR: STRIIM_ADMIN_PW must be set" >&2
  exit 2
fi
if [ ! -f "${TEMPLATE}" ]; then
  echo "ERROR: template not found: ${TEMPLATE}" >&2
  exit 2
fi

echo "== authenticate against ${BASE_URL} =="
TOK="$(curl -s -X POST "${BASE_URL}/security/authenticate" \
        --data-urlencode 'username=admin' \
        --data-urlencode "password=${STRIIM_ADMIN_PW}" \
      | python3 -c 'import sys, json; print(json.load(sys.stdin)["token"])' 2>/dev/null)" || TOK=""
if [ -z "${TOK}" ]; then
  echo "ERROR: REST authentication failed" >&2
  exit 3
fi
echo "   token acquired"

tungsten() {
  curl -s -X POST "${BASE_URL}/api/v2/tungsten" \
       -H "authorization: STRIIM-TOKEN ${TOK}" \
       -H 'content-type: text/plain' \
       --data "$1"
}

tolerant() {
  echo ">> $1"
  local resp
  resp="$(tungsten "$1")"
  echo "   ${resp}"
}

require() {
  echo ">> $1"
  local resp
  resp="$(tungsten "$1")"
  echo "   ${resp}"
  printf '%s' "${resp}" | python3 -c '
import sys, json
try:
    envs = json.load(sys.stdin)
except Exception:
    sys.exit(1)
if not isinstance(envs, list) or not envs:
    sys.exit(1)
sys.exit(0 if all(e.get("executionStatus") == "Success" for e in envs) else 1)
' || { echo "ERROR: required command failed: $1" >&2; exit 4; }
}

# statusChange of one app via mon over REST ("" when absent/undeployed).
app_status() {
  tungsten "mon $1;" | python3 -c '
import sys, json

def find(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find(v, key)
            if r is not None:
                return r
    if isinstance(obj, list):
        for v in obj:
            r = find(v, key)
            if r is not None:
                return r
    return None

try:
    print(find(json.load(sys.stdin), "statusChange") or "")
except Exception:
    print("")
'
}

# Poll one app to RUNNING within a deadline; exit 7 on timeout.
poll_running() {
  local app="$1" timeout="${2:-60}" status=""
  local deadline=$(( $(date +%s) + timeout ))
  echo "== poll ${app} until RUNNING (timeout ${timeout}s) =="
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    status="$(app_status "${app}")"
    echo "   statusChange: ${status:-<none>}"
    case "${status}" in
      *RUNNING*) echo "OK: ${app} is RUNNING"; return 0 ;;
    esac
    sleep 5
  done
  echo "ERROR: ${app} did not reach RUNNING within ${timeout}s (last: ${status:-<none>})" >&2
  exit 7
}

deploy_console() {
  local tql="$1"
  echo "== deploy via console.sh -f ${tql} =="
  if ! "${STRIIM_HOME}/bin/console.sh" -c "${CLUSTER}" -u admin -p "${STRIIM_ADMIN_PW}" -f "${tql}"; then
    echo "WARN: console.sh exited nonzero; continuing to the status poll" >&2
  fi
}

# =====================================================================
# --reload-module: the shared-scm cycle (touches taxi apps, restores them)
# =====================================================================
if [ "${RELOAD_MODULE}" = "1" ]; then
  echo
  echo "== preflight: record taxi app states and detect the live FareInference TQL variant =="
  QUALITYMON_STATUS="$(app_status qualitymon.QualityMonitorApp)"
  FARE_STATUS="$(app_status qualitydemo.FareInference)"
  echo "   qualitymon.QualityMonitorApp: ${QUALITYMON_STATUS:-<absent>}"
  echo "   qualitydemo.FareInference:    ${FARE_STATUS:-<absent>}"

  RESTORE_FARE_TQL=""
  if [ -n "${FARE_STATUS}" ]; then
    DESCRIBE="$(tungsten 'DESCRIBE SOURCE qualitydemo.TripSource;')"
    if printf '%s' "${DESCRIBE}" | grep -qi 'MysqlReader'; then
      RESTORE_FARE_TQL="${TAXI_MYSQL_TQL}"
    elif printf '%s' "${DESCRIBE}" | grep -qi 'FileReader'; then
      RESTORE_FARE_TQL="${TAXI_FILE_TQL}"
    else
      echo "ERROR: cannot determine FareInference variant from DESCRIBE SOURCE" >&2
      echo "       (no FileReader/MysqlReader in: ${DESCRIBE})" >&2
      echo "       Refusing to tear down what cannot be restored." >&2
      exit 8
    fi
    echo "   FareInference restore TQL: ${RESTORE_FARE_TQL}"
  fi

  echo
  echo "== compile gate BEFORE any teardown (build.sh runs only after UNLOAD) =="
  if ! mvn -q -f "${QA_DIR}/pom.xml" clean package; then
    echo "ERROR: ModelQualityAgent build failed; nothing was torn down" >&2
    exit 5
  fi
  echo "   compile OK"

  echo
  echo "== teardown every app using ${SCM_AGENT} (tolerant) =="
  tolerant "STOP APPLICATION qualitymon.QualityMonitorApp;"
  tolerant "UNDEPLOY APPLICATION qualitymon.QualityMonitorApp;"
  tolerant "DROP APPLICATION qualitymon.QualityMonitorApp CASCADE;"
  # qualitymon's namespace is dropped so its TQL (CREATE NAMESPACE qualitymon;)
  # redeploys as-is. NEVER drop qualitydemo: the secrets vault lives there.
  tolerant "use admin; DROP NAMESPACE qualitymon CASCADE;"
  tolerant "STOP APPLICATION qualitydemo.FareInference;"
  tolerant "UNDEPLOY APPLICATION qualitydemo.FareInference;"
  tolerant "DROP APPLICATION qualitydemo.FareInference CASCADE;"
  tolerant "STOP APPLICATION ${MON_APP};"
  tolerant "UNDEPLOY APPLICATION ${MON_APP};"
  tolerant "DROP APPLICATION ${MON_APP} CASCADE;"
  tolerant "use admin; DROP NAMESPACE fcvaemon CASCADE;"
  # The UNLOAD must be REQUIRED, not tolerated: a silently failed UNLOAD makes
  # the later LOAD an idempotent no-op that leaves the OLD template live (found
  # the hard way; the platform's module cache is $STRIIM_HOME/.striim/
  # OpenProcessor/<name>.scm, removed on UNLOAD and recopied on LOAD).
  # Exception: after a previous ABORTED run the module may already be unloaded,
  # so accept exactly the not-loaded failure and require success otherwise.
  UNLOAD_RESP="$(tungsten "UNLOAD OPEN PROCESSOR '${SCM_AGENT}';")"
  echo ">> UNLOAD OPEN PROCESSOR '${SCM_AGENT}';"
  echo "   ${UNLOAD_RESP}"
  if ! printf '%s' "${UNLOAD_RESP}" | python3 -c '
import sys, json
try:
    envs = json.load(sys.stdin)
except Exception:
    sys.exit(1)
def not_loaded(msg):
    m = str(msg).lower()
    return "no open processor loaded" in m or "not loaded" in m
ok = all(e.get("executionStatus") == "Success"
         or not_loaded(e.get("failureMessage", "")) for e in envs)
sys.exit(0 if ok else 1)
'; then
    echo "ERROR: UNLOAD failed while the module appears loaded; refusing to" >&2
    echo "       continue (a no-op LOAD would silently keep the old code)." >&2
    exit 4
  fi

  echo
  echo "== rebuild + stage the agent scm (quality-agent/build.sh) =="
  if ! STRIIM_HOME="${STRIIM_HOME}" "${QA_DIR}/build.sh"; then
    echo "ERROR: quality-agent/build.sh failed; module not staged" >&2
    exit 5
  fi
  # The stale-scm trap (CLAUDE.md): verify the staged jar really contains the
  # F3 classes before loading it. NO grep -q here: under pipefail its early
  # exit SIGPIPEs unzip and fails the pipeline even on a match.
  if ! unzip -l "${STRIIM_HOME}/${SCM_AGENT}" | grep 'EvalCombo' > /dev/null; then
    echo "ERROR: staged ${SCM_AGENT} does not contain EvalCombo (stale build?)" >&2
    exit 5
  fi
  echo "   staged scm contains the F3 classes"

  echo
  echo "== load the rebuilt module =="
  require "LOAD OPEN PROCESSOR '${SCM_AGENT}';"
  # Definitive staleness gate: LOAD copies the scm into the platform module
  # cache; if the cached bytes differ from the staged bytes, the LOAD was a
  # no-op against a still-loaded old template.
  MODULE_CACHE="${STRIIM_HOME}/.striim/OpenProcessor/$(basename "${SCM_AGENT}")"
  if [ -f "${MODULE_CACHE}" ]; then
    STAGED_SHA="$(shasum "${STRIIM_HOME}/${SCM_AGENT}" | cut -d' ' -f1)"
    CACHED_SHA="$(shasum "${MODULE_CACHE}" | cut -d' ' -f1)"
    if [ "${STAGED_SHA}" != "${CACHED_SHA}" ]; then
      echo "ERROR: module cache ${MODULE_CACHE} (${CACHED_SHA}) != staged scm" >&2
      echo "       (${STAGED_SHA}): the LOAD did not take the new bytes" >&2
      exit 4
    fi
    echo "   module cache matches the staged scm (${STAGED_SHA})"
  else
    echo "   note: no module cache entry at ${MODULE_CACHE} to verify"
  fi

  if [ -n "${RESTORE_FARE_TQL}" ]; then
    echo
    echo "== restore qualitydemo.FareInference from ${RESTORE_FARE_TQL} =="
    # Generated deploy copy: strip CREATE NAMESPACE qualitydemo; (the namespace
    # survives, and its failure would abort console.sh -f) and append quit;
    # (neither taxi TQL has one; without it console.sh drops to the prompt).
    FARE_TMP="$(mktemp -t fare_redeploy_XXXX).tql"
    grep -v '^CREATE NAMESPACE qualitydemo;' "${RESTORE_FARE_TQL}" > "${FARE_TMP}"
    printf '\nquit;\n' >> "${FARE_TMP}"
    deploy_console "${FARE_TMP}"
    rm -f "${FARE_TMP}"
    poll_running qualitydemo.FareInference 90
  else
    echo "   (FareInference was not deployed; skipping its restore)"
  fi

  if [ -n "${QUALITYMON_STATUS}" ]; then
    echo
    echo "== restore qualitymon.QualityMonitorApp from ${QUALITYMON_TQL} =="
    deploy_console "${QUALITYMON_TQL}"
    poll_running qualitymon.QualityMonitorApp 90
  else
    echo "   (QualityMonitorApp was not deployed; skipping its restore)"
  fi
fi

# =====================================================================
# app-only fcvaemon deploy (both modes end here)
# =====================================================================
echo
echo "== teardown fcvaemon (tolerant; nothing else lives in the namespace) =="
tolerant "STOP APPLICATION ${MON_APP};"
tolerant "UNDEPLOY APPLICATION ${MON_APP};"
tolerant "DROP APPLICATION ${MON_APP} CASCADE;"
tolerant "use admin; DROP NAMESPACE fcvaemon CASCADE;"

echo
echo "== render the template =="
MON_TMP="$(mktemp -t fcvae_monitor_XXXX).tql"
sed -e "s|@ENABLED_SIGNALS@|${ENABLED_SIGNALS}|g" \
    -e "s|@TICK_SEC@|${TICK_SEC}|g" \
    -e "s|@EVAL_FILE@|${EVAL_FILE}|g" \
    -e "s|@EVAL_MAX_AGE_SEC@|${EVAL_MAX_AGE_SEC}|g" \
    -e "s|@EVAL_COMBOS@|${EVAL_COMBOS}|g" \
    -e "s|@METRIC_WARN_PCT@|${METRIC_WARN_PCT}|g" \
    -e "s|@METRIC_FAIL_PCT@|${METRIC_FAIL_PCT}|g" \
    -e "s|@RATE_WARN_PCT@|${RATE_WARN_PCT}|g" \
    -e "s|@RATE_FAIL_PCT@|${RATE_FAIL_PCT}|g" \
    -e "s|@SRC_WARN_SEC@|${SRC_WARN_SEC}|g" \
    -e "s|@SRC_FAIL_SEC@|${SRC_FAIL_SEC}|g" \
    -e "s|@TGT_WARN_SEC@|${TGT_WARN_SEC}|g" \
    -e "s|@TGT_FAIL_SEC@|${TGT_FAIL_SEC}|g" \
    "${TEMPLATE}" > "${MON_TMP}"
# comment lines may legitimately mention @TOKENS@; only real statements count
if grep -v '^[[:space:]]*--' "${MON_TMP}" | grep '@[A-Z_]*@' > /dev/null; then
  echo "ERROR: unrendered tokens remain in ${MON_TMP}:" >&2
  grep -v '^[[:space:]]*--' "${MON_TMP}" | grep -n '@[A-Z_]*@' >&2
  exit 6
fi
echo "   rendered -> ${MON_TMP} (signals: ${ENABLED_SIGNALS})"

deploy_console "${MON_TMP}"
rm -f "${MON_TMP}"
poll_running "${MON_APP}" 90

echo
echo "== postflight: fcvaedemo.FcvaeInference untouched check =="
FCVAE_STATUS="$(app_status fcvaedemo.FcvaeInference)"
echo "   fcvaedemo.FcvaeInference: ${FCVAE_STATUS:-<absent>}"
echo "DONE"
