#!/usr/bin/env bash
# deploy_fcvae.sh -- tear down any previous FCVAE apps, rebuild + stage + load the
# FCVAEOnnxScorer AND FCVAEParamsOp modules, deploy the F2 pipeline
# (fcvae_inference.tql, app fcvaedemo.FcvaeInference), and wait for the app to
# reach RUNNING.
#
# Transport (see CLAUDE.md "Tungsten Console + REST API"):
#   - REST tungsten endpoint for teardown / LOAD / status polling. Teardown
#     commands are posted ONE AT A TIME and TOLERATE failure (absent objects are
#     fine on a clean cluster); every response envelope is printed.
#   - console.sh -f for the TQL deploy. The file is passed as an ABSOLUTE path
#     and ends in quit; (both are console.sh -f requirements). The TQL contains
#     CREATE NAMESPACE fcvaedemo, and a failed CREATE NAMESPACE ABORTS a -f file
#     on 5.2.0.4, which is exactly why this script pre-drops the namespace.
#
# Environment:
#   STRIIM_ADMIN_PW  (REQUIRED) admin password for REST auth and console.sh
#   STRIIM_HOME      default /opt/Striim
#   STRIIM_BASE_URL  default http://localhost:9080
#   STRIIM_CLUSTER   default JonPaino (the WAClusterName in conf/startUp.properties,
#                    NOT the $USER-derived name the interactive prompt shows)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
BASE_URL="${STRIIM_BASE_URL:-http://localhost:9080}"
CLUSTER="${STRIIM_CLUSTER:-JonPaino}"
TQL_FILE="${SCRIPT_DIR}/fcvae_inference.tql"
BUILD_SCORER="${SCRIPT_DIR}/../fcvae-scorer/build.sh"
BUILD_PARAMS="${SCRIPT_DIR}/../fcvae-params-op/build.sh"
APP="fcvaedemo.FcvaeInference"
SCM_SCORER="UploadedFiles/FCVAEOnnxScorer.scm"
SCM_PARAMS="UploadedFiles/FCVAEParamsOp.scm"

if [ -z "${STRIIM_ADMIN_PW:-}" ]; then
  echo "ERROR: STRIIM_ADMIN_PW must be set (Striim admin password for REST + console.sh)" >&2
  exit 2
fi
if [ ! -f "${TQL_FILE}" ]; then
  echo "ERROR: TQL file not found: ${TQL_FILE}" >&2
  exit 2
fi

echo "== authenticate against ${BASE_URL} =="
TOK="$(curl -s -X POST "${BASE_URL}/security/authenticate" \
        --data-urlencode 'username=admin' \
        --data-urlencode "password=${STRIIM_ADMIN_PW}" \
      | python3 -c 'import sys, json; print(json.load(sys.stdin)["token"])' 2>/dev/null)" || TOK=""
if [ -z "${TOK}" ]; then
  echo "ERROR: REST authentication failed (check STRIIM_ADMIN_PW and STRIIM_BASE_URL)" >&2
  exit 3
fi
echo "   token acquired"

# Post one raw tungsten command; print nothing, emit the response body.
tungsten() {
  curl -s -X POST "${BASE_URL}/api/v2/tungsten" \
       -H "authorization: STRIIM-TOKEN ${TOK}" \
       -H 'content-type: text/plain' \
       --data "$1"
}

# Post one command, print its response envelope, TOLERATE failure (used for
# teardown of objects that may not exist).
tolerant() {
  echo ">> $1"
  local resp
  resp="$(tungsten "$1")"
  echo "   ${resp}"
}

# Post one command and REQUIRE executionStatus Success on every envelope.
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

echo
echo "== teardown (tolerant: absent objects are fine) =="
tolerant "STOP APPLICATION ${APP};"
tolerant "UNDEPLOY APPLICATION ${APP};"
tolerant "DROP APPLICATION ${APP} CASCADE;"
# stale F1 app name (pre-F2 rename) may still be deployed
tolerant "STOP APPLICATION fcvaedemo.PennyInference;"
tolerant "UNDEPLOY APPLICATION fcvaedemo.PennyInference;"
tolerant "DROP APPLICATION fcvaedemo.PennyInference CASCADE;"
# DROP NAMESPACE only works from ANOTHER namespace, so 'use admin;' rides in the
# SAME post (REST posts do not share console session state across calls).
tolerant "use admin; DROP NAMESPACE fcvaedemo CASCADE;"
# stale sibling-repo (fcvae-anomaly-detection) leftovers
tolerant "use admin; DROP NAMESPACE fcvae CASCADE;"
tolerant "use admin; DROP NAMESPACE fcvae_onnx CASCADE;"
tolerant "UNLOAD OPEN PROCESSOR '${SCM_SCORER}';"
tolerant "UNLOAD OPEN PROCESSOR '${SCM_PARAMS}';"

echo
echo "== rebuild + stage the scorer module (striim/fcvae-scorer/build.sh) =="
if ! STRIIM_HOME="${STRIIM_HOME}" "${BUILD_SCORER}"; then
  echo "ERROR: fcvae-scorer/build.sh failed; module not staged" >&2
  exit 5
fi

echo
echo "== rebuild + stage the params module (striim/fcvae-params-op/build.sh) =="
if ! STRIIM_HOME="${STRIIM_HOME}" "${BUILD_PARAMS}"; then
  echo "ERROR: fcvae-params-op/build.sh failed; module not staged" >&2
  exit 5
fi

echo
echo "== load the modules =="
require "LOAD OPEN PROCESSOR '${SCM_SCORER}';"
require "LOAD OPEN PROCESSOR '${SCM_PARAMS}';"

echo
echo "== list open processors (ADVISORY: on 5.2.0.4 this lists deployed OP"
echo "   INSTANCES, not loaded modules, so a fresh template may be absent; the"
echo "   real gate is the CREATE OPEN PROCESSOR compile during the TQL deploy) =="
LIST_RESP="$(tungsten 'LIST OPENPROCESSORS;')"
echo "   ${LIST_RESP}"
if ! printf '%s' "${LIST_RESP}" | grep -q 'FCVAEOnnxScorer'; then
  echo "   note: FCVAEOnnxScorer not listed (expected before its app deploys)"
fi
if ! printf '%s' "${LIST_RESP}" | grep -q 'FCVAEParamsOp'; then
  echo "   note: FCVAEParamsOp not listed (expected before its app deploys)"
fi

echo
echo "== deploy the pipeline via console.sh (absolute -f path; the TQL ends in quit;) =="
if ! "${STRIIM_HOME}/bin/console.sh" -c "${CLUSTER}" -u admin -p "${STRIIM_ADMIN_PW}" -f "${TQL_FILE}"; then
  echo "WARN: console.sh exited nonzero; continuing to the status poll" >&2
fi

echo
echo "== poll app status until RUNNING (timeout 60s) =="
DEADLINE=$(( $(date +%s) + 60 ))
STATUS=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  MON_RESP="$(tungsten "mon ${APP};")"
  STATUS="$(printf '%s' "${MON_RESP}" | python3 -c '
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
')"
  echo "   statusChange: ${STATUS:-<none>}"
  case "${STATUS}" in
    *RUNNING*) break ;;
  esac
  sleep 5
done

case "${STATUS}" in
  *RUNNING*)
    echo "OK: ${APP} is RUNNING"
    ;;
  *)
    echo "ERROR: ${APP} did not reach RUNNING within 60s (last statusChange: ${STATUS:-<none>})" >&2
    exit 7
    ;;
esac
