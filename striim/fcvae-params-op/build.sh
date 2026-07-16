#!/usr/bin/env bash
# Build the FCVAEParamsOp Open Processor and install it into Striim's UploadedFiles.
#
# All Striim dependencies are system-scoped; gson is bundled by shade. Maven
# shade produces target/FCVAEParamsOp.jar; we copy it to
# $STRIIM_HOME/UploadedFiles/FCVAEParamsOp.scm, then load it in the Striim console:
#
#   LOAD OPEN PROCESSOR 'UploadedFiles/FCVAEParamsOp.scm';
set -euo pipefail
cd "$(dirname "$0")"

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
MODULE="FCVAEParamsOp"

mvn clean package

# Drop any stale copy from Striim's OpenProcessor cache (see CLAUDE.md).
rm -f "${STRIIM_HOME}/.striim/OpenProcessor/${MODULE}.scm"

cp "target/${MODULE}.jar" "${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
echo ""
echo "Installed ${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
echo "Load it with:  LOAD OPEN PROCESSOR 'UploadedFiles/${MODULE}.scm';"
echo "Reminder: UNLOAD any previously loaded copy before re-LOAD:"
echo "  UNLOAD OPEN PROCESSOR 'UploadedFiles/${MODULE}.scm';"
