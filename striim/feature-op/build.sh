#!/usr/bin/env bash
# Build the FeatureOp Open Processor and install it into Striim's UploadedFiles.
#
# All Striim dependencies are system-scoped, so no `mvn install:install-file`
# step is needed. Maven shade produces target/FeatureOp.jar; we copy it to
# $STRIIM_HOME/UploadedFiles/FeatureOp.scm, then load it in the Striim console:
#
#   LOAD OPEN PROCESSOR 'UploadedFiles/FeatureOp.scm';
set -euo pipefail
cd "$(dirname "$0")"

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
MODULE="FeatureOp"

mvn clean package

# Drop any stale copy from Striim's OpenProcessor cache (see CLAUDE.md).
rm -f "${STRIIM_HOME}/.striim/OpenProcessor/${MODULE}.scm"

cp "target/${MODULE}.jar" "${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
# Also refresh the committed copy alongside the source so the repo always
# carries a deployable artifact for this OP.
cp "target/${MODULE}.jar" "${MODULE}.scm"
echo ""
echo "Installed ${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
echo "Repo copy:  $(pwd)/${MODULE}.scm"
echo "Load it with:  LOAD OPEN PROCESSOR 'UploadedFiles/${MODULE}.scm';"
echo "Reminder: do a full Striim restart before re-loading an updated .scm (ZLIB cache)."
