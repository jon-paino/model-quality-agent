#!/usr/bin/env bash
# Build the FCVAEOnnxScorer Open Processor and install it into Striim's UploadedFiles.
#
# All Striim dependencies are system-scoped; ONNX Runtime + gson are bundled by
# shade. Maven shade produces target/FCVAEOnnxScorer.jar; we copy it to
# $STRIIM_HOME/UploadedFiles/FCVAEOnnxScorer.scm, then load it in the Striim console:
#
#   LOAD OPEN PROCESSOR 'UploadedFiles/FCVAEOnnxScorer.scm';
set -euo pipefail
cd "$(dirname "$0")"

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
MODULE="FCVAEOnnxScorer"

mvn clean package

# Drop any stale copy from Striim's OpenProcessor cache (see CLAUDE.md).
rm -f "${STRIIM_HOME}/.striim/OpenProcessor/${MODULE}.scm"

cp "target/${MODULE}.jar" "${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
echo ""
echo "Installed ${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
echo "Load it with:  LOAD OPEN PROCESSOR 'UploadedFiles/${MODULE}.scm';"
echo "Reminder: a stale ${MODULE}.scm from the sibling fcvae-anomaly-detection repo may"
echo "already be loaded in the cluster. UNLOAD it before re-LOAD:"
echo "  UNLOAD OPEN PROCESSOR 'UploadedFiles/${MODULE}.scm';"
