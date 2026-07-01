#!/usr/bin/env bash
# Build the ModelQualityAgent Open Processor (Quality Monitoring Agent, Layer 1)
# and stage it into Striim's UploadedFiles.
#
# All Striim dependencies are system-scoped; Jackson is provided (Striim ships it
# in lib/), so the agent bundles nothing else. Maven shade produces
# target/ModelQualityAgent.jar; we copy it to
# $STRIIM_HOME/UploadedFiles/ModelQualityAgent.scm and load it from there (the
# pass-through UploadedFiles + unload/load path from CLAUDE.md).
#
# Prerequisite: JMX must be enabled so the com.striim.metrics health beans exist
# (EnableJmx=true in conf/startUp.properties AND -Dstriim.node.jmx.enabled=true in
# bin/server.sh JMX_ARGS; see the team's JMX-enablement notes). The agent reads the
# in-JVM MBeanServer only; it does not need the remote JMX connector.
#
# In the Striim console:
#   LOAD OPEN PROCESSOR 'UploadedFiles/ModelQualityAgent.scm';
# then deploy the agent app (see striim/quality-agent/quality_agent.tql).
# To iterate:
#   UNLOAD OPEN PROCESSOR 'UploadedFiles/ModelQualityAgent.scm';  rebuild;  LOAD again.
set -euo pipefail
cd "$(dirname "$0")"

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"
MODULE="ModelQualityAgent"

mvn clean package

cp "target/${MODULE}.jar" "${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
# Keep a deployable copy alongside the source (matches feature-op/model-op).
cp "target/${MODULE}.jar" "${MODULE}.scm"
echo ""
echo "Installed ${STRIIM_HOME}/UploadedFiles/${MODULE}.scm"
echo "Repo copy:  $(pwd)/${MODULE}.scm"
echo "Load it with:  LOAD OPEN PROCESSOR 'UploadedFiles/${MODULE}.scm';"
echo "Then DEPLOY/START the agent app from striim/quality-agent/quality_agent.tql"
