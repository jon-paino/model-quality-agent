#!/usr/bin/env bash
# Compile and run ParamsLookupTest against the shaded FCVAEParamsOp jar (which
# supplies com.google.gson.* on the classpath).
#
# Usage: test/run_params_test.sh <feastUrl> <comboKey> [featureRefsCsv]
set -euo pipefail
cd "$(dirname "$0")/.."

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"

if [ ! -f target/FCVAEParamsOp.jar ]; then
    mvn -q clean package -DSTRIIM_HOME="${STRIIM_HOME}"
fi

javac -cp target/FCVAEParamsOp.jar -d test/classes test/ParamsLookupTest.java
java -cp "target/FCVAEParamsOp.jar:test/classes" ParamsLookupTest "$@"
