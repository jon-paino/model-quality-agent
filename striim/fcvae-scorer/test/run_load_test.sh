#!/usr/bin/env bash
# Compile and run OnnxLoadTest against the shaded FCVAEOnnxScorer jar (which
# supplies ai.onnxruntime.* and com.google.gson.* on the classpath).
#
# Usage: test/run_load_test.sh <model.onnx> <golden_windows.jsonl> [maxAbsTol] [meanAbsTol]
set -euo pipefail
cd "$(dirname "$0")/.."

STRIIM_HOME="${STRIIM_HOME:-/opt/Striim}"

if [ ! -f target/FCVAEOnnxScorer.jar ]; then
    mvn -q clean package -DSTRIIM_HOME="${STRIIM_HOME}"
fi

javac -cp target/FCVAEOnnxScorer.jar -d test/classes test/OnnxLoadTest.java
java -cp "target/FCVAEOnnxScorer.jar:test/classes" OnnxLoadTest "$@"
