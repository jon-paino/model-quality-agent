# MQA training container (Week 3 Phase 3c).
#
# One-shot run: features build -> train fit -> export ONNX (parity gate) ->
# publish model.onnx + model.manifest.json to the volume ModelOp watches.
# Non-zero exit on the first failure; publish is the only step that touches
# the output volume and it refuses (writing nothing) unless every gate passes.
#
# Build (context = this python/ directory):
#   docker build -t mqa-trainer python/
#
# Run (same VM as Striim; training is CPU-heavy, cap it):
#   docker run --rm --cpus 4 \
#     -v <repo>/model/data/processed/trips_cleaned.parquet:/work/data/processed/trips_cleaned.parquet:ro \
#     -v /opt/Striim/UploadedFiles:/out \
#     mqa-trainer
#
# macOS Docker Desktop note: /opt is not in the default file-sharing list;
# add /opt/Striim (Settings -> Resources -> File Sharing) or publish to a
# shared staging dir and copy. On a Linux VM (the deployment target per the
# 6-week plan) the bind mount just works.

FROM python:3.12-slim AS builder
# python-geohash ships sdist-only; its small C++ extension needs g++.
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-install-project --no-dev --extra training

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY model/ model/
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && mkdir -p /work/data/processed /work/artifacts /out
ENV PATH="/app/.venv/bin:$PATH" \
    MODEL_DATA_PROCESSED=/work/data/processed \
    MODEL_ARTIFACTS=/work/artifacts \
    MODEL_PUBLISH_OUT=/out
ENTRYPOINT ["/entrypoint.sh"]
