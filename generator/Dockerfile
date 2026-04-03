FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "confluent-kafka>=2.6.0" \
    "aws-msk-iam-sasl-signer-python>=1.0.0"

COPY src/ ./src/
COPY config/benchmark-run.json ./config/benchmark-run.json
COPY generator/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Build from repo root:
#   docker build -f generator/Dockerfile -t benchmark-generator .
#
# MSK_BOOTSTRAP_SERVERS must be set in the ECS task environment.
# Per-instance seed is passed via ECS run-task --overrides command: ["--seed", "2"]
ENTRYPOINT ["/entrypoint.sh"]
