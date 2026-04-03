#!/bin/sh
# Reads MSK_BOOTSTRAP_SERVERS from ECS task environment.
# Extra args (e.g. --seed 2) are passed via ECS run-task --overrides command array.
exec python src/generator/bounded_run_cli.py \
  --config config/benchmark-run.json \
  --sink kafka \
  --msk-iam \
  --bootstrap-servers "${MSK_BOOTSTRAP_SERVERS}" \
  "$@"
