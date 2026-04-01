#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage: run_rule_baseline_publish_acceptance.sh

Environment overrides:
  SPARK_CONTAINER
  TABLE_NAME
  RESET_TABLE
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[RULE-BASELINE] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

SPARK_CONTAINER="${SPARK_CONTAINER:-lakehouse-spark}"
TABLE_NAME="${TABLE_NAME:-lakehouse.dims.rt_rule_quantile_baselines}"
RESET_TABLE="${RESET_TABLE:-1}"

printf '[RULE-BASELINE] Assuming infrastructure is up. Run: make reset-infra\n'

if [ "$RESET_TABLE" = "1" ]; then
  printf '[RULE-BASELINE] Resetting table for deterministic local acceptance...\n'
  docker exec -i -e RULE_BASELINE_TABLE="$TABLE_NAME" "$SPARK_CONTAINER" python - <<'PY'
from __future__ import annotations

import os

from pyspark.sql import SparkSession

table_name = os.environ["RULE_BASELINE_TABLE"]
spark = SparkSession.builder.getOrCreate()
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f"[RULE-BASELINE] Dropped table if existed: {table_name}")
PY
fi

printf '[RULE-BASELINE] Publishing global-only baseline rows via Spark publisher...\n'
docker exec "$SPARK_CONTAINER" python /home/iceberg/local/src/spark/rt_rule_quantile_baselines_publish.py \
  --table "$TABLE_NAME"

printf '[RULE-BASELINE] Re-running publisher to validate idempotency...\n'
docker exec "$SPARK_CONTAINER" python /home/iceberg/local/src/spark/rt_rule_quantile_baselines_publish.py \
  --table "$TABLE_NAME"

printf '[RULE-BASELINE] Running validation checks...\n'
docker exec -i -e RULE_BASELINE_TABLE="$TABLE_NAME" "$SPARK_CONTAINER" python - <<'PY'
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/home/iceberg/local/src")
from spark.rt_rule_quantile_baselines_sql import (  # noqa: E402
    BASELINE_EFFECTIVE_FROM,
    BASELINE_EFFECTIVE_TO,
    BASELINE_RULE_VERSION,
)

table_name = os.environ["RULE_BASELINE_TABLE"]
parts = table_name.split(".")
if len(parts) != 3:
    raise ValueError(f"Expected 3-part table name, got: {table_name}")
catalog_name, schema_name, relation_name = parts

spark = SparkSession.builder.getOrCreate()
published_filter = (
    f"rule_version = '{BASELINE_RULE_VERSION}' "
    f"AND effective_from = DATE '{BASELINE_EFFECTIVE_FROM}' "
    f"AND effective_to = DATE '{BASELINE_EFFECTIVE_TO}'"
)

published_count = spark.sql(
    f"""
    SELECT COUNT(*) AS row_count
    FROM {table_name}
    WHERE {published_filter}
    """
).collect()[0]["row_count"]
if published_count != 2:
    raise AssertionError(f"Expected 2 global-only rows for published set, found {published_count}")

table_exists = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").where(
    f"tableName = '{relation_name}'"
).count()
if table_exists != 1:
    raise AssertionError(f"Table {table_name} does not exist or is not queryable")

null_count = spark.sql(
    f"""
    SELECT COUNT(*) AS null_count
    FROM {table_name}
    WHERE {published_filter}
      AND (threshold_value IS NULL OR sample_size IS NULL)
    """
).collect()[0]["null_count"]
if null_count != 0:
    raise AssertionError(f"Published rows contain null threshold/sample values: {null_count}")

global_velocity = spark.sql(
    f"""
    SELECT COUNT(*) AS combo_count
    FROM {table_name}
    WHERE {published_filter}
      AND metric_name = 'velocity_30m'
      AND percentile = 90
      AND cohort_category IS NULL
      AND cohort_region IS NULL
      AND sample_size >= 1000
    """
).collect()[0]["combo_count"]
if global_velocity != 1:
    raise AssertionError("Missing required global velocity p90 baseline row with guard")

global_impressions = spark.sql(
    f"""
    SELECT COUNT(*) AS combo_count
    FROM {table_name}
    WHERE {published_filter}
      AND metric_name = 'impressions_30m'
      AND percentile = 40
      AND cohort_category IS NULL
      AND cohort_region IS NULL
      AND sample_size >= 1000
    """
).collect()[0]["combo_count"]
if global_impressions != 1:
    raise AssertionError("Missing required global impressions p40 baseline row with guard")

cohort_rows = spark.sql(
    f"""
    SELECT COUNT(*) AS combo_count
    FROM {table_name}
    WHERE {published_filter}
      AND (cohort_category IS NOT NULL OR cohort_region IS NOT NULL)
    """
).collect()[0]["combo_count"]
if cohort_rows != 0:
    raise AssertionError("Global-only scope expects no cohort rows in published set")

global_min_sample = spark.sql(
    f"""
    SELECT MIN(sample_size) AS min_sample
    FROM {table_name}
    WHERE {published_filter}
      AND cohort_category IS NULL
      AND cohort_region IS NULL
    """
).collect()[0]["min_sample"]
if global_min_sample is None or global_min_sample < 1000:
    raise AssertionError(
        "Global publish guard violated: expected min global sample_size >= 1000"
    )

print("[RULE-BASELINE] Validation checks passed.")
PY

printf '[RULE-BASELINE] Acceptance flow completed.\n'
