"""Iceberg branch lifecycle operations for the WAP batch publish pattern.

Environment variables:
    ICEBERG_BRANCH_OP   one of 'create', 'fast_forward', 'drop'
    ICEBERG_WAP_BRANCH  the run-scoped branch name (e.g. run_scheduled__2026_03_27)
"""

from __future__ import annotations

import os
import sys

# Full catalog-qualified table names used in ALTER TABLE statements.
GOVERNED_TABLES = [
    "lakehouse.silver.events_conformed",
    "lakehouse.dims.dim_users_scd2",
    "lakehouse.dims.dim_videos_scd2",
    "lakehouse.silver.user_activity_sessions_30m",
    "lakehouse.gold.batch_retention_daily",
    "lakehouse.gold.batch_engagement_daily",
    "lakehouse.gold.batch_sessionization_daily",
]

# Namespace-qualified refs (without catalog) used in CALL procedure arguments.
GOVERNED_TABLE_REFS = [
    "silver.events_conformed",
    "dims.dim_users_scd2",
    "dims.dim_videos_scd2",
    "silver.user_activity_sessions_30m",
    "gold.batch_retention_daily",
    "gold.batch_engagement_daily",
    "gold.batch_sessionization_daily",
]

CATALOG = "lakehouse"


def _create_branches(spark: object, branch: str) -> None:
    for table in GOVERNED_TABLES:
        try:
            spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE BRANCH `{branch}`")  # type: ignore[union-attr]
            print(f"[BRANCH-LIFECYCLE] created branch={branch} on table={table}")
        except Exception as exc:
            # Table may not yet exist on first run; skip gracefully.
            print(f"[BRANCH-LIFECYCLE] WARN: could not create branch on {table}: {exc}")


def _fast_forward_branches(spark: object, branch: str) -> None:
    for table_ref in GOVERNED_TABLE_REFS:
        try:
            spark.sql(  # type: ignore[union-attr]
                f"CALL {CATALOG}.system.fast_forward("
                f"table => '{table_ref}', branch => 'main', to => '{branch}')"
            )
            print(
                f"[BRANCH-LIFECYCLE] fast_forward main <- {branch} on {CATALOG}.{table_ref}"
            )
        except Exception as exc:
            print(
                f"[BRANCH-LIFECYCLE] WARN: could not fast_forward {CATALOG}.{table_ref}: {exc}"
            )


def _drop_branches(spark: object, branch: str) -> None:
    for table in GOVERNED_TABLES:
        try:
            spark.sql(f"ALTER TABLE {table} DROP BRANCH IF EXISTS `{branch}`")  # type: ignore[union-attr]
            print(f"[BRANCH-LIFECYCLE] dropped branch={branch} on table={table}")
        except Exception as exc:
            print(f"[BRANCH-LIFECYCLE] WARN: could not drop branch on {table}: {exc}")


def main() -> int:
    op = os.environ.get("ICEBERG_BRANCH_OP", "").strip()
    branch = os.environ.get("ICEBERG_WAP_BRANCH", "").strip()

    if not op:
        print("ERROR: ICEBERG_BRANCH_OP env var is required", file=sys.stderr)
        return 1
    if not branch:
        print("ERROR: ICEBERG_WAP_BRANCH env var is required", file=sys.stderr)
        return 1
    if op not in ("create", "fast_forward", "drop"):
        print(f"ERROR: unknown ICEBERG_BRANCH_OP={op!r}", file=sys.stderr)
        return 1

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"iceberg_branch_{op}").getOrCreate()

    if op == "create":
        _create_branches(spark, branch)
    elif op == "fast_forward":
        _fast_forward_branches(spark, branch)
    else:
        _drop_branches(spark, branch)

    print(f"[BRANCH-LIFECYCLE] op={op} branch={branch} complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
