"""SQL builders for MIC-50 quantile baseline registry publish."""

from __future__ import annotations

from typing import Iterable, List, Tuple

RT_RULE_QUANTILE_BASELINES_TABLE = "lakehouse.dims.rt_rule_quantile_baselines"
M1_RULE_VERSION = "rt_rules_v1"
M1_EFFECTIVE_FROM = "2026-01-01"
M1_EFFECTIVE_TO = "2099-12-31"
M1_COMPUTED_AT = "2026-03-10 00:00:00"

_REQUIRED_RT_RULE_QUANTILE_BASELINES_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("rule_version", "STRING"),
    ("effective_from", "DATE"),
    ("effective_to", "DATE"),
    ("metric_name", "STRING"),
    ("percentile", "INT"),
    ("cohort_category", "STRING"),
    ("cohort_region", "STRING"),
    ("threshold_value", "DOUBLE"),
    ("sample_size", "BIGINT"),
    ("is_fallback", "BOOLEAN"),
    ("computed_at", "TIMESTAMP"),
)

_M1_SEED_ROWS_SQL: Tuple[str, ...] = (
    (
        f"('{M1_RULE_VERSION}', DATE '{M1_EFFECTIVE_FROM}', DATE '{M1_EFFECTIVE_TO}', "
        "'velocity_30m', 90, NULL, NULL, 0.68, 1800, FALSE, "
        f"TIMESTAMP '{M1_COMPUTED_AT}')"
    ),
    (
        f"('{M1_RULE_VERSION}', DATE '{M1_EFFECTIVE_FROM}', DATE '{M1_EFFECTIVE_TO}', "
        "'impressions_30m', 40, NULL, NULL, 160.0, 1800, FALSE, "
        f"TIMESTAMP '{M1_COMPUTED_AT}')"
    ),
)


def required_rt_rule_quantile_baselines_columns() -> Tuple[Tuple[str, str], ...]:
    return _REQUIRED_RT_RULE_QUANTILE_BASELINES_COLUMNS


def create_rt_rule_quantile_baselines_sql(
    table_name: str = RT_RULE_QUANTILE_BASELINES_TABLE,
) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        rule_version STRING,
        effective_from DATE,
        effective_to DATE,
        metric_name STRING,
        percentile INT,
        cohort_category STRING,
        cohort_region STRING,
        threshold_value DOUBLE,
        sample_size BIGINT,
        is_fallback BOOLEAN,
        computed_at TIMESTAMP
    ) USING iceberg
    """.strip()


def missing_rt_rule_quantile_baselines_columns(
    existing_columns: Iterable[str],
) -> List[Tuple[str, str]]:
    existing = {name.lower() for name in existing_columns}
    return [
        (name, data_type)
        for name, data_type in _REQUIRED_RT_RULE_QUANTILE_BASELINES_COLUMNS
        if name.lower() not in existing
    ]


def manual_alter_rt_rule_quantile_baselines_statements(
    existing_columns: Iterable[str],
    table_name: str = RT_RULE_QUANTILE_BASELINES_TABLE,
) -> List[str]:
    return [
        f"ALTER TABLE {table_name} ADD COLUMNS ({name} {data_type});"
        for name, data_type in missing_rt_rule_quantile_baselines_columns(existing_columns)
    ]


def publish_rt_rules_v1_seed_sql(
    table_name: str = RT_RULE_QUANTILE_BASELINES_TABLE,
) -> str:
    values_sql = ",\n        ".join(_M1_SEED_ROWS_SQL)
    return f"""
    INSERT INTO {table_name}
    SELECT
        rule_version,
        effective_from,
        effective_to,
        metric_name,
        percentile,
        cohort_category,
        cohort_region,
        threshold_value,
        sample_size,
        is_fallback,
        computed_at
    FROM (
        VALUES
        {values_sql}
    ) AS seed (
        rule_version,
        effective_from,
        effective_to,
        metric_name,
        percentile,
        cohort_category,
        cohort_region,
        threshold_value,
        sample_size,
        is_fallback,
        computed_at
    )
    WHERE NOT EXISTS (
        SELECT 1
        FROM {table_name} existing
        WHERE existing.rule_version = '{M1_RULE_VERSION}'
          AND existing.effective_from = DATE '{M1_EFFECTIVE_FROM}'
    )
    """.strip()
