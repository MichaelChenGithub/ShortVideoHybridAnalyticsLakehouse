-- Batch semantic serving views for governed retention, engagement, and sessionization outputs.
-- Contract refs:
-- - docs/architecture/serving/trino-batch-semantic-serving-contract.md (sections 5, 6, 7)
-- - docs/architecture/batch-analytics/batch-metrics-contract.md

CREATE SCHEMA IF NOT EXISTS lakehouse.serving;

CREATE OR REPLACE VIEW lakehouse.serving.v_bt_retention_daily AS
SELECT
    cohort_date,
    day_n,
    category,
    region,
    new_vs_returning_user,
    cohort_users,
    retained_users,
    retention_rate,
    data_date,
    published_at
FROM lakehouse.gold.batch_retention_daily
WHERE day_n IN (1, 7);

CREATE OR REPLACE VIEW lakehouse.serving.v_bt_engagement_daily AS
SELECT
    data_date,
    category,
    region,
    new_vs_returning_user,
    impressions,
    play_start,
    play_finish,
    likes,
    shares,
    skips,
    play_start_rate,
    completion_rate,
    interaction_rate,
    skip_rate,
    published_at
FROM lakehouse.gold.batch_engagement_daily;

CREATE OR REPLACE VIEW lakehouse.serving.v_bt_sessionization_daily AS
SELECT
    data_date,
    category,
    region,
    new_vs_returning_user,
    sessions,
    sessions_per_user,
    avg_session_duration_sec,
    events_per_session,
    watch_time_per_session_ms,
    published_at
FROM lakehouse.gold.batch_sessionization_daily;

-- Serving acceptance validates semantic health for the ET D-1 target slice only.
-- Publish-manifest readiness gating is deferred until lakehouse.gold.batch_publish_manifest
-- is implemented in its own issue scope.
WITH expected_publish_date AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS expected_data_date
),
retention_target_rows AS (
    SELECT r.*
    FROM lakehouse.serving.v_bt_retention_daily r
    CROSS JOIN expected_publish_date e
    WHERE r.data_date = e.expected_data_date
),
retention_dupes AS (
    SELECT
        cohort_date,
        day_n,
        category,
        region,
        new_vs_returning_user,
        COUNT(*) AS row_count
    FROM retention_target_rows
    GROUP BY 1, 2, 3, 4, 5
    HAVING COUNT(*) > 1
),
retention_target_summary AS (
    SELECT COUNT(*) AS target_row_count
    FROM retention_target_rows
)
SELECT
    MAX(duplicate_keys) AS duplicate_keys,
    MAX(target_row_count) AS target_row_count,
    SUM(CASE WHEN cohort_date IS NULL THEN 1 ELSE 0 END) AS null_cohort_date,
    SUM(CASE WHEN day_n IS NULL THEN 1 ELSE 0 END) AS null_day_n,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
    SUM(CASE WHEN new_vs_returning_user IS NULL THEN 1 ELSE 0 END) AS null_new_vs_returning_user,
    SUM(CASE WHEN cohort_users IS NULL THEN 1 ELSE 0 END) AS null_cohort_users,
    SUM(CASE WHEN retained_users IS NULL THEN 1 ELSE 0 END) AS null_retained_users,
    SUM(CASE WHEN retention_rate IS NULL THEN 1 ELSE 0 END) AS null_retention_rate,
    SUM(CASE WHEN data_date IS NULL THEN 1 ELSE 0 END) AS null_data_date,
    SUM(CASE WHEN published_at IS NULL THEN 1 ELSE 0 END) AS null_published_at,
    MAX(expected_data_date) AS expected_data_date
FROM retention_target_rows
CROSS JOIN (SELECT COUNT(*) AS duplicate_keys FROM retention_dupes)
CROSS JOIN retention_target_summary
CROSS JOIN expected_publish_date;

-- engagement acceptance check: grain uniqueness and required fields for the ET D-1 target slice.
WITH expected_publish_date AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS expected_data_date
),
engagement_target_rows AS (
    SELECT e.*
    FROM lakehouse.serving.v_bt_engagement_daily e
    CROSS JOIN expected_publish_date p
    WHERE e.data_date = p.expected_data_date
),
engagement_dupes AS (
    SELECT
        data_date,
        category,
        region,
        new_vs_returning_user,
        COUNT(*) AS row_count
    FROM engagement_target_rows
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
),
engagement_target_summary AS (
    SELECT COUNT(*) AS target_row_count
    FROM engagement_target_rows
)
SELECT
    MAX(duplicate_keys) AS duplicate_keys,
    MAX(target_row_count) AS target_row_count,
    SUM(CASE WHEN data_date IS NULL THEN 1 ELSE 0 END) AS null_data_date,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
    SUM(CASE WHEN new_vs_returning_user IS NULL THEN 1 ELSE 0 END) AS null_new_vs_returning_user,
    SUM(CASE WHEN impressions IS NULL THEN 1 ELSE 0 END) AS null_impressions,
    SUM(CASE WHEN play_start IS NULL THEN 1 ELSE 0 END) AS null_play_start,
    SUM(CASE WHEN play_finish IS NULL THEN 1 ELSE 0 END) AS null_play_finish,
    SUM(CASE WHEN likes IS NULL THEN 1 ELSE 0 END) AS null_likes,
    SUM(CASE WHEN shares IS NULL THEN 1 ELSE 0 END) AS null_shares,
    SUM(CASE WHEN skips IS NULL THEN 1 ELSE 0 END) AS null_skips,
    SUM(CASE WHEN play_start_rate IS NULL THEN 1 ELSE 0 END) AS null_play_start_rate,
    SUM(CASE WHEN completion_rate IS NULL THEN 1 ELSE 0 END) AS null_completion_rate,
    SUM(CASE WHEN interaction_rate IS NULL THEN 1 ELSE 0 END) AS null_interaction_rate,
    SUM(CASE WHEN skip_rate IS NULL THEN 1 ELSE 0 END) AS null_skip_rate,
    SUM(CASE WHEN published_at IS NULL THEN 1 ELSE 0 END) AS null_published_at,
    MAX(expected_data_date) AS expected_data_date
FROM engagement_target_rows
CROSS JOIN (SELECT COUNT(*) AS duplicate_keys FROM engagement_dupes)
CROSS JOIN engagement_target_summary
CROSS JOIN expected_publish_date;

-- sessionization acceptance check: grain uniqueness and required fields for the ET D-1 target slice.
WITH expected_publish_date AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS expected_data_date
),
sessionization_target_rows AS (
    SELECT s.*
    FROM lakehouse.serving.v_bt_sessionization_daily s
    CROSS JOIN expected_publish_date e
    WHERE s.data_date = e.expected_data_date
),
sessionization_dupes AS (
    SELECT
        data_date,
        category,
        region,
        new_vs_returning_user,
        COUNT(*) AS row_count
    FROM sessionization_target_rows
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
),
sessionization_target_summary AS (
    SELECT COUNT(*) AS target_row_count
    FROM sessionization_target_rows
)
SELECT
    MAX(duplicate_keys) AS duplicate_keys,
    MAX(target_row_count) AS target_row_count,
    SUM(CASE WHEN data_date IS NULL THEN 1 ELSE 0 END) AS null_data_date,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_region,
    SUM(CASE WHEN new_vs_returning_user IS NULL THEN 1 ELSE 0 END) AS null_new_vs_returning_user,
    SUM(CASE WHEN sessions IS NULL THEN 1 ELSE 0 END) AS null_sessions,
    SUM(CASE WHEN sessions_per_user IS NULL THEN 1 ELSE 0 END) AS null_sessions_per_user,
    SUM(CASE WHEN avg_session_duration_sec IS NULL THEN 1 ELSE 0 END) AS null_avg_session_duration_sec,
    SUM(CASE WHEN events_per_session IS NULL THEN 1 ELSE 0 END) AS null_events_per_session,
    SUM(CASE WHEN watch_time_per_session_ms IS NULL THEN 1 ELSE 0 END) AS null_watch_time_per_session_ms,
    SUM(CASE WHEN published_at IS NULL THEN 1 ELSE 0 END) AS null_published_at,
    MAX(expected_data_date) AS expected_data_date
FROM sessionization_target_rows
CROSS JOIN (SELECT COUNT(*) AS duplicate_keys FROM sessionization_dupes)
CROSS JOIN sessionization_target_summary
CROSS JOIN expected_publish_date;
