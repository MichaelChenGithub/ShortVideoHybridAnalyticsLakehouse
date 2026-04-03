-- Metabase Batch Dashboard: batch analytics dashboard + acceptance SQL pack (M2 scope).
-- Scope lock:
-- 1) Uses only batch semantic serving views:
--    - lakehouse.serving.v_bt_retention_daily
--    - lakehouse.serving.v_bt_engagement_daily
--    - lakehouse.serving.v_bt_sessionization_daily
-- 2) Dashboard default context targets ET D-1 publish slice.
-- 3) BI SQL does not recompute governed formulas; it reads semantic fields as-is.

-- ============================================================================
-- Section A: Metabase Dashboard Queries
-- ============================================================================

-- A1. Retention trend (D1 vs D7) for the latest published data_date.
-- Intended chart: line chart grouped by day_n over cohort_date.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
)
SELECT
    r.cohort_date,
    r.day_n,
    SUM(r.cohort_users) AS cohort_users,
    SUM(r.retained_users) AS retained_users,
    CASE
        WHEN SUM(r.cohort_users) > 0
        THEN CAST(SUM(r.retained_users) AS DOUBLE) / SUM(r.cohort_users)
        ELSE NULL
    END AS avg_retention_rate,
    MAX(r.published_at) AS published_at
FROM lakehouse.serving.v_bt_retention_daily r
CROSS JOIN target t
WHERE r.data_date = t.data_date
GROUP BY r.cohort_date, r.day_n
ORDER BY r.cohort_date ASC, r.day_n ASC;


-- A2. Engagement KPI summary by category for the latest published data_date.
-- Intended chart: table/bar chart with category ranking.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
)
SELECT
    e.category,
    SUM(e.impressions) AS impressions,
    SUM(e.play_start) AS play_start,
    SUM(e.play_finish) AS play_finish,
    SUM(e.likes) AS likes,
    SUM(e.shares) AS shares,
    SUM(e.skips) AS skips,
    CAST(SUM(e.play_start) AS DOUBLE) / GREATEST(CAST(SUM(e.impressions) AS DOUBLE), 1.0) AS avg_play_start_rate,
    CAST(SUM(e.play_finish) AS DOUBLE) / GREATEST(CAST(SUM(e.play_start) AS DOUBLE), 1.0) AS avg_completion_rate,
    CAST(SUM(e.likes + e.shares) AS DOUBLE) / GREATEST(CAST(SUM(e.play_finish) AS DOUBLE), 1.0) AS avg_interaction_rate,
    CAST(SUM(e.skips) AS DOUBLE) / GREATEST(CAST(SUM(e.play_start) AS DOUBLE), 1.0) AS avg_skip_rate,
    MAX(e.published_at) AS published_at
FROM lakehouse.serving.v_bt_engagement_daily e
CROSS JOIN target t
WHERE e.data_date = t.data_date
GROUP BY e.category
ORDER BY impressions DESC
LIMIT 100;


-- A3. Sessionization summary by region for the latest published data_date.
-- Intended chart: table/bar chart with region ranking.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
)
SELECT
    s.region,
    SUM(s.sessions) AS sessions,
    CASE
        WHEN SUM(
            CASE
                WHEN s.sessions_per_user > 0
                THEN CAST(s.sessions AS DOUBLE) / s.sessions_per_user
                ELSE 0.0
            END
        ) > 0
        THEN CAST(SUM(s.sessions) AS DOUBLE) / SUM(
            CASE
                WHEN s.sessions_per_user > 0
                THEN CAST(s.sessions AS DOUBLE) / s.sessions_per_user
                ELSE 0.0
            END
        )
        ELSE NULL
    END AS avg_sessions_per_user,
    CASE
        WHEN SUM(s.sessions) > 0
        THEN CAST(SUM(s.avg_session_duration_sec * s.sessions) AS DOUBLE) / SUM(s.sessions)
        ELSE NULL
    END AS avg_session_duration_sec,
    CASE
        WHEN SUM(s.sessions) > 0
        THEN CAST(SUM(s.events_per_session * s.sessions) AS DOUBLE) / SUM(s.sessions)
        ELSE NULL
    END AS avg_events_per_session,
    CASE
        WHEN SUM(s.sessions) > 0
        THEN CAST(SUM(s.watch_time_per_session_ms * s.sessions) AS DOUBLE) / SUM(s.sessions)
        ELSE NULL
    END AS avg_watch_time_per_session_ms,
    MAX(s.published_at) AS published_at
FROM lakehouse.serving.v_bt_sessionization_daily s
CROSS JOIN target t
WHERE s.data_date = t.data_date
GROUP BY s.region
ORDER BY sessions DESC
LIMIT 100;


-- A4. Segment deep-dive table (engagement + sessionization) for latest published data_date.
-- Intended chart: drilldown table by category/region/new_vs_returning_user.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
),
engagement AS (
    SELECT
        e.data_date,
        e.category,
        e.region,
        e.new_vs_returning_user,
        e.impressions,
        e.play_start,
        e.play_finish,
        e.likes,
        e.shares,
        e.skips,
        e.play_start_rate,
        e.completion_rate,
        e.interaction_rate,
        e.skip_rate,
        e.published_at AS engagement_published_at
    FROM lakehouse.serving.v_bt_engagement_daily e
    CROSS JOIN target t
    WHERE e.data_date = t.data_date
),
sessionization AS (
    SELECT
        s.data_date,
        s.category,
        s.region,
        s.new_vs_returning_user,
        s.sessions,
        s.sessions_per_user,
        s.avg_session_duration_sec,
        s.events_per_session,
        s.watch_time_per_session_ms,
        s.published_at AS sessionization_published_at
    FROM lakehouse.serving.v_bt_sessionization_daily s
    CROSS JOIN target t
    WHERE s.data_date = t.data_date
)
SELECT
    e.data_date,
    e.category,
    e.region,
    e.new_vs_returning_user,
    e.impressions,
    e.play_start,
    e.play_finish,
    e.likes,
    e.shares,
    e.skips,
    e.play_start_rate,
    e.completion_rate,
    e.interaction_rate,
    e.skip_rate,
    s.sessions,
    s.sessions_per_user,
    s.avg_session_duration_sec,
    s.events_per_session,
    s.watch_time_per_session_ms,
    e.engagement_published_at,
    s.sessionization_published_at
FROM engagement e
JOIN sessionization s
  ON e.data_date = s.data_date
 AND e.category = s.category
 AND e.region = s.region
 AND e.new_vs_returning_user = s.new_vs_returning_user
ORDER BY e.impressions DESC, e.category, e.region, e.new_vs_returning_user
LIMIT 500;


-- ============================================================================
-- Section B: Acceptance SQL Checks
-- ============================================================================

-- B1. Retention grain uniqueness check.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
)
SELECT
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT
        cohort_date,
        day_n,
        category,
        region,
        new_vs_returning_user,
        COUNT(*) AS row_count
    FROM lakehouse.serving.v_bt_retention_daily r
    CROSS JOIN target t
    WHERE r.data_date = t.data_date
    GROUP BY 1, 2, 3, 4, 5
    HAVING COUNT(*) > 1
) dupes;


-- B2. Engagement grain uniqueness check.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
)
SELECT
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT
        e.data_date,
        e.category,
        e.region,
        e.new_vs_returning_user,
        COUNT(*) AS row_count
    FROM lakehouse.serving.v_bt_engagement_daily e
    CROSS JOIN target t
    WHERE e.data_date = t.data_date
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
) dupes;


-- B3. Sessionization grain uniqueness check.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
)
SELECT
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT
        s.data_date,
        s.category,
        s.region,
        s.new_vs_returning_user,
        COUNT(*) AS row_count
    FROM lakehouse.serving.v_bt_sessionization_daily s
    CROSS JOIN target t
    WHERE s.data_date = t.data_date
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
) dupes;


-- B4. Retention domain constraints check (`day_n` and segment domain).
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
),
base AS (
    SELECT *
    FROM lakehouse.serving.v_bt_retention_daily r
    CROSS JOIN target t
    WHERE r.data_date = t.data_date
)
SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(day_n NOT IN (1, 7) OR day_n IS NULL) AS invalid_day_n_rows,
    COUNT_IF(new_vs_returning_user NOT IN ('new', 'returning', 'unknown') OR new_vs_returning_user IS NULL) AS invalid_new_vs_returning_rows
FROM base;


-- B5. Segment-domain check across all three serving views.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
),
retention_segment_violations AS (
    SELECT COUNT(*) AS cnt
    FROM lakehouse.serving.v_bt_retention_daily r
    CROSS JOIN target t
    WHERE r.data_date = t.data_date
      AND (r.new_vs_returning_user NOT IN ('new', 'returning', 'unknown') OR r.new_vs_returning_user IS NULL)
),
engagement_segment_violations AS (
    SELECT COUNT(*) AS cnt
    FROM lakehouse.serving.v_bt_engagement_daily e
    CROSS JOIN target t
    WHERE e.data_date = t.data_date
      AND (e.new_vs_returning_user NOT IN ('new', 'returning', 'unknown') OR e.new_vs_returning_user IS NULL)
),
sessionization_segment_violations AS (
    SELECT COUNT(*) AS cnt
    FROM lakehouse.serving.v_bt_sessionization_daily s
    CROSS JOIN target t
    WHERE s.data_date = t.data_date
      AND (s.new_vs_returning_user NOT IN ('new', 'returning', 'unknown') OR s.new_vs_returning_user IS NULL)
)
SELECT
    r.cnt AS retention_invalid_segment_rows,
    e.cnt AS engagement_invalid_segment_rows,
    s.cnt AS sessionization_invalid_segment_rows
FROM retention_segment_violations r
CROSS JOIN engagement_segment_violations e
CROSS JOIN sessionization_segment_violations s;


-- B6. Required-field null-rate snapshot for all three views.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
),
retention_base AS (
    SELECT r.*
    FROM lakehouse.serving.v_bt_retention_daily r
    CROSS JOIN target t
    WHERE r.data_date = t.data_date
),
engagement_base AS (
    SELECT e.*
    FROM lakehouse.serving.v_bt_engagement_daily e
    CROSS JOIN target t
    WHERE e.data_date = t.data_date
),
sessionization_base AS (
    SELECT s.*
    FROM lakehouse.serving.v_bt_sessionization_daily s
    CROSS JOIN target t
    WHERE s.data_date = t.data_date
)
SELECT
    'retention' AS view_name,
    COUNT(*) AS total_rows,
    CAST(COUNT_IF(cohort_date IS NULL OR day_n IS NULL OR category IS NULL OR region IS NULL OR new_vs_returning_user IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_key_fields,
    CAST(COUNT_IF(cohort_users IS NULL OR retained_users IS NULL OR (cohort_users > 0 AND retention_rate IS NULL)) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_metric_fields,
    CAST(COUNT_IF(data_date IS NULL OR published_at IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_publish_fields
FROM retention_base
UNION ALL
SELECT
    'engagement' AS view_name,
    COUNT(*) AS total_rows,
    CAST(COUNT_IF(data_date IS NULL OR category IS NULL OR region IS NULL OR new_vs_returning_user IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_key_fields,
    CAST(COUNT_IF(impressions IS NULL OR play_start IS NULL OR play_finish IS NULL OR likes IS NULL OR shares IS NULL OR skips IS NULL OR play_start_rate IS NULL OR completion_rate IS NULL OR interaction_rate IS NULL OR skip_rate IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_metric_fields,
    CAST(COUNT_IF(published_at IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_publish_fields
FROM engagement_base
UNION ALL
SELECT
    'sessionization' AS view_name,
    COUNT(*) AS total_rows,
    CAST(COUNT_IF(data_date IS NULL OR category IS NULL OR region IS NULL OR new_vs_returning_user IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_key_fields,
    CAST(COUNT_IF(sessions IS NULL OR sessions_per_user IS NULL OR avg_session_duration_sec IS NULL OR events_per_session IS NULL OR watch_time_per_session_ms IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_metric_fields,
    CAST(COUNT_IF(published_at IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_publish_fields
FROM sessionization_base;


-- B7. Publish-date and freshness check across serving views.
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS expected_data_date
),
retention_latest AS (
    SELECT MAX(data_date) AS latest_data_date
    FROM lakehouse.serving.v_bt_retention_daily
),
retention_freshness AS (
    SELECT
        l.latest_data_date,
        MAX(r.published_at) AS latest_published_at
    FROM retention_latest l
    LEFT JOIN lakehouse.serving.v_bt_retention_daily r
      ON r.data_date = l.latest_data_date
    GROUP BY 1
),
engagement_latest AS (
    SELECT MAX(data_date) AS latest_data_date
    FROM lakehouse.serving.v_bt_engagement_daily
),
engagement_freshness AS (
    SELECT
        l.latest_data_date,
        MAX(e.published_at) AS latest_published_at
    FROM engagement_latest l
    LEFT JOIN lakehouse.serving.v_bt_engagement_daily e
      ON e.data_date = l.latest_data_date
    GROUP BY 1
),
sessionization_latest AS (
    SELECT MAX(data_date) AS latest_data_date
    FROM lakehouse.serving.v_bt_sessionization_daily
),
sessionization_freshness AS (
    SELECT
        l.latest_data_date,
        MAX(s.published_at) AS latest_published_at
    FROM sessionization_latest l
    LEFT JOIN lakehouse.serving.v_bt_sessionization_daily s
      ON s.data_date = l.latest_data_date
    GROUP BY 1
)
SELECT
    current_timestamp AS checked_at,
    t.expected_data_date,
    r.latest_data_date AS retention_latest_data_date,
    e.latest_data_date AS engagement_latest_data_date,
    s.latest_data_date AS sessionization_latest_data_date,
    date_diff('minute', r.latest_published_at, current_timestamp) AS retention_publish_lag_minutes,
    date_diff('minute', e.latest_published_at, current_timestamp) AS engagement_publish_lag_minutes,
    date_diff('minute', s.latest_published_at, current_timestamp) AS sessionization_publish_lag_minutes
FROM target t
CROSS JOIN retention_freshness r
CROSS JOIN engagement_freshness e
CROSS JOIN sessionization_freshness s;


-- B8. Serving-readiness evidence check (non-empty semantic outputs for ET D-1).
WITH target AS (
    SELECT date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date)) AS data_date
),
retention_rows AS (
    SELECT COUNT(*) AS row_count
    FROM lakehouse.serving.v_bt_retention_daily r
    CROSS JOIN target t
    WHERE r.data_date = t.data_date
),
engagement_rows AS (
    SELECT COUNT(*) AS row_count
    FROM lakehouse.serving.v_bt_engagement_daily e
    CROSS JOIN target t
    WHERE e.data_date = t.data_date
),
sessionization_rows AS (
    SELECT COUNT(*) AS row_count
    FROM lakehouse.serving.v_bt_sessionization_daily s
    CROSS JOIN target t
    WHERE s.data_date = t.data_date
)
SELECT
    t.data_date,
    r.row_count AS retention_rows,
    e.row_count AS engagement_rows,
    s.row_count AS sessionization_rows,
    CASE
        WHEN r.row_count > 0
         AND e.row_count > 0
         AND s.row_count > 0
        THEN true
        ELSE false
    END AS publish_ready
FROM target t
CROSS JOIN retention_rows r
CROSS JOIN engagement_rows e
CROSS JOIN sessionization_rows s;
