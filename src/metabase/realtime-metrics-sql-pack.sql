-- MIC-57: Metabase realtime dashboard + acceptance SQL pack (M1-S2).
-- Scope lock:
-- 1) Uses only serving views:
--    - lakehouse.serving.v_rt_video_metrics_30m_1m
--    - lakehouse.serving.v_rt_video_decision_context_30m_1m
-- 2) Dashboard/query windows are anchored to MAX(metric_minute) from source views
--    to keep demo/replay datasets queryable even when event time is in the past.
-- 3) No recomputation of rolling formulas or decision mapping logic in BI SQL.

-- ============================================================================
-- Section A: Metabase Dashboard Queries
-- ============================================================================

-- A1. Realtime health trend (rate-only, platform-level, 1-minute grain).
-- Purpose:
-- 1) Provide a stable health trend view for the whole platform over time.
-- 2) Use only rate metrics to avoid mixed-scale visualization distortion.
-- 3) Intended chart: one line chart with 3 lines
--    - avg_velocity_30m
--    - avg_completion_rate_30m
--    - avg_skip_rate_30m
-- Interpretation:
-- - "Healthy" means these lines stay within acceptable ranges with controlled volatility.
-- - Flat alone is not enough; the level must also be acceptable.
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
)
SELECT
    m.metric_minute,
    AVG(m.velocity_30m) AS avg_velocity_30m,
    AVG(m.completion_rate_30m) AS avg_completion_rate_30m,
    AVG(m.skip_rate_30m) AS avg_skip_rate_30m
FROM lakehouse.serving.v_rt_video_metrics_30m_1m m
CROSS JOIN anchor a
WHERE m.metric_minute >= a.anchor_metric_minute - INTERVAL '4' HOUR
GROUP BY m.metric_minute
ORDER BY m.metric_minute ASC;


-- A1b. Platform coverage trend (active video count, separate scale).
-- Purpose:
-- 1) Track how broad the realtime sample is, independent from quality-rate movement.
-- 2) Prevent misread caused by plotting count and rate on the same axis.
-- 3) Intended chart: separate single-line chart for active_videos.
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
)
SELECT
    m.metric_minute,
    COUNT(DISTINCT m.video_id) AS active_videos
FROM lakehouse.serving.v_rt_video_metrics_30m_1m m
CROSS JOIN anchor a
WHERE m.metric_minute >= a.anchor_metric_minute - INTERVAL '4' HOUR
GROUP BY m.metric_minute
ORDER BY m.metric_minute ASC;


-- A3. Recommendation table (latest row per video, includes NO_ACTION for auditability).
-- Window policy: fixed 30-minute lookback for operator-facing recommendation recency.
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
ranked AS (
    SELECT
        video_id,
        metric_minute,
        category,
        region,
        status,
        upload_time,
        upload_age_minutes,
        velocity_30m,
        completion_rate_30m,
        skip_rate_30m,
        decision_type_preview,
        candidate_flag,
        quality_gate_pass,
        under_exposed_flag,
        p90_velocity_threshold,
        p40_impressions_threshold,
        rule_version,
        processed_at_max,
        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY metric_minute DESC
        ) AS row_num
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '30' MINUTE
)
SELECT
    metric_minute,
    video_id,
    category,
    region,
    status,
    upload_time,
    upload_age_minutes,
    decision_type_preview,
    candidate_flag,
    quality_gate_pass,
    under_exposed_flag,
    velocity_30m,
    completion_rate_30m,
    skip_rate_30m,
    p90_velocity_threshold,
    p40_impressions_threshold,
    rule_version,
    processed_at_max
FROM ranked
WHERE row_num = 1
ORDER BY metric_minute DESC, decision_type_preview, video_id
LIMIT 500;


-- A4. Actionable recommendation queue preview (latest row per video, excludes NO_ACTION).
-- Window policy: fixed 30-minute lookback for actionable operator queue.
-- Demo view policy: neutral ordering by recency (avoid action-type-biased ranking in UI).
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
ranked AS (
    SELECT
        video_id,
        metric_minute,
        category,
        region,
        status,
        upload_age_minutes,
        decision_type_preview,
        candidate_flag,
        quality_gate_pass,
        under_exposed_flag,
        velocity_30m,
        completion_rate_30m,
        skip_rate_30m,
        p90_velocity_threshold,
        p40_impressions_threshold,
        rule_version,
        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY metric_minute DESC
        ) AS row_num
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '30' MINUTE
)
SELECT
    metric_minute,
    video_id,
    category,
    region,
    status,
    upload_age_minutes,
    decision_type_preview,
    candidate_flag,
    quality_gate_pass,
    under_exposed_flag,
    velocity_30m,
    completion_rate_30m,
    skip_rate_30m,
    p90_velocity_threshold,
    p40_impressions_threshold,
    rule_version
FROM ranked
WHERE row_num = 1
  AND decision_type_preview IN ('BOOST', 'REVIEW', 'RESCUE')
ORDER BY metric_minute DESC, video_id
LIMIT 500;


-- ============================================================================
-- Section B: Acceptance SQL Checks
-- ============================================================================

-- B1. Grain uniqueness check: metrics view must be unique on (video_id, metric_minute).
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
)
SELECT
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT
        video_id,
        metric_minute,
        COUNT(*) AS row_count
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '4' HOUR
    GROUP BY video_id, metric_minute
    HAVING COUNT(*) > 1
) dupes;


-- B2. Grain uniqueness check: decision-context view must be unique on (video_id, metric_minute).
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
)
SELECT
    COUNT(*) AS duplicate_key_groups
FROM (
    SELECT
        video_id,
        metric_minute,
        COUNT(*) AS row_count
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '4' HOUR
    GROUP BY video_id, metric_minute
    HAVING COUNT(*) > 1
) dupes;


-- B3. Grain amplification check: decision-context rows should not exceed metrics rows.
WITH metrics_anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
),
context_anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
metrics_rows AS (
    SELECT COUNT(*) AS row_count
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
    CROSS JOIN metrics_anchor
    WHERE metric_minute >= metrics_anchor.anchor_metric_minute - INTERVAL '4' HOUR
),
context_rows AS (
    SELECT COUNT(*) AS row_count
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN context_anchor
    WHERE metric_minute >= context_anchor.anchor_metric_minute - INTERVAL '4' HOUR
)
SELECT
    m.row_count AS metrics_rows,
    c.row_count AS context_rows,
    c.row_count - m.row_count AS row_delta
FROM metrics_rows m
CROSS JOIN context_rows c;


-- B4. Freshness check for both serving views.
WITH metrics_freshness AS (
    SELECT
        MAX(metric_minute) AS latest_metric_minute,
        MAX(processed_at_max) AS latest_processed_at
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
),
context_freshness AS (
    SELECT
        MAX(metric_minute) AS latest_metric_minute,
        MAX(processed_at_max) AS latest_processed_at
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
)
SELECT
    current_timestamp AS checked_at,
    m.latest_metric_minute AS metrics_latest_metric_minute,
    date_diff('second', m.latest_metric_minute, current_timestamp) AS metrics_metric_lag_seconds,
    date_diff('second', m.latest_processed_at, current_timestamp) AS metrics_processed_lag_seconds,
    c.latest_metric_minute AS context_latest_metric_minute,
    date_diff('second', c.latest_metric_minute, current_timestamp) AS context_metric_lag_seconds,
    date_diff('second', c.latest_processed_at, current_timestamp) AS context_processed_lag_seconds
FROM metrics_freshness m
CROSS JOIN context_freshness c;


-- B5. Null-rate check for strict required fields in decision-context serving output.
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
base AS (
    SELECT
        video_id,
        metric_minute,
        velocity_30m,
        completion_rate_30m,
        skip_rate_30m,
        rule_version,
        candidate_flag,
        quality_gate_pass,
        under_exposed_flag,
        decision_type_preview,
        p90_velocity_threshold,
        p40_impressions_threshold,
        processed_at_max
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '4' HOUR
)
SELECT
    COUNT(*) AS total_rows,
    CAST(COUNT_IF(video_id IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_video_id,
    CAST(COUNT_IF(metric_minute IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_metric_minute,
    CAST(COUNT_IF(velocity_30m IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_velocity_30m,
    CAST(COUNT_IF(completion_rate_30m IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_completion_rate_30m,
    CAST(COUNT_IF(skip_rate_30m IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_skip_rate_30m,
    CAST(COUNT_IF(rule_version IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_rule_version,
    CAST(COUNT_IF(candidate_flag IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_candidate_flag,
    CAST(COUNT_IF(quality_gate_pass IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_quality_gate_pass,
    CAST(COUNT_IF(under_exposed_flag IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_under_exposed_flag,
    CAST(COUNT_IF(decision_type_preview IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_decision_type_preview,
    CAST(COUNT_IF(p90_velocity_threshold IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_p90_velocity_threshold,
    CAST(COUNT_IF(p40_impressions_threshold IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_p40_impressions_threshold,
    CAST(COUNT_IF(processed_at_max IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_processed_at_max
FROM base;


-- B6. Dimension fallback observability (allowed to be non-zero; monitor trend for data quality).
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
base AS (
    SELECT
        category,
        region,
        status,
        upload_time
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '4' HOUR
)
SELECT
    COUNT(*) AS total_rows,
    CAST(COUNT_IF(category IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_category,
    CAST(COUNT_IF(region IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_region,
    CAST(COUNT_IF(status IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_status,
    CAST(COUNT_IF(upload_time IS NULL) AS DOUBLE) / NULLIF(COUNT(*), 0) AS null_rate_upload_time
FROM base;


-- B7. Recommendation-field/domain/traceability check.
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
latest_per_video AS (
    SELECT
        video_id,
        metric_minute,
        decision_type_preview,
        candidate_flag,
        quality_gate_pass,
        under_exposed_flag,
        p90_velocity_threshold,
        p40_impressions_threshold,
        rule_version,
        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY metric_minute DESC
        ) AS row_num
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '4' HOUR
)
SELECT
    COUNT(*) AS latest_video_rows,
    COUNT_IF(decision_type_preview NOT IN ('BOOST', 'REVIEW', 'RESCUE', 'NO_ACTION')) AS invalid_decision_domain_rows,
    COUNT_IF(rule_version <> 'rt_rules_v1' OR rule_version IS NULL) AS unexpected_rule_version_rows,
    COUNT_IF(
        decision_type_preview IN ('BOOST', 'REVIEW', 'RESCUE')
        AND (
            candidate_flag IS NULL
            OR quality_gate_pass IS NULL
            OR under_exposed_flag IS NULL
            OR p90_velocity_threshold IS NULL
            OR p40_impressions_threshold IS NULL
            OR rule_version IS NULL
        )
    ) AS actionable_traceability_gap_rows
FROM latest_per_video
WHERE row_num = 1;


-- B8. Recommendation distribution snapshot (latest per video) for sign-off evidence.
WITH anchor AS (
    SELECT MAX(metric_minute) AS anchor_metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
latest_per_video AS (
    SELECT
        video_id,
        decision_type_preview,
        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY metric_minute DESC
        ) AS row_num
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
    CROSS JOIN anchor
    WHERE metric_minute >= anchor.anchor_metric_minute - INTERVAL '4' HOUR
)
SELECT
    decision_type_preview,
    COUNT(*) AS video_count
FROM latest_per_video
WHERE row_num = 1
GROUP BY decision_type_preview
ORDER BY video_count DESC, decision_type_preview;
