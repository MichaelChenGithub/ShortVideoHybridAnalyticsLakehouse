-- MIC-55: Canonical serving view for rolling 30-minute video metrics.
-- Contract refs:
-- - docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md (sections 5.1, 6, 7.1)
-- - docs/architecture/realtime-decisioning/metric-contract.md

CREATE SCHEMA IF NOT EXISTS lakehouse.serving;

CREATE OR REPLACE VIEW lakehouse.serving.v_rt_video_metrics_30m_1m AS
WITH minute_rollup AS (
    SELECT
        video_id,
        window_start AS metric_minute,
        SUM(impressions) OVER w AS impressions_30m,
        SUM(play_start) OVER w AS play_start_30m,
        SUM(play_finish) OVER w AS play_finish_30m,
        SUM(likes) OVER w AS likes_30m,
        SUM(shares) OVER w AS shares_30m,
        SUM(skips) OVER w AS skips_30m,
        MAX(processed_at) OVER w AS processed_at_max
    FROM lakehouse.gold.rt_video_stats_1min
    WINDOW w AS (
        PARTITION BY video_id
        ORDER BY window_start
        RANGE BETWEEN INTERVAL '29' MINUTE PRECEDING AND CURRENT ROW
    )
)
SELECT
    video_id,
    metric_minute,
    impressions_30m,
    play_start_30m,
    play_finish_30m,
    likes_30m,
    shares_30m,
    skips_30m,
    CAST((likes_30m + 5 * shares_30m) AS DOUBLE) / CAST(GREATEST(impressions_30m, 100) AS DOUBLE) AS velocity_30m,
    CAST(play_finish_30m AS DOUBLE) / CAST(GREATEST(play_start_30m, 1) AS DOUBLE) AS completion_rate_30m,
    CAST(skips_30m AS DOUBLE) / CAST(GREATEST(play_start_30m, 1) AS DOUBLE) AS skip_rate_30m,
    processed_at_max
FROM minute_rollup;

-- Minimal executable freshness check query (MIC-55 acceptance #4).
-- Healthy target <= 180s; severe breach > 600s.
SELECT
    current_timestamp AS checked_at,
    MAX(metric_minute) AS latest_metric_minute,
    date_diff('second', MAX(metric_minute), current_timestamp) AS lag_seconds
FROM lakehouse.serving.v_rt_video_metrics_30m_1m;

-- MIC-56: Traceable decision-context serving view for recommendation preview.
-- Contract refs:
-- - docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md (sections 5.2, 6, 7.2)
-- - docs/architecture/realtime-decisioning/metric-contract.md
CREATE OR REPLACE VIEW lakehouse.serving.v_rt_video_decision_context_30m_1m AS
WITH locked_global_baselines AS (
    SELECT
        rule_version,
        effective_from,
        metric_name,
        percentile,
        threshold_value
    FROM lakehouse.dims.rt_rule_quantile_baselines
    WHERE rule_version = 'rt_rules_v1'
      AND cohort_category IS NULL
      AND cohort_region IS NULL
),
locked_latest_effective_from AS (
    SELECT MAX(effective_from) AS effective_from
    FROM locked_global_baselines
),
locked_thresholds AS (
    SELECT
        'rt_rules_v1' AS rule_version,
        MAX(
            CASE
                WHEN metric_name = 'velocity_30m' AND percentile = 90 THEN threshold_value
            END
        ) AS p90_velocity_threshold,
        MAX(
            CASE
                WHEN metric_name = 'impressions_30m' AND percentile = 40 THEN threshold_value
            END
        ) AS p40_impressions_threshold
    FROM locked_global_baselines b
    INNER JOIN locked_latest_effective_from e
        ON b.effective_from = e.effective_from
),
decision_inputs AS (
    SELECT
        m.video_id,
        m.metric_minute,
        d.category,
        d.region,
        d.status,
        d.upload_time,
        date_diff('minute', d.upload_time, m.metric_minute) AS upload_age_minutes,
        m.impressions_30m,
        m.play_start_30m,
        m.velocity_30m,
        m.completion_rate_30m,
        m.skip_rate_30m,
        m.processed_at_max,
        t.rule_version,
        t.p90_velocity_threshold,
        t.p40_impressions_threshold,
        COALESCE(
            (
                m.velocity_30m >= t.p90_velocity_threshold
                AND m.impressions_30m >= 100
            ),
            FALSE
        ) AS candidate_flag,
        (
            m.completion_rate_30m >= 0.55
            AND m.skip_rate_30m <= 0.35
            AND m.play_start_30m >= 30
        ) AS quality_gate_pass,
        COALESCE(
            (
                m.impressions_30m <= t.p40_impressions_threshold
            ),
            FALSE
        ) AS under_exposed_flag
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m m
    LEFT JOIN lakehouse.dims.dim_videos d
        ON m.video_id = d.video_id
    CROSS JOIN locked_thresholds t
)
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
    rule_version,
    candidate_flag,
    quality_gate_pass,
    under_exposed_flag,
    CASE
        WHEN category IS NULL OR region IS NULL OR status IS NULL OR upload_time IS NULL THEN 'NO_ACTION'
        WHEN candidate_flag AND quality_gate_pass THEN 'BOOST'
        WHEN candidate_flag AND NOT quality_gate_pass THEN 'REVIEW'
        WHEN
            NOT candidate_flag
            AND quality_gate_pass
            AND upload_age_minutes <= 60
            AND under_exposed_flag THEN 'RESCUE'
        ELSE 'NO_ACTION'
    END AS decision_type_preview,
    p90_velocity_threshold,
    p40_impressions_threshold,
    processed_at_max
FROM decision_inputs;

-- MIC-56 acceptance check: grain safety (`video_id + metric_minute`) must be preserved.
WITH metrics_grain AS (
    SELECT
        video_id,
        metric_minute
    FROM lakehouse.serving.v_rt_video_metrics_30m_1m
),
context_grain AS (
    SELECT
        video_id,
        metric_minute
    FROM lakehouse.serving.v_rt_video_decision_context_30m_1m
),
metrics_dupes AS (
    SELECT
        video_id,
        metric_minute,
        COUNT(*) AS row_count
    FROM metrics_grain
    GROUP BY video_id, metric_minute
    HAVING COUNT(*) > 1
),
context_dupes AS (
    SELECT
        video_id,
        metric_minute,
        COUNT(*) AS row_count
    FROM context_grain
    GROUP BY video_id, metric_minute
    HAVING COUNT(*) > 1
)
SELECT
    (SELECT COUNT(*) FROM metrics_grain) AS metrics_rows,
    (SELECT COUNT(*) FROM context_grain) AS context_rows,
    (SELECT COUNT(*) FROM metrics_dupes) AS metrics_duplicate_keys,
    (SELECT COUNT(*) FROM context_dupes) AS context_duplicate_keys,
    (SELECT COUNT(*) FROM context_grain) - (SELECT COUNT(*) FROM metrics_grain) AS row_delta;
