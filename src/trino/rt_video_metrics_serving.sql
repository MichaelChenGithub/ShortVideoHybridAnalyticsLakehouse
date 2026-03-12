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
