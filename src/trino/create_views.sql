-- ====================================================================
-- Trino/Presto Views for Dashboard Serving Layer
-- Purpose: Encapsulate business logic and metric definitions (Analytics Engineering)
-- ====================================================================

CREATE SCHEMA IF NOT EXISTS lakehouse.serving;

-- View 1: Real-time Viral Velocity (The "Pulse")
-- Source: Gold Layer (Aggregated Logs) + Dims (CDC)
-- Logic: Calculates a weighted viral score over a sliding 30-minute window.
--        Demonstrates "Read-Time Join" pattern.

CREATE OR REPLACE VIEW lakehouse.serving.viral_velocity_30m AS
WITH recent_stats AS (
    SELECT 
        video_id,
        SUM(impressions) as total_impressions,
        SUM(likes) as total_likes,
        SUM(shares) as total_shares,
        SUM(play_finish) as total_completions,
        SUM(play_start) as total_starts
    FROM lakehouse.gold.rt_video_stats_1min
    WHERE window_start >= NOW() - INTERVAL '30' MINUTE
    GROUP BY video_id
),
enriched AS (
    SELECT 
        r.*,
        d.category,
        d.creator_id,
        d.duration_ms
    FROM recent_stats r
    LEFT JOIN lakehouse.dims.dim_videos d ON r.video_id = d.video_id
)
SELECT 
    video_id,
    category,
    total_impressions,
    -- Metric Definition: Viral Score = (Likes*5 + Shares*10) / Impressions
    (total_likes * 5 + total_shares * 10) / CAST(NULLIF(total_impressions, 0) AS DOUBLE) as viral_score,
    -- Metric Definition: Completion Rate
    total_completions / CAST(NULLIF(total_starts, 0) AS DOUBLE) as completion_rate
FROM enriched
WHERE total_impressions > 50 -- Noise Filter
ORDER BY viral_score DESC;

-- View 2: Global Doomscroll Rate
-- Logic: Monitors system health by tracking skip rates.
CREATE OR REPLACE VIEW lakehouse.serving.global_doomscroll_rate AS
SELECT 
    date_trunc('minute', window_start) as time_bucket,
    SUM(impressions) as total_impressions,
    -- Assuming 'impressions' minus 'play_start' roughly equals skips/swipes in this simplified model
    (SUM(impressions) - SUM(play_start)) / CAST(NULLIF(SUM(impressions), 0) AS DOUBLE) as skip_rate
FROM lakehouse.gold.rt_video_stats_1min
WHERE window_start >= NOW() - INTERVAL '4' HOUR
GROUP BY 1
ORDER BY 1 DESC;