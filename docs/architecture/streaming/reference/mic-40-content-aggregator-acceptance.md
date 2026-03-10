# MIC-40 Content Aggregator Bring-up Acceptance Reference

## Scope and Boundaries

In scope:
1. Bring-up for `rt_content_events_aggregator` on `content_events`.
2. Valid writes to `lakehouse.bronze.raw_events` and `lakehouse.gold.rt_video_stats_1min`.
3. Checkpoint evidence for both valid sinks.

Out of scope:
1. `invalid_events_content` contract enforcement validation (`MIC-39`).
2. CDC pipeline validation (`MIC-37`/`MIC-43`).

Runtime defaults (MIC-40):
1. `startingOffsets=latest`
2. trigger intervals: `raw_events=10 seconds`, `rt_video_stats_1min=1 minute`
3. watermark policy for content aggregation: baseline `2 minutes`, lag-prone environments `5 minutes`
4. checkpoints:
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1`
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1`

## One-Command Entrypoint

```bash
bash src/scripts/run_mic40_acceptance.sh
```

## Required Environment Variables and Defaults

None required for default acceptance.

Default verifier thresholds:
1. `min_raw_rows=1`
2. `min_gold_rows=1`
3. `max_freshness_minutes=10`

## Artifacts and Output Interpretation

Pass criteria:
1. Verifier confirms minimum Bronze/Gold rows and freshness threshold.
2. Checkpoint files exist in both expected checkpoint prefixes.

Manual verifier command:

```bash
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py --min-raw-rows 1 --min-gold-rows 1 --max-freshness-minutes 10
```
