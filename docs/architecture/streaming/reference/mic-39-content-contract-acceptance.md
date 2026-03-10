# MIC-39 Content Contract Enforcement Acceptance Reference

## Scope and Boundaries

In scope:
1. `content_events` schema contract enforcement.
2. Invalid routing to `lakehouse.bronze.invalid_events_content`.
3. Continued valid flow for `lakehouse.bronze.raw_events` and `lakehouse.gold.rt_video_stats_1min`.

Out of scope:
1. CDC contract enforcement for `cdc.content.videos` (`MIC-43`).

Runtime defaults (MIC-39):
1. `startingOffsets=latest`
2. trigger intervals: `raw_events=10 seconds`, `rt_video_stats_1min=1 minute`, `invalid_events_content=10 seconds`
3. watermark policy for content aggregation: baseline `2 minutes`, lag-prone environments `5 minutes`
4. checkpoint roots:
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1`
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1`
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1`

## One-Command Entrypoint

```bash
bash src/scripts/run_mic39_acceptance.sh
```

## Required Environment Variables and Defaults

Defaults enforced by verifier flow:
1. `MIN_RAW_ROWS=1`
2. `MIN_GOLD_ROWS=1`
3. `MIN_INVALID_ROWS=1`
4. `MAX_INVALID_RATE=0.20`
5. `MAX_FRESHNESS_MINUTES=10`

Manual verifier boundary variable:
1. `MIN_INGESTED_AT_MS=$(( $(date +%s) * 1000 ))`

## Artifacts and Output Interpretation

Pass criteria:
1. Verifier returns success for raw/gold minimums, invalid minimums, invalid rate threshold, and freshness threshold.
2. Checkpoint evidence exists for all three sinks.

Manual verifier command:

```bash
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_contract_enforcement.py \
  --min-raw-rows 1 \
  --min-gold-rows 1 \
  --min-invalid-rows 1 \
  --max-invalid-rate 0.20 \
  --max-freshness-minutes 10 \
  --min-ingested-at-ms "$MIN_INGESTED_AT_MS"
```
