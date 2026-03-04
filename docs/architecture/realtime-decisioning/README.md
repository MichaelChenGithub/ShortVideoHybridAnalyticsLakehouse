# Realtime Decisioning (M1)

This module defines the realtime decision system for short-video operations.

## Business Decisions in Scope

1. `BOOST`: high momentum and quality-passing videos.
2. `REVIEW`: high momentum but quality-failing videos.
3. `RESCUE`: high-quality new videos with under-exposure.

## Policy Priority

1. `BOOST`
2. `REVIEW`
3. `RESCUE`
4. `NO_ACTION`

## Rule Baseline

1. `rule_version = rt_rules_v1`
2. Rolling window = 30 minutes
3. Core grain = `video_id + window_start` (1-minute event-time bucket)

## Spec Files

1. `metric-contract.md`
2. `action-queue-contract.md`
3. `reconciliation-and-slo.md`
4. `acceptance-criteria.md`

