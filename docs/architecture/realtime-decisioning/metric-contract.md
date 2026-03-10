# Metric Contract: Realtime Decision Metrics (M1)

## 1. Data Grain

Realtime source grain:

1. `video_id`
2. `window_start` (1-minute event-time bucket)

Contract:

1. `video_id + window_start` must be unique.

## 2. Rolling Window

All decision metrics are computed over rolling 30 minutes.

## 3. Metric Definitions

### 3.1 Viral Velocity

```text
velocity_30m = (likes_30m + 5 * shares_30m) / max(impressions_30m, 100)
```

Notes:

1. `share` has higher weight than `like` by design.
2. denominator floor `100` prevents low-volume score inflation.

### 3.2 Quality Gate

```text
completion_rate_30m = play_finish_30m / max(play_start_30m, 1)
skip_rate_30m       = skips_30m / max(play_start_30m, 1)
```

Gate thresholds:

1. `completion_rate_30m >= 0.55`
2. `skip_rate_30m <= 0.35`
3. `play_start_30m >= 30` (minimum sample condition)

### 3.3 Candidate and Under-exposure

Candidate:

1. `velocity_30m >= p90`
2. `impressions_30m >= 100`

Under-exposed:

1. M1 uses global threshold: `impressions_30m <= global_p40_impressions_threshold`
2. future plan: switch to cohort `category + region` `p40` with global fallback when cohort sample is insufficient

## 4. Decision Mapping

1. `BOOST`: candidate and gate pass
2. `REVIEW`: candidate and gate fail
3. `RESCUE`: not candidate, gate pass, `upload_age <= 60m`, under-exposed
4. `NO_ACTION`: otherwise

## 5. Quantile Governance

1. Quantile baselines are refreshed daily after T+1 completion.
2. No intraday threshold drift in M1.
3. Baseline set is tied to `rule_version`.
4. Baselines are published to `lakehouse.dims.rt_rule_quantile_baselines`.
5. `p90` for candidate evaluation uses global `velocity_30m` distribution.
6. M1 `p40` for under-exposure uses global `impressions_30m` distribution.
7. Future plan (deferred after M1):
   - use cohort `p40` on `impressions_30m` by `category + region`
   - use cohort `p40` when `sample_size >= 200`
   - fallback to global `p40` when `sample_size < 200`
8. Global baseline publish guard:
   - publish only when global `sample_size >= 1000`
9. Baseline rows are immutable after publish for a given `rule_version` and `effective_from`.
10. Any threshold logic change requires a new `rule_version`.
11. Baseline registry physical schema is defined in:
   - `docs/architecture/data-model/m1-data-model-v1.md` (`5.9 lakehouse.dims.rt_rule_quantile_baselines`)
