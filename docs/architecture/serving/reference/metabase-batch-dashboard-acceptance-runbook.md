# Metabase Batch Dashboard Acceptance Runbook

## 1. Purpose and Scope

This runbook defines how to use the Metabase batch dashboard SQL pack for:

1. analyst-facing batch dashboard panels
2. BI/QA acceptance checks
3. M2 sign-off evidence collection

In scope:

1. serving-view-only BI consumption for batch analytics
2. retention, engagement, and sessionization panel coverage
3. contract-mapped acceptance evidence for ET `D-1` publish slice

Out of scope:

1. realtime dashboard panels and realtime serving checks
2. batch formula redesign in BI SQL
3. batch orchestration execution itself

## 2. Canonical SQL Pack

Source of truth:

1. `src/metabase/batch-metrics-sql-pack.sql`

Serving views used by this pack:

1. `lakehouse.serving.v_bt_retention_daily`
2. `lakehouse.serving.v_bt_engagement_daily`
3. `lakehouse.serving.v_bt_sessionization_daily`

Default date context:

1. ET `D-1` using:
   - `date_add('day', -1, CAST(current_timestamp AT TIME ZONE 'America/New_York' AS date))`

## 3. Panel-to-Query Mapping (Section A)

1. `A1` retention trend (`D1` vs `D7`):
   - line chart grouped by `day_n` over `cohort_date`
   - metrics: `avg_retention_rate`, `cohort_users`, `retained_users`
2. `A2` engagement KPI summary by category:
   - bar/table panel
   - metrics: impressions, starts, finishes, likes, shares, skips, and governed rates
3. `A3` sessionization summary by region:
   - bar/table panel
   - metrics: sessions, sessions-per-user, avg duration, events/session, watch time/session
4. `A4` segment deep-dive table:
   - table panel keyed by `data_date + category + region + new_vs_returning_user`
   - joins engagement and sessionization semantic outputs for same ET `D-1` slice

## 4. Acceptance Query Mapping (Section B)

1. `B1` retention grain uniqueness
2. `B2` engagement grain uniqueness
3. `B3` sessionization grain uniqueness
4. `B4` retention domain constraints (`day_n` domain and segment domain)
5. `B5` segment-domain integrity across all three serving views
6. `B6` required-field null-rate snapshot by view
7. `B7` publish-date and freshness snapshot
8. `B8` serving-readiness evidence (`non-empty views`)

## 5. Contract Clause Traceability

Primary contract references:

1. `docs/architecture/serving/trino-batch-semantic-serving-contract.md`
2. `docs/architecture/batch-analytics/batch-metrics-contract.md`
3. `docs/milestone/batch-platform-acceptance.md`

Clause mapping:

1. BI must read serving views and avoid formula re-implementation:
   - serving contract sections `6` and `8`
   - evidence: Section A queries read only `v_bt_*` semantic fields
2. key uniqueness and required-field expectations:
   - serving contract section `7`
   - evidence: `B1`-`B3` uniqueness + `B6` null-rate checks
3. domain constraints (`day_n`, segment values):
   - serving contract sections `5.1`, `7.1`, and batch acceptance section `2`
   - evidence: `B4` and `B5`
4. serving/readiness evidence for ET `D-1` outputs:
   - batch acceptance section `3`
   - evidence: `B7` and `B8`

## 6. Execution Checklist

1. Open `src/metabase/batch-metrics-sql-pack.sql`.
2. Create or refresh Metabase questions for Section A queries.
3. Build/update a Metabase dashboard from those Section A cards.
4. Run Section B queries and capture result tables.
5. Confirm expected acceptance outcomes:
   - `B1`, `B2`, `B3` duplicate groups = `0`
   - `B4` invalid domain counters = `0`
   - `B5` invalid segment counters = `0`
   - `B8` publish_ready = `true`
6. Record `B6` and `B7` outputs as observability context.

## 7. M2 Sign-off Evidence Package

Default artifact folder:

1. `artifacts/metabase_batch_dashboard_signoff/<RUN_ID>/`

Required artifacts:

1. screenshot: `A1` panel
2. screenshot: `A2` panel
3. screenshot: `A3` panel
4. screenshot: `A4` panel
5. query output: `B1`
6. query output: `B2`
7. query output: `B3`
8. query output: `B4`
9. query output: `B5`
10. query output: `B6`
11. query output: `B7`
12. query output: `B8`
13. summary note linking outputs to section-5 contract mappings

## 8. Notes

1. Section B queries are QA/sign-off checks; they are not operator business panels.
2. If a query returns zero target rows for ET `D-1`, treat as acceptance failure for readiness sign-off.
