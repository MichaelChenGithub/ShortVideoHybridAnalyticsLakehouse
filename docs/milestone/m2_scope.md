# Milestone 2 Scope (Current Through 2026-03-28)

## 1. Purpose

Define the single active scope narrative through M2 end date (`2026-03-28`).

This is the authoritative current-scope document for planning and documentation alignment.

## 2. In Scope (M2)

1. Batch metrics expansion:
   - retention metrics
   - engagement metrics
   - sessionization metrics
2. Semantic + dbt quality expansion:
   - semantic serving layer refinement for analytics usage
   - dbt model organization and quality checks
3. Cloud deployment + scale benchmark:
   - cloud baseline stack fixed to AWS:
     - `MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`
   - benchmark-oriented evidence for data volume, throughput, and freshness behavior

## 3. Out of Scope (M2, Deferred)

1. T+1 reconciliation implementation (deferred to M3).
2. Operational action queue execution and consumer automation (deferred to M3).
3. Automated degraded-mode switching and automated rollout blocking workflow (deferred to M3).
4. Broader optimization-only initiatives not required for feature delivery narrative.

## 4. M2 Definition of Done

1. Batch metrics scope is documented and implemented with clear serving intent.
2. Semantic and dbt quality scope is documented with testability expectations.
3. Cloud deployment and scale benchmark scope is documented with measurable evidence targets.
4. Cross-document wording is aligned to this through-M2 scope without conflicting milestone claims.
5. Deferred/conflicting items are centralized under `docs/milestone/m3_scope.md`.

## 5. Related Documents

1. `docs/milestone/m1_scope.md`
2. `docs/milestone/m3_scope.md`
3. `README.md`
4. `docs/README.md`

