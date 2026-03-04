# Documentation Architecture (v2)

This `docs/` tree is the new documentation layout for production-style specs.

Legacy docs remain in `design_doc/` and are intentionally untouched during this phase.

## Goals

1. Keep business decisions, metric contracts, and acceptance criteria separate.
2. Make docs easy to review, version, and audit.
3. Support incremental rollout by milestone.

## Current Scope

Only realtime metrics and decision contracts are included in this phase.

## Folder Structure

1. `architecture/`
   - Long-lived architecture and domain-level design specs.
2. `architecture/realtime-decisioning/`
   - Realtime metric definitions, policy rules, queue contract, reconciliation, and acceptance criteria.

## Ownership

1. Primary owner: Data Engineering (project owner)
2. Change policy: any threshold or rule update must increment `rule_version`.

