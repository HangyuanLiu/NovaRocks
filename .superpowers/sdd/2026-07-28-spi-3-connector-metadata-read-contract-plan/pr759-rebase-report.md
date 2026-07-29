# PR #759 Rebase Report

Status: DONE_WITH_CONCERNS

## Rebase result

- Branch: `codex/spi-3-connector-metadata-read-contract`
- New base: `e4110766cc53199191331a51880b66bd63d27e6c`
  (`feat(query): add explicit query cancellation control plane`, PR #759)
- Rebased implementation HEAD before this report-only commit:
  `85c33b147`
- All 43 SPI-3 commits were replayed and the rebase completed without a
  remaining conflict or design decision.
- The two Task 9 stashes were not applied, dropped, or modified.

## Semantic conflict resolution

- PR #759 remains the owner of request/session query cancellation through
  `QueryCancellationView`, `QueryExecutionService`, and the query-control
  plane.
- SPI-3 keeps `ConnectorRequestContext` explicit. The MySQL request boundary
  adapts the exact PR #759 cancellation view into connector cancellation, then
  carries the same deadline and payload budgets through canonical metadata and
  read calls.
- No connector path consults a hidden thread-local/default request context.
  Explicit background/test owners may still construct their own visible
  operation-scoped context.
- Current `query_execution` ownership was preserved; deleted legacy
  coordinator execution code was not restored.
- Upstream runtime-filter conformance test deletions were preserved.
- MV analysis/refresh, write paths, catalog lifecycle, query preparation, and
  scan preparation retain explicit connector context propagation.

## Conflict and compile cleanup

- Migrated reintroduced test fixtures from removed `TableSource`/legacy
  catalog builder arguments to the canonical connector registry.
- Migrated new fixture access from the removed `mv_repo` field and transactional
  repository calls to the current `mv_repository` service.
- Supplied the required explicit `StandaloneOpenServices` composition in the
  new catalog lifecycle regression.
- Applied `cargo fmt` to the one indentation difference exposed after replay.

## Verification

- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS
- `cargo check -p novarocks --lib --profile dev-opt`: PASS
  (763 existing warnings)
- `cargo test -p novarocks-spi --features connector-conformance --profile dev-opt`:
  PASS (6 connector conformance, 8 connector contract, 8 state-store contract)
- `cargo test -p novarocks --test connector --profile dev-opt`:
  PASS (6 passed, 0 failed)
- Focused core cancellation unit-test compilation was attempted. After all
  branch-added stale calls were migrated, compilation remains blocked by seven
  pre-existing `origin/main` errors: the parent engine tests reference removed
  `IcebergOptimizeJobOutcome`, `CreateIcebergOptimizeJobRequest`, and
  `JobMetaRepository::{create,claim,record,finish}_iceberg_optimize_job`.
  The corresponding test references are unchanged from `origin/main`, and
  `novarocks/core/src/meta/repository/job.rs` is byte-identical to the parent.
  The unrelated deleted optimize-job API was not restored.

## Remaining concerns

- The focused core unit cancellation tests cannot execute until the
  `origin/main` optimize-job test compilation regression is repaired.
- Distributed 1FE+3BE acceptance was not run; that remains Task 9 work.
- The fork tracking branch still points at the pre-rebase history. No push was
  performed as part of this conflict-resolution task.
