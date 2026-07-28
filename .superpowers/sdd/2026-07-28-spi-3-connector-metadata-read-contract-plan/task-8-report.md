# Task 8 Report

Status: DONE_WITH_CONCERNS

## Changed behavior/files

- Deleted the legacy `scan_planning` module, Iceberg/StarRocks scan planners,
  `TableSource`, their registry maps, and provider-owned `BoundScanRanges`
  variants.
- Routed Iceberg metadata discovery, snapshot selection, split planning, and
  reader opening through the canonical connector SPI instance.
- Added a persistent StarRocks standalone metadata/read planning instance and
  kept execution on the canonical per-BE connector instance.
- Migrated SQL catalog/query preparation, MV analysis/rewrite, query statistics,
  coordinator preparation, and native protocol fixtures off legacy read
  contracts.
- Kept schema scan and file-sidecar range data as explicit core-owned scan
  state; no provider identity or legacy fallback was added.
- Updated live conformance fixtures to register canonical typed connector
  instances, including table-specific split sets for join inputs.

## RED

- Initial `cargo check -p novarocks --all-targets --profile dev-opt`
  - Exit: 101, as expected immediately after deleting the contracts.
  - Compiler reported 10 library errors and 27 test-target errors.
  - The errors identified all remaining planner, SQL catalog/query-prep, and
    fixture consumers; each was migrated without an alias, facade, or fallback.

## GREEN / required gates

- `cargo fmt --all -- --check`: PASS
- `cargo check -p novarocks --all-targets --profile dev-opt`: PASS
- `cargo check -p novarocks-server --all-targets --profile dev-opt`: PASS
- `cargo test -p novarocks-spi --features connector-conformance --profile dev-opt`:
  PASS (6 connector conformance, 8 connector contract, 8 state-store contract)
- `cargo test -p novarocks --lib connector --profile dev-opt`:
  PASS (838 passed, 0 failed, 6761 filtered)

The complete gate sequence was rerun after the final live-test fixture migration.

## One-time completion audit

- Legacy contract audit: no `ScanConnector`, `ConnectorScanPlanner`, connector
  handle `as_any`/`downcast_ref`, or `TableSource` contract remains. Broad
  matches are Arrow/general Rust downcasts, provider-private read configs, and
  the core-owned `BoundScanRanges`.
- SPI dependency audit: no core, catalog, frontend, server, Thrift, Prost,
  Serde, or connector-module Tokio dependency remains. `novarocks-spi` still
  has its pre-existing optional Tokio dependency used only by the
  `state-store-conformance` feature.
- Registry-call audit: no `table_source`, `scan_planner`, or `scan_connector`
  call remains. Remaining `catalog_backend` calls are mutation/DDL/sink/MV
  owners deliberately outside the SPI-3 read migration, plus registry tests
  and declarations; query planning and read metadata discovery use typed SPI
  instances.

## Commit

- Message: `refactor(connector): remove legacy read contracts`

## Remaining concerns

- The audit command's literal `tokio` pattern still reports the existing
  optional state-store conformance dependency in `novarocks-spi/Cargo.toml`.
  This task did not change or broaden it, and connector SPI production code
  does not depend on Tokio.
- Distributed 1FE+3BE acceptance was not part of the Task 8 gate set and was
  not run here.
