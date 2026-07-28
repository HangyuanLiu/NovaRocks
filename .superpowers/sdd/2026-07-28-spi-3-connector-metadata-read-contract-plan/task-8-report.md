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

## Code-review fix follow-up

### Changed files/behavior

- Fixed the compat-only StarRocks connector descriptor serialization and
  supplied the complete `ConnectorSplitPlanningRequest` contract in
  `connector/scan_model/starrocks.rs` and
  `connector/starrocks/table/scan_adapter.rs`.
- Made Iceberg metadata load return one provider-owned opaque descriptor that
  contains the schema/read information needed by core. SQL catalog, analyzer,
  query prep, coordinator split planning, metadata-table planning, and MV
  callers now consume that SPI result without a second registry lookup.
- Removed the Iceberg registry field from `sql/catalog/iceberg.rs`, removed the
  registry argument from `build_iceberg_catalog`, and deleted the exported
  `resolve_iceberg_*` facade helpers.
- Removed namespace/table/schema/view read methods from `CatalogBackend`.
  Namespace/table/schema consumers now call `ConnectorMetadata`; Iceberg view
  reads use the existing provider-owned view service. `CatalogBackend` retains
  only mutation/admin operations.
- Added query-derived `ConnectorRequestContext` construction with client
  disconnect cancellation and query-option deadlines. Statement/query,
  catalog-provider, coordinator, Iceberg, and StarRocks planning callers pass
  this real context; the production `NeverCancelled` plus fixed 60-second
  contexts were removed.
- Updated all affected engine write/mutation flows, schema evolution,
  maintenance, view/MV code, coordinator tests, and backend fixtures to use the
  canonical metadata path.

### Additional RED evidence

- `cargo check -p novarocks --features compat --lib --profile dev-opt`
  initially exited 101 with five errors: four missing Serde implementations
  for `StarRocksScanSourceDescriptor` and one incomplete
  `ConnectorSplitPlanningRequest`.
- After deleting the remaining `CatalogBackend` read surface,
  `cargo check -p novarocks --lib --profile dev-opt` exposed 23 production
  consumer errors. The subsequent all-target check exposed 17 stale
  test/fixture consumers. All were migrated without restoring a read facade.
- The first opaque Iceberg descriptor compile exposed six errors because
  `iceberg::spec::Literal` is not Serde-enabled. Literal fields are excluded
  from the transport representation while their already canonical
  type-aware JSON defaults remain preserved.

### Final GREEN evidence

- `cargo fmt --all -- --check`: PASS
- `cargo check -p novarocks --all-targets --profile dev-opt`: PASS
- `cargo check -p novarocks --features compat --lib --profile dev-opt`: PASS
- `cargo check -p novarocks-server --all-targets --profile dev-opt`: PASS
- `cargo test -p novarocks-spi --features connector-conformance --profile dev-opt`:
  PASS (6 connector conformance, 8 connector contract, 8 state-store contract)
- `cargo test -p novarocks --lib connector --profile dev-opt`:
  PASS (838 passed, 0 failed, 6761 filtered)
- `git diff --check`: PASS

### Follow-up audit

- No `resolve_iceberg_*` metadata/read facade remains.
- No Iceberg SQL catalog or catalog-service provider retains an Iceberg
  registry field or performs a second registry-based table resolution.
- Remaining `catalog_backend` calls are mutation/admin operations, registry
  declarations, or registry tests.
- No `NeverCancelled` or fixed 60-second production request context remains in
  the Iceberg or StarRocks metadata/read adapters.
