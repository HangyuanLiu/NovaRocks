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

## Code-review fix follow-up 2

### Changed files/behavior

- Replaced the thread-local `QueryConnectorCancellation` adapter with an
  `Arc<AtomicBool>`-backed request cancellation adapter. The MySQL request
  boundary constructs one `ConnectorRequestContext` from the request's real
  disconnect/timeout signal and query-option deadline.
- Made coordinator fragment and scan preparation require that context
  explicitly. SELECT, EXPLAIN ANALYZE, and time-travel query preparation carry
  the same request context into metadata, `begin_scan`, and `plan_splits`.
- Direct session APIs, catalog trait callbacks, and independently initiated
  MV/maintenance/write operations now construct an explicit operation-scoped
  context at their visible owner boundary. No connector helper consults a
  thread-local/global cancellation source or silently manufactures a default
  context.
- Reduced Iceberg `SplitPayload` to the split-owned namespace/table identity,
  pinned snapshot ID, data file, projection, and limit. Serialized table
  metadata remains in the scan handle once and is no longer cloned into every
  split. Reader open reloads the provider-owned table and rejects an expired
  pinned snapshot.
- Statistics capability work remains owned by SPI-4 and was not changed.

### RED evidence

- `split_payload_does_not_repeat_serialized_table_metadata` initially failed
  with `split payload repeated table metadata: 262899 bytes per split` after a
  256 KiB serialized metadata fixture exposed metadata-size × split-count
  amplification.
- `scan_preparation_propagates_caller_cancellation` initially failed to compile
  with `E0061`: `prepare_scan_bindings` accepted only three arguments and had
  no way to receive the caller's context. Removing the old helper also exposed
  every remaining implicit context caller at compile time.

### Final GREEN evidence

- `cargo fmt --all -- --check`: PASS
- `cargo check -p novarocks --all-targets --profile dev-opt`: PASS
- `cargo check -p novarocks --features compat --lib --profile dev-opt`: PASS
- `cargo check -p novarocks-server --all-targets --profile dev-opt`: PASS
- `cargo test -p novarocks-spi --features connector-conformance --profile dev-opt`:
  PASS (6 connector conformance, 8 connector contract, 8 state-store contract)
- `cargo test -p novarocks --lib connector --profile dev-opt`:
  PASS (839 passed, 0 failed, 6762 filtered)
- `cargo test -p novarocks --lib coordinator::prepare::scan_preparation::tests::dispatch::scan_preparation_propagates_caller_cancellation --profile dev-opt -- --exact --nocapture`:
  PASS (1 passed, 0 failed)
- `cargo test -p novarocks --lib connector::iceberg::provider::tests::split_payload_does_not_repeat_serialized_table_metadata --profile dev-opt -- --exact --nocapture`:
  PASS (1 passed, 0 failed)
- `git diff --check`: PASS

### Follow-up audit

- No `query_request_context`, `QueryConnectorCancellation`, or connector-side
  `client_disconnected()` lookup remains.
- `prepare_fragments` and `prepare_scan_bindings` cannot be called without an
  explicit `ConnectorRequestContext`.
- The split regression proves that a 256 KiB table metadata document does not
  appear in a split payload and that 512 ordinary split payloads remain within
  the SPI total-payload budget.
- No statistics source file changed in this follow-up.

### Commit

- Message: `fix(connector): propagate request context and bound splits`

## Code-review fix follow-up 3

### Changed files/behavior

- Kept the MySQL request's `ConnectorRequestContext` intact after
  `execute_in_context_with_connector_context`. Custom statement dispatch,
  CREATE/CREATE LIKE/DROP/TRUNCATE metadata checks, INSERT/CTAS,
  DELETE/UPDATE/MERGE/equality-delete planning, and their coordinated write
  scans now receive the caller's cancellation signal, deadline, and payload
  budgets explicitly.
- Added context-taking query and Iceberg write helpers for request-owned work.
  The existing context-owning wrappers remain only for visible direct or
  background operation owners with no client request.
- Reworked Iceberg split construction into an incremental admission loop.
  Each candidate payload is bounded by the per-handle limit, aggregate bytes
  are checked with `checked_add`, and only an admitted candidate is converted
  into and pushed as a `ConnectorSplit`. Thus rejection retains at most the
  already-admitted bounded vector plus one bounded candidate payload.
- Restored pinned-snapshot membership validation in `open_reader`. The
  provider reloads the pinned snapshot manifest and requires the split's file
  path, size, and row count to match a live snapshot file before any reader is
  opened. An expired snapshot remains `NotFound`; a mismatched split is
  `CorruptData`.
- Statistics capability work remains owned by SPI-4 and was not changed.

### RED evidence

- `cargo test -p novarocks --lib
  mysql_request_cancellation_reaches_insert_metadata_lookup --profile dev-opt
  -- --nocapture` failed against the round-2 implementation because the
  cancelled caller context was replaced inside INSERT:
  `cancelled MySQL request must abort INSERT metadata lookup: Ok`.
- Code review found that aggregate budget admission occurred only after
  collecting the complete split vector. The new boundary regression directly
  observes the production admission helper and requires the output vector and
  consumed-byte counter to remain unchanged on the rejected candidate.
- Code review found that `open_reader` accepted any file when the referenced
  snapshot still existed. The integration regression mutates a valid planned
  split to a path outside that snapshot and requires a `CorruptData` failure
  before reader construction.

### Focused GREEN evidence

- `cargo test -p novarocks --lib
  mysql_request_cancellation_reaches_insert_metadata_lookup --profile dev-opt
  -- --nocapture`: PASS (1 passed, 0 failed, 7604 filtered).
- `cargo test -p novarocks --lib
  custom_statement_dispatch_honors_caller_cancellation --profile dev-opt --
  --nocapture`: PASS (1 passed, 0 failed, 7604 filtered).
- `cargo test -p novarocks --lib
  aggregate_budget_rejects_candidate_before_split_is_pushed --profile dev-opt
  -- --nocapture`: PASS (1 passed, 0 failed, 7604 filtered). The regression
  asserts `ConnectorErrorKind::ResourceExhausted`, one previously admitted
  split, and an unchanged aggregate-byte counter.
- `cargo test -p novarocks --lib
  plan_splits_enforces_aggregate_budget_incrementally --profile dev-opt --
  --nocapture`: PASS (1 passed, 0 failed, 7604 filtered). The full provider
  entrypoint returns `ConnectorErrorKind::ResourceExhausted`.
- `cargo test -p novarocks --lib
  iceberg_instance_resolves_metadata_and_plans_a_snapshot_split --profile
  dev-opt -- --nocapture`: PASS (1 passed, 0 failed, 7604 filtered). A mutated
  split returns `ConnectorErrorKind::CorruptData` with `does not belong`; the
  original split still opens and reads one row.

### Final GREEN evidence

- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS
- `cargo check -p novarocks --all-targets --profile dev-opt`: PASS
- `cargo check -p novarocks --features compat --lib --profile dev-opt`: PASS
- `cargo check -p novarocks-server --all-targets --profile dev-opt`: PASS
- `cargo test -p novarocks-spi --features connector-conformance --profile
  dev-opt`: PASS (6 connector conformance, 8 connector contract, 8 state-store
  contract)
- `cargo test -p novarocks --lib connector --profile dev-opt`: PASS
  (841 passed, 0 failed, 6764 filtered)

### Follow-up audit

- `statement.rs`, `insert_flow.rs`, `delete_flow.rs`, `mutation_flow.rs`,
  `equality_delete_flow.rs`, `iceberg_ctas.rs`, and `iceberg_writer.rs` contain
  no `connector_request_context(...)` construction. Request-owned DML and
  coordinated write scans use the context supplied by the session boundary.
- Request-owned DML modules contain no call to the context-owning
  `execute_query_as_iceberg_write` wrapper; they call the explicit
  `execute_query_as_iceberg_write_with_connector_context` path.
- Aggregate split accounting happens before `ConnectorSplit::try_new` and
  `Vec::push`; the post-collection total-budget check is gone.
- Pinned-snapshot reader validation checks file path, size, and row count and
  reports corrupt split identity as `ConnectorErrorKind::CorruptData`.
- No statistics source file or Task 9 artifact changed in this follow-up.

### Planned commit

- Message: `fix(connector): enforce request and split integrity`

## Code-review fix follow-up 4

### Changed files/behavior

- Preserved the caller's `ConnectorRequestContext` through custom
  materialized-view and Iceberg-ref dispatch. Iceberg and StarRocks MV
  analysis, pinned reads, delta reads, change-stream preparation, and refresh
  lifecycle execution now reuse that context instead of creating a fresh
  cancellation flag. The background scheduler constructs a context only at
  its own operation-owner boundary.
- Added cancellation/deadline validation after custom dispatch entry and at
  the Iceberg-ref metadata/mutation boundaries, so cancellation that arrives
  after the initial session precheck still aborts before mutation.
- Replaced per-open pinned-manifest walks with a provider-owned bounded cache
  of compact snapshot membership identities `(path, size, row_count)`.
  `plan_splits` seeds the cache; a cache miss loads the pinned snapshot once;
  repeated `open_reader` calls validate in memory. The cache has a fixed
  64-snapshot capacity, LRU eviction, single initialization per resident key,
  and failed loads are removed so later requests can retry.
- Expired snapshots remain `NotFound`, while stale or forged split identities
  remain `CorruptData`. Split payloads still do not carry serialized table
  metadata. Statistics capability work remains owned by Task 9 and was not
  changed.

### RED evidence

- `materialized_view_dispatch_observes_cancellation_after_entry` initially
  failed because dispatch continued to MV lookup and returned
  `materialized view does not exist: ice.analytics.orders_mv` instead of
  `connector request was cancelled`.
- `iceberg_ref_dispatch_observes_cancellation_after_entry` initially failed
  because dispatch continued to catalog lookup and returned
  `unknown catalog: ice` instead of `connector request was cancelled`.
- `iceberg_instance_resolves_metadata_and_plans_a_snapshot_split` initially
  failed after the planned snapshot manifest files were removed: repeated
  `open_reader` returned `NotFound`, proving it rewalked the manifest rather
  than using provider-owned snapshot membership.

### Focused GREEN evidence

- `cargo test -p novarocks --lib
  materialized_view_dispatch_observes_cancellation_after_entry --profile
  dev-opt -- --nocapture`: PASS (1 passed, 0 failed).
- `cargo test -p novarocks --lib
  iceberg_ref_dispatch_observes_cancellation_after_entry --profile dev-opt --
  --nocapture`: PASS (1 passed, 0 failed).
- `cargo test -p novarocks --lib
  iceberg_instance_resolves_metadata_and_plans_a_snapshot_split --profile
  dev-opt -- --nocapture`: PASS (1 passed, 0 failed). After planning, all
  manifest files are removed; the original split still opens and reads twice,
  while a corrupted split remains `ConnectorErrorKind::CorruptData`.
- `cargo test -p novarocks --lib
  snapshot_membership_cache_is_bounded_and_reloads_evicted_snapshot --profile
  dev-opt -- --nocapture`: PASS (1 passed, 0 failed). A capacity-one cache
  reuses the resident membership, evicts it when another snapshot enters, and
  reloads it on the next access while retaining only one entry.

### Final GREEN evidence

- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS
- `cargo check -p novarocks --all-targets --profile dev-opt`: PASS
- `cargo check -p novarocks --features compat --lib --profile dev-opt`: PASS
- `cargo check -p novarocks-server --all-targets --profile dev-opt`: PASS
- `cargo test -p novarocks-spi --features connector-conformance --profile
  dev-opt`: PASS (6 connector conformance, 8 connector contract, 8 state-store
  contract)
- `cargo test -p novarocks --lib connector --profile dev-opt`: PASS
  (842 passed, 0 failed, 6766 filtered)

### Follow-up audit

- Request-owned MV/ref paths contain no fresh `AtomicBool(false)` cancellation
  source. Explicit contexts reach both connector metadata calls and distributed
  scan preparation. Test-only wrappers use test contexts, and the scheduler is
  the sole background refresh operation owner.
- The snapshot cache stores only compact file identities, is fixed-capacity,
  and does not copy `TablePayload` or serialized metadata into each split.
- The cache preserves fail-fast identity validation and retries failed
  membership loads instead of retaining transient errors.
- No statistics source file or Task 9 artifact changed in this follow-up.

### Planned commit

- Message: `fix(connector): preserve refresh context and cache split validation`
