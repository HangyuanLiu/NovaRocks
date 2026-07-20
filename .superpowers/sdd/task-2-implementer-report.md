# PBF-3 Task 2 Implementer Report

## Commit boundary

- BASE: `b2f9ffc4713159ccccf7bd15d9b84d86cd68bcf6`
- Final commit: `SELF` (`refactor(protocol): move StarRocks plan decoding`); the exact object ID is returned with this report because a commit cannot embed its own final hash.

## RED evidence

The owner-boundary test was added before moving the decoder modules:

```text
cargo test -p novarocks --lib protocol::starrocks::decode --features compat --no-run
exit 101
E0433: could not find `expr` in `decode`
E0433: could not find `layout` in `decode`
E0433: could not find `node` in `decode`
E0433: could not find `descriptor` in `decode`
```

This was the expected RED: expression, layout, node, and descriptor decode ownership still lived under lower/runtime rather than the StarRocks protocol boundary.

The review follow-up also used RED-first fixtures for the three requested fixes:

```text
cargo test -p novarocks --lib exec::chunk::tests -- --list
exit 0; 0 tests (unexpected)

cargo test -p novarocks --lib exec::chunk::tests --features compat -- --list
exit 0; 2 tests

cargo test -p novarocks --lib lower::compat::fragment::tests --features compat
exit 101; typed FragmentDecodeAttempt/process helpers were absent
exit 101; compat SchemaScanOp expected TNetworkAddress instead of RuntimeEndpoint
```

These failures established that default chunk tests were hidden, dependency completion had no typed state machine/full sink attempt, and Schema scan execution storage remained feature-dependent.

## Ownership migration

- Physically moved StarRocks expression, layout, node/scan, sink, type-lowering, runtime-filter-pushdown, descriptor, and chunk-schema conversions under `protocol/starrocks/decode/**`.
- Removed the corresponding `lower::compat` module facade; `lower::compat` now owns only fragment execution orchestration.
- Replaced nested `TPlanFragmentExecParams`, `TQueryOptions`, and raw FE-address threading with a decoder-local context containing domain query options, IDs, scan assignments, exchange sender maps, and a decoded frontend endpoint.
- Converted fetch node endpoint metadata to `LookupNodesInfo`, fragment FE metadata to `RuntimeEndpoint`, and lake scan schema metadata to a domain endpoint. The named table-schema transport adapter performs the final `TNetworkAddress` conversion.
- Moved data-stream partition wire conversion out of the execution operator and added the explicit `decode_expression_for_layout` boundary used by lake schema change.
- `SchemaScanOp` now stores `RuntimeEndpoint` in every feature build. Its named connector transport adapter creates a short-lived `TNetworkAddress` immediately before FE RPC dispatch; the nested decoder no longer constructs or stores a Thrift endpoint.

## External facts and I/O boundaries

- `get_query_profile` expression decode now declares a stable query-profile dependency. It does not import or call `fe_report`; fragment orchestration resolves the dependency and re-runs pure decode.
- Lake-meta decode now declares a stable `LakeMetaStorageRequest` and consumes materialized `LakeMetaStorageFacts`. Tablet-registry, object-store, snapshot, footer, and segment-page I/O lives in `connector/starrocks/lake_meta_storage.rs`, outside the decoder.
- Iceberg position-delete manifest index materialization now uses `DeferredPositionDeleteDataFilePartitionIndex`; decoder-time manifest I/O was deleted.
- Exchange sender fallback consumes only the explicit submission/batch sender-count maps and no longer queries `QueryContextManager`.
- Dependency discovery now has explicit `Ready`, `Pending`, and `DecodeError` outcomes. Decode errors are returned unchanged without invoking resolvers; a non-empty requirement set can never become executable, and each resolution round must complete every declared requirement.
- The complete attempt covers node and sink lowering. A private `PreparedFragmentSink` is produced by the sole wire sink dispatch; pending draft arenas/prepared values are discarded, while a ready prepared sink is consumed directly by execution without reading `TDataSink` again.
- Unresolved query-profile and lake-meta lookups return type-correct discovery placeholders only inside a draft. The requirement set prevents those placeholder-bearing values from reaching execution.

## Descriptor isolation

- Moved `descriptor_snapshot_from_thrift` into the StarRocks decoder while retaining the raw descriptor cache as the documented Task 5 exception.
- Added per-submission `IcebergTableLocationMap` derivation and threaded it into HDFS decode/config.
- Deleted the process-global Iceberg table-location cache and removed all three fragment-ingress writes, preventing same-table-ID cross-query pollution.

## GREEN and verification evidence

```text
cargo test -p novarocks --lib protocol::starrocks::decode --features compat -- --test-threads=1
exit 0; 6 passed, 0 failed, 6920 filtered out

cargo test -p novarocks --lib lower::compat::fragment::tests --features compat
exit 0; 5 passed, 0 failed, 6921 filtered out

cargo test -p novarocks --lib exec::chunk::tests
exit 0; 2 passed, 0 failed

cargo test -p novarocks --lib exec::chunk::tests --features compat
exit 0; 2 passed, 0 failed

cargo test -p novarocks --lib connector::schema::op::tests
exit 0; 3 passed, 0 failed

cargo test -p novarocks --lib connector::schema::op::tests --features compat
exit 0; 2 passed, 0 failed (the third default test is intentionally cfg(not(feature = "compat")))

cargo test -p novarocks --lib connector::starrocks::lake::schema_change --features compat
exit 0; compile gate passed, 0 failed

cargo test -p novarocks --lib --features compat per_submission_iceberg_table_locations_are_isolated
exit 0; 1 passed, 0 failed, 6921 filtered out

cargo check -p novarocks --all-targets --features compat
exit 0

cargo check -p novarocks --all-targets
exit 0

cargo fmt --all -- --check
exit 0

git diff --check
exit 0
```

Ownership scans also returned no matches for:

- old `lower::compat::{expr,node,sink,layout,type_lowering,runtime_filter_pushdown}` imports/facades;
- `descriptor_snapshot_thrift` or `schema_thrift` modules;
- global Iceberg table-location cache APIs;
- `fe_report`, frontend RPC, query-context lookup, tablet resolution, or storage-read calls inside `protocol/starrocks/decode`;
- `TNetworkAddress` inside `protocol/starrocks/decode/node/schema_scan.rs`;
- a second `match sink.type_` or post-ready `TDataSink` payload read in fragment execution.

The commands emit the repository's existing warning baseline; unrelated warning cleanup was not attempted.

## Scope and remaining risk

- No SQL suites were run and no SQL golden files were changed.
- No vault design/roadmap/spec/plan file and no `.superpowers/sdd/progress.md` file was changed.
- The raw descriptor cache/resolver remains intentionally in runtime until Task 5, as required by the plan.
- Validation covers focused decoder/domain tests, schema-change compilation, per-submission descriptor isolation, and both default/compat all-target builds; it does not execute end-to-end FE traffic.
