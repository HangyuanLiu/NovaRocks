# PBF-3 Task 3 Implementer Report

## Commit boundary

- BASE: `34e65524a24228226ca84da61eebf383874a7e66`
- Final commit: `SELF` (`refactor(exec): model StarRocks fragment sinks`); the exact object ID is returned with this report because a commit cannot embed its own final hash.

## Contract and ownership changes

- Added `FragmentSinkProgram::{SplitDataStream, StarRocksTable}` and the corresponding sink kinds and assignment requirements. Split branch programs/expressions are immutable; destination groups and sender identity remain instance assignments. StarRocks transaction ID, load ID, and frontend endpoint exist only in `StarRocksTableSinkAssignment`.
- Replaced the compat-only `PreparedFragmentSink` execution enum with the official `FragmentSinkSpec + FragmentSinkAssignment` contract. `lower/compat/fragment.rs` now performs one generic runtime materialization/execution path, including `SCHEMA_TABLE_SINK -> Noop`.
- Made `runtime::fragment::sink` the sole construction owner for `SplitDataStreamSinkFactory` and `OlapTableSinkFactory`. The StarRocks combined factory input is assembled there from immutable program metadata plus dynamic instance identity.
- Added dependency-neutral StarRocks schema domain types under `connector/starrocks/schema.rs` and propagated `StarRocksKeysType`, `StarRocksTabletSchema`, and domain `UniqueId` through sink, scan, lake, table, and format state.
- Confined generated schema/identity protobuf conversion to `lake/storage_schema_wire.rs` and storage-RPC identity encoding to `sink/storage_rpc_wire.rs`. Thrift keys type, partial-update mode, and schema conversion remain in the StarRocks protocol decoder/adapter boundary.
- Kept the StarRocks static program and materializer behind the existing `compat` feature so the default build remains protocol-neutral and compilable.

## TDD evidence

The initial contract tests were written before the variants/materializer existed. Both requested no-run gates failed with exit `101` because the new Split/StarRocks variants, assignment, and materializer arms were absent:

```text
cargo test -p novarocks --lib --features compat runtime::fragment::submission --no-run
cargo test -p novarocks --lib --features compat runtime::fragment::sink --no-run
```

The implementation then added focused coverage for:

- Split cardinality and assignment-kind validation at submission composition;
- Split static branch/expression decode versus dynamic destination groups;
- `SCHEMA_TABLE_SINK -> Noop`;
- exhaustive immutable OLAP program destructuring, which fails to compile if transaction/load/frontend state is added to the static program;
- runtime StarRocks factory-input assembly from a separate assignment;
- tablet-schema storage wire round-trip and storage-RPC `UniqueId` encoding.

## GREEN and final verification

Fresh contract gate:

```text
cargo test -p novarocks --lib runtime::fragment::submission --features compat
exit 0; 45 passed, 0 failed, 6919 filtered out
```

The freshly rebuilt compat test binary then ran the remaining focused filters:

```text
runtime::fragment::sink
exit 0; 9 passed, 0 failed

protocol::starrocks::decode::sink
exit 0; 1 passed, 0 failed

lower::compat::fragment::tests
exit 0; 7 passed, 0 failed

storage_
exit 0; 13 passed, 0 failed
```

The `storage_` set includes both target boundary tests:
`tablet_schema_storage_wire_round_trips_domain_schema` and
`unique_id_is_encoded_only_at_storage_rpc_boundary`.

Build and formatting gates:

```text
cargo check -p novarocks --all-targets
exit 0; 39.14s

cargo check -p novarocks --all-targets --features compat
exit 0; 23.38s

cargo fmt --all --check
exit 0

git diff --check
exit 0
```

The default all-target gate initially exposed three missing `compat` guards around the new StarRocks program/materializer references. After applying the existing connector feature boundary to those references, the fresh default and compat gates above both passed. Commands retain the repository's existing warning baseline; unrelated warning cleanup was not attempted.

## Boundary audits

All of the following audits returned no illegal matches:

- `PUniqueId`, generated `KeysType`, or `TabletSchemaPb` in sink/scan/lake/table/format/program/assignment state after excluding the two named wire adapters;
- sink/lake/table/protocol/generated dependencies imported by `connector/starrocks/schema.rs`;
- `SplitDataStreamSinkFactory` or `OlapTableSinkFactory` construction outside `runtime/fragment/sink.rs` and their defining/export modules;
- `PreparedFragmentSink`, or concrete Split/OLAP factory imports, under compat fragment orchestration and StarRocks sink decoders.

The positive generated-type scan found only the two allowed files:

- `connector/starrocks/sink/storage_rpc_wire.rs`;
- `connector/starrocks/lake/storage_schema_wire.rs`.

## Scope confirmation and residual risk

- No SQL suite was run and no SQL golden file was changed.
- No vault roadmap/spec/plan document and no `.superpowers/sdd/progress.md` file was changed.
- Validation covers the static/dynamic fragment contract, decoder/materializer/wire unit surfaces, formatting, ownership shape, and default/compat all-target builds. It does not execute end-to-end FE traffic or a distributed SQL suite.
