# PBF-3 Task 1 Implementer Report

## Commit boundary

- BASE: `3b57a3528b6844e9c618046d8ecf63fe7169e794`
- Final commit: `SELF` (`refactor(protocol): isolate StarRocks wire normalization`); the exact object ID is returned with this report because a commit cannot embed its own final hash.

## Changed files

- Added `novarocks/core/src/protocol/starrocks/mod.rs`.
- Added the StarRocks compatibility modules under `novarocks/core/src/protocol/starrocks/compat/`:
  `endpoint.rs`, `options.rs`, `request.rs`, `sink.rs`, and `mod.rs`.
- Added the StarRocks decoding modules under `novarocks/core/src/protocol/starrocks/decode/`:
  `endpoint.rs`, `error.rs`, `options.rs`, `runtime_filter.rs`, and `mod.rs`.
- Registered the compat-gated boundary in `novarocks/core/src/protocol/mod.rs`.
- Removed Thrift conversion ownership from:
  `runtime/endpoint.rs`, `runtime/query_options.rs`, and `runtime/runtime_filter_params.rs`.
- Migrated direct callers in:
  `lower/compat/fragment.rs`, `lower/compat/node/hdfs_scan.rs`,
  `lower/compat/sink/starrocks.rs`, `service/internal_service.rs`, and
  `connector/starrocks/sink/frontend_wire.rs`.

## RED evidence

Primary interface RED:

```text
cargo test -p novarocks --lib protocol::starrocks --features compat --no-run
exit 101
E0433: could not find `compat` in `super`
E0432: could not find `decode` in `super`
```

Focused error-path RED added during implementation:

```text
cargo test -p novarocks --lib protocol::starrocks::tests::legacy_endpoint_validation_reports_the_legacy_field_path --features compat -- --exact
exit 101
left:  exec_plan_fragment.params.destinations[0].brpc_server.port
right: exec_plan_fragment.params.destinations[0].deprecated_server.port
```

The decoder was then changed to retain which endpoint field supplied the selected address.

Review-fix RED for nested sink destination paths:

```text
cargo test -p novarocks --lib lower::compat::fragment::tests --features compat -- --test-threads=1
exit 101; 0 passed, 2 failed
multicast actual: exec_plan_fragment.params.destinations[0].brpc_server
multicast expected: exec_plan_fragment.fragment.output_sink.multi_cast_stream_sink.destinations[1][0].brpc_server
router actual: exec_plan_fragment.params.destinations[0].brpc_server
router expected: exec_plan_fragment.fragment.output_sink.iceberg_change_stream_router_sink.branches[1].destinations[0].brpc_server
```

The shared destination decoder now receives the destination-list base path from its caller.
Multicast, split, and router callers enumerate their branch and pass the precise fragment-owned
path; the top-level data-stream caller continues to pass the instance-assignment path under
`exec_plan_fragment.params`.

## GREEN and verification evidence

```text
cargo fmt --all -- --check
exit 0

cargo test -p novarocks --lib protocol::starrocks --features compat -- --test-threads=1
exit 0; 7 passed, 0 failed, 6909 filtered out

cargo test -p novarocks --lib lower::compat::fragment::tests --features compat -- --test-threads=1
exit 0; 2 passed, 0 failed, 6914 filtered out

! rg -n "from_thrift|TQueryOptions|TRuntimeFilterParams|TNetworkAddress" novarocks/core/src/runtime/{query_options.rs,runtime_filter_params.rs,endpoint.rs}
exit 0; no matches

cargo check -p novarocks --all-targets --features compat
exit 0

cargo check -p novarocks --all-targets
exit 0

git diff --check
exit 0
```

The test and check commands emit the repository's existing warning baseline; no unrelated warning cleanup was attempted.

## Design tradeoffs

- Runtime structs remain protocol-neutral. All Thrift field selection, alias handling, validation paths, and conversion errors now live under `protocol::starrocks`.
- Compatibility normalization implements absence-only fallback: current fields always win, including valid values that differ from legacy defaults. It does not introduce conflict rejection.
- Per-driver scan ranges replace only missing or placeholder node-level ranges and preserve concrete node-level ranges.
- Partition boundaries use a generic absence-selection helper so both StarRocks sink call sites share the same current-versus-legacy rule.
- Decode failures carry the `StarRocks` protocol family plus a precise field path; dependency-contract error plumbing is typed but intentionally left for the later dependency-wiring task.

## Remaining risks and scope confirmation

- Verification covers the focused protocol and nested-path unit surfaces plus default and compat all-target builds. It does not execute SQL suites or end-to-end FE traffic.
- No SQL golden files were changed.
- No roadmap, spec, plan, or `.superpowers/sdd/progress.md` file was changed.
- Task 2+ scanner, allowlist, facade, lowering-restoration, and dependency-wiring work was not implemented.
