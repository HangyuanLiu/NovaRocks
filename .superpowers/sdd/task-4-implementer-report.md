# Task 4 Implementer Report

## Scope

- Base: `98198e142443f5b1d2b8c1bf17e3c8c000e7688d`
- Task: pure, validated StarRocks fragment submission decoder
- Commit message: `feat(protocol): decode StarRocks fragment submissions`

## RED evidence

Command:

```text
cargo test -p novarocks --lib protocol::starrocks::decode::submission --features compat --no-run
```

Result: exit 101. The compile-use test failed on the intentionally missing Task 4 surface:

- `DecodedStarRocksFragment`
- `StarRocksDecodeInput`
- `StarRocksFragmentDraft`
- `StarRocksResolvedDependencies`
- `StarRocksSubmissionMetadata`
- `prepare_fragment_submission`
- `finish_fragment_submission`

## Implementation

- Added an instance-first decoder for IDs, backend identity, query options, pipeline DOP, runtime-filter parameters, exchange sender counts, destinations, sender ID, and endpoints.
- Added pure two-phase `prepare_fragment_submission` / `finish_fragment_submission` APIs.
- Prepare now retains a decoder-private program draft; finish only validates and injects typed query-profile/lake-meta resolutions, patches pending slots, and calls the sole `FragmentSubmission::try_new` exit. It never re-decodes the raw fragment tree.
- Added exact external-dependency validation with stable missing, extra, and wrong-kind `DependencyContract` errors.
- Moved fragment sink, result sink, statistic sink, split sink, multi-cast sink, Iceberg router, and StarRocks sink assembly to the protocol boundary; removed the duplicate semantic owner from `lower::compat::fragment`.
- Constructed real typed scan contracts and assignments for FILE, HDFS, LAKE, and supported SCHEMA leaves. FILE/HDFS/LAKE/LAKE_META/SCHEMA plan leaves consume the same normalized domain assignments/facts; raw range DTOs remain only in the ingress adapter.
- Snapshot immutable FILE_STREAM paths, path rewrite, datacache capability, JDBC fallback fields, and object-store retry/timeout defaults in the legacy bridge. The decoder does not read app config, registries, or cache singletons.
- Threaded typed `FieldPath` through plan-node, expression, sink, and scan recursion. Production node/sink lowering has no direct legacy `lower_t_expr` / `lower_expr_node` calls; flat node indices, expression indices, branch indices, map keys, and raw scan-range indices are preserved.
- Restored the legacy bridge fail-fast boundary for DATA_STREAM, MULTI_CAST, SPLIT, and ICEBERG_CHANGE_STREAM_ROUTER sinks when `exec_params` is absent.

## GREEN evidence

```text
cargo test -p novarocks --lib protocol::starrocks::decode::submission --features compat
18 passed; 0 failed

cargo test -p novarocks --lib protocol::starrocks::decode --features compat
28 passed; 0 failed

cargo test -p novarocks --lib protocol::starrocks::decode::expr::path_tests --features compat
3 passed; 0 failed

cargo test -p novarocks --lib destination_reports_fragment_branch_path --features compat
2 passed; 0 failed

cargo test -p novarocks --lib split_nested_destination_reports_fragment_branch_path --features compat
1 passed; 0 failed

cargo test -p novarocks --lib scan_range_error_preserves_map_key_and_raw_range_index --features compat
1 passed; 0 failed

cargo test -p novarocks --lib distributed_stream_sinks_without_exec_params_preserve_fail_fast_boundary --features compat
1 passed; 0 failed (covers DATA_STREAM, MULTI_CAST, SPLIT, and ICEBERG_CHANGE_STREAM_ROUTER)

cargo test -p novarocks --lib runtime::fragment::submission --features compat
45 passed; 0 failed

cargo check -p novarocks --all-targets
PASS

cargo check -p novarocks --all-targets --features compat
PASS

cargo fmt --all -- --check
PASS

git diff --check
PASS
```

Existing repository warnings remain; the verification commands produced no new errors.

## Ownership and purity scans

- `TPlanFragmentExecParams` below `protocol/starrocks/decode/node` and `sink`: zero matches.
- `novarocks_app_config`, `apply_object_store_runtime_defaults`, `stream_load_registry`, and `DataCacheManager::instance` below `protocol/starrocks/decode`: zero matches.
- Production node/sink direct legacy `lower_t_expr`, `lower_t_expr_with_common_slot_map`, and `lower_expr_node`: zero matches.
- `FragmentSubmission::try_new` in `submission.rs`: one match, only in `finish_fragment_submission`.
- Returned submission and metadata fields are domain types; no Thrift reference is retained.
- Legacy `query_profile_slots` / `(u64, ExprId)` pending-slot ownership: zero matches.

## Second-review fixes

- Replaced arena-ambiguous query-profile slots with typed `QueryProfilePatch` targets carrying a closed `FragmentExprArenaOwner` plus the arena-local `ExprId`.
- DataStream, MultiCast, Split, and Iceberg router expressions now lower directly into their retained sink-owned arenas. Iceberg table and StarRocks output, partition, and per-index predicate arenas record explicit owners.
- Finish resolves each query-profile patch through an exact owner/sink match. Invalid sink kinds and StarRocks index targets fail at the dependency boundary; finish still does not re-decode wire input.
- Added real submission acceptance for plan, DataStream, and MultiCast arenas, plus direct StarRocks output/partition/index routing and fail-fast coverage.
- Added `candidate_node` to the protocol-neutral `FileScanRange`. The StarRocks adapter preserves the raw value exactly, while the HDFS leaf performs the existing trim/filter normalization before constructing `ExternalDataCacheRangeOptions`.
- Added end-to-end HDFS acceptance proving the typed assignment retains `"  backend-7  "` and the runtime morsel receives `"backend-7"`.

Second-review RED evidence:

```text
query_profile_resolution_patches_{data_stream,multicast}_sink_owned_arena
0 passed; 2 failed; exit 101

hdfs_candidate_node_survives_typed_assignment_into_runtime_cache_options
0 passed; 1 failed; exit 101
```

Second-review focused GREEN evidence is included in the updated 18-test submission and 28-test decoder totals above. The StarRocks owner-routing test, plan-owned acceptance, and HDFS candidate-node acceptance each pass in that fresh suite.

## Residual review notes

- PBF-3 intentionally keeps execution-ready scan morsels physically embedded in the transitional `ExecPlan`; PBF-4 owns their physical removal. Task 4 nevertheless builds and validates non-empty protocol-neutral scan contracts/assignments from the same normalized facts, so the transitional duplication cannot diverge at ingress.
- Pure leaf/domain constructors may still return `String`, but each is mapped once at the exact typed wire owner. Recursive plan, expression, sink, and scan spines preserve typed errors end to end.
