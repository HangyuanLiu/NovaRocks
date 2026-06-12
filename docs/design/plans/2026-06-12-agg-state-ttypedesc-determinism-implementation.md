# Aggregate-State TTypeDesc Determinism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make standalone aggregate intermediate-state descriptors match the Arrow arrays produced at runtime, then remove the execution tolerances that currently hide descriptor/runtime drift.

**Architecture:** Treat aggregate phase output type as an explicit contract with three authorities: standalone codegen owns `TTypeDesc`, aggregate kernels own runtime Arrow arrays, and chunk/exchange code enforces descriptor conformity. Phase 0 adds tests and evidence for the current contract, Phase 1 fixes source descriptors or runtime serialization where they disagree, Phase 2 removes tolerance paths, and Phase 3 removes cross-chunk "actual wins" schema merging.

**Tech Stack:** Rust, Apache Arrow `RecordBatch`/`DataType`, StarRocks thrift `TTypeDesc`, NovaRocks standalone codegen/lowering, `tests/sql-test-runner`, cross-process standalone cluster mode.

---

## Source Design

This plan implements the design in `docs/design/plans/2026-06-12-agg-state-ttypedesc-determinism.md`.

Important corrections from current code inspection:

- `avg` standalone intermediate is currently `Utf8`, not `Binary`: see `src/sql/codegen/expr_compiler.rs::infer_agg_function_types` and `src/exec/expr/agg/functions/avg.rs`.
- Ordinary partial and streaming aggregates already call `kernel.build_array(..., output_intermediate=true)` and should continue producing serialized/intermediate arrays.
- Window outputs already use return types in standalone `visit_window`; they are not part of the aggregate-state drift.
- The drift is the passthrough/pre-agg case where a raw child expression flows into a slot whose descriptor says opaque/intermediate state.
- `src/exec/operators/aggregate/mod.rs::align_schema_with_arrays` is an additional tolerance not called out in the source design; it must be removed with the other "actual wins" paths.

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `src/sql/codegen/expr_compiler.rs` | Modify tests and small helpers | Aggregate function return/intermediate type source of truth for standalone thrift expressions. |
| `src/sql/codegen/fragment_builder.rs` | Modify | Use one helper for aggregate output slot contract in `visit_hash_aggregate`; keep window slots return-typed. |
| `src/lower/node/sort.rs` | Modify tests and fallback behavior | Reject opaque pre-agg passthrough drift instead of skipping casts. This is FE-compatible defensive cleanup; standalone acceptance does not depend on FE changes. |
| `src/exec/expr/agg/kernel.rs` | Modify tests | Runtime kernel output-type contract for `output_intermediate=true/false`. |
| `src/exec/operators/aggregate/mod.rs` | Modify tests and schema enforcement | Stop rewriting aggregate output schema to actual array types. |
| `src/exec/operators/aggregate/streaming_sink.rs` | Modify if needed | Uses the shared aggregate schema enforcement from `aggregate/mod.rs`; no duplicate type policy. |
| `src/runtime/exchange.rs` | Modify tests and encode/decode | Remove opaque numeric passthrough and cross-chunk schema merge. |
| `src/exec/chunk/schema.rs` | Modify tests and schema reconciliation | Make chunk schema alignment descriptor-conforming instead of actual-conforming where arrays are relatable. |
| `src/exec/chunk/type_relation.rs` | Modify tests/comment | Keep `retag_column` and `merge_fields_nullability` as the only permitted type materialization/nullability primitive. |
| `src/exec/operators/analytic_shared.rs` | Modify tests and output schema path | Stop rebuilding analytic output schema from actual arrays. |
| `sql-tests/aggregate/sql/agg_state_typedesc_contract.sql` | Create | Cross-process aggregate state coverage for avg/ndv/hll/percentile and streaming preaggregation. |
| `sql-tests/aggregate/result/agg_state_typedesc_contract.result` | Create | Golden results for the aggregate state contract case. |
| `sql-tests/analytic/sql/analytic_preagg_state_contract.sql` | Create | Cross-process window/topN coverage for passthrough-sensitive aggregate windows. |
| `sql-tests/analytic/result/analytic_preagg_state_contract.result` | Create | Golden results for analytic/topN contract case. |

## Scope Boundaries

- Acceptance target is standalone NovaRocks coordinator plus 3 NovaRocks BE processes:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite aggregate --only agg_state_typedesc_contract --mode verify
```

- Do not require StarRocks FE-compatible plan changes for acceptance. FE-compatible sort-lowering tolerances can be removed only after the defensive tests below pass; if an FE plan still sends opaque passthrough descriptors, the Rust side must fail fast with a clear error.
- Do not change analytic/window output typing to intermediate types. Window outputs stay return-typed.
- Do not convert all opaque states to `Binary`. Preserve current `avg` `Utf8` intermediate unless Phase 0 evidence proves a specific path needs a different representation.

---

### Task 1: Add Phase-0 Aggregate Type Contract Tests

**Files:**
- Modify: `src/sql/codegen/expr_compiler.rs`
- Modify: `src/exec/expr/agg/kernel.rs`

- [ ] **Step 1: Add standalone aggregate descriptor type tests**

Append this test to the existing `#[cfg(test)] mod tests` in `src/sql/codegen/expr_compiler.rs`. If the file has multiple test modules, place it in the module that already imports `infer_agg_function_types`.

```rust
#[test]
fn p5_inferred_intermediate_types_match_standalone_contract() {
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    let cases = vec![
        ("avg", vec![DataType::Int64], DataType::Float64, Some(DataType::Utf8)),
        ("avg", vec![DataType::Decimal128(12, 2)], DataType::Decimal128(38, 6), Some(DataType::Utf8)),
        ("ndv", vec![DataType::Int64], DataType::Int64, Some(DataType::Binary)),
        ("hll_union_agg", vec![DataType::Int64], DataType::Int64, Some(DataType::Binary)),
        ("bitmap_union_count", vec![DataType::Int64], DataType::Int64, Some(DataType::Binary)),
        ("percentile_approx", vec![DataType::Float64], DataType::Float64, Some(DataType::Binary)),
        (
            "array_agg",
            vec![DataType::Int64],
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            Some(DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))),
        ),
    ];

    for (name, args, expected_output, expected_intermediate) in cases {
        let (output, intermediate) =
            super::infer_agg_function_types(name, &args, false).expect(name);
        assert_eq!(output, expected_output, "{name} output type");
        assert_eq!(intermediate, expected_intermediate, "{name} intermediate type");
    }
}
```

- [ ] **Step 2: Run descriptor type tests**

Run:

```bash
cargo test --lib sql::codegen::expr_compiler::tests::p5_inferred_intermediate_types_match_standalone_contract
```

Expected: PASS. This is a Phase-0 characterization test; a failure means the source design is stale and the implementation plan must be amended before production changes.

- [ ] **Step 3: Add runtime kernel output contract tests**

Append this test to `src/exec/expr/agg/kernel.rs` under a new `#[cfg(test)] mod tests` if the file does not already have one.

```rust
#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    use super::build_kernel_set;
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

    fn agg_func(
        name: &str,
        input_is_intermediate: bool,
        intermediate_type: DataType,
        output_type: DataType,
        input_arg_type: Option<DataType>,
    ) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate,
            types: Some(AggTypeSignature {
                intermediate_type: Some(intermediate_type),
                output_type: Some(output_type),
                input_arg_type,
            }),
            order: Default::default(),
        }
    }

    #[test]
    fn p5_runtime_kernel_output_types_match_descriptor_contract() {
        let cases = vec![
            (
                agg_func("avg", false, DataType::Utf8, DataType::Float64, Some(DataType::Int64)),
                Some(DataType::Int64),
                DataType::Utf8,
                DataType::Float64,
            ),
            (
                agg_func("ndv", false, DataType::Binary, DataType::Int64, Some(DataType::Int64)),
                Some(DataType::Int64),
                DataType::Binary,
                DataType::Int64,
            ),
            (
                agg_func("hll_union_agg", false, DataType::Binary, DataType::Int64, Some(DataType::Int64)),
                Some(DataType::Int64),
                DataType::Binary,
                DataType::Int64,
            ),
            (
                agg_func("percentile_approx", false, DataType::Binary, DataType::Float64, Some(DataType::Float64)),
                Some(DataType::Float64),
                DataType::Binary,
                DataType::Float64,
            ),
            (
                agg_func(
                    "array_agg",
                    false,
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    Some(DataType::Int64),
                ),
                Some(DataType::Int64),
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            ),
        ];

        for (func, input_type, expected_intermediate, expected_final) in cases {
            let kernels = build_kernel_set(&[func.clone()], &[input_type]).expect(&func.name);
            assert_eq!(kernels.entries[0].output_type(true), expected_intermediate, "{} intermediate", func.name);
            assert_eq!(kernels.entries[0].output_type(false), expected_final, "{} final", func.name);
        }
    }
}
```

- [ ] **Step 4: Run runtime kernel output tests**

Run:

```bash
cargo test --lib exec::expr::agg::kernel::tests::p5_runtime_kernel_output_types_match_descriptor_contract
```

Expected: PASS. If it fails, stop and align the design document with the actual kernel contract before continuing.

- [ ] **Step 5: Commit Phase-0 characterization tests**

```bash
git add src/sql/codegen/expr_compiler.rs src/exec/expr/agg/kernel.rs
git commit -m "test: pin aggregate state type contracts"
```

### Task 2: Add Failing Tests For Drift Tolerances

**Files:**
- Modify: `src/exec/operators/aggregate/mod.rs`
- Modify: `src/runtime/exchange.rs`
- Modify: `src/exec/chunk/schema.rs`

- [ ] **Step 1: Add aggregate schema strictness test**

Append to `src/exec/operators/aggregate/mod.rs` tests:

```rust
#[test]
fn aggregate_output_schema_rejects_runtime_type_drift() {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![Field::new("__avg_state", DataType::Utf8, true)]));
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(10_i64)]))];

    let err = super::align_schema_with_arrays(&schema, &arrays, "p5 aggregate output")
        .expect_err("aggregate output must not adopt actual array type");
    assert!(err.contains("p5 aggregate output type mismatch"), "err={err}");
}
```

- [ ] **Step 2: Add exchange decode strictness test**

Append to `src/runtime/exchange.rs` tests. Reuse existing imports in the module.

```rust
#[test]
fn decode_chunks_for_sender_rejects_numeric_for_opaque_descriptor() {
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::lower::thrift::type_lowering::scalar_type_desc;
    use crate::types::TPrimitiveType;

    let key = ExchangeKey {
        finst_id_hi: 501,
        finst_id_lo: 502,
        node_id: 31,
    };

    let wire_schema = Arc::new(Schema::new(vec![Field::new("__opaque_state", DataType::Int64, true)]));
    let wire_batch = RecordBatch::try_new(
        wire_schema,
        vec![Arc::new(Int64Array::from(vec![Some(7_i64)])) as ArrayRef],
    )
    .expect("wire batch");
    let wire_chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        wire_batch.schema().as_ref(),
        &[SlotId::new(41)],
    )
    .expect("wire chunk schema");
    let wire_chunk = Chunk::new_with_chunk_schema(wire_batch, wire_chunk_schema);

    let expected_schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new(
            SlotId::new(41),
            "__opaque_state",
            true,
            Some(scalar_type_desc(TPrimitiveType::VARBINARY)),
            Some(41),
        )])
        .expect("expected chunk schema"),
    );
    register_expected_chunk_schema(key, 1, expected_schema).expect("register expected schema");

    let payload = encode_chunks(&[wire_chunk], true).expect("encode drifted chunk");
    let err = decode_chunks_for_sender(key, 3, 1, &payload)
        .expect_err("numeric payload must not pass as an opaque aggregate state");
    assert!(err.contains("exchange decoded arrow type mismatch"), "err={err}");

    cancel_exchange_key(key);
}
```

- [ ] **Step 3: Add chunk schema contract test**

Append to `src/exec/chunk/schema.rs` tests:

```rust
#[test]
fn align_chunk_schema_to_columns_keeps_descriptor_type_for_retaggable_columns() {
    let schema = ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
        SlotId::new(9),
        Field::new("payload", DataType::Binary, true),
        None,
        None,
    )])
    .expect("chunk schema");
    let column = Arc::new(arrow::array::StringArray::from(vec![Some("abc")])) as ArrayRef;

    let aligned = super::align_chunk_schema_to_columns(&[column], &schema).expect("align schema");
    assert_eq!(aligned.slots()[0].data_type(), &DataType::Binary);
}
```

- [ ] **Step 4: Run the new failing tests**

Run each command:

```bash
cargo test --lib exec::operators::aggregate::tests::aggregate_output_schema_rejects_runtime_type_drift
cargo test --lib runtime::exchange::tests::decode_chunks_for_sender_rejects_numeric_for_opaque_descriptor
cargo test --lib exec::chunk::schema::tests::align_chunk_schema_to_columns_keeps_descriptor_type_for_retaggable_columns
```

Expected: all three FAIL before implementation. The aggregate test currently succeeds incorrectly by returning `Ok`; exchange currently accepts numeric/opaque passthrough; chunk schema alignment currently keeps the actual `Utf8` type.

- [ ] **Step 5: Commit the failing tests after confirming RED**

Do not commit if any test passes unexpectedly. If they all fail for the expected reason:

```bash
git add src/exec/operators/aggregate/mod.rs src/runtime/exchange.rs src/exec/chunk/schema.rs
git commit -m "test: expose aggregate descriptor drift tolerances"
```

### Task 3: Centralize Standalone Aggregate Slot Contracts

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Add slot-contract helper tests**

Inside `src/sql/codegen/fragment_builder.rs` tests, add a small helper-level test rather than constructing a full plan:

```rust
#[test]
fn aggregate_slot_contract_uses_intermediate_only_for_non_finalize() {
    use arrow::datatypes::DataType;
    use crate::lower::type_lowering::arrow_type_from_desc;
    use crate::sql::codegen::expr_compiler::infer_agg_function_types;

    let (_, avg_intermediate) =
        infer_agg_function_types("avg", &[DataType::Int64], false).expect("avg types");
    let avg_intermediate = avg_intermediate.expect("avg intermediate");
    let contract = super::aggregate_slot_contract_for_phase(
        false,
        &DataType::Float64,
        Some(&avg_intermediate),
        "avg",
    )
    .expect("local avg contract");
    assert_eq!(contract.data_type, DataType::Utf8);
    assert_eq!(arrow_type_from_desc(&contract.type_desc), Some(DataType::Utf8));

    let final_contract = super::aggregate_slot_contract_for_phase(
        true,
        &DataType::Float64,
        Some(&avg_intermediate),
        "avg",
    )
    .expect("final avg contract");
    assert_eq!(final_contract.data_type, DataType::Float64);
    assert_eq!(arrow_type_from_desc(&final_contract.type_desc), Some(DataType::Float64));
}
```

- [ ] **Step 2: Run helper test and verify RED**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::aggregate_slot_contract_uses_intermediate_only_for_non_finalize
```

Expected: FAIL because `aggregate_slot_contract_for_phase` does not exist.

- [ ] **Step 3: Implement the helper**

Add near the top-level helper functions in `src/sql/codegen/fragment_builder.rs`:

```rust
#[derive(Clone, Debug, PartialEq)]
struct AggregateSlotContract {
    data_type: arrow::datatypes::DataType,
    type_desc: crate::types::TTypeDesc,
}

fn aggregate_slot_contract_for_phase(
    need_finalize: bool,
    result_type: &arrow::datatypes::DataType,
    intermediate_type: Option<&arrow::datatypes::DataType>,
    display_name: &str,
) -> Result<AggregateSlotContract, String> {
    let data_type = if need_finalize {
        result_type.clone()
    } else {
        intermediate_type
            .cloned()
            .unwrap_or_else(|| result_type.clone())
    };
    let type_desc = type_infer::arrow_type_to_type_desc(&data_type)
        .map_err(|e| format!("aggregate `{display_name}` output type descriptor failed: {e}"))?;
    Ok(AggregateSlotContract { data_type, type_desc })
}
```

- [ ] **Step 4: Use the helper in `visit_hash_aggregate`**

In `visit_hash_aggregate`, replace the separate `data_type` and `slot_type_desc` branches around the aggregate loop with:

```rust
let intermediate_type = texpr
    .nodes
    .first()
    .and_then(|root| root.fn_.as_ref())
    .and_then(|func| func.aggregate_fn.as_ref())
    .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type));
let name = agg_call_display_name(agg_call);
let slot_contract = aggregate_slot_contract_for_phase(
    need_finalize,
    &agg_call.result_type,
    intermediate_type.as_ref(),
    &name,
)?;
let data_type = slot_contract.data_type.clone();
let slot_type_desc = slot_contract.type_desc.clone();
```

Keep the existing nullable, slot allocation, descriptor registration, and `ColumnBinding` code.

- [ ] **Step 5: Run helper and aggregate codegen tests**

Run:

```bash
cargo test --lib sql::codegen::fragment_builder::tests::aggregate_slot_contract_uses_intermediate_only_for_non_finalize
cargo test --lib sql::codegen::fragment_builder::tests
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "refactor: centralize aggregate slot type contracts"
```

### Task 4: Fix Or Fail Fast On Pre-Agg Passthrough Drift

**Files:**
- Modify: `src/lower/node/sort.rs`

- [ ] **Step 1: Add failing test for opaque passthrough**

Add a focused unit test in `src/lower/node/sort.rs` tests that builds a `pre_agg_expr` for `avg(Int64)` with root type `VARCHAR` and child type `BIGINT`, then calls the private `lower_pre_agg_fallback_expr`.

Use the existing sort test helper style in this file and assert:

```rust
let err = lower_pre_agg_fallback_expr(
    &agg_expr,
    &mut arena,
    &input_layout,
    None,
    None,
    7001,
)
.expect_err("opaque pre-agg passthrough must fail until descriptor is raw");
assert!(err.contains("pre-agg passthrough declares opaque aggregate state"), "err={err}");
```

The `agg_expr` root must have `fn_.aggregate_fn.intermediate_type` set to `VARCHAR`, `type_` set to `VARCHAR`, and one child slot-ref whose lowered arena type is `Int64`.

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
cargo test --lib lower::node::sort::tests::lower_pre_agg_fallback_rejects_opaque_passthrough_type_drift
```

Expected: FAIL because current code returns a `Cast(child, Utf8)` for `VARCHAR` drift, while `Binary`/`LargeBinary` drift is silently passed through. Both behaviors hide source descriptor drift for aggregate-state passthrough.

- [ ] **Step 3: Replace the opaque skip with an explicit error**

In `lower_pre_agg_fallback_expr`, replace the `&& !matches!(agg_output_type, DataType::Null | DataType::Binary | DataType::LargeBinary)` guard with this logic:

```rust
if child_type != agg_output_type {
    if matches!(agg_output_type, DataType::Binary | DataType::LargeBinary | DataType::Utf8 | DataType::LargeUtf8) {
        return Err(format!(
            "SORT_NODE node_id={node_id} pre-agg passthrough declares opaque aggregate state {:?} but child expression is {:?}; source descriptor must be raw/return-typed or runtime must serialize",
            agg_output_type, child_type
        ));
    }
    if !matches!(agg_output_type, DataType::Null) {
        return Ok(arena.push_typed(ExprNode::Cast(child_id), agg_output_type));
    }
}
```

- [ ] **Step 4: Run sort lowering tests**

Run:

```bash
cargo test --lib lower::node::sort::tests::lower_pre_agg_fallback_rejects_opaque_passthrough_type_drift
cargo test --lib lower::node::sort::tests
```

Expected: PASS. If an existing test now hits the new error, inspect whether that test is modeling an FE-compatible opaque passthrough. For this standalone-focused work, keep the fail-fast behavior and update the test expectation.

- [ ] **Step 5: Commit**

```bash
git add src/lower/node/sort.rs
git commit -m "fix: fail fast on opaque pre-agg passthrough drift"
```

### Task 5: Remove Aggregate Output Schema Actual-Wins Behavior

**Files:**
- Modify: `src/exec/operators/aggregate/mod.rs`
- Modify: `src/exec/operators/aggregate/streaming_sink.rs` only if compiler errors require call-site changes

- [ ] **Step 1: Implement strict aggregate schema alignment**

Replace `align_schema_with_arrays` in `src/exec/operators/aggregate/mod.rs` with:

```rust
pub(super) fn align_schema_with_arrays(
    schema: &SchemaRef,
    arrays: &[ArrayRef],
    context: &str,
) -> Result<SchemaRef, String> {
    if schema.fields().len() != arrays.len() {
        return Err(format!(
            "{context} schema/array length mismatch: schema_fields={} arrays={}",
            schema.fields().len(),
            arrays.len()
        ));
    }
    for (idx, (field, array)) in schema.fields().iter().zip(arrays.iter()).enumerate() {
        if field.data_type() != array.data_type() {
            return Err(format!(
                "{context} type mismatch at column {idx}: descriptor={:?} actual={:?}",
                field.data_type(),
                array.data_type()
            ));
        }
    }
    Ok(Arc::clone(schema))
}
```

Do not widen aggregate output nullability here. Group-key nullability is already handled by `build_output_schema_from_kernels`; aggregate state slots should be declared nullable by the codegen descriptor.

- [ ] **Step 2: Run aggregate strictness tests**

Run:

```bash
cargo test --lib exec::operators::aggregate::tests::aggregate_output_schema_rejects_runtime_type_drift
cargo test --lib exec::operators::aggregate::tests
```

Expected: PASS.

- [ ] **Step 3: Run streaming aggregate compile check**

Run:

```bash
cargo test --lib exec::operators::aggregate::streaming_sink
```

Expected: PASS or zero matching tests with exit 0. If compile fails, adjust only imports/call signatures in `src/exec/operators/aggregate/streaming_sink.rs`; do not duplicate schema policy there.

- [ ] **Step 4: Commit**

```bash
git add src/exec/operators/aggregate/mod.rs src/exec/operators/aggregate/streaming_sink.rs
git commit -m "fix: enforce aggregate output descriptor types"
```

### Task 6: Make Chunk Schema Alignment Descriptor-Conforming

**Files:**
- Modify: `src/exec/chunk/schema.rs`
- Modify: `src/exec/chunk/type_relation.rs`

- [ ] **Step 1: Replace actual-wins reconciliation**

In `src/exec/chunk/schema.rs`, change `reconcile_chunk_data_type` so relatable types return the descriptor type, not the actual type. The body should become:

```rust
fn reconcile_chunk_data_type(expected: &DataType, actual: &DataType) -> Result<DataType, String> {
    if expected == actual {
        return Ok(expected.clone());
    }
    crate::exec::chunk::type_relation::relate(
        expected,
        actual,
        crate::exec::chunk::type_relation::CompatibilityPolicy::SameScaleWiden,
    )
    .map_err(|_| format!("chunk schema type mismatch: expected {:?}, got {:?}", expected, actual))?;
    Ok(expected.clone())
}
```

Keep `reconcile_chunk_field_to_field` and `reconcile_chunk_field_to_data_type` as the nullability merge points for now.

- [ ] **Step 2: Update type-relation comment**

In `src/exec/chunk/type_relation.rs`, replace the "array_agg actual-wins" note above `merge_fields_nullability` with:

```rust
/// Descriptor nullability is the contract; runtime nullability may widen it
/// when a producer observes NULL values. Type selection stays descriptor-led.
```

- [ ] **Step 3: Run chunk schema tests**

Run:

```bash
cargo test --lib exec::chunk::schema::tests::align_chunk_schema_to_columns_keeps_descriptor_type_for_retaggable_columns
cargo test --lib exec::chunk::schema::tests
cargo test --lib exec::chunk::type_relation::tests
```

Expected: PASS. If an existing chunk-schema test expects actual type selection, update the expectation to descriptor type only when `relate(expected, actual, SameScaleWiden)` succeeds.

- [ ] **Step 4: Commit**

```bash
git add src/exec/chunk/schema.rs src/exec/chunk/type_relation.rs
git commit -m "fix: keep chunk schemas descriptor-led"
```

### Task 7: Remove Exchange Opaque Passthrough And Merge Schema

**Files:**
- Modify: `src/runtime/exchange.rs`

- [ ] **Step 1: Remove numeric-to-opaque decode passthrough**

In `materialize_chunk_for_wire_meta`, delete the `is_opaque_binary_expected && is_numeric_actual` passthrough branch. The mismatch branch should always call `retag_column`; when the source is numeric and expected is opaque, `retag_column` returns a clear error.

The resulting code shape should be:

```rust
if let Some(type_desc) = expected_slot.type_desc()
    && let Some(expected_arrow_type) = arrow_type_from_desc(type_desc)
    && field.data_type() != &expected_arrow_type
{
    out_column = crate::exec::chunk::type_relation::retag_column(batch.column(idx), &expected_arrow_type)
        .map_err(|m| {
            format!(
                "exchange decoded arrow type mismatch at index {} for slot {}: batch={:?} expected={:?} ({:?})",
                idx, slot_id, field.data_type(), expected_arrow_type, m.kind
            )
        })?;
    out_field = arrow::datatypes::Field::new(field.name(), expected_arrow_type, field.is_nullable());
    any_materialized = true;
}
```

- [ ] **Step 2: Replace cross-chunk merged schema with first contract schema**

In `encode_arrow_ipc_chunks`, replace:

```rust
let schema = merged_exchange_schema(chunks)?;
```

with a new helper:

```rust
let schema = exchange_wire_schema_from_first_chunk(chunks)?;
```

Add:

```rust
fn exchange_wire_schema_from_first_chunk(chunks: &[Chunk]) -> Result<SchemaRef, String> {
    let first = chunks
        .first()
        .ok_or_else(|| "exchange chunks must not be empty".to_string())?;
    Ok(first.schema())
}
```

Keep `normalize_exchange_batch_for_schema` so every chunk is retagged to the first chunk's descriptor schema before IPC writing. Delete `merge_exchange_field_type`, `merge_exchange_field`, and `merged_exchange_schema` after tests pass and no callers remain.

- [ ] **Step 3: Run exchange tests**

Run:

```bash
cargo test --lib runtime::exchange::tests::decode_chunks_for_sender_rejects_numeric_for_opaque_descriptor
cargo test --lib runtime::exchange::tests
```

Expected: PASS. Existing decimal/nullable tests must still pass; they prove relatable types are retagged without value loss.

- [ ] **Step 4: Commit**

```bash
git add src/runtime/exchange.rs
git commit -m "fix: enforce descriptor types in exchange payloads"
```

### Task 8: Remove Analytic Output Schema Rebuild

**Files:**
- Modify: `src/exec/operators/analytic_shared.rs`

- [ ] **Step 1: Add a helper test for analytic descriptor enforcement**

Add a private helper near `compute` in `src/exec/operators/analytic_shared.rs`:

```rust
fn validate_analytic_output_columns(
    columns: &[ArrayRef],
    output_chunk_schema: &crate::exec::chunk::ChunkSchemaRef,
) -> Result<(), String> {
    for (idx, (col, slot)) in columns.iter().zip(output_chunk_schema.slots()).enumerate() {
        let expected = slot.field();
        if col.data_type() != expected.data_type() {
            return Err(format!(
                "analytic output type mismatch at column {idx}: descriptor={:?} actual={:?}",
                expected.data_type(),
                col.data_type()
            ));
        }
    }
    Ok(())
}
```

Add a unit test that builds a one-column schema expecting `DataType::Binary`, passes an `Int64Array`, and expects the error substring `analytic output type mismatch`.

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
cargo test --lib exec::operators::analytic_shared::tests::analytic_output_rejects_descriptor_type_drift
```

Expected: FAIL before the helper is wired or before the old rebuild path is removed.

- [ ] **Step 3: Replace schema adjustment with validation**

In `compute`, delete `needs_schema_adjustment` and `effective_schema`. Replace:

```rust
split_analytic_output_chunks(effective_schema, &columns, input)
```

with:

```rust
validate_analytic_output_columns(&columns, &output_chunk_schema)?;
split_analytic_output_chunks(output_chunk_schema, &columns, input)
```

Do not rebuild fields from actual arrays.

- [ ] **Step 4: Run analytic unit tests**

Run:

```bash
cargo test --lib exec::operators::analytic_shared::tests::analytic_output_rejects_descriptor_type_drift
cargo test --lib exec::operators::analytic_shared::tests
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/exec/operators/analytic_shared.rs
git commit -m "fix: enforce analytic output descriptor types"
```

### Task 9: Add Cross-Process SQL Regressions

**Files:**
- Create: `sql-tests/aggregate/sql/agg_state_typedesc_contract.sql`
- Create: `sql-tests/aggregate/result/agg_state_typedesc_contract.result`
- Create: `sql-tests/analytic/sql/analytic_preagg_state_contract.sql`
- Create: `sql-tests/analytic/result/analytic_preagg_state_contract.result`

- [ ] **Step 1: Create aggregate SQL case**

Create `sql-tests/aggregate/sql/agg_state_typedesc_contract.sql`:

```sql
-- @tags=aggregate,p5,typedesc,intermediate-state,cross-process
DROP TABLE IF EXISTS ${case_db}.agg_state_typedesc_contract;
CREATE TABLE ${case_db}.agg_state_typedesc_contract (
    grp INT,
    k INT,
    v BIGINT,
    d DOUBLE
);
INSERT INTO ${case_db}.agg_state_typedesc_contract VALUES
    (1, 10, 10, 0.10),
    (1, 11, 20, 0.20),
    (1, 12, 20, 0.30),
    (2, 20, 100, 0.40),
    (2, 21, 200, 0.50),
    (2, 22, NULL, NULL);

SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation') */
       grp,
       CAST(avg(v) AS DECIMAL(18, 4)) AS avg_v,
       ndv(k) AS ndv_k,
       hll_union_agg(k) AS hll_k,
       CAST(percentile_approx(d, 0.5) AS DECIMAL(18, 4)) AS p50_d
FROM ${case_db}.agg_state_typedesc_contract
GROUP BY grp
ORDER BY grp;

SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming') */
       grp,
       CAST(avg(v) AS DECIMAL(18, 4)) AS avg_v,
       ndv(k) AS ndv_k
FROM ${case_db}.agg_state_typedesc_contract
GROUP BY grp
ORDER BY grp;
```

- [ ] **Step 2: Create analytic SQL case**

Create `sql-tests/analytic/sql/analytic_preagg_state_contract.sql`:

```sql
-- @tags=analytic,p5,typedesc,topn,cross-process
DROP TABLE IF EXISTS ${case_db}.analytic_preagg_state_contract;
CREATE TABLE ${case_db}.analytic_preagg_state_contract (
    grp INT,
    score INT,
    v BIGINT,
    d DOUBLE
);
INSERT INTO ${case_db}.analytic_preagg_state_contract VALUES
    (1, 100, 10, 0.10),
    (1, 90, 20, 0.20),
    (1, 90, 30, 0.30),
    (2, 100, 100, 0.40),
    (2, 80, 200, 0.50),
    (2, 70, NULL, NULL);

SELECT grp, score, avg_v, p50_d
FROM (
    SELECT
        grp,
        score,
        CAST(avg(v) OVER (
            PARTITION BY grp
            ORDER BY score DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18, 4)) AS avg_v,
        CAST(percentile_approx(d, 0.5) OVER (
            PARTITION BY grp
            ORDER BY score DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS DECIMAL(18, 4)) AS p50_d,
        rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
    FROM ${case_db}.analytic_preagg_state_contract
) t
WHERE rk <= 2
ORDER BY grp, score DESC, avg_v;
```

- [ ] **Step 3: Record goldens**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite aggregate --only agg_state_typedesc_contract --mode record
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite analytic --only analytic_preagg_state_contract --mode record
```

Expected: both record commands exit 0 and create the two result files.

- [ ] **Step 4: Verify goldens**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite aggregate --only agg_state_typedesc_contract --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite analytic --only analytic_preagg_state_contract --mode verify
```

Expected: both cases PASS in cross-process mode.

- [ ] **Step 5: Commit SQL regressions**

```bash
git add sql-tests/aggregate/sql/agg_state_typedesc_contract.sql \
        sql-tests/aggregate/result/agg_state_typedesc_contract.result \
        sql-tests/analytic/sql/analytic_preagg_state_contract.sql \
        sql-tests/analytic/result/analytic_preagg_state_contract.result
git commit -m "test: cover aggregate state descriptor contracts"
```

### Task 10: Final Verification

**Files:**
- No new files

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt --check
```

Expected: exit 0.

- [ ] **Step 2: Focused Rust tests**

Run:

```bash
cargo test --lib sql::codegen::expr_compiler::tests::p5_inferred_intermediate_types_match_standalone_contract
cargo test --lib exec::expr::agg::kernel::tests::p5_runtime_kernel_output_types_match_descriptor_contract
cargo test --lib sql::codegen::fragment_builder::tests::aggregate_slot_contract_uses_intermediate_only_for_non_finalize
cargo test --lib lower::node::sort::tests::lower_pre_agg_fallback_rejects_opaque_passthrough_type_drift
cargo test --lib exec::operators::aggregate::tests::aggregate_output_schema_rejects_runtime_type_drift
cargo test --lib runtime::exchange::tests::decode_chunks_for_sender_rejects_numeric_for_opaque_descriptor
cargo test --lib exec::chunk::schema::tests::align_chunk_schema_to_columns_keeps_descriptor_type_for_retaggable_columns
cargo test --lib exec::operators::analytic_shared::tests::analytic_output_rejects_descriptor_type_drift
```

Expected: every command exits 0.

- [ ] **Step 3: Focused module tests**

Run:

```bash
cargo test --lib exec::operators::aggregate::tests
cargo test --lib runtime::exchange::tests
cargo test --lib exec::chunk::schema::tests
cargo test --lib exec::chunk::type_relation::tests
cargo test --lib exec::operators::analytic_shared::tests
```

Expected: every command exits 0.

- [ ] **Step 4: Build**

Run:

```bash
cargo build --profile dev-opt
```

Expected: exit 0.

- [ ] **Step 5: Cross-process SQL**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite aggregate --only agg_state_typedesc_contract --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite analytic --only analytic_preagg_state_contract --mode verify
```

Expected: both commands exit 0.

- [ ] **Step 6: Existing focused suites**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite aggregate --only agg_test_agg_split_two_phase,agg_test_streaming_agg,agg_group_sum_count_avg,agg_percentile_semantics,agg_sketch_bitmap_varbinary_semantics --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite analytic --only analytic_avg_rows_sliding,analytic_ntile_percentile,analytic_test_window_pre_agg_with_rank,analytic_test_window_hll_bitmap --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --cluster-mode cross-process --cluster-size 3 \
  --suite sort --only topn_rank_partition_filter_tie_expand,topn_rank_filter_tie_expand --mode verify
```

Expected: all listed cases PASS. Any failure here is in scope and must be debugged before publishing.

- [ ] **Step 7: Diff hygiene**

Run:

```bash
git diff --check
git diff --cached --check
```

Expected: both exit 0.

## Self-Review

**Spec coverage:** Phase 0 evidence is covered by Tasks 1-2. Phase 1 source contract is covered by Tasks 3-4. Phase 2 tolerance removal is covered by Tasks 5, 7, and 8. Phase 3 exchange/chunk descriptor authority is covered by Tasks 6-7. Cross-process acceptance is covered by Tasks 9-10.

**Placeholder scan:** This plan contains no unresolved placeholders, no deferred implementation slots, and no "write tests for the above" step without concrete test content or commands.

**Type consistency:** The plan consistently uses `Utf8` for standalone `avg` intermediate, `Binary` for NDV/HLL/bitmap/percentile opaque states, `output_intermediate=true` for partial/streaming aggregate state output, and return types for final/window outputs.

## Execution Handoff

Plan complete and saved to `docs/design/plans/2026-06-12-agg-state-ttypedesc-determinism-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** - dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** - execute tasks in this session using `superpowers:executing-plans`, with checkpoints after each task.
