# DistributedPlan IR — M0 Slice 2 (Sort / HashAggregate) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the `DistributedPlan` IR + `build_via_distributed_plan` path to lower `Sort` and `HashAggregate` (single-fragment) through the same two-pass build/lower, with byte-identical equivalence to today's `PlanFragmentBuilder::build`.

**Architecture:** Continues milestone **M0** on top of slice 1 (Scan/Filter/Project, merged in PR #318). Same strategy: **extract-and-share** — move each operator's lowering core out of `visit_*` into a `LoweringCtx` method that both the old builder and the new Pass-2 call, so Pass-2 behavior is identical by construction. Single-fragment only (the existing `validate_m0_root_fragment` guard stays). Engine execution path stays on the old `build`; the new path runs only in equivalence tests. **Out of scope:** `Limit` (a 0-node fold that mutates Sort's `limit`/`offset` — groups with `TopN` in slice 3), exchange/fragmentation (slice for S5), cutover.

**Tech Stack:** Rust, thrift 0.17 generated types (`plan_nodes::TPlanNode`, all derive `PartialEq`+`Debug`), `cargo test`.

**Read first — slice-1 code to mirror (on this branch, from PR #318):**
- `src/sql/codegen/ir/{node,body,build,lowering,fragment,equiv}.rs` — the slice-1 pattern this slice extends.
- `src/sql/codegen/ir/lowering.rs`: `LoweringStateAccess` trait (`:133`), `LoweringCtx::lower_node` (`:348`), `lower_scan` (`:394`), `lower_project` (`:857`), `scan_body_to_physical_op`/`project_body_to_physical_op` adapters (`:947`/`:961`).
- `src/sql/codegen/fragment_builder.rs`: thin wrappers `visit_scan` (`:1560`), `visit_project` (`:1816`); the cores to extract this slice — `visit_hash_aggregate` (`:2307-2502`), `visit_sort` (`:2508-2608`); helper `slot_ref_exprs_for_columns` (used by `visit_sort`; grep for its def).
- `src/sql/codegen/nodes.rs`: `build_aggregation_node` (`:463`), `default_plan_node` (`:1925`).
- `src/sql/optimizer/operator.rs`: `PhysicalHashAggregateOp` (`:357`), `PhysicalSortOp` (`:370`), `AggMode` (`:29`).

**Run tests for this slice:** `cargo test --lib sql::codegen`

---

## File Structure

- **Modify** `src/sql/codegen/ir/body.rs` — add `SortBody`, `AggregateBody` (mirror `PhysicalSortOp`/`PhysicalHashAggregateOp` fields).
- **Modify** `src/sql/codegen/ir/node.rs` — add `Sort` + `HashAggregate` variants to `DistributedPlanNodeBody`.
- **Modify** `src/sql/codegen/ir/lowering.rs` — add `lower_sort`/`lower_aggregate` cores onto `LoweringCtx`; add `lower_node` arms; add `sort_body_to_physical_op`/`aggregate_body_to_physical_op` adapters; relocate `slot_ref_exprs_for_columns` onto `LoweringStateAccess`.
- **Modify** `src/sql/codegen/ir/build.rs` — add `Sort`/`HashAggregate` arms to `build_distributed_plan`.
- **Modify** `src/sql/codegen/fragment_builder.rs` — `visit_sort`/`visit_hash_aggregate` become thin wrappers delegating to the extracted cores.
- **Modify** `src/sql/codegen/ir/equiv.rs` — add Sort/Aggregate equivalence cases.

---

## Task 1: IR body types + enum variants for Sort/HashAggregate

**Files:** Modify `src/sql/codegen/ir/body.rs`, `src/sql/codegen/ir/node.rs`

- [ ] **Step 1: Add `SortBody` and `AggregateBody` to `body.rs`**

Append to `src/sql/codegen/ir/body.rs` (these mirror `PhysicalSortOp`/`PhysicalHashAggregateOp` field-for-field so the extracted cores read `body.*` in place of `op.*`):

```rust
use crate::sql::optimizer::operator::{AggMode, AggregateCall};

/// Mirrors `PhysicalSortOp` plus the node's output columns (the extracted
/// `lower_sort` needs them to build `TSortInfo.sort_tuple_slot_exprs`, which
/// the old `visit_sort` read from `PhysicalPlanNode.output_columns`).
#[derive(Clone, Debug)]
pub(crate) struct SortBody {
    pub items: Vec<crate::sql::analysis::SortItem>,
    pub analytic_partition_exprs: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

/// Mirrors `PhysicalHashAggregateOp` field-for-field.
#[derive(Clone, Debug)]
pub(crate) struct AggregateBody {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub is_merge: Vec<bool>,
    pub output_columns: Vec<OutputColumn>,
}
```

(If `AggregateCall`'s path differs, copy the exact `use` that `operator.rs` uses for `PhysicalHashAggregateOp.aggregates`. `SortItem`/`OutputColumn` are in `crate::sql::analysis`.)

- [ ] **Step 2: Add enum variants in `node.rs`**

In `src/sql/codegen/ir/node.rs`, extend the body imports and enum:

```rust
use super::body::{AggregateBody, ProjectBody, ScanBody, SortBody};
```

```rust
#[derive(Clone, Debug)]
pub(crate) enum DistributedPlanNodeBody {
    Scan(Box<ScanBody>),
    Project(ProjectBody),
    Sort(SortBody),
    HashAggregate(Box<AggregateBody>),
}
```

(`AggregateBody` is boxed per the spec's large-variant rule; `SortBody` is small enough to leave unboxed.)

- [ ] **Step 3: Keep `lower_node` exhaustive with temporary arms**

Adding the two enum variants breaks the (currently exhaustive, Scan/Project-only) `match &node.body` in `LoweringCtx::lower_node` (`lowering.rs:348`). To keep every commit compiling, add temporary error arms now — Task 4 replaces them with the real lowering:

```rust
// TEMPORARY — replaced in Task 4. Keeps lower_node exhaustive so the crate
// compiles between slice-2 tasks.
super::node::DistributedPlanNodeBody::Sort(_)
| super::node::DistributedPlanNodeBody::HashAggregate(_) => {
    Err("sort/aggregate lowering not yet implemented (slice-2 Task 4)".to_string())
}
```

`build_distributed_plan`'s `match` already has a catch-all `other =>`, so Sort/HashAggregate hit that (runtime error) until Task 3 adds their arms — no compile break there.

- [ ] **Step 4: Build to verify it compiles**

Run: `cargo build --lib 2>&1 | tail -5`
Expected: compiles cleanly (unused-field warnings on the new bodies are fine until Tasks 3–4 consume them).

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/ir/body.rs src/sql/codegen/ir/node.rs src/sql/codegen/ir/lowering.rs
git commit -m "codegen/ir: add SortBody/AggregateBody IR types + enum variants (temp lower arms)"
```

---

## Task 2: Extract `lower_sort` + `lower_aggregate` cores; `visit_*` delegate

Goal: move the bodies of `visit_sort`/`visit_hash_aggregate` into reusable `LoweringCtx` methods (parameterized on the pre-allocated ids + child result), leaving the old visitors as thin wrappers. **Behavior must not change** — existing `sql::codegen::fragment_builder` tests stay green. Mirror exactly how slice 1 extracted `lower_scan`/`lower_project`.

**Files:** Modify `src/sql/codegen/ir/lowering.rs`, `src/sql/codegen/fragment_builder.rs`

- [ ] **Step 1: Relocate `slot_ref_exprs_for_columns` onto `LoweringStateAccess`**

`visit_sort` calls `self.slot_ref_exprs_for_columns(&child.scope, &node.output_columns, "Sort")`. The extracted `lower_sort` needs it via the trait. Find its current definition on `PlanFragmentBuilder` in `fragment_builder.rs` (grep `fn slot_ref_exprs_for_columns`). It only reads a scope + output columns and builds `Vec<TExpr>` slot refs (no id allocation), so add it to the `LoweringStateAccess` trait in `lowering.rs` as a provided/required method, implement it once, and have the old `PlanFragmentBuilder` method delegate to (or be replaced by) it — exactly as slice 1 did for `refresh_scan_table_for_codegen` / `propagate_dict_to_slot` (`lowering.rs:156`/`:160`).

```rust
// in trait LoweringStateAccess<'a> (lowering.rs)
fn slot_ref_exprs_for_columns(
    &self,
    scope: &ExprScope,
    output_columns: &[AnalysisOutputColumn],
    context: &str,
) -> Result<Vec<exprs::TExpr>, String>;
```

Implement it for both `PlanFragmentBuilder` (move the existing body) and `OwnedLoweringState` (same body — it touches no builder-only state; if it currently uses only `self`-free logic, make it a free fn `fn slot_ref_exprs_for_columns(scope, cols, ctx)` and have both impls call it). Run `cargo build --lib` to confirm it still compiles and the old call site resolves.

- [ ] **Step 2: Move the aggregate body into `LoweringCtx::lower_aggregate`**

Cut the body of `visit_hash_aggregate` (`fragment_builder.rs:2312-2501`, everything after the signature through the `Ok(VisitResult{..})`). Changes:
1. Remove the first line `let child = self.visit(&node.children[0])?;` — the caller provides the child.
2. Remove `let agg_tuple_id = self.alloc_tuple();` / `let agg_node_id = self.alloc_node();` — receive as params.
3. Replace `self.` accesses with the ctx/state (`self.slot_allocator()`, `self.desc_builder()`, `self.propagate_dict_to_slot(..)` — all already on `LoweringStateAccess`).
4. Replace references to `child.scope` with the passed `child_scope`.
5. Return `(agg_plan_node, agg_scope)` instead of `VisitResult`.

```rust
impl<'s, 'a, S: LoweringStateAccess<'a> + ?Sized> LoweringCtx<'s, 'a, S> {
    pub(crate) fn lower_aggregate(
        &mut self,
        agg_node_id: i32,
        agg_tuple_id: i32,
        op: &PhysicalHashAggregateOp,
        child_scope: &ExprScope,
    ) -> Result<(plan_nodes::TPlanNode, ExprScope), String> {
        // ... moved body from visit_hash_aggregate:2313-2489, returning
        //     (agg_plan_node, agg_scope). need_finalize/group-by/aggregate
        //     compile + add_slot_with_type_desc + add_tuple +
        //     build_aggregation_node stay verbatim; `child.scope` -> child_scope.
    }
}
```

- [ ] **Step 3: Rewrite `visit_hash_aggregate` as a thin wrapper**

```rust
fn visit_hash_aggregate(
    &mut self,
    op: &PhysicalHashAggregateOp,
    node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    let child = self.visit(&node.children[0])?;
    let agg_tuple_id = self.alloc_tuple();
    let agg_node_id = self.alloc_node();
    let (agg_plan_node, agg_scope) = self
        .lowering_ctx()
        .lower_aggregate(agg_node_id, agg_tuple_id, op, &child.scope)?;
    let mut plan_nodes = vec![agg_plan_node];
    plan_nodes.extend(child.plan_nodes);
    Ok(VisitResult {
        plan_nodes,
        scope: agg_scope,
        tuple_ids: vec![agg_tuple_id],
        cte_exchange_nodes: child.cte_exchange_nodes,
        ordering: OrderingSpec::Any,
    })
}
```

(Note the alloc order — tuple then node — matches the original `visit_hash_aggregate:2315-2316`.)

- [ ] **Step 4: Move the sort body into `LoweringCtx::lower_sort`**

Cut the body of `visit_sort` (`fragment_builder.rs:2513-2607`). Changes:
1. Remove `let child = self.visit(&node.children[0])?;` and `let sort_node_id = self.alloc_node();` — params.
2. Replace `child.scope` → `child_scope`, `child.tuple_ids` → `row_tuples` (param), `node.output_columns` → `output_columns` (param).
3. `self.slot_ref_exprs_for_columns(...)` now resolves via the trait (Step 1).
4. Return the `sort_plan_node` (the function builds one `TPlanNode`).

```rust
impl<'s, 'a, S: LoweringStateAccess<'a> + ?Sized> LoweringCtx<'s, 'a, S> {
    pub(crate) fn lower_sort(
        &mut self,
        sort_node_id: i32,
        op: &PhysicalSortOp,
        output_columns: &[AnalysisOutputColumn],
        child_scope: &ExprScope,
        row_tuples: &[i32],
    ) -> Result<plan_nodes::TPlanNode, String> {
        // ... moved body from visit_sort:2515-2595, building `sort_plan_node`
        //     (TSortNode, use_top_n=false, offset:None). ordering/analytic-
        //     partition compile stay verbatim; row_tuples replaces
        //     child.tuple_ids; output_columns replaces node.output_columns.
        //     Return sort_plan_node.
    }
}
```

- [ ] **Step 5: Rewrite `visit_sort` as a thin wrapper**

```rust
fn visit_sort(
    &mut self,
    op: &PhysicalSortOp,
    node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    let child = self.visit(&node.children[0])?;
    let sort_node_id = self.alloc_node();
    let sort_plan_node = self.lowering_ctx().lower_sort(
        sort_node_id,
        op,
        &node.output_columns,
        &child.scope,
        &child.tuple_ids,
    )?;
    let mut plan_nodes = vec![sort_plan_node];
    plan_nodes.extend(child.plan_nodes);
    Ok(VisitResult {
        plan_nodes,
        scope: child.scope,
        tuple_ids: child.tuple_ids,
        cte_exchange_nodes: child.cte_exchange_nodes,
        ordering: OrderingSpec::from_sort_items(&op.items),
    })
}
```

- [ ] **Step 6: Run existing fragment_builder tests — behavior unchanged**

Run: `cargo test --lib sql::codegen::fragment_builder`
Expected: PASS, unchanged. This proves the extraction preserved behavior. If a sort/aggregate test fails, diff the moved body against the original.

- [ ] **Step 7: Commit**

```bash
git add src/sql/codegen/ir/lowering.rs src/sql/codegen/fragment_builder.rs
git commit -m "codegen/ir: extract lower_sort/lower_aggregate cores; visit_* delegate (no behavior change)"
```

---

## Task 3: Pass 1 — `build_distributed_plan` arms for Sort / HashAggregate

**Files:** Modify `src/sql/codegen/ir/build.rs`

- [ ] **Step 1: Add the failing tests**

In `build.rs` `#[cfg(test)] mod tests`, add helpers + tests (reuse the existing `scan_plan`/`project_plan`/`physical_node*` helpers already in this module):

```rust
#[test]
fn build_distributed_plan_shapes_sort_over_scan() {
    let physical = sort_plan(scan_plan());
    let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
    let root = &dp.fragments[0].root;
    assert!(matches!(root.body, DistributedPlanNodeBody::Sort(_)));
    // Sort passes the child's tuple through (no new tuple allocated).
    assert_eq!(root.tuple_ids, root.children[0].tuple_ids);
}

#[test]
fn build_distributed_plan_shapes_aggregate_over_scan() {
    let physical = aggregate_plan(scan_plan());
    let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
    let root = &dp.fragments[0].root;
    let DistributedPlanNodeBody::HashAggregate(agg) = &root.body else {
        panic!("expected aggregate root");
    };
    assert_eq!(agg.group_by.len(), 1);
    // Aggregate allocates its own output tuple (distinct from the scan's).
    assert_ne!(root.tuple_ids, root.children[0].tuple_ids);
}
```

Add these test helpers in the same module:

```rust
fn sort_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let output_columns = child.output_columns.clone();
    physical_node(
        Operator::PhysicalSort(crate::sql::optimizer::operator::PhysicalSortOp {
            items: vec![crate::sql::analysis::SortItem {
                expr: column_ref_expr(1, "k", DataType::Int64, false),
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_exprs: vec![],
        }),
        vec![child],
        output_columns,
    )
}

fn aggregate_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
    let group_col = output_col(1, "k", DataType::Int64, false);
    physical_node(
        Operator::PhysicalHashAggregate(crate::sql::optimizer::operator::PhysicalHashAggregateOp {
            mode: crate::sql::optimizer::operator::AggMode::Single,
            group_by: vec![column_ref_expr(1, "k", DataType::Int64, false)],
            aggregates: vec![],
            is_merge: vec![],
            output_columns: vec![group_col.clone()],
        }),
        vec![child],
        vec![group_col],
    )
}
```

(Confirm `SortItem`'s field names — `expr`/`asc`/`nulls_first` — against `src/sql/analysis/mod.rs:41`; adjust if the real struct differs.)

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test --lib sql::codegen::ir::build`
Expected: FAIL — `build_distributed_plan` errors `does not handle operator PhysicalSort/PhysicalHashAggregate`.

- [ ] **Step 3: Add the `Sort` and `HashAggregate` arms**

In `build.rs`, in `DistributedPlanBuilder::visit`, add before the catch-all `other =>`:

```rust
Operator::PhysicalSort(op) => {
    let child = expect_single_child(node, "PhysicalSort")?;
    let child = self.visit(child, fragment_id)?;
    let node_id = self.alloc_node();
    // Sort passes the child's tuples through unchanged (no new tuple).
    let tuple_ids = child.tuple_ids.clone();
    Ok(DistributedPlanNode {
        node_id,
        fragment_id,
        tuple_ids,
        nullable_tuple_ids: vec![],
        limit: -1,
        children: vec![child],
        stats: PlanNodeStats::from_statistics(&node.stats),
        body: DistributedPlanNodeBody::Sort(SortBody {
            items: op.items.clone(),
            analytic_partition_exprs: op.analytic_partition_exprs.clone(),
            output_columns: node.output_columns.clone(),
        }),
    })
}
Operator::PhysicalHashAggregate(op) => {
    let child = expect_single_child(node, "PhysicalHashAggregate")?;
    let child = self.visit(child, fragment_id)?;
    let node_id = self.alloc_node();
    let tuple_id = self.alloc_tuple();
    Ok(DistributedPlanNode {
        node_id,
        fragment_id,
        tuple_ids: vec![tuple_id],
        nullable_tuple_ids: vec![],
        limit: -1,
        children: vec![child],
        stats: PlanNodeStats::from_statistics(&node.stats),
        body: DistributedPlanNodeBody::HashAggregate(Box::new(AggregateBody {
            mode: op.mode,
            group_by: op.group_by.clone(),
            aggregates: op.aggregates.clone(),
            is_merge: op.is_merge.clone(),
            output_columns: op.output_columns.clone(),
        })),
    })
}
```

Add the imports at the top of `build.rs`:

```rust
use super::body::{AggregateBody, ProjectBody, ScanBody, SortBody};
```

- [ ] **Step 4: Run to confirm pass**

Run: `cargo test --lib sql::codegen::ir::build`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/ir/build.rs
git commit -m "codegen/ir: Pass 1 build_distributed_plan arms for sort/aggregate"
```

---

## Task 4: Pass 2 — `lower_node` arms + adapters for Sort / HashAggregate

**Files:** Modify `src/sql/codegen/ir/lowering.rs`

- [ ] **Step 1: Add the adapters**

Next to `scan_body_to_physical_op`/`project_body_to_physical_op` (`lowering.rs:947`), add:

```rust
fn sort_body_to_physical_op(body: &super::body::SortBody) -> PhysicalSortOp {
    PhysicalSortOp {
        items: body.items.clone(),
        analytic_partition_exprs: body.analytic_partition_exprs.clone(),
    }
}

fn aggregate_body_to_physical_op(body: &super::body::AggregateBody) -> PhysicalHashAggregateOp {
    PhysicalHashAggregateOp {
        mode: body.mode,
        group_by: body.group_by.clone(),
        aggregates: body.aggregates.clone(),
        is_merge: body.is_merge.clone(),
        output_columns: body.output_columns.clone(),
    }
}
```

Add imports for `PhysicalSortOp`/`PhysicalHashAggregateOp` at the top of `lowering.rs` if not present.

- [ ] **Step 2: Add the `lower_node` arms**

In `LoweringCtx::lower_node` (`lowering.rs:348`), **replace the temporary Sort/HashAggregate error arms added in Task 1 Step 3** with the real lowering:

```rust
super::node::DistributedPlanNodeBody::Sort(sort) => {
    if node.children.len() != 1 {
        return Err(format!(
            "DistributedPlan Sort node_id={} expected 1 child, got {}",
            node.node_id, node.children.len()
        ));
    }
    let child = self.lower_node(&node.children[0])?;
    let op = sort_body_to_physical_op(sort);
    let sort_plan_node = self.lower_sort(
        node.node_id,
        &op,
        &sort.output_columns,
        &child.scope,
        &node.tuple_ids, // == child tuple ids (passthrough, set in Pass 1)
    )?;
    let mut plan_nodes = vec![sort_plan_node];
    plan_nodes.extend(child.plan_nodes);
    Ok(LoweredDistributedNode {
        plan_nodes,
        scope: child.scope,
        output_columns: child.output_columns,
    })
}
super::node::DistributedPlanNodeBody::HashAggregate(agg) => {
    if node.children.len() != 1 {
        return Err(format!(
            "DistributedPlan HashAggregate node_id={} expected 1 child, got {}",
            node.node_id, node.children.len()
        ));
    }
    let agg_tuple_id = first_tuple_id(node, "HashAggregate")?;
    let child = self.lower_node(&node.children[0])?;
    let op = aggregate_body_to_physical_op(agg);
    let (agg_plan_node, scope) =
        self.lower_aggregate(node.node_id, agg_tuple_id, &op, &child.scope)?;
    let mut plan_nodes = vec![agg_plan_node];
    plan_nodes.extend(child.plan_nodes);
    Ok(LoweredDistributedNode {
        plan_nodes,
        scope,
        output_columns: agg.output_columns.clone(),
    })
}
```

(For aggregate, lower the child **before** reading `agg_tuple_id`? No — `first_tuple_id` just reads `node.tuple_ids[0]`, no ordering constraint. But call `self.lower_node(child)` and `self.lower_aggregate` in child-first order so slot allocation matches the old DFS. The order shown — read tuple id, then lower child, then lower_aggregate — is correct: `lower_aggregate` allocates this node's slots *after* the child's, matching `visit_hash_aggregate` which visits child first.)

- [ ] **Step 3: Build to confirm the match is exhaustive and compiles**

Run: `cargo build --lib 2>&1 | tail -5`
Expected: compiles (all 4 body variants handled in `lower_node`).

- [ ] **Step 4: Add a lowering smoke test**

In `lowering.rs` tests, add:

```rust
#[test]
fn build_via_distributed_plan_lowers_aggregate_over_scan() {
    let physical = aggregate_over_scan_plan(); // hand-built Aggregate(Scan), AggMode::Single
    let build = PlanFragmentBuilder::build_via_distributed_plan(
        &physical, &DummyCatalog, &ConnectorRegistry::new(), "default",
    ).expect("build_via_distributed_plan");
    let root = build.fragment_results.iter()
        .find(|f| f.fragment_id == build.root_fragment_id).unwrap();
    assert!(root.plan.nodes.iter().any(|n|
        n.node_type == plan_nodes::TPlanNodeType::AGGREGATION_NODE));
}
```

(Build `aggregate_over_scan_plan()` with the same hand-built-plan helpers used by the existing `lowering.rs` tests / `build.rs` tests.)

- [ ] **Step 5: Run**

Run: `cargo test --lib sql::codegen::ir::lowering`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/ir/lowering.rs
git commit -m "codegen/ir: Pass 2 lower_node arms + adapters for sort/aggregate"
```

---

## Task 5: Equivalence tests (`build_via_distributed_plan == build`)

**Files:** Modify `src/sql/codegen/ir/equiv.rs`

- [ ] **Step 1: Add equivalence cases**

In `equiv.rs` tests, add cases that route hand-built physical plans through both paths and assert byte-identical results via the existing `assert_distributed_plan_equivalent` helper:

```rust
#[test]
fn sort_over_scan_matches_direct_fragment_builder() {
    assert_distributed_plan_equivalent("sort_over_scan", sort_scan_plan());
}

#[test]
fn aggregate_single_over_scan_matches_direct_fragment_builder() {
    assert_distributed_plan_equivalent("aggregate_single", aggregate_scan_plan());
}

#[test]
fn aggregate_with_count_matches_direct_fragment_builder() {
    assert_distributed_plan_equivalent("aggregate_count", aggregate_count_scan_plan());
}

#[test]
fn sort_over_project_over_scan_matches_direct_fragment_builder() {
    assert_distributed_plan_equivalent("sort_project_scan", sort_project_scan_plan());
}
```

Add the four `*_plan()` builders next to the existing equiv plan builders, composing the existing `scan_plan`/`project_plan` helpers with `PhysicalSort`/`PhysicalHashAggregate` wrappers. `aggregate_single` = group-by-only (empty `aggregates`/`is_merge`); `aggregate_count` = one `count(*)`-style `AggregateCall` with `is_merge=[false]`, `mode=Single` (so `need_finalize=true`). Build the `AggregateCall` the same way the analyzer/optimizer does — copy the construction from an existing aggregate test in `fragment_builder.rs` (grep `AggregateCall {` in tests) so the call’s `result_type`/args are valid.

- [ ] **Step 2: Run the equivalence tests**

Run: `cargo test --lib sql::codegen::ir::equiv`
Expected: PASS (byte-identical `plan`/`desc_tbl`/`exec_params`). If `plan` inequality appears, `Debug`-diff the two `TPlan`s; the likely cause is slot-allocation order — confirm `lower_node` lowers child-before-parent and `lower_aggregate`/`lower_sort` allocate slots in the same sequence as the original `visit_*`.

- [ ] **Step 3: Run the full codegen suite (no regression)**

Run: `cargo test --lib sql::codegen`
Expected: PASS — slice-1 equivalence + old fragment_builder tests + new sort/aggregate tests all green.

- [ ] **Step 4: Commit**

```bash
git add src/sql/codegen/ir/equiv.rs
git commit -m "codegen/ir: equivalence harness for sort/aggregate (build_via_distributed_plan == build)"
```

---

## Self-review notes / forward items

- **Scope:** Sort + HashAggregate only, single-fragment, extract-and-share, equivalence-tested. `Limit` deferred to slice 3 (it is a 0-node fold that mutates Sort's `limit`/`offset` → groups with `TopN`). Exchange/fragmentation, multi-phase-agg-with-exchange, cutover are later slices.
- **Aggregate merge phase (`is_merge=true`)**: handled inside the moved `lower_aggregate` body (positional `child.scope.iter_columns()` binding), but **not exercised single-fragment** — a Global merge agg needs a Local child via an exchange, which arrives with fragmentation (S5). Slice-2 equivalence tests cover `AggMode::Single`. Flag: add a merge-phase equivalence case when S5 lands.
- **`slot_ref_exprs_for_columns` relocation** (Task 2 Step 1): if it turns out to touch builder-only state, keep it as a `LoweringStateAccess` method with two impls rather than a free fn.
- **Adapters** `sort_body_to_physical_op`/`aggregate_body_to_physical_op` are interim (same as slice-1's scan/project adapters); the cores can take `&SortBody`/`&AggregateBody` directly in a later cleanup once all slices land.
- **Dead-code clippy:** `build_via_distributed_plan`/`build_distributed_plan`/`lower_distributed_plan` remain unused outside tests until cutover (carried over from slice 1; same `#[allow(dead_code)]` treatment applies if not already added).
