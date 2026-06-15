# PlanNode IR — M0 Slice 1 (Scan / Filter / Project) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the owned `PlanNode`/`PlanFragment` IR and a parallel `build_via_ir` path that lowers Scan/Filter/Project through a two-pass `build_ir` (structure) + `lower_fragmented` (binding), producing a `MultiFragmentBuildResult` provably equivalent to today's `PlanFragmentBuilder::build` for those operators.

**Architecture:** Spec `docs/superpowers/specs/2026-06-15-plannode-ir-explain-observability-design.md`. This slice is the foundation of milestone **M0** (behavior-preserving IR introduction). It does **not** change the execution path: the engine keeps calling the old `build`; the new path runs only inside equivalence tests. Strategy = **extract-and-share**: each operator's lowering core moves out of `visit_*` into a `LoweringCtx` method that both the old builder and the new Pass-2 call, so Pass-2 behavior is identical by construction.

**Tech Stack:** Rust, thrift 0.17 generated types (`plan_nodes::TPlanNode`, `descriptors::TDescriptorTable`, all derive `PartialEq`+`Debug`), `cargo test`.

**Verbatim source anchors (read before starting):**
- `src/sql/codegen/fragment_builder.rs`: `VisitResult:65`, `alloc_node:1331`/`alloc_slot:1337`/`alloc_tuple:1351`, `visit_scan:1523-1989`, `visit_filter:1995-2026`, `visit_project:2227-2314`, `split_and_compile_conjuncts:1505`, `build_with_mv_refresh_ctx:806-964`.
- `src/sql/codegen/nodes.rs`: `default_plan_node:1925`, `build_scan_node:33`, `build_project_node:304`, `build_exec_params_multi_with_refresh_context:600`, `PlannedScanTable:24`.
- `src/sql/codegen/mod.rs`: `PlanBuildResult:64`, `FragmentBuildResult:151`, `MultiFragmentBuildResult:122`, `FragmentEdge:111`.
- `src/sql/codegen/resolve.rs`: `ExprScope:36`, `ColumnBinding:26`, `ResolvedTable:17`, `PlannedConnectorScan:11`.
- `src/sql/codegen/descriptors.rs`: `DescriptorTableBuilder` API (`add_slot`, `add_slot_with_type_desc`, `add_tuple`, `add_table`/`add_table_for_scan`, `build`, `widen_tuple_nullable`).
- `src/sql/optimizer/operator.rs`: `PhysicalScanOp:298`, `PhysicalProjectOp:330`, `PhysicalFilterOp:325`.

**Run tests for this slice:** `cargo test --lib sql::codegen`

---

## File Structure

- **Create** `src/sql/codegen/ir/mod.rs` — IR module root, re-exports.
- **Create** `src/sql/codegen/ir/node.rs` — `PlanNode`, `PlanNodeStats`, `PlanNodeBody` (Scan/Project variants this slice; more variants added by later slices).
- **Create** `src/sql/codegen/ir/body.rs` — `ScanBody`, `ProjectBody`.
- **Create** `src/sql/codegen/ir/fragment.rs` — `PlanFragment`, `FragmentedPlan`, `DataSink`, `DataPartition`, `PartitionKind`.
- **Create** `src/sql/codegen/ir/build.rs` — Pass 1 `build_ir` (PhysicalPlanNode → FragmentedPlan) for Scan/Filter/Project.
- **Create** `src/sql/codegen/ir/lowering.rs` — `LoweringCtx` + Pass 2 `lower_fragmented` (FragmentedPlan → MultiFragmentBuildResult) + the extracted `lower_scan`/`lower_project` cores.
- **Create** `src/sql/codegen/ir/equiv.rs` — `#[cfg(test)]` canonicalization + equivalence assertion helpers.
- **Modify** `src/sql/codegen/mod.rs` — `pub(crate) mod ir;`.
- **Modify** `src/sql/codegen/fragment_builder.rs` — extract `lower_scan`/`lower_project` cores onto `LoweringCtx`; `visit_scan`/`visit_project` delegate to them (behavior unchanged). Add `pub(crate) fn build_via_ir(...)`.

The two passes are deliberately split into `build.rs` and `lowering.rs` so each file has one responsibility and stays small.

---

## Task 1: IR core types (Scan/Project)

**Files:**
- Create: `src/sql/codegen/ir/mod.rs`, `ir/node.rs`, `ir/body.rs`, `ir/fragment.rs`
- Modify: `src/sql/codegen/mod.rs`

- [ ] **Step 1: Register the module**

In `src/sql/codegen/mod.rs`, add next to the other `mod` declarations:

```rust
pub(crate) mod ir;
```

- [ ] **Step 2: Write `ir/mod.rs`**

```rust
//! Owned PlanNode/PlanFragment IR (spec 2026-06-15-plannode-ir-explain-observability).
//! Single source from which both EXPLAIN and thrift derive. This slice covers
//! Scan/Filter/Project; later slices add the remaining operators.

pub(crate) mod body;
pub(crate) mod build;
pub(crate) mod fragment;
pub(crate) mod lowering;
pub(crate) mod node;

#[cfg(test)]
pub(crate) mod equiv;

pub(crate) use build::build_ir;
pub(crate) use fragment::{DataPartition, DataSink, FragmentedPlan, PartitionKind, PlanFragment};
pub(crate) use lowering::lower_fragmented;
pub(crate) use node::{PlanNode, PlanNodeBody, PlanNodeStats};

pub(crate) type FragmentId = u32;
```

- [ ] **Step 3: Write `ir/node.rs`**

```rust
use crate::sql::analysis::TypedExpr;
use crate::sql::optimizer::statistics::{Confidence, Statistics};

use super::FragmentId;
use super::body::{ProjectBody, ScanBody};

/// Self-contained copy of the estimated stats this node carries, so EXPLAIN /
/// ANALYZE never reach back into `PhysicalPlanNode`.
#[derive(Clone, Debug)]
pub(crate) struct PlanNodeStats {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,
}

impl PlanNodeStats {
    pub fn from_statistics(stats: &Statistics) -> Self {
        Self {
            output_row_count: stats.output_row_count,
            row_count_confidence: stats.row_count_confidence,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PlanNode {
    /// Allocated once in Pass 1; never reallocated. In a thrift-lowered
    /// fragment every PlanNode produces exactly one TPlanNode, so
    /// `node_id == TPlanNode.node_id == profile plan_node_id`.
    pub node_id: i32,
    pub fragment_id: FragmentId,
    /// Output tuples (thrift `row_tuples`). Allocated in Pass 1.
    pub tuple_ids: Vec<i32>,
    /// Subset of `tuple_ids` widened to nullable (outer-join side). Empty here.
    pub nullable_tuple_ids: Vec<i32>,
    /// -1 == no limit.
    pub limit: i64,
    pub children: Vec<PlanNode>,
    pub stats: PlanNodeStats,
    pub body: PlanNodeBody,
}

/// Operator-specific payload. Grows one variant per operator as slices land.
/// Filter has no variant: its predicate folds into the child's `ScanBody.predicates`.
#[derive(Clone, Debug)]
pub(crate) enum PlanNodeBody {
    Scan(Box<ScanBody>),
    Project(ProjectBody),
}
```

- [ ] **Step 4: Write `ir/body.rs`**

`ScanBody` mirrors `PhysicalScanOp`'s fields and `ProjectBody` mirrors `PhysicalProjectOp`'s, so the lowering cores extracted in Task 2 reference `scan_body.*` / `project_body.*` in place of `op.*` verbatim.

```rust
use crate::sql::analysis::TypedExpr;
use crate::sql::catalog::TableDef;
use crate::sql::analysis::ProjectItem;
use crate::sql::planner::plan::{ScanDictionaryColumn, ScanVariantColumn};

#[derive(Clone, Debug)]
pub(crate) struct ScanBody {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<crate::sql::analysis::OutputColumn>,
    /// Scan predicates plus any folded Filter conjuncts (see build_ir filter handling).
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub dict_columns: Vec<ScanDictionaryColumn>,
    pub variant_columns: Vec<ScanVariantColumn>,
    pub mv_rewritten_from: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectBody {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
}
```

(Confirm the exact import path of `ProjectItem`/`OutputColumn` against `src/sql/analysis/mod.rs:107`/`:29`; adjust the `use` if they live under a submodule.)

- [ ] **Step 5: Write `ir/fragment.rs`**

```rust
use crate::sql::analysis::{OutputColumn, TypedExpr};

use super::FragmentId;
use super::node::PlanNode;

#[derive(Clone, Debug)]
pub(crate) enum PartitionKind {
    Unpartitioned,
    Random,
    Hash,
}

#[derive(Clone, Debug)]
pub(crate) struct DataPartition {
    pub kind: PartitionKind,
    pub exprs: Vec<TypedExpr>,
}

impl DataPartition {
    pub const UNPARTITIONED: DataPartition = DataPartition {
        kind: PartitionKind::Unpartitioned,
        exprs: Vec::new(),
    };
}

/// Sink intent. This slice only produces the root result sink.
#[derive(Clone, Debug)]
pub(crate) enum DataSink {
    Result,
    Noop,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFragment {
    pub fragment_id: FragmentId,
    pub root: PlanNode,
    pub data_partition: DataPartition,
    pub output_partition: DataPartition,
    pub sink: DataSink,
    pub output_exprs: Option<Vec<TypedExpr>>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct FragmentedPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
}
```

- [ ] **Step 6: Run build + a construction unit test**

Add to `ir/node.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::statistics::Statistics;

    #[test]
    fn plan_node_stats_copies_row_count() {
        let stats = Statistics { output_row_count: 7.0, ..Default::default() };
        let s = PlanNodeStats::from_statistics(&stats);
        assert_eq!(s.output_row_count, 7.0);
    }
}
```

Run: `cargo test --lib sql::codegen::ir`
Expected: compiles; the one test PASSES. (Unused-field warnings on IR types are acceptable this slice — they get consumed in Tasks 3–4.)

- [ ] **Step 7: Commit**

```bash
git add src/sql/codegen/ir/ src/sql/codegen/mod.rs
git commit -m "codegen/ir: add PlanNode/PlanFragment IR core types (scan/project slice)"
```

---

## Task 2: Extract lowering cores (`LoweringCtx::lower_scan`/`lower_project`)

Goal: move the *bodies* of `visit_scan`/`visit_project` into reusable methods that take pre-allocated ids and an explicit child result, leaving the old visitors as thin wrappers. **Behavior must not change** — existing `sql::codegen::fragment_builder` tests stay green. This is the load-bearing, no-behavior-change refactor.

**Files:** Modify `src/sql/codegen/fragment_builder.rs`; new core lives on a `LoweringCtx` in `src/sql/codegen/ir/lowering.rs`.

- [ ] **Step 1: Define `LoweringCtx` holding the mutable build state the cores touch**

In `ir/lowering.rs`:

```rust
use std::cell::RefCell;
use std::rc::Rc;

use crate::sql::catalog::CatalogProvider;
use crate::sql::codegen::descriptors::DescriptorTableBuilder;
use crate::sql::codegen::nodes::PlannedScanTable;

/// Mutable state shared by every per-operator lowering core. This is exactly
/// the subset of today's `PlanFragmentBuilder` fields the cores mutate.
pub(crate) struct LoweringCtx<'a> {
    pub catalog: &'a dyn CatalogProvider,
    pub connectors: &'a crate::connector::ConnectorRegistry,
    pub desc_builder: DescriptorTableBuilder,
    pub scan_tables: Vec<PlannedScanTable>,
    pub next_slot_id: Rc<RefCell<i32>>,
    // dict accumulators carried for parity with today's builder; unused until
    // a later slice exercises dict_columns (always empty in this slice).
    pub query_global_dicts_per_fragment:
        std::collections::HashMap<u32, Vec<crate::data::TGlobalDict>>,
    pub slot_to_global_dict: std::collections::HashMap<i32, crate::data::TGlobalDict>,
}

impl<'a> LoweringCtx<'a> {
    pub fn alloc_slot(&self) -> i32 {
        let mut next = self.next_slot_id.borrow_mut();
        let id = *next;
        *next += 1;
        id
    }
}
```

- [ ] **Step 2: Move the scan body into `LoweringCtx::lower_scan`**

Cut the body of `visit_scan` (`fragment_builder.rs:1528-1988`, i.e. everything after the signature up to the `Ok(VisitResult { ... })`) into a new method. Two surgical changes:
1. Remove the first two lines `let scan_tuple_id = self.alloc_tuple();` / `let scan_node_id = self.alloc_node();` — receive them as parameters instead.
2. Replace `self.` field accesses with `self.` on the ctx where the field exists on `LoweringCtx` (`desc_builder`, `scan_tables`, `connectors`, `catalog`, `slot_to_global_dict`, `query_global_dicts_per_fragment`); replace `self.alloc_slot()` with `self.alloc_slot()` (now on ctx); replace `self.refresh_scan_table_for_codegen` / `self.propagate_dict_to_slot` by moving those helpers onto `LoweringCtx` too (they touch only ctx fields).

Signature (returns the same triple the body already computes):

```rust
impl<'a> LoweringCtx<'a> {
    pub fn lower_scan(
        &mut self,
        scan_node_id: i32,
        scan_tuple_id: i32,
        op: &crate::sql::optimizer::operator::PhysicalScanOp,
    ) -> Result<(crate::plan_nodes::TPlanNode, crate::sql::codegen::resolve::ExprScope), String> {
        // ... moved body from visit_scan:1530-1981, returning (scan_plan_node, scope) ...
    }
}
```

(The body already builds `scan_plan_node` and `scope` and pushes the `PlannedScanTable` into `self.scan_tables`; keep that push. Drop the `VisitResult` wrapper — the caller rebuilds it.)

- [ ] **Step 3: Rewrite `visit_scan` as a thin wrapper that calls the core**

```rust
fn visit_scan(
    &mut self,
    op: &PhysicalScanOp,
    _node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    let scan_tuple_id = self.alloc_tuple();
    let scan_node_id = self.alloc_node();
    let (scan_plan_node, scope) = self.lowering_ctx().lower_scan(scan_node_id, scan_tuple_id, op)?;
    Ok(VisitResult {
        plan_nodes: vec![scan_plan_node],
        scope,
        tuple_ids: vec![scan_tuple_id],
        cte_exchange_nodes: Vec::new(),
        ordering: OrderingSpec::Any,
    })
}
```

For this to compile, `PlanFragmentBuilder` must expose its mutable state as a `LoweringCtx`. Simplest path: give `PlanFragmentBuilder` a `fn lowering_ctx(&mut self) -> LoweringCtx<'_>` that borrows its existing fields into a `LoweringCtx`. Because both structs hold the *same* `Rc<RefCell<i32>>` slot allocator and move the `DescriptorTableBuilder`/`scan_tables` by `&mut`, allocations stay shared.

> Note: borrowing several disjoint fields of `PlanFragmentBuilder` into a `LoweringCtx` requires a struct-of-references form, or making `LoweringCtx` borrow `&mut PlanFragmentBuilder` directly. If the borrow checker fights the disjoint-field approach, define `LoweringCtx` over `&mut PlanFragmentBuilder` for now (it owns all the fields) and move to owned state in Task 4 where `lower_fragmented` constructs a standalone `LoweringCtx`. Pick whichever compiles cleanly; the cores' bodies are identical either way.

- [ ] **Step 4: Move the project body into `LoweringCtx::lower_project`**

Cut `visit_project`'s body (`fragment_builder.rs:2227-2313`). It already takes the child via `self.visit(...)`; the core instead receives the child's `VisitResult` (it only reads `child.scope`, `child.plan_nodes`, `child.cte_exchange_nodes`). Parameterize `project_tuple_id`/`project_node_id`:

```rust
impl<'a> LoweringCtx<'a> {
    pub fn lower_project(
        &mut self,
        project_node_id: i32,
        project_tuple_id: i32,
        op: &crate::sql::optimizer::operator::PhysicalProjectOp,
        child_scope: &crate::sql::codegen::resolve::ExprScope,
    ) -> Result<(crate::plan_nodes::TPlanNode, crate::sql::codegen::resolve::ExprScope, Vec<crate::sql::analysis::OutputColumn>), String> {
        // ... moved body from visit_project:2231-2310 (the for-loop over op.items,
        // slot alloc, desc_builder.add_slot_with_type_desc, slot_map, scope build,
        // propagate_dict_to_slot), returning (project_plan_node, project_scope, output_columns) ...
    }
}
```

- [ ] **Step 5: Rewrite `visit_project` as a thin wrapper**

```rust
fn visit_project(
    &mut self,
    op: &PhysicalProjectOp,
    node: &PhysicalPlanNode,
) -> Result<VisitResult, String> {
    let child = self.visit(&node.children[0])?;
    let project_tuple_id = self.alloc_tuple();
    let project_node_id = self.alloc_node();
    let (project_plan_node, project_scope, _output_columns) =
        self.lowering_ctx().lower_project(project_node_id, project_tuple_id, op, &child.scope)?;
    let mut plan_nodes = vec![project_plan_node];
    plan_nodes.extend(child.plan_nodes);
    Ok(VisitResult {
        plan_nodes,
        scope: project_scope,
        tuple_ids: vec![project_tuple_id],
        cte_exchange_nodes: child.cte_exchange_nodes,
        ordering: OrderingSpec::Any,
    })
}
```

- [ ] **Step 6: Run the existing fragment_builder tests — behavior must be unchanged**

Run: `cargo test --lib sql::codegen::fragment_builder`
Expected: PASS, unchanged from before the refactor (this is the proof the extraction preserved behavior). If any scan/project test fails, the extraction altered behavior — diff the moved body against the original.

- [ ] **Step 7: Commit**

```bash
git add src/sql/codegen/ir/lowering.rs src/sql/codegen/fragment_builder.rs
git commit -m "codegen/ir: extract lower_scan/lower_project cores; visit_* delegate (no behavior change)"
```

---

## Task 3: Pass 1 — `build_ir` for Scan/Filter/Project

**Files:** `src/sql/codegen/ir/build.rs`

Pass 1 walks `PhysicalPlanNode`, allocates `node_id`/`fragment_id`/`tuple_id` (NOT slots), translates ops into IR bodies, and folds Filter into the child scan's `predicates`. It produces a single-fragment `FragmentedPlan` (this slice has no exchange).

- [ ] **Step 1: Write the failing test (hand-built scan→project plan)**

Add `#[cfg(test)] mod tests` in `ir/build.rs`. Reuse the test construction pattern from `fragment_builder.rs:7131` (`scan_plan`) and `physical_node_for_test`/`output_columns` helpers — import or duplicate the minimal helpers needed to hand-build:

```rust
#[test]
fn build_ir_scan_project_shapes_one_fragment() {
    let physical = scan_then_project_plan(); // helper: Project over Scan, hand-built
    let fp = build_ir(&physical).expect("build_ir");
    assert_eq!(fp.fragments.len(), 1);
    let root = &fp.fragments[0].root;
    // node ids allocated top-down from 1: project=1, scan=2 (mirrors alloc_node order)
    assert!(matches!(root.body, crate::sql::codegen::ir::PlanNodeBody::Project(_)));
    assert_eq!(root.children.len(), 1);
    assert!(matches!(root.children[0].body, crate::sql::codegen::ir::PlanNodeBody::Scan(_)));
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test --lib sql::codegen::ir::build`
Expected: FAIL — `build_ir` not yet implemented / `scan_then_project_plan` undefined.

- [ ] **Step 3: Implement `build_ir`**

```rust
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;

use super::body::{ProjectBody, ScanBody};
use super::fragment::{DataPartition, DataSink, FragmentedPlan, PlanFragment};
use super::node::{PlanNode, PlanNodeBody, PlanNodeStats};
use super::FragmentId;

struct IrBuilder {
    next_node_id: i32,
    next_tuple_id: i32,
}

impl IrBuilder {
    fn alloc_node(&mut self) -> i32 { let id = self.next_node_id; self.next_node_id += 1; id }
    fn alloc_tuple(&mut self) -> i32 { let id = self.next_tuple_id; self.next_tuple_id += 1; id }

    /// Returns the IR subtree root for this physical node. Filter returns its
    /// child's subtree after folding the predicate (no node of its own).
    fn visit(&mut self, node: &PhysicalPlanNode, fragment_id: FragmentId) -> Result<PlanNode, String> {
        match &node.op {
            Operator::PhysicalScan(op) => {
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(PlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    children: vec![],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: PlanNodeBody::Scan(Box::new(ScanBody {
                        database: op.database.clone(),
                        table: op.table.clone(),
                        alias: op.alias.clone(),
                        columns: op.columns.clone(),
                        predicates: op.predicates.clone(),
                        required_columns: op.required_columns.clone(),
                        dict_columns: op.dict_columns.clone(),
                        variant_columns: op.variant_columns.clone(),
                        mv_rewritten_from: op.mv_rewritten_from.clone(),
                    })),
                })
            }
            Operator::PhysicalFilter(op) => {
                // Fold into the child scan's predicates, mirroring visit_filter.
                let mut child = self.visit(&node.children[0], fragment_id)?;
                fold_filter_into_scan(&mut child, &op.predicate)?;
                Ok(child)
            }
            Operator::PhysicalProject(op) => {
                // Children-first, matching the old visitor: visit child, THEN
                // allocate this node's ids (node/tuple are independent counters,
                // so node-vs-tuple order within a node is irrelevant — only the
                // cross-node DFS order must match the old builder).
                let child = self.visit(&node.children[0], fragment_id)?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(PlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    children: vec![child],
                    stats: PlanNodeStats::from_statistics(&node.stats),
                    body: PlanNodeBody::Project(ProjectBody {
                        items: op.items.clone(),
                        output_qualifier: op.output_qualifier.clone(),
                    }),
                })
            }
            other => Err(format!("build_ir slice 1 does not handle operator {other:?}")),
        }
    }
}

/// Append the filter's AND-split conjuncts to the child scan's `predicates`.
/// Mirrors visit_filter, which pushes filter conjuncts onto the scan node.
fn fold_filter_into_scan(child: &mut PlanNode, predicate: &crate::sql::analysis::TypedExpr) -> Result<(), String> {
    use crate::sql::codegen::helpers::split_and_conjuncts_typed;
    let target = scan_body_mut(child)
        .ok_or_else(|| "slice 1: Filter child is not a Scan".to_string())?;
    for conj in split_and_conjuncts_typed(predicate) {
        target.predicates.push(conj.clone());
    }
    Ok(())
}

fn scan_body_mut(node: &mut PlanNode) -> Option<&mut ScanBody> {
    match &mut node.body {
        PlanNodeBody::Scan(b) => Some(b),
        _ => node.children.first_mut().and_then(scan_body_mut),
    }
}

pub(crate) fn build_ir(plan: &PhysicalPlanNode) -> Result<FragmentedPlan, String> {
    let mut b = IrBuilder { next_node_id: 1, next_tuple_id: 1 };
    let root_fragment_id: FragmentId = 0;
    let root = b.visit(plan, root_fragment_id)?;
    let fragment = PlanFragment {
        fragment_id: root_fragment_id,
        root,
        data_partition: DataPartition::UNPARTITIONED,
        output_partition: DataPartition::UNPARTITIONED,
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: plan.output_columns.clone(),
    };
    Ok(FragmentedPlan { fragments: vec![fragment], root_fragment_id })
}
```

> **Filter-fold parity note:** folding the filter predicate into `ScanBody.predicates` (so `lower_scan` compiles scan predicates + filter conjuncts together) must reproduce the old split path, where `visit_scan` lowers `op.predicates` and `visit_filter` separately appends its conjuncts to the scan node + `min_max` + `scan_tables`. Order is preserved (op.predicates first, then folded filter conjuncts). Whether the `min_max` treatment matches exactly is **verified by the `scan_filter_plan` case in the Task 5 equivalence test** — not assumed. If it diverges, the fix is to mirror `visit_filter`'s `append_hdfs_scan_min_max_conjuncts` call inside `lower_scan`'s predicate handling.

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test --lib sql::codegen::ir::build`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/ir/build.rs
git commit -m "codegen/ir: Pass 1 build_ir for scan/filter/project"
```

---

## Task 4: Pass 2 — `lower_fragmented` + `build_via_ir`

**Files:** `src/sql/codegen/ir/lowering.rs`; `src/sql/codegen/fragment_builder.rs` (add `build_via_ir`)

Pass 2 walks the `FragmentedPlan`, calls the extracted `lower_scan`/`lower_project` cores (children-first) to allocate slots/build desc_tbl/compile exprs, then assembles a `MultiFragmentBuildResult` identical in shape to `build_with_mv_refresh_ctx`'s single-fragment output (`fragment_builder.rs:873-941`).

- [ ] **Step 1: Write `lower_node` (DFS, children-first)**

```rust
impl<'a> LoweringCtx<'a> {
    /// Lower one IR node to its thrift nodes (pre-order: self then children),
    /// returning (pre_order_nodes, output_scope, output_columns).
    fn lower_node(
        &mut self,
        node: &super::node::PlanNode,
    ) -> Result<(Vec<crate::plan_nodes::TPlanNode>, crate::sql::codegen::resolve::ExprScope, Vec<crate::sql::analysis::OutputColumn>), String> {
        match &node.body {
            super::node::PlanNodeBody::Scan(scan) => {
                let op = scan_body_to_op(scan); // reconstruct PhysicalScanOp view for the core
                let (tnode, scope) =
                    self.lower_scan(node.node_id, node.tuple_ids[0], &op)?;
                let out_cols = op.columns.clone();
                Ok((vec![tnode], scope, out_cols))
            }
            super::node::PlanNodeBody::Project(proj) => {
                let child = &node.children[0];
                let (mut child_nodes, child_scope, _child_cols) = self.lower_node(child)?;
                let op = project_body_to_op(proj);
                let (tnode, scope, out_cols) =
                    self.lower_project(node.node_id, node.tuple_ids[0], &op, &child_scope)?;
                let mut nodes = vec![tnode];
                nodes.append(&mut child_nodes);
                Ok((nodes, scope, out_cols))
            }
        }
    }
}
```

> `scan_body_to_op`/`project_body_to_op` build a `PhysicalScanOp`/`PhysicalProjectOp` from the IR body (the inverse of build_ir's clone). Since the cores were written against the op types in Task 2, this keeps the cores untouched. (A later cleanup can change the cores to take `&ScanBody` directly and delete these adapters — out of scope for slice 1.)

- [ ] **Step 2: Write `lower_fragmented` assembling the result**

Mirror the assembly at `fragment_builder.rs:873-941` exactly (root fragment, shared `desc_tbl`, `exec_params`, result sink, output exprs/columns, boundary schema). Reuse `nodes::build_exec_params_multi_with_refresh_context`, `build_result_sink`, `result_root_boundary_schema_report`, `result_output_exprs_for_columns`, `output_columns_for_boundary`.

```rust
pub(crate) fn lower_fragmented(
    fp: &super::fragment::FragmentedPlan,
    catalog: &dyn crate::sql::catalog::CatalogProvider,
    connectors: &crate::connector::ConnectorRegistry,
) -> Result<crate::sql::codegen::MultiFragmentBuildResult, String> {
    let root = fp.fragments.iter().find(|f| f.fragment_id == fp.root_fragment_id)
        .ok_or("lower_fragmented: missing root fragment")?;
    let mut ctx = LoweringCtx {
        catalog,
        connectors,
        desc_builder: DescriptorTableBuilder::new(),
        scan_tables: Vec::new(),
        next_slot_id: std::rc::Rc::new(std::cell::RefCell::new(1)),
        query_global_dicts_per_fragment: Default::default(),
        slot_to_global_dict: Default::default(),
    };
    let (plan_nodes, result_scope, _out_cols) = ctx.lower_node(&root.root)?;
    let desc_tbl = std::mem::replace(&mut ctx.desc_builder, DescriptorTableBuilder::new()).build();
    let exec_params = crate::sql::codegen::nodes::build_exec_params_multi_with_refresh_context(
        connectors, &ctx.scan_tables, None,
    )?;
    // ... build output_exprs from result_scope + root.output_columns,
    //     output_columns_for_boundary, boundary schema, FragmentBuildResult with
    //     plan: TPlan::new(plan_nodes), result sink, direct_exec: None, dicts: None.
    //     Return MultiFragmentBuildResult { fragment_results: vec![root_fragment],
    //         root_fragment_id: fp.root_fragment_id, edges: vec![], boundary_schemas, rf_plan: None }
}
```

- [ ] **Step 3: Add `build_via_ir` on `PlanFragmentBuilder`**

```rust
// fragment_builder.rs
impl<'a> PlanFragmentBuilder<'a> {
    /// Slice-1 IR path: build_ir + lower_fragmented. Parallel to `build`;
    /// used only by equivalence tests until M0 cutover.
    pub(crate) fn build_via_ir(
        plan: &PhysicalPlanNode,
        catalog: &'a dyn CatalogProvider,
        connectors: &'a crate::connector::ConnectorRegistry,
        _current_database: &str,
    ) -> Result<MultiFragmentBuildResult, String> {
        let fp = crate::sql::codegen::ir::build_ir(plan)?;
        crate::sql::codegen::ir::lower_fragmented(&fp, catalog, connectors)
    }
}
```

- [ ] **Step 4: Unit test — `build_via_ir` produces a scan node**

```rust
#[test]
fn build_via_ir_lowers_scan_project_to_thrift() {
    let physical = scan_then_project_plan();
    let build = PlanFragmentBuilder::build_via_ir(&physical, &DummyCatalog, &ConnectorRegistry::new(), "default").expect("build_via_ir");
    let root = build.fragment_results.iter().find(|f| f.fragment_id == build.root_fragment_id).unwrap();
    assert!(root.plan.nodes.iter().any(|n| n.node_type == plan_nodes::TPlanNodeType::PROJECT_NODE));
    assert!(root.plan.nodes.iter().any(|n| matches!(n.node_type,
        plan_nodes::TPlanNodeType::HDFS_SCAN_NODE | plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)));
}
```

Run: `cargo test --lib sql::codegen::ir::lowering`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/ir/lowering.rs src/sql/codegen/fragment_builder.rs
git commit -m "codegen/ir: Pass 2 lower_fragmented + build_via_ir (scan/project slice)"
```

---

## Task 5: Equivalence harness + tests

**Files:** `src/sql/codegen/ir/equiv.rs` (test-only)

Prove the IR path matches the old builder for scan/filter/project. Because thrift types derive `PartialEq`, compare after **canonicalizing** transient ids (slot/tuple/node ids assigned in possibly-different order). Since Pass-2 reuses the same cores, ids should already match; the canonicalizer is the safety net the spec mandates (`§6`).

- [ ] **Step 1: Write the canonicalizer**

```rust
//! Test-only equivalence checks between the old `build` and new `build_via_ir`.
use crate::plan_nodes::TPlan;

/// Relabel node ids by first occurrence in pre-order so two structurally-equal
/// plans compare equal even if absolute ids differ. (Slot/tuple ids inside
/// TExpr/desc_tbl get the same treatment in step 2 if needed.)
fn canonical_node_ids(plan: &TPlan) -> Vec<i32> {
    plan.nodes.iter().map(|n| n.node_id).collect()
}
```

- [ ] **Step 2: Write the equivalence test over representative plans**

```rust
#[test]
fn ir_path_matches_old_builder_scan_filter_project() {
    for physical in [scan_plan(), scan_filter_plan(), scan_then_project_plan(), scan_filter_project_plan()] {
        let old = PlanFragmentBuilder::build(&physical, &DummyCatalog, &ConnectorRegistry::new(), "default").unwrap();
        let new = PlanFragmentBuilder::build_via_ir(&physical, &DummyCatalog, &ConnectorRegistry::new(), "default").unwrap();
        let oroot = old.fragment_results.iter().find(|f| f.fragment_id == old.root_fragment_id).unwrap();
        let nroot = new.fragment_results.iter().find(|f| f.fragment_id == new.root_fragment_id).unwrap();
        assert_eq!(oroot.plan, nroot.plan, "thrift TPlan must match for {physical:?}");
        assert_eq!(oroot.desc_tbl, nroot.desc_tbl, "desc_tbl must match");
        assert_eq!(oroot.exec_params, nroot.exec_params, "exec_params must match");
    }
}
```

(Build the four `*_plan()` helpers by composing the `scan_plan` pattern from `fragment_builder.rs:7131` with `PhysicalFilterOp`/`PhysicalProjectOp` wrappers. If raw `assert_eq!` on `plan` fails only on absolute ids, switch to comparing `canonical_node_ids(&oroot.plan) == canonical_node_ids(&nroot.plan)` plus per-node field equality — but first confirm whether absolute equality already holds, since the shared cores allocate identically.)

- [ ] **Step 3: Run the equivalence tests**

Run: `cargo test --lib sql::codegen::ir::equiv`
Expected: PASS. If `plan` inequality appears, diff the two `TPlan`s (they `Debug`-print) to find where the IR path diverges; the most likely cause is id allocation order — fix by matching `build_ir`'s alloc order to the old visitor's (node before tuple, parent before child), not by loosening the assertion.

- [ ] **Step 4: Run the full codegen suite to confirm no regression**

Run: `cargo test --lib sql::codegen`
Expected: PASS (old fragment_builder tests + new ir tests).

- [ ] **Step 5: Commit**

```bash
git add src/sql/codegen/ir/equiv.rs
git commit -m "codegen/ir: equivalence harness proving build_via_ir == build for scan/filter/project"
```

---

## Self-review notes (carried into later slices)

- **Scope covered vs spec M0-S1/S2:** IR core types (subset), extract-and-share cores, build_ir + lower_fragmented + build_via_ir for Scan/Filter/Project, equivalence harness. The engine cutover/flag (spec M0-S6) and the remaining operator slices (Sort/Agg, Joins/SetOps, Fragmentation) are **out of scope** for this plan and get their own plans.
- **No runtime flag this slice:** the engine keeps calling old `build`; `build_via_ir` runs only in tests. This is deliberate — cutover is a later, separately-verified slice.
- **`scan_body_to_op`/`project_body_to_op` adapters** are interim. Once all slices land, the cores should take `&ScanBody`/`&ProjectBody` directly and the adapters + the op clones in `build_ir` collapse. Tracked as cleanup, not this slice.
- **Borrow-checker fallback** (Task 2 Step 3): if disjoint-field borrowing of `PlanFragmentBuilder` into `LoweringCtx` is awkward, make `LoweringCtx` wrap `&mut PlanFragmentBuilder`. Either compiles; the core bodies are identical.
