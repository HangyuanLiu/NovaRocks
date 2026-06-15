# OQ-13 Ranking-Window Predicate Pushdown (top/rank-per-group) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the standalone optimizer rewrite `Filter(rank_col <= k)` over `Window(row_number/rank/dense_rank PARTITION BY p ORDER BY o)` into a per-partition TopN that truncates each partition to the top-K superset before the analytic operator — keeping the Window + Filter so results are row-for-row identical, while reducing rows into the analytic operator.

**Architecture:** Built bottom-up in 5 phases. Phase 1–2 add per-partition TopN to the **execution engine** (exec `SortNode` fields + sorter partition-boundary truncation + lowering) — independently testable and mergeable. Phase 3 adds `partition_limit`/`topn_type` to the optimizer Sort op and wires convert/implement/codegen/explain. Phase 4 adds the `RankingWindowPredicatePushdown` logical rewrite rule. Phase 5 adds plan goldens, correctness cases, and the OQ-13 closeout artifacts (tpc-h q2/q17 WINDOW golden, scalar-multi-row error case).

**Tech Stack:** Rust. Standalone optimizer (`src/sql/optimizer/`), planner (`src/sql/planner/`), codegen (`src/sql/codegen/`), thrift lowering (`src/lower/`), execution operators (`src/exec/operators/sort/`), sql-test golden runner (`sql-tests/`).

**Design spec:** `docs/design/specs/2026-06-15-oq13-ranking-window-predicate-pushdown-design.md`

**Build/test note:** Use `cargo test --lib <filter>` for Rust unit tests during iteration (debug profile, fast compile). For sql-test suites, start standalone-server per CLAUDE.md §8.4 and run the runner. The acceptance bar (spec §8.9) is: new goldens identical with rule on/off, and no new failures in `optimizer`/`join`/`filter`/`sort`/`cte`.

---

## Phase 1 — Exec: per-partition rank-TopN

The exec `SortNode` today carries only global `topn_type`+`limit`. We add partition awareness so a single sort can truncate each PARTITION-BY group to its top-K independently. Sort the input by `(partition_exprs ASC, order_by)`, walk partition-key groups, and apply the existing per-group rank cutoff within each group.

### Task 1.1: Expose shared partition + cutoff helpers

`rank_like_cutoff` (private in `sort_processor.rs`) and `compute_partitions`/`row_equal_on_keys` (private in `analytic_shared.rs`) are exactly the boundary logic we need from `chunks_sorter_topn.rs`. Make them reachable.

**Files:**
- Modify: `src/exec/operators/sort/sort_processor.rs` (around `fn rank_like_cutoff` at line 995)
- Modify: `src/exec/operators/analytic_shared.rs` (`fn compute_partitions` line 314, `fn row_equal_on_keys` line 529)
- Modify: `src/exec/operators/sort/mod.rs` (module visibility, if `analytic_shared` is not already reachable from `sort`)

- [ ] **Step 1: Change visibility of the three helpers to `pub(crate)`**

In `src/exec/operators/sort/sort_processor.rs`, change:
```rust
fn rank_like_cutoff<F>(
```
to:
```rust
pub(crate) fn rank_like_cutoff<F>(
```

In `src/exec/operators/analytic_shared.rs`, change `fn compute_partitions` and `fn row_equal_on_keys` likewise:
```rust
pub(crate) fn compute_partitions(keys: &[ArrayRef], rows: usize) -> Result<Vec<(usize, usize)>, String> {
```
```rust
pub(crate) fn row_equal_on_keys(keys: &[ArrayRef], left: usize, right: usize) -> Result<bool, String> {
```

- [ ] **Step 2: Build to confirm visibility compiles**

Run: `cargo build --lib 2>&1 | tail -5`
Expected: builds (warnings about now-unused-pub are acceptable; they are consumed in Task 1.2).

- [ ] **Step 3: Commit**

```bash
git add src/exec/operators/sort/sort_processor.rs src/exec/operators/analytic_shared.rs
git commit -m "refactor(exec): expose rank_like_cutoff + partition helpers for partition-topn"
```

### Task 1.2: Add `sort_chunks_partition_topn` (per-partition truncation)

**Files:**
- Modify: `src/exec/operators/sort/chunks_sorter_topn.rs` (add new fn + tests)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `src/exec/operators/sort/chunks_sorter_topn.rs`. Mirror the existing test helpers in that file (they build `Chunk`s + an `ExprArena` + `SortExpression`s; reuse the same constructors the existing `dense_rank_topn_keeps_first_distinct_peer_groups` test uses).

```rust
#[test]
fn partition_topn_resets_rank_per_partition() {
    // Two partitions p=1 and p=2; within each, ORDER BY o ASC.
    // RANK limit=2 must keep top-2 of EACH partition independently.
    // Rows (p, o): (1,10) (1,20) (1,30) (2,5) (2,6) (2,7)
    let (arena, partition_exprs, order_by, chunks) =
        build_partition_topn_fixture(/* see helper below */);
    let out = sort_chunks_partition_topn(
        &arena,
        &partition_exprs,
        &order_by,
        SortTopNType::Rank,
        2, // partition_limit
        &chunks,
    )
    .unwrap()
    .expect("non-empty");
    // 2 partitions * 2 kept = 4 rows; the (1,30) and (2,7) rows are dropped.
    assert_eq!(out.len(), 4);
    let os = int_column_values(&out, /* o column index */);
    assert_eq!(os, vec![10, 20, 5, 6]);
}
```

Build the fixture by copying the exact chunk/arena/`SortExpression` construction from the existing `dense_rank_topn_keeps_first_distinct_peer_groups` test in this same module (read it first: `grep -n "fn dense_rank_topn_keeps_first_distinct_peer_groups" src/exec/operators/sort/chunks_sorter_topn.rs`). That test already shows how this file builds a `Chunk` from integer columns, an `Arc<ExprArena>`, and `SortExpression`s referencing column `ExprId`s. Construct one `Chunk` with two INT columns — col 0 = `p` rows `[1,1,1,2,2,2]`, col 1 = `o` rows `[10,20,30,5,6,7]` — then `partition_exprs = [SortExpression { expr: <col0 ExprId>, asc: true, nulls_first: true }]` and `order_by = [SortExpression { expr: <col1 ExprId>, asc: true, nulls_first: true }]`, using that test's helpers verbatim. Inline this directly in the test (no separate fixture fn needed). `int_column_values` likewise mirrors how the existing tests read a column's values back for assertions.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --lib chunks_sorter_topn::tests::partition_topn_resets_rank_per_partition 2>&1 | tail -15`
Expected: FAIL — `sort_chunks_partition_topn` not found (and the fixture `unimplemented!`).

- [ ] **Step 3: Implement `sort_chunks_partition_topn`**

Add to `src/exec/operators/sort/chunks_sorter_topn.rs`. Reuse `sort_chunks_by_order`, `eval_order_by_columns` (already in this file) and the now-`pub(crate)` `rank_like_cutoff` + `compute_partitions` + `row_equal_on_keys`.

```rust
use crate::exec::operators::analytic_shared::{compute_partitions, row_equal_on_keys};
use crate::exec::operators::sort::sort_processor::rank_like_cutoff;

/// Per-partition rank-TopN. Sorts by (partition_exprs ASC, order_by) so rows of
/// the same partition are adjacent and ordered, then keeps, within each
/// partition segment, the rows whose RowNumber/Rank/DenseRank is <= `partition_limit`.
/// Output stays sorted by (partition, order) — the shape the analytic operator needs.
pub(crate) fn sort_chunks_partition_topn(
    arena: &ExprArena,
    partition_exprs: &[SortExpression],
    order_by: &[SortExpression],
    topn_type: SortTopNType,
    partition_limit: usize,
    chunks: &[Chunk],
) -> Result<Option<Chunk>, String> {
    if partition_limit == 0 || chunks.is_empty() {
        return Ok(None);
    }
    // Combined sort key: partition keys first (ASC), then the window ORDER BY.
    let mut combined: Vec<SortExpression> = partition_exprs.to_vec();
    combined.extend_from_slice(order_by);
    let sorted = sort_chunks_by_order(arena, &combined, chunks)?;
    if sorted.is_empty() {
        return Ok(None);
    }
    // Partition-key arrays on the sorted chunk, for group-boundary detection.
    let part_keys = eval_order_by_columns(arena, partition_exprs, &sorted)?;
    // Order-key arrays on the sorted chunk, for per-group rank peer equality.
    let order_keys = eval_order_by_columns(arena, order_by, &sorted)?;
    let partitions = compute_partitions(&part_keys, sorted.len())?;

    let mut keep_indices = Vec::<u32>::new();
    for (start, end) in partitions {
        let seg_rows = end - start;
        // Within this partition, peer equality is on the ORDER BY columns,
        // indices offset by `start`.
        let cutoff = rank_like_cutoff(topn_type, partition_limit, seg_rows, |a, b| {
            // Best-effort: equality compares order-key arrays at absolute indices.
            row_equal_on_keys(&order_keys, start + a, start + b).unwrap_or(false)
        });
        for local in 0..cutoff {
            keep_indices.push(u32::try_from(start + local).map_err(|_| {
                format!("row index {} exceeds UInt32Array range", start + local)
            })?);
        }
    }
    if keep_indices.is_empty() {
        return Ok(None);
    }
    if keep_indices.len() == sorted.len() {
        return Ok(Some(sorted));
    }
    take_rows(&sorted, &keep_indices) // see Step 3b
}
```

- [ ] **Step 3b: Add the `take_rows` helper if one does not already exist in the file**

Search first: `grep -n "fn take_rows\|UInt32Array::from(indices)" src/exec/operators/sort/chunks_sorter_topn.rs`. `filter_chunk_by_boundary` (lines 334-376) already contains the take-by-`UInt32Array` pattern. Extract its take logic into:
```rust
fn take_rows(chunk: &Chunk, indices: &[u32]) -> Result<Option<Chunk>, String> {
    if indices.is_empty() {
        return Ok(None);
    }
    let selection = UInt32Array::from(indices.to_vec());
    let schema = merged_sort_schema_for_chunks(std::slice::from_ref(chunk))?;
    let batch = normalize_sort_batch_for_schema(chunk, &schema, 0)?;
    let columns = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &selection, None))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let filtered = RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())?;
    Chunk::try_new_like(filtered, chunk).map(Some).map_err(|e| e.to_string())
}
```
Then refactor `filter_chunk_by_boundary` to call `take_rows` (DRY) — keep its boundary computation, replace the inline take block with `return take_rows(chunk, &indices);`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib chunks_sorter_topn::tests::partition_topn 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Add edge-case tests (RowNumber exact-K, DenseRank, NULL partition key, single global group)**

```rust
#[test]
fn partition_topn_row_number_keeps_exactly_k_per_partition() { /* topn_type RowNumber, limit=1 → 1 row per partition */ }
#[test]
fn partition_topn_dense_rank_keeps_distinct_peer_groups_per_partition() { /* ties share dense_rank within a partition */ }
#[test]
fn partition_topn_null_partition_key_groups_nulls_together() { /* a partition key column with NULLs forms one group */ }
#[test]
fn partition_topn_empty_partition_exprs_matches_global() { /* empty partition_exprs → identical to sort_chunks_rank global */ }
```
Fill each body with the file's existing scaffolding (concrete inputs + asserted kept rows).

- [ ] **Step 6: Run all new tests, then commit**

Run: `cargo test --lib chunks_sorter_topn 2>&1 | tail -15`
Expected: PASS (all).
```bash
git add src/exec/operators/sort/chunks_sorter_topn.rs
git commit -m "feat(exec): sort_chunks_partition_topn — per-partition rank/row_number/dense_rank truncation"
```

### Task 1.3: Add partition fields to exec `SortNode` and thread through the processor

**Files:**
- Modify: `src/exec/node/sort.rs` (`SortNode` struct, lines 42-54)
- Modify: `src/exec/operators/sort/sort_processor.rs` (`SortProcessorFactory` lines 56-67; `build_topn_sorter` line 377; constructor that reads `SortNode`)

- [ ] **Step 1: Add fields to exec `SortNode`**

In `src/exec/node/sort.rs`, extend the struct:
```rust
#[derive(Clone, Debug)]
pub struct SortNode {
    pub input: Box<ExecNode>,
    pub node_id: i32,
    pub use_top_n: bool,
    pub order_by: Vec<SortExpression>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub topn_type: SortTopNType,
    /// Partition keys for per-partition TopN. Empty for non-partition sorts.
    pub partition_exprs: Vec<SortExpression>,
    /// Per-partition row cap. `Some` ⇒ truncate each `partition_exprs` group to
    /// the top-`partition_limit` by `topn_type`. `None` ⇒ ordinary sort/topn.
    pub partition_limit: Option<usize>,
    pub max_buffered_rows: Option<usize>,
    pub max_buffered_bytes: Option<usize>,
}
```

- [ ] **Step 2: Build — expect failures at every `SortNode { ... }` construction site**

Run: `cargo build --lib 2>&1 | grep -E "missing field|error" | head`
Expected: errors at each `SortNode { ... }` literal (lowering, tests, any other). This enumerates the sites to fix.

- [ ] **Step 3: Set the new fields at every construction site to the inert default**

At each site found in Step 2 (the lowering at `src/lower/node/sort.rs` is updated for real in Task 2.1; everywhere else and the lowering's interim value), add:
```rust
partition_exprs: Vec::new(),
partition_limit: None,
```

- [ ] **Step 4: Thread fields into `SortProcessorFactory`**

In `src/exec/operators/sort/sort_processor.rs`, add to the struct (after `topn_type`):
```rust
    partition_exprs: Vec<SortExpression>,
    partition_limit: Option<usize>,
```
Set them in the factory constructor from the `SortNode` (find where `SortProcessorFactory` is built from `SortNode` — grep `SortProcessorFactory {` — and copy `node.partition_exprs.clone()` / `node.partition_limit`).

- [ ] **Step 5: Use the partition path in `build_topn_sorter`**

Wrap the existing rank-like branch so that when `partition_limit` is set we use a partition-aware sorter. Add a small `ChunksSorter` adaptor that calls `sort_chunks_partition_topn`, or (simpler) special-case in the full-sort finalize path. Minimal change in `build_topn_sorter`:
```rust
} else if let Some(partition_limit) = self.partition_limit.filter(|_| !self.partition_exprs.is_empty()) {
    Box::new(ChunksSorterPartitionTopN::new(
        Arc::clone(&self.arena),
        self.partition_exprs.clone(),
        self.order_by.clone(),
        self.topn_type,
        partition_limit,
    ))
} else if let Some(rank_limit) = self.rank_like_limit_for_topn() {
```
Add `ChunksSorterPartitionTopN` in `chunks_sorter_topn.rs` implementing the existing `ChunksSorter` trait (mirror `ChunksSorterTopN`), whose sort entrypoint calls `sort_chunks_partition_topn`. Show the struct:
```rust
pub(crate) struct ChunksSorterPartitionTopN {
    arena: Arc<ExprArena>,
    partition_exprs: Vec<SortExpression>,
    order_by: Vec<SortExpression>,
    topn_type: SortTopNType,
    partition_limit: usize,
}
```
Implement the same `ChunksSorter` method `ChunksSorterTopN` implements (grep `impl ChunksSorter for ChunksSorterTopN` to copy the trait method signature), delegating to `sort_chunks_partition_topn`.

- [ ] **Step 6: Gate `use_top_n` requirement** — partition_limit requires `use_top_n=true` semantics handled in lowering (Task 2.1). For now ensure `build_topn_sorter` only takes the partition branch when `partition_limit.is_some()`.

- [ ] **Step 7: Build + run exec sort tests**

Run: `cargo test --lib exec::operators::sort 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/exec/node/sort.rs src/exec/operators/sort/
git commit -m "feat(exec): thread partition_exprs/partition_limit into SortNode + processor"
```

---

## Phase 2 — Lowering: read partition fields from `TSortNode`

`TSortNode` already declares `partition_exprs` (field 23) and `partition_limit` (field 24). Lowering currently ignores them.

### Task 2.1: Parse `partition_exprs`/`partition_limit` in `lower_sort_node`

**Files:**
- Modify: `src/lower/node/sort.rs` (`lower_sort_node` lines 35-134; mirror `build_sort_order_by` line 173)

- [ ] **Step 1: Write the failing lowering test**

Mirror `lower_sort_node_accepts_rank_topn_type` (line 815). Add:
```rust
#[test]
fn lower_sort_node_reads_partition_limit_and_exprs() {
    let mut sort_node = /* same scaffold as lower_sort_node_accepts_rank_topn_type */;
    sort_node.use_top_n = true;
    sort_node.topn_type = Some(plan_nodes::TTopNType::RANK);
    sort_node.partition_limit = Some(3);
    sort_node.partition_exprs = Some(vec![/* a TExpr slot-ref to the partition column */]);
    // node.limit must be >= 0 because use_top_n requires it.
    let lowered = lower_sort_node(/* args */).expect("lower ok");
    let ExecNodeKind::Sort(s) = lowered.node.kind else { panic!("expected Sort") };
    assert_eq!(s.partition_limit, Some(3));
    assert_eq!(s.partition_exprs.len(), 1);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --lib lower::node::sort::tests::lower_sort_node_reads_partition_limit_and_exprs 2>&1 | tail -15`
Expected: FAIL (`partition_limit`/`partition_exprs` on lowered SortNode are always default).

- [ ] **Step 3: Implement parsing in `lower_sort_node`**

After `topn_type` is parsed (line 103), add:
```rust
    // Per-partition TopN (StarRocks PartitionSort). `partition_exprs` are
    // grouping keys; `partition_limit` caps rows per group. Compiled like
    // ordering exprs but without asc/nulls semantics (grouping only — we sort
    // them ASC NULLS FIRST in exec to make groups adjacent).
    let partition_exprs = match sort.partition_exprs.as_ref() {
        None => Vec::new(),
        Some(exprs) => {
            let mut out = Vec::with_capacity(exprs.len());
            for e in exprs {
                let expr_id = lower_t_expr(e, arena, &sort_input_layout, last_query_id, fe_addr)?;
                out.push(SortExpression { expr: expr_id, asc: true, nulls_first: true });
            }
            out
        }
    };
    let partition_limit = match sort.partition_limit {
        None => None,
        Some(v) if v < 0 => {
            return Err(format!(
                "SORT_NODE node_id={} partition_limit must be >= 0, got {v}",
                node.node_id
            ));
        }
        Some(v) => Some(v as usize),
    };
    if partition_limit.is_some() && !use_top_n {
        return Err(format!(
            "SORT_NODE node_id={} partition_limit requires use_top_n=true",
            node.node_id
        ));
    }
```
Then add the two fields to the `SortNode { ... }` literal (replacing the interim defaults from Task 1.3 Step 3):
```rust
                partition_exprs,
                partition_limit,
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --lib lower::node::sort 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lower/node/sort.rs
git commit -m "feat(lower): read TSortNode.partition_exprs/partition_limit into exec SortNode"
```

---

## Phase 3 — Optimizer: Sort op fields + convert/implement/codegen/explain

Now the standalone optimizer can carry the fields and codegen can emit them into `TSortNode` (which Phase 2 lowering now consumes).

### Task 3.1: Add fields to planner `SortNode`

**Files:**
- Modify: `src/sql/planner/plan.rs` (`SortNode` line 404)

- [ ] **Step 1: Add fields**

```rust
#[derive(Clone, Debug)]
pub(crate) struct SortNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<SortItem>,
    pub analytic_partition_by: Vec<TypedExpr>,
    /// Set by RankingWindowPredicatePushdown: per-partition rank cap + ranking
    /// kind. `None` ⇒ ordinary sort. See design spec §4.
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
    pub required_output_columns: Option<HashSet<ColumnId>>,
}
```

- [ ] **Step 2: Build — fix every `SortNode { ... }` planner construction site**

Run: `cargo build --lib 2>&1 | grep -E "missing field.*partition_limit|missing field.*topn_type" | head`
At each site, add `partition_limit: None, topn_type: None,`.

- [ ] **Step 3: Build clean, commit**

Run: `cargo build --lib 2>&1 | tail -3`
```bash
git add src/sql/planner/plan.rs src/sql/
git commit -m "feat(planner): add partition_limit/topn_type to SortNode (default None)"
```

### Task 3.2: Add fields to memo `LogicalSortOp` + `PhysicalSortOp`

**Files:**
- Modify: `src/sql/optimizer/operator.rs` (`LogicalSortOp` line 168, `PhysicalSortOp` line 370)

- [ ] **Step 1: Add the same two fields to both ops**

```rust
// LogicalSortOp (after analytic_partition_exprs):
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
```
```rust
// PhysicalSortOp (after analytic_partition_exprs):
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
```

- [ ] **Step 2: Build — fix construction sites with `partition_limit: None, topn_type: None`**

Run: `cargo build --lib 2>&1 | grep -E "missing field" | head`
Fix sites in `convert.rs`, `cost.rs` (lines ~569/631/659), `extract.rs:136`, `cascades_rules/implement.rs`, `cascades_rules/sort_limit_to_top_n.rs`, `topn_compactness.rs` — set both to `None` for now (convert/implement get the real propagation in 3.3/3.4).

- [ ] **Step 3: Build clean, commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/optimizer/
git commit -m "feat(optimizer): add partition_limit/topn_type to Logical/PhysicalSortOp"
```

### Task 3.3: Propagate planner→memo in `convert.rs`

**Files:**
- Modify: `src/sql/optimizer/convert.rs` (Sort arm, lines 96-108)

- [ ] **Step 1: Set fields from the planner node**

```rust
        let op = Operator::LogicalSort(LogicalSortOp {
            items: node.items.clone(),
            analytic_partition_exprs: node.analytic_partition_by.clone(),
            partition_limit: node.partition_limit,
            topn_type: node.topn_type,
        });
```

- [ ] **Step 2: Build, commit**

Run: `cargo build --lib 2>&1 | tail -3`
```bash
git add src/sql/optimizer/convert.rs
git commit -m "feat(optimizer): convert.rs carries SortNode partition_limit/topn_type into memo"
```

### Task 3.4: Propagate memo logical→physical in `implement.rs`

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/implement.rs` (Sort implement rule, lines 726-739)

- [ ] **Step 1: Propagate**

```rust
        op: Operator::PhysicalSort(PhysicalSortOp {
            items: op.items.clone(),
            analytic_partition_exprs: op.analytic_partition_exprs.clone(),
            partition_limit: op.partition_limit,
            topn_type: op.topn_type,
        }),
```

- [ ] **Step 2: Build, commit**

```bash
git add src/sql/optimizer/cascades_rules/implement.rs
git commit -m "feat(optimizer): implement.rs propagates partition_limit/topn_type to PhysicalSort"
```

### Task 3.5: Emit `TSortNode` fields in codegen

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs` (`visit_sort`, the `TSortNode { ... }` at lines 2569-2595)

- [ ] **Step 1: Compile partition fields and set them**

Before the `TSortNode { ... }` literal, after the `analytic_partition_exprs` block (line 2551), add:
```rust
        // Per-partition TopN (RankingWindowPredicatePushdown). When set, reuse
        // the analytic partition keys as the partition-topn grouping keys.
        let (partition_exprs_t, partition_limit_t, topn_type_t) = if let Some(limit) = op.partition_limit {
            let mut keys = Vec::with_capacity(op.analytic_partition_exprs.len());
            for expr in &op.analytic_partition_exprs {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
                keys.push(compiler.compile_typed(expr)?);
            }
            let tt = match op.topn_type {
                Some(crate::exec::node::sort::SortTopNType::RowNumber) => plan_nodes::TTopNType::ROW_NUMBER,
                Some(crate::exec::node::sort::SortTopNType::Rank) => plan_nodes::TTopNType::RANK,
                Some(crate::exec::node::sort::SortTopNType::DenseRank) => plan_nodes::TTopNType::DENSE_RANK,
                None => plan_nodes::TTopNType::ROW_NUMBER,
            };
            (Some(keys), Some(limit as i64), Some(tt))
        } else {
            (None, None, None)
        };
```
Then in the `TSortNode { ... }` literal replace the three hardcoded `None`s:
```rust
            partition_exprs: partition_exprs_t,
            partition_limit: partition_limit_t,
            topn_type: topn_type_t,
```
Also set `use_top_n: op.partition_limit.is_some()` when partitioned (replace the hardcoded `use_top_n: false` only when `partition_limit` is set — lowering requires `use_top_n=true` for partition_limit; and `node.limit` must be ≥0, so also set `sort_plan_node.limit = partition_limit as i64` in the partitioned case instead of `-1`).

- [ ] **Step 2: Build, commit**

Run: `cargo build --lib 2>&1 | tail -3`
```bash
git add src/sql/codegen/fragment_builder.rs
git commit -m "feat(codegen): emit TSortNode partition_exprs/partition_limit/topn_type"
```

### Task 3.6: Render the token in EXPLAIN

**Files:**
- Modify: `src/sql/explain.rs` (`Operator::PhysicalSort(op)` arm, lines 675-696)

- [ ] **Step 1: Append partition_limit/topn_type to the SORT line**

```rust
Operator::PhysicalSort(op) => {
    let items: Vec<String> = op.items.iter().map(|s| {
        let dir = if s.asc { "ASC" } else { "DESC" };
        let nulls = if s.nulls_first { " NULLS FIRST" } else { " NULLS LAST" };
        format!("{} {dir}{nulls}", format_expr(&s.expr))
    }).collect();
    let mut suffix = String::new();
    if let Some(limit) = op.partition_limit {
        let tt = match op.topn_type {
            Some(crate::exec::node::sort::SortTopNType::RowNumber) => "ROW_NUMBER",
            Some(crate::exec::node::sort::SortTopNType::Rank) => "RANK",
            Some(crate::exec::node::sort::SortTopNType::DenseRank) => "DENSE_RANK",
            None => "ROW_NUMBER",
        };
        suffix = format!(" partition_limit={limit} topn_type={tt}");
    }
    out.push(format!(
        "{pad}SORT BY [{}]{suffix}{costs_suffix}{stats_suffix}",
        items.join(", ")
    ));
    for child in &node.children {
        format_physical_node(child, level, indent + 1, out);
    }
}
```

- [ ] **Step 2: Build, commit**

```bash
git add src/sql/explain.rs
git commit -m "feat(explain): render SORT partition_limit/topn_type token"
```

### Task 3.7: Reflect the per-partition row reduction in stats (cost quality)

The rewrite is unconditional (not cost-gated), so this does not affect *whether* it fires — but downstream operators should see the reduced row count (spec §7). Cap the Sort's estimated output rows when `partition_limit` is set.

**Files:**
- Modify: the Sort row-count estimate. Locate it first: `grep -rn "PhysicalSort" src/sql/optimizer/stats.rs src/sql/optimizer/derive/sort.rs src/sql/optimizer/cost.rs`. The output-cardinality of a Sort is normally pass-through (= child rows); add the cap there.

- [ ] **Step 1: Write the failing test** (in the stats/derive module that owns Sort cardinality)

```rust
#[test]
fn sort_partition_limit_caps_output_rows() {
    // child rows = 1000, NDV(partition key) = 10, partition_limit = 3
    // → estimated output rows = min(1000, 10*3) = 30.
    let est = estimate_sort_output_rows(/* child_rows=1000, ndv_partition=Some(10), partition_limit=Some(3) */);
    assert_eq!(est, 30.0);
}
#[test]
fn sort_without_partition_limit_is_passthrough() {
    let est = estimate_sort_output_rows(/* child_rows=1000, ndv=_, partition_limit=None */);
    assert_eq!(est, 1000.0);
}
```

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement the cap** where Sort output rows are computed:
```rust
// When partition_limit is set, each partition keeps at most `limit` rows
// (rank/dense_rank may keep ties → use limit as a lower-ish estimate; conservative).
let output_rows = match (op.partition_limit, ndv_of_partition_keys) {
    (Some(limit), Some(ndv)) => child_rows.min((ndv as f64) * (limit as f64)),
    (Some(limit), None) => child_rows.min(/* fallback */ child_rows), // unknown NDV → no reduction claimed
    (None, _) => child_rows,
};
```
> Use the existing NDV lookup the optimizer already has for the partition-key columns (the same `analytic_partition_exprs` columns). If no per-call NDV is available in this code path, fall back to pass-through (no reduction) rather than guessing — correctness of cost is less important than not over-claiming.

- [ ] **Step 4: Run → PASS. Commit.**
```bash
git add src/sql/optimizer/
git commit -m "feat(optimizer): cap Sort output-row estimate under partition_limit"
```

---

## Phase 4 — The `RankingWindowPredicatePushdown` rule

### Task 4.1: Rule module skeleton + registration

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/mod.rs`
- Create: `src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`

- [ ] **Step 1: Create the rule with a no-op `apply` (returns Unchanged)**

`rule.rs`:
```rust
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::LogicalPlan;

pub(crate) struct RankingWindowPredicatePushdownRule;

impl LogicalRewriteRule for RankingWindowPredicatePushdownRule {
    fn name(&self) -> &'static str { "RankingWindowPredicatePushdown" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Filter(_))
    }
    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        Ok(RewriteResult::Unchanged)
    }
}
```
`mod.rs`:
```rust
mod rule;
pub(crate) use rule::RankingWindowPredicatePushdownRule;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
pub(crate) fn ranking_window_predicate_pushdown_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(RankingWindowPredicatePushdownRule)]
}
```

- [ ] **Step 2: Register** — in `rules/mod.rs` add `pub(crate) mod ranking_window_predicate_pushdown;`. In `registry.rs` `query_rewrite_pipeline(...)` add a stage (place AFTER predicate pushdown so the Filter has settled directly over the Window):
```rust
        RewriteStage::new(
            "RankingWindowPredicatePushdown",
            RewritePhase::StructuralRewrite,
            rules::ranking_window_predicate_pushdown::ranking_window_predicate_pushdown_rules(),
        ),
```

- [ ] **Step 3: Test that the name is known/disable-able**

```rust
#[test]
fn ranking_window_rule_is_known() {
    assert!(crate::sql::optimizer::rewrite::registry::is_known_rewrite_rule_name(
        "RankingWindowPredicatePushdown"
    ));
}
```
Run: `cargo test --lib ranking_window_rule_is_known 2>&1 | tail -5` → PASS.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/ src/sql/optimizer/rewrite/rules/mod.rs src/sql/optimizer/rewrite/registry.rs
git commit -m "feat(optimizer): RankingWindowPredicatePushdown rule skeleton + registration"
```

### Task 4.2: Predicate→K extraction helper (pure function, unit-tested)

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs` (add `fn rank_upper_bound`)

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn rank_upper_bound_extracts_le_lt_eq_between_in() {
    let col = ColumnId(7);
    assert_eq!(rank_upper_bound(&le(col, 5), col), Some(5));   // rk <= 5
    assert_eq!(rank_upper_bound(&lt(col, 5), col), Some(4));   // rk < 5  → 4
    assert_eq!(rank_upper_bound(&eq(col, 3), col), Some(3));   // rk = 3
    assert_eq!(rank_upper_bound(&between(col, 2, 9), col), Some(9));
    assert_eq!(rank_upper_bound(&in_list(col, &[1,3,5]), col), Some(5));
    assert_eq!(rank_upper_bound(&ge(col, 5), col), None);      // lower bound only
    assert_eq!(rank_upper_bound(&le(col, 0), col), None);      // K<=0
    assert_eq!(rank_upper_bound(&le(ColumnId(8), 5), col), None); // different col
}
```
(`le/lt/eq/ge/between/in_list` are local test builders constructing `TypedExpr` per `src/sql/analysis/mod.rs` `ExprKind`/`BinOp`/`LiteralValue`.)

- [ ] **Step 2: Run → FAIL** (`rank_upper_bound` undefined).

- [ ] **Step 3: Implement**

```rust
use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::utils::split_and;

/// Smallest finite upper bound K such that the conjunctive predicate can only
/// pass rows with rank_col <= K. Returns None if no finite positive bound on
/// `rank_col` exists (rule must not fire).
fn rank_upper_bound(predicate: &TypedExpr, rank_col: ColumnId) -> Option<usize> {
    let mut best: Option<i64> = None;
    for conj in split_and(predicate.clone()) {
        if let Some(k) = conjunct_upper_bound(&conj, rank_col) {
            best = Some(best.map_or(k, |b| b.min(k)));
        }
    }
    match best {
        Some(k) if k >= 1 => usize::try_from(k).ok(),
        _ => None,
    }
}

fn is_rank_col(e: &TypedExpr, rank_col: ColumnId) -> bool {
    matches!(&e.kind, ExprKind::ColumnRef { column_id, .. } if *column_id == rank_col)
}
fn int_lit(e: &TypedExpr) -> Option<i64> {
    match &e.kind { ExprKind::Literal(LiteralValue::Int(v)) => Some(*v), _ => None }
}

fn conjunct_upper_bound(e: &TypedExpr, rank_col: ColumnId) -> Option<i64> {
    match &e.kind {
        ExprKind::BinaryOp { left, op, right } => {
            // rank_col <op> lit  OR  lit <op> rank_col
            let (lit, col_on_left) = if is_rank_col(left, rank_col) {
                (int_lit(right)?, true)
            } else if is_rank_col(right, rank_col) {
                (int_lit(left)?, false)
            } else {
                return None;
            };
            match (op, col_on_left) {
                (BinOp::Le, true) | (BinOp::Ge, false) => Some(lit),        // rk<=k / k>=rk
                (BinOp::Lt, true) | (BinOp::Gt, false) => Some(lit - 1),    // rk<k  / k>rk
                (BinOp::Eq, _) => Some(lit),                                // rk=k
                _ => None,
            }
        }
        ExprKind::Between { expr, low: _, high, negated: false } if is_rank_col(expr, rank_col) => {
            int_lit(high)
        }
        ExprKind::InList { expr, list, negated: false } if is_rank_col(expr, rank_col) => {
            list.iter().map(int_lit).collect::<Option<Vec<_>>>()?.into_iter().max()
        }
        _ => None,
    }
}
```
> Implementer: confirm exact `ExprKind::Between`/`InList` field names against `src/sql/analysis/mod.rs` and adjust the patterns (the探查 confirmed `Between { expr, low, high, negated }` and `InList { expr, list, negated }`).

- [ ] **Step 4: Run → PASS.** Commit:
```bash
git add src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs
git commit -m "feat(optimizer): rank_upper_bound predicate→K extraction + tests"
```

### Task 4.3: Window ranking-kind detection + match/guards/transform

**Files:**
- Modify: `ranking_window_predicate_pushdown/rule.rs`

- [ ] **Step 1: Write failing transform tests**

```rust
#[test]
fn fires_on_rank_per_group_sets_partition_limit() {
    // Filter(rk <= 2) -> Window(rank() PARTITION BY p ORDER BY o, out=rk) -> Sort(analytic_partition_by=[p])
    let plan = build_filter_window_sort(/* rank, limit 2 */);
    let out = apply_rule(plan);
    let sort = find_sort(&out);
    assert_eq!(sort.partition_limit, Some(2));
    assert_eq!(sort.topn_type, Some(SortTopNType::Rank));
    // Window + Filter still present (results identical).
    assert!(has_window(&out) && has_filter(&out));
}
#[test]
fn rejects_when_window_has_aggregate_over() {
    // Window contains avg(x) OVER (...) alongside rank() → must not fire.
    let plan = build_filter_window_sort_with_agg_over();
    assert_unchanged(apply_rule_result(plan));
}
#[test]
fn rejects_empty_partition_by() { /* global ranking → Unchanged */ }
#[test]
fn rejects_no_upper_bound() { /* rk >= 5 → Unchanged */ }
#[test]
fn idempotent_when_sort_already_has_partition_limit() { /* second apply → Unchanged */ }
#[test]
fn sees_through_one_project_between_filter_and_window() { /* Filter→Project→Window→Sort fires */ }
#[test]
fn rejects_when_project_transforms_rank_col() { /* Project applies an expr (not bare passthrough) to rk → Unchanged */ }
```

> **AssertOneRow (spec §6.3) — resolved by results-identity, no explicit guard.** The committed spec precautionarily said "don't fire below AssertOneRow." On review that is unnecessary: the rewrite preserves the Window *and* the outer Filter, so the rows reaching any ancestor (including an `AssertOneRow` over a scalar subquery) are byte-identical to the pre-rewrite plan — a >1-row subquery still errors, a 1-row one still passes. A BottomUp tree rule also cannot see its parent cheaply. So instead of a guard we add a correctness test (below) proving the AssertOneRow behavior is unchanged. Record this deviation from spec §6.3 in the PR description.

Add to the correctness sql-test (Task 5.2): a scalar subquery whose body is `... rank() OVER (PARTITION BY p ORDER BY o) rk ... WHERE rk <= 1` that spans 2 partitions must still raise the AssertOneRow error with the rule on (identical to rule off).

- [ ] **Step 2: Run → FAIL.**

- [ ] **Step 3: Implement `matches` + `apply`**

```rust
const RANKING_FNS: [&str; 3] = ["row_number", "rank", "dense_rank"];

fn ranking_topn_type(name: &str) -> Option<crate::exec::node::sort::SortTopNType> {
    use crate::exec::node::sort::SortTopNType::*;
    match name.to_ascii_lowercase().as_str() {
        "row_number" => Some(RowNumber),
        "rank" => Some(Rank),
        "dense_rank" => Some(DenseRank),
        _ => None,
    }
}

fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
    let LogicalPlan::Filter(f) = plan else { return false };
    // input is Window, or Project over Window
    let win = match f.input.as_ref() {
        LogicalPlan::Window(w) => w,
        LogicalPlan::Project(p) => match p.input.as_ref() {
            LogicalPlan::Window(w) => w,
            _ => return false,
        },
        _ => return false,
    };
    matches!(win.input.as_ref(), LogicalPlan::Sort(s) if !s.analytic_partition_by.is_empty())
}
```
`apply` (BottomUp; `f.input` already materialized):
```rust
fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
    let LogicalPlan::Filter(filter) = &plan else { return Ok(RewriteResult::Unchanged) };

    // Resolve optional Project between Filter and Window.
    let (window, project_opt): (&WindowNode, Option<&ProjectNode>) = match filter.input.as_ref() {
        LogicalPlan::Window(w) => (w, None),
        LogicalPlan::Project(p) => match p.input.as_ref() {
            LogicalPlan::Window(w) => (w, Some(p)),
            _ => return Ok(RewriteResult::Unchanged),
        },
        _ => return Ok(RewriteResult::Unchanged),
    };
    let LogicalPlan::Sort(sort) = window.input.as_ref() else { return Ok(RewriteResult::Unchanged) };

    // GUARD: idempotency.
    if sort.partition_limit.is_some() { return Ok(RewriteResult::Unchanged); }
    // GUARD: every window expr must be a ranking fn (no aggregate-over).
    if window.window_exprs.is_empty()
        || !window.window_exprs.iter().all(|w| ranking_topn_type(&w.name).is_some())
    {
        return Ok(RewriteResult::Unchanged);
    }
    // GUARD: PARTITION BY non-empty (per-group only).
    if sort.analytic_partition_by.is_empty() { return Ok(RewriteResult::Unchanged); }

    // Find a ranking window expr whose output column the filter bounds.
    // (If a Project renames the rank col, map through it — see note.)
    let mut chosen: Option<(usize, crate::exec::node::sort::SortTopNType)> = None;
    for w in &window.window_exprs {
        if let Some(k) = rank_upper_bound(&filter.predicate, w.output_column_id) {
            chosen = Some((k, ranking_topn_type(&w.name).unwrap()));
            break;
        }
        let _ = project_opt; // see note on Project column mapping
    }
    let Some((k, topn_type)) = chosen else { return Ok(RewriteResult::Unchanged) };

    // Rebuild the tree, setting fields on the Sort, keeping Filter/Project/Window.
    let mut new_sort = sort.clone();
    new_sort.partition_limit = Some(k);
    new_sort.topn_type = Some(topn_type);
    let new_window = WindowNode { input: Box::new(LogicalPlan::Sort(new_sort)), ..window.clone() };
    let new_filter_input = match project_opt {
        None => LogicalPlan::Window(new_window),
        Some(p) => LogicalPlan::Project(ProjectNode { input: Box::new(LogicalPlan::Window(new_window)), ..p.clone() }),
    };
    let new_filter = FilterNode { input: Box::new(new_filter_input), ..filter.clone() };
    Ok(RewriteResult::Changed(LogicalPlan::Filter(new_filter)))
}
```
> Note (Project column mapping): when a Project sits between Filter and Window, the filter may reference the projected output of the rank column. If the Project simply passes the rank column through (an identity `ColumnRef` item), the `output_column_id` matches and no mapping is needed. If the Project renames it, map the filter column back to `w.output_column_id` via the Project item whose expr is a bare `ColumnRef` to `w.output_column_id`. Add a `rejects_when_project_transforms_rank_col` test and keep the conservative path: if mapping is not a bare passthrough, return `Unchanged`.

- [ ] **Step 4: Run → PASS.** Run the whole rule test module:
Run: `cargo test --lib ranking_window_predicate_pushdown 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/rules/ranking_window_predicate_pushdown/rule.rs
git commit -m "feat(optimizer): RankingWindowPredicatePushdown match + guards + transform"
```

### Task 4.4: Coordinate with topn_compactness / sort_limit_to_top_n

**Files:**
- Modify: `src/sql/optimizer/cascades_rules/topn_compactness.rs` (the `analytic_partition_exprs.is_empty()` check, line 235)

- [ ] **Step 1: Add a regression test** that a Sort carrying `partition_limit` (and analytic_partition_exprs) is NOT clobbered by topn_compactness / sort_limit_to_top_n (they should leave it intact). Build a physical Sort with `partition_limit=Some(2)` and assert the rules don't drop it.

- [ ] **Step 2: Run → if FAIL, guard those rules** to skip when `partition_limit.is_some()` (mirror the existing `!analytic_partition_exprs.is_empty()` guard). Add `|| op.partition_limit.is_some()` to the skip condition.

- [ ] **Step 3: Run → PASS. Commit.**
```bash
git add src/sql/optimizer/cascades_rules/
git commit -m "fix(optimizer): topn rules preserve Sort partition_limit"
```

---

## Phase 5 — Goldens, correctness, OQ-13 closeout artifacts

> Setup for all sql-test steps: `source docker/iceberg-rest/runtime/current/env.sh`, start standalone-server against `$NOVAROCKS_STANDALONE_CONFIG` (gate on `NOVAROCKS_READY` per CLAUDE.md §7.3), then run the runner with `--config "$NOVAROCKS_SQL_TEST_CONFIG"`.

### Task 5.1: Optimizer plan goldens

**Files:**
- Create: `sql-tests/optimizer/sql/ranking_window_topn.sql`
- Create: `sql-tests/optimizer/sql/ranking_window_topn_rejected.sql`
- Create (via `--mode record`): `sql-tests/optimizer/result/ranking_window_topn.result`, `.../ranking_window_topn_rejected.result`

- [ ] **Step 1: Write `ranking_window_topn.sql`** (mirror `subquery_scalar_to_window.sql` header style)

```sql
-- @tags=optimizer,oq13,ranking_window_topn
-- Objective: Filter(rank <= k) over Window(rank/row_number/dense_rank PARTITION BY ... )
-- pushes partition_limit + topn_type onto the analytic Sort; Window + Filter stay.
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE rw_t (p INT, o INT, v INT);
INSERT INTO rw_t VALUES (1,10,100),(1,20,100),(1,30,100),(2,5,7),(2,6,7),(2,7,7);

-- @explain_contains=partition_limit=2
-- @explain_contains=topn_type=RANK
-- @explain_contains=WINDOW
SELECT * FROM (
  SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o) rk FROM rw_t
) t WHERE rk <= 2;

-- result-identity: same query, rule on (golden captures rows)
SELECT * FROM (
  SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o) rk FROM rw_t
) t WHERE rk <= 2 ORDER BY p, o;

SET disable_optimizer_rules='RankingWindowPredicatePushdown';
-- @explain_not_contains=partition_limit=
SELECT * FROM (
  SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o) rk FROM rw_t
) t WHERE rk <= 2 ORDER BY p, o;
```

- [ ] **Step 2: Write `ranking_window_topn_rejected.sql`** — three sub-cases each `@explain_not_contains=partition_limit=`: (a) `avg(v) OVER (PARTITION BY p)` present in the window, (b) `WHERE rk >= 2` (no upper bound), (c) `ORDER BY o` window with no PARTITION BY.

- [ ] **Step 3: Record goldens, then verify**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer \
  --only ranking_window_topn,ranking_window_topn_rejected --mode record
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer \
  --only ranking_window_topn,ranking_window_topn_rejected --mode verify
```
Expected: verify PASS; inspect the recorded `.result` to confirm the rule-on and rule-off rows are identical.

- [ ] **Step 4: Commit**
```bash
git add sql-tests/optimizer/sql/ranking_window_topn*.sql sql-tests/optimizer/result/ranking_window_topn*.result
git commit -m "test(optimizer): ranking-window partition-topn plan goldens (fire + reject)"
```

### Task 5.2: Correctness cases (rule on/off identical results)

**Files:**
- Create: `sql-tests/sort/sql/ranking_window_topn_correctness.sql` + recorded `.result`

- [ ] **Step 1: Write cases** covering: rank=1 per group; rank<=k with a tie at the boundary (two equal `o` in one partition); dense_rank<=k with ties; row_number<=k (exact); NULL in ORDER BY key; an empty/one-row partition. Each case appears twice — once default, once after `SET disable_optimizer_rules='RankingWindowPredicatePushdown'` — and both must yield identical rows (the golden captures one; the runner checks both produce it).

- [ ] **Step 2: Record + verify** (same runner pattern, `--suite sort --only ranking_window_topn_correctness`). Expected PASS. Commit.

### Task 5.3: tpc-h q2/q17 WINDOW golden (Apply closeout)

**Files:**
- Modify: `sql-tests/tpc-h/sql/q2.sql`, `sql-tests/tpc-h/sql/q17.sql`

- [ ] **Step 1: Add plan-shape assertions** above the existing query in each (the Apply/WinMagic rewrite makes these emit a WINDOW). Add:
```sql
-- @explain_contains=WINDOW
```
to q17 (correlated `avg`) and q2 (correlated `min`). If q2's shape does not produce WINDOW (min may take the to-join form), instead assert `-- @explain_contains=ASSERT` / the actual observed shape — record first (Step 2) and lock what is actually produced; do not force a shape the optimizer doesn't emit.

- [ ] **Step 2: Verify** against current behavior:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-h --only q2,q17 --mode verify
```
Expected: PASS (or adjust the asserted token to the actually-produced plan, per Step 1 fallback). Commit.

### Task 5.4: Scalar-multi-row error case (Apply §8.3 closeout)

**Files:**
- Create: `sql-tests/subquery/sql/scalar_subquery_multi_row_error.sql` (+ no `.result` needed for an error-only case; follow the `@expect_error` convention)

- [ ] **Step 1: Write the case** (model `-- @expect_error=` from `sql-tests/analytic/sql/analytic_lead_lag_multi_type.sql:54`):
```sql
-- @tags=subquery,oq13,assert_one_row
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE smr (k INT, v INT);
INSERT INTO smr VALUES (1,10),(1,20);
-- A scalar subquery that returns >1 row must error at runtime (AssertOneRow).
-- @expect_error=more than one row
SELECT (SELECT v FROM smr WHERE k = 1);
```
> Implementer: confirm the exact runtime error substring NovaRocks emits for AssertOneRow (grep `subquery_text` / the AssertOneRow operator runtime message in `src/exec`) and use that substring in `@expect_error`.

- [ ] **Step 2: Verify**: `--suite subquery --only scalar_subquery_multi_row_error --mode verify` → PASS. Commit.

### Task 5.5: Full no-regression validation

- [ ] **Step 1: Run the acceptance suites**
```bash
for s in optimizer join filter sort cte; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify 2>&1 | tail -3
done
```
Expected: no new failures vs the pre-change baseline (record the baseline first on `main`/the branch point if unsure).

- [ ] **Step 2: Run the full lib test suite**
Run: `cargo test --lib 2>&1 | tail -5`
Expected: PASS (the pre-existing failures noted in memory stay unchanged; diff against baseline).

- [ ] **Step 3: Final commit / branch ready for PR**
```bash
git add -A && git commit -m "test(oq13): ranking-window closeout — correctness + suite validation"
```

---

## Acceptance (spec §10)
- [ ] `ranking_window_topn.sql` golden shows `partition_limit` + `topn_type` + `WINDOW`; rule-on and rule-off rows identical.
- [ ] `ranking_window_topn_rejected.sql` confirms the three guards (aggregate-over / no-upper-bound / no-partition).
- [ ] Correctness suite: rank=1, ties, dense_rank, row_number, NULL order key, empty group — identical with rule on/off.
- [ ] tpc-h q2/q17 plan goldens locked; scalar-multi-row error case added.
- [ ] `optimizer`/`join`/`filter`/`sort`/`cte` + `cargo test --lib`: no new failures vs baseline.
