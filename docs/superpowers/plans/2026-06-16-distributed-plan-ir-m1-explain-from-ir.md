# DistributedPlan IR — M1: EXPLAIN from the IR (with fragmentation prerequisite)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render `EXPLAIN` (Normal / Verbose / Costs / Analyze-body) from the `DistributedPlan` IR in StarRocks structural form (`PLAN FRAGMENT N` headers, input/output partition, `node_id:` prefixes, distribution, RF, cost), retiring the legacy `PhysicalPlanNode`-based `format_physical_node`. **Prerequisite folded in:** the merged M0 (#322) IR is single-fragment only — `build_distributed_plan` errors on `PhysicalDistribution`/CTE/UNION-DISTINCT/TopN-split/Limit-offset. M1 therefore **first completes the IR's multi-fragment support** (Phase A), then renders from it (Phase B).

**Architecture:** Spec `docs/superpowers/specs/2026-06-15-plannode-ir-explain-observability-design.md` (§7 E.1/E.2, §9 M1). Phase A extends `build_distributed_plan`/`lower_distributed_plan` (`src/sql/codegen/ir/`) to multi-fragment, gated by the existing **byte-identical** `equiv.rs` harness (`build_via_distributed_plan` == old `build`). Phase B adds `src/sql/codegen/ir/explain.rs` rendering the IR and switches the engine EXPLAIN entrypoints to it. **Execution cutover is NOT in scope** — execution keeps using the legacy `build`; EXPLAIN independently builds the IR via `build_distributed_plan` (faithful because Phase A proves byte equivalence).

**Tech Stack:** Rust, thrift 0.17 (`PartialEq`+`Debug`), `cargo test`, `sql-tests` runner.

**Branch:** `claude/dist-plan-ir-m1-explain` (off `origin/main` = #326; M0 lowering coverage #322 merged).

**Actual IR names (verified on this branch — use these, NOT the spec's `*Body` names):**
- `DistributedPlanNode { node_id, fragment_id, tuple_ids, nullable_tuple_ids, limit, execution_join_distribution: Option<JoinExecutionDistribution>, build_runtime_filters: Vec<RuntimeFilterDesc>, probe_runtime_filters: Vec<RuntimeFilterProbe>, children, stats: PlanNodeStats, kind: DistributedPlanNodeKind }` (`ir/node.rs`).
- `DistributedPlanNodeKind` enum (`ir/node.rs`): `Scan/Project/Sort/TopN/HashAggregate/HashJoin/NestLoopJoin/Values/AssertOneRow/Decode/Repeat/SetOp/Window/GenerateSeries/TableFunction` — per-op structs `Distributed<Op>Node` in `ir/kind.rs`.
- `PlanNodeStats { output_row_count: f64, row_count_confidence: Confidence }` — **no per-column stats yet** (Task A5 adds them for COSTS).
- `DistributedPlan { fragments: Vec<PlanFragment>, root_fragment_id }`; `PlanFragment { fragment_id, root, data_partition, output_partition, sink, output_exprs, output_columns }` (`ir/fragment.rs`). **No `edges` yet** (Task A1 adds).
- `build_distributed_plan(&PhysicalPlanNode) -> Result<DistributedPlan, String>` (`ir/build.rs:659`); `build_via_distributed_plan` (`fragment_builder.rs:489`); `lower_distributed_plan` (`ir/lowering.rs`); `equiv.rs` byte-comparison harness with `assert_distributed_plan_equivalent` + `assert_multi_fragment_equivalent`.

**Legacy multi-fragment machinery to mirror (verbatim source for Phase A):** `fragment_builder.rs` `visit_distribution`, `visit_cte_anchor/produce/consume`, `visit_limit_offset_exchange`, `visit_physical_top_n_final_split`, `build_with_mv_refresh_ctx` assembly; `nodes.rs` `build_exchange_node`/`build_limit_exchange_node`/`build_merging_exchange_node`/`build_noop_sink`; `mod.rs` `FragmentEdge`/`FragmentStreamKind`/`FragmentEdgeKind`; `property.rs` `DistributionSpec`.

**Legacy renderer to port (verbatim source for Phase B):** `src/sql/explain.rs` `format_physical_node` (all operator arms), `format_stats_trailer*`, `format_column_stats_costs`, `join_distribution_label`, `push_probe_rf_lines`, scan-hint helpers, `format_expr*`, `format_boundary_schema_reports`. Engine entrypoints `explain_query` (`engine/mod.rs:3289`), `explain_analyze_query` (`:3217`).

**Run unit tests:** `cargo test --lib sql::codegen` (IR/equiv) and `cargo test --lib sql::explain` (renderer).
**Re-record explain goldens:** `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --mode record --record-from target` (per memory: NovaRocks-only uses `--record-from target`).

---

## Phase ordering

```
Phase A  IR multi-fragment support   (build_distributed_plan covers exchange/CTE/split; multi-fragment lower; +colstats)
           gate: equiv.rs byte-identical vs legacy build (single + multi fragment)
Phase B  EXPLAIN renders from the IR  (fragment headers + node_id prefixes; retire format_physical_node; switch entrypoints; golden re-record)
```

Phase A is additive + test-only (engine execution unchanged; the equiv harness exercises `build_via_distributed_plan`). Phase B is the user-visible change. Each task is independently committable and `cargo test --lib sql::codegen`-green.

---

## Phase A — Complete the IR's multi-fragment support

Goal: `build_distributed_plan` produces correct multi-fragment `DistributedPlan` for **any** SELECT physical plan (the ones EXPLAIN sees), byte-identical to legacy `build` (via `build_via_distributed_plan` + the `equiv.rs` harness). Mirrors the legacy fragment machinery into the IR. (Direct-exec / AggregateStateMerge are MV-refresh-only and never appear in a SELECT EXPLAIN — out of scope.)

### Task A1: Multi-fragment IR shape + multi-fragment `lower_distributed_plan`

**Files:** `ir/fragment.rs`, `ir/lowering.rs`

- [ ] **Add `edges` to `DistributedPlan`** and (if absent) `cte_id: Option<CteId>` + `cte_exchange_nodes: Vec<(CteId, i32)>` to `PlanFragment`, mirroring `FragmentBuildResult`. Reuse `crate::sql::codegen::{FragmentEdge, FragmentStreamKind, FragmentEdgeKind}`.
```rust
// ir/fragment.rs
pub(crate) struct DistributedPlan {
    pub fragments: Vec<PlanFragment>,
    pub root_fragment_id: FragmentId,
    pub edges: Vec<crate::sql::codegen::FragmentEdge>,   // NEW
}
// PlanFragment: add `pub cte_id: Option<CteId>` + `pub cte_exchange_nodes: Vec<(CteId, i32)>`.
```
- [ ] **Relax `validate_m0_root_fragment` → `validate_distributed_plan`** (`ir/lowering.rs`): allow N fragments; require exactly one root (`fragment_id == root_fragment_id`, `sink == Result`) and the rest `sink == Noop`; allow partitioned `output_partition` on non-root fragments; keep failing on genuinely-unsupported shapes.
- [ ] **Multi-fragment assembly in `lower_distributed_plan`** — mirror `build_with_mv_refresh_ctx`'s assembly (the verbatim block): lower **every** fragment's `root` tree through **one shared** `OwnedLoweringState` (single `desc_builder`, slot counter, `scan_tables`, RF + dict accumulators across all fragments — the legacy builder shares them, so slot/tuple ids match only if we share too); build shared `desc_tbl` + `exec_params` once; **patch them into every** `FragmentBuildResult`; assemble `edges` from `DistributedPlan.edges`; drain per-fragment dicts; assemble `rf_plan` from the shared RF accumulators; root fragment gets `build_result_sink()`, children `build_noop_sink()`. Return `MultiFragmentBuildResult { fragment_results, root_fragment_id, edges, boundary_schemas, rf_plan }`.
- [ ] Existing single-fragment equiv cases must still pass (N=1 is a special case). Run `cargo test --lib sql::codegen::ir`.
- [ ] Commit: `codegen/ir: multi-fragment DistributedPlan shape + lower assembly`.

### Task A2: Exchange (`PhysicalDistribution`)

**Files:** `ir/kind.rs`, `ir/node.rs`, `ir/build.rs`, `ir/lowering.rs`

- [ ] **Add `Exchange` kind** (`ir/node.rs` enum + `ir/kind.rs` struct):
```rust
// ir/kind.rs
pub(crate) struct DistributedExchangeNode {
    pub partition_type: crate::sql::codegen::partitions::TPartitionType,
    pub partition_exprs: Vec<TypedExpr>,        // hash keys when HASH (resolved at lower time)
    pub source_fragment_id: FragmentId,         // producer fragment (edge source)
    pub flavor: ExchangeFlavor,
}
pub(crate) enum ExchangeFlavor {
    Distribution,
    LimitOffset { limit: Option<i64>, offset: Option<i64> },   // Task A4
    TopNSplit,                                                  // Task A4 (sort info rebuilt from child at lower)
    CteMulticast { cte_id: CteId },                            // Task A3
}
```
- [ ] **`DistributedPlanBuilder` gains the legacy fragment stack** (`ir/build.rs`): add `next_fragment_id`, `fragment_stack: Vec<FragmentId>`, `completed_fragments: Vec<PlanFragment>`, `edges: Vec<FragmentEdge>`, `cte_fragments: HashMap<CteId, usize>`. Top-level `build_distributed_plan` pushes a root fragment id, visits, then assembles `DistributedPlan { fragments: completed ++ [root], root_fragment_id, edges }` — mirroring `build_with_mv_refresh_ctx:902-992`.
- [ ] **`PhysicalDistribution` arm** (mirror `visit_distribution`): `alloc_fragment_id()`, push stack, visit child into it, pop; build `DataPartition` from `op.spec` (`DistributionSpec::{Gather,Broadcast,HashPartitioned{cols,source}}` — store the spec/exprs in `DistributedExchangeNode` so `HashPartitioned`'s `cols: Vec<ColumnId>` resolve against the child scope at **lower** time); push the child `PlanFragment` (sink `Noop`); emit an `Exchange{flavor:Distribution}` IR node in the parent referencing `source_fragment_id`; record a `FragmentEdge` (reuse legacy `stream_kind_for_distribution`). Replace the current `Err("...Phase 2")`.
- [ ] **Pass-2 `Exchange` arm** in `lower_node`: `nodes::build_exchange_node(node_id, tuple_ids, partition_type)`. The child-fragment `Noop` sink + the `FragmentEdge.output_partition` are assembled in `lower_distributed_plan` (Task A1); relocate the legacy `build_output_partition`/`record_completed_edge` onto `LoweringStateAccess`/free fns.
- [ ] Equiv: `shuffle_agg` (Aggregate over `PhysicalDistribution(HashPartitioned)` over Scan → 2 fragments), `broadcast_join`, `gather_root`. Use `assert_multi_fragment_equivalent`. Assert `fragment_results` + `edges` + each `plan` byte-identical.
- [ ] Commit: `codegen/ir: lower PhysicalDistribution / exchange (multi-fragment)`.

### Task A3: CTE (anchor / produce / consume)

**Files:** `ir/build.rs`, `ir/lowering.rs`

- [ ] Mirror `visit_cte_anchor`/`visit_cte_produce`/`visit_cte_consume`: CTEProduce allocates a CTE fragment (recorded in `cte_fragments`), emits no node; CTEConsume emits an `Exchange{flavor:CteMulticast{cte_id}}` node + records a `FragmentEdgeKind::CteMulticast` edge; CTEAnchor visits produce (side-effect) then returns consume. Replace the catch-all `Err`.
- [ ] Equiv: `cte_produce_consume`. Assert fragments + edges (incl. `CteMulticast`) byte-identical.
- [ ] Commit: `codegen/ir: lower CTE produce/consume/anchor`.

### Task A4: TopN-split + Limit-offset-exchange + UNION DISTINCT

**Files:** `ir/build.rs`, `ir/lowering.rs`

- [ ] **TopN `(Final, is_split)`** (mirror `visit_physical_top_n_final_split`): child fragment with partial SORT; parent merging exchange (`Exchange{flavor:TopNSplit}`) → lower via `build_merging_exchange_node` rebuilding `TSortInfo` from the child's lowered SORT node. Replace `Err("TopN split is Phase 2")`.
- [ ] **Limit-offset-exchange** (mirror `visit_limit_offset_exchange`): `Exchange{flavor:LimitOffset{limit,offset}}` → `build_limit_exchange_node`. Replace `Err("limit-offset-exchange is Phase 2")`.
- [ ] **UNION DISTINCT** (mirror `visit_union` distinct path): UNION ALL → synthetic `PhysicalDistribution(Gather)` → group-by-all `HashAggregate` (`emit_distinct_on_top`). The Pass-1 Union arm for `!op.all` constructs that sub-shape. Replace `Err("UNION DISTINCT is Phase 2")`.
- [ ] Equiv: `topn_split`, `limit_offset_exchange`, `union_distinct_two_scans`.
- [ ] Commit: `codegen/ir: lower topn-split + limit-offset-exchange + union distinct`.

### Task A5: Per-column stats in `PlanNodeStats` (for EXPLAIN COSTS)

**Files:** `ir/node.rs`, `ir/build.rs`

- [ ] Extend `PlanNodeStats` to carry the per-column stats COSTS renders: `pub column_statistics: HashMap<ColumnId, ColumnStatistic>` (clone from `PhysicalPlanNode.stats.column_statistics` in `build_distributed_plan`'s per-node construction — currently only `output_row_count`+`confidence` are copied). `ColumnStatistic` is `crate::sql::optimizer::statistics::ColumnStatistic`.
- [ ] No equiv-harness change (the harness compares thrift, which doesn't carry colstats); add a `build.rs` unit test asserting a scan node's `stats.column_statistics` is populated from the physical plan.
- [ ] Commit: `codegen/ir: carry per-column stats on PlanNodeStats for COSTS`.

**Phase A exit:** `build_via_distributed_plan` produces byte-identical multi-fragment thrift for every SELECT shape (exchange/CTE/split/union-distinct); `PlanNodeStats` carries colstats. `cargo test --lib sql::codegen` green.

---

## Phase B — EXPLAIN renders from the IR

Goal: one renderer over `DistributedPlan`, StarRocks-structural, retiring `format_physical_node`. Engine EXPLAIN entrypoints build the IR and render it.

### Task B1: `explain_distributed_plan` skeleton — fragments + node_id prefixes + per-node rendering

**Files:** create `src/sql/codegen/ir/explain.rs`; `ir/mod.rs` (export)

- [ ] **Fragment walk + headers.** `pub(crate) fn explain_distributed_plan(dp: &DistributedPlan, level: ExplainLevel) -> Vec<String>` iterates `dp.fragments` in StarRocks order (root last-built → printed as `PLAN FRAGMENT 0`; order children after root by reversing, matching `ExecPlan.getExplainString`). Per fragment emit (Verbose/Costs/Analyze):
```
PLAN FRAGMENT <n>
  OUTPUT EXPRS: <output_exprs via format_expr, or "*">
  PARTITION: <data_partition label>
  STREAM DATA SINK / EXCHANGE ID + <output_partition label>   (non-root fragments / when an edge targets this fragment)
  <root DistributedPlanNode tree, node_id-prefixed>
```
Add `DataPartition::explain_label()` (`UNPARTITIONED` / `RANDOM` / `HASH_PARTITIONED (col, …)`). Normal level: suppress fragment headers + partitions + stats trailers + RF lines (flat tree, but **with** node_id prefixes — confirmed decision).
- [ ] **Per-node rendering** `fn format_distributed_node(node: &DistributedPlanNode, level, indent, out)` — a `match &node.kind` that **ports the verbatim text from `format_physical_node`** (each arm), reading the IR fields instead of `Operator::Physical*`:
  - node line gains a **`<node_id>:` prefix** at all levels (`format!("{pad}{}:{LABEL}…", node.node_id)`).
  - Scan: `DistributedScanNode` (database/table/alias/columns/predicates/mv_rewritten_from + min-max/decode/pruned-type/variant hints — port the scan-hint helpers to read `DistributedScanNode`).
  - HashJoin: distribution label from `node.execution_join_distribution` (port `join_distribution_label`); eq conditions from `DistributedHashJoinNode.eq_conditions`; build RF from `node.build_runtime_filters`.
  - HashAggregate/Sort/TopN/Limit(none — folded)/NestLoopJoin/Values/AssertOneRow/Decode/Repeat/SetOp(Union/Intersect/Except)/Window/GenerateSeries/TableFunction: port each arm's text from `format_physical_node`.
  - Exchange (`DistributedExchangeNode`): render the StarRocks exchange label (`EXCHANGE`/`HASH EXCHANGE`/`GATHER`) with the `node_id:` prefix; it is a fragment-boundary leaf.
  - stats trailer (`stats={rows=…}`) from `node.stats` (port `format_stats_trailer*`); probe RF lines from `node.probe_runtime_filters` (port `push_probe_rf_lines`).
- [ ] Reuse `format_expr`/`format_expr_kind` as-is (they take `TypedExpr`, which the IR carries) — move them to a shared location or re-export so both the (temporarily still-present) old renderer and the new one compile.
- [ ] Unit tests in `ir/explain.rs`: a single-fragment scan/project/agg plan renders with `node_id:` prefixes; a 2-fragment shuffle-agg renders `PLAN FRAGMENT 0` + `PLAN FRAGMENT 1` + an `EXCHANGE`. Build the `DistributedPlan` via `build_distributed_plan` on a hand-built physical plan.
- [ ] Commit: `codegen/ir: explain_distributed_plan skeleton + per-node rendering`.

### Task B2: Costs (colstats) + stats trailers + RF/dict lines from the IR

**Files:** `ir/explain.rs`

- [ ] Costs level: render `(rows=N)` + `colstats={col#…}` from `node.stats` (now carrying `column_statistics` after Task A5) — port `format_column_stats_costs` to read `PlanNodeStats`. Stats trailer confidence suffix (`conf=estimated/fallback`) for Costs/Analyze.
- [ ] Build/probe RF lines (Verbose/Costs/Analyze) from `node.build_runtime_filters`/`node.probe_runtime_filters`.
- [ ] Port the explain RF unit tests (`explain_shows_build_and_probe_rf`, `explain_normal_level_hides_rf`) to construct a `DistributedPlan` (via `build_distributed_plan` on a join physical plan) and assert the same RF lines.
- [ ] Commit: `codegen/ir: COSTS colstats + RF lines from the IR`.

### Task B3: Switch engine EXPLAIN entrypoints; retire `format_physical_node`; golden re-record

**Files:** `src/engine/mod.rs`, `src/sql/explain.rs`, `sql-tests/optimizer/*`

- [ ] **`explain_query`** (`engine/mod.rs:3289`): after `optimize` → `physical`, build `let dp = crate::sql::codegen::ir::build_distributed_plan(&physical)?;` then `lines.extend(crate::sql::codegen::ir::explain_distributed_plan(&dp, level));`. Keep the Costs `Statistics:` table prefix. Drop the separate `PlanFragmentBuilder::build` boundary-schema call (boundary schemas now derivable from the IR's fragments if still wanted — or drop the `Boundary Schemas` block per spec's "concise default"; **decision: drop it from default Verbose**, matching spec §5/§7 "concise").
- [ ] **`explain_analyze_query`** (`engine/mod.rs:3217`): render the body via `build_distributed_plan(&physical)` + `explain_distributed_plan(&dp, Analyze)` (keep the existing `Planning/Execution/Rows` header). Per-operator **actual** stats remain M2 — Analyze body == Verbose body here.
- [ ] **Retire** `explain_physical_plan` + `format_physical_node` + the now-unused physical-node helpers in `src/sql/explain.rs`. Keep `explain_plan`/`format_node` over `LogicalPlan` (separate, unrelated). Keep `format_expr*` (shared). Move the still-needed helpers (stats trailer, colstats, scan-hint, RF) into `ir/explain.rs` or a shared module.
- [ ] **Re-record explain goldens** (reviewed diff): the 28 `sql-tests/optimizer/` files with `@explain_contains`/`@normalize_explain_timing` + the ~26 in-module `explain.rs` tests churn due to `node_id:` prefixes + fragment headers. Re-record with `--mode record --record-from target`; **review the diff** to confirm only prefix/fragment-structure changes (no semantic plan changes). Port the in-module explain tests to drive `build_distributed_plan` + `explain_distributed_plan`.
- [ ] Run `cargo test --lib` + the optimizer sql-test suite in `--mode verify`.
- [ ] Commit: `codegen/ir: EXPLAIN renders from the IR; retire format_physical_node; re-record goldens`.

**Phase B exit:** `EXPLAIN [VERBOSE|COSTS]` shows StarRocks fragment structure (`PLAN FRAGMENT N`, partitions, `node_id:` prefixes, distribution, cost, RF) rendered from the IR; the legacy physical-node renderer is gone; one renderer, one source.

---

## Risks & notes

- **Phase A is the bulk and the risk.** Multi-fragment lower must be **byte-identical** to `build_with_mv_refresh_ctx` — same shared `desc_builder`/slot-counter/`scan_tables`/RF/dict across all fragments (single shared `OwnedLoweringState`), else slot/tuple ids diverge and the equiv harness fails. Mirror the legacy lifetimes exactly. The `equiv.rs` byte comparison (single + multi fragment) is the gate.
- **EXPLAIN builds the IR independently of execution.** Execution still uses legacy `build`; EXPLAIN uses `build_distributed_plan`. Faithful because Phase A proves byte equivalence. (Execution cutover + legacy-visitor deletion remain the separately-tracked deferred M0 Phase 4–5.)
- **`build_distributed_plan` is pure (no catalog/connectors)** — good for EXPLAIN. But scan exec-params/connector planning happen at **lower** time (in `lower_distributed_plan`, which EXPLAIN does not call). EXPLAIN renders structure from the un-lowered `DistributedPlan` — confirm `DistributedScanNode` carries enough (table/columns/predicates) to render the scan line without lowering. It does (Task A2/Phase-1 fields).
- **AggregateStateMerge / MV-refresh direct-exec** never appear in a SELECT EXPLAIN (MV-refresh-only; `EXPLAIN ANALYZE REFRESH MV` is already unsupported) — deliberately out of Phase A scope.
- **Golden churn is large** (node_id prefixes touch every node line, incl. Normal). Budget the re-record as its own reviewed step; do not hand-edit goldens.
- **`Boundary Schemas` block**: dropped from default Verbose (spec "concise default"). If any consumer needs it, gate behind a debug flag — not in this plan.
- **No backwards-compat shim** (project memory): delete `format_physical_node` outright in B3.

## Self-review (spec §7/§9 coverage)
- IR multi-fragment (prerequisite) — Phase A (A1 shape/assembly, A2 exchange, A3 CTE, A4 split/offset/union-distinct, A5 colstats). ✓
- `explain_distributed_plan` + fragment headers + node_id prefixes — B1. ✓
- COSTS colstats + RF from IR — A5 + B2. ✓
- Switch entrypoints + retire `format_physical_node` + Normal via IR + golden re-record — B3. ✓
- EXPLAIN ANALYZE per-operator **actuals** — explicitly **M2**, not here (Analyze body == Verbose body in M1).
- Expression-parenthesization display fix (spec Workstream D) — orthogonal; not in M1 unless a golden surfaces it.
