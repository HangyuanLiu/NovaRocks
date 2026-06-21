# DistributedPlan IR — Complete M0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish milestone **M0** of the DistributedPlan IR refactor: bring `build_distributed_plan` + `lower_distributed_plan` to full parity with the legacy `PlanFragmentBuilder::build*` over **every** operator, fragmentation, direct-exec, MV-refresh and Iceberg-sink path; **cut the engine over** to the IR path; and **delete the old eager visitor**. End state: one IR (`PhysicalPlanNode → DistributedPlan → thrift`) is the single source for both EXPLAIN and execution.

**Architecture:** Spec `docs/design/specs/2026-06-15-plannode-ir-explain-observability-design.md`. Strategy throughout = **extract-and-share**: each operator's lowering core moves out of `visit_*` (in `src/sql/codegen/fragment_builder.rs`) into a `LoweringCtx` method (`src/sql/codegen/ir/lowering.rs`) that both the legacy builder and the new Pass-2 call, so behavior is identical by construction. Pass 1 (`build_distributed_plan`, `ir/build.rs`) = structure/identity/topology; Pass 2 (`lower_distributed_plan`, `ir/lowering.rs`) = slot alloc + compile + thrift. Equivalence is gated by a **byte-identical** thrift comparison (`ir/equiv.rs`) plus the full SQL suites at cutover.

**Tech Stack:** Rust, thrift 0.17 generated types (all derive `PartialEq`+`Debug`), `cargo test`, `sql-tests` runner.

**Status / prior art:**
- **Slice 1 (Scan/Filter/Project) merged = PR #318** (`b88bbd17`). The IR module, `LoweringStateAccess` trait, `LoweringCtx`, `lower_scan`/`lower_project`, the `*_body_to_physical_op` adapters, and the `equiv.rs` byte-comparison harness already exist and are the template every phase below copies.
- This plan supersedes the slice-2-only plan (folded into Phase 1).

**Branch:** `claude/dist-plan-ir-m0-slice2` (off `origin/main`; rename optional).

**Run unit tests:** `cargo test --lib sql::codegen`
**Run a SQL suite (cutover gate):** `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite <suite> --mode verify`

---

## Phase ordering & rationale

```
Phase 1  Remaining single-fragment operators   (extract-and-share; equiv per op)
Phase 2  Fragmentation (multi-fragment IR)      (Exchange/CTE/TopN-split/Limit-offset/UNION DISTINCT)
Phase 3  Direct-exec + MV-refresh + Iceberg sink (AggregateStateMerge/UnionAll/Physicalize; mv ctx; sink)
Phase 4  Cutover                                (real-plan equiv harness → switch 5 call sites → SQL suites)
Phase 5  Delete the legacy visitor              (remove visit_*/old build entry; IR is the only path)
```

Phases 1–3 are **additive and behind tests only** (engine still calls the old builder; `build_via_distributed_plan` is exercised solely by `equiv.rs`). Phase 4 flips the engine. Phase 5 deletes the dead path. Within a phase, each task is independently committable and `cargo test --lib sql::codegen`-green.

---

## The extract-and-share recipe (Phase 1 & most of Phase 2)

Every single-fragment operator follows this 8-step recipe. Tasks below give only the **operator-specific deltas** (a spec row); apply the recipe verbatim using slice 1's `lower_scan`/`lower_project` and slice-1 `equiv.rs` cases as the worked template.

1. **Body type** — add `<Op>Body` to `ir/body.rs` mirroring `Physical<Op>Op`'s fields. Add `output_columns: Vec<OutputColumn>` **iff** the `visit_<op>` body reads `node.output_columns` (column-count table).
2. **Enum variant** — add `<Op>(<Box?><Op>Body)` to `DistributedPlanNodeBody` (`ir/node.rs`); box variants ≥ ~3 `Vec` fields.
3. **Keep `lower_node` exhaustive** — add a temporary `… => Err("<op> lowering not yet implemented")` arm in `LoweringCtx::lower_node` so the crate compiles between tasks; replaced in step 6.
4. **Pass 1 arm** (`ir/build.rs` `DistributedPlanBuilder::visit`) — `expect_single_child` (or visit both for binary ops), `self.visit(child, fragment_id)`, `alloc_node()`, `alloc_tuple()` **only if the op materializes a new output tuple** (see "new tuples" column; passthrough ops reuse `child.tuple_ids`), build the `DistributedPlanNode { node_id, fragment_id, tuple_ids, nullable_tuple_ids: vec![], limit: -1, children, stats: PlanNodeStats::from_statistics(&node.stats), body }`.
5. **Extract core** (`fragment_builder.rs` → `ir/lowering.rs`) — cut the `visit_<op>` body into `LoweringCtx::lower_<op>(node_id[, tuple_id], op, child_scope[, …])`: drop the `self.visit(child)` line and the `alloc_*` lines (now params), replace `child.scope`→param, `node.output_columns`→param, `self.X`→trait method where `X` is builder state. Rewrite `visit_<op>` as a thin wrapper that allocs the ids then delegates (exactly like the new `visit_scan` at `fragment_builder.rs:1560`).
6. **Pass 2 arm** — replace the temporary `lower_node` arm: lower child(ren) first, build the `Physical<Op>Op` via a new `<op>_body_to_physical_op` adapter (next to `scan_body_to_physical_op`, `lowering.rs:947`), call `lower_<op>`, assemble `LoweredDistributedNode { plan_nodes: [op_nodes…, child.plan_nodes…], scope, output_columns }` in **pre-order** (this node's thrift node(s) first, then children).
7. **Equivalence test** (`ir/equiv.rs`) — add a `<op>_matches_direct_fragment_builder` case calling `assert_distributed_plan_equivalent("<op>", <hand-built plan>)`.
8. **Verify + commit** — `cargo test --lib sql::codegen` (existing fragment_builder tests prove the extract preserved behavior; new equiv case proves byte-identical). Commit.

> Relocations needed by the extracted cores (do once, when first required): `slot_ref_exprs_for_columns` (used by `lower_sort`/joins) onto `LoweringStateAccess`; verify `propagate_dict_to_slot`, `refresh_scan_table_for_codegen`, `slot_allocator`, `desc_builder`, `add_slot*`, `widen_tuple_nullable` are already on the trait (slice 1 added them).

---

## Phase 1 — Remaining single-fragment operators

All operators below are single-fragment (the existing `validate_m0_root_fragment` guard stays). Each task = the recipe applied with this spec.

### Task 1.1: Sort + HashAggregate

| Op | visit lines | thrift nodes | new tuples | Body fields | Special handling |
|---|---|---|---|---|---|
| Sort | `2508-2608` | 1 (`SORT_NODE`, `use_top_n=false`) | 0 (passthrough `child.tuple_ids`) | `items: Vec<SortItem>`, `analytic_partition_exprs: Vec<TypedExpr>`, `output_columns` | needs `slot_ref_exprs_for_columns(child_scope, output_columns)` → relocate it onto `LoweringStateAccess` this task; `lower_sort` returns one node; row_tuples = child tuple ids |
| HashAggregate | `2307-2502` | 1 (`AGGREGATION_NODE`) | 1 (`agg_tuple`; intermediate==output==agg_tuple) | `mode: AggMode`, `group_by: Vec<TypedExpr>`, `aggregates: Vec<AggregateCall>`, `is_merge: Vec<bool>`, `output_columns` (box the body) | group-by/agg compile + `add_slot_with_type_desc` + `propagate_dict_to_slot` + `build_aggregation_node` stay verbatim; merge phase (`is_merge[i]`) uses positional `child_scope.iter_columns().get(group_by.len()+i)` binding — works against the Pass-2 child scope unchanged |

- [ ] Apply recipe steps 1–8 for **Sort** and **HashAggregate**. Alloc order in `visit_hash_aggregate` wrapper: `agg_tuple_id` then `agg_node_id` (matches original `:2315-2316`).
- [ ] Equiv cases: `sort_over_scan`, `aggregate_single_over_scan` (group-by only, empty aggregates), `aggregate_with_count` (one `count(*)`-style `AggregateCall`, `is_merge=[false]`, `mode=Single`), `sort_over_project_over_scan`. Build `AggregateCall` by copying an existing aggregate test's construction (`grep 'AggregateCall {' src/sql/codegen/fragment_builder.rs`).
- [ ] Commit: `codegen/ir: lower sort + hash aggregate (single-fragment)`.

### Task 1.2: HashJoin + NestLoopJoin (binary, single-fragment)

| Op | visit lines | thrift nodes | new tuples | Body fields | Special handling |
|---|---|---|---|---|---|
| HashJoin | `1847-2040` | 1 (`HASH_JOIN_NODE`) | 0 (tuple_ids = left ++ right) | `join_type: JoinKind`, `eq_conditions: Vec<PhysicalHashJoinEqCondition>`, `other_condition: Option<TypedExpr>`, `distribution: JoinDistribution` (box the body) | **binary**: visit both children; merge scopes (SEMI/ANTI expose surviving side only); **nullable-tuple widening** per join type via `desc_builder.widen_tuple_nullable`; **runtime-filter build** (`build_rf_descriptors`) — see note below; eq-condition demotion to `other_join_conjuncts` |
| NestLoopJoin | `2216-2301` | 1 (`NESTLOOP_JOIN_NODE`) | 0 (tuple_ids = left ++ right) | `join_type: JoinKind`, `condition: Option<TypedExpr>` | binary; merge scopes; nullable widening; condition compiled against merged scope |

- [ ] **Recipe deltas for binary ops:** Pass-1 arm visits `node.children[0]` and `node.children[1]` (require exactly 2 children), `tuple_ids = [left.tuple_ids…, right.tuple_ids…]`. `lower_node` lowers both children, merges their scopes (mirror the original `visit_hash_join` scope-merge), calls `lower_hash_join(node_id, &left.tuple_ids, &right.tuple_ids, op, &left.scope, &right.scope)`.
- [ ] **Runtime filters:** `lower_hash_join`'s extracted body still calls `self.build_rf_descriptors(...)` and patches `hash_join_node.build_runtime_filters`, and records probe targets into the builder's RF accumulators (`rf_all_filters`/`rf_build_side_filters`/`rf_probe_side_filters`). These accumulators live on the legacy `PlanFragmentBuilder` today — **add them to `OwnedLoweringState` and the `LoweringStateAccess` trait** (mirror how dict accumulators were added in slice 1) so `lower_distributed_plan` can assemble `rf_plan` from them (the single-fragment `lower_distributed_plan` currently hardcodes `rf_plan: None` — change it to assemble from the state's RF accumulators, exactly like `build_with_mv_refresh_ctx:976-984`).
- [ ] Equiv cases: `inner_hash_join_two_scans` (with an eq condition that produces a runtime filter — assert `rf_plan` matches), `left_outer_hash_join` (nullable widening), `nest_loop_cross_join`.
- [ ] Commit: `codegen/ir: lower hash join + nest loop join (single-fragment, incl. RF)`.

### Task 1.3: Limit (fold) + TopN single/partial

| Op | visit lines | thrift nodes | new tuples | Representation | Special handling |
|---|---|---|---|---|---|
| Limit | `2824-2861` (single-node path) | **0 (fold)** | 0 | no body — folds into child | sets the child node's `limit`; if child is `Sort`, also sets `SortBody.offset`; `offset>0` without a `Sort`/`TopN` child that can absorb it ⇒ `visit_limit_offset_exchange` (multi-fragment, **Phase 2**) — Pass 1 must **fail-fast** on that shape here |
| TopN (single/partial) | `2632-2706` | 1 (`SORT_NODE`, `use_top_n=true`) | 0 (passthrough) | `TopNBody { items, limit, offset, phase, is_split }` | `(Final, is_split=true)` ⇒ multi-fragment (**Phase 2**) — Pass 1 fails-fast on it here; single/partial build the top-n SORT node with `limit`/`offset` |

- [ ] **Limit fold mechanics:** add a `limit: i64` propagation step to `lower_node` — after building each node's thrift node, set `tnode.limit = node.limit` (slice-1 nodes always had `limit == -1`, so this is a no-op for them and correct for folded limits). Add `offset: Option<i64>` to `SortBody` (set by the Limit fold; `lower_sort` writes it into `TSortNode.offset` instead of the hardcoded `None`). Pass-1 Limit arm: visit child, set `child.limit = op.limit.unwrap_or(-1)`; if `child.body` is `Sort`, set its `offset`; else if `op.offset > 0` return `Err("LIMIT/OFFSET without a SORT child is not supported")`; if the offset-exchange condition holds (`op.offset>0 && !limit_child_can_apply_offset_locally(child)`) return `Err("limit-offset-exchange is Phase 2")`. Return the child node (no new node).
- [ ] **TopN:** Pass-1 arm dispatches on `(op.phase, op.is_split)`: `(Final, true)` ⇒ `Err("TopN split is Phase 2")`; else build a `TopN` node (1 thrift SORT node, `use_top_n=true`). Extract `lower_top_n_single_or_partial` from `:2632-2706`.
- [ ] Equiv cases: `limit_over_scan`, `limit_over_sort` (order-by-limit → sort with `limit`+`offset`), `top_n_single_over_scan`.
- [ ] Commit: `codegen/ir: lower limit (fold) + topn single/partial`.

### Task 1.4: Set ops (UNION ALL / Intersect / Except) + Values + AssertOneRow + Decode + Repeat

| Op | visit lines | thrift nodes | new tuples | Body fields | Special handling |
|---|---|---|---|---|---|
| Union (ALL only) | `4403-4459` (+`visit_set_op_common`) | 1 (`UNION_NODE`) | 1 | `SetOpBody { kind: SetOpKind::UnionAll, output_columns, child_output_columns: Vec<Vec<OutputColumn>> }` | N children; per-child cast to output col types; `result_expr_lists`. **UNION DISTINCT is Phase 2** (it inserts a Gather + distinct agg) — Pass 1 fails-fast on `!op.all` here |
| Intersect | `4461-4481` | 1 (`INTERSECT_NODE`) | 1 | `SetOpBody { kind: Intersect, … }` | N children via `visit_set_op_common` |
| Except | `4483-4502` | 1 (`EXCEPT_NODE`) | 1 | `SetOpBody { kind: Except, … }` | N children |
| Values | `3422-3497` | 1 (`UNION_NODE` const) | 1 | `ValuesBody { rows: Vec<Vec<TypedExpr>>, columns }` | 0 children (source); const_expr_lists |
| AssertOneRow | `2952-2991` | 1 (`ASSERT_NUM_ROWS_NODE`) | 0 (passthrough) | `AssertOneRowBody { subquery_text }` | assertion `LE`, desired 1 |
| Decode | `1639-1810` | 1 (`DECODE_NODE`) | 1 (decode tuple) | `DecodeBody { mappings: Vec<DecodeMapping>, output_columns }` | new string slots for decoded cols, passthrough others; `dict_id_to_string_ids` map |
| Repeat | `3936-4099` | 1 (`REPEAT_NODE`) | 1 (virtual tuple) | `RepeatBody { …full PhysicalRepeatOp fields }` (box) | virtual grouping_id + grouping_fn slots; grouping_list |

- [ ] **Set ops are N-ary:** Pass-1 arm visits all `node.children`, stores them as `children`. `lower_node` lowers all children, calls `lower_set_op(node_id, tuple_id, kind, op, &child_results)`. Extract a single `lower_set_op` from `visit_set_op_common`; `visit_union`(ALL)/`visit_intersect`/`visit_except` delegate with the right `kind`.
- [ ] Apply recipe for Values, AssertOneRow, Decode, Repeat (each single-child or source). Repeat's body carries the full `PhysicalRepeatOp` (many fields) — mirror it field-for-field.
- [ ] Equiv cases: `union_all_two_scans`, `intersect_two_scans`, `except_two_scans`, `values_rows`, `assert_one_row_over_scan`, `decode_over_scan` (needs a dict-encoded scan — reuse an existing decode test's plan), `repeat_grouping_sets`.
- [ ] Commit: `codegen/ir: lower set-ops + values + assert-one-row + decode + repeat`.

### Task 1.5: Window + GenerateSeries + TableFunction (multi-thrift-node operators)

| Op | visit lines | thrift nodes | new tuples | Body fields | Special handling |
|---|---|---|---|---|---|
| Window | `2997-3186` (+ multi-group `3195-3422`) | 1–2 per group (`SORT_NODE`? + `ANALYTIC_EVAL_NODE`); multiple groups chain | int + out tuple per group | `WindowBody { window_exprs: Vec<WindowExpr>, output_columns }` (box) | groups by (partition_by, order_by) signature; each group emits Sort?+Analytic chained in pre-order; intermediate+output tuples per group |
| GenerateSeries | `3503-3653` | **2** (`TABLE_FUNCTION_NODE` + `UNION_NODE`) | 2 (param + output) | `GenerateSeriesBody { start, end, step, column_name, alias, output_column_id }` | emits 2 nodes in pre-order [TABLE_FUNCTION, UNION] |
| TableFunction (unnest) | `3673-3930` | **2** (`TABLE_FUNCTION_NODE` + `PROJECT_NODE`) | 2 (project + output) | `TableFunctionBody { function_name, args, output_columns, alias, is_left_join }` | emits 2 nodes [TABLE_FUNCTION, PROJECT]; outer-slot remapping |

- [ ] **Multi-node lower:** for these, `lower_<op>` returns `Vec<TPlanNode>` (not one). `lower_node` extends `plan_nodes` with the returned vec **then** the child's nodes, preserving the exact pre-order the original `visit_<op>` produced.
- [ ] Window: extract both `lower_window` (single-group) and the multi-group chaining; the body carries `window_exprs` and the lower re-derives groups (same grouping logic as `visit_window`).
- [ ] Equiv cases: `window_row_number_over_scan`, `generate_series`, `unnest_table_function_over_scan`.
- [ ] Commit: `codegen/ir: lower window + generate-series + table-function (multi-node)`.

**Phase 1 exit:** every single-fragment operator lowers via the IR; `cargo test --lib sql::codegen` green; `build_via_distributed_plan` produces byte-identical thrift to `build` for all single-fragment hand-built plans. `validate_m0_root_fragment` still rejects anything multi-fragment.

---

## Phase 2 — Fragmentation (multi-fragment IR)

This phase relaxes the single-fragment guard and ports the multi-fragment machinery (`fragment_builder.rs` `visit_distribution`/CTE/`build_with_mv_refresh_ctx` assembly) into the IR. After this, `build_via_distributed_plan` handles exchanges, CTE, TopN-split, Limit-offset, and UNION DISTINCT.

### Task 2.1: Multi-fragment IR shape + multi-fragment `lower_distributed_plan`

**Files:** `ir/fragment.rs`, `ir/node.rs`, `ir/lowering.rs`

- [ ] **Extend `PlanFragment`/`DistributedPlan`** (`ir/fragment.rs`) to carry what the legacy `FragmentBuildResult` needs per fragment: add `cte_id: Option<CteId>`, `cte_exchange_nodes: Vec<(CteId, i32)>`, and a richer `DataSink` (`Result` for root, `Noop` for child fragments). `DistributedPlan` already has `fragments: Vec<PlanFragment>` + `edges` (add `edges: Vec<FragmentEdge>` reusing `crate::sql::codegen::FragmentEdge`) + `root_fragment_id`.
- [ ] **Add `Exchange` body** (`ir/node.rs`): `Exchange(ExchangeBody)` where `ExchangeBody { partition_type: TPartitionType, partition_exprs: Vec<TypedExpr>, source_fragment_id: FragmentId, flavor: ExchangeFlavor }` and `enum ExchangeFlavor { Distribution, LimitOffset { limit, offset }, TopNSplit { sort_info_source_node: i32 }, CteMulticast { cte_id: CteId } }`. (The exchange's tuple_ids come from the IR node's `tuple_ids`.)
- [ ] **Relax `validate_m0_root_fragment`** → `validate_distributed_plan`: allow N fragments; require exactly one root fragment with `DataSink::Result` and the rest `Noop`; allow partitioned `output_partition` on non-root fragments; keep failing on shapes nothing produces.
- [ ] **Multi-fragment assembly in `lower_distributed_plan`** — mirror `build_with_mv_refresh_ctx:902-992` exactly: lower **every** fragment's root tree with a **shared** `OwnedLoweringState` (one `desc_builder`, one slot counter, one `scan_tables`, shared RF/dict accumulators across all fragments — because the legacy builder shares them), build the shared `desc_tbl` + `exec_params` once, **patch them into every** `FragmentBuildResult`, assemble `edges` from the IR's `edges`, drain per-fragment dicts, assemble `rf_plan`. Root fragment gets `build_result_sink()`, children `build_noop_sink()`.
- [ ] Build + the existing single-fragment equiv cases must still pass (single-fragment is N=1).
- [ ] Commit: `codegen/ir: multi-fragment DistributedPlan shape + lower assembly`.

### Task 2.2: Exchange / PhysicalDistribution (the fragment boundary)

**Files:** `ir/build.rs`, `ir/lowering.rs`

- [ ] **Pass-1 needs a fragment stack.** Give `DistributedPlanBuilder` the legacy builder's fragment management: `next_fragment_id`, `fragment_stack: Vec<FragmentId>`, `completed_fragments: Vec<PlanFragment>`, `edges: Vec<FragmentEdge>`, `cte_fragments: HashMap<CteId, usize>`. The top-level `build_distributed_plan` pushes a root fragment id, visits, then assembles `DistributedPlan { fragments: completed ++ [root], root_fragment_id, edges }` — mirroring `build_with_mv_refresh_ctx`.
- [ ] **`PhysicalDistribution` arm** (mirror `visit_distribution:4222-4311`): alloc child fragment id, push stack, visit child into it, pop; compute `output_partition` (`DataPartition`) from `op.spec` (`DistributionSpec::{Gather,Broadcast,HashPartitioned{cols,source}}` — `HashPartitioned` resolves `cols: Vec<ColumnId>` against the child scope **at lower time**, so the IR Exchange body stores the `DistributionSpec`/resolved exprs to defer); push the child `PlanFragment` (sink `Noop`); emit an `Exchange` IR node in the parent referencing `source_fragment_id`; record a `FragmentEdge`.
- [ ] **Pass-2 `Exchange` arm** in `lower_node`: build the thrift exchange node via `nodes::build_exchange_node(node_id, tuple_ids, partition_type)`; the child fragment's `DataStreamSink`/`NoOpSink` and the `FragmentEdge`'s `output_partition` are assembled in `lower_distributed_plan` (Task 2.1). Reuse the legacy `build_output_partition` / `record_completed_edge` logic — relocate them onto `LoweringStateAccess`/free fns.
- [ ] Equiv cases: `shuffle_agg` (Aggregate over `PhysicalDistribution(HashPartitioned)` over Scan → 2 fragments), `broadcast_join` (HashJoin with a `PhysicalDistribution(Broadcast)` build side), `gather_root` (already elided — assert parity). Assert `fragment_results`, `edges`, and each fragment's `plan` are byte-identical.
- [ ] Commit: `codegen/ir: lower PhysicalDistribution / exchange (multi-fragment)`.

### Task 2.3: CTE (anchor / produce / consume)

**Files:** `ir/build.rs`, `ir/lowering.rs`

- [ ] Mirror `visit_cte_anchor:4736`, `visit_cte_produce:4753`, `visit_cte_consume:4813`. CTEProduce allocates a CTE fragment (recorded in `cte_fragments`), emits no node; CTEConsume emits an `Exchange{flavor: CteMulticast{cte_id}}` node + records a `FragmentEdgeKind::CteMulticast` edge; CTEAnchor visits produce (side-effect) then consume.
- [ ] Equiv case: `cte_produce_consume` (a `WITH x AS (...) SELECT ... FROM x` shaped plan). Assert fragments + edges (incl. `CteMulticast`) byte-identical.
- [ ] Commit: `codegen/ir: lower CTE produce/consume/anchor`.

### Task 2.4: TopN-split + Limit-offset-exchange + UNION DISTINCT

**Files:** `ir/build.rs`, `ir/lowering.rs`

- [ ] **TopN `(Final, is_split)`** (mirror `visit_physical_top_n_final_split:2713`): child fragment with partial SORT, parent merging exchange via `build_merging_exchange_node` carrying the partial's `TSortInfo`. Represent as `Exchange{flavor: TopNSplit}`; the merging exchange's sort info is rebuilt in lower from the child's lowered SORT node.
- [ ] **Limit-offset-exchange** (mirror `visit_limit_offset_exchange:2863`): the Phase-1 fail-fast Limit arm now handles it — child fragment + `build_limit_exchange_node`. Represent as `Exchange{flavor: LimitOffset{limit,offset}}`.
- [ ] **UNION DISTINCT** (mirror `visit_union:4403` distinct path): build UNION ALL, wrap in a synthetic `PhysicalDistribution(Gather)`, then `emit_distinct_on_top` (the single group-by-all `HashAggregate`). In the IR this is the SetOp child → Exchange → HashAggregate; the Pass-1 Union arm for `!op.all` constructs that sub-shape.
- [ ] Equiv cases: `topn_split`, `limit_offset_exchange`, `union_distinct_two_scans`.
- [ ] Commit: `codegen/ir: lower topn-split + limit-offset-exchange + union distinct`.

**Phase 2 exit:** `build_via_distributed_plan` produces byte-identical multi-fragment thrift (`fragment_results` + `edges` + `rf_plan` + per-fragment dicts) for all exchange/CTE/split shapes. Equivalence harness extended to multi-fragment via the existing `assert_multi_fragment_equivalent` (`equiv.rs:105`).

---

## Phase 3 — Direct-exec + MV-refresh context + Iceberg sink

These are the build-entry behaviors `build_via_distributed_plan` does not yet have but the execute path needs.

### Task 3.1: MV-refresh context threading

**Files:** `ir/build.rs`, `ir/lowering.rs`, `fragment_builder.rs`

- [ ] Thread `mv_refresh_ctx: Option<&IcebergMvRefreshContext>` through `build_distributed_plan` and `lower_distributed_plan` (the legacy `OwnedLoweringState::new` already accepts an `Option`; wire it). It feeds `refresh_scan_table_for_codegen` and `build_exec_params_multi_with_refresh_context` — both already on the trait / called in lower. Add a `build_via_distributed_plan_with_mv_refresh_ctx` entry.
- [ ] Equiv case: an IMV-refresh-shaped plan (reuse an `iceberg-ivm` test plan) compared with `build_with_mv_refresh_ctx`.
- [ ] Commit: `codegen/ir: thread mv_refresh_ctx through build/lower`.

### Task 3.2: Direct-exec (AggregateStateMerge / branch-union UnionAll)

**Files:** `ir/build.rs`, `ir/fragment.rs`, `ir/lowering.rs`

- [ ] Add `direct_exec: Option<Box<DirectExecPlan>>` to `PlanFragment`. In `build_distributed_plan`, mirror the two short-circuits from `build_with_mv_refresh_ctx:882-899`: `try_build_branch_union_aggregate_direct` (PhysicalUnion(all) of Project(branch_id)/AggregateStateMerge → `DirectExecPlan::UnionAll`) and root `PhysicalAggregateStateMerge` → `build_aggregate_state_merge_direct`. These build child `PlanBuildResult`s directly (recurse via `build_via_distributed_plan` for the inner inputs) and set `direct_exec`; `lower_distributed_plan` passes `direct_exec` straight through into the `FragmentBuildResult` (no thrift node). `lower_plan_build_result` (engine, `:4040`) already handles `direct_exec` downstream — unchanged.
- [ ] Add an `AggregateStateMerge` IR body **only** as a structural placeholder for completeness (it never reaches `lower_node` — the short-circuit handles it before tree-walking), or skip the body entirely and keep the short-circuit purely in `build_distributed_plan`. Prefer the latter (YAGNI).
- [ ] Equiv case: an MV-refresh AggregateStateMerge plan + a branch-union plan compared with `build_with_mv_refresh_ctx`.
- [ ] Commit: `codegen/ir: direct-exec (aggregate-state-merge + branch union)`.

### Task 3.3: Iceberg write sink

**Files:** `ir/lowering.rs` (or a thin `fragment_builder.rs` wrapper)

- [ ] Add `build_via_distributed_plan_with_iceberg_sink` mirroring `build_with_iceberg_sink:749`: call `build_via_distributed_plan_with_mv_refresh_ctx`, then modify the root fragment's `output_sink` to the Iceberg write sink + set `output_exprs` from the root tuple + extend the descriptor table — identical post-processing to the legacy method (reuse `root_output_tuple_id_for_sink`, `iceberg_sink_output_exprs_for_tuple`, `sink_spec.build_sink`, `DescriptorTableBuilder::from_existing`).
- [ ] Equiv case: an INSERT-INTO-iceberg plan compared with `build_with_iceberg_sink` (assert root `output_sink` + `output_exprs` + `desc_tbl`).
- [ ] Commit: `codegen/ir: iceberg write sink via distributed plan`.

**Phase 3 exit:** `build_via_distributed_plan*` covers everything the three legacy `build*` entries do: single + multi fragment, direct-exec, mv-refresh, iceberg sink.

---

## Phase 4 — Cutover

### Task 4.1: Real-optimizer-output equivalence harness

**Files:** `ir/equiv.rs` (test-only)

- [ ] Today no test produces a `PhysicalPlanNode` from SQL (all hand-build). Add a `#[cfg(test)]` helper that, given a SQL string + a test catalog, runs `analyze → plan_query → optimize` to get a `PhysicalPlanNode` (reuse the standalone test catalog setup at `engine/mod.rs:5880+`), then asserts `build_with_mv_refresh_ctx(plan, …)` **==** `build_via_distributed_plan_with_mv_refresh_ctx(plan, …)` (full `MultiFragmentBuildResult`: per-fragment `plan`/`desc_tbl`/`exec_params`/`output_sink`/`output_exprs`, `edges`, `rf_plan`).
- [ ] Drive it over a corpus of representative SQL (a handful per operator family: scan/filter/project, agg, sort/limit/topn, joins, set-ops, window, cte, shuffle/broadcast, union-distinct). This proves equivalence on **real** optimizer output, not just hand-built trees.
- [ ] Run: `cargo test --lib sql::codegen::ir::equiv`. Expected: PASS. Any divergence is a real gap — fix the relevant phase task before proceeding.
- [ ] Commit: `codegen/ir: real-optimizer-output equivalence harness`.

### Task 4.2: Switch the engine call sites to the IR path

**Files:** `src/engine/mod.rs`

Switch all five legacy call sites (the old builder stays in the tree until Phase 5):

- [ ] **Main execute** (`:3629-3636`): `build_with_mv_refresh_ctx(...)` → `build_via_distributed_plan_with_mv_refresh_ctx(...)`. `choose_standalone_execution` consumes the result unchanged.
- [ ] **INSERT iceberg** (`:3442`): `build_with_iceberg_sink(...)` → `build_via_distributed_plan_with_iceberg_sink(...)`.
- [ ] **EXPLAIN ANALYZE** (`:3280`) and **EXPLAIN VERBOSE/ANALYZE** (`:3336`): `build(...)` → `build_via_distributed_plan(...)`.
- [ ] **testutil** (`:5887`): `build(...)` → `build_via_distributed_plan(...)`.
- [ ] Run: `cargo test --lib`. Expected: PASS.
- [ ] Commit: `codegen/ir: cut standalone engine over to build_via_distributed_plan`.

### Task 4.3: Full SQL-suite gate

- [ ] Run the SQL regression suites in `--mode verify` (the end-to-end behavior gate — actual query results, the ultimate equivalence check):
  ```bash
  for s in filter sort join cte aggregate ssb tpc-h tpc-ds; do
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite "$s" --mode verify
  done
  ```
  And the Iceberg suites with the docker env (per CLAUDE.md §7.3):
  ```bash
  source docker/iceberg-rest/runtime/current/env.sh && docker/iceberg-rest/up.sh
  for s in iceberg iceberg-rest iceberg-ivm; do
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify
  done
  ```
- [ ] Expected: all suites pass unchanged. Any failure ⇒ a remaining behavioral gap in the IR path; fix in the owning phase task and re-run. **Do not proceed to Phase 5 until every suite is green.**
- [ ] Commit (if any fixes were needed): `codegen/ir: fix <gap> surfaced by SQL-suite cutover gate`.

**Phase 4 exit:** the engine executes exclusively through the IR; all unit tests + SQL suites green.

---

## Phase 5 — Delete the legacy visitor

Now the old `visit_*` wrappers and the old build entry are dead (the extracted cores live on `LoweringCtx` and are called by the IR path; nothing calls the old visitor).

- [ ] Delete `PlanFragmentBuilder::visit` and every `visit_*` wrapper in `fragment_builder.rs` (they now only forward to the extracted cores). Delete `build`, `build_with_mv_refresh_ctx`, `build_with_iceberg_sink`'s **visitor bodies** — but keep the now-shared cores (`lower_scan`, `lower_aggregate`, … on `LoweringCtx`) and the helpers the IR path uses. Delete `VisitResult` and the legacy fragment-stack fields on `PlanFragmentBuilder` if no longer referenced.
- [ ] Rename `build_via_distributed_plan*` → `build*` (it is now *the* builder), or keep the names and delete the old shells. Update the five engine call sites accordingly.
- [ ] Remove the `#[allow(dead_code)]` on the IR entry points (now live).
- [ ] Run: `cargo build --lib` (no dead-code warnings for the IR entries), `cargo test --lib`, `cargo clippy --lib` (clean), and re-run the SQL suites once more.
- [ ] Commit: `codegen/ir: delete legacy fragment-builder visitor; IR is the only lowering path`.

**M0 complete:** `PhysicalPlanNode → DistributedPlan → thrift` is the single lowering path; EXPLAIN and execution derive from one IR. M1 (EXPLAIN VERBOSE from the IR) and M2 (EXPLAIN ANALYZE) can now build on it.

---

## Risks & notes

- **Phase 2 is the hard one.** The multi-fragment assembly (shared desc_tbl/slot-counter/scan_tables/RF/dict across all fragments) must be **byte-identical** to `build_with_mv_refresh_ctx`. The safest path: have `build_distributed_plan` own the same fragment-stack + shared-state structure the legacy builder has, so the only difference is "emit IR nodes" vs "emit thrift nodes". The byte-equiv harness (incl. Task 4.1 real-plan corpus) is the gate.
- **Shared mutable state across fragments.** The legacy builder uses ONE `desc_builder`/slot counter/`scan_tables`/RF accumulators for the whole query (all fragments). `OwnedLoweringState` must do the same (single instance threaded through all fragment lowering), or slot/tuple ids diverge. This is the central correctness constraint — mirror the legacy lifetimes exactly.
- **Filter-over-non-Scan** (flagged in slice-1 review): the IR Filter-fold requires the child to be a Scan; the legacy path pushed conjuncts onto the first node (could be a Project). Before cutover (Task 4.1 corpus), confirm the optimizer never emits Filter-over-non-Scan post-pushdown, or generalize the fold. The real-plan corpus + SQL suites will surface it.
- **RF / dict accumulators on the trait.** Several cores (`lower_hash_join`, dict-emitting scans) write builder-global accumulators. Each must be added to `LoweringStateAccess` + `OwnedLoweringState` (Task 1.2 does RF; slice 1 did dicts) so the single-source `lower_distributed_plan` can assemble `rf_plan`/`query_global_dicts`.
- **No backwards-compat shim** (per project memory): delete the legacy visitor outright in Phase 5; do not keep a dual path.
- **Granularity:** Phase 1 tasks may each be split per-operator if a reviewer prefers smaller commits; the recipe makes each operator independently shippable + equiv-gated.
- **clippy dead-code:** the IR entries stay `#[allow(dead_code)]` until Phase 4 wires them; removed in Phase 5.

## Self-review (spec coverage)

- IR types/enum/bodies — Phase 1 (per op) + Phase 2 (Exchange) + Phase 3 (direct-exec). ✓ all 24 inventory operators covered (Scan/Filter/Project done; Sort/Agg/Join/NLJ/Limit/TopN/SetOps/Values/AssertOneRow/Decode/Repeat/Window/GenerateSeries/TableFunction/Exchange/CTE×3/AggStateMerge).
- Two-pass build/lower — every phase. ✓
- Multi-fragment + edges + dicts + RF — Phase 2. ✓
- Direct-exec + MV + Iceberg sink — Phase 3. ✓
- Cutover (5 call sites) + equivalence gate (byte + real-plan + SQL suites) — Phase 4. ✓
- Delete legacy visitor — Phase 5. ✓
- EXPLAIN rendering from IR / ANALYZE = **M1/M2**, explicitly out of M0 scope.
