# NovaRocks MV Query Rewrite (IVM-scoped v1) — Design

**Status**: Approved (autonomous run mode)
**Date**: 2026-05-21
**Author**: Claude (Opus 4.7)
**Reference implementation**: StarRocks `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/`
**Branch**: `claude/stupefied-turing-c93a76`

---

## 1. Goals & Non-Goals

### Goals (v1)

1. Support **transparent query rewrite** for 4 IVM shapes:
   - **Projection-Filter** (single-base SPJF)
   - **Aggregate** (single-base SPJG)
   - **Join-Projection-Filter** (two-base inner equi-join SPJF)
   - **Join-Aggregate** (two-base inner equi-join SPJG)
2. **Predicate compensation**: when query's `WHERE` is more selective than MV's, apply the residual filter on top of MV scan. Reject if query is *less* selective than MV.
3. **Partition-level transparent UNION**: when MV's per-partition freshness covers only a subset of query's partitions, synthesize `UNION ALL(Scan(MV) on fresh partitions, ReExecute(MV-definition over base) on stale partitions)` as an equivalent plan candidate.
4. **CBO integration**: register MV rewrite as cascades transformation rules; the original plan and rewritten plan coexist in the Memo, picked by top-down cost search.
5. **Iceberg-backed MV scope**: v1 only handles MVs whose backend is `IcebergMvBackend`. Managed-lake MVs are out of scope.
6. **Observability**: `EXPLAIN VERBOSE` / `EXPLAIN ANALYZE` must show whether MV rewrite fired, which MV was chosen, and (for each rejected MV) the rejection reason.
7. **Kill switches**: session var `enable_materialized_view_rewrite` plus per-rule `disable_optimizer_rules='Mv*Rewrite'`.

### Non-Goals (push to v2+)

1. Managed-lake (StarRocks backend) MV rewrite.
2. View-delta / extra-join rewrite (`enable_materialized_view_view_delta_rewrite`).
3. Nested MV (MV-on-MV) rewrite.
4. Text-match rewrite (`enable_materialized_view_text_match_rewrite`).
5. Complex aggregate rollup (HLL / BITMAP / `percentile_approx`). v1 supports only `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` (stored as `SUM`+`COUNT`).
6. Global ColumnId-ification (ARCH G1) and `LogicalProperty` equivalence classes (ARCH G7). v1 introduces a local `MvColumnId` facility limited to the rewrite module; a mechanical replacement is expected when G1 lands.
7. MV hint syntax (`/*+ USE_MV(...) */`).
8. Staleness budget (`mv_rewrite_staleness_second`); partition-level transparent covers the partial-freshness scenario.
9. Time-series / time-bucket-aware rollup (StarRocks `AggregateTimeSeriesRule`).
10. CTAS-style / aux-index MVs and any MV shape not currently supported by IVM execution.

---

## 2. Architecture Overview

```
Query AST  ──→  Analyzer  ──→  LogicalPlan (planner)  ──→  RBO 4-pass  ──→  Operator tree
                                                                                  │
                                                                                  ▼
                                                              convert::logical_plan_to_memo
                                                                                  │
                                                                                  ▼
                                                                              Memo (CBO)
                                                                                  │
                          ┌───────────────────────────────────────────────────────┤
                          │                                                       │
                          ▼                                                       ▼
              existing transform rules                              new MV rewrite rules
                                                                                  │
                              ┌─────────────────────────────────────────┬─────────┴─────────┬───────────────────────────┐
                              │                                         │                   │                           │
                  MvProjectionRewriteRule              MvAggregateScanRewriteRule    MvJoinRewriteRule         MvAggregateJoinRewriteRule
                              │                                         │                   │                           │
                              └──────────────────────────┬──────────────┴───────────────────┴───────────────────────────┘
                                                         ▼
                                              MvRewriter (shared engine)
                                                         │
            ┌────────────────────────────┬───────────────┼────────────────────────────┬──────────────────────────────┐
            ▼                            ▼               ▼                            ▼                              ▼
  MvCandidateRegistry        MvColumnId factory     PredicateSplit              ColumnRewriter             PartitionCompensator
  (base FQN → mv_ids)        (canonical IDs)        (eq/range/residual)         (query↔MV col map)         (fresh/stale split + UNION)
                                                         │                                                          │
                                                         ▼                                                          ▼
                                          (StoredMvDefinition,                                       Iceberg snapshot-diff
                                           MvSchemaContract,                                         (reused IVM delta-scan
                                           MvPartitionContract)                                       infrastructure)
```

**Key flow per query**:
1. Optimizer enters explore phase. For each MExpr whose op is a `LogicalScan` / `LogicalAggregate` / `LogicalJoin`, the corresponding MV rule's `matches()` returns true.
2. The rule's `apply()` calls `MvRewriter::try_rewrite(memo, expr, shape)`.
3. `MvRewriter` looks up candidate MVs via `MvCandidateRegistry` (indexed by base table FQN).
4. For each candidate: build MV's `Operator` tree (cached, re-parsed on first use), tag both query and MV trees with `MvColumnId`s seeded from base-table field IDs.
5. Run shape match + predicate decomposition + column mapping. If match fails, record rejection reason in trace, skip.
6. If match succeeds, compute `Fresh(MV, query)` and `Stale(MV, query)` via Iceberg snapshot diff against base tables.
7. Synthesize rewritten subtree (pure MV scan, or UNION ALL with stale partitions).
8. Insert as alternative `MExpr` into the same Memo group. Cost search picks.

---

## 3. MvColumnId Facility (local, will retire when G1 lands)

**Module**: `src/sql/optimizer/mv_rewrite/column_id.rs`

```rust
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MvColumnId(u32);

#[derive(Default)]
pub struct MvColumnIdFactory {
    next: u32,
    // Seed for stable assignment across query and MV sides.
    // Key = (canonical_base_table_uuid, base_field_id) for scan columns.
    // For derived columns (Project / Aggregate output), key = (parent_id, derivation_hash).
    forward: HashMap<MvColumnIdKey, MvColumnId>,
    display: HashMap<MvColumnId, String>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum MvColumnIdKey {
    /// Scan column: (Iceberg base table UUID, base field-id from MvSchemaContract).
    Base { table_uuid: String, field_id: i32 },
    /// Derived column: hash of the canonical scalar expression in terms of
    /// already-assigned MvColumnIds.
    Derived { expr_hash: u64 },
    /// Aggregate-output column: (agg-fn-name, args-as-MvColumnIds, group-key-set-hash).
    AggOutput { fn_name: String, args: Vec<MvColumnId>, group_hash: u64 },
}
```

**Identity guarantee**: because both the query's `Operator` tree and the MV's `Operator` tree are tagged from the *same* `MvColumnIdKey` rule space, identical sub-expressions on identical Iceberg fields get identical `MvColumnId`s. This is what makes query↔MV column equality decidable without ColumnRef name parsing.

**Plumbing**: the rewriter walks an `Operator` tree once with a `MvColumnIdFactory`, returning a side-table `HashMap<OperatorAddress, Vec<MvColumnId>>` (output-column IDs per op). This side-table is *not* persisted in the Memo — it's transient per rewrite attempt.

**Equivalence classes** (the local-G7): union-find over `MvColumnId`, populated from:
- Join-eq conditions: `t1.a = t2.b` → `union(MvColumnId(t1.a), MvColumnId(t2.b))`.
- Filter equality: `col = literal` → no union (literals not in union-find).
- Project rename: pass-through, no new id (Project just rewires display names).

---

## 4. MV Catalog & Candidate Lookup

**New index**: `MvBaseTableIndex`, mapping `base_table_fqn → Vec<mv_id>`.

**Where it lives**:
- `src/meta/repository/mv.rs`: extend `MvMetaRepository` with `list_mvs_by_base_table(base_fqn) -> Vec<i64>`. Backed by an inverse iteration over `list_definitions` for v1 (cached at session level); a proper inverted index can come later.
- `src/sql/optimizer/mv_rewrite/registry.rs`: `MvCandidateRegistry` wraps the repository, caches candidates per query, and gates candidates by:
  - `definition.storage_engine == "iceberg"` (Iceberg backend only)
  - `definition.refresh_in_progress == false`
  - `definition.last_refresh_snapshots` is non-empty (MV has been refreshed at least once)

**MV `Operator` tree resolution**: re-parse `definition.select_sql` through `parser → analyzer → planner → RBO`. The result is cached in `MvCandidateRegistry` (LRU, ~32 entries). Cache key includes the MV's `schema_contract` content hash so DDL invalidates.

**Why re-parse and not store**: storing a parsed plan would require versioning the optimizer IR. Re-parse on first access keeps v1 cheap and always consistent. Cost is amortized via session-level cache.

---

## 5. Shape Matching & Predicate Decomposition

### 5.1 Per-shape match preconditions

| Rule | `matches(&Operator)` | Required MV shape |
|---|---|---|
| `MvProjectionRewriteRule` | `LogicalProject` whose **only** child group has a logical expr of `LogicalScan` (optional `LogicalFilter` between) | SPJF: `Project(Filter?(Scan(T)))` |
| `MvAggregateScanRewriteRule` | `LogicalAggregate` whose only child group has `LogicalScan / LogicalProject / LogicalFilter` chain | SPJG single-base: `Agg(Project?(Filter?(Scan(T))))` |
| `MvJoinRewriteRule` | `LogicalJoin` (`Inner`) with both children resolvable to `LogicalScan` (optionally through `LogicalFilter` / `LogicalProject`) | SPJF two-base inner equi-join |
| `MvAggregateJoinRewriteRule` | `LogicalAggregate` whose child is a `LogicalJoin` matching the join-MV constraint above | SPJG two-base |

**Children resolution**: `matches()` does a shallow check only (root op kind). Deeper structural check (does the Memo group below actually contain the required child shape?) happens in `apply()` via `MvRewriter::extract_shape(memo, group_id, expected_kind)`. If extraction fails, `apply()` returns empty `Vec<NewExpr>` (no rewrite, no error).

### 5.2 Predicate decomposition

**Module**: `src/sql/optimizer/mv_rewrite/predicate_split.rs`

```rust
pub struct PredicateSplit {
    /// `col = literal` after column rewriting
    pub equality: Vec<(MvColumnId, ScalarValue)>,
    /// `col CMP literal`, `col BETWEEN`, etc. on totally-ordered types
    pub range: Vec<RangePredicate>,
    /// Everything else (CASE, function calls, OR-of-non-trivial)
    pub residual: Option<TypedExpr>,
}
```

**Containment check** (`query_split ⊆ mv_split`):
- Every MV equality `(c, v)` must appear in query equalities (query is at least as restrictive).
- Every MV range `c ∈ [a, b]` must be a superset of query's range on `c`.
- Residual: query's residual implies MV's residual. v1: require exact textual+structural equality (after canonicalization). Anything stronger is v2.

**Compensation predicate** = `query_predicates AND NOT mv_predicates`, simplified by:
- Drop conjuncts that the MV definition already enforces.
- Remaining conjuncts wrap the rewritten MV scan in an extra `LogicalFilter`.

### 5.3 Column rewriting

**Module**: `src/sql/optimizer/mv_rewrite/column_rewriter.rs`

Given:
- Query output columns: `Vec<(MvColumnId, display_name)>`.
- MV output columns: `Vec<(MvColumnId, mv_display_name)>`.

For each query output column, find an MV output column with the same `MvColumnId` (after equivalence-class normalization). If any query output column has no corresponding MV column, rewrite is rejected.

**Aggregate rollup** (within `MvAggregateScanRewriteRule` and `MvAggregateJoinRewriteRule`):
- Query: `SUM(x), COUNT(y), MIN(z), MAX(z), AVG(w)` over GROUP BY (g1, g2).
- MV: precomputed columns `sum_x, cnt_y, min_z, max_z, sum_w, cnt_w` over GROUP BY (g1, g2, g3).
- If query's GROUP BY ⊆ MV's GROUP BY: rollup-aggregate over MV by summing/min-maxing across the dropped keys.
- If query's GROUP BY ⊋ MV's GROUP BY: reject (cannot un-aggregate).
- If query GROUP BY == MV GROUP BY: trivial pass-through, just project.

---

## 6. Partition Compensation & Freshness Model

### 6.1 Computing fresh vs stale partitions

**For each base table referenced by MV**, the MV stores `last_refresh_snapshots[base_fqn] = S_mv`. Base's current snapshot is `S_now` (Iceberg catalog lookup).

**Per-base-partition freshness**:
- If `S_mv == S_now`: all partitions of this base are fresh.
- Else: use Iceberg snapshot-diff API (already used by IVM's `IcebergDeltaScan`) to enumerate the set of base partitions touched by snapshots in `(S_mv, S_now]`. Partitions outside this set are fresh; inside are stale.

**MV→base partition mapping**: `MvPartitionContract` (in `mv_contract.rs`) defines partition transforms (`MvPartitionTransformContract`). For each MV partition `p_mv`, we know which base partitions it covers. Used to translate fresh/stale base partitions into fresh/stale MV partitions.

**Query partition predicate intersection**:
- Extract partition predicate from query (e.g., `dt BETWEEN '2026-05-01' AND '2026-05-20'`).
- Enumerate base partitions matching this predicate.
- `Fresh = matching_base_partitions ∩ fresh_base_partitions`.
- `Stale = matching_base_partitions \ Fresh`.

### 6.2 Rewrite-plan synthesis based on fresh/stale split

| Case | Synthesized plan |
|---|---|
| `Stale.is_empty()` (100% fresh) | `compensating_filter(Scan(MV))` |
| `Fresh.is_empty()` (0% fresh) | **Skip rewrite** — pure MV scan would return empty, adding base scan defeats the purpose. |
| Partial freshness | `UnionAll(compensating_filter(Scan(MV)) where partition ∈ Fresh, ReExecute(MV-definition over base) where partition ∈ Stale)` |

**`ReExecute(MV-definition over base)`**: take the MV's `Operator` tree, push an additional partition filter (`partition ∈ Stale`) into each `LogicalScan` of base tables, then graft into the query plan. This is the equivalent of StarRocks's `MvPartitionCompensator.compensate(...)`.

### 6.3 Module

`src/sql/optimizer/mv_rewrite/partition_compensator.rs`:
- `compute_freshness(mv: &StoredMvDefinition, base_partition_pred: &BasePartitionPred, iceberg_catalog: &dyn IcebergCatalog) -> FreshnessSplit`
- `synthesize_union(rewritten_mv: GroupId, mv_definition_tree: &Operator, stale: &BasePartitionPred, memo: &mut Memo) -> GroupId`

Shared trait `PartitionFreshnessOracle` so IVM's existing partition-affected planner (`src/engine/mv/partition/planner.rs`) and the rewrite path can both consume the same diff result.

---

## 7. Rule Integration (CBO transformation + cost)

### 7.1 Registration

Edit `src/sql/optimizer/rules/mod.rs`:

```rust
pub(crate) fn all_transformation_rules(ctx: &MvRewriteCtx) -> Vec<Box<dyn Rule>> {
    let mut rules: Vec<Box<dyn Rule>> = vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_top_n::SplitTopN),
    ];
    if ctx.enable_mv_rewrite {
        rules.push(Box::new(mv_rewrite::rules::MvProjectionRewriteRule::new(ctx.clone())));
        rules.push(Box::new(mv_rewrite::rules::MvAggregateScanRewriteRule::new(ctx.clone())));
        rules.push(Box::new(mv_rewrite::rules::MvJoinRewriteRule::new(ctx.clone())));
        rules.push(Box::new(mv_rewrite::rules::MvAggregateJoinRewriteRule::new(ctx.clone())));
    }
    rules
}
```

The signature gains a `&MvRewriteCtx` argument that carries the `MvCandidateRegistry`, the active catalog handle, and session options. `optimize()` builds the context once per query and passes it down. Existing rules ignore it; new rules consume it.

### 7.2 The `apply()` body skeleton

```rust
fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
    let mv_rewriter = MvRewriter::new(&self.ctx);
    // 1. Extract query shape from `expr` and child groups.
    let Some(query_shape) = mv_rewriter.extract_shape(memo, expr, self.expected_kind) else {
        return vec![];
    };
    // 2. Find candidate MVs by base-table set.
    let candidates = mv_rewriter.find_candidates(&query_shape);
    // 3. Try each candidate, record trace.
    let mut out = vec![];
    for cand in candidates {
        match mv_rewriter.try_rewrite(memo, &query_shape, &cand) {
            Ok(Some(new_root_group)) => {
                out.push(NewExpr {
                    op: Operator::LogicalProject(/* identity project to match output schema */),
                    children: vec![new_root_group],
                });
                self.trace_success(cand.mv_id);
            }
            Ok(None) => self.trace_skip(cand.mv_id, "no benefit"),
            Err(reason) => self.trace_reject(cand.mv_id, reason),
        }
    }
    out
}
```

### 7.3 Cost integration

No new cost model. Use existing `derive_group_statistics` plus:
- `Scan(MV)` row count = `definition.last_refresh_rows`.
- `Scan(base)` row count = existing statistics path.
- `UnionAll` cost = sum of children + small materialization overhead (existing `UnionToPhysical` already handles this).
- `Filter` (compensation) selectivity = standard estimate from `stats::estimate_selectivity`.

The existing top-down cost search picks the cheaper. Empirically, MV-scan is much smaller than base-scan, so when MV is fully fresh it wins. When MV is fully stale, only the base plan exists (we skipped the rewrite). When partial, cost decides.

---

## 8. Observability & Configuration

### 8.1 Session variables

Add in `src/sql/optimizer/options.rs`:
- `enable_materialized_view_rewrite` (default `true`). Global kill-switch — when false, MV rules are not even registered.
- `enable_materialized_view_union_rewrite` (default `true`). When false, only fully-fresh MVs match; partial freshness is treated as a rejection.

Individual rule names also participate in the standard `disable_optimizer_rules` set (already supported by `is_known_rule_name` / `OptimizerOptions::is_enabled`):
- `MvProjectionRewrite`
- `MvAggregateScanRewrite`
- `MvJoinRewrite`
- `MvAggregateJoinRewrite`

### 8.2 EXPLAIN integration

Build on PR #147's optimizer observability (OPT-5). Add:
- `EXPLAIN VERBOSE`: per-`Scan(MV)` node print `mv=<name> fresh_partitions=N stale_partitions=M`.
- `EXPLAIN ANALYZE` query-header lines: `MV Rewrite: <mv_name> (fresh: N/M)` or `MV Rewrite: skipped` with one-line reason.
- Rewrite-trace dump (gated by `SET debug_print_mv_rewrite=true`): per-candidate attempt, accepted/rejected, reason. Written to the same trace channel as PR #147.

### 8.3 SQL-test directives

Reuse existing directives:
- `-- @explain_contains=mv1` to assert which MV was chosen.
- `-- @explain_contains=UNION ALL` to assert UNION compensation fired.
- `-- @normalize_explain_timing` to keep goldens stable.

---

## 9. File-Level Change Plan

New module: `src/sql/optimizer/mv_rewrite/`
- `mod.rs` — public entry + `MvRewriteCtx`
- `column_id.rs` — `MvColumnId`, factory, equivalence union-find
- `registry.rs` — `MvCandidateRegistry`, MV definition cache, base-table index
- `predicate_split.rs` — `PredicateSplit`, containment check, compensation derivation
- `column_rewriter.rs` — query↔MV column mapping + aggregate rollup helpers
- `partition_compensator.rs` — freshness oracle, UNION synthesis
- `shape.rs` — `QueryShape` / `MvShape` extraction from Memo
- `rewriter.rs` — `MvRewriter::try_rewrite` orchestrator
- `rules/mod.rs`, `rules/projection.rs`, `rules/aggregate_scan.rs`, `rules/join.rs`, `rules/aggregate_join.rs`
- `trace.rs` — observability trace records

Touched files:
- `src/sql/optimizer/rules/mod.rs` — register new rules behind ctx flag.
- `src/sql/optimizer/mod.rs` — build `MvRewriteCtx` once per query.
- `src/sql/optimizer/options.rs` — new session vars.
- `src/sql/optimizer/rule.rs` — possibly extend `Rule` to allow rules carrying state (or just keep state in rule struct).
- `src/sql/optimizer/stats.rs` — handle `LogicalUnion` over fresh-MV + stale-base statistics (already does basic union).
- `src/sql/explain.rs` — EXPLAIN VERBOSE/ANALYZE additions per §8.2.
- `src/meta/repository/mv.rs` — `list_mvs_by_base_table` helper.
- `src/sql/optimizer/options.rs` — register rule names in `is_known_rule_name`.

Tests:
- `sql-tests/optimizer/mv_rewrite_*.sql` — plan-shape goldens (one per shape, plus union-compensation case).
- `sql-tests/mv-on-iceberg/rewrite/*.sql` — end-to-end correctness (create base + MV, refresh partial, query, assert result equals base-only query).

---

## 10. Commit Plan (single PR on branch `claude/stupefied-turing-c93a76`)

| # | Commit | What lands |
|---|---|---|
| 1 | `feat(mv-rewrite): scaffold module + MvRewriteCtx + session vars` | Empty `mv_rewrite/` module; `enable_materialized_view_rewrite` session var; `MvRewriteCtx` plumbed into optimizer (but no rules registered yet). Build green. |
| 2 | `feat(mv-rewrite): MvColumnId facility + equivalence union-find` | `column_id.rs` with unit tests. |
| 3 | `feat(mv-rewrite): MvCandidateRegistry + base-table lookup + MV plan cache` | `registry.rs`, `list_mvs_by_base_table` in meta repo, unit tests against a stub repo. |
| 4 | `feat(mv-rewrite): PredicateSplit + containment + compensation derivation` | `predicate_split.rs` with unit tests. |
| 5 | `feat(mv-rewrite): ColumnRewriter + aggregate-rollup helpers` | `column_rewriter.rs`, unit tests. |
| 6 | `feat(mv-rewrite): shape extraction + MvRewriter orchestrator skeleton` | `shape.rs`, `rewriter.rs` (still no rule registered). |
| 7 | `feat(mv-rewrite): MvProjectionRewriteRule` | First rule wired into `all_transformation_rules`. Plan-shape goldens for single-table SPJF. End-to-end correctness for non-partitioned case. |
| 8 | `feat(mv-rewrite): PartitionCompensator + UNION transparent synthesis` | `partition_compensator.rs`, freshness oracle, UNION ALL synthesis for projection shape. Partial-freshness end-to-end test. |
| 9 | `feat(mv-rewrite): MvAggregateScanRewriteRule` | Aggregate-shape rule + plan-shape goldens + e2e (full + partial freshness). |
| 10 | `feat(mv-rewrite): MvJoinRewriteRule` | Join-shape rule + plan-shape goldens + e2e. |
| 11 | `feat(mv-rewrite): MvAggregateJoinRewriteRule` | Join-aggregate rule + plan-shape goldens + e2e. |
| 12 | `feat(mv-rewrite): EXPLAIN VERBOSE / ANALYZE + trace channel` | Observability + tests asserting EXPLAIN output. |
| 13 | `test(mv-rewrite): full SQL suite + memory snapshot` | Final regression sweep across `iceberg`, `iceberg-ivm`, `mv-on-iceberg` suites; memory progress note. |

Each commit must:
- Build (`cargo build`) clean.
- Existing `cargo test` regression-clean (no pre-existing test failure introduced).
- For commits ≥7, run the relevant new `sql-tests/optimizer/mv_rewrite_*` and `sql-tests/mv-on-iceberg/rewrite/*` suites in `verify` mode and pass.

---

## 11. Testing Strategy

### 11.1 Layer 1 — unit tests (per commit)

- `column_id.rs`: same Iceberg field → same `MvColumnId` from both sides; equivalence union-find correctness.
- `predicate_split.rs`: containment matrix (eq-eq, eq-range, range-range, residual exact).
- `column_rewriter.rs`: rollup-mapping correctness for SUM/COUNT/MIN/MAX/AVG.
- `partition_compensator.rs`: with a stub `PartitionFreshnessOracle`, assert correct fresh/stale split for hand-crafted snapshot diffs.

### 11.2 Layer 2 — plan-shape goldens (`sql-tests/optimizer/`)

Naming: `mv_rewrite_<shape>_<scenario>.sql`. Examples:
- `mv_rewrite_projection_full_fresh.sql` — assert `Scan: mv_proj_v1` appears.
- `mv_rewrite_projection_partial_fresh.sql` — assert `UNION ALL` appears with `mv_proj_v1` on fresh side.
- `mv_rewrite_aggregate_rollup.sql` — query has fewer GROUP BY keys than MV; assert MV scan + outer SUM.
- `mv_rewrite_join_inner.sql` — query is `t1 JOIN t2`; MV is also `t1 JOIN t2`; assert MV scan.
- `mv_rewrite_aggregate_join.sql` — query is `SELECT t1.k, SUM(t2.v) FROM t1 JOIN t2 GROUP BY t1.k`; MV is identical.
- `mv_rewrite_reject_<reason>.sql` — assert original plan (no MV) when rewrite must reject.

Use `-- @explain_contains` and `-- @normalize_explain_timing` for stable matching.

### 11.3 Layer 3 — end-to-end correctness (`sql-tests/mv-on-iceberg/rewrite/`)

Each test:
1. `CREATE` base table (Iceberg v3, row-lineage enabled).
2. `INSERT` initial data.
3. `CREATE MATERIALIZED VIEW`.
4. `REFRESH MATERIALIZED VIEW` (full).
5. Run query against base table → store golden result.
6. Run identical query (which should now route through MV) → assert result == golden.
7. `INSERT` more data into base (creates stale partitions).
8. Run query again → assert result *still* == golden + new data (correctness of UNION compensation).
9. `REFRESH MATERIALIZED VIEW` (incremental).
10. Run query again → assert result matches.

One e2e fixture per shape, plus one "stress" fixture that mixes shapes.

### 11.4 Layer 4 — full SQL suite regression

In commit #13, run:
- `iceberg`
- `iceberg-ivm`
- `iceberg-rest`
- `mv-on-iceberg`
- `tpc-h`
- `tpc-ds`

All suites must pass at parity with `main`. Any new failure must be investigated, not waived.

---

## 12. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| **MvColumnId identity drift** between query side and MV side when an MV's SELECT contains a derived expression that the query writes differently (`a+b` vs `b+a`). | Canonicalize scalar expressions (sort commutative operators, fold constants) before hashing for `MvColumnIdKey::Derived`. |
| **MV SELECT re-parse semantic drift** if base table schema evolved since MV was registered. | Use `MvSchemaContract.base.base_field_records` as the source of truth for field IDs. If a base field referenced by the contract no longer exists, reject the MV candidate (registry filter). |
| **Partial-freshness UNION cost may exceed base-scan**. | Heuristic gate: when `fresh_partitions / total_partitions < 0.2`, skip the rewrite attempt (don't even insert as alternative). Above 0.2, let cost search decide. Threshold exposed as `mv_rewrite_min_fresh_ratio` session var; default 0.2. |
| **Cascades framework lacks Pattern matcher (ARCH G5)**. Each rule manually traverses Memo to extract child shape. | Implement `mv_rewrite::shape::extract_*` helpers as a small reusable subset of Pattern matching, used only by MV rules. When G5 lands, replace with real Pattern. |
| **Rule signature change** (adding `&MvRewriteCtx`) is a breaking change to `Rule` trait. | Two options: (a) extend the trait with a default no-op accessor, (b) keep `Rule` unchanged and stash the ctx inside the rule struct at construction time via `Arc<MvRewriteCtx>`. **v1 picks (b)** — no trait change. |
| **Memo group explosion** if many MV candidates fire on one group. | Cap per-group MV-rewrite alternatives at 3 (chosen by base-table-set narrowest first). Configurable via `mv_rewrite_max_candidates_per_group` (default 3). |
| **Tests interfere via shared catalog state**. | Use existing `iceberg-rest` Docker fixture; each test uses a unique namespace. Tear down at suite end. |
| **A concurrent branch (`claude/blissful-wing-22e8af`) is doing G1 ColumnId work**. | v1 is isolated under `mv_rewrite/` and uses `MvColumnId` only internally. When the other branch lands, a single follow-up PR replaces `MvColumnId` with the global ColumnId (mechanical). |

---

## 13. Open Questions (resolved during execution by reference to StarRocks)

When ambiguity arises during implementation, the resolution rule is: **`~/project/starrocks` is the authoritative reference** for behavior. Specifically:
- Predicate containment semantics: follow `PredicateSplit` and `MvUtils.predicateInPullup`.
- Column-rewriting edge cases: follow `ColumnRewriter`.
- Aggregate rollup table: follow the `RewriteEquivalent` subclasses for each agg function.
- UNION compensation shape: follow `MvPartitionCompensator.compensate` (skip view-delta and nested branches).

If a question is *not* answered by reading StarRocks code, record it in a `TODO(mv-rewrite-v2)` comment with enough context for a follow-up.

---

## 14. Out-of-Branch Coordination

This work is fully isolated under `src/sql/optimizer/mv_rewrite/` and touches `rules/mod.rs`, `mod.rs`, `options.rs`, `explain.rs`, and `meta/repository/mv.rs`. The concurrent branch `claude/blissful-wing-22e8af` is reportedly working on plan-capability gaps (likely G2/G3 from the ARCH doc). No file conflict expected. When that branch lands, this branch rebases and re-runs the regression suite.

If a merge conflict appears in `rules/mod.rs`, resolution is straightforward (both branches just add to `all_transformation_rules`). If a conflict appears in operator definitions or cascades search, prefer the other branch's structural change and adapt MV rewrite to it (this branch's `MvColumnId` is the local outlier).

---

## Appendix A — StarRocks ↔ NovaRocks class mapping (for porting reference)

| StarRocks | NovaRocks (v1) |
|---|---|
| `BaseMaterializedViewRewriteRule` | `MvRewriter` (shared) + 4 `Mv*RewriteRule` (per-shape) |
| `OnlyScanRule` | `MvProjectionRewriteRule` |
| `OnlyJoinRule` | `MvJoinRewriteRule` |
| `AggregateScanRule` | `MvAggregateScanRewriteRule` |
| `AggregateJoinRule` | `MvAggregateJoinRewriteRule` |
| `EquationRewriter` | merged into `ColumnRewriter` + `column_id::MvColumnId` |
| `ColumnRewriter` | `ColumnRewriter` |
| `PredicateSplit` | `PredicateSplit` |
| `PredicateExtractor` | folded into `PredicateSplit` |
| `MvPartitionCompensator` | `PartitionCompensator` |
| `RewriteContext` | `MvRewriteCtx` + transient `RewriteAttempt` |
| `MvUtils` | utilities are spread; no single class |
| `RewriteEquivalent` (agg rollup) | helpers in `column_rewriter.rs` |
| `MaterializedView.partitionIds` | computed via `PartitionFreshnessOracle` |
| `enable_materialized_view_rewrite` | same name, in `options.rs` |
| `enable_materialized_view_union_rewrite` | same name |
| `enable_materialized_view_view_delta_rewrite` | NOT IMPLEMENTED (v2) |
| `enable_materialized_view_text_match_rewrite` | NOT IMPLEMENTED (v2) |

---

## Appendix B — Worked Example (Projection MV)

**Schema**:
```sql
CREATE TABLE sales (
    order_id  BIGINT,
    region    STRING,
    amount    DECIMAL(18,2),
    sold_at   DATE
) PARTITION BY days(sold_at);

CREATE MATERIALIZED VIEW mv_west_sales AS
SELECT order_id, amount, sold_at
FROM sales
WHERE region = 'west';
```

**Query**:
```sql
SELECT order_id, amount FROM sales
WHERE region = 'west' AND amount > 100 AND sold_at = '2026-05-15';
```

**Rewrite trace**:
1. `MvProjectionRewriteRule.matches(LogicalProject)` → true.
2. `extract_shape` returns `QueryShape { base: sales, filter: [region='west', amount>100, sold_at='2026-05-15'], project: [order_id, amount] }`.
3. `find_candidates(base=sales)` returns `[mv_west_sales]`.
4. `column_rewriter`: query columns `(order_id, amount)` map to MV's `(order_id, amount)` via `MvColumnId`. OK.
5. `predicate_split`:
   - Query: eq=`{region='west', sold_at='2026-05-15'}`, range=`{amount>100}`, residual=∅.
   - MV: eq=`{region='west'}`, range=∅, residual=∅.
   - Containment: ✓. Compensation = `amount>100 AND sold_at='2026-05-15'`.
6. `partition_compensator.compute_freshness`:
   - MV's `last_refresh_snapshots[sales] = S_mv`. Now `S_now`.
   - Snapshot-diff: partitions `[2026-05-15, 2026-05-16, 2026-05-20]` changed.
   - Query's partition predicate `sold_at='2026-05-15'` → single partition `2026-05-15`.
   - `Stale = {2026-05-15}`, `Fresh = {}`. **0% fresh** → skip rewrite (per Risk #3 / §6.2).
7. After next refresh: `last_refresh_snapshots[sales] = S_now`. Re-run query → 100% fresh → pure `Scan(mv_west_sales) WHERE amount>100 AND sold_at='2026-05-15'`.

**Cost outcome**: MV scan touches one partition file, base scan would touch one partition file too — both equally cheap. Cost search picks MV because of lower row count after MV's `region='west'` selectivity reduction.
