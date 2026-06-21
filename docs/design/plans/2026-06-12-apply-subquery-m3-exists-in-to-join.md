# Apply / CorrelatedSubquery M3 — EXISTS / IN to-join migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement M3 of the Apply framework (design: `docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md`, §6.1 rules 10/11, §6.2, §7.2/§7.3): route WHERE-clause top-level-AND `EXISTS` / `NOT EXISTS` / `IN` / `NOT IN` subqueries through `LogicalApply`, and add two `SubqueryRewrite`-stage rules — `ExistentialApplyToJoin` (→ `LeftSemi` / `LeftAnti`) and `QuantifiedApplyToJoin` (→ `LeftSemi` / `NullAwareLeftAnti` | `LeftAnti`) — that decorrelate them into semi/anti joins whose results are identical to the legacy analyzer rewrite.

**Architecture:** Additive, mirroring how M1 added the scalar path. The analyzer gains a new `ApplyPredicateSpec` (parallel to the shipped, untouched `ApplyScalarSpec`) carrying the analyzed inner query, correlation column ids, the IN LHS, and `use_semi_anti`; a new `collect_predicate_apply_spec` records it and removes the placeholder conjunct from WHERE (mirroring legacy semi/anti collapse); the routing gate in `rewrite_subqueries` widens to EXISTS/IN. The planner gains `wrap_predicate_applies` constructing `ApplyKind::{Exists,In}` nodes. Two new **self-contained** rewrite rules read the inner subquery's `Filter` directly (via `decorrelate_util::partition_conjuncts`) and emit the semi/anti `Join`; they do NOT depend on `PushDownApplyFilter` (which stays scalar-only and untouched). On any unsupported shape the analyzer falls back to legacy (apply mode) or errors (apply_strict).

**Tech Stack:** Rust; the existing rewrite framework (`LogicalRewriteRule` / `RewritePipeline` / `RewriteContext`); `decorrelate_util` (`partition_conjuncts` / `orient_eq` / `all_binary_eq`); `rules::utils` (`split_and` / `combine_and` / `collect_column_id_refs`); the M1 analyzer helpers (`analyze_query_in_scope_with_inner`, `collect_correlation_column_ids`, `is_placeholder_inside_or`, `replace_placeholder_in_*`); the planner `plan_scoped_query` / `plan_output_columns`; the M1 engine-level test harness in `src/engine/mod.rs`.

**Key constraints:**

1. **Opt-in, gated. Default mode stays `Legacy`.** The framework is opt-in today (`#[default] Legacy`, `src/sql/optimizer/options.rs:14`); M1/M2 shipped opt-in and #297 did NOT flip the default. M3 keeps that: no default flip, no legacy-code deletion. M3 adds the apply path for EXISTS/IN plus `apply_strict` CI that proves apply == legacy. Every task keeps the branch baseline `cargo test --lib` at **0 failed**, because in `Legacy` mode (the default for the whole existing suite) none of the new code is reached.
2. **Faithful to legacy, no coverage-chasing.** The to-join output must be result-identical to the legacy rewrite. When a shape is not covered (HAVING/JOIN-ON position, OR/projection value-form, multi-column IN, non-EQ-only where the rule can't build a join), the analyzer returns `Ok(false)` → legacy fallback (apply mode) or `Err` → fatal (apply_strict). Never emit a wrong plan to widen coverage.
3. **Two correctness invariants (replicate legacy exactly):**
   - **NOT IN null-aware join-kind:** `negated && (lhs.nullable || inner_col.nullable)` → `JoinKind::NullAwareLeftAnti`; both statically non-nullable → downgrade to plain `JoinKind::LeftAnti`. Reproduce for both correlated and uncorrelated. For correlated NOT IN, the lifted inner WHERE that goes into the ON is wrapped `coalesce(<pred>, false)` when it is nullable (legacy `subquery_rewrite.rs:1594-1615`).
   - **Bare `Eq` join key:** the IN key (`lhs = inner_col`) is always a bare `Eq` (AND-chained never needed for single-column M3); null-aware semantics live ENTIRELY in the `JoinKind`, never in an `IS NULL OR` wrapper. (Legacy lesson `subquery_rewrite.rs:1339-1346`: IS-NULL-OR wrapping degraded `NOT IN` to a NestLoopJoin that timed out at 60K×40K scale.)
4. **Self-contained rules.** `ExistentialApplyToJoin` / `QuantifiedApplyToJoin` read the inner subquery's `Filter` themselves; `PushDownApplyFilter` / `PushDownApplyAggFilter` are NOT generalized (they stay `kind == Scalar` only). This keeps the shipped scalar path byte-for-byte unchanged and gives the new rules precise control over the ON condition (needed for the NAAJ `coalesce` detail).
5. **WHERE top-level AND only.** M3 routes EXISTS/IN to apply only when the placeholder is a top-level AND conjunct of the WHERE `Filter` (`use_semi_anti == true`). HAVING, JOIN-ON, OR/projection (value-form), and multi-column IN stay legacy (M4).
6. **English** code/comments/errors; commit message bodies in English, matching the landed #294/#297 series style. Work on the current worktree branch (`claude/optimistic-shirley-24796b`) or a dedicated `claude/apply-subquery-m3-exists-in` branch.

---

## Design-doc reconciliation (read before coding — checked against the live code)

1. **M3 is emission + rules, not just rules.** M1 only built the *scalar* emission path. EXISTS/IN have NO analyzer spec, NO `use_semi_anti` computation, and the planner hard-codes `ApplyKind::Scalar` (`wrap_scalar_applies`, `src/sql/planner/mod.rs:723`). So M3a (emission) must land before M3b (rules) can fire. This plan does M3a (Tasks 1–3) then M3b (Tasks 4–6) then verification (Tasks 7–9).
2. **`use_semi_anti` is computed post-hoc, not at emission time.** `analyze_expr` (`src/sql/analyzer/resolve_expr.rs:17`) has no clause/position context. Like legacy, M3 reconstructs "top-level AND conjunct of WHERE" *after* analysis by walking `select.filter` with `is_placeholder_inside_or` (`src/sql/analyzer/subquery_rewrite.rs:3791`). The design §5.3's emission-time `use_semi_anti` tracking and the `SubqueryInfo { resolved, use_semi_anti, in_exprs, ... }` upgrade do NOT exist and are NOT built here.
3. **`PushDownApplyFilter` stays scalar-only (design-intended).** `partition_conjuncts` / `all_binary_eq` already live in `decorrelate_util` unused — the agent confirmed they were placed there "precisely for the Existential/Quantified rules." M3's rules call them directly; `PushDownApplyFilter` is untouched.
4. **Parallel spec, not a rename.** Add `ApplyPredicateSpec` + `ResolvedSelect.predicate_apply_specs` + `wrap_predicate_applies`. The shipped `ApplyScalarSpec` / `apply_specs` / `wrap_scalar_applies` / `collect_scalar_apply_spec` are NOT modified. (Lowest regression risk to the M1/M2 scalar path.)
5. **`subquery_expr` carries the IN LHS.** `ApplyNode.subquery_expr` is documented as "`lhs IN (inner_col)` / `EXISTS(inner_col)` / bare `ColumnRef`"; in practice the scalar to-join rule never reads it. For M3: `In` stores the analyzed single-column LHS expression in `subquery_expr`; `QuantifiedApplyToJoin` builds the bare `Eq` IN key as `subquery_expr = ColumnRef(inner_output_column_id)`. `Exists` stores a vestigial `ColumnRef(output_column)` (boolean) — `ExistentialApplyToJoin` uses only `kind` + the inner Filter.
6. **Uncorrelated EXISTS → SEMI/ANTI JOIN ON `true`.** Cleaner than legacy's `LEFT OUTER JOIN ON true + SELECT 1 LIMIT 1 + IS [NOT] NULL` indicator shape, and result-identical (the Task 7 parity test is the bar). Plan-golden asserts the SEMI/ANTI shape.
7. **EXPLAIN strings** (`src/sql/explain.rs:204-208`): `LEFT SEMI JOIN`, `LEFT ANTI JOIN`, `NULL AWARE LEFT ANTI JOIN`. The logical APPLY formatter renders `APPLY ({kind}, correlated={}, use_semi_anti={})` (`explain.rs:363`).
8. **`SubqueryKind` (analyzer) vs `ApplyKind` (planner) spelling differ:** analyzer is `InSubquery { negated }`; planner is `In { negated }`. `wrap_predicate_applies` maps between them.

---

## Construction toolbox (verbatim from the current code — use these exactly)

**`ApplyKind` / `ApplyNode`** (`src/sql/planner/plan.rs:111-166`) — variants already exist (currently `#[allow(dead_code)]`; drop the allow once M3 reads them):
```rust
pub(crate) enum ApplyKind { Scalar, Exists { negated: bool }, In { negated: bool } }

pub(crate) struct ApplyNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,           // subquery plan; may reference correlation_column_ids
    pub kind: ApplyKind,
    pub subquery_expr: TypedExpr,          // M3 In: the LHS expr; M3 Exists: ColumnRef(output_column)
    pub output_column: OutputColumn,       // M3 EXISTS/IN: a fresh Boolean indicator (removed from filter)
    pub inner_output_column_id: ColumnId,  // inner subquery's first output column (the IN RHS)
    pub correlation_column_ids: Vec<ColumnId>,
    pub correlation_conjuncts: Vec<TypedExpr>,   // STAYS EMPTY for EXISTS/IN (rules read the inner Filter)
    pub residual_predicate: Option<TypedExpr>,   // None for EXISTS/IN
    pub need_check_max_rows: bool,               // false for EXISTS/IN
    pub use_semi_anti: bool,                     // true for M3 (top-level AND of WHERE)
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,  // empty for EXISTS/IN
    pub required_output_columns: Option<HashSet<ColumnId>>,
}
```

**`JoinNode`** (`src/sql/planner/plan.rs:415-424`) and **`JoinKind`** (`src/sql/analysis/mod.rs:235-256`):
```rust
pub(crate) struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,   // None only for CROSS
    pub required_output_columns: Option<HashSet<ColumnId>>,
}
// JoinKind variants M3 emits: LeftSemi, LeftAnti, NullAwareLeftAnti
```
`plan_output_columns` and `join_output_columns` already map `LeftSemi | LeftAnti | NullAwareLeftAnti => left` (`src/sql/planner/mod.rs:647-676`), so the join's output schema = the outer (left) columns — exactly what the parent expects after the placeholder conjunct was removed.

**`SubqueryKind`** (`src/sql/analysis/mod.rs:417-427`) and **`SubqueryInfo`** (`:477-488`):
```rust
pub(crate) enum SubqueryKind { Scalar, Exists { negated: bool }, InSubquery { negated: bool } }
pub(crate) struct SubqueryInfo {
    pub id: usize,
    pub kind: SubqueryKind,
    pub subquery: Box<sqlparser::ast::Query>,
    pub data_type: DataType,                       // Boolean for EXISTS/IN
    pub in_expr: Option<Box<sqlparser::ast::Expr>>,// raw AST LHS for IN; None for EXISTS/Scalar
}
```

**`ApplyScalarSpec` (the model to parallel) + `ApplyClause`** (`src/sql/analysis/mod.rs:429-475`): scalar-only struct with `{ subquery_id, clause, output_column, inner: ResolvedQuery, correlation_column_ids, need_check_max_rows, subquery_text }`; `ApplyClause = Where | Having | Projection`.

**`ResolvedSelect.apply_specs`** (`src/sql/analysis/mod.rs:58-80`): `pub apply_specs: Vec<ApplyScalarSpec>` (consumed by the planner). M3 adds a sibling `predicate_apply_specs`.

**Routing site** (`src/sql/analyzer/subquery_rewrite.rs:74-100`) — the gate M3 widens:
```rust
let route_to_apply = !matches!(mode, SubqueryUnnestMode::Legacy)
    && matches!(sq_info.kind, SubqueryKind::Scalar);
if route_to_apply {
    match self.collect_scalar_apply_spec(select, scope, &sq_info) {
        Ok(true) => continue,
        Ok(false) => { /* fall through to legacy */ }
        Err(e) => { if ApplyStrict { return Err(e) } /* else legacy */ }
    }
}
// legacy path...
```

**`collect_scalar_apply_spec` (the model to parallel)** (`src/sql/analyzer/subquery_rewrite.rs:151-227`): analyzes the inner via `analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?` → `(resolved_sub, inner_scope)`; collects corr ids via `collect_correlation_column_ids(&resolved_sub, &inner_scope, scope)`; mints the output column via `self.alloc_column_id(None, name, dtype, /*nullable*/ true)`; replaces the placeholder via `Self::replace_placeholder_in_filter` / `_in_projection`; pushes the spec.

**Placeholder removal vs replacement.** Scalar *replaces* the placeholder with a `ColumnRef`. EXISTS/IN at `use_semi_anti` *remove* the whole conjunct (semantics carried by the join). The legacy removal helper:
```rust
// src/sql/analyzer/subquery_rewrite.rs — used by rewrite_exists at :1081
Self::remove_placeholder_from_filter(&mut select.filter, sq_info.id);
```
(Confirm the exact name/signature in `subquery_rewrite.rs` before use; legacy `rewrite_exists` calls it for `filter` and `having`.)

**`is_placeholder_inside_or` + `has_placeholder`** (`src/sql/analyzer/subquery_rewrite.rs:3791-3820`): returns true iff the placeholder sits under an `Or` (recursing through `And`/`Nested`). `use_semi_anti` for M3 = placeholder is in `select.filter` AND `!is_placeholder_inside_or(filter, id)`.

**`analyze_query_in_scope_with_inner`** (`src/sql/analyzer/subquery_rewrite.rs:1816`): `fn(&self, &sqlparser::ast::Query, &AnalyzerScope) -> Result<(ResolvedQuery, AnalyzerScope), String>` — merged-scope analysis; outer refs inside the subquery resolve to the OUTER `ColumnId`. Reuse verbatim.

**`collect_correlation_column_ids`** (`src/sql/analyzer/subquery_rewrite.rs:3107`): `fn(&ResolvedQuery, &AnalyzerScope /*inner*/, &AnalyzerScope /*outer*/) -> Vec<ColumnId>`. Runs `extract_correlation_predicates` over the inner WHERE and collects outer-side column ids. Reuse verbatim. NOTE the D7 gap: it only finds EQ-comparison correlations; broad outer refs are detected separately by `expr_references_outer_scope` (`subquery_rewrite.rs:2633`).

**`decorrelate_util`** (`src/sql/optimizer/rewrite/rules/subquery/decorrelate_util.rs`, all `pub(super)`):
```rust
fn partition_conjuncts(predicate: TypedExpr, corr_ids: &HashSet<ColumnId>) -> (Vec<TypedExpr>, Vec<TypedExpr>) // (correlated, residual)
fn all_binary_eq(conjuncts: &[TypedExpr]) -> bool
fn orient_eq<'a>(conjunct: &'a TypedExpr, corr_ids: &HashSet<ColumnId>) -> Option<(&'a TypedExpr /*outer*/, &'a TypedExpr /*inner*/)>
```

**`rules::utils`** (`src/sql/optimizer/rewrite/rules/utils.rs`):
```rust
pub(crate) fn split_and(expr: TypedExpr) -> Vec<TypedExpr>
pub(crate) fn combine_and(exprs: Vec<TypedExpr>) -> TypedExpr   // PANICS if empty — guard len>0
pub(crate) fn collect_column_id_refs(expr: &TypedExpr) -> HashSet<ColumnId>
```

**`ScalarApplyToJoin` rule skeleton (the structural model)** (`src/sql/optimizer/rewrite/rules/subquery/scalar_apply_to_join.rs:35-127`):
```rust
pub(crate) struct ScalarApplyToJoin;
impl LogicalRewriteRule for ScalarApplyToJoin {
    fn name(&self) -> &'static str { "ScalarApplyToJoin" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(a) if a.kind == ApplyKind::Scalar)
    }
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else { return Ok(RewriteResult::Unchanged) };
        // uncorrelated arm → CROSS JOIN; correlated arm → LEFT OUTER JOIN ...
        let join = LogicalPlan::Join(JoinNode {
            left: a.left, right: a.right,
            join_type: crate::sql::analysis::JoinKind::LeftOuter,
            condition: Some(combine_and(a.correlation_conjuncts.clone())),
            required_output_columns: None,
        });
        Ok(RewriteResult::Changed(/* Project over join */))
    }
}
```

**Rule registration** (`src/sql/optimizer/rewrite/rules/subquery/mod.rs:7-32`): add two `mod` lines + two `pub(crate) use` re-exports, and insert the two rules in `subquery_rewrite_rules()` AFTER `ScalarApplyToJoin`, BEFORE `ApplyException` (which "must stay LAST").

**Test-context factory helper** (`scalar_apply_to_join.rs:764-768`):
```rust
fn ctx_with_factory() -> RewriteContext {
    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
    ctx
}
```

**Engine-level test harness** (`src/engine/mod.rs`, M1 tests at `:9157-9693`):
```rust
fn open_scalar_subquery_test_engine(warehouse: &TempDir) -> (StandaloneNovaRocks, StandaloneSession) // creates `ice` memory catalog + db `db1`
fn run_scalar_query_i64(session, sql, mode: SubqueryUnnestMode) -> Result<Vec<Option<i64>>, String>   // wraps with_session_optimizer_settings { subquery_unnest_mode: mode }
fn expect_apply_error(session, sql, needle: &str)                                                     // pins Apply mode, asserts err contains needle
// DDL/DML: session.execute_in_database(sql, "default"); query: session.execute_in_context(sql, Some("ice"), "db1", None)
```

**sql-test anchors** (parity/regression — must not change under apply_strict):
- EXISTS: `sql-tests/join/sql/join_exists_subquery_semantics.sql`, `join_not_exists_subquery_semantics.sql`
- IN/NOT IN: `sql-tests/join/sql/join_not_in_without_null.sql`, `join_not_in_with_null.sql`, `join_not_in_correlated_conjunct_null_aware.sql`
- NAAJ: `sql-tests/join/sql/join_null_aware_anti.sql`
- multi-subquery: `sql-tests/filter/sql/filter_multiple_subqueries.sql`
- plan-golden model: `sql-tests/optimizer/sql/subquery_scalar_to_window.sql` (the `SET subquery_unnest_mode='apply'` + `@explain_contains` + `SET disable_optimizer_rules=...` + fallback pattern)

---

## M3a — EXISTS / IN emission

### Task 1: `ApplyPredicateSpec` + `ResolvedSelect.predicate_apply_specs` (pure addition, no behavior change)

**Files:**
- Modify: `src/sql/analysis/mod.rs` (add the struct + the field; init the field everywhere `ResolvedSelect` is built)
- Modify: `src/sql/analyzer/mod.rs:578` and `src/sql/analyzer/subquery_rewrite.rs:2070` (init the new field empty — the two `ResolvedSelect { .. }` construction sites)

This is scaffolding: a new spec type and a sibling vec. Nothing reads them yet, so behavior is unchanged. The shipped `ApplyScalarSpec` / `apply_specs` are untouched.

- [ ] **Step 1: Add `ApplyPredicateSpec`** below `ApplyScalarSpec` in `src/sql/analysis/mod.rs`:

```rust
/// An EXISTS / NOT EXISTS / IN / NOT IN subquery routed to the Apply framework
/// (apply mode). Parallel to `ApplyScalarSpec`; the planner consumes these to
/// emit `LogicalPlan::Apply` with `ApplyKind::Exists` / `ApplyKind::In`. The
/// inner query is left INTACT — its WHERE (correlation + residual) is read by
/// the M3 to-join rules (`ExistentialApplyToJoin` / `QuantifiedApplyToJoin`).
#[derive(Clone, Debug)]
pub(crate) struct ApplyPredicateSpec {
    /// Placeholder id this spec replaced (matches the original SubqueryInfo.id).
    pub subquery_id: usize,
    /// EXISTS{negated} or InSubquery{negated}. Maps to the planner ApplyKind.
    pub kind: SubqueryKind,
    /// Which clause the placeholder lived in. M3 only records `Where`.
    pub clause: ApplyClause,
    /// Fresh Boolean indicator column for the subquery in the Apply schema.
    /// Removed from the outer filter (semantics carried by the semi/anti join),
    /// so it is never referenced; it disappears when the join replaces the Apply.
    pub output_column: OutputColumn,
    /// Fully-analyzed inner subquery (outer refs carry outer column ids).
    pub inner: ResolvedQuery,
    /// Outer columns referenced inside the subquery (the correlation keys).
    pub correlation_column_ids: Vec<ColumnId>,
    /// For IN/NOT IN: the analyzed single-column LHS. None for EXISTS.
    pub in_lhs: Option<TypedExpr>,
    /// True iff the subquery is a top-level AND conjunct of WHERE (always true
    /// for an M3-recorded spec; carried for the planner and EXPLAIN parity).
    pub use_semi_anti: bool,
    /// Original subquery SQL text (diagnostics).
    pub subquery_text: String,
}
```

- [ ] **Step 2: Add the field** to `ResolvedSelect` (next to `apply_specs`):

```rust
    /// EXISTS/IN subqueries routed to the Apply framework (apply mode only;
    /// always empty in legacy mode). Consumed by the planner alongside
    /// `apply_specs` to emit `LogicalPlan::Apply`.
    pub predicate_apply_specs: Vec<ApplyPredicateSpec>,
```

- [ ] **Step 3: Initialize the new field empty** at every `ResolvedSelect { .. }` literal. Find them: `grep -rn "ResolvedSelect {" src/sql/`. Each gets `predicate_apply_specs: Vec::new(),` (the two production sites are `src/sql/analyzer/mod.rs:578` and `src/sql/analyzer/subquery_rewrite.rs:2070`; fix any test fixtures the same way).

- [ ] **Step 4: Build + baseline.**

Run: `cargo build 2>&1 | tail -3`
Expected: success (a `dead_code` warning on `ApplyPredicateSpec` fields is acceptable here — they are read starting Task 3; if `-D warnings` is on, add `#[allow(dead_code)]` on the struct with a `// Read by the planner (Task 3)` note, mirroring `ApplyScalarSpec`).

Run: `cargo test --lib 2>&1 | grep '^test result' | tail -1`
Expected: `... 0 failed ...`

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(optimizer): ApplyPredicateSpec scaffold for EXISTS/IN apply path

Adds ApplyPredicateSpec (parallel to the scalar-only ApplyScalarSpec) and a
sibling ResolvedSelect.predicate_apply_specs vec, carrying the analyzed inner
query, correlation column ids, the IN LHS, kind, and use_semi_anti. Nothing
reads them yet (analyzer collection lands next), so behaviour is unchanged;
the scalar apply path is untouched."
```

---

### Task 2: `collect_predicate_apply_spec` + widen the routing gate (analyzer)

**Files:**
- Modify: `src/sql/analyzer/subquery_rewrite.rs` (new method + the `route_to_apply` gate at `:80-100`)

This records an `ApplyPredicateSpec` for a WHERE top-level-AND EXISTS/IN subquery, removing its placeholder conjunct. Unsupported shapes return `Ok(false)` (→ legacy) or `Err` (→ fatal in apply_strict). It reuses the scalar helpers verbatim.

- [ ] **Step 1: Write failing unit tests** in the analyzer test module (model on `src/sql/analyzer/mod.rs:6855-6919`, the existing apply-spec tests). Use `with_session_optimizer_settings` to force `Apply` mode, analyze a SELECT, and assert on `ResolvedSelect.predicate_apply_specs`. Cover:
  - `exists_correlated_where_records_predicate_spec`: `SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k)` → `predicate_apply_specs.len() == 1`; `kind == SubqueryKind::Exists { negated: false }`; `use_semi_anti == true`; `correlation_column_ids` has 1 id; `in_lhs.is_none()`; and the WHERE filter no longer contains the placeholder.
  - `not_exists_sets_negated`: `... WHERE NOT EXISTS (...)` → `kind == Exists { negated: true }`.
  - `in_uncorrelated_records_spec_with_lhs`: `SELECT a FROM t1 WHERE t1.a IN (SELECT t2.b FROM t2)` → 1 spec; `kind == InSubquery { negated: false }`; `in_lhs.is_some()`; `correlation_column_ids.is_empty()`.
  - `not_in_sets_negated`: `... WHERE t1.a NOT IN (...)` → `kind == InSubquery { negated: true }`.
  - `exists_inside_or_falls_back_to_legacy`: `... WHERE x=1 OR EXISTS (...)` → `predicate_apply_specs.is_empty()` (routed to legacy; the legacy join rewrite ran instead).
  - `exists_in_having_falls_back_to_legacy`: EXISTS in HAVING → `predicate_apply_specs.is_empty()`.
  - `multi_column_in_falls_back_to_legacy`: `(a, b) IN (SELECT ...)` → `predicate_apply_specs.is_empty()`.
  - `non_eq_only_outer_ref_falls_back`: `... WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.flag = t1.flag AND t2.v > t1.v)` where the correlation is detectable but mixes a non-collectable shape — assert it still records a spec (EXISTS allows the whole inner WHERE in the ON) OR falls back; pin whichever the implementation chooses and document it. (Simplest: EXISTS records the spec; the non-EQ correlation goes into the semi-join ON unchanged. Choose this and assert a spec is recorded.)

  Run: `cargo test --lib -- collect_predicate_apply_spec exists_ in_ not_in not_exists multi_column` (or the module path) → FAIL.

- [ ] **Step 2: Implement `collect_predicate_apply_spec`** as a method on the analyzer (place next to `collect_scalar_apply_spec` in `subquery_rewrite.rs`):

```rust
    /// Apply-mode handler for an EXISTS / NOT EXISTS / IN / NOT IN subquery.
    /// Returns Ok(true) if an ApplyPredicateSpec was recorded and the placeholder
    /// conjunct removed; Ok(false) if the shape should fall back to legacy
    /// (not a top-level AND of WHERE, HAVING/JOIN-ON/projection position,
    /// multi-column IN, or a correlated form this milestone does not handle);
    /// Err on a hard analysis failure.
    fn collect_predicate_apply_spec(
        &self,
        select: &mut ResolvedSelect,
        scope: &mut AnalyzerScope,
        sq_info: &SubqueryInfo,
    ) -> Result<bool, String> {
        use crate::sql::analysis::{ApplyClause, ApplyPredicateSpec, OutputColumn};

        // 1. M3 scope: the placeholder must be a top-level AND conjunct of WHERE
        //    (use_semi_anti). HAVING / projection / JOIN-ON / inside-OR → legacy.
        let in_where = select
            .filter
            .as_ref()
            .map(|f| expr_contains_placeholder(f, sq_info.id))
            .unwrap_or(false);
        if !in_where {
            return Ok(false);
        }
        let inside_or = select
            .filter
            .as_ref()
            .map(|f| is_placeholder_inside_or(f, sq_info.id))
            .unwrap_or(false);
        if inside_or {
            return Ok(false);
        }

        // 2. Analyze the inner subquery with the merged outer scope (same call
        //    the scalar path and legacy use). Outer refs carry outer ColumnIds.
        let (resolved_sub, inner_scope) =
            self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;

        // 3. IN: analyze the single-column LHS. Multi-column (tuple) → legacy (M4).
        let in_lhs = match &sq_info.kind {
            SubqueryKind::InSubquery { .. } => {
                let in_expr = sq_info
                    .in_expr
                    .as_ref()
                    .ok_or_else(|| "IN subquery missing LHS expression".to_string())?;
                if matches!(
                    in_expr.as_ref(),
                    sqlparser::ast::Expr::Tuple(_)
                ) || matches!(
                    in_expr.as_ref(),
                    sqlparser::ast::Expr::Nested(inner)
                        if matches!(inner.as_ref(), sqlparser::ast::Expr::Tuple(_))
                ) {
                    return Ok(false); // multi-column IN → legacy (M4)
                }
                if resolved_sub.output_columns.len() != 1 {
                    return Ok(false); // shape mismatch → legacy
                }
                Some(self.analyze_expr(in_expr, scope)?)
            }
            SubqueryKind::Exists { .. } => None,
            SubqueryKind::Scalar => return Ok(false), // not our kind
        };

        // 4. Correlation. corr_ids are the outer columns referenced via EQ
        //    correlation predicates. Guard the D7 gap: if the inner references
        //    outer columns but we extracted NO clean correlation, this is a
        //    shape M3 does not handle — fall back to legacy.
        let corr_ids = collect_correlation_column_ids(&resolved_sub, &inner_scope, scope);
        let references_outer = subquery_references_outer_scope(&resolved_sub, &inner_scope, scope);
        if references_outer && corr_ids.is_empty() {
            return Ok(false);
        }

        // 5. Mint a Boolean indicator output column for the Apply schema.
        let output_name = format!("__pred_sq_{}", sq_info.id);
        let output_id =
            self.alloc_column_id(None, output_name.clone(), DataType::Boolean, false);
        let output_column = OutputColumn {
            column_id: output_id,
            name: output_name,
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: true,
        };

        // 6. Remove the placeholder conjunct from WHERE (semantics move to the
        //    semi/anti join). Mirrors legacy rewrite_exists.
        Self::remove_placeholder_from_filter(&mut select.filter, sq_info.id);

        // 7. Record the spec. Inner left INTACT.
        select.predicate_apply_specs.push(ApplyPredicateSpec {
            subquery_id: sq_info.id,
            kind: sq_info.kind.clone(),
            clause: ApplyClause::Where,
            output_column,
            inner: resolved_sub,
            correlation_column_ids: corr_ids,
            in_lhs,
            use_semi_anti: true,
            subquery_text: sq_info.subquery.to_string(),
        });
        Ok(true)
    }
```

  Implement the small helper `subquery_references_outer_scope` if a single-call wrapper does not already exist — it should return true iff any ColumnRef in the inner WHERE resolves in the outer-but-not-inner scope. Reuse the existing `expr_references_outer_scope` (`subquery_rewrite.rs:2633`) over the inner SELECT's filter:

```rust
    fn subquery_references_outer_scope(
        resolved_sub: &crate::sql::analysis::ResolvedQuery,
        inner_scope: &super::scope::AnalyzerScope,
        outer_scope: &super::scope::AnalyzerScope,
    ) -> bool {
        use crate::sql::analysis::QueryBody;
        if let QueryBody::Select(sel) = &resolved_sub.body {
            if let Some(f) = &sel.filter {
                return expr_references_outer_scope(f, inner_scope, outer_scope);
            }
        }
        false
    }
```

  Confirm the exact signature of `expr_references_outer_scope` and `remove_placeholder_from_filter` before wiring (they are free fns / `Self::` methods in the same file). If `remove_placeholder_from_filter` does not exist under that name, the legacy EXISTS path's conjunct-removal call (`rewrite_exists`, near `subquery_rewrite.rs:1081`) names it — use whatever it is.

- [ ] **Step 3: Widen the routing gate** at `subquery_rewrite.rs:80-100`. Replace the scalar-only block with one that dispatches by kind:

```rust
            let mode = super::subquery_unnest_mode();
            let apply_enabled = !matches!(mode, SubqueryUnnestMode::Legacy);
            if apply_enabled {
                let routed = match &sq_info.kind {
                    SubqueryKind::Scalar => {
                        self.collect_scalar_apply_spec(select, scope, &sq_info)
                    }
                    SubqueryKind::Exists { .. } | SubqueryKind::InSubquery { .. } => {
                        self.collect_predicate_apply_spec(select, scope, &sq_info)
                    }
                };
                match routed {
                    Ok(true) => continue, // spec recorded; placeholder handled
                    Ok(false) => { /* unsupported shape — fall through to legacy */ }
                    Err(e) => {
                        if matches!(mode, SubqueryUnnestMode::ApplyStrict) {
                            return Err(e);
                        }
                        // apply (non-strict): fall back to legacy for this subquery.
                    }
                }
            }
            // Legacy path (unchanged) below.
```
  (Import `SubqueryUnnestMode` at the call site or keep the fully-qualified `crate::sql::optimizer::options::SubqueryUnnestMode` as the existing code does.)

- [ ] **Step 4: Run tests + build clean.**

Run: `cargo test --lib -- <the Step-1 test names>` → all PASS.
Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(optimizer): analyzer routes WHERE EXISTS/IN to the Apply framework

Adds collect_predicate_apply_spec: for a top-level-AND WHERE EXISTS / NOT
EXISTS / IN / NOT IN subquery (apply mode), it analyzes the inner with the
merged outer scope, collects EQ correlation column ids, analyzes the IN LHS
(single column only), mints a Boolean indicator, removes the placeholder
conjunct, and records an ApplyPredicateSpec. HAVING / JOIN-ON / projection /
inside-OR / multi-column IN / hidden-correlation shapes return Ok(false) and
fall back to legacy (or Err under apply_strict). The routing gate now
dispatches scalar vs EXISTS/IN by kind. Default Legacy mode is unaffected."
```

---

### Task 3: `wrap_predicate_applies` — planner constructs `ApplyKind::{Exists,In}`

**Files:**
- Modify: `src/sql/planner/mod.rs` (new `wrap_predicate_applies` fn; call it at the WHERE placement point; take `predicate_apply_specs` up-front; extend the trailing `debug_assert!`)
- Modify: `src/sql/planner/plan.rs:111` (drop the `#[allow(dead_code)]` on `ApplyKind` / `ApplyNode` once read — optional; keep if other fields still unread)

- [ ] **Step 1: Write failing unit tests** (planner test module; model on the scalar planner tests). Build a `ResolvedSelect` with one `ApplyPredicateSpec` (EXISTS correlated; then IN uncorrelated), plan it, and walk the result:
  - `plan_exists_builds_apply_exists`: the plan contains `LogicalPlan::Apply` with `kind == ApplyKind::Exists { negated: false }`, `use_semi_anti == true`, `need_check_max_rows == false`, `correlation_conjuncts.is_empty()`, and the Apply sits directly below the WHERE `Filter` (or is the `current` when there is no residual filter).
  - `plan_not_in_builds_apply_in_negated`: `kind == ApplyKind::In { negated: true }`, `subquery_expr` equals the spec's `in_lhs` expression.
  - `plan_exists_subquery_expr_is_boolean_colref`: for EXISTS, `subquery_expr` is `ColumnRef(output_column.column_id)` of Boolean type.

  Run: `cargo test --lib -- plan_exists plan_not_in` → FAIL.

- [ ] **Step 2: Implement `wrap_predicate_applies`** in `src/sql/planner/mod.rs` (next to `wrap_scalar_applies`):

```rust
/// Wrap `input` in a left-deep chain of `LogicalPlan::Apply` nodes for each
/// EXISTS/IN predicate spec whose clause matches `clause`. Mirrors
/// `wrap_scalar_applies` but builds `ApplyKind::Exists` / `ApplyKind::In`
/// semi/anti-collapsing applies (use_semi_anti = true, need_check_max_rows =
/// false, correlation_conjuncts empty — the M3 to-join rules read the inner
/// Filter directly).
fn wrap_predicate_applies(
    input: LogicalPlan,
    specs: &mut Vec<ApplyPredicateSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    use crate::sql::analysis::SubqueryKind;
    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory)?;
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "EXISTS/IN subquery inner has no output column".to_string())?;

        let kind = match spec.kind {
            SubqueryKind::Exists { negated } => ApplyKind::Exists { negated },
            SubqueryKind::InSubquery { negated } => ApplyKind::In { negated },
            SubqueryKind::Scalar => {
                return Err("scalar spec routed to wrap_predicate_applies".to_string());
            }
        };

        // subquery_expr: IN carries the LHS (the to-join rule builds `lhs = inner_col`);
        // EXISTS carries a vestigial Boolean ColumnRef(output_column) (unused by the rule).
        let subquery_expr = match (&kind, spec.in_lhs.clone()) {
            (ApplyKind::In { .. }, Some(lhs)) => lhs,
            (ApplyKind::In { .. }, None) => {
                return Err("IN spec missing analyzed LHS".to_string());
            }
            _ => TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: spec.output_column.column_id,
                    qualifier: None,
                    column: spec.output_column.name.clone(),
                },
                data_type: spec.output_column.data_type.clone(),
                nullable: spec.output_column.nullable,
            },
        };

        current = LogicalPlan::Apply(ApplyNode {
            left: Box::new(current),
            right: Box::new(right),
            kind,
            subquery_expr,
            output_column: spec.output_column,
            inner_output_column_id,
            correlation_column_ids: spec.correlation_column_ids,
            correlation_conjuncts: Vec::new(),
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: spec.use_semi_anti,
            uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            required_output_columns: None,
        });
    }
    *specs = remaining;
    Ok(current)
}
```

- [ ] **Step 3: Wire it into `plan_select_scoped`.** At the top (next to `let mut apply_specs = std::mem::take(&mut select.apply_specs);`, `mod.rs:762`) add:
```rust
    let mut predicate_apply_specs = std::mem::take(&mut select.predicate_apply_specs);
```
  At the WHERE placement point (immediately after the `wrap_scalar_applies(... ApplyClause::Where ...)` call, `mod.rs:776`) add:
```rust
    current = wrap_predicate_applies(
        current,
        &mut predicate_apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
    )?;
```
  (M3 only records `ApplyClause::Where`, so no Having/Projection wrap calls are needed.) Extend the trailing guard (`mod.rs:899`):
```rust
    debug_assert!(
        apply_specs.is_empty() && predicate_apply_specs.is_empty(),
        "unplaced apply specs: scalar={:?} predicate={:?}",
        apply_specs.iter().map(|s| s.clause).collect::<Vec<_>>(),
        predicate_apply_specs.iter().map(|s| s.clause).collect::<Vec<_>>()
    );
```

- [ ] **Step 4: Run tests + build clean.**

Run: `cargo test --lib -- plan_exists plan_not_in` → PASS.
Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

> **End-to-end check after Task 3:** in `apply` mode a WHERE EXISTS/IN now plans to a `LogicalPlan::Apply(kind=Exists|In)` that NO rule eliminates yet → `ApplyException` fires → the analyzer's non-strict fallback already ran legacy, so default behaviour is unaffected, but an `apply_strict` EXISTS query will error with "subquery decorrelation failed" until Task 4/5. That is expected; the to-join rules land next.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(optimizer): planner emits ApplyKind::Exists/In for WHERE EXISTS/IN

Adds wrap_predicate_applies, the EXISTS/IN analogue of wrap_scalar_applies:
plans the inner subquery, maps SubqueryKind to ApplyKind, stores the IN LHS in
subquery_expr (a vestigial Boolean ColumnRef for EXISTS), and builds a
semi/anti-collapsing Apply (use_semi_anti=true, need_check_max_rows=false,
correlation_conjuncts empty). Wired at the WHERE placement point; the trailing
unplaced-spec guard now covers predicate specs. The to-join rules that
eliminate these Apply nodes land in M3b."
```

## M3b — to-join rules

> **Shared rule shape (both rules are self-contained).** For a matched `Apply`:
> - **Uncorrelated** (`correlation_column_ids.is_empty()`): the inner is self-contained → `right = a.right` (intact); ON = the IN key (`In`) or `Literal(true)` (`Exists`).
> - **Correlated**: the inner `Filter` references outer columns → it MUST move to the join ON. Locate the inner `Filter` (peeling one optional leading `Project`); `right` = the inner with that `Filter` replaced by its input (Project preserved); ON = the lifted Filter predicate (AND the IN key for `In`). For `NOT IN` with a nullable lifted predicate, wrap it `coalesce(<pred>, false)`. If the expected `[Project?] Filter(<rel>)` shape is not found, return `Unchanged` (→ `ApplyException` → legacy fallback).
>
> A shared helper does the locate/lift; each rule supplies the IN key (or none) and the `JoinKind`.

### Task 4: `ExistentialApplyToJoin` (EXISTS / NOT EXISTS → LeftSemi / LeftAnti)

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/existential_apply_to_join.rs`
- Create: `src/sql/optimizer/rewrite/rules/subquery/predicate_apply_util.rs` (the shared locate/lift helper, reused by Task 5)
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs` (declare both modules; the rule is registered in Task 6)

- [ ] **Step 1: Write the shared helper + its failing tests** in `predicate_apply_util.rs`:

```rust
//! Shared helpers for the EXISTS/IN to-join rules (ExistentialApplyToJoin,
//! QuantifiedApplyToJoin). Locates the inner subquery's correlated WHERE and
//! lifts it into a join ON condition, leaving an outer-reference-free `right`
//! subtree.

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::plan::{FilterNode, LogicalPlan, ProjectNode};

/// Result of lifting a correlated subquery's WHERE into a join ON.
pub(super) struct LiftedInner {
    /// The outer subtree's right child for the join (no outer references).
    pub right: LogicalPlan,
    /// The predicate lifted out of the inner Filter (correlation + residual),
    /// or None when the inner had no Filter to lift.
    pub on_predicate: Option<TypedExpr>,
}

/// For a correlated Apply.right of shape `[Project?] Filter(<rel>)`, return the
/// `<rel>` (with the Project re-applied if present) plus the Filter predicate to
/// move into the ON. Returns None if the expected shape is absent (caller →
/// Unchanged). For an uncorrelated inner, callers should NOT call this — they
/// keep `right` intact and build the ON from the IN key / `true`.
pub(super) fn lift_correlated_inner(inner: LogicalPlan) -> Option<LiftedInner> {
    match inner {
        LogicalPlan::Project(p) => {
            let ProjectNode { input, items, output_qualifier, required_output_columns } = p;
            match *input {
                LogicalPlan::Filter(f) => Some(LiftedInner {
                    right: LogicalPlan::Project(ProjectNode {
                        input: f.input,
                        items,
                        output_qualifier,
                        required_output_columns,
                    }),
                    on_predicate: Some(f.predicate),
                }),
                other => Some(LiftedInner { // Project over a non-Filter: nothing to lift
                    right: LogicalPlan::Project(ProjectNode {
                        input: Box::new(other),
                        items,
                        output_qualifier,
                        required_output_columns,
                    }),
                    on_predicate: None,
                }),
            }
        }
        LogicalPlan::Filter(f) => Some(LiftedInner {
            right: *f.input,
            on_predicate: Some(f.predicate),
        }),
        // No leading Project, no top Filter: nothing to lift (defensive; a
        // correlated inner is expected to have a Filter carrying the correlation).
        _ => None,
    }
}

/// `coalesce(pred, false)` as a Boolean TypedExpr — used for NOT IN's lifted
/// predicate when it is nullable (legacy NAAJ semantics).
pub(super) fn coalesce_false(pred: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![
                pred,
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(false)),
                    data_type: crate::sql::analysis::DataType::Boolean,
                    nullable: false,
                },
            ],
            distinct: false,
        },
        data_type: crate::sql::analysis::DataType::Boolean,
        nullable: false,
    }
}

/// `Literal(true)` Boolean expr (uncorrelated EXISTS join ON).
pub(super) fn literal_true() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Bool(true)),
        data_type: crate::sql::analysis::DataType::Boolean,
        nullable: false,
    }
}

/// Build a bare `Eq` Boolean predicate `left = right` (the IN join key).
pub(super) fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::Eq,
            right: Box::new(right),
        },
        data_type: crate::sql::analysis::DataType::Boolean,
        nullable: false,
    }
}
```
  Tests (in the same file's `#[cfg(test)]`): `lift_project_filter_returns_rel_and_pred` (Project(Filter(Scan)) → right is Project(Scan), `on_predicate.is_some()`); `lift_bare_filter` (Filter(Scan) → right is Scan, pred Some); `lift_scan_returns_none` (bare Scan → None). Confirm the exact `LiteralValue::Bool`, `FunctionCall { distinct }`, and `DataType` paths compile (adjust imports to the real module paths — `DataType` is `arrow::datatypes::DataType` re-exported in `crate::sql::analysis`).

  Run: `cargo test --lib -- predicate_apply_util` → FAIL (then PASS after the impl).

- [ ] **Step 2: Write the failing rule tests** in `existential_apply_to_join.rs` (fixtures modeled on `scalar_apply_to_join.rs` tests; use `ctx_with_factory()` — the rule does not mint columns, but the helper is the standard way to build a ctx). Build EXISTS `ApplyNode` fixtures over `Scan` leaves:
  - `exists_correlated_emits_left_semi`: `Apply{ kind: Exists{negated:false}, correlation_column_ids:[outer_k], right: Project(Filter(inner.k == outer.k)(Scan inner)) }` → `Changed(Join{ join_type: LeftSemi, condition: Some(<inner.k == outer.k>), left: <outer>, right: Project(Scan inner) })`. Assert `find_residual_apply(&result).is_none()`.
  - `not_exists_correlated_emits_left_anti`: `negated:true` → `JoinKind::LeftAnti`.
  - `exists_uncorrelated_emits_left_semi_on_true`: `correlation_column_ids:[]`, inner `Scan` (no Filter) → `Join{ LeftSemi, condition: Some(Literal(true)), right: <inner intact> }`.
  - `not_exists_uncorrelated_emits_left_anti_on_true`: `negated:true`, uncorrelated → `LeftAnti`, ON `true`.

  Run: `cargo test --lib -- existential_apply_to_join` → FAIL.

- [ ] **Step 3: Implement `ExistentialApplyToJoin`:**

```rust
//! `ExistentialApplyToJoin` — EXISTS / NOT EXISTS → LeftSemi / LeftAnti join.
//!
//! Self-contained: reads the inner subquery's WHERE directly (no dependency on
//! PushDownApplyFilter). Output is plan-isomorphic with the legacy rewrite:
//! correlated EXISTS → `outer LEFT SEMI JOIN inner ON <inner WHERE>`;
//! NOT EXISTS → LEFT ANTI; uncorrelated → semi/anti ON true.

use super::predicate_apply_util::{lift_correlated_inner, literal_true};
use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::{ApplyKind, JoinNode, LogicalPlan};

pub(crate) struct ExistentialApplyToJoin;

impl LogicalRewriteRule for ExistentialApplyToJoin {
    fn name(&self) -> &'static str {
        "ExistentialApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(a) if matches!(a.kind, ApplyKind::Exists { .. }))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let negated = match a.kind {
            ApplyKind::Exists { negated } => negated,
            _ => return Ok(RewriteResult::Unchanged),
        };
        let join_type = if negated { JoinKind::LeftAnti } else { JoinKind::LeftSemi };

        let (right, condition) = if a.correlation_column_ids.is_empty() {
            // Uncorrelated: keep the inner intact; ON = true.
            (*a.right, literal_true())
        } else {
            // Correlated: lift the inner WHERE into the ON.
            let Some(lifted) = lift_correlated_inner(*a.right) else {
                return Ok(RewriteResult::Unchanged); // unexpected inner shape → fallback
            };
            let Some(pred) = lifted.on_predicate else {
                // Correlated but no Filter found — cannot place the correlation.
                return Ok(RewriteResult::Unchanged);
            };
            (lifted.right, pred)
        };

        Ok(RewriteResult::Changed(LogicalPlan::Join(JoinNode {
            left: a.left,
            right: Box::new(right),
            join_type,
            condition: Some(condition),
            required_output_columns: None,
        })))
    }
}
```

- [ ] **Step 4: Declare the modules** in `subquery/mod.rs`: add `mod existential_apply_to_join;` and `mod predicate_apply_util;` to the `mod` block, and `pub(crate) use existential_apply_to_join::ExistentialApplyToJoin;` to the re-exports. (Registration in `subquery_rewrite_rules()` is Task 6.)

- [ ] **Step 5: Run tests + build clean.**

Run: `cargo test --lib -- existential_apply_to_join predicate_apply_util` → PASS.
Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(optimizer): ExistentialApplyToJoin (EXISTS/NOT EXISTS to semi/anti join)

Adds the self-contained ExistentialApplyToJoin rule plus predicate_apply_util
(lift_correlated_inner: moves a correlated inner WHERE into the join ON,
leaving an outer-reference-free right subtree). Correlated EXISTS -> LEFT SEMI
JOIN ON <inner WHERE>; NOT EXISTS -> LEFT ANTI; uncorrelated -> semi/anti ON
true. Output is plan-isomorphic with the legacy rewrite. Not yet registered."
```

---

### Task 5: `QuantifiedApplyToJoin` (IN / NOT IN → LeftSemi / NullAwareLeftAnti | LeftAnti)

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/quantified_apply_to_join.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs` (declare module; registration in Task 6)

- [ ] **Step 1: Write failing rule tests** in `quantified_apply_to_join.rs`. Build `In` `ApplyNode` fixtures. `subquery_expr` = the LHS expr; `inner_output_column_id` = the inner's output column id; the inner's first output column's `nullable` flag drives the NAAJ decision. Cover:
  - `in_uncorrelated_emits_left_semi`: `kind: In{negated:false}`, `correlation_column_ids:[]`, `subquery_expr` = `ColumnRef(outer_a)` (non-nullable), inner `Scan` exposing `inner_b` (non-nullable) as its first output. Expect `Changed(Join{ LeftSemi, condition: Some(<outer_a = inner_b>), right: <inner intact> })`; assert the condition is a bare `BinaryOp{ Eq }` (NOT wrapped in IS NULL / coalesce).
  - `not_in_nullable_emits_null_aware_left_anti`: `negated:true`; either `outer_a` or `inner_b` nullable → `JoinKind::NullAwareLeftAnti`; condition still bare `Eq`.
  - `not_in_non_nullable_downgrades_to_left_anti`: `negated:true`; both `outer_a` and `inner_b` non-nullable → `JoinKind::LeftAnti`.
  - `in_correlated_emits_semi_with_lifted_on`: `correlation_column_ids:[outer_k]`, inner `Project(Filter(inner.k == outer.k)(Scan))`; expect `LeftSemi`, condition = `combine_and([outer_a = inner_b, inner.k == outer.k])` (the IN key bare Eq AND the lifted correlation), `right` = `Project(Scan)`.
  - `not_in_correlated_nullable_coalesces_lifted_pred`: `negated:true`, nullable operands, correlated; expect `NullAwareLeftAnti`, condition = `combine_and([outer_a = inner_b, coalesce(<inner.k == outer.k>, false)])` — the IN key stays bare Eq, the lifted predicate is coalesce-wrapped.

  Run: `cargo test --lib -- quantified_apply_to_join` → FAIL.

- [ ] **Step 2: Implement `QuantifiedApplyToJoin`:**

```rust
//! `QuantifiedApplyToJoin` — IN / NOT IN → LeftSemi / NullAwareLeftAnti | LeftAnti.
//!
//! Self-contained. The IN key (`lhs = inner_col`) is ALWAYS a bare `Eq` so the
//! Cascades implement phase can extract a hash key; NULL-aware NOT IN semantics
//! live entirely in the JoinKind (NullAwareLeftAnti), never in an IS-NULL-OR
//! wrapper (legacy lesson: IS-NULL-OR wrapping degraded NOT IN to a NestLoop
//! join that timed out). For correlated NOT IN with a nullable lifted inner
//! WHERE, that lifted predicate is wrapped coalesce(pred, false) (legacy NAAJ).

use super::predicate_apply_util::{coalesce_false, eq, lift_correlated_inner};
use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::combine_and;
use crate::sql::planner::plan::{ApplyKind, JoinNode, LogicalPlan};
use crate::sql::planner::plan_output_columns;

pub(crate) struct QuantifiedApplyToJoin;

impl LogicalRewriteRule for QuantifiedApplyToJoin {
    fn name(&self) -> &'static str {
        "QuantifiedApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(a) if matches!(a.kind, ApplyKind::In { .. }))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let negated = match a.kind {
            ApplyKind::In { negated } => negated,
            _ => return Ok(RewriteResult::Unchanged),
        };

        // The IN LHS lives in subquery_expr (Task 3). The RHS is the inner's
        // single output column. Build a column ref to it from the inner schema.
        let lhs = a.subquery_expr.clone();
        let inner_cols = plan_output_columns(&a.right)?;
        let inner_col_oc = inner_cols
            .iter()
            .find(|c| c.column_id == a.inner_output_column_id)
            .or_else(|| inner_cols.first())
            .ok_or_else(|| "IN subquery inner has no output column".to_string())?;
        let inner_col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: inner_col_oc.column_id,
                qualifier: None,
                column: inner_col_oc.name.clone(),
            },
            data_type: inner_col_oc.data_type.clone(),
            nullable: inner_col_oc.nullable,
        };

        // NOT IN null-aware decision: any nullable operand → NAAJ; else LeftAnti.
        let either_nullable = lhs.nullable || inner_col_ref.nullable;
        let join_type = if negated {
            if either_nullable {
                JoinKind::NullAwareLeftAnti
            } else {
                JoinKind::LeftAnti
            }
        } else {
            JoinKind::LeftSemi
        };

        // IN key — ALWAYS a bare Eq (hash-key extractable).
        let in_key = eq(lhs, inner_col_ref);

        let (right, condition) = if a.correlation_column_ids.is_empty() {
            // Uncorrelated: keep inner intact; ON = the IN key.
            (*a.right, in_key)
        } else {
            // Correlated: lift the inner WHERE into the ON alongside the IN key.
            let Some(lifted) = lift_correlated_inner(*a.right) else {
                return Ok(RewriteResult::Unchanged);
            };
            let Some(lifted_pred) = lifted.on_predicate else {
                return Ok(RewriteResult::Unchanged);
            };
            // NOT IN: coalesce-wrap a nullable lifted predicate (legacy NAAJ).
            let extra = if negated && lifted_pred.nullable {
                coalesce_false(lifted_pred)
            } else {
                lifted_pred
            };
            (lifted.right, combine_and(vec![in_key, extra]))
        };

        Ok(RewriteResult::Changed(LogicalPlan::Join(JoinNode {
            left: a.left,
            right: Box::new(right),
            join_type,
            condition: Some(condition),
            required_output_columns: None,
        })))
    }
}
```

- [ ] **Step 3: Declare the module** in `subquery/mod.rs`: `mod quantified_apply_to_join;` + `pub(crate) use quantified_apply_to_join::QuantifiedApplyToJoin;`.

- [ ] **Step 4: Run tests + build clean.**

Run: `cargo test --lib -- quantified_apply_to_join` → PASS.
Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(optimizer): QuantifiedApplyToJoin (IN/NOT IN to semi/anti/NAAJ)

Adds the self-contained QuantifiedApplyToJoin rule. IN -> LEFT SEMI; NOT IN ->
NULL AWARE LEFT ANTI when either operand is nullable, else plain LEFT ANTI
(legacy downgrade). The IN key (lhs = inner_col) is always a bare Eq so the
implement phase can extract a hash key; null-aware semantics live in the
JoinKind. Correlated forms lift the inner WHERE into the ON; for NOT IN a
nullable lifted predicate is coalesce(pred, false)-wrapped (legacy NAAJ).
Not yet registered."
```

---

### Task 6: Register both rules in the SubqueryRewrite stage

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`

- [ ] **Step 1: Write a failing test** in `subquery/mod.rs`'s test module (model on `apply_exception_never_fires_for_decorrelatable_apply`, `:127`). Build an `Apply{ kind: Exists{negated:false}, correlation_column_ids:[], right: Values }` (uncorrelated EXISTS over empty Values), run `query_rewrite_pipeline(&HashMap::new()).rewrite(plan, &mut ctx)`, and assert `find_residual_apply(&result).is_none()` (the Apply was eliminated to a LeftSemi join) — this fails today because the rule is not in the list.

  Run: `cargo test --lib -- subquery::tests` → FAIL on the new test.

- [ ] **Step 2: Register the rules.** In `subquery_rewrite_rules()` insert the two new rules AFTER `ScalarApplyToJoin` and BEFORE `ApplyException`:

```rust
pub(crate) fn subquery_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(PushDownApplyAggFilter),
        Box::new(PushDownApplyFilter),
        Box::new(ApplyToWindow), // to-window BEFORE to-join (StarRocks ordering)
        Box::new(ScalarApplyToJoin),
        Box::new(ExistentialApplyToJoin), // EXISTS / NOT EXISTS → LeftSemi / LeftAnti
        Box::new(QuantifiedApplyToJoin),  // IN / NOT IN → LeftSemi / NullAwareLeftAnti|LeftAnti
        Box::new(ApplyException), // must stay LAST
    ]
}
```
  Ensure the `pub(crate) use` re-exports for both rules (added in Tasks 4/5) are present.

- [ ] **Step 3: Run tests + build clean.**

Run: `cargo test --lib -- subquery::tests` → PASS.
Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.
Run: `cargo clippy --lib 2>&1 | grep -iE 'existential|quantified|predicate_apply' ` → no new lints.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(optimizer): register Existential/QuantifiedApplyToJoin in SubqueryRewrite

Inserts the two EXISTS/IN to-join rules after ScalarApplyToJoin and before the
ApplyException terminal guard, matching the design rule ordering. WHERE
EXISTS/IN subqueries now decorrelate end-to-end in apply mode. Each rule name
is disable-able via SET disable_optimizer_rules."
```

## M3c — verification (parity, plan-golden, CI)

### Task 7: Engine-level apply == legacy parity tests (+ NOT IN NULL matrix)

**Files:**
- Modify: `src/engine/mod.rs` (add tests in the `#[cfg(test)] mod tests` module, next to the M1 `scalar_subquery_*` tests at `:9319+`)

These are the correctness bar: in `Apply` mode the result must equal `Legacy` mode, row-for-row, across EXISTS/IN shapes — and the NOT IN NULL matrix must match SQL semantics exactly. Reuse the M1 harness verbatim (`open_scalar_subquery_test_engine`, `run_scalar_query_i64`, `with_session_optimizer_settings`).

- [ ] **Step 1: Write the parity tests** (each follows the M1 pattern: create tables, insert, run the same SQL under `Legacy` and `Apply`, `assert_eq!(apply, legacy)`):
  - `exists_correlated_apply_matches_legacy`: `SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k) ORDER BY 1`.
  - `not_exists_correlated_apply_matches_legacy`: `... WHERE NOT EXISTS (...) ORDER BY 1`.
  - `exists_uncorrelated_apply_matches_legacy`: `SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v > 100) ORDER BY 1` (non-empty vs empty t2 → all rows vs none).
  - `in_correlated_apply_matches_legacy`: `SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1`.
  - `in_uncorrelated_apply_matches_legacy`: `SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2) ORDER BY 1`.
  - **NOT IN NULL matrix** (the critical NAAJ cases — `assert_eq!(apply, legacy)` AND assert the expected SQL-semantics value, since the whole point is NULL correctness):
    - `not_in_uncorrelated_no_null`: t2.v has no NULL → standard anti-membership.
    - `not_in_uncorrelated_build_null`: t2 contains a NULL v → result MUST be empty (any build NULL ⇒ all probe rows dropped). Assert `apply == legacy == vec![]`.
    - `not_in_uncorrelated_probe_null`: t1 contains a NULL v → that probe row is dropped.
    - `not_in_correlated_conjunct`: `SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1` with a nullable t2.v in some group.
  - **multi-subquery** (regression for stacked applies): `SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) AND EXISTS (SELECT 1 FROM t2 b WHERE b.k = t1.k) ORDER BY 1` → `assert_eq!(apply, legacy)`.

  Use BIGINT columns so `run_scalar_query_i64` reads them. For NULL cases insert explicit `NULL` values (`insert into ice.db1.t2 values (1, NULL)`).

  Run: `cargo test --lib -- exists_ not_exists_ in_ not_in_` → FAIL until the M3a/M3b code is in place (it is, after Task 6) — so these should actually PASS once written; if any FAILS, the apply path diverges from legacy → debug before proceeding (this is the bar, not a formality).

- [ ] **Step 2: Run + build clean.**

Run: `cargo test --lib -- exists_ not_exists_ in_ not_in_ multi_subquery` → all PASS (apply == legacy everywhere).
Run: `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

> If a NOT IN NULL case diverges, the bug is almost certainly the `either_nullable` decision (Task 5) or the `coalesce` wrapping — re-check against legacy `subquery_rewrite.rs:1517-1529` (uncorrelated) and `:1584,1594-1615,1630-1638` (correlated). Do NOT "fix" by wrapping the IN key in IS-NULL-OR (constraint #3).

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test(optimizer): engine-level apply==legacy parity for EXISTS/IN (+ NOT IN NULL)

Adds end-to-end tests asserting apply mode returns the same rows as legacy for
correlated/uncorrelated EXISTS, NOT EXISTS, IN, and NOT IN, plus the NOT IN
NULL matrix (build-NULL drops all rows, probe-NULL drops the row) and a stacked
IN+EXISTS multi-subquery case. These pin the M3 to-join output to legacy
semantics."
```

---

### Task 8: Optimizer plan-golden cases (apply shape + disable-rule fallback)

**Files:**
- Create: `sql-tests/optimizer/sql/subquery_exists_to_join.sql` + `sql-tests/optimizer/result/subquery_exists_to_join.result`
- Create: `sql-tests/optimizer/sql/subquery_in_to_join.sql` + `result/subquery_in_to_join.result`
- Possibly modify: `sql-tests/optimizer/init.sql` (add small tables if the existing ones don't fit; reuse existing tables where possible)

Model exactly on `sql-tests/optimizer/sql/subquery_scalar_to_window.sql` (the `SET subquery_unnest_mode='apply'` + `@explain_contains` + `SET disable_optimizer_rules` fallback pattern).

- [ ] **Step 1: Write `subquery_exists_to_join.sql`:**

```sql
-- @tags=optimizer,subquery,exists,apply
SET subquery_unnest_mode='apply';

-- Correlated EXISTS rewrites to a LEFT SEMI JOIN; no APPLY survives.
-- @explain_contains=LEFT SEMI JOIN
-- @explain_not_contains=APPLY
SELECT a.k FROM ${case_db}.sq_t1 a
WHERE EXISTS (SELECT 1 FROM ${case_db}.sq_t2 b WHERE b.k = a.k)
ORDER BY 1;

-- Correlated NOT EXISTS rewrites to a LEFT ANTI JOIN.
-- @explain_contains=LEFT ANTI JOIN
-- @explain_not_contains=APPLY
SELECT a.k FROM ${case_db}.sq_t1 a
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.sq_t2 b WHERE b.k = a.k)
ORDER BY 1;

-- Disabling the rule leaves the Apply unresolved -> the analyzer fallback ran
-- legacy, which still produces a semi join. (Sanity: result unchanged.)
SET disable_optimizer_rules='ExistentialApplyToJoin';
-- @explain_not_contains=APPLY
SELECT a.k FROM ${case_db}.sq_t1 a
WHERE EXISTS (SELECT 1 FROM ${case_db}.sq_t2 b WHERE b.k = a.k)
ORDER BY 1;
SET disable_optimizer_rules='';
SET subquery_unnest_mode='legacy';
```

- [ ] **Step 2: Write `subquery_in_to_join.sql`:**

```sql
-- @tags=optimizer,subquery,in,not_in,apply
SET subquery_unnest_mode='apply';

-- IN rewrites to a LEFT SEMI JOIN.
-- @explain_contains=LEFT SEMI JOIN
-- @explain_not_contains=APPLY
SELECT a.k FROM ${case_db}.sq_t1 a
WHERE a.v IN (SELECT b.v FROM ${case_db}.sq_t2 b)
ORDER BY 1;

-- NOT IN on a nullable column rewrites to a NULL AWARE LEFT ANTI JOIN.
-- @explain_contains=NULL AWARE LEFT ANTI JOIN
-- @explain_not_contains=APPLY
SELECT a.k FROM ${case_db}.sq_t1 a
WHERE a.v NOT IN (SELECT b.v FROM ${case_db}.sq_t2 b)
ORDER BY 1;

SET subquery_unnest_mode='legacy';
```

  (If `sq_t1`/`sq_t2` aren't in `init.sql`, add them: `CREATE TABLE sq_t1(k BIGINT, v BIGINT); CREATE TABLE sq_t2(k BIGINT, v BIGINT);` plus a few INSERT rows with at least one NULL `v` in `sq_t2` so the NAAJ case is real. Use `-- @skip_result_check=true` on DDL/INSERT setup statements per the `join_apply_to_join.sql` convention.)

- [ ] **Step 3: Record the goldens** (server must be running — see CLAUDE.md §7.3 startup):

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  ${NOVAROCKS_SQL_TEST_CONFIG:+--config "$NOVAROCKS_SQL_TEST_CONFIG"} \
  --suite optimizer --only subquery_exists_to_join,subquery_in_to_join --mode record --record-from target
```
  Then verify:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  ${NOVAROCKS_SQL_TEST_CONFIG:+--config "$NOVAROCKS_SQL_TEST_CONFIG"} \
  --suite optimizer --only subquery_exists_to_join,subquery_in_to_join --mode verify
```
  Expected: PASS, and the recorded `.result` plan lines contain `LEFT SEMI JOIN` / `LEFT ANTI JOIN` / `NULL AWARE LEFT ANTI JOIN` and no `APPLY`.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(optimizer): plan-golden for EXISTS/IN to-join rewrites

Adds optimizer-suite plan goldens asserting WHERE EXISTS -> LEFT SEMI JOIN,
NOT EXISTS -> LEFT ANTI JOIN, IN -> LEFT SEMI JOIN, NOT IN (nullable) -> NULL
AWARE LEFT ANTI JOIN, with no surviving APPLY, plus a disable-rule sanity case."
```

---

### Task 9: `apply_strict` regression sweep over the EXISTS/IN/NAAJ anchors + final baseline

**Files:**
- Modify: the parity anchor sql-test files OR a CI wrapper — add a `SET subquery_unnest_mode='apply_strict';` prelude variant. Prefer the lowest-friction mechanism the runner already supports (a per-suite init or a `-- @session=...` directive if one exists; otherwise add a small dedicated case that re-runs the anchor queries under apply_strict).

The goal: prove the EXISTS/IN/NAAJ anchors produce identical results under `apply_strict` (every shape goes through Apply or errors — no silent legacy fallback masking gaps).

- [ ] **Step 1: Run the join + runtime-filter + filter anchors under apply_strict.** Easiest reliable path: a throwaway driver case or a manual run with the session var set. Verify each anchor's results are unchanged vs the committed goldens:
```bash
# With a running server (CLAUDE.md §7.3):
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  ${NOVAROCKS_SQL_TEST_CONFIG:+--config "$NOVAROCKS_SQL_TEST_CONFIG"} \
  --suite join --only join_exists_subquery_semantics,join_not_exists_subquery_semantics,\
join_not_in_without_null,join_not_in_with_null,join_not_in_correlated_conjunct_null_aware,\
join_null_aware_anti --mode verify
```
  If the runner supports injecting `subquery_unnest_mode='apply_strict'` via config/env, run the same `--only` set with it set. If a query ERRORS under apply_strict, that shape is not yet covered by M3 — confirm it is an out-of-scope shape (HAVING/JOIN-ON/OR/multi-column IN) and that in plain `apply` mode it falls back to legacy correctly (results unchanged). Document any such shape in the PR description as "remains legacy (M4)".

- [ ] **Step 2: Full library + targeted suite baseline.**
```bash
cargo test --lib 2>&1 | grep '^test result' | tail -1          # 0 failed
# Targeted suites most likely to regress (run in apply default = legacy, so unchanged):
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  ${NOVAROCKS_SQL_TEST_CONFIG:+--config "$NOVAROCKS_SQL_TEST_CONFIG"} \
  --suite join --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  ${NOVAROCKS_SQL_TEST_CONFIG:+--config "$NOVAROCKS_SQL_TEST_CONFIG"} \
  --suite optimizer --mode verify
```
  Expected: all PASS (default mode is `Legacy`, so existing suites are untouched; the new optimizer goldens pass).

- [ ] **Step 3: `cargo fmt` + `cargo clippy`.**
```bash
cargo fmt
cargo clippy --lib 2>&1 | tail -5   # no new warnings in the M3 files
```

- [ ] **Step 4: Commit**
```bash
git add -A && git commit -m "test(optimizer): apply_strict sweep of EXISTS/IN/NAAJ anchors; fmt+clippy

Verifies the join/runtime-filter EXISTS/IN/NAAJ anchors produce identical
results under apply_strict (no silent legacy fallback). Records any shapes that
remain legacy (HAVING/JOIN-ON/value-form/multi-column IN -> M4). fmt + clippy
clean."
```

---

## Self-Review (run against the spec before handing off)

**Spec coverage (design §6.1 rules 10/11, §6.2, §7.2/§7.3 M3):**
- ✅ `ExistentialApplyToJoin` (LeftSemi/LeftAnti) — Task 4. `QuantifiedApplyToJoin` (LeftSemi/NullAwareLeftAnti|LeftAnti) — Task 5. Registered after ScalarApplyToJoin, before ApplyException — Task 6.
- ✅ EXISTS/NOT EXISTS + IN/NOT IN, correlated + uncorrelated, WHERE top-level AND, single-column — Tasks 2/4/5.
- ✅ Isomorphic-to-legacy / bare-Eq / NAAJ-nullability-downgrade / correlated-NOT-IN-coalesce — Tasks 5/7 (constraint #3), pinned by the Task 7 NULL matrix.
- ✅ Opt-in (default Legacy, no flip, no legacy deletion) — constraint #1; `apply_strict` CI — Task 9.
- ✅ Out-of-scope shapes (HAVING/JOIN-ON/OR-projection/multi-column IN/hidden-correlation) fall back to legacy — Task 2 (`Ok(false)`), documented Task 9.
- ✅ Observability: rule names disable-able; EXPLAIN `LEFT SEMI/ANTI/NULL AWARE LEFT ANTI` + no `APPLY` — Task 8.

**Placeholder scan:** No TBD/TODO. Every code step shows complete code. The only "confirm the exact name" notes are for two legacy helpers (`remove_placeholder_from_filter`, `expr_references_outer_scope`) whose call sites are cited (`subquery_rewrite.rs:1081`, `:2633`) — resolve by reading those lines, not by guessing.

**Type consistency:** `ApplyPredicateSpec` fields (Task 1) match their reads in `collect_predicate_apply_spec` (Task 2) and `wrap_predicate_applies` (Task 3). `SubqueryKind` (analyzer) → `ApplyKind` (planner) mapping is explicit in Task 3. `lift_correlated_inner` / `coalesce_false` / `eq` / `literal_true` signatures (Task 4) match their callers (Tasks 4/5). `JoinKind::{LeftSemi,LeftAnti,NullAwareLeftAnti}` spellings match `src/sql/analysis/mod.rs:235-256`.

**Known risks to watch during execution:**
1. `DataType` import path in `predicate_apply_util.rs` — it is `arrow::datatypes::DataType`, re-exported via `crate::sql::analysis`. Use whichever the sibling rule files use (`scalar_apply_to_join.rs` imports `DataType`).
2. `combine_and` panics on empty — every call site here passes ≥1 element (the IN key is always present for `In`; correlated EXISTS always has a lifted predicate). Verified.
3. `coalesce` function name/registration — confirm `coalesce` is the registered name (legacy uses it; `ifnull` is the scalar-path analogue). If the registry uses a different spelling, match it.
4. Uncorrelated EXISTS `LEFT SEMI JOIN ON true` must lower/execute — verify in Task 7 (the parity test exercises it end-to-end). If the exec layer rejects a semi join with a constant ON, fall back: keep the inner intact and use the inner's first column `IS NOT NULL`-free semi (or document and route uncorrelated EXISTS to legacy via `Ok(false)` in Task 2). Decide based on the Task 7 result.



