# Apply / CorrelatedSubquery M1a — Scalar Apply Emission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement M1a of the Apply framework (design: `docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md`): in `apply`/`apply_strict` mode, route **scalar** subqueries (WHERE, HAVING, SELECT-list) away from the legacy analyzer rewrite and instead have the analyzer record an `ApplyScalarSpec` and the planner emit a `LogicalPlan::Apply` node. Decorrelation rules are **M1b** — so in `apply` mode a scalar subquery becomes an Apply that hits the existing `ApplyException` guard (a clean "not yet supported" error). **Default `legacy` mode behavior is unchanged.**

**Architecture:** Correlation resolution stays in the analyzer (only it has scopes). M1a reuses the *same* machinery legacy already uses for scalar subqueries — `analyze_query_in_scope_with_inner` (analyzes the inner subquery with the outer scope merged, so outer-column refs carry the outer `ColumnId`) and `extract_correlation_predicates` — but stops short of building joins. It mints the Apply `output_column`, replaces the `SubqueryPlaceholder` with a `ColumnRef` to it, and records an `ApplyScalarSpec` (clause + intact inner `ResolvedQuery` + correlation column ids + max-rows flag) on the `ResolvedSelect`. The planner wraps the block plan in `LogicalPlan::Apply` at the clause-appropriate point. The inner query is left **intact** (correlation predicates still in its WHERE, `correlation_conjuncts` empty) — M1b's `PushDownApplyFilter` extracts them.

**Tech Stack:** Rust; the existing analyzer (`src/sql/analyzer/`), planner (`src/sql/planner/`), `ColumnRefFactory`, and the M0 `ApplyNode` / `SubqueryRewrite` stage.

**Key constraints:**

1. **Zero default-mode change.** Everything is gated on `subquery_unnest_mode != Legacy`. In `legacy` mode (the default), not one code path changes. Every task must keep the full existing suite green (the 6 pre-existing iceberg/runtime_filter failures stay at 6).
2. **Reuse, don't reinvent.** M1a's analyzer path must call the *same* `analyze_query_in_scope_with_inner` + `extract_correlation_predicates` legacy uses, so it inherits legacy's proven handling of all three clauses. The only divergence: don't remove correlation predicates, don't add GROUP BY, don't build a join — emit a spec instead.
3. **Fallback is explicit.** In `apply` mode, any scalar shape M1a can't faithfully turn into a spec (e.g. correlation that doesn't resolve against the available scope) **falls back to the legacy rewrite for that subquery**. In `apply_strict` mode the same shape **errors** (so CI can see the coverage frontier). Never silently drop a subquery.
4. **Apply is M1a's terminal state.** In `apply` mode a scalar query's Apply reaches `ApplyException` and errors. That is correct for M1a. Tests assert this, not end-to-end scalar results (those are M1b).
5. English comments/identifiers/errors; commit messages English, **no `Co-Authored-By` trailer**; stay on branch `claude/apply-subquery-m1-scalar`; push to `fork`, PR fork→upstream.

---

### Task 1: `ApplyScalarSpec` / `ApplyClause` types + `ResolvedSelect.apply_specs` field

**Files:**
- Modify: `src/sql/analysis/mod.rs`

- [ ] **Step 1: Add the spec types** near `SubqueryInfo` (around `src/sql/analysis/mod.rs:425`):

```rust
/// Which clause of the enclosing SELECT a scalar subquery was found in.
/// Determines where the planner inserts the Apply node relative to the
/// WHERE filter, the aggregate, and the projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyClause {
    Where,
    Having,
    Projection,
}

/// A scalar subquery the analyzer routed to the Apply framework (apply mode).
/// The planner consumes these to emit `LogicalPlan::Apply`. The inner query is
/// left INTACT — correlation predicates remain in its WHERE; M1b's
/// PushDownApplyFilter rule extracts them into the Apply's correlation_conjuncts.
#[derive(Clone, Debug)]
pub(crate) struct ApplyScalarSpec {
    /// Placeholder id this spec replaced (matches the original SubqueryInfo.id).
    pub subquery_id: usize,
    pub clause: ApplyClause,
    /// Fresh column representing the subquery's scalar value in outer exprs.
    pub output_column: OutputColumn,
    /// Fully-analyzed inner subquery, with outer references carrying the outer
    /// column ids (via merged-scope analysis). Becomes the Apply's right child.
    pub inner: ResolvedQuery,
    /// Outer columns referenced inside the subquery (their ids are the outer
    /// factory's ids, since the outer scope was merged into the inner analysis).
    pub correlation_column_ids: Vec<ColumnId>,
    /// Scalar subqueries must yield <= 1 row; M1b discharges this when the inner
    /// is a scalar aggregate grouped by the correlation key.
    pub need_check_max_rows: bool,
    /// Original subquery SQL text, for the M1b AssertOneRow runtime message.
    pub subquery_text: String,
}
```

- [ ] **Step 2: Add the field to `ResolvedSelect`** (around `src/sql/analysis/mod.rs:59`), after `repeat`:

```rust
    /// Scalar subqueries routed to the Apply framework (apply mode only;
    /// always empty in legacy mode). Consumed by the planner to emit
    /// `LogicalPlan::Apply`.
    pub apply_specs: Vec<ApplyScalarSpec>,
```

- [ ] **Step 3: Build to find all `ResolvedSelect` construction sites**

Run: `cargo build 2>&1 | grep -A2 "missing field .apply_specs" | head -40`
Expected: a list of `ResolvedSelect { ... }` literals missing the field. Add `apply_specs: Vec::new(),` to each (they're in `src/sql/analyzer/mod.rs` and `src/sql/analyzer/subquery_rewrite.rs`; there may be a few). Do NOT add it via `..Default::default()` — `ResolvedSelect` is not `Default`; set it explicitly to `Vec::new()`.

- [ ] **Step 4: Build green**

Run: `cargo build 2>&1 | tail -2`
Expected: success. (No behavior change yet — the field is unused.)

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(analyzer): add ApplyScalarSpec and ResolvedSelect.apply_specs

Carries scalar subqueries routed to the Apply framework: clause, minted
output column, intact inner ResolvedQuery, correlation column ids, and the
max-rows guard flag. Always empty in legacy mode; populated by apply mode
in a later task. No behavior change."
```

---

### Task 2: `subquery_unnest_mode` access helper

**Files:**
- Modify: `src/sql/analyzer/mod.rs`

The analyzer runs inside `with_session_optimizer_settings` (verified: `src/server/mod.rs::execute_sql_in_worker` installs the TLS on the same thread the analyzer runs on), so `current_session_optimizer_settings()` is readable at analyzer time.

- [ ] **Step 1: Add a private helper** on `AnalyzerContext` (or a free fn in `analyzer/mod.rs`):

```rust
/// The active subquery-unnesting mode for this statement. Reads the
/// thread-local session settings installed by the server before execution.
fn subquery_unnest_mode() -> crate::sql::optimizer::options::SubqueryUnnestMode {
    crate::sql::optimizer::options::current_session_optimizer_settings().subquery_unnest_mode
}
```

- [ ] **Step 2: Remove the `#[allow(dead_code)]`** on `SessionOptimizerSettings.subquery_unnest_mode` in `src/sql/optimizer/options.rs` (it is now read). If `SubqueryUnnestMode` or its variants still warn unused, keep their allows for the not-yet-used `ApplyStrict` distinction only as needed; prefer removing allows that are now satisfied.

- [ ] **Step 3: Build green**

Run: `cargo build 2>&1 | tail -2`
Expected: success, no new warnings about `subquery_unnest_mode` being unused.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(analyzer): read subquery_unnest_mode at analysis time

Helper to consult the session's subquery-unnesting mode (plumbed in M0)
from inside the analyzer, where the legacy-vs-apply routing fork lives."
```

---

### Task 3: Analyzer routing fork — collect scalar Apply specs

This is the core analyzer change. Refactor the subquery-rewrite hook so that in `apply`/`apply_strict` mode, **scalar** subqueries become `ApplyScalarSpec`s while non-scalar (EXISTS/IN) subqueries still go through the legacy rewrite.

**Files:**
- Modify: `src/sql/analyzer/subquery_rewrite.rs`
- Modify: `src/sql/analyzer/mod.rs` (the hook site, if needed)

Reference (read before editing): the legacy `rewrite_subqueries` (`subquery_rewrite.rs:63`), `rewrite_scalar_subquery` (`:1546`), `build_correlated_scalar_subquery_from_resolved` (`:1977`), `analyze_query_in_scope_with_inner` (`:1705`), `extract_correlation_predicates` (`:2597`), and how the placeholder is replaced in `filter`/`having`/`projection` (the substitution helpers `rewrite_scalar_subquery` already uses). Also note the existing clause detection in `rewrite_subqueries` (the `in_filter`/`in_having` checks that scan `select.filter`/`select.having` for the placeholder id).

- [ ] **Step 1: Write a failing analyzer unit test** in `src/sql/analyzer/mod.rs` tests module. It analyzes a correlated WHERE-clause scalar subquery in `apply` mode and asserts an `ApplyScalarSpec` was produced (not a join). Use the existing test harness pattern in that module (find an existing `fn analyze_*` test for the setup helpers — catalog, `analyze()` call). Wrap the analyze call in `with_session_optimizer_settings` to set apply mode:

```rust
    #[test]
    fn apply_mode_routes_where_scalar_subquery_to_apply_spec() {
        use crate::sql::optimizer::options::{
            with_session_optimizer_settings, SessionOptimizerSettings, SubqueryUnnestMode,
        };
        use crate::sql::analysis::{ApplyClause, QueryBody};

        // Reuse whatever catalog/table fixture the neighboring analyzer tests use.
        let sql = "SELECT v1 FROM t0 WHERE v1 = (SELECT max(v2) FROM t1 WHERE t1.k = t0.v1)";
        let query = parse_single_query(sql); // use the module's existing parse helper
        let settings = SessionOptimizerSettings {
            subquery_unnest_mode: SubqueryUnnestMode::Apply,
            ..Default::default()
        };
        let (resolved, _cte, _factory) = with_session_optimizer_settings(settings, || {
            crate::sql::analyzer::analyze(&query, &test_catalog(), "test")
        })
        .expect("analyze in apply mode");

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.apply_specs.len(), 1, "one scalar apply spec");
        let spec = &select.apply_specs[0];
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.need_check_max_rows);
        assert!(
            !spec.correlation_column_ids.is_empty(),
            "correlated subquery must record outer column ids"
        );
        // The placeholder must be gone from the WHERE predicate, replaced by a
        // ColumnRef to the spec's output column.
        // (Assert the filter contains no SubqueryPlaceholder; see Step 5 helper.)
    }
```

Adapt `parse_single_query` / `test_catalog` to the actual helpers used by existing tests in this module (read them first; do NOT invent new fixtures if the module already has them).

Run: `cargo test --lib -- apply_mode_routes_where_scalar_subquery_to_apply_spec`
Expected: FAIL (apply mode not implemented; spec list empty).

- [ ] **Step 2: Refactor the rewrite entry to partition by mode + kind.** In `rewrite_subqueries` (`subquery_rewrite.rs:63`), after draining `collected_subqueries`, branch:

```rust
let mode = subquery_unnest_mode(); // from Task 2 (import or qualify)
for sq_info in subqueries {
    let route_to_apply = !matches!(mode, SubqueryUnnestMode::Legacy)
        && matches!(sq_info.kind, SubqueryKind::Scalar);
    if route_to_apply {
        match self.collect_scalar_apply_spec(select, scope, &sq_info) {
            Ok(true) => continue, // spec recorded; placeholder replaced
            Ok(false) => { /* fall through to legacy below */ }
            Err(e) => {
                if matches!(mode, SubqueryUnnestMode::ApplyStrict) {
                    return Err(e);
                }
                // apply (non-strict): fall back to legacy for this subquery
            }
        }
    }
    // legacy path (unchanged): JOIN-ON detection then rewrite_single_subquery
    let in_filter = /* existing */;
    let in_having = /* existing */;
    if !in_filter && !in_having && let Some(from) = select.from.as_mut() {
        if self.rewrite_subquery_in_relation(from, scope, &sq_info)? { continue; }
    }
    self.rewrite_single_subquery(select, scope, sq_info)?;
}
```

Keep the existing loop body verbatim for the legacy path; only add the `route_to_apply` branch in front. `Ok(false)` means "this scalar shape isn't M1a-supported, use legacy" (e.g. uncorrelated is fine; a shape whose inner analysis fails returns `Err`).

- [ ] **Step 3: Implement `collect_scalar_apply_spec`.** New method on `AnalyzerContext` in `subquery_rewrite.rs`. It mirrors the front half of `rewrite_scalar_subquery` but emits a spec instead of a join:

```rust
/// Apply-mode handling of a scalar subquery. Returns Ok(true) if a spec was
/// recorded (and the placeholder replaced), Ok(false) if the shape should fall
/// back to the legacy rewrite, Err on a hard analysis failure.
fn collect_scalar_apply_spec(
    &self,
    select: &mut ResolvedSelect,
    scope: &mut AnalyzerScope,
    sq_info: &SubqueryInfo,
) -> Result<bool, String> {
    // 1. Determine the clause by scanning for the placeholder id (same approach
    //    as the legacy in_filter/in_having checks). Order: Where, Having, else
    //    Projection. If the placeholder is inside a JOIN-ON or elsewhere we don't
    //    place yet, return Ok(false) (legacy fallback).
    let clause = match self.locate_scalar_placeholder_clause(select, sq_info.id) {
        Some(c) => c,
        None => return Ok(false),
    };

    // 2. Re-analyze the inner subquery with the merged outer scope (SAME call
    //    legacy uses). Outer refs inside it now carry outer ColumnIds.
    let (resolved_sub, inner_scope) =
        self.analyze_query_in_scope_with_inner(&sq_info.subquery, scope)?;
    if resolved_sub.output_columns.len() != 1 {
        return Err("scalar subquery must produce exactly one output column".into());
    }

    // 3. Extract correlation column ids WITHOUT modifying the inner query.
    let corr_ids = collect_correlation_column_ids(&resolved_sub, &inner_scope, scope);

    // 4. Mint the output column from the inner's single output type (nullable).
    let inner_out = &resolved_sub.output_columns[0];
    let name = format!("__scalar_sq_{}", sq_info.id);
    let output_id = self.factory.borrow_mut().create(
        None, name.clone(), inner_out.data_type.clone(), true,
    );
    let output_column = OutputColumn {
        column_id: output_id, name: name.clone(),
        data_type: inner_out.data_type.clone(), nullable: true, is_internal: true,
    };

    // 5. Replace the placeholder (in filter/having/projection) with a ColumnRef
    //    to output_column. Reuse the placeholder-substitution helper that
    //    rewrite_scalar_subquery uses for its ColumnRef replacement.
    let replacement = TypedExpr {
        kind: ExprKind::ColumnRef { column_id: output_id, qualifier: None, column: name },
        data_type: inner_out.data_type.clone(),
        nullable: true,
    };
    self.replace_scalar_placeholder(select, sq_info.id, &replacement);

    // 6. Record the spec (inner left INTACT; correlation_conjuncts stays empty —
    //    M1b's PushDownApplyFilter extracts it).
    select.apply_specs.push(ApplyScalarSpec {
        subquery_id: sq_info.id,
        clause,
        output_column,
        inner: resolved_sub,
        correlation_column_ids: corr_ids,
        need_check_max_rows: true,
        subquery_text: sq_info.subquery.to_string(),
    });
    Ok(true)
}
```

Implement the two helpers used above:
- `locate_scalar_placeholder_clause(select, id) -> Option<ApplyClause>`: scan `select.filter` → `Where`, else `select.having` → `Having`, else any `select.projection[*].expr` → `Projection`, else `None`. Use a small recursive `expr_contains_placeholder(&TypedExpr, id)` walker (or reuse an existing one if present).
- `replace_scalar_placeholder(select, id, &replacement)`: walk `filter`, `having`, and each `projection[*].expr`, replacing `ExprKind::SubqueryPlaceholder { id: this_id, .. }` with `replacement.clone()`. Reuse the existing substitution helper `rewrite_scalar_subquery` calls if one exists; otherwise add a small recursive replacer.
- `collect_correlation_column_ids(resolved_sub, inner_scope, outer_scope) -> Vec<ColumnId>`: if the inner body is a Select with a filter, run the existing `extract_correlation_predicates(filter, inner_scope, outer_scope)` and, for each `CorrelationPred.outer_col`, collect **every** `ExprKind::ColumnRef` `column_id` appearing within that expression (the outer side can be a wrapped expr like `coalesce(l.k, 2)`, not just a bare ref — recurse), deduped across all preds. Empty if uncorrelated. (A small `collect_column_ids(&TypedExpr, &mut Vec<ColumnId>)` walker; reuse one if the analyzer already has it.)

- [ ] **Step 4: Run the Task 3 test**

Run: `cargo test --lib -- apply_mode_routes_where_scalar_subquery_to_apply_spec`
Expected: PASS.

- [ ] **Step 5: Add a "placeholder replaced" assertion helper + a legacy-unchanged test**

Add a second test asserting legacy mode still rewrites the same query to a join (no `apply_specs`):

```rust
    #[test]
    fn legacy_mode_still_rewrites_scalar_subquery_to_join() {
        // same SQL, default (Legacy) settings — no with_session override
        let (resolved, _cte, _factory) =
            crate::sql::analyzer::analyze(&parse_single_query(SQL), &test_catalog(), "test")
                .expect("analyze legacy");
        let QueryBody::Select(select) = &resolved.body else { panic!() };
        assert!(select.apply_specs.is_empty(), "legacy must not record apply specs");
        // and the FROM should now contain a join (the legacy rewrite result)
        assert!(matches!(select.from, Some(crate::sql::analysis::Relation::Join(_))));
    }
```

Run: `cargo test --lib -- apply_mode_routes legacy_mode_still_rewrites`
Expected: both PASS.

- [ ] **Step 6: Full suite regression check**

Run: `cargo test --lib 2>&1 | grep -E "^test result" | tail -1`
Expected: failures still exactly 6 (pre-existing). Pass count rises by the new tests.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(analyzer): route scalar subqueries to ApplyScalarSpec in apply mode

In apply/apply_strict mode, scalar subqueries are recorded as
ApplyScalarSpec (clause + intact inner ResolvedQuery + correlation column
ids) instead of being rewritten into joins; the placeholder is replaced
with a ColumnRef to the minted output column. Non-scalar (EXISTS/IN)
subqueries and unsupported scalar shapes fall back to the legacy rewrite
(apply_strict errors instead). Legacy mode is unchanged. Reuses the same
analyze_query_in_scope_with_inner + extract_correlation_predicates the
legacy path uses, so the inner query (with outer-id correlation refs) is
identical — it is just left intact for M1b's push-down rules."
```

---

### Task 4: Planner — emit `LogicalPlan::Apply` from specs at the clause-appropriate point

**Files:**
- Modify: `src/sql/planner/mod.rs`

Reference: `plan_select_scoped` (`mod.rs:689`) build order is FROM → Filter(WHERE) → Repeat → Aggregate → Filter(HAVING) → Window/Project → Distinct. Insert Apply wrapping at three points so each scalar's `output_column` is produced just below its consumer.

- [ ] **Step 1: Write a failing planner unit test** in `src/sql/planner/mod.rs` tests module: feed a `ResolvedSelect` carrying one `Where`-clause `ApplyScalarSpec` (with a trivial inner `ResolvedQuery`, e.g. a `Values`-backed select) through `plan_select_scoped`/`plan_query`, and assert the resulting `LogicalPlan` contains an `Apply` whose `left` is below the WHERE `Filter` and whose `output_column` matches the spec. Use the module's existing plan-construction test helpers. Model the inner `ResolvedQuery` on the simplest existing test fixture in this module.

Run it; expect FAIL (planner ignores `apply_specs`).

- [ ] **Step 2: Add the wrapping helper** to `src/sql/planner/mod.rs`:

```rust
/// Wrap `input` in a left-deep chain of `LogicalPlan::Apply` nodes, one per
/// spec whose clause matches `clause`. Each Apply's right child is the planned
/// inner subquery. Specs are consumed (removed) from `specs`.
fn wrap_scalar_applies(
    input: LogicalPlan,
    specs: &mut Vec<ApplyScalarSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory)?;
        current = LogicalPlan::Apply(ApplyNode {
            left: Box::new(current),
            right: Box::new(right),
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: spec.output_column.column_id,
                    qualifier: None,
                    column: spec.output_column.name.clone(),
                },
                data_type: spec.output_column.data_type.clone(),
                nullable: true,
            },
            output_column: spec.output_column,
            correlation_column_ids: spec.correlation_column_ids,
            correlation_conjuncts: Vec::new(),
            residual_predicate: None,
            need_check_max_rows: spec.need_check_max_rows,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            required_output_columns: None,
        });
    }
    *specs = remaining;
    Ok(current)
}
```

- [ ] **Step 3: Insert the three wrapping points in `plan_select_scoped`.** Take ownership of the specs at the top (`let mut apply_specs = std::mem::take(&mut select.apply_specs);`), then:
  - **WHERE**: after building the FROM `current` and **before** the `if let Some(predicate) = select.filter.take()` WHERE-Filter block, do `current = wrap_scalar_applies(current, &mut apply_specs, ApplyClause::Where, cte_registry, factory)?;`
  - **HAVING**: after the `Aggregate` node is built and **before** the HAVING `Filter` block, wrap with `ApplyClause::Having`.
  - **Projection**: **before** the `build_window_and_project(...)` call (both the aggregated and non-aggregated branches), wrap with `ApplyClause::Projection`.
  - After all three, assert none remain: `debug_assert!(apply_specs.is_empty(), "unplaced scalar apply specs: {:?}", apply_specs.iter().map(|s| s.clause).collect::<Vec<_>>());` — if any clause isn't one of the three placement points, that's a planner bug (the analyzer only tags Where/Having/Projection).

- [ ] **Step 4: Run the Task 4 test + add HAVING and Projection placement tests**

Add two more planner tests (one `Having`, one `Projection` spec) asserting the Apply sits at the right level (e.g. for `Having`, the Apply's child subtree contains the `Aggregate`; for `Projection`, the `Project` is above the `Apply`). Use `plan_output_columns` and structural matching.

Run: `cargo test --lib -- <the three planner test names>`
Expected: all PASS.

- [ ] **Step 5: Full suite regression**

Run: `cargo test --lib 2>&1 | grep -E "^test result" | tail -1`
Expected: failures still exactly 6.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(planner): emit LogicalPlan::Apply from scalar apply specs

plan_select_scoped wraps the block plan in a left-deep Apply chain at the
clause-appropriate point (WHERE specs between FROM and the WHERE filter,
HAVING specs above the aggregate, projection specs below the project), so
each scalar subquery's output column is produced just under its consumer.
The inner ResolvedQuery is planned into Apply.right via the shared
ColumnRefFactory. correlation_conjuncts/residual stay empty for M1b."
```

---

### Task 5: End-to-end routing proof (`apply_strict` → ApplyException) + golden

**Files:**
- Modify: `src/sql/optimizer/mod.rs` (tests) OR add a focused integration test where `optimize()` is reachable
- Add: `sql-tests/optimizer/subquery_apply_mode_scalar_unsupported.sql` (+ `.result`) — optional, if a SQL-level test fits the runner

- [ ] **Step 1: Add an integration test** proving the full analyze→plan→optimize path: in `apply_strict` mode (or `apply`), a scalar-subquery query reaches `optimize()` and fails with the `ApplyException` message ("subquery decorrelation failed"), confirming the Apply was constructed and routed into the SubqueryRewrite stage. If a convenient in-crate harness exists (e.g. an `engine`-level test that runs SQL through `execute_query_with_options`), use it with `with_session_optimizer_settings`. Otherwise construct the `LogicalPlan::Apply` via the planner from an analyzed query and call `optimize()` directly, asserting the error.

```rust
    #[test]
    fn apply_mode_scalar_subquery_errors_at_apply_exception() {
        // analyze "SELECT v1 FROM t0 WHERE v1 = (SELECT max(v2) FROM t1 WHERE t1.k = t0.v1)"
        // in apply mode -> plan_query -> optimize(); expect Err containing
        // "subquery decorrelation failed".
    }
```

- [ ] **Step 2: Run it**

Run: `cargo test --lib -- apply_mode_scalar_subquery_errors_at_apply_exception`
Expected: PASS (the error is the expected M1a terminal state).

- [ ] **Step 3 (optional): SQL-level golden.** If the sql-test runner can set a session variable per case, add a case that does `SET subquery_unnest_mode = 'apply_strict';` then a scalar-subquery SELECT, expecting the decorrelation-failed error via `-- @expect_error`. Record with `--record-from target`. If per-case session SET isn't supported by the runner, skip this step and note it.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(optimizer): prove scalar apply-mode routing reaches ApplyException

End-to-end analyze->plan->optimize in apply mode shows a scalar subquery
becomes an Apply that the SubqueryRewrite stage rejects with the
unsupported-shape error. This is M1a's expected terminal state; M1b adds
the decorrelation rules that make apply-mode scalar actually execute."
```

---

### Task 6: Final verification

- [ ] **Step 1: fmt + clippy**

```bash
cargo fmt
cargo clippy --lib 2>&1 | grep -iE "Apply|subquery_unnest|apply_spec|scalar_sq" | grep -v "^\s*|" | head
```
Expected: no new clippy lints attributable to M1a code. Fix any; don't blanket-`allow`.

- [ ] **Step 2: full build + test**

```bash
cargo build && cargo test --lib 2>&1 | grep -E "^test result" | tail -1
```
Expected: failures still exactly 6 (pre-existing); pass count up by the M1a tests.

- [ ] **Step 3: legacy-mode suite sanity (optional, needs env).** Run the `optimizer` and `join` suites in default (legacy) mode; expect no golden changes (M1a changes nothing in legacy mode).

- [ ] **Step 4: fmt fixup commit if needed**

```bash
git add -A && git diff --cached --quiet || git commit -m "style: cargo fmt for M1a scalar apply emission"
```

---

## Acceptance checklist (maps to design §7.3 M1, scalar-emission half)

- [covered] `subquery_unnest_mode` consumed by analyzer routing (Task 2/3).
- [covered] Scalar subqueries (WHERE/HAVING/SELECT-list) become `ApplyScalarSpec` → `LogicalPlan::Apply` in apply mode (Tasks 3/4); inner query left intact for M1b.
- [covered] Correlation represented as outer `ColumnId`s via the existing merged-scope analysis (Task 3).
- [covered] Non-scalar + unsupported scalar shapes fall back to legacy (apply) or error (apply_strict) — no silent drops (Task 3).
- [covered] Zero legacy-mode change; suite stays green (every task).
- [covered] Apply reaches ApplyException in apply mode — routing proven (Task 5).

## Out of scope (M1b — separate plan)

PushDownApplyProject/Filter/AggFilter/AggProjectFilter, NormalizeCountScalarApply, ScalarApplyToJoin, AssertOneRow production from the uncorrelated case, and making `apply`-mode scalar subqueries actually execute (tpc-h q2/q17 correctness, multi-row error, empty-group/NULL-key cases). Those consume the `ApplyNode` this plan emits.

## Known open risk (design §9 item 6)

A HAVING/projection scalar correlating on **post-aggregate** columns (group keys or aggregate outputs) may not resolve against the FROM-based outer scope used by `analyze_query_in_scope_with_inner`. M1a handles this conservatively: if `collect_scalar_apply_spec` can't produce a faithful spec (inner analysis error or correlation that doesn't resolve), it returns `Err`/`Ok(false)` → legacy fallback (apply) or hard error (apply_strict). `apply_strict` in CI is how we map the real coverage frontier before deciding (in a later task) whether to support it directly.
