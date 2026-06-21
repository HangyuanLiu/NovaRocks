# Apply / CorrelatedSubquery M1b — Scalar Decorrelation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `apply`-mode **scalar** subqueries actually execute by adding decorrelation rules to the `SubqueryRewrite` stage that consume the `LogicalPlan::Apply` (kind Scalar) that M1a emits and rewrite it into joins + (vector) aggregate + an at-most-one-row guard. Scope: **all three** scalar shapes — uncorrelated, correlated-aggregate (q2/q17), and correlated-non-aggregate (with runtime row-check) — finishing M1's scalar decorrelation. The result in `apply` mode must match `legacy` mode (and `default` legacy is unchanged).

**Architecture:** New `LogicalRewriteRule`s in `src/sql/optimizer/rewrite/rules/subquery/`, registered in `subquery_rewrite_rules()` BEFORE `ApplyException` (the terminal guard). They run to fixpoint in the `SubqueryRewrite` stage. The pipeline runs rules in vec order each iteration; once a rule removes the Apply, `ApplyException` no-ops. Rules port StarRocks `PushDownApplyAggFilterRule` + `PushDownApplyFilterRule` + `ScalarApply2JoinRule` (the to-join, NOT to-window, paths; to-window/WinMagic is M2). The emitted `ApplyNode` from M1a has the inner subquery **intact** (correlation predicates still in its WHERE) and `correlation_column_ids` recording the outer columns referenced — exactly the input these rules need.

**Tech Stack:** Rust; the existing rewrite framework (`LogicalRewriteRule`/`RewritePipeline`), `ColumnRefFactory` (mint columns via `ctx.column_ref_factory()`), the M0 `AssertOneRow` node (logical→physical→codegen chain already exists), and `utils::{split_and, combine_and, collect_column_id_refs}` (already present).

## Scope and the deferred sub-case

**This plan finishes ALL of M1's scalar decorrelation in one pass (no M1c split).** Three scalar shapes:
- **Uncorrelated scalar** → CROSS JOIN with the inner; wrap the inner in `AssertOneRow` unless it is provably ≤1 row (a scalar aggregate); Project maps the Apply `output_column` to the inner's single output column.
- **Correlated *aggregate* scalar** (inner is a global `Aggregate` with empty group_by over a `Filter` holding the correlation, optionally under a `Project`) → vector aggregate grouped by the correlation key + LEFT OUTER JOIN on the de-correlated equality + Project mapping `output_column` to the inner agg result (with `count`→`ifnull(count,0)` normalization). This is tpc-h q2 (`min`) / q17 (`avg`).
- **Correlated *non-aggregate* scalar** (inner returns possibly-many rows per outer key; needs a runtime per-group ≤1-row check). → an Aggregate(GROUP BY inner-correlation-cols, `count(1) AS cnt`, `any_value(innerScalar) AS anyval`) + LEFT OUTER JOIN on the de-correlated equality + Project mapping `output_column` to `anyval` and adding `assert_true(cnt IS NULL OR cnt <= 1, 'correlate scalar subquery result must 1 row')`. Mirrors StarRocks `ScalarApply2JoinRule.transformCorrelateWithCheckOneRows`.

**`assert_true` (resolved):** NovaRocks's exec kernel ALREADY supports 2 args (`src/exec/expr/function/conditional/assert_true.rs`: `min_args:1, max_args:2`; the 2nd arg becomes the error message; FALSE → `Err(message)`, NULL → `Err("assert_true failed due to null value")`). Only the SQL-layer registry registers a 1-arg signature. Task 1 adds the 2-arg `assert_true(bool, varchar) -> bool` signature (one line) so the with-check path can emit the StarRocks message. Our condition `cnt IS NULL OR cnt <= 1` is never NULL (the `IS NULL` disjunct covers the no-match LEFT-OUTER case), so the null-error path is not triggered.

**Still out of scope (separate plans):** Non-EQ correlation is rejected with an explicit error (matches StarRocks `EXIST_NON_EQ_PREDICATE`). WinMagic (scalar→window, the q2/q17 plan-compactness optimization) is **M2**. EXISTS/IN decorrelation is **M3**.

**Key constraints:**
1. **`apply`-mode scalar result == `legacy`-mode result** for the in-scope shapes (the correctness bar). Default `legacy` mode is untouched.
2. Branch baseline `cargo test --lib` = **0 failed**. Every task keeps it at 0.
3. Rules port StarRocks faithfully but emit NovaRocks `LogicalPlan` nodes. Do NOT hardcode anything for q2/q17.
4. English code/comments/errors; commit messages English, **no `Co-Authored-By` trailer**; stay on branch `claude/apply-subquery-m1-scalar`.

---

## Construction toolbox (verbatim, from research — use these exactly)

**Mint a column inside a rule** (model: `aggregate_pushdown/rule.rs:49-52`):
```rust
let factory = ctx
    .column_ref_factory()
    .ok_or_else(|| "<RuleName> requires ColumnRefFactory".to_string())?;
let mut factory = factory.borrow_mut();
let new_id = factory.create(None, "<display name>".to_string(), data_type.clone(), nullable);
```

**Conjunct split/combine + column-ref test** (already in `src/sql/optimizer/rewrite/rules/utils.rs`):
```rust
pub(crate) fn split_and(expr: TypedExpr) -> Vec<TypedExpr>            // utils.rs:12
pub(crate) fn combine_and(exprs: Vec<TypedExpr>) -> TypedExpr         // utils.rs:34 (panics if empty)
pub(crate) fn collect_column_id_refs(expr: &TypedExpr) -> HashSet<ColumnId>  // utils.rs:272
```
"is this conjunct correlated?" = `!collect_column_id_refs(&c).is_disjoint(&corr_ids)`.

**Node structs** (`src/sql/planner/plan.rs`): `AggregateNode { input, group_by: Vec<TypedExpr>, aggregates: Vec<AggregateCall>, output_columns: Vec<OutputColumn>, already_pushed, required_output_columns }`; `AggregateCall { name, args: Vec<TypedExpr>, distinct, result_type, order_by, output_column_id }`; `JoinNode { left, right, join_type: JoinKind, condition: Option<TypedExpr>, required_output_columns }`; `FilterNode { input, predicate, required_output_columns }`; `ProjectNode { input, items: Vec<ProjectItem>, output_qualifier, required_output_columns }`; `ProjectItem { expr, output_name, output_column_id }`; `AssertOneRowNode { input, subquery_text, required_output_columns }`. `ApplyNode` fields per M1a.

**Expr** (`src/sql/analysis/mod.rs`): `BinOp` = `Add,Sub,Mul,Div,Mod,Eq,Ne,Lt,Le,Gt,Ge,EqForNull,And,Or`. `ExprKind::ColumnRef { column_id, qualifier, column }`, `BinaryOp { left, op, right }`, `FunctionCall { name, args, distinct }`, `Literal(LiteralValue::Int(i64))`. `ifnull(x, 0)` = `FunctionCall { name: "ifnull", args: [x, Literal(Int(0))], distinct: false }` (type widened — set `data_type` to the count's `Int64`/`BIGINT`).

**`count(1)` AggregateCall:** `AggregateCall { name: "count", args: vec![TypedExpr{ kind: Literal(LiteralValue::Int(1)), data_type: Int64, nullable: false }], distinct: false, result_type: Int64, order_by: vec![], output_column_id: <minted> }`. **`any_value(col)`:** `name: "any_value", args: vec![<col ref>], result_type: <col type>`.

**Apply.right shapes M1a produces** (research-confirmed):
- correlated agg: `[Project?] Aggregate{group_by:[]}( Filter{pred: inner.k == outer.k AND ...}( Scan ) )` — the Filter's correlated conjunct references an **outer** `ColumnId` ∈ `correlation_column_ids`.
- uncorrelated: `Aggregate{group_by:[]}( Scan )` (no Filter referencing outer ids), or a non-agg `[Project] (Scan/...)`.

**Join output + the output_column mapping (research item F):** `join_output_columns(LeftOuter, left, right)` = `left ++ make_nullable(right)`. The inner agg's single output column has the **inner** id; the Apply's `output_column` has a **distinct minted** id. So every `ScalarApplyToJoin` sub-case MUST wrap the join in a `Project` whose items are: all left columns passed through, plus one `ProjectItem { output_column_id: apply.output_column.column_id, expr: ColumnRef(inner_scalar_output_id) }` (with `count`→`ifnull` wrapping when applicable). `plan_output_columns(Apply)` returns `left ++ [output_column]`, so the Project must reproduce exactly that.

**Pipeline ordering (research item E):** within the `SubqueryRewrite` stage, rules run in vec order each iteration to fixpoint (`pipeline.rs`); BottomUp default traversal. Order the vec: `[PushDownApplyAggFilter, ScalarApplyToJoin, ApplyException]`. PushDownApplyAggFilter must fire (and set `need_check_max_rows=false` + populate `correlation_conjuncts`) before ScalarApplyToJoin's correlated-without-check arm matches; ApplyException stays LAST and no-ops once the Apply is gone.

---

### Task 1: ApplyNode inner-output-id field + rule module scaffolding + correlated-conjunct helper

**Files:**
- Modify: `src/sql/planner/plan.rs` (add `ApplyNode.inner_output_column_id`)
- Modify: `src/sql/planner/mod.rs` (set it in `wrap_scalar_applies`)
- Create: `src/sql/optimizer/rewrite/rules/subquery/decorrelate_util.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`

> **Why the new field (correctness, not convenience):** the Apply's scalar value is the inner subquery's single output column. For the uncorrelated case `plan_output_columns(&apply.right)[0]` is that column. But `PushDownApplyAggFilter` (Task 2) adds correlation columns as group-by keys, and `AggregateNode.output_columns` is "group keys first, then agg results" — so post-pushdown `[0]` is a group key, not the scalar result. The inner scalar output column id is stable across pushdown (the original aggregate keeps its `output_column_id`), so we capture it once at M1a emission time and read it in Task 3.

- [ ] **Step 1: Add the field to `ApplyNode`** (`src/sql/planner/plan.rs`): `pub inner_output_column_id: ColumnId,` with a doc comment ("the inner subquery's single scalar output column; the Apply's output_column is mapped to this after decorrelation"). Add it to the M0 `#[allow(dead_code)]` set if needed (it's read by M1b's ScalarApplyToJoin; drop the allow when that lands — or just leave the struct-level allow that M0 already has).

- [ ] **Step 2: Set it in M1a's `wrap_scalar_applies`** (`src/sql/planner/mod.rs`): after planning `let right = plan_scoped_query(spec.inner, cte_registry, factory)?;`, capture `let inner_output_column_id = plan_output_columns(&right)?.first().map(|c| c.column_id).ok_or_else(|| "scalar subquery inner has no output column".to_string())?;` and set `inner_output_column_id` in the `ApplyNode { ... }` literal. (At emission, before any pushdown, the inner's output is the single scalar column.) Update any other `ApplyNode { .. }` construction sites (e.g. the M1a planner test fixtures and the M0 optimizer test in `rules/subquery/mod.rs`) to set the new field — the compiler lists them.

- [ ] **Step 3: Build green** (`cargo build`); the field is set but not yet read — fine.

- [ ] **Step 4: Write a failing unit test** in a new `decorrelate_util.rs` `#[cfg(test)]` module for a helper `partition_conjuncts(predicate, &corr_ids) -> (Vec<TypedExpr> /*correlated*/, Vec<TypedExpr> /*residual*/)`:

```rust
#[test]
fn partition_splits_correlated_and_residual() {
    // pred = (inner.k == OUTER) AND (inner.v > 5)
    // corr_ids = {OUTER}
    // expect correlated=[inner.k==OUTER], residual=[inner.v>5]
}
```
(Construct `TypedExpr`s inline; `OUTER`/`inner.k` are `ColumnRef`s with distinct `ColumnId`s; reuse the construction style from `utils.rs` tests.)

Run: `cargo test --lib -- partition_splits_correlated_and_residual` → FAIL (helper absent).

- [ ] **Step 5: Implement `decorrelate_util.rs`:**

```rust
//! Shared helpers for scalar subquery decorrelation rules.
use std::collections::HashSet;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};

/// Split a predicate's AND-conjuncts into (correlated, residual): a conjunct is
/// correlated iff it references any column id in `corr_ids` (an outer column).
pub(super) fn partition_conjuncts(
    predicate: TypedExpr,
    corr_ids: &HashSet<ColumnId>,
) -> (Vec<TypedExpr>, Vec<TypedExpr>) {
    let mut correlated = Vec::new();
    let mut residual = Vec::new();
    for c in split_and(predicate) {
        if collect_column_id_refs(&c).is_disjoint(corr_ids) {
            residual.push(c);
        } else {
            correlated.push(c);
        }
    }
    (correlated, residual)
}

/// True iff every conjunct is a binary `=` comparison (the only correlation
/// shape decorrelation supports; mirrors StarRocks checkAllIsBinaryEQ).
pub(super) fn all_binary_eq(conjuncts: &[TypedExpr]) -> bool {
    conjuncts.iter().all(|c| matches!(&c.kind, ExprKind::BinaryOp { op: BinOp::Eq, .. }))
}

/// For a correlated EQ conjunct `a == b`, return (outer_side, inner_side) by
/// testing which side references an outer (corr) id. The inner side becomes a
/// GROUP BY key; the outer side becomes the join-condition outer operand.
pub(super) fn orient_eq<'a>(
    conjunct: &'a TypedExpr,
    corr_ids: &HashSet<ColumnId>,
) -> Option<(&'a TypedExpr, &'a TypedExpr)> {
    let ExprKind::BinaryOp { left, op: BinOp::Eq, right } = &conjunct.kind else { return None; };
    let left_outer = !collect_column_id_refs(left).is_disjoint(corr_ids);
    let right_outer = !collect_column_id_refs(right).is_disjoint(corr_ids);
    match (left_outer, right_outer) {
        (true, false) => Some((left, right)),   // (outer, inner)
        (false, true) => Some((right, left)),
        _ => None, // both/neither outer → not a clean correlation key
    }
}
```

(Adjust to the real `ExprKind::BinaryOp`/`BinOp` shapes; the research confirms these names.)

- [ ] **Step 6: declare the module** in `subquery/mod.rs`: `mod decorrelate_util;`

- [ ] **Step 7:** Run the test → PASS. `cargo test --lib 2>&1 | grep "^test result"` → 0 failed.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(optimizer): scalar decorrelation scaffolding

Add ApplyNode.inner_output_column_id (the inner scalar output column,
captured at emission and stable across pushdown) set by wrap_scalar_applies;
add partition_conjuncts / all_binary_eq / orient_eq decorrelation helpers
built on utils::{split_and, collect_column_id_refs}."
```

---

### Task 2: `PushDownApplyAggFilter` — correlated aggregate → vector aggregate

Ports StarRocks `PushDownApplyAggFilterRule`. Matches a Scalar Apply whose right is `[Project?] Aggregate{group_by: empty}( Filter(...) )` and whose correlation lives in that Filter; rewrites the inner to a vector aggregate grouped by the correlation key, hoists the correlated EQ conjuncts onto `Apply.correlation_conjuncts` (as `outer == inner` join predicates), keeps residual conjuncts as a Filter below the agg, and sets `need_check_max_rows = false`.

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/push_down_apply_agg_filter.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`

- [ ] **Step 1: Write failing unit test** building an `ApplyNode` (correlated scalar agg, `right = Aggregate{group_by:[], aggregates:[max(v2)]}(Filter(t2.k==OUTER, Scan t2))`, `correlation_column_ids={OUTER}`, `need_check_max_rows=true`, `correlation_conjuncts=[]`), running the rule, and asserting the result Apply has: `need_check_max_rows=false`; `correlation_conjuncts=[OUTER == t2.k]`; `right` = `Aggregate{group_by:[t2.k], aggregates:[max(v2)]}(Scan t2)` (the correlated Filter removed, t2.k promoted to group key; residual filter — none here — would remain). Run → FAIL.

- [ ] **Step 2: Implement the rule.** Structure (BottomUp `LogicalRewriteRule`):
  - `matches`: `LogicalPlan::Apply(a)` where `a.kind == Scalar`, `!a.correlation_column_ids.is_empty()`, `a.need_check_max_rows` is true, and `inner_is_correlated_scalar_agg(&a.right, &corr_ids)` (a helper that peels an optional leading `Project`, requires an `Aggregate{group_by: empty}` whose `input` is a `Filter` whose predicate has ≥1 conjunct referencing `corr_ids`).
  - `apply`:
    1. destructure the inner to `(leading_project, agg, filter)`.
    2. `corr_ids = a.correlation_column_ids.iter().copied().collect()`.
    3. `(correlated, residual) = partition_conjuncts(filter.predicate, &corr_ids)`.
    4. if `correlated.is_empty()` → `Err("correlated subquery without correlation predicate is not supported")`; if `!all_binary_eq(&correlated)` → `Err("non-EQ correlated predicate in correlated subquery is not supported")`.
    5. For each correlated EQ conjunct, `orient_eq` → `(outer, inner)`. Collect the distinct `inner` expressions as new group-by keys; collect the conjuncts themselves (already `outer == inner`) as the `correlation_conjuncts` to set on the Apply. (The inner side as written is a `ColumnRef` to an inner column for the common case; if it is a non-column expr, that's an M1c concern — for M1b require the inner side be a `ColumnRef` else `Ok(RewriteResult::Unchanged)` to fall back.)
    6. Rebuild inner: `new_agg = Aggregate{ group_by: agg.group_by ++ inner_key_exprs, aggregates: agg.aggregates, output_columns: agg.output_columns ++ <OutputColumn for each new group key>, ... , input: if residual.is_empty() { agg.input } else { Filter{ predicate: combine_and(residual), input: agg.input } } }`. The new group-key `OutputColumn`s reuse the inner key columns' ids (they already exist in the scan output — do NOT mint; use the `ColumnRef`'s `column_id`/type). Re-wrap in the leading `Project` if present (extend its items to pass through the new group-key columns so they're visible to the join condition).
    7. Return `Changed(Apply { right: new_inner, correlation_conjuncts: correlated, need_check_max_rows: false, ..a })`.
  - Note: `correlation_conjuncts` are stored as `outer == inner` `TypedExpr`s; ScalarApplyToJoin (Task 3) uses them directly as the LEFT OUTER JOIN condition (both sides' ids are in scope: outer from `Apply.left`, inner group key from the vector agg output).

- [ ] **Step 3:** register in `subquery_rewrite_rules()` (Task 4 does the wiring + ordering; for now you may add `mod push_down_apply_agg_filter;` and the `pub(crate) use`).

- [ ] **Step 4:** Run the unit test → PASS; `cargo test --lib` 0 failed; `cargo build` warning-clean.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(optimizer): PushDownApplyAggFilter decorrelates aggregate scalar Apply

Ports StarRocks PushDownApplyAggFilterRule: a correlated scalar aggregate
subquery (Aggregate over a Filter holding the correlation) becomes a vector
aggregate grouped by the correlation key, the correlated EQ conjuncts move
onto Apply.correlation_conjuncts, residual predicates stay as an inner
Filter, and need_check_max_rows is cleared. EQ-only; non-EQ / no-correlation
error explicitly."
```

---

### Task 2b: `PushDownApplyFilter` — hoist correlation for non-aggregate inner

Ports StarRocks `PushDownApplyFilterRule`. For a correlated scalar Apply whose inner is `[Project?] Filter(...)` with **no Aggregate** (the non-aggregate case), split the inner Filter's conjuncts: correlated EQ conjuncts (referencing `correlation_column_ids`) move onto `Apply.correlation_conjuncts`; residual conjuncts stay as the inner Filter (or the Filter is removed if all hoisted). `need_check_max_rows` stays `true` (no aggregate ⇒ ScalarApplyToJoin's with-check branch handles it). This is the counterpart of PushDownApplyAggFilter for inners that are NOT aggregated.

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/push_down_apply_filter.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`

- [ ] **Step 1: Write failing unit test** building a correlated non-agg Apply (`right = [Project(v2)] Filter(t2.k == OUTER AND v2 > 5)(Scan t2)`, `correlation_column_ids={OUTER}`, `correlation_conjuncts=[]`, `need_check_max_rows=true`), running the rule, asserting: `correlation_conjuncts == [OUTER == t2.k]`; `need_check_max_rows` still `true`; the inner Filter now holds only `v2 > 5` (residual) — or is gone if there were no residual; the leading `Project` (if any) is preserved. Run → FAIL.

- [ ] **Step 2: Implement the rule** (BottomUp `LogicalRewriteRule`):
  - `matches`: `LogicalPlan::Apply(a)` where `a.kind == Scalar`, `!a.correlation_column_ids.is_empty()`, and `inner_has_correlated_nonagg_filter(&a.right, &corr_ids)` — a helper that peels an optional leading `Project`, requires the next node be a `Filter` (NOT an Aggregate) whose predicate has ≥1 conjunct referencing `corr_ids`. (If the inner is `Aggregate(Filter(...))`, this rule does NOT match — PushDownApplyAggFilter owns that.)
  - `apply`:
    1. peel `(leading_project, filter)` from `a.right`.
    2. `corr_ids = a.correlation_column_ids.iter().copied().collect()`.
    3. `(correlated, residual) = partition_conjuncts(filter.predicate, &corr_ids)`.
    4. if `correlated.is_empty()` → `Ok(Unchanged)` (nothing to hoist — shouldn't happen given `matches`); if `!all_binary_eq(&correlated)` → `Err("non-EQ correlated predicate in correlated subquery is not supported")`.
    5. rebuild inner: `new_filter_input = if residual.is_empty() { *filter.input } else { LogicalPlan::Filter(FilterNode { predicate: combine_and(residual), input: filter.input, required_output_columns: None }) }`; re-wrap in `leading_project` if present (input = new_filter_input).
    6. `new_correlation = a.correlation_conjuncts ++ correlated` (AND-combine — append; ScalarApplyToJoin combines them).
    7. return `Changed(Apply { right: new_inner, correlation_conjuncts: new_correlation, ..a })` (need_check_max_rows unchanged = true).
  - Require the inner side of each correlated EQ conjunct (`orient_eq`) be a `ColumnRef` (a scan column); if not, `Ok(Unchanged)` (fall to ApplyException — non-trivial inner-side expr is out of M1 scope).

- [ ] **Step 3:** register in `subquery_rewrite_rules()` (Task 4 wires ordering); add `mod push_down_apply_filter;` + `pub(crate) use`.

- [ ] **Step 4:** unit test → PASS; `cargo build` warning-clean; `cargo test --lib` 0 failed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(optimizer): PushDownApplyFilter hoists correlation for non-agg scalar Apply

Ports StarRocks PushDownApplyFilterRule: for a correlated scalar subquery
whose inner is a Filter (no aggregate), the correlated EQ conjuncts move
onto Apply.correlation_conjuncts and residual predicates stay as an inner
Filter; need_check_max_rows stays true so ScalarApplyToJoin emits the
count(1)/any_value/assert_true row-check. EQ-only."
```

---

### Task 3: `ScalarApplyToJoin` — emit join + AssertOneRow/assert_true + output Project

Ports StarRocks `ScalarApply2JoinRule` (all three arms: uncorrelated, correlated-without-check, correlated-with-check). Folds `ScalarApplyNormalizeCount` (count→ifnull) into the output Project.

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/scalar_apply_to_join.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`
- Modify: `src/sql/functions/registry.rs` (add the 2-arg `assert_true` signature)

- [ ] **Step 0: Register the 2-arg `assert_true` signature.** In `src/sql/functions/registry.rs`, right after the existing 1-arg `assert_true` registration (~line 671), add:

```rust
// `assert_true(bool, varchar) -> bool` — 2-arg form with a custom error
// message. The exec kernel already supports 2 args (assert_true.rs uses the
// 2nd arg as the message); only the SQL-layer signature was missing.
add(
    m,
    "assert_true",
    Signature::new(vec![TypeSpec::Boolean, TypeSpec::Utf8], TypeSpec::Boolean),
);
```

Add a quick resolver/registry unit test (or extend an existing one) that `resolve_scalar_function("assert_true", &[Boolean, Utf8])` resolves to `Boolean`. `cargo test --lib` 0 failed.

- [ ] **Step 1: Write failing unit tests** (4): (a) uncorrelated scalar-agg Apply → `Project( CrossJoin( left, inner_agg ) )` with NO AssertOneRow (scalar agg is provably ≤1 row) and a ProjectItem mapping `output_column` → inner agg col; (b) uncorrelated NON-agg Apply (`right = Project(Scan)` or similar) → `Project( CrossJoin( left, AssertOneRow(inner) ) )`; (c) correlated Apply with `need_check_max_rows=false` and `correlation_conjuncts=[OUTER==t2.k]` → `Project( LeftOuterJoin(left, vector_agg, ON OUTER==t2.k) )` mapping `output_column` → inner agg col; (d) correlated Apply with `need_check_max_rows=true`, `correlation_conjuncts=[OUTER==t2.k]`, inner `[Project(v2)]Scan(t2)` (no agg) → `Project( LeftOuterJoin(left, Agg[group by t2.k]{count(1)→cnt, any_value(v2)→anyval}, ON OUTER==t2.k) )` mapping `output_column`→`anyval` and adding an `assert_true(cnt IS NULL OR cnt<=1, 'correlate scalar subquery result must 1 row')` project item. Run → FAIL.

- [ ] **Step 2: Implement the rule** (TopDown or BottomUp — BottomUp default is fine; it matches the Apply node after children). `matches`: `LogicalPlan::Apply(a)` with `a.kind == Scalar`. `apply` dispatch:
  - **uncorrelated** (`a.correlation_column_ids.is_empty()`):
    - `inner = a.right`; if NOT `inner_is_provably_le_one_row(&inner)` (scalar agg with empty group_by, possibly under a Project; or a Values with ≤1 row) wrap: `inner = AssertOneRow(AssertOneRowNode { input: inner, subquery_text: <from where? see note>, required_output_columns: None })`.
    - `join = Join(JoinNode { left: a.left, right: inner, join_type: Cross, condition: None, ... })`.
    - return `Changed(build_output_project(join, &a))` (see helper below).
  - **Correlation-not-yet-hoisted guard:** for BOTH correlated branches below, if `!a.correlation_column_ids.is_empty() && a.correlation_conjuncts.is_empty()`, return `Ok(RewriteResult::Unchanged)` — the push-down rule (PushDownApplyAggFilter or PushDownApplyFilter) hasn't run yet; let it fire first this iteration. (Mirrors StarRocks's `containsCorrelationSubquery` gate.)
  - **correlated, `!a.need_check_max_rows`** (PushDownApplyAggFilter already ran — aggregate inner):
    - `cond = combine_and(a.correlation_conjuncts.clone())`.
    - `join = Join(JoinNode { left: a.left, right: a.right, join_type: LeftOuter, condition: Some(cond), ... })`.
    - return `Changed(build_output_project(join, &a))`.
  - **correlated, `a.need_check_max_rows`** (PushDownApplyFilter already ran — non-aggregate inner; the runtime row-check case):
    - The group keys are the INNER sides of the correlation conjuncts. For each conjunct in `a.correlation_conjuncts`, `orient_eq` → `(outer, inner)`; require each `inner` be a `ColumnRef` (else `Ok(Unchanged)` → ApplyException). Collect the distinct inner key column refs `gk`.
    - **Ensure the inner exposes both the group keys and the scalar output:** the agg input is `a.right` (a `[Project] Filter?/Scan`). If a leading `Project` does NOT already output every `gk` column, extend it with pass-through `ProjectItem`s for the missing `gk`s (the scalar output `apply.inner_output_column_id` is already projected). If there is no leading Project, the input is a Scan/Filter that already exposes all columns — use it directly. Call the resulting plan `agg_input`.
    - Mint `cnt_id = factory.create(None, "count(1)", Int64, false)` and `anyval_id = factory.create(None, "any_value", <scalar type>, true)`.
    - `vector_agg = Aggregate(AggregateNode { input: agg_input, group_by: gk.iter().map(ColumnRef).collect(), aggregates: vec![ count(1) AggregateCall {output_column_id: cnt_id}, any_value(ColumnRef(inner_output_column_id)) AggregateCall {output_column_id: anyval_id} ], output_columns: <gk OutputColumns> ++ [cnt OutputColumn, anyval OutputColumn], already_pushed: false, required_output_columns: None })`.
    - `cond = combine_and(a.correlation_conjuncts.clone())` (the `outer == inner-gk` predicates; both sides in scope: outer from `a.left`, gk from the vector agg).
    - `join = Join(JoinNode { left: a.left, right: vector_agg, join_type: LeftOuter, condition: Some(cond), ... })`.
    - Output Project items: pass-through every `plan_output_columns(&a.left)?` column; `{ output_column_id: a.output_column.column_id, expr: ColumnRef(anyval_id) }`; and an internal assertion item `{ output_column_id: <minted internal id>, output_name: "__subquery_assertion", expr: assert_true( (cnt IS NULL) OR (cnt <= 1), "correlate scalar subquery result must 1 row" ) }` where the condition is `BinaryOp{ op: Or, left: IsNull{expr: ColumnRef(cnt_id), negated: false}, right: BinaryOp{op: Le, left: ColumnRef(cnt_id), right: Literal(Int(1))} }` and `assert_true` is `FunctionCall { name: "assert_true", args: [cond, Literal(LiteralValue::String("correlate scalar subquery result must 1 row"))], distinct: false }` (Boolean, non-null). Mark the assertion `OutputColumn.is_internal = true` so column pruning keeps it. Return `Changed(Project(...))`.
    - **Note on the assertion column surviving pruning:** the assertion item produces a column the outer query never references; ensure it isn't pruned away (set `is_internal: true` on its output and keep it in the Project). If the existing pruning still drops it, the fallback is to AND the assertion into the residual/filter — but prefer the internal-column approach; verify with the Task 5 multi-row test that the assert actually fires.
  - **`build_output_project(child, apply)` helper:** the inner scalar output column id is `apply.inner_output_column_id` (Task 1 — do NOT use `plan_output_columns(&apply.right)[0]`, which is a group key post-pushdown). Build items: every column of `plan_output_columns(&apply.left)?` as a pass-through `ProjectItem { output_column_id: c.column_id, expr: ColumnRef(c) }`, plus one item `{ output_column_id: apply.output_column.column_id, expr: <inner_out_expr> }` where `<inner_out_expr>` is `ColumnRef(apply.inner_output_column_id)` normally, OR `ifnull(ColumnRef(apply.inner_output_column_id), 0)` when that inner column is produced by a `count` aggregate (detect by scanning `apply.right`'s `AggregateNode.aggregates` for an `AggregateCall { name: "count", output_column_id }` equal to `apply.inner_output_column_id`). Set the Project's `output_qualifier: None`. The data type of `<inner_out_expr>` is the inner column's type (look it up in `plan_output_columns(&apply.right)?` by id); nullable=true for the LEFT OUTER / non-matching case.
  - **subquery_text for AssertOneRow:** the `ApplyNode` doesn't carry the subquery text; pass an empty string to `AssertOneRowNode { subquery_text: String::new(), .. }` (StarRocks's uncorrelated path likewise passes `""`). The runtime message is still informative ("assert_num_rows failed..."). M1c can thread real text if desired.

- [ ] **Step 3:** Run the 3 unit tests → PASS. `cargo build` warning-clean; `cargo test --lib` 0 failed.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(optimizer): ScalarApplyToJoin lowers scalar Apply to joins

Three arms: uncorrelated -> CROSS JOIN (+ AssertOneRow unless the inner is
a provably-single-row scalar aggregate); correlated aggregate (after
PushDownApplyAggFilter) -> LEFT OUTER JOIN on the de-correlated equality;
correlated non-aggregate (after PushDownApplyFilter) -> LEFT OUTER JOIN over
an Aggregate(group by correlation key, count(1)/any_value) with an
assert_true(count IS NULL OR count<=1, ...) per-group row-check. The output
Project maps the Apply output column to the inner scalar result, normalizing
count -> ifnull(count, 0). Adds the 2-arg assert_true signature."
```

---

### Task 4: Register rules in the SubqueryRewrite stage + update registry tests

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs` (tests)

- [ ] **Step 1:** Update `subquery_rewrite_rules()`:

```rust
pub(crate) fn subquery_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(push_down_apply_agg_filter::PushDownApplyAggFilter),
        Box::new(push_down_apply_filter::PushDownApplyFilter),
        Box::new(scalar_apply_to_join::ScalarApplyToJoin),
        Box::new(ApplyException), // must stay LAST
    ]
}
```
(The two push-down rules match disjoint shapes — aggregate vs non-aggregate inner — so their relative order is immaterial; both must precede `ScalarApplyToJoin`, which precedes `ApplyException`.)

- [ ] **Step 2:** Update the registry tests in `registry.rs` that assert the rule-name set / stage rules (the sorted `rule_names` list gains `"PushDownApplyAggFilter"`, `"PushDownApplyFilter"`, `"ScalarApplyToJoin"`; `is_known_rewrite_rule_name` must accept all three). Run those tests first to see them fail, then update the expected lists.

- [ ] **Step 3:** Run `cargo test --lib -- query_pipeline_uses_expected_stage_order rewrite_registry_recognizes is_known_rule_name` → PASS. Confirm the two new names are `disable_optimizer_rules`-able.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat(optimizer): register scalar decorrelation rules in SubqueryRewrite

PushDownApplyAggFilter + PushDownApplyFilter + ScalarApplyToJoin run before
the ApplyException guard; all are disable_optimizer_rules-able. Registry
tests updated."
```

---

### Task 5: SQL correctness — apply mode == legacy for in-scope shapes

**Files:**
- Add: `sql-tests/optimizer/` plan-golden cases and/or a correctness suite case (per the runner's conventions).
- Possibly add an engine-level in-crate test if the SQL runner can't set the session var per case.

- [ ] **Step 1:** Add an in-crate end-to-end test (engine or optimizer level) that runs a correlated-aggregate scalar query (q17-shape: `SELECT sum(l_ext)/7 FROM lineitem, part WHERE p_partkey=l_partkey AND ... AND l_quantity < (SELECT 0.2*avg(l_quantity) FROM lineitem WHERE l_partkey=p_partkey)`) in BOTH `legacy` and `apply` mode against the same fixture and asserts identical results. If the in-crate harness can't execute full queries, use the SQL test runner (Step 2).

- [ ] **Step 2:** SQL correctness cases (record with `--record-from target`), each run in `apply` mode (a leading `SET subquery_unnest_mode='apply';` step if the runner supports per-case SET — research/M1a noted the runner talks to a live server, so the TLS install per statement should make it work; verify):
  - `subquery_scalar_correlated_agg`: correlated `min`/`avg` scalar (q2/q17 shape) — result matches legacy.
  - `subquery_scalar_uncorrelated`: `(SELECT max(x) FROM t)` and `(SELECT x FROM t LIMIT 1)`.
  - `subquery_scalar_empty_group`: correlated agg where some outer rows have NO matching inner group → scalar result is NULL (LEFT OUTER JOIN null-extension).
  - `subquery_scalar_count_zero`: correlated `count(*)` scalar → 0 (not NULL) for non-matching outer rows (proves the `ifnull(count,0)` normalization).
  - `subquery_scalar_null_key`: outer correlation key is NULL → no match → NULL scalar.
  - `subquery_scalar_correlated_nonagg_ok`: correlated NON-aggregate scalar where each outer key matches ≤1 inner row (e.g. inner key is unique) → returns the value (matches legacy); proves the with-check path's happy path.
  - `subquery_scalar_correlated_nonagg_multirow`: correlated NON-aggregate scalar where some outer key matches >1 inner row → query ERRORS with the `assert_true` message `correlate scalar subquery result must 1 row` (use `-- @expect_error`). Confirms the per-group row-check actually fires (this is the SQL-standard "more than one row returned by a scalar subquery" semantics, which `legacy` mode also enforces — compare error behavior).
- [ ] **Step 3:** Plan-golden (optional) under `sql-tests/optimizer/`: `subquery_scalar_to_join_shape` asserting (via `@explain_contains`) the apply-mode plan has the LEFT OUTER JOIN + aggregate (and NO residual `APPLY`), and `@explain_not_contains` `ASSERT NUM ROWS` for the agg case; plus a `subquery_scalar_nonagg_assert_shape` asserting the non-agg path's plan contains the `assert_true`/aggregate guard. Record from target.

- [ ] **Step 4:** Run the relevant suites in apply mode; confirm pass. `cargo test --lib` 0 failed.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "test(optimizer): scalar decorrelation correctness in apply mode

Correlated-aggregate (q2/q17 shape), uncorrelated, empty-group->NULL,
count->0, NULL-correlation-key, and correlated-non-aggregate (single-row OK
+ multi-row assert error) cases assert apply-mode scalar results match
legacy. Plan-golden locks the join+aggregate decorrelation shape."
```

---

### Task 6: tpc-h q2/q17 apply-mode validation + final verification

**Files:**
- Modify: final-check only; possibly a focused tpc-h apply-mode assertion.

- [ ] **Step 1:** Run tpc-h q2 and q17 in `apply` mode and confirm results match the existing `legacy` goldens (`sql-tests/tpc-h/result/q2.result`, `q17.result`). If the runner supports a per-case `SET subquery_unnest_mode='apply'`, add a focused apply-mode variant; otherwise document how it was validated (e.g. a one-off run with the session var set). The bar: **same results as legacy**; plan compactness (window form) is M2, not required here.
- [ ] **Step 2:** `cargo fmt`; `cargo clippy --lib` (no new lints on M1b symbols — grep for the rule/helper names); fix any.
- [ ] **Step 3:** `cargo build && cargo test --lib` → 0 failed.
- [ ] **Step 4:** Optional: run the `optimizer` and `join` suites in default (legacy) mode → no golden changes (M1b changes nothing in legacy mode).
- [ ] **Step 5:** fmt fixup commit if needed.

---

## Acceptance checklist (maps to design §7.3 M1, decorrelation half — full M1 scalar)

- [covered] PushDownApplyAggFilter + PushDownApplyFilter + ScalarApplyToJoin in the SubqueryRewrite stage, disable-able (Tasks 2, 2b, 3, 4).
- [covered] 2-arg `assert_true(bool, varchar)` registered (Task 3 Step 0).
- [covered] Uncorrelated scalar → CROSS JOIN (+ AssertOneRow when needed) (Task 3).
- [covered] Correlated aggregate scalar → LEFT OUTER JOIN over vector aggregate (Tasks 2, 3) — q2/q17.
- [covered] Correlated non-aggregate scalar → LEFT OUTER JOIN over count(1)/any_value aggregate + `assert_true` per-group row-check (Tasks 2b, 3).
- [covered] count→ifnull(count,0) normalization (Task 3).
- [covered] apply-mode scalar results == legacy; empty-group→NULL; NULL key; count→0; non-agg single-row OK + multi-row error (Task 5).
- [covered] tpc-h q2/q17 apply-mode results correct (Task 6).
- [covered] EQ-only correlation; non-EQ errors explicitly (Tasks 2, 2b).
- [covered] Default legacy mode unchanged (every task).

## Out of scope (M2 / M3 — separate plans)

- **ScalarApply2AnalyticRule (WinMagic)**: rewriting a correlated scalar aggregate to a window — the OQ-13 plan-compactness optimization for q2/q17 (this plan makes q2/q17 *correct* in apply mode via joins; the window *shape* is M2).
- **EXISTS / IN** decorrelation: M3.
- **Non-column inner correlation key** or **non-EQ correlation**: rejected/`Unchanged` → ApplyException (a narrow tail; revisit if real queries need it).

## Risks

- **Group-key column ids in the vector agg:** the promoted inner correlation column already exists in the inner scan output — reuse its `ColumnId` (do NOT mint a new one) so the join condition `outer == inner` and the agg output stay consistent. Getting this wrong surfaces as an `id_binding_verifier` error at codegen — a good tripwire; the Task 5 end-to-end test exercises it.
- **`output_column` mapping:** the wrap Project must map the minted `output_column.column_id` to the inner scalar result id, else the outer predicate's `ColumnRef(output_column)` won't resolve. Covered by Task 3 tests + Task 5 e2e.
- **Per-case session SET in the SQL runner:** if unsupported, fall back to in-crate end-to-end tests (Task 5 Step 1) and document.
- **`assert_true` column surviving column pruning (with-check path):** the assertion is a computed column the outer query never references, so pruning could drop it and silently disable the row-check. Mark its output `is_internal: true` and verify with the Task 5 `..._nonagg_multirow` test that the error actually fires; if pruning still drops it, AND the assertion into a residual predicate instead. Do not consider the with-check path done until that test goes red→green on the assert.
- **Inner group-key/scalar visibility (with-check path):** the count/any_value aggregate groups by the inner correlation columns and aggregates the inner scalar — both must be exposed by the agg input. A leading inner `Project` may hide the group-key columns; extend it with pass-through items (or aggregate over its input). An `id_binding_verifier` error at codegen is the tripwire.
