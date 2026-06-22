# CSE v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add common-subexpression elimination (CSE) to the standalone optimizer so a repeated, non-trivial scalar subexpression within one operator's expression set is computed once (materialized as a Project output column) and referenced by ColumnId, instead of being re-evaluated per occurrence.

**Architecture:** A post-CBO physical-tree pass (`cse_pass`) modeled on `runtime_filter_pass`, run inside `optimize()` after `extract_best` and before `attach_scalar_arena` (so it can take `&mut memo.scalars` + `&mut memo.factory`). Detection is a per-operator ScalarId frequency count (the ScalarArena hash-conses, so structural-equality ⟺ same `ScalarId`). The rewrite materializes each common subexpression as a Project output column by inserting/reusing a `PhysicalProject` below the consuming operator, including projection-list CSE. This avoids same-Project self-reference because standalone `lower_project` compiles each Project item against child scope only. Consumers are rewritten to reference the CSE output via `ScalarNode::ColumnRef`. Cross-input join-condition CSE is **out of scope** (v2). v1 needs **zero** new exec/codegen/thrift code — it produces standard `PhysicalProject` nodes that the existing `build_distributed_plan` bridge and project working-chunk already handle.

**Tech Stack:** Rust; `src/sql/optimizer/**`; the `ScalarArena`/`ScalarId` IR (`src/sql/optimizer/scalar/mod.rs`); `ColumnRefFactory` (`src/sql/column_id.rs`); sql-test runner (`tests/sql-test-runner`) for plan-golden + result regression.

**Spec:** `docs/design/specs/2026-06-21-optimizer-cse-v1-design.md`.

---

## File Structure

- **Create** `src/sql/optimizer/cse_pass.rs` — the entire CSE pass: detection (`count_subexprs`, `eligible`), traversal helpers (`child_ids`, `subtree_size`, `substitute`), common-materialization (`build_commons`, `insert_or_reuse_project_below`), and per-operator drivers (`rewrite_project`, `rewrite_filter`, `rewrite_aggregate`, `rewrite_sort`, `rewrite_window`, `rewrite_join`). One module, one responsibility.
- **Modify** `src/sql/optimizer/mod.rs` — `mod cse_pass;`, call `cse_pass::rewrite(...)` at the `optimize()` tail, add `CSE_RULE` to `is_known_rule_name`.
- **Modify** `src/sql/optimizer/options.rs` — add `enable_common_subexpr_reuse: Option<bool>` to `SessionOptimizerSettings`, gate the rule in `from_session`.
- **Modify** `src/server/mod.rs` — `SET enable_common_subexpr_reuse` handler.
- **Create** `sql-tests/optimizer/sql/cse_*.sql` — plan-golden + result cases.

**Shared signatures (defined in Task 2/3, referenced by later tasks — keep these stable):**
```rust
pub(crate) const CSE_RULE: &str = "CommonSubexpressionReuse";
fn count_subexprs(scalars: &ScalarArena, roots: &[ScalarId]) -> HashMap<ScalarId, usize>;
fn child_ids(scalars: &ScalarArena, id: ScalarId) -> Vec<ScalarId>;
fn eligible(scalars: &ScalarArena, id: ScalarId) -> bool;
fn subtree_size(scalars: &ScalarArena, id: ScalarId) -> usize;
fn pick_commons(scalars: &ScalarArena, roots: &[ScalarId]) -> Vec<ScalarId>;
fn substitute(scalars: &mut ScalarArena, id: ScalarId, subst: &HashMap<ScalarId, ScalarId>) -> ScalarId;
fn build_commons(scalars: &mut ScalarArena, factory: &mut ColumnRefFactory, commons: &[ScalarId])
    -> (Vec<ScalarProjectItem>, HashMap<ScalarId, ScalarId>);
fn insert_or_reuse_project_below(child: &mut PhysicalPlanNode, prelude: Vec<ScalarProjectItem>);
```

---

## Task 1: Pass skeleton + gating wiring (no-op, gated)

**Files:**
- Create: `src/sql/optimizer/cse_pass.rs`
- Modify: `src/sql/optimizer/mod.rs` (module decl ~line 1-40; call site line 231-232; `is_known_rule_name` line 258-268)

- [ ] **Step 1: Create the module with a gated no-op `rewrite`**

Create `src/sql/optimizer/cse_pass.rs`:
```rust
//! Common-subexpression elimination (CSE v1): a post-CBO physical-tree pass that
//! detects repeated non-trivial scalar subexpressions within an operator's
//! expression set and materializes each as a Project output column computed once,
//! rewriting consumers to reference it by ColumnId.
//!
//! See docs/design/specs/2026-06-21-optimizer-cse-v1-design.md.

use crate::sql::optimizer::options::OptimizerOptions;
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::scalar::ScalarArena;

/// Stable rule name for `SET disable_optimizer_rules`.
pub(crate) const CSE_RULE: &str = "CommonSubexpressionReuse";

/// Entry point: rewrite the physical tree in place. Gated by `CSE_RULE`.
pub(crate) fn rewrite(
    root: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    options: &OptimizerOptions,
) {
    if !options.is_enabled(CSE_RULE) {
        return;
    }
    rewrite_node(root, scalars, factory);
}

/// Post-order walk. Per-operator drivers are added in later tasks.
fn rewrite_node(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    for child in &mut node.children {
        rewrite_node(child, scalars, factory);
    }
    // Per-operator rewrite dispatch added in Tasks 3-6.
    let _ = (scalars, factory, &node.op);
}
```

- [ ] **Step 2: Declare the module and wire the call site**

In `src/sql/optimizer/mod.rs`, add the module declaration next to the other `mod` lines (e.g. near `mod runtime_filter_pass;`):
```rust
mod cse_pass;
```

Then at the `optimize()` tail (currently lines 231-232), insert the CSE call between `annotate` and `attach_scalar_arena`:
```rust
    // 12. Annotate physical plan with runtime filter descriptors.
    runtime_filter_pass::annotate(&mut physical, &memo.scalars, &options);
    // 13. Common-subexpression elimination (materializes repeats as Project columns).
    cse_pass::rewrite(&mut physical, &mut memo.scalars, &mut memo.factory, &options);
    physical_plan::attach_scalar_arena(&mut physical, Arc::new(memo.scalars.clone()));
```

- [ ] **Step 3: Register the rule name**

In `src/sql/optimizer/mod.rs` `is_known_rule_name` (line 258-268), add a disjunct:
```rust
    || name == cse_pass::CSE_RULE
```

- [ ] **Step 4: Build to verify it compiles and is a no-op**

Run: `cargo build`
Expected: compiles clean. (`memo.scalars` and `memo.factory` are disjoint fields, so the two `&mut` borrows in one call are allowed.)

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cse_pass.rs src/sql/optimizer/mod.rs
git commit -m "feat(optimizer): CSE pass skeleton + gating wiring (no-op)"
```

---

## Task 2: Detection — frequency count + eligibility

**Files:**
- Modify: `src/sql/optimizer/cse_pass.rs`
- Test: inline `#[cfg(test)]` module in `cse_pass.rs`

- [ ] **Step 1: Write the failing detection tests**

Append to `src/sql/optimizer/cse_pass.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{BinOp, ScalarArena, ScalarNode};
    use arrow::datatypes::DataType;

    fn col(arena: &mut ScalarArena, id: u32) -> super::super::scalar::ScalarId {
        arena.intern(ScalarNode::ColumnRef(ColumnId(id)), DataType::Int64, true)
    }
    fn add(arena: &mut ScalarArena, l: super::super::scalar::ScalarId, r: super::super::scalar::ScalarId)
        -> super::super::scalar::ScalarId {
        arena.intern(ScalarNode::BinaryOp { op: BinOp::Add, left: l, right: r }, DataType::Int64, true)
    }

    #[test]
    fn repeated_binary_op_is_a_count2_candidate() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let ab = add(&mut arena, a, b);          // a+b
        // two root exprs both referencing a+b: (a+b), (a+b)+a  -> wrap to force 2 occurrences
        let ab_plus_a = add(&mut arena, ab, a);
        let commons = pick_commons(&arena, &[ab, ab_plus_a]);
        assert_eq!(commons, vec![ab], "a+b occurs twice and is non-trivial");
    }

    #[test]
    fn leaves_and_bare_cast_of_column_are_not_candidates() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        // a referenced twice as a root: column refs are never candidates
        let commons = pick_commons(&arena, &[a, a]);
        assert!(commons.is_empty());
    }

    #[test]
    fn volatile_function_is_never_factored() {
        let mut arena = ScalarArena::new();
        let rnd = arena.intern(
            ScalarNode::FunctionCall { name: "rand".to_string(), args: vec![], distinct: false },
            DataType::Float64, false);
        assert!(!eligible(&arena, rnd));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks cse_pass:: 2>&1 | head -30` (adjust crate name to the lib's actual package name from `Cargo.toml`)
Expected: FAIL — `pick_commons`, `eligible` not found.

- [ ] **Step 3: Implement the detection functions**

Add to `cse_pass.rs` (above the `tests` module). Note the `ScalarId`/`ScalarNode` imports:
```rust
use std::collections::HashMap;
use crate::sql::optimizer::scalar::{ScalarId, ScalarNode};

/// Immediate child ScalarIds we descend into. Window/lambda/leaves are opaque
/// (returns empty) — v1 does not factor inside those, keeping counting and
/// `substitute` consistent.
fn child_ids(scalars: &ScalarArena, id: ScalarId) -> Vec<ScalarId> {
    match scalars.node(id) {
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. } => vec![*child],
        ScalarNode::FunctionCall { args, .. } => args.clone(),
        ScalarNode::AggregateCall { args, order_by, .. } => {
            let mut v = args.clone();
            v.extend(order_by.iter().map(|k| k.expr));
            v
        }
        ScalarNode::Cast { child, .. } => vec![*child],
        ScalarNode::IsNull { child, .. } => vec![*child],
        ScalarNode::InList { child, list, .. } => {
            let mut v = vec![*child];
            v.extend(list.iter().copied());
            v
        }
        ScalarNode::Between { child, low, high, .. } => vec![*child, *low, *high],
        ScalarNode::Like { child, pattern, .. } => vec![*child, *pattern],
        ScalarNode::Case { operand, when_then, else_expr } => {
            let mut v = Vec::new();
            if let Some(o) = operand { v.push(*o); }
            for (w, t) in when_then { v.push(*w); v.push(*t); }
            if let Some(e) = else_expr { v.push(*e); }
            v
        }
        ScalarNode::IsTruthValue { child, .. } => vec![*child],
        ScalarNode::Nested(inner) => vec![*inner],
        // Opaque for v1: leaves, window, lambda.
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => Vec::new(),
    }
}

fn count_one(scalars: &ScalarArena, id: ScalarId, counts: &mut HashMap<ScalarId, usize>) {
    *counts.entry(id).or_insert(0) += 1;
    for c in child_ids(scalars, id) {
        count_one(scalars, c, &mut *counts);
    }
}

fn count_subexprs(scalars: &ScalarArena, roots: &[ScalarId]) -> HashMap<ScalarId, usize> {
    let mut counts = HashMap::new();
    for &r in roots {
        count_one(scalars, r, &mut counts);
    }
    counts
}

fn subtree_size(scalars: &ScalarArena, id: ScalarId) -> usize {
    1 + child_ids(scalars, id).into_iter().map(|c| subtree_size(scalars, c)).sum::<usize>()
}

fn is_volatile(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "rand" | "random" | "uuid" | "uuid_numeric" | "uniq_id"
    )
}

/// A subexpression is worth materializing if it is non-trivial.
fn eligible(scalars: &ScalarArena, id: ScalarId) -> bool {
    match scalars.node(id) {
        ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. } => false,
        // Bare cast of a column ref is cheap; materializing only adds a column.
        ScalarNode::Cast { child, .. } => {
            !matches!(scalars.node(*child), ScalarNode::ColumnRef(_))
        }
        ScalarNode::FunctionCall { name, .. } if is_volatile(name) => false,
        // Opaque-in-v1 shapes are never factored as a whole.
        ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. } => false,
        _ => true,
    }
}

/// Common subexpressions, ordered ascending by subtree size so a nested common
/// is materialized (and assigned a ColumnId) before the larger common that
/// contains it.
fn pick_commons(scalars: &ScalarArena, roots: &[ScalarId]) -> Vec<ScalarId> {
    let counts = count_subexprs(scalars, roots);
    let mut commons: Vec<ScalarId> = counts
        .iter()
        .filter(|(id, &c)| c >= 2 && eligible(scalars, **id))
        .map(|(id, _)| *id)
        .collect();
    commons.sort_by_key(|&id| (subtree_size(scalars, id), id.as_u32()));
    commons
}
```

If `ScalarId` has no `as_u32()` accessor, sort by `subtree_size` only: `commons.sort_by_key(|&id| subtree_size(scalars, id));` (deterministic enough; the test does not depend on tie order). Confirm by reading `scalar/mod.rs:54-55`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p novarocks cse_pass:: 2>&1 | tail -20`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/cse_pass.rs
git commit -m "feat(optimizer): CSE detection — ScalarId frequency count + eligibility"
```

---

## Task 3: Projection-list CSE (insert child CSE Project, smallest end-to-end)

This validates the whole chain (optimizer rewrite → `build_distributed_plan` bridge → project working-chunk) using a child CSE Project. Same-Project prelude is invalid in the current codebase because `lower_project` compiles each item against child scope, not prior items in the same Project.

**Files:**
- Modify: `src/sql/optimizer/cse_pass.rs`
- Test: inline tests + `sql-tests/optimizer/sql/cse_projection.sql` and `sql-tests/optimizer/result/cse_projection.result`

- [ ] **Step 1: Write the failing `substitute` + `build_commons` unit test**

Append to the `tests` module in `cse_pass.rs`:
```rust
    #[test]
    fn substitute_replaces_common_and_reinterns() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let ab = add(&mut arena, a, b);
        let k = ColumnId(99);
        let kref = arena.intern(ScalarNode::ColumnRef(k), DataType::Int64, true);
        let mut subst = std::collections::HashMap::new();
        subst.insert(ab, kref);
        let ab_plus_a = add(&mut arena, ab, a);
        let rewritten = substitute(&mut arena, ab_plus_a, &subst);
        // rewritten should be (ColumnRef(99)) + a
        match arena.node(rewritten) {
            ScalarNode::BinaryOp { left, right, .. } => {
                assert!(matches!(arena.node(*left), ScalarNode::ColumnRef(ColumnId(99))));
                assert!(matches!(arena.node(*right), ScalarNode::ColumnRef(ColumnId(1))));
            }
            other => panic!("unexpected node: {other:?}"),
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks cse_pass::tests::substitute_replaces_common 2>&1 | tail -10`
Expected: FAIL — `substitute` not found.

- [ ] **Step 3: Implement `substitute` and `build_commons`**

Add to `cse_pass.rs`:
```rust
use crate::sql::column_id::ColumnRefFactory;
use crate::sql::optimizer::operator::ScalarProjectItem;

/// Rebuild `id` replacing any node present in `subst` with its mapped id;
/// unchanged subtrees re-intern to the same id (hash-consing), so this is
/// O(changed path). Opaque shapes (leaves/window/lambda) return unchanged.
fn substitute(
    scalars: &mut ScalarArena,
    id: ScalarId,
    subst: &HashMap<ScalarId, ScalarId>,
) -> ScalarId {
    if let Some(&mapped) = subst.get(&id) {
        return mapped;
    }
    let node = scalars.node(id).clone();
    let ty = scalars.data_type(id).clone();
    let nullable = scalars.nullable(id);
    let rebuilt = match node {
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: substitute(scalars, left, subst),
            right: substitute(scalars, right, subst),
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp { op, child: substitute(scalars, child, subst) },
        ScalarNode::FunctionCall { name, args, distinct } => ScalarNode::FunctionCall {
            name,
            args: args.into_iter().map(|a| substitute(scalars, a, subst)).collect(),
            distinct,
        },
        ScalarNode::AggregateCall { name, args, distinct, order_by } => ScalarNode::AggregateCall {
            name,
            args: args.into_iter().map(|a| substitute(scalars, a, subst)).collect(),
            distinct,
            order_by, // order_by exprs not substituted in v1 (rare); keep as-is
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast { child: substitute(scalars, child, subst), target },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull { child: substitute(scalars, child, subst), negated },
        ScalarNode::InList { child, list, negated } => ScalarNode::InList {
            child: substitute(scalars, child, subst),
            list: list.into_iter().map(|i| substitute(scalars, i, subst)).collect(),
            negated,
        },
        ScalarNode::Between { child, low, high, negated } => ScalarNode::Between {
            child: substitute(scalars, child, subst),
            low: substitute(scalars, low, subst),
            high: substitute(scalars, high, subst),
            negated,
        },
        ScalarNode::Like { child, pattern, negated } => ScalarNode::Like {
            child: substitute(scalars, child, subst),
            pattern: substitute(scalars, pattern, subst),
            negated,
        },
        ScalarNode::Case { operand, when_then, else_expr } => ScalarNode::Case {
            operand: operand.map(|o| substitute(scalars, o, subst)),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (substitute(scalars, w, subst), substitute(scalars, t, subst)))
                .collect(),
            else_expr: else_expr.map(|e| substitute(scalars, e, subst)),
        },
        ScalarNode::IsTruthValue { child, value, negated } => ScalarNode::IsTruthValue {
            child: substitute(scalars, child, subst), value, negated,
        },
        ScalarNode::Nested(inner) => ScalarNode::Nested(substitute(scalars, inner, subst)),
        // Opaque: return unchanged.
        opaque @ (ScalarNode::ColumnRef(_)
        | ScalarNode::Literal(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::WindowCall { .. }
        | ScalarNode::Lambda { .. }
        | ScalarNode::LambdaFunction { .. }) => {
            let _ = opaque;
            return id;
        }
    };
    scalars.intern(rebuilt, ty, nullable)
}

/// For each common, build an independent producer Project item into a freshly
/// minted internal ColumnId. Producer items must not reference earlier CSE
/// outputs because a standalone Project item is compiled against child scope
/// only. Returns the prelude items and a subst map (common ScalarId -> interned
/// ColumnRef id) for rewriting consumers.
fn build_commons(
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
    commons: &[ScalarId],
) -> (Vec<ScalarProjectItem>, HashMap<ScalarId, ScalarId>) {
    let mut subst: HashMap<ScalarId, ScalarId> = HashMap::new();
    let mut items: Vec<ScalarProjectItem> = Vec::new();
    for &c in commons {
        let producer = c;
        let ty = scalars.data_type(c).clone();
        let nullable = scalars.nullable(c);
        let name = format!("__cse_{}", items.len());
        let k = factory.create(None, name.clone(), ty.clone(), nullable);
        items.push(ScalarProjectItem {
            expr: producer,
            output_name: name,
            output_column_id: k,
            expr_display: None,
        });
        let kref = scalars.intern(ScalarNode::ColumnRef(k), ty, nullable);
        subst.insert(c, kref);
    }
    (items, subst)
}
```

- [ ] **Step 4: Run the `substitute` test**

Run: `cargo test -p novarocks cse_pass::tests::substitute_replaces_common 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Write the failing `rewrite_project` test**

Append to the `tests` module. It builds a `PhysicalProject` with two items both using `a+b`, runs the rewrite, and asserts a child CSE `PhysicalProject` is inserted below the original Project. The original Project keeps only user-visible items, and those items now reference the CSE child output. Use the construction template from `runtime_filter_pass.rs:919-934`:
```rust
    use crate::sql::optimizer::operator::{Operator, ProjectOp};
    use crate::sql::optimizer::physical_plan::{PhysicalPlanNode, PlanExecutionProps};
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::column_id::ColumnRefFactory;

    fn proj_item(arena: &ScalarArena, expr: ScalarId, out: u32, name: &str) -> ScalarProjectItem {
        let _ = arena;
        ScalarProjectItem { expr, output_name: name.to_string(), output_column_id: ColumnId(out), expr_display: None }
    }

    #[test]
    fn rewrite_project_factors_repeated_subexpr() {
        let mut arena = ScalarArena::new();
        let mut factory = ColumnRefFactory::new();
        let a = col(&mut arena, 1);
        let b = col(&mut arena, 2);
        let ab = add(&mut arena, a, b);
        let ab2 = add(&mut arena, ab, ab);           // (a+b)+(a+b) — occurs >=2
        let items = vec![
            proj_item(&arena, ab, 10, "x"),          // x = a+b
            proj_item(&arena, ab2, 11, "y"),         // y = (a+b)+(a+b)
        ];
        let child = PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![out_col(1, "a"), out_col(2, "b")],
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![out_col(1, "a"), out_col(2, "b")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut node = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp { items, output_qualifier: None }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![out_col(10, "x"), out_col(11, "y")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        rewrite_node(&mut node, &mut arena, &mut factory);
        let Operator::PhysicalProject(p) = &node.op else { panic!() };
        assert_eq!(p.items.len(), 2);
        let Operator::PhysicalProject(cse_project) = &node.children[0].op else { panic!() };
        assert_eq!(cse_project.items[2].output_name, "__cse_0");
        let common_col = cse_project.items[2].output_column_id;
        assert!(matches!(arena.node(p.items[0].expr), ScalarNode::ColumnRef(c) if *c == common_col));
    }
```

- [ ] **Step 6: Run to verify failure**

Run: `cargo test -p novarocks cse_pass::tests::rewrite_project_factors 2>&1 | tail -10`
Expected: FAIL — `rewrite_project` dispatch not implemented (the no-op `rewrite_node` leaves items unchanged).

- [ ] **Step 7: Implement `rewrite_project` and dispatch**

In `cse_pass.rs`, replace the dispatch stub in `rewrite_node` with:
```rust
    match &node.op {
        Operator::PhysicalProject(_) => rewrite_project(node, scalars, factory),
        _ => {}
    }
```

Add the driver:
```rust
use crate::sql::optimizer::operator::Operator;
use crate::sql::common::schema::OutputColumn;

fn rewrite_project(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    let Operator::PhysicalProject(project) = &node.op else { return };
    let roots: Vec<ScalarId> = project.items.iter().map(|it| it.expr).collect();
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() {
        return;
    }
    let (prelude, subst) = build_commons(scalars, factory, &commons);

    let Operator::PhysicalProject(project) = &mut node.op else { unreachable!() };
    for item in &mut project.items {
        item.expr = substitute(scalars, item.expr, &subst);
    }
    // Insert a child CSE Project. The original Project remains the visible
    // result contract and consumes the CSE child output by ColumnId.
    //
    // Passthrough columns must come from leaf ColumnRefs referenced by the
    // original project roots. Do not blindly copy child.output_columns: some
    // pass-through physical nodes can carry metadata that is not the verifier's
    // true child scope.
    insert_cse_project_below_current_project(node, roots, prelude, scalars);
}
```

- [ ] **Step 8: Run unit test to verify pass**

Run: `cargo test -p novarocks cse_pass:: 2>&1 | tail -20`
Expected: PASS (all cse_pass tests).

- [ ] **Step 9: Write the end-to-end plan-golden + result case**

Create `sql-tests/optimizer/sql/cse_projection.sql` (follow an existing optimizer suite case for exact directive syntax — e.g. `aggregate_pushdown_*.sql`). It must (a) assert the common appears once in the plan and (b) verify the result columns are exactly the user-selected ones (no `__cse_` leakage):
```sql
-- @tags=optimizer,cse
DROP TABLE IF EXISTS ${case_db}.cse_projection_t;
CREATE TABLE ${case_db}.cse_projection_t (a BIGINT, b BIGINT);
INSERT INTO ${case_db}.cse_projection_t VALUES (3, 4), (5, 6);

-- Golden result: exactly two visible columns x,y.
SELECT (a + b) AS x, (a + b) + a AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;

-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT (a + b) AS x, (a + b) + a AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;
```

- [ ] **Step 10: Run the end-to-end case**

Start standalone-server, then:
```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --only cse_projection --mode record --record-from target
```
Inspect the recorded golden: the result must have exactly columns `x`,`y`. The second SELECT validates `EXPLAIN VERBOSE` contains `__cse_0` and the result text does not contain any `__cse_` internal column. Then re-run with `--mode verify` and confirm PASS.
**If `__cse_0` leaks into the result columns**, the `is_internal` flag is not being honored by the final output projection — fix the output path (or filter internal columns at result assembly) before proceeding; this is the single end-to-end assumption from spec §8.

- [ ] **Step 11: Commit**

```bash
git add src/sql/optimizer/cse_pass.rs sql-tests/optimizer/sql/cse_projection.sql sql-tests/optimizer/result/cse_projection.result
git commit -m "feat(optimizer): CSE for projection lists"
```

---

## Task 4: Insert/reuse Project below Filter

**Files:**
- Modify: `src/sql/optimizer/cse_pass.rs`
- Test: inline test + `sql-tests/optimizer/sql/cse_filter.sql`

- [ ] **Step 1: Write the failing `insert_or_reuse_project_below` test**

Append to `tests`:
```rust
    #[test]
    fn insert_project_below_wraps_child() {
        let mut arena = ScalarArena::new();
        let a = col(&mut arena, 1);
        let scan = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp { items: vec![], output_qualifier: None }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![OutputColumn {
                column_id: ColumnId(1), name: "a".into(), data_type: arrow::datatypes::DataType::Int64,
                nullable: true, is_internal: false }],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut child = scan;
        let item = ScalarProjectItem { expr: a, output_name: "__cse_0".into(), output_column_id: ColumnId(50), expr_display: None };
        insert_or_reuse_project_below(&mut child, vec![item], &arena);
        // child is now a PhysicalProject whose child is the original scan
        assert!(matches!(child.op, Operator::PhysicalProject(_)));
        assert_eq!(child.children.len(), 1);
        let Operator::PhysicalProject(p) = &child.op else { panic!() };
        // passthrough of original column + the new common
        assert!(p.items.iter().any(|it| it.output_column_id == ColumnId(50)));
        assert!(p.items.iter().any(|it| it.output_column_id == ColumnId(1)));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p novarocks cse_pass::tests::insert_project_below 2>&1 | tail -10`
Expected: FAIL — `insert_or_reuse_project_below` not found.

- [ ] **Step 3: Implement the insert/reuse helper**

```rust
/// Ensure a Project carrying `prelude` (the common producers) plus passthroughs
/// of `child`'s output columns sits directly below the consuming operator.
/// If `child` is already a PhysicalProject, prepend the prelude (reuse);
/// otherwise wrap `child` in a new PhysicalProject.
fn insert_or_reuse_project_below(
    child: &mut PhysicalPlanNode,
    prelude: Vec<ScalarProjectItem>,
    scalars: &ScalarArena,
) {
    if prelude.is_empty() {
        return;
    }
    if let Operator::PhysicalProject(p) = &mut child.op {
        let mut new_items = prelude.clone();
        new_items.append(&mut p.items);
        p.items = new_items;
        for it in &prelude {
            child.output_columns.push(internal_output_column(it, scalars));
        }
        return;
    }
    // Wrap: new project passes through child's output columns + the prelude.
    let passthrough: Vec<ScalarProjectItem> = child
        .output_columns
        .iter()
        .map(|oc| ScalarProjectItem {
            // a passthrough item references the column by id; the bridge maps
            // ColumnRef(id) -> the child's slot for that id.
            expr: scalars_column_ref(oc),
            output_name: oc.name.clone(),
            output_column_id: oc.column_id,
            expr_display: None,
        })
        .collect();
    let mut items = prelude.clone();
    items.extend(passthrough);
    let mut new_output_columns: Vec<OutputColumn> =
        prelude.iter().map(|it| internal_output_column(it, scalars)).collect();
    new_output_columns.extend(child.output_columns.iter().cloned());

    let original = std::mem::replace(
        child,
        PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp { items, output_qualifier: None }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: new_output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        },
    );
    child.stats = original.stats.clone();
    child.execution_props.output_property = original.execution_props.output_property.clone();
    child.children = vec![original];
}

fn internal_output_column(it: &ScalarProjectItem, scalars: &ScalarArena) -> OutputColumn {
    OutputColumn {
        column_id: it.output_column_id,
        name: it.output_name.clone(),
        data_type: scalars.data_type(it.expr).clone(),
        nullable: scalars.nullable(it.expr),
        is_internal: true,
    }
}
```
`scalars_column_ref(oc)` must intern a `ScalarNode::ColumnRef(oc.column_id)` — but the helper takes `&ScalarArena` (immutable). Since the passthrough column refs may already exist in the arena (the child produced them), but to be safe make `insert_or_reuse_project_below` take `&mut ScalarArena` and intern: change the signature to `(child, prelude, scalars: &mut ScalarArena)` and replace `scalars_column_ref(oc)` with `scalars.intern(ScalarNode::ColumnRef(oc.column_id), oc.data_type.clone(), oc.nullable)`. Update the test call to pass `&mut arena`.

- [ ] **Step 4: Run the helper test**

Run: `cargo test -p novarocks cse_pass::tests::insert_project_below 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Add the Filter driver + dispatch**

Add dispatch arm in `rewrite_node`:
```rust
        Operator::PhysicalFilter(_) => rewrite_filter(node, scalars, factory),
```
Driver:
```rust
fn rewrite_filter(
    node: &mut PhysicalPlanNode,
    scalars: &mut ScalarArena,
    factory: &mut ColumnRefFactory,
) {
    let Operator::PhysicalFilter(filter) = &node.op else { return };
    let commons = pick_commons(scalars, &[filter.predicate]);
    if commons.is_empty() {
        return;
    }
    let (prelude, subst) = build_commons(scalars, factory, &commons);
    let new_pred = substitute(scalars, filter.predicate, &subst);
    if let Operator::PhysicalFilter(filter) = &mut node.op {
        filter.predicate = new_pred;
    }
    // child[0] computes the commons; filter now reads them by ColumnId.
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}
```

- [ ] **Step 6: Write + record the end-to-end Filter case**

Create `sql-tests/optimizer/sql/cse_filter.sql` with a `WHERE (a+b) > 1 AND (a+b) < 100` query; `-- @explain_contains=__cse_0`; and a result-correctness check. Record then verify (commands as Task 3 Step 10, `--only cse_filter`).
**Verify the spec §8 ordering assumption**: the filter conjunct must evaluate against the inserted Project's output. If the result is wrong/empty, the project operator is applying conjuncts pre-materialization — capture the failure and stop (this is the must-verify item); fix by ensuring conjuncts evaluate on project output.

- [ ] **Step 7: Run full optimizer suite to catch plan-golden drift**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
```
Expected: only cse_* new cases differ; if pre-existing goldens drift, re-record intentionally and review the diff shows only added `__cse_` columns/Projects.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer/cse_pass.rs sql-tests/optimizer/sql/cse_filter.sql
git commit -m "feat(optimizer): CSE for Filter (insert/reuse Project below)"
```

---

## Task 5: Aggregate / Sort / TopN / Window

Each reuses `pick_commons` + `build_commons` + `insert_or_reuse_project_below`; only the per-operator root extraction and consumer rewrite differ.

**Files:** Modify `src/sql/optimizer/cse_pass.rs`; tests `sql-tests/optimizer/sql/cse_agg.sql`.

- [ ] **Step 1: Write the failing Aggregate unit test**

`SUM(a*b)` + `AVG(a*b)` share `a*b`. Build a `PhysicalHashAggregate` with two `ScalarAggregateSpec`s whose `args=[a*b]`, run `rewrite_node`, assert a Project with `__cse_0=a*b` is inserted as child[0] and both aggregate args now reference the common ColumnId. (Mirror the unit-test construction style from Task 3 Step 5; `PhysicalHashAggregateOp` fields: `mode`, `group_by`, `aggregates`, `output_columns`, `is_merge` — see operator.rs:396-406.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p novarocks cse_pass::tests::aggregate 2>&1 | tail -10` → FAIL.

- [ ] **Step 3: Implement Aggregate / Sort / TopN / Window drivers + dispatch**

Dispatch arms:
```rust
        Operator::PhysicalHashAggregate(_) => rewrite_aggregate(node, scalars, factory),
        Operator::PhysicalSort(_) => rewrite_sort(node, scalars, factory),
        Operator::PhysicalTopN(_) => rewrite_topn(node, scalars, factory),
        Operator::PhysicalWindow(_) => rewrite_window(node, scalars, factory),
```
Aggregate (roots = all agg `args` + `group_by`; rewrite both):
```rust
fn rewrite_aggregate(node: &mut PhysicalPlanNode, scalars: &mut ScalarArena, factory: &mut ColumnRefFactory) {
    let Operator::PhysicalHashAggregate(agg) = &node.op else { return };
    let mut roots: Vec<ScalarId> = agg.group_by.clone();
    for spec in &agg.aggregates {
        roots.extend(spec.args.iter().copied());
    }
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() { return; }
    let (prelude, subst) = build_commons(scalars, factory, &commons);
    if let Operator::PhysicalHashAggregate(agg) = &mut node.op {
        for g in &mut agg.group_by { *g = substitute(scalars, *g, &subst); }
        for spec in &mut agg.aggregates {
            for a in &mut spec.args { *a = substitute(scalars, *a, &subst); }
        }
    }
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}
```
Sort (roots = `items[*].expr`; rewrite each `SortKey.expr`):
```rust
fn rewrite_sort(node: &mut PhysicalPlanNode, scalars: &mut ScalarArena, factory: &mut ColumnRefFactory) {
    let Operator::PhysicalSort(sort) = &node.op else { return };
    let roots: Vec<ScalarId> = sort.items.iter().map(|k| k.expr).collect();
    let commons = pick_commons(scalars, &roots);
    if commons.is_empty() { return; }
    let (prelude, subst) = build_commons(scalars, factory, &commons);
    if let Operator::PhysicalSort(sort) = &mut node.op {
        for k in &mut sort.items { k.expr = substitute(scalars, k.expr, &subst); }
    }
    insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
}
```
TopN is identical to Sort but matches `Operator::PhysicalTopN` / `TopNOp { items, .. }`. Window: roots = each `ScalarWindowSpec`'s `args` + `partition_by` + `order_by[*].expr`; rewrite the same fields. Write each driver in full (no "similar to" references).

- [ ] **Step 4: Run unit tests** — `cargo test -p novarocks cse_pass:: 2>&1 | tail -20` → PASS.

- [ ] **Step 5: Add end-to-end agg case + record/verify** — `cse_agg.sql` with `SUM(a*b), AVG(a*b)`; `-- @explain_contains=__cse_0`; result check. Record then verify.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/cse_pass.rs sql-tests/optimizer/sql/cse_agg.sql
git commit -m "feat(optimizer): CSE for Aggregate/Sort/TopN/Window (Project below)"
```

---

## Task 6: Join single-side condition CSE

Only **single-side** subexpressions of a join's non-equi condition are factored — pushed to that side's child Project. Cross-input subexpressions are left untouched (v2).

**Files:** Modify `src/sql/optimizer/cse_pass.rs`; tests `sql-tests/optimizer/sql/cse_join.sql`.

- [ ] **Step 1: Write the failing "single-side only" unit test**

Build a `PhysicalNestLoopJoin` with `condition = (l.x * 2) > (r.y)` where `l.x*2` appears twice (left-only) — assert `l.x*2` is factored into the LEFT child Project; and a second condition mixing `l.x * r.y` twice (cross-input) — assert it is NOT factored. Determine a subexpr's side by collecting its `ColumnId`s (reuse the `column_ids` traversal pattern from `runtime_filter_pass.rs:110`) and testing subset against each child's `output_columns` column-id set.

- [ ] **Step 2: Run to verify failure** — FAIL (`rewrite_join` not implemented).

- [ ] **Step 3: Implement `rewrite_join` + dispatch**

Dispatch arms:
```rust
        Operator::PhysicalHashJoin(_) | Operator::PhysicalNestLoopJoin(_) => rewrite_join(node, scalars, factory),
```
Driver outline (full code in implementation):
```rust
use std::collections::HashSet;
use crate::sql::column_id::ColumnId;

fn rewrite_join(node: &mut PhysicalPlanNode, scalars: &mut ScalarArena, factory: &mut ColumnRefFactory) {
    // condition source: PhysicalNestLoopJoin.condition, or PhysicalHashJoin.other_condition
    let cond = match &node.op {
        Operator::PhysicalNestLoopJoin(j) => j.condition,
        Operator::PhysicalHashJoin(j) => j.other_condition,
        _ => return,
    };
    let Some(cond) = cond else { return };
    let left_cols = output_column_set(&node.children[0]);
    let right_cols = output_column_set(&node.children[1]);

    let commons = pick_commons(scalars, &[cond]);
    // keep only commons whose columns are entirely on one side
    let left_commons: Vec<ScalarId> = commons.iter().copied()
        .filter(|c| side_subset(scalars, *c, &left_cols)).collect();
    let right_commons: Vec<ScalarId> = commons.iter().copied()
        .filter(|c| side_subset(scalars, *c, &right_cols)).collect();
    if left_commons.is_empty() && right_commons.is_empty() { return; }

    let mut subst = HashMap::new();
    if !left_commons.is_empty() {
        let (prelude, s) = build_commons(scalars, factory, &left_commons);
        subst.extend(s);
        insert_or_reuse_project_below(&mut node.children[0], prelude, scalars);
    }
    if !right_commons.is_empty() {
        let (prelude, s) = build_commons(scalars, factory, &right_commons);
        subst.extend(s);
        insert_or_reuse_project_below(&mut node.children[1], prelude, scalars);
    }
    let new_cond = substitute(scalars, cond, &subst);
    match &mut node.op {
        Operator::PhysicalNestLoopJoin(j) => j.condition = Some(new_cond),
        Operator::PhysicalHashJoin(j) => j.other_condition = Some(new_cond),
        _ => unreachable!(),
    }
}

fn output_column_set(node: &PhysicalPlanNode) -> HashSet<ColumnId> {
    node.output_columns.iter().map(|c| c.column_id).collect()
}
fn side_subset(scalars: &ScalarArena, id: ScalarId, side: &HashSet<ColumnId>) -> bool {
    let mut ids = HashSet::new();
    collect_column_ids(scalars, id, &mut ids);
    !ids.is_empty() && ids.iter().all(|c| side.contains(c))
}
fn collect_column_ids(scalars: &ScalarArena, id: ScalarId, out: &mut HashSet<ColumnId>) {
    if let ScalarNode::ColumnRef(c) = scalars.node(id) { out.insert(*c); }
    for c in child_ids(scalars, id) { collect_column_ids(scalars, c, out); }
}
```

- [ ] **Step 4: Run unit tests** — PASS (cross-input common not factored; single-side factored to its child).

- [ ] **Step 5: End-to-end join case + record/verify** — `cse_join.sql`: a join with a single-side repeated subexpr in the ON/other condition; assert `__cse_0` on the correct child; result correctness. Record then verify.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/cse_pass.rs sql-tests/optimizer/sql/cse_join.sql
git commit -m "feat(optimizer): CSE for join single-side conditions (cross-input deferred to v2)"
```

---

## Task 7: Session var + full regression

**Files:** Modify `src/sql/optimizer/options.rs`, `src/server/mod.rs`; tests inline + suite re-record.

- [ ] **Step 1: Write the failing gating test**

In `options.rs` `#[cfg(test)]`, assert that `SessionOptimizerSettings { enable_common_subexpr_reuse: Some(false), ..Default::default() }` produces an `OptimizerOptions` where `is_enabled(CSE_RULE)` is false:
```rust
    #[test]
    fn disabling_cse_via_session_disables_rule() {
        let s = SessionOptimizerSettings { enable_common_subexpr_reuse: Some(false), ..Default::default() };
        let opts = OptimizerOptions::from_session(&s);
        assert!(!opts.is_enabled(crate::sql::optimizer::cse_pass::CSE_RULE));
    }
```

- [ ] **Step 2: Run to verify failure** — FAIL (field does not exist).

- [ ] **Step 3: Add the field + gate**

In `options.rs`, add to `SessionOptimizerSettings` (line 10-44):
```rust
    pub enable_common_subexpr_reuse: Option<bool>,
```
In `OptimizerOptions::from_session` (line 146-184), after the `disabled_rules` loop, add:
```rust
    if settings.enable_common_subexpr_reuse == Some(false) {
        opts.disable(crate::sql::optimizer::cse_pass::CSE_RULE);
    }
```
(Default `None` ⇒ enabled.)

- [ ] **Step 4: Run gating test** — `cargo test -p novarocks options:: 2>&1 | tail -10` → PASS.

- [ ] **Step 5: Add the SET handler**

In `src/server/mod.rs` SET match (line 1012-1037), add:
```rust
        "enable_common_subexpr_reuse" => {
            shim.optimizer_settings.enable_common_subexpr_reuse = Some(enabled)
        }
```

- [ ] **Step 6: Add a disable-path plan-golden case**

Add to `cse_projection.sql`: `SET disable_optimizer_rules = 'CommonSubexpressionReuse';` then the same `EXPLAIN VERBOSE` with `-- @explain_contains` asserting the absence of `__cse_` (i.e. the plan reverts). Record/verify.

- [ ] **Step 7: Full regression**

```bash
cargo build && cargo test 2>&1 | tail -30
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
# Result-stability on the big suites (CSE must not change results):
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-ds --mode verify -j1
```
Expected: lib tests green; optimizer goldens drift only as intentional `__cse_`/Project additions (re-record + review); TPC-DS/SSB/TPC-H results unchanged.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer/options.rs src/server/mod.rs sql-tests/optimizer/sql/cse_projection.sql
git commit -m "feat(optimizer): CSE session var + disable-rule gating + full regression"
```

---

## Self-Review

**Spec coverage:**
- §4.1 phase (post-CBO, before attach_scalar_arena) → Task 1 Step 2. ✓
- §4.2 detection (id-frequency + eligible: leaves/bare-Cast/volatile/lambda/window) → Task 2. ✓
- §4.3 rewrite (mint ColumnId, build producers, substitute, nested-common ordering) → Task 3 (`build_commons`, `substitute`, `pick_commons` ascending). ✓
- §4.3 per-operator table: projection-list → T3; Filter → T4; Agg/Sort/Window → T5; join single-side → T6. ✓
- §5 zero exec/codegen/thrift → all tasks produce standard `PhysicalProject`; no exec/codegen files modified. ✓
- §6 gating (rule name + session var) → T1 Step 3, T7. ✓
- §7 EXPLAIN/plan-golden + verify → T3/T4/T5/T6 end-to-end cases, T7 regression. ✓
- §8 must-verify (conjunct on project output) → T4 Step 6; is_internal no-leak → T3 Step 10. ✓
- §9 v2 (cross-input join) explicitly excluded → T6 single-side filter. ✓

**Placeholder scan:** Task 5 (Sort/TopN/Window) and Task 6 give full driver code for Aggregate/Sort/Join and explicit field-level instructions for TopN/Window ("identical to Sort but matches PhysicalTopN/TopNOp.items"; "Window: roots = each ScalarWindowSpec's args+partition_by+order_by"). The implementer must write those two drivers in full per the field lists in operator.rs:221-233 — flagged, not hand-waved. SQL case bodies reference "match suite's table setup convention" because the optimizer suite's DDL/engine differs per environment; the implementer copies an existing case's preamble.

**Type consistency:** `rewrite`/`rewrite_node` take `(&mut PhysicalPlanNode, &mut ScalarArena, &mut ColumnRefFactory)`; `insert_or_reuse_project_below` takes `&mut ScalarArena` (corrected in T4 Step 3); `build_commons` returns `(Vec<ScalarProjectItem>, HashMap<ScalarId, ScalarId>)`; `substitute` returns `ScalarId`. `ScalarProjectItem`/`ProjectOp`/`OutputColumn`/`PhysicalPlanNode`/`PlanExecutionProps`/`Statistics` field names match operator.rs:76-134, physical_plan.rs:42-52, common/schema.rs:8-14, statistics.rs:90-94. `CSE_RULE` is referenced consistently in mod.rs, options.rs, and the disable test.

**Open verification items (encoded as test steps, not placeholders):** (1) `is_internal` excludes commons from user-visible output (T3 S10); (2) project applies fused conjuncts against output working-chunk (T4 S6); (3) the `build_distributed_plan` bridge preserves item order and resolves `ColumnRef(k)` → the common's slot (T3 S10 end-to-end). Each has an explicit fail-and-stop instruction.
