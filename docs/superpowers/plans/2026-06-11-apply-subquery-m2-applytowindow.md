# Apply / CorrelatedSubquery M2 — ApplyToWindow (WinMagic) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement M2 of the Apply framework (design: `docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md`, §3.2 / §6.2 / §7.3): add the `ApplyToWindow` rule (StarRocks `ScalarApply2AnalyticRule`, "WinMagic") to the `SubqueryRewrite` stage. It rewrites a decorrelated correlated-scalar-**aggregate** `Apply` that sits under a WHERE `Filter` comparison into a `Window` (analytic) over the outer relation, eliminating the redundant subquery re-scan. This is the OQ-13 main deliverable (tpc-h q2/q17 plan-compactness).

**Architecture:** A new `LogicalRewriteRule` in `src/sql/optimizer/rewrite/rules/subquery/apply_to_window.rs`, registered in `subquery_rewrite_rules()` **before** `ScalarApplyToJoin` (so to-window wins over to-join, mirroring StarRocks `SUBQUERY_REWRITE_TO_WINDOW_RULES` before `SUBQUERY_REWRITE_TO_JOIN_RULES`). It matches a `Filter` whose input is a scalar `Apply` already normalized by `PushDownApplyAggFilter` (`need_check_max_rows == false`, `correlation_conjuncts` populated, inner is a vector `Aggregate` with a single non-DISTINCT whitelisted aggregate). It verifies StarRocks' strict preconditions (whitelist, single agg, no limit, operator whitelist, table-set identity, predicate identity); on any failure it returns `Unchanged` so `ScalarApplyToJoin` produces the M1 join shape. On success it discards the subquery subtree and emits `Project_unchanged_above → Filter(after-window) → Window(agg OVER PARTITION BY outer-corr-keys) → Sort(partition keys) → Filter(before-window) → outer-subtree`.

**Tech Stack:** Rust; the existing rewrite framework (`LogicalRewriteRule`/`RewritePipeline`/`RewriteContext`), `ColumnRefFactory` (mint via `ctx.column_ref_factory()`), the existing `WindowNode`/`WindowExpr`/`SortNode` (logical→physical `ANALYTIC` lowering already exists; window stats-derive already exists), and the M1 decorrelation helpers in `decorrelate_util.rs` + `utils.rs`.

**Key constraints:**

1. **Pure addition, gated.** Tasks 1–6 add the rule and tests **without changing the default mode** (`subquery_unnest_mode` stays `legacy`). The rule only fires when an `Apply` reaches the stage, which today only happens in `apply`/`apply_strict` mode. Every task keeps the branch baseline `cargo test --lib` at **0 failed** (M1b's baseline). The default flip is the isolated, optional **Task 7**.
2. **Faithful port, no coverage-chasing.** Port StarRocks' preconditions exactly. Do **not** relax them to make more queries hit the window form — relaxing = mis-rewrite risk (design §9.1). When a precondition fails, return `RewriteResult::Unchanged` (NOT an error): `ScalarApplyToJoin` then produces the correct M1 join form. Never hardcode anything for q2/q17.
3. **`apply`-mode result with ApplyToWindow == `apply`-mode result without it (== legacy).** WinMagic is a *plan-shape* optimization; results must be identical to the M1 join form. The plan-golden + correctness tests in Task 5 are the bar.
4. **English** code/comments/errors; commit messages English, **no `Co-Authored-By` trailer** (matches the landed M1 commit series #294). Work on a dedicated branch (e.g. `claude/apply-subquery-m2-window`); push to `fork`, PR base `origin` (triangular remote, already configured).

---

## Design-doc reconciliation (read before coding — three corrections confirmed against the live code)

1. **EXPLAIN token is `WINDOW [`, not `ANALYTIC`.** Design §7.3 says "q17 EXPLAIN 出现 `ANALYTIC`". That is the FE/thrift node name. NovaRocks **standalone** EXPLAIN prints the logical/physical window node as `WINDOW [<fn>(...)]` (confirmed in golden `sql-tests/optimizer/result/g3_broadcast_join_output_inherits_left_hash.result`: `WINDOW [row_number()]`). All Task 5 golden assertions use `@explain_contains=WINDOW [`.
2. **The matched shape is `Filter(Apply)` directly — no intervening `Project`.** StarRocks matches `Filter(Project(Apply))`. NovaRocks' M1a planner inserts the `Apply` directly below the WHERE `Filter` (no Project between), and the SELECT-list `Aggregate`/`Project` sit *above* that Filter. M2 matches `Filter` whose `.input` is the `Apply`. A `Project` between Filter and Apply is out of M2 scope (falls back to `ScalarApplyToJoin`).
3. **A logical `Sort` child is required under `Window`.** The fragment builder does NOT auto-insert the partition sort; the planner emits `LogicalPlan::Sort { items, analytic_partition_by, .. }` directly below `LogicalPlan::Window`. Our rule must do the same.

---

## Construction toolbox (verbatim from the current code — use these exactly)

**Rule trait** (`src/sql/optimizer/rewrite/rule.rs`):
```rust
pub(crate) trait LogicalRewriteRule: Send + Sync {
    fn name(&self) -> &'static str;
    fn phase(&self) -> RewritePhase;                 // return RewritePhase::StructuralRewrite
    fn traversal(&self) -> RewriteTraversal { RewriteTraversal::BottomUp } // default; keep it
    fn matches(&self, plan: &LogicalPlan, ctx: &RewriteContext) -> bool;
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String>;
}
```
`RewriteResult` (`rewrite/result.rs`): `Unchanged | Changed(LogicalPlan) | Rejected(RewriteDiagnostic)`. Return `Ok(RewriteResult::Unchanged)` for "preconditions not met" (NOT `Err`).

**Mint a column inside a rule** (model: `scalar_apply_to_join.rs:149-152`):
```rust
let factory = ctx.column_ref_factory()
    .ok_or_else(|| "ApplyToWindow requires ColumnRefFactory".to_string())?;
let mut factory = factory.borrow_mut();
let win_id = factory.create(None, "<display name>".to_string(), data_type.clone(), nullable);
drop(factory);
```

**Test context with a factory** (model: `scalar_apply_to_join.rs:764-768`):
```rust
fn ctx_with_factory() -> RewriteContext {
    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    ctx.set_column_ref_factory(std::rc::Rc::new(std::cell::RefCell::new(ColumnRefFactory::new())));
    ctx
}
```

**Conjunct split/combine + column-ref test** (`rewrite/rules/utils.rs`):
```rust
pub(crate) fn split_and(expr: TypedExpr) -> Vec<TypedExpr>
pub(crate) fn combine_and(exprs: Vec<TypedExpr>) -> TypedExpr        // panics if empty — guard len>0
pub(crate) fn collect_column_id_refs(expr: &TypedExpr) -> HashSet<ColumnId>
pub(crate) fn collect_output_ids_ordered(plan: &LogicalPlan) -> Vec<ColumnId>
```

**Decorrelation helpers** (`rewrite/rules/subquery/decorrelate_util.rs`, `pub(super)` — same module):
```rust
fn orient_eq<'a>(conjunct: &'a TypedExpr, corr_ids: &HashSet<ColumnId>)
    -> Option<(&'a TypedExpr /*outer*/, &'a TypedExpr /*inner*/)>
```

**Schema accessor** (`src/sql/planner/mod.rs`, imported as `use crate::sql::planner::plan_output_columns;`):
```rust
pub(crate) fn plan_output_columns(plan: &LogicalPlan) -> Result<Vec<OutputColumn>, String>
```

**Node structs** (`src/sql/planner/plan.rs`):
```rust
WindowNode  { input: Box<LogicalPlan>, window_exprs: Vec<WindowExpr>,
              output_columns: Vec<OutputColumn>, required_output_columns: Option<HashSet<ColumnId>> }
WindowExpr  { name: String, args: Vec<TypedExpr>, distinct: bool,
              partition_by: Vec<TypedExpr>, order_by: Vec<SortItem>,
              window_frame: Option<WindowFrame>, result_type: DataType,
              output_name: String, output_column_id: ColumnId, ignore_nulls: bool }
SortNode    { input: Box<LogicalPlan>, items: Vec<SortItem>,
              analytic_partition_by: Vec<TypedExpr>, required_output_columns: Option<HashSet<ColumnId>> }
FilterNode  { input: Box<LogicalPlan>, predicate: TypedExpr, required_output_columns: Option<HashSet<ColumnId>> }
ProjectNode { input: Box<LogicalPlan>, items: Vec<ProjectItem>, output_qualifier: Option<String>,
              required_output_columns: Option<HashSet<ColumnId>> }
AggregateNode { input, group_by: Vec<TypedExpr>, aggregates: Vec<AggregateCall>,
                output_columns: Vec<OutputColumn>, already_pushed: bool, required_output_columns }
ScanNode    { database: String, table: TableDef, alias: Option<String>,
              columns: Vec<OutputColumn>, predicates: Vec<TypedExpr>, required_columns: Option<Vec<String>>,
              dict_columns: Vec<ScanDictionaryColumn>, required_output_columns: Option<HashSet<ColumnId>> }
```
`TableDef { name: String, columns: Vec<..>, iceberg_row_lineage_metadata_columns: Vec<..>, source: ScanSource }`.
`ScanSource::StarRocks { db_id: i64, table_id: i64 }` and the `Iceberg*` variants carry `table: IcebergTableInfo { catalog, namespace, table, table_uuid, .. }` (in `src/sql/catalog.rs`).

**Expr** (`src/sql/analysis/mod.rs`): `TypedExpr { kind: ExprKind, data_type: DataType, nullable: bool }`. `ExprKind::{ColumnRef { column_id, qualifier, column }, BinaryOp { left, op, right }, FunctionCall { name, args, distinct }, Literal(LiteralValue), IsNull { expr, negated }}`. `BinOp::{Eq,Ne,Lt,Le,Gt,Ge,And,Or,Add,Sub,Mul,Div,Mod,EqForNull}`. `SortItem { expr: TypedExpr, asc: bool, nulls_first: bool }`. **Neither `ExprKind` nor `TypedExpr` derives `PartialEq`** — Task 1 builds a physical-column-aware comparator.

**Post-`PushDownApplyAggFilter` shape the rule keys on** (research-confirmed). For
`... WHERE p.c='x' AND o.v < (SELECT 0.2*avg(i.w) FROM inner1 i WHERE i.k = p.k)` over `FROM o, p`:
```
Filter{ (p.k = o.fk) AND (p.c = 'x') AND (o.v < APPLY_OUT) }          ← the MATCHED node
  Apply{ kind: Scalar, need_check_max_rows: false,
         correlation_column_ids: [p.k_id],
         correlation_conjuncts: [ p.k_id == i.k_id ],                 ← (outer == inner), oriented
         output_column: APPLY_OUT, inner_output_column_id: VAL_ID }
    left:  Join{ Cross, condition: None }( Scan(o), Scan(p) )         ← outer subtree
    right: Project[ items: { VAL_ID := 0.2 * AVG_ID }, { i.k_id passthrough } ]
             Aggregate{ group_by: [i.k_id], aggregates: [ avg(i.w_id) -> AVG_ID ],
                        output_columns: [i.k_id, AVG_ID] }
               Scan(inner1 i)      (or Filter(residual)(Scan) if the subquery had residual preds)
```
- `APPLY_OUT` = `Apply.output_column.column_id`; `VAL_ID` = `Apply.inner_output_column_id` (the inner's single scalar output — `0.2*avg`, or just `avg` if no arithmetic wrapper).
- For q2 (`min(...)`, no arithmetic) there is **no leading Project**: `Apply.right` is the bare `Aggregate{ group_by:[corr key], min(..) -> VAL_ID }` and `inner_output_column_id == VAL_ID == AVG_ID`.

**Target output shape** the transform produces (replacing the matched `Filter`):
```
Filter{ o.v < <value_expr> }                              ← after-window (rewritten subquery comparison)
  Window[ avg(o.w_outer) OVER (PARTITION BY p.k_id) -> WIN_ID ]
    Sort{ items:[p.k_id ASC NULLS FIRST], analytic_partition_by:[p.k_id] }
      Filter{ (p.k = o.fk) AND (p.c = 'x') }              ← before-window (all outer conjuncts minus the subquery one)
        Join{ Cross }( Scan(o), Scan(p) )                 ← Apply.left, unchanged
```
where `<value_expr>` is the inner value expr (`0.2 * AVG_ID`) with `ColumnRef(AVG_ID)` rewritten to `ColumnRef(WIN_ID)` — i.e. `0.2 * WIN_ID` (for q2: just `WIN_ID`). The agg arg `i.w_id` is remapped to the **outer** instance `o.w_outer` by physical-column identity (Task 1).

---

### Task 1: `win_magic_util.rs` — table identity, column→table map, physical-column expr equality

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/win_magic_util.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs` (add `mod win_magic_util;`)

This is the foundation: the precondition checks (Task 2) need physical table identity (for table-set identity and "column belongs to table X"), and a structural-identity comparison that treats two scan instances of the same physical table as equal (StarRocks `PredicateComparator.isIdentical`). Plain `ColumnId` equality is wrong here (two scans of `lineitem` have different ColumnIds for `l_partkey`).

- [ ] **Step 1: Write failing unit tests** in a `#[cfg(test)]` module in the new file. Model fixtures on `scalar_apply_to_join.rs:703-737` (`make_t2_scan`). Cover:
  - `table_identity_from_starrocks_scan`: a `ScanNode` with `ScanSource::StarRocks { db_id: 7, table_id: 42 }` → `TableIdentity::StarRocks { db_id: 7, table_id: 42 }`.
  - `collect_table_ids_two_scans_under_join`: a `Join{Cross}(Scan(table_id=1), Scan(table_id=2))` → set `{StarRocks{0,1}, StarRocks{0,2}}` and the returned `Vec` has length 2 (no dup).
  - `collect_table_ids_self_join_detects_dup`: a `Join{Cross}(Scan(table_id=1), Scan(table_id=1))` → `Vec` length 2 but set length 1 (caller uses `vec.len() != set.len()` to reject self-joins).
  - `column_to_table_map_resolves_scan_column`: build a `Scan(table_id=5)` exposing `ColumnId(3)` named `"l_partkey"`; `collect_scan_column_map(plan)` maps `ColumnId(3) -> (StarRocks{0,5}, "l_partkey")`.
  - `expr_phys_eq_same_physical_column_diff_instance`: two `ColumnRef`s with **different** `ColumnId`s but both mapping (via the map) to `(StarRocks{0,5}, "l_partkey")` compare **equal**; mapping to different `(table,col)` compare **unequal**; a `ColumnId` not in the map compares unequal to anything.
  - `expr_phys_eq_binary_op_structural`: `a Eq b` vs `a Eq b` (with phys-equal sides) → equal; `a Eq b` vs `a Lt b` → unequal; literal `Int(5)` vs `Int(5)` → equal, vs `Int(6)` → unequal.

  Run: `cargo test --lib -- win_magic_util` → FAIL (module/helpers absent).

- [ ] **Step 2: Implement `win_magic_util.rs`:**

```rust
//! Helpers for `ApplyToWindow` (StarRocks WinMagic): physical table identity,
//! ColumnId -> (table, column) resolution, and structural expression equality
//! that ignores which scan instance a column came from.

use std::collections::{HashMap, HashSet};

use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::{LogicalPlan, ScanNode};

/// Physical identity of a scanned table. Two scans of the same physical table
/// (e.g. a self-join's two legs, or an outer table re-scanned in a subquery)
/// share one `TableIdentity`, even though their output ColumnIds differ.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum TableIdentity {
    StarRocks { db_id: i64, table_id: i64 },
    Iceberg { catalog: String, namespace: String, table: String, table_uuid: Option<String> },
}

impl TableIdentity {
    pub(super) fn from_scan(scan: &ScanNode) -> Self {
        match &scan.table.source {
            ScanSource::StarRocks { db_id, table_id } => {
                TableIdentity::StarRocks { db_id: *db_id, table_id: *table_id }
            }
            ScanSource::IcebergDataFiles { table, .. }
            | ScanSource::IcebergMetadataTable { table, .. }
            | ScanSource::IcebergDeltaTable { table, .. }
            | ScanSource::IcebergVersionTable { table, .. } => TableIdentity::Iceberg {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
                table_uuid: table.table_uuid.clone(),
            },
            // MV-target-state scans never appear inside a SELECT-time subquery;
            // if one does, WinMagic must not fire. Use a name-based identity that
            // can still distinguish tables but never matches an ordinary scan
            // (callers reject via the operator whitelist before this is reached).
            ScanSource::IcebergMvTargetState(mv) => TableIdentity::Iceberg {
                catalog: format!("__mv__{}", mv.catalog),
                namespace: mv.database.clone(),
                table: mv.table.clone(),
                table_uuid: None,
            },
        }
    }
}

/// Collect the physical table identities of every `Scan` in `plan`, in
/// left-to-right order, WITH duplicates preserved (so callers can detect a
/// self-join / duplicate-table by comparing `Vec::len()` against the set size).
pub(super) fn collect_table_ids(plan: &LogicalPlan) -> Vec<TableIdentity> {
    let mut out = Vec::new();
    collect_table_ids_inner(plan, &mut out);
    out
}

fn collect_table_ids_inner(plan: &LogicalPlan, out: &mut Vec<TableIdentity>) {
    match plan {
        LogicalPlan::Scan(s) => out.push(TableIdentity::from_scan(s)),
        LogicalPlan::Join(j) => {
            collect_table_ids_inner(&j.left, out);
            collect_table_ids_inner(&j.right, out);
        }
        LogicalPlan::Filter(n) => collect_table_ids_inner(&n.input, out),
        LogicalPlan::Project(n) => collect_table_ids_inner(&n.input, out),
        LogicalPlan::Aggregate(n) => collect_table_ids_inner(&n.input, out),
        LogicalPlan::Sort(n) => collect_table_ids_inner(&n.input, out),
        LogicalPlan::Window(n) => collect_table_ids_inner(&n.input, out),
        LogicalPlan::AssertOneRow(n) => collect_table_ids_inner(&n.input, out),
        LogicalPlan::Apply(a) => {
            collect_table_ids_inner(&a.left, out);
            collect_table_ids_inner(&a.right, out);
        }
        // Leaves and shapes the operator whitelist (Task 2) already rejects:
        // Values / GenerateSeries / Limit / Union / ... contribute nothing here.
        _ => {}
    }
}

/// Build `ColumnId -> (TableIdentity, physical_column_name)` by walking every
/// `Scan` in `plan` and recording its output columns. A column produced by a
/// Project/Aggregate/Window (not a base scan column) is intentionally absent;
/// the predicate-identity check only compares base-table column references.
pub(super) fn collect_scan_column_map(plan: &LogicalPlan) -> HashMap<ColumnId, (TableIdentity, String)> {
    let mut map = HashMap::new();
    collect_scan_column_map_inner(plan, &mut map);
    map
}

fn collect_scan_column_map_inner(
    plan: &LogicalPlan,
    map: &mut HashMap<ColumnId, (TableIdentity, String)>,
) {
    match plan {
        LogicalPlan::Scan(s) => {
            let id = TableIdentity::from_scan(s);
            for c in &s.columns {
                map.insert(c.column_id, (id.clone(), c.name.clone()));
            }
        }
        LogicalPlan::Join(j) => {
            collect_scan_column_map_inner(&j.left, map);
            collect_scan_column_map_inner(&j.right, map);
        }
        LogicalPlan::Filter(n) => collect_scan_column_map_inner(&n.input, map),
        LogicalPlan::Project(n) => collect_scan_column_map_inner(&n.input, map),
        LogicalPlan::Aggregate(n) => collect_scan_column_map_inner(&n.input, map),
        LogicalPlan::Sort(n) => collect_scan_column_map_inner(&n.input, map),
        LogicalPlan::Window(n) => collect_scan_column_map_inner(&n.input, map),
        LogicalPlan::AssertOneRow(n) => collect_scan_column_map_inner(&n.input, map),
        LogicalPlan::Apply(a) => {
            collect_scan_column_map_inner(&a.left, map);
            collect_scan_column_map_inner(&a.right, map);
        }
        _ => {}
    }
}

/// Structural equality of two expressions where a `ColumnRef` is compared by its
/// resolved physical `(TableIdentity, column_name)` rather than by `ColumnId`.
/// A ColumnRef whose id is absent from `map` (e.g. a derived/agg column) only
/// matches another ColumnRef with the *same* `ColumnId`. Mirrors StarRocks
/// `PredicateComparator.isIdentical`.
pub(super) fn expr_phys_eq(
    a: &TypedExpr,
    b: &TypedExpr,
    map: &HashMap<ColumnId, (TableIdentity, String)>,
) -> bool {
    match (&a.kind, &b.kind) {
        (
            ExprKind::ColumnRef { column_id: ia, .. },
            ExprKind::ColumnRef { column_id: ib, .. },
        ) => match (map.get(ia), map.get(ib)) {
            (Some(pa), Some(pb)) => pa == pb,
            _ => ia == ib,
        },
        (
            ExprKind::BinaryOp { left: la, op: oa, right: ra },
            ExprKind::BinaryOp { left: lb, op: ob, right: rb },
        ) => oa == ob && expr_phys_eq(la, lb, map) && expr_phys_eq(ra, rb, map),
        (
            ExprKind::FunctionCall { name: na, args: aa, distinct: da },
            ExprKind::FunctionCall { name: nb, args: ab, distinct: db },
        ) => {
            na == nb
                && da == db
                && aa.len() == ab.len()
                && aa.iter().zip(ab).all(|(x, y)| expr_phys_eq(x, y, map))
        }
        (ExprKind::IsNull { expr: ea, negated: ga }, ExprKind::IsNull { expr: eb, negated: gb }) => {
            ga == gb && expr_phys_eq(ea, eb, map)
        }
        (ExprKind::Literal(la), ExprKind::Literal(lb)) => literal_eq(la, lb),
        // Any other / mixed kinds: fall back to debug-structural equality, which
        // is conservative (stricter). Two distinct shapes never compare equal.
        _ => format!("{:?}", a.kind) == format!("{:?}", b.kind),
    }
}

fn literal_eq(a: &LiteralValue, b: &LiteralValue) -> bool {
    format!("{a:?}") == format!("{b:?}")
}
```

> **Note on `BinOp`/`LiteralValue` deriving `PartialEq`:** `BinOp` derives `PartialEq` (used as `op: BinOp::Eq` matches throughout the codebase, and the agent confirmed `WindowFrameType`/`ColumnId` derive it). If `op: oa == ob` fails to compile because `BinOp` lacks `PartialEq`, compare with `format!("{oa:?}") == format!("{ob:?}")` instead. `LiteralValue` is compared via debug-format (`literal_eq`) precisely to avoid depending on its derives.

- [ ] **Step 3: Declare the module** in `subquery/mod.rs`: add `mod win_magic_util;` next to the other `mod` lines (no `pub(crate) use` needed — the rule in Task 2 is in a sibling module and imports `use super::win_magic_util::{...}`).

- [ ] **Step 4: Run tests + build clean.**
  Run: `cargo test --lib -- win_magic_util` → PASS.
  Run: `cargo build 2>&1 | tail -2` → success. `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 5: Commit**
```bash
git add -A && git commit -m "feat(optimizer): WinMagic util — table identity + physical-column expr equality

Adds win_magic_util for the ApplyToWindow rule: TableIdentity (StarRocks
db/table id or Iceberg FQN+uuid) with from_scan; collect_table_ids
(dup-preserving, for self-join detection); collect_scan_column_map
(ColumnId -> (table, physical column name)); and expr_phys_eq, a structural
comparator that treats two scan instances of the same physical column as
equal (ports StarRocks PredicateComparator.isIdentical). No rule wired yet."
```

---

### Task 2: `ApplyToWindow` rule skeleton + `matches()` + precondition checks

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/subquery/apply_to_window.rs`
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`

This task implements everything up to (but not including) the tree-building transform: the `LogicalRewriteRule` impl, `matches()`, and a `check_preconditions()` that returns the data the transform (Task 3) needs OR `None` (→ `Unchanged`). Until Task 3, `apply()` returns `Unchanged` even on a successful check (so the rule is a no-op that always falls through to `ScalarApplyToJoin`).

**Matched/normalized state assumed** (from `PushDownApplyAggFilter`, see toolbox): `plan` is `Filter`; `filter.input` is `Apply { kind: Scalar, need_check_max_rows == false, !correlation_conjuncts.is_empty() }`; `Apply.right` is `[Project?] Aggregate{ group_by: <inner corr keys>, aggregates: [single] }`.

- [ ] **Step 1: Write failing unit tests** (`#[cfg(test)]`, fixtures modeled on `scalar_apply_to_join.rs`). Build a q17-shaped `Filter(Apply)` helper `fn winmagic_filter_apply() -> LogicalPlan` with two outer scans (`lineitem` table_id=1, `part` table_id=2), inner scan re-using `lineitem` table_id=1, `avg(l_quantity)` inner agg grouped by inner `l_partkey`, `correlation_conjuncts = [part.p_partkey == inner.l_partkey]`, and a WHERE Filter `(part.p_partkey == lineitem.l_partkey) AND (part.p_brand == 'x') AND (lineitem.l_quantity < APPLY_OUT)`. Tests assert via a temporary `pub(super) fn check_preconditions(...) -> Option<WinMagicMatch>` (return type defined below):
  - `precond_accepts_q17_shape`: returns `Some`.
  - `precond_rejects_non_whitelist_agg`: inner agg `array_agg` → `None`.
  - `precond_rejects_distinct_agg`: inner agg `avg` with `distinct: true` → `None`.
  - `precond_rejects_two_aggregates`: inner agg list has 2 calls → `None`.
  - `precond_rejects_self_join_outer`: outer has two `lineitem` (table_id=1) scans → `None`.
  - `precond_rejects_table_set_mismatch`: subquery scans a table absent from outer → `None`.
  - `precond_rejects_limit_in_subtree`: a `Limit` node in the outer subtree → `None`.
  - `precond_rejects_predicate_mismatch`: the subquery has a residual Filter conjunct with NO outer twin → `None`.
  - `precond_rejects_no_subquery_conjunct`: the WHERE Filter does not reference `APPLY_OUT` (subquery output only used in projection) → `None`.

  Run: `cargo test --lib -- apply_to_window` → FAIL.

- [ ] **Step 2: Define the match-result struct and skeleton** in `apply_to_window.rs`:

```rust
//! `ApplyToWindow` — ports StarRocks `ScalarApply2AnalyticRule` ("WinMagic").
//!
//! Rewrites `Filter( ... lhs op APPLY_OUT ... )` over a decorrelated correlated
//! scalar-aggregate `Apply` into a `Window` (analytic) over the OUTER relation,
//! discarding the subquery subtree. Runs BEFORE `ScalarApplyToJoin`; on any
//! precondition failure returns `Unchanged` so `ScalarApplyToJoin` produces the
//! M1 join form. Never errors (the join form is always a valid fallback).

use std::collections::{HashMap, HashSet};

use super::win_magic_util::{collect_scan_column_map, collect_table_ids, expr_phys_eq, TableIdentity};
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};
use crate::sql::planner::plan::{AggregateCall, AggregateNode, ApplyKind, ApplyNode, FilterNode, LogicalPlan};

const WHITELIST: &[&str] = &["count", "sum", "avg", "min", "max"];

pub(crate) struct ApplyToWindow;

/// Everything Task 3's transform needs, validated by `check_preconditions`.
pub(super) struct WinMagicMatch {
    /// All conjuncts of the matched WHERE Filter (already AND-split).
    pub outer_conjuncts: Vec<TypedExpr>,
    /// The single outer conjunct that references `APPLY_OUT` (the subquery
    /// comparison, e.g. `l_quantity < APPLY_OUT`).
    pub subquery_conjunct: TypedExpr,
    /// Outer-side ColumnRef of each correlation conjunct — the window PARTITION BY keys.
    pub partition_by: Vec<TypedExpr>,
    /// The inner single aggregate call (name in WHITELIST, non-distinct).
    pub inner_agg: AggregateCall,
}

impl LogicalRewriteRule for ApplyToWindow {
    fn name(&self) -> &'static str { "ApplyToWindow" }
    fn phase(&self) -> RewritePhase { RewritePhase::StructuralRewrite }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        let LogicalPlan::Filter(f) = plan else { return false };
        let LogicalPlan::Apply(a) = f.input.as_ref() else { return false };
        a.kind == ApplyKind::Scalar
            && !a.need_check_max_rows
            && !a.correlation_conjuncts.is_empty()
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Filter(f) = &plan else { return Ok(RewriteResult::Unchanged) };
        let LogicalPlan::Apply(a) = f.input.as_ref() else { return Ok(RewriteResult::Unchanged) };
        let Some(_m) = check_preconditions(&f.predicate, a) else {
            return Ok(RewriteResult::Unchanged);
        };
        // Task 3 replaces this with the transform.
        Ok(RewriteResult::Unchanged)
    }
}
```

- [ ] **Step 3: Implement `check_preconditions`** (port of StarRocks `check*` methods). Place it as a `pub(super) fn` below the impl:

```rust
/// Port of StarRocks ScalarApply2AnalyticRule's check() family. Returns the
/// validated match data, or None if any precondition fails (→ caller Unchanged).
pub(super) fn check_preconditions(
    where_pred: &TypedExpr,
    a: &ApplyNode,
) -> Option<WinMagicMatch> {
    // (0) Inner: peel optional leading Project, require a vector Aggregate with a
    // single non-DISTINCT whitelisted aggregate. (PushDownApplyAggFilter already
    // grouped it by the correlation key.)
    let agg = peel_to_aggregate(&a.right)?;
    if agg.aggregates.len() != 1 { return None; }
    let inner_agg = agg.aggregates[0].clone();
    if inner_agg.distinct { return None; }
    if !WHITELIST.contains(&inner_agg.name.as_str()) { return None; }

    // (1) No LIMIT and only whitelisted operators in either subtree.
    if !operator_whitelist_ok(&a.left, /*is_subquery=*/ false) { return None; }
    if !operator_whitelist_ok(&a.right, /*is_subquery=*/ true) { return None; }

    // (2) Table-set identity: outerTables == subqueryTables + exactly 1 extra;
    // no duplicate physical table on either side (rejects self-joins).
    let outer_tabs = collect_table_ids(&a.left);
    let sub_tabs = collect_table_ids(&a.right);
    let outer_set: HashSet<TableIdentity> = outer_tabs.iter().cloned().collect();
    let sub_set: HashSet<TableIdentity> = sub_tabs.iter().cloned().collect();
    if outer_tabs.len() != outer_set.len() || sub_tabs.len() != sub_set.len() { return None; } // dup
    if outer_set.len() != sub_set.len() + 1 { return None; }
    if !sub_set.is_subset(&outer_set) { return None; }
    // The single extra table is the "correlated outer table" (e.g. `part`).
    let extra: Vec<&TableIdentity> = outer_set.difference(&sub_set).collect();
    if extra.len() != 1 { return None; }
    let correlated_outer_table = extra[0].clone();

    // (3) Partition-by keys = outer side of each correlation conjunct. Verify each
    // outer side is a ColumnRef of `correlated_outer_table`.
    let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
    let col_map = collect_scan_column_map(&a.left); // outer columns -> (table, name)
    let mut partition_by = Vec::new();
    for conj in &a.correlation_conjuncts {
        let (outer_side, _inner) = super::decorrelate_util::orient_eq(conj, &corr_ids)?;
        let ExprKind::ColumnRef { column_id, .. } = &outer_side.kind else { return None; };
        match col_map.get(column_id) {
            Some((tab, _)) if *tab == correlated_outer_table => {}
            _ => return None,
        }
        partition_by.push(outer_side.clone());
    }

    // (4) Predicate identity (StarRocks checkPredicate, 4 steps). Work on a
    // physical-column map spanning BOTH subtrees so inner/outer instances unify.
    let full_map = {
        let mut m = collect_scan_column_map(&a.left);
        m.extend(collect_scan_column_map(&a.right));
        m
    };
    let mut outer_conjuncts = split_and(where_pred.clone());

    // 4a. Each correlation conjunct must have a phys-identical twin among the
    //     outer conjuncts; remove matched pairs. After: every correlation
    //     conjunct matched.
    let mut unmatched_corr = a.correlation_conjuncts.clone();
    unmatched_corr.retain(|cc| {
        if let Some(pos) = outer_conjuncts.iter().position(|oc| expr_phys_eq(cc, oc, &full_map)) {
            outer_conjuncts.remove(pos);
            false // matched → drop from unmatched
        } else {
            true
        }
    });
    if !unmatched_corr.is_empty() { return None; }

    // 4b. Exactly the subquery-comparison conjunct references APPLY_OUT; remove it.
    let apply_out = a.output_column.column_id;
    let sub_pos = outer_conjuncts
        .iter()
        .position(|oc| collect_column_id_refs(oc).contains(&apply_out))?;
    let subquery_conjunct = outer_conjuncts.remove(sub_pos);
    // No OTHER conjunct may reference APPLY_OUT.
    if outer_conjuncts.iter().any(|oc| collect_column_id_refs(oc).contains(&apply_out)) {
        return None;
    }

    // 4c. Drop outer conjuncts that reference ONLY `correlated_outer_table`
    //     (e.g. p_brand='x'); these are fine (they become before-window filters).
    outer_conjuncts.retain(|oc| {
        let refs = collect_column_id_refs(oc);
        let only_extra = !refs.is_empty()
            && refs.iter().all(|id| matches!(col_map.get(id), Some((t, _)) if *t == correlated_outer_table));
        !only_extra
    });

    // 4d. Remaining outer conjuncts must 1:1 phys-match the subquery's residual
    //     Filter conjuncts (the Filter still under the inner aggregate, if any).
    let mut sub_residual = subquery_residual_conjuncts(&a.right);
    if outer_conjuncts.len() != sub_residual.len() { return None; }
    for oc in &outer_conjuncts {
        match sub_residual.iter().position(|sc| expr_phys_eq(oc, sc, &full_map)) {
            Some(pos) => { sub_residual.remove(pos); }
            None => return None,
        }
    }
    // (all matched; both lists now empty)

    Some(WinMagicMatch {
        outer_conjuncts: split_and(where_pred.clone()),
        subquery_conjunct,
        partition_by,
        inner_agg,
    })
}
```

  Implement these `fn`s in the same file (concrete, no placeholders):
  - `fn peel_to_aggregate(plan: &LogicalPlan) -> Option<&AggregateNode>`: if `Project`, recurse into `.input`; if `Aggregate`, return it; else `None`. (One level of leading Project is what `PushDownApplyAggFilter` produces.)
  - `fn operator_whitelist_ok(plan: &LogicalPlan, is_subquery: bool) -> bool`: recurse; allow `Scan`; `Join` only if `join_type == JoinKind::Cross` (recurse both); `Filter`/`Project` (recurse input); for `is_subquery` also allow `Aggregate` (recurse input). Any other node (`Limit`, `Sort`, `Window`, `Union`, `Apply`, …) → `false`. (This is the no-limit + operator-whitelist + cross-join-only checks fused, matching StarRocks `checkOperatorType`/`checkJoinType`.)
  - `fn subquery_residual_conjuncts(apply_right: &LogicalPlan) -> Vec<TypedExpr>`: peel optional leading `Project`, then the `Aggregate`; if the aggregate's `.input` is a `Filter`, return `split_and(filter.predicate.clone())`; else return `vec![]`. (After `PushDownApplyAggFilter`, the correlated conjunct is already hoisted, so only true residual remains.)

- [ ] **Step 4: Register the module** in `subquery/mod.rs`: add `mod apply_to_window;` and `pub(crate) use apply_to_window::ApplyToWindow;` (alongside the existing `use`s). Do **not** add it to `subquery_rewrite_rules()` yet — that is Task 4.

- [ ] **Step 5: Run tests + build clean.**
  Run: `cargo test --lib -- apply_to_window` → all PASS.
  Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 6: Commit**
```bash
git add -A && git commit -m "feat(optimizer): ApplyToWindow rule skeleton + WinMagic preconditions

Adds the ApplyToWindow LogicalRewriteRule with matches() (Filter over a
decorrelated correlated scalar-aggregate Apply) and check_preconditions(),
a faithful port of StarRocks ScalarApply2AnalyticRule's checks: single
non-distinct whitelisted aggregate; no-limit + cross-join-only operator
whitelist; table-set identity (outer == subquery + exactly one extra,
self-join rejected); and the 4-step predicate-identity match (correlation
twin, subquery comparison, extra-table-only filters, 1:1 residual). apply()
returns Unchanged for now (transform lands next), so the rule is a no-op
that always falls through to ScalarApplyToJoin. Not yet registered."
```

---

### Task 3: `ApplyToWindow::apply` — the transform (Window + Sort + before/after Filters)

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/subquery/apply_to_window.rs`

Replace the `apply()` stub with the real transform, using the `WinMagicMatch` from Task 2.

- [ ] **Step 1: Write failing unit tests** (extend the `#[cfg(test)]` module). Reuse the q17-shaped `winmagic_filter_apply()` fixture. After `rule.apply(plan, &mut ctx)`:
  - `transform_emits_window_over_outer`: result is `Filter` (after-window). Its `.input` is `Window` with exactly one `WindowExpr` whose `name == "avg"`, whose `partition_by` is one `ColumnRef` to the **outer** `part.p_partkey` id, and whose single `args[0]` `ColumnRef` resolves to the **outer** `lineitem` instance (NOT the inner scan's id — assert the id equals the outer `l_quantity` id, proving the inner→outer remap). The `Window.input` is `Sort` (items length 1, `analytic_partition_by` length 1). The `Sort.input` is `Filter` (before-window) whose predicate's conjunct set excludes the subquery comparison and includes `p_partkey == l_partkey` + `p_brand == 'x'`. The before-window `Filter.input` is the original `Join{Cross}` (Apply.left) — assert no `Apply` and no inner `Aggregate` anywhere in the result (`super::super::find_residual_apply(&result).is_none()` and a small walker for `Aggregate` finding only the *top* one if the fixture has it; for this fixture the inner agg must be gone).
  - `transform_rewrites_subquery_comparison_to_window_col`: the after-window `Filter` predicate is `l_quantity < <value_expr>` where `<value_expr>` references the minted window output id (`WIN_ID`), NOT `APPLY_OUT` and NOT the inner `AVG_ID`. For the q2-shape (no leading Project, `min`), `<value_expr>` is the bare `ColumnRef(WIN_ID)`.
  - `transform_disabled_via_unchanged_when_precondition_fails`: feed a self-join fixture; assert `RewriteResult::Unchanged`.

  Run: `cargo test --lib -- apply_to_window` → FAIL on the new tests.

- [ ] **Step 2: Implement the transform.** Replace `apply()`'s body after the `check_preconditions` call:

```rust
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Filter(f) = &plan else { return Ok(RewriteResult::Unchanged) };
        let LogicalPlan::Apply(a) = f.input.as_ref() else { return Ok(RewriteResult::Unchanged) };
        let Some(m) = check_preconditions(&f.predicate, a) else {
            return Ok(RewriteResult::Unchanged);
        };

        // Re-own the pieces. (matches() guaranteed Filter(Apply).)
        let LogicalPlan::Filter(f) = plan else { unreachable!() };
        let LogicalPlan::Apply(a) = *f.input else { unreachable!() };

        // --- 1. Remap the inner aggregate's args from inner-scan columns to the
        // outer instance of the same physical column. ---
        let outer_map = collect_scan_column_map(&a.left);             // outer ColumnId -> (table, col)
        let inner_map = collect_scan_column_map(&a.right);            // inner ColumnId -> (table, col)
        // Reverse lookup for outer: (table, col) -> outer ColumnId (with type via plan_output_columns).
        let outer_cols = crate::sql::planner::plan_output_columns(&a.left)?;
        let mut phys_to_outer: HashMap<(TableIdentity, String), &crate::sql::analysis::OutputColumn> =
            HashMap::new();
        for oc in &outer_cols {
            if let Some((tab, name)) = outer_map.get(&oc.column_id) {
                phys_to_outer.insert((tab.clone(), name.clone()), oc);
            }
        }
        let mut agg_args = m.inner_agg.args.clone();
        for arg in &mut agg_args {
            if !remap_inner_to_outer(arg, &inner_map, &phys_to_outer) {
                // A required column is not available on the outer side — bail out
                // to the join form rather than emit a dangling reference.
                return Ok(RewriteResult::Unchanged);
            }
        }

        // --- 2. Mint the window output column; build the WindowExpr. ---
        let factory = ctx
            .column_ref_factory()
            .ok_or_else(|| "ApplyToWindow requires ColumnRefFactory".to_string())?;
        let win_id = factory.borrow_mut().create(
            None,
            format!("{}_window", m.inner_agg.name),
            m.inner_agg.result_type.clone(),
            true,
        );
        let win_expr = crate::sql::planner::plan::WindowExpr {
            name: m.inner_agg.name.clone(),
            args: agg_args,
            distinct: false,
            partition_by: m.partition_by.clone(),
            order_by: vec![],
            window_frame: None,
            result_type: m.inner_agg.result_type.clone(),
            output_name: format!("{}_window", m.inner_agg.name),
            output_column_id: win_id,
            ignore_nulls: false,
        };

        // --- 3. before-window Filter = all outer conjuncts except the subquery one. ---
        let before: Vec<TypedExpr> = m
            .outer_conjuncts
            .iter()
            .filter(|oc| !expr_struct_eq(oc, &m.subquery_conjunct))
            .cloned()
            .collect();
        let outer_subtree = *a.left;
        let before_filtered = if before.is_empty() {
            outer_subtree
        } else {
            LogicalPlan::Filter(FilterNode {
                predicate: crate::sql::optimizer::rewrite::rules::utils::combine_and(before),
                input: Box::new(outer_subtree),
                required_output_columns: None,
            })
        };

        // --- 4. Sort(partition keys) under the Window. ---
        let sort_items: Vec<crate::sql::analysis::SortItem> = m
            .partition_by
            .iter()
            .map(|e| crate::sql::analysis::SortItem { expr: e.clone(), asc: true, nulls_first: true })
            .collect();
        let sorted = LogicalPlan::Sort(crate::sql::planner::plan::SortNode {
            input: Box::new(before_filtered),
            items: sort_items,
            analytic_partition_by: m.partition_by.clone(),
            required_output_columns: None,
        });

        // --- 5. Window node: output = base outer columns + the window column. ---
        let mut window_output = crate::sql::planner::plan_output_columns(&sorted)?;
        window_output.push(crate::sql::analysis::OutputColumn {
            column_id: win_id,
            name: format!("{}_window", m.inner_agg.name),
            data_type: m.inner_agg.result_type.clone(),
            nullable: true,
            is_internal: true,
        });
        let window = LogicalPlan::Window(crate::sql::planner::plan::WindowNode {
            input: Box::new(sorted),
            window_exprs: vec![win_expr],
            output_columns: window_output,
            required_output_columns: None,
        });

        // --- 6. after-window Filter = subquery comparison with APPLY_OUT replaced
        // by the inner value expr (with the inner agg column rewritten to win_id). ---
        let value_expr = build_value_expr(&a.right, a.inner_output_column_id, m.inner_agg.output_column_id, win_id, &m.inner_agg.result_type)?;
        let mut after_pred = m.subquery_conjunct.clone();
        replace_column_ref(&mut after_pred, a.output_column.column_id, &value_expr);
        let after = LogicalPlan::Filter(FilterNode {
            predicate: after_pred,
            input: Box::new(window),
            required_output_columns: None,
        });

        Ok(RewriteResult::Changed(after))
    }
```

  Implement the helpers (concrete):
  - `fn remap_inner_to_outer(expr: &mut TypedExpr, inner_map, phys_to_outer) -> bool`: recurse; for each `ExprKind::ColumnRef { column_id, qualifier, column }`, look up `inner_map.get(column_id)` → `(tab, name)`, then `phys_to_outer.get(&(tab, name))` → outer `OutputColumn`; if found, overwrite `column_id`/`column` (and the `TypedExpr.data_type`/`nullable`) with the outer column's; return `false` if any ColumnRef can't be remapped (column absent on outer side). Recurse into `BinaryOp`/`FunctionCall`/`IsNull` args. Non-ColumnRef leaves (literals) → `true`.
  - `fn build_value_expr(apply_right, inner_output_col_id, agg_out_col_id, win_id, win_type) -> Result<TypedExpr, String>`: if `apply_right` has a leading `Project` with an item whose `output_column_id == inner_output_col_id`, clone that item's `expr` and `replace_column_ref(&mut e, agg_out_col_id, &col_ref(win_id, win_type))`, return it. Otherwise (no Project; `inner_output_col_id == agg_out_col_id`) return a bare `ColumnRef(win_id)` of `win_type`. (`col_ref` is a 2-line local builder.)
  - `fn replace_column_ref(expr: &mut TypedExpr, target: ColumnId, replacement: &TypedExpr)`: recurse; when a `ColumnRef`'s `column_id == target`, overwrite `*expr = replacement.clone()`.
  - `fn expr_struct_eq(a: &TypedExpr, b: &TypedExpr) -> bool { format!("{:?}", a.kind) == format!("{:?}", b.kind) }` (used only to delete the exact subquery conjunct from the before-window set — both come from the same `where_pred` so debug-eq is exact here).

- [ ] **Step 3: Run tests + build clean.**
  Run: `cargo test --lib -- apply_to_window` → all PASS.
  Run: `cargo build` and `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed; `cargo clippy --lib 2>&1 | grep -i apply_to_window` → no new lints.

- [ ] **Step 4: Commit**
```bash
git add -A && git commit -m "feat(optimizer): ApplyToWindow transform — emit Window over the outer relation

On a successful WinMagic match, discard the subquery subtree and emit
Filter(after-window) -> Window(agg OVER PARTITION BY outer corr keys) ->
Sort(partition keys) -> Filter(before-window) -> outer subtree. The inner
aggregate's args are remapped from the inner scan to the outer instance of
the same physical column; the subquery comparison's APPLY_OUT reference is
rewritten to the inner value expression with the aggregate column replaced
by the window output column. Falls back to Unchanged (the join form) if a
required column is unavailable on the outer side."
```

---

### Task 4: Register `ApplyToWindow` before `ScalarApplyToJoin` + registry/ordering tests

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/subquery/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs` (tests)

- [ ] **Step 1: Insert into `subquery_rewrite_rules()`** (BEFORE `ScalarApplyToJoin`, AFTER the push-down rules):
```rust
pub(crate) fn subquery_rewrite_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![
        Box::new(PushDownApplyAggFilter),
        Box::new(PushDownApplyFilter),
        Box::new(ApplyToWindow),       // to-window BEFORE to-join (StarRocks ordering)
        Box::new(ScalarApplyToJoin),
        Box::new(ApplyException),      // must stay LAST
    ]
}
```
(`ApplyToWindow` matches `Filter(Apply)`; `ScalarApplyToJoin` matches the `Apply` node itself. When `ApplyToWindow` fires it removes the Apply, so `ScalarApplyToJoin` no-ops; when it returns `Unchanged`, `ScalarApplyToJoin` produces the join form. The fixpoint pipeline applies each rule to the whole tree in vec order per iteration, so `PushDownApplyAggFilter` normalizes the Apply in the same iteration before `ApplyToWindow` is tried.)

- [ ] **Step 2: Update registry tests.** In `registry.rs`, find the test(s) asserting the known-rule-name set (e.g. `rewrite_registry_recognizes_migrated_query_rules`, ~line 216) and add `assert!(is_known_rewrite_rule_name("ApplyToWindow"));`. If any test asserts the exact ordered `rule_names()` of the SubqueryRewrite stage, insert `"ApplyToWindow"` between `"PushDownApplyFilter"` and `"ScalarApplyToJoin"`. Run them first to see the failure, then update.

- [ ] **Step 3: Add a pipeline integration test** in `apply_to_window.rs` `#[cfg(test)]`: feed `winmagic_filter_apply()` (a correlated scalar-**agg** Apply that has NOT yet been through PushDownApplyAggFilter — i.e. `Apply.right = Aggregate{group_by:[], avg}(Filter(corr)(Scan))`, `need_check_max_rows = true`, `correlation_conjuncts = []`, wrapped in the WHERE `Filter`) through the **full** `query_rewrite_pipeline(&HashMap::new()).rewrite(plan, &mut ctx)` (model: `subquery/mod.rs:128-137`). Assert the result contains a `Window`, contains **no** `Apply` (`find_residual_apply(&result).is_none()`), and the inner subquery `Aggregate` (group_by over the inner correlation) is gone. Add a second variant disabling the rule: `RewriteContext::for_query(vec!["ApplyToWindow".to_string()])` → assert the result contains a `Join` (LEFT OUTER, the M1 form) and **no** `Window`.

- [ ] **Step 4: Run + build.**
  Run: `cargo test --lib -- apply_to_window registry` → PASS. `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 5: Commit**
```bash
git add -A && git commit -m "feat(optimizer): register ApplyToWindow before ScalarApplyToJoin

ApplyToWindow runs in the SubqueryRewrite stage ahead of ScalarApplyToJoin
so the window form wins over the join form (StarRocks SUBQUERY_REWRITE_TO_
WINDOW before _TO_JOIN). disable_optimizer_rules='ApplyToWindow' falls back
to the M1 LEFT OUTER JOIN form. Registry name set + a full-pipeline
integration test (window appears, no Apply; disabled -> join, no window)."
```

---

### Task 5: Plan-golden + SQL correctness (apply mode), incl. rejection cases

**Files:**
- Add: `sql-tests/optimizer/sql/subquery_scalar_to_window.sql` (+ recorded `.result`)
- Add: `sql-tests/optimizer/sql/subquery_scalar_to_window_rejected.sql` (+ `.result`)
- Possibly add: an in-crate end-to-end test if per-case `SET` is unavailable (see note).

Convention (confirmed): files in `sql-tests/optimizer/sql/` are auto-discovered; goldens live in `sql-tests/optimizer/result/<name>.result`; record with `--mode record --record-from target --update-expected`; the optimizer suite default catalog is `iceberg_opt`. Use `${case_db}` for table names, `ANALYZE TABLE` after load (stats drive the window-vs-join cost path), and `EXPLAIN VERBOSE` for plan-shape steps. Per-case session vars are inline `SET ...;` steps (the runner talks to a live server; the TLS install per statement applies). **First confirm** `SET subquery_unnest_mode='apply';` takes effect per-case by recording one EXPLAIN and checking the `WINDOW` line appears; if the runner does not honor per-case SET, fall back to the in-crate e2e test below and note it in the case comment.

- [ ] **Step 1: Positive plan-golden** `subquery_scalar_to_window.sql`. Header + a q17-shaped case sized so the rule's cost path fires (give the tables real `ANALYZE`d stats; tiny fixtures may still pick window since WinMagic is rewrite-stage, not cost-gated — but size it like the other optimizer cases). Steps:
```sql
-- @tags=optimizer,oq13,subquery_to_window
-- Test Objective: a correlated scalar-aggregate subquery in a WHERE comparison
-- is rewritten to an analytic WINDOW over the outer relation (StarRocks WinMagic),
-- not a re-scan + LEFT OUTER JOIN. apply mode only; legacy is unchanged.
DROP TABLE IF EXISTS ${case_db}.wm_line;
DROP TABLE IF EXISTS ${case_db}.wm_part;
CREATE TABLE ${case_db}.wm_line (l_partkey INT, l_quantity INT, l_ext INT);
CREATE TABLE ${case_db}.wm_part (p_partkey INT, p_brand VARCHAR(16));
INSERT INTO ${case_db}.wm_line VALUES (1,5,100),(1,50,200),(2,7,300),(2,8,150),(3,9,90);
INSERT INTO ${case_db}.wm_part VALUES (1,'B1'),(2,'B1'),(3,'B2');
ANALYZE TABLE ${case_db}.wm_line;
ANALYZE TABLE ${case_db}.wm_part;

SET subquery_unnest_mode='apply';

-- @explain_contains=WINDOW [
-- @explain_not_contains=APPLY
EXPLAIN VERBOSE
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

-- Correctness: same result as legacy (run the SELECT in apply mode; the golden
-- captures the rows, which must equal the legacy-mode rows).
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

-- Disable the rule: must fall back to the M1 LEFT OUTER JOIN form (no WINDOW),
-- and the result must be identical.
SET disable_optimizer_rules='ApplyToWindow';
-- @explain_contains=LEFT OUTER
-- @explain_not_contains=WINDOW [
EXPLAIN VERBOSE
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);
SET disable_optimizer_rules='';
```

- [ ] **Step 2: Rejection plan-golden** `subquery_scalar_to_window_rejected.sql` — the "don't mis-rewrite" reverse insurance (design §8.2). Each case asserts `@explain_not_contains=WINDOW [` (the rule must decline and `ScalarApplyToJoin` must produce the join form):
  - **self-join / duplicate outer table**: `FROM wm_line a, wm_line b WHERE ... AND a.l_quantity < (SELECT avg(l_quantity) FROM wm_line WHERE l_partkey = a.l_partkey)` (outer has two `wm_line`).
  - **table-set mismatch**: subquery scans a third table not present in the outer.
  - **predicate mismatch**: subquery has an extra residual filter (`AND l_quantity > 0`) with no outer twin.
  - **non-whitelist / distinct agg**: `(SELECT avg(DISTINCT l_quantity) ...)`.
  - **limit in subquery**: `(SELECT avg(l_quantity) FROM (SELECT l_quantity FROM wm_line WHERE l_partkey = p_partkey LIMIT 10) t)` (or the closest shape the parser accepts) — assert no WINDOW.
  All under `SET subquery_unnest_mode='apply';`. The result rows must still be correct (the join fallback executes).

- [ ] **Step 3: Record goldens.**
```bash
source docker/iceberg-rest/runtime/current/env.sh
# start standalone-server against $NOVAROCKS_STANDALONE_CONFIG (see CLAUDE.md §7.3, wait for NOVAROCKS_READY)
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode record --record-from target --update-expected \
  --only subquery_scalar_to_window,subquery_scalar_to_window_rejected
```
Inspect the recorded `.result` files: confirm `WINDOW [avg` appears in the positive case and is absent in every rejection case, and that the apply-mode `SELECT` rows match what legacy mode produces (run the same SELECT once with `SET subquery_unnest_mode='legacy';` in a scratch session and diff the rows — they must be identical).

- [ ] **Step 4: Verify the suite in verify mode.**
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify \
  --only subquery_scalar_to_window,subquery_scalar_to_window_rejected
```
Expected: both PASS. `cargo test --lib 2>&1 | grep '^test result' | tail -1` → 0 failed.

- [ ] **Step 5: Commit**
```bash
git add -A && git commit -m "test(optimizer): ApplyToWindow plan-golden + rejection cases

Positive: a correlated scalar-aggregate subquery in a WHERE comparison
rewrites to an analytic WINDOW over the outer relation in apply mode;
disable_optimizer_rules='ApplyToWindow' falls back to the LEFT OUTER JOIN
form with identical results. Rejection family (self-join, table-set
mismatch, predicate mismatch, distinct/non-whitelist agg, limit) locks the
ABSENCE of WINDOW — the reverse insurance against mis-rewrite (design §8.2)."
```

> **If per-case `SET subquery_unnest_mode` is not honored by the runner:** add an in-crate end-to-end test (engine/optimizer level, model `subquery/mod.rs` pipeline tests) that runs the q17-shaped query through analyze→plan→optimize in `apply` mode via `with_session_optimizer_settings` and asserts a `Window` node in the optimized plan + result equality against legacy. Note the fallback in the `.sql` comment and the commit body.

---

### Task 6: tpc-h q2/q17 apply-mode validation + final verification

**Files:** verification only (no production code changes expected).

- [ ] **Step 1: q2/q17 result parity.** Run tpc-h q2 and q17 in `apply` mode and confirm results match the committed legacy goldens (`sql-tests/tpc-h/result/q2.result`, `q17.result`). If the runner honors per-case `SET subquery_unnest_mode='apply';`, add a focused apply-mode assertion; otherwise validate via a one-off run with the session var set and record how it was validated. **Bar: identical results to legacy.**
- [ ] **Step 2: q17 plan-shape acceptance** (design §7.3 M2). In apply mode, q17's EXPLAIN must contain `WINDOW [` (WinMagic fired) OR — if a precondition legitimately fails on the real schema — keep the compact M1 agg+join with **no join-count regression** vs legacy. q2 (`min`) should show a window or a reduced join count. Capture the EXPLAIN for the PR description. (q2/q17 results are already correct in stable CI; this milestone is a plan-shape gain — state that explicitly in the PR.)
- [ ] **Step 3: Final gates.**
```bash
cargo fmt
cargo clippy --lib 2>&1 | grep -iE "apply_to_window|win_magic|TableIdentity|expr_phys" | grep -v '^\s*|' | head
cargo build && cargo test --lib 2>&1 | grep '^test result' | tail -1   # 0 failed
```
Fix any clippy lints on M2 symbols (no blanket `allow`).
- [ ] **Step 4: legacy-mode no-op check.** Run the `optimizer` suite in default (legacy) mode → no golden changes (M2 changes nothing in legacy mode; the only new goldens are the two apply-mode cases which set the var themselves).
- [ ] **Step 5: fmt fixup commit if needed.**
```bash
git add -A && git diff --cached --quiet || git commit -m "style: cargo fmt for M2 ApplyToWindow"
```

---

### Task 7 (isolated, optionally deferrable): flip the default `subquery_unnest_mode` to `apply` for scalar

> **This is the highest-blast-radius step in M2 and is intentionally last and separable.** It changes the default so *every* scalar subquery in *every* query decorrelates through the Apply framework (EXISTS/IN still fall back to legacy until M3). Land it only after Tasks 1–6 are green AND the broad suites pass in the new default. If validation surfaces any regression, hold this task and ship Tasks 1–6 (the rule is fully functional under explicit `SET subquery_unnest_mode='apply'`); the default flip can be its own follow-up PR.

**Files:**
- Modify: `src/sql/optimizer/options.rs` (default of `subquery_unnest_mode`)

- [ ] **Step 1: Locate the default.** In `src/sql/optimizer/options.rs`, find where `SessionOptimizerSettings::default()` (or the `SubqueryUnnestMode` default) sets `subquery_unnest_mode`. Read the surrounding tests.

- [ ] **Step 2: Pre-flip baseline.** With the default still `legacy`, run the broad suites and record pass/fail counts as the baseline:
```bash
source docker/iceberg-rest/runtime/current/env.sh
# start standalone-server (CLAUDE.md §7.3)
for s in ssb tpc-h tpc-ds join filter sort cte; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify 2>&1 | tail -3
done
```
Note the known-flaky/pre-existing failures from memory (TPC-DS 92/99 with 6 pre-existing #256 binding fails + q39 timing-flaky).

- [ ] **Step 3: Flip the default** to `Apply` (the scalar whitelist is already what `apply` mode routes; EXISTS/IN auto-fall-back to legacy per M1a's routing). Update any `options.rs` unit test that asserts the default value.

- [ ] **Step 4: Re-run the broad suites in the new default** (same loop as Step 2). **Acceptance: no NEW failures vs the Step-2 baseline** (the pre-existing/flaky set is unchanged). Pay special attention to every query with a scalar subquery (tpc-h q2/q4/q17/q20/q22, tpc-ds correlated-scalar set q1/q6/q30/q32/q81). Any new failure → revert the flip (`git revert` the flip commit or reset the one-line change) and STOP; report the failing case for a follow-up.

- [ ] **Step 5: `cargo test --lib`** → 0 failed (some lib tests may assert the default mode; update them to expect `Apply`).

- [ ] **Step 6: Commit**
```bash
git add -A && git commit -m "feat(optimizer): default subquery_unnest_mode to apply for scalar subqueries

Scalar subqueries now decorrelate through the Apply framework by default
(WinMagic + the M1 join forms); EXISTS/IN still fall back to legacy until
M3. Validated: ssb/tpc-h/tpc-ds/join/filter/sort/cte show no new failures
vs the pre-flip baseline; q2/q17 results unchanged with the window form."
```

---

## Self-review (run against design §3.2 / §6.2 / §7.3 with fresh eyes)

- **Spec coverage:** ApplyToWindow rule (Task 2–3) ✓; ordered before to-join (Task 4) ✓; whitelist/single-agg/no-distinct/no-limit/operator-whitelist/table-identity/predicate-identity preconditions (Task 2, all five StarRocks check groups) ✓; transform = Project(unchanged)→Filter(after)→Window→Sort→Filter(before)→outer (Task 3) ✓; PARTITION BY = outer correlation keys ✓; agg→window with inner→outer arg remap ✓; subquery comparison rewritten to window col ✓; subquery subtree discarded ✓; plan-golden + rejection cases (Task 5) ✓; `disable_optimizer_rules='ApplyToWindow'` fallback (Task 4 test + Task 5 golden) ✓; q2/q17 acceptance (Task 6) ✓; default flip (Task 7) ✓.
- **Iceberg snapshot identity (design §9.3):** `TableIdentity::Iceberg` includes `table_uuid`; same-FQN tables resolving to different snapshots get distinct identities only if `table_uuid` differs. `query_prep.rs` pins a statement-level snapshot, so two scans of the same table in one query share the snapshot — the table-set identity check therefore treats them as the same table (correct). No extra guard needed beyond using `table_uuid` in the identity.
- **Type consistency:** `WinMagicMatch` fields (`outer_conjuncts`, `subquery_conjunct`, `partition_by`, `inner_agg`) are produced by `check_preconditions` (Task 2) and consumed by `apply` (Task 3) — names match. `collect_table_ids`/`collect_scan_column_map`/`expr_phys_eq`/`TableIdentity` (Task 1) are imported via `use super::win_magic_util::...` in Task 2/3 — names match. `orient_eq` is `pub(super)` in `decorrelate_util` and reached as `super::decorrelate_util::orient_eq` — confirmed signature `(&TypedExpr, &HashSet<ColumnId>) -> Option<(&TypedExpr, &TypedExpr)>`.
- **No placeholders:** every code step shows real struct literals/signatures; the few prose-described helpers (`operator_whitelist_ok`, `remap_inner_to_outer`, `build_value_expr`, `replace_column_ref`, `subquery_residual_conjuncts`, `peel_to_aggregate`) each have a complete behavioral spec and exact signatures.

## Out of scope (later milestones / follow-ups)

- **EXISTS / IN to-window or to-join** — M3.
- **value-form (OR / projection), JOIN-ON subqueries, D2/D3/D4/D5 fixes** — M4.
- **`Filter(Project(Apply))` shape** (a Project between the WHERE Filter and the Apply) — falls back to `ScalarApplyToJoin`; revisit if a real query needs it.
- **Multi-aggregate or non-cross-join WinMagic** — StarRocks itself restricts to one aggregate + cross joins; do not loosen.

## Risks

- **Predicate-identity faithfulness** is the correctness crux. The 4-step port (Task 2 step 4) plus `expr_phys_eq` must exactly mirror StarRocks; the rejection golden family (Task 5 step 2) is the guard. If any rejection case shows a spurious `WINDOW`, the check is too loose — fix before proceeding (do NOT relax to gain coverage).
- **Inner→outer column remap** (`remap_inner_to_outer`): if a needed agg-arg column is not exposed on the outer side, the rule must decline (`Unchanged`) rather than emit a dangling ColumnRef — an `id_binding_verifier` error at codegen is the tripwire; the Task 3 e2e/pipeline test and Task 5 correctness rows exercise it.
- **Window needs a Sort child** — omitting it yields a wrong or invalid analytic plan. Task 3 emits `Sort{analytic_partition_by}`; the Task 5 EXPLAIN golden will show the `SORT`/`WINDOW` pairing.
- **EXPLAIN token drift** — assert `WINDOW [` (not `ANALYTIC`); confirmed against existing goldens.
- **Default flip blast radius** (Task 7) — isolated and revertible; gated on broad-suite parity against a pre-flip baseline.
