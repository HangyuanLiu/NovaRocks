# MV Query Rewrite (IVM v1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement transparent query rewrite for IVM materialized views (4 shapes: Projection-Filter / Aggregate / Join-Projection-Filter / Join-Aggregate) on Iceberg-backed MVs, with partition-level UNION ALL compensation and CBO cost-based selection.

**Architecture:** New isolated module `src/sql/optimizer/mv_rewrite/` holding a local `MvColumnId` facility, four cascades transformation rules, a candidate registry indexed by base-table FQN, predicate decomposition, column rewriting, and a partition compensator. Rules register conditionally via a new `MvRewriteCtx` plumbed into `all_transformation_rules`.

**Tech Stack:** Rust, Cascades optimizer (`src/sql/optimizer/`), Iceberg Catalog (`src/connector/iceberg/`), MV metadata (`src/meta/repository/mv.rs`), sql-test runner (`tests/sql-test-runner/`).

**Reference:** Spec at `docs/superpowers/specs/2026-05-21-mv-rewrite-ivm-design.md`. StarRocks reference code at `~/project/starrocks/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/`. **When uncertain, prefer StarRocks behavior.**

---

## File Structure (locked in by this plan)

**New module** `src/sql/optimizer/mv_rewrite/`:
```
mod.rs                        — public entry, MvRewriteCtx, MvRewriter
column_id.rs                  — MvColumnId, MvColumnIdFactory, equivalence union-find
registry.rs                   — MvCandidateRegistry, MV definition cache
predicate_split.rs            — PredicateSplit, containment, compensation
column_rewriter.rs            — query↔MV column mapping, aggregate rollup
partition_compensator.rs      — freshness oracle, UNION ALL synthesis
shape.rs                      — QueryShape / MvShape extraction from Memo
rewriter.rs                   — MvRewriter::try_rewrite orchestrator
trace.rs                      — observability trace records
rules/mod.rs                  — rule re-exports
rules/projection.rs           — MvProjectionRewriteRule
rules/aggregate_scan.rs       — MvAggregateScanRewriteRule
rules/join.rs                 — MvJoinRewriteRule
rules/aggregate_join.rs       — MvAggregateJoinRewriteRule
```

**Modified files**:
- `src/sql/optimizer/mod.rs` — declare module; build `MvRewriteCtx` per query; pass to `all_transformation_rules`.
- `src/sql/optimizer/rules/mod.rs` — accept `&MvRewriteCtx`; register new rules.
- `src/sql/optimizer/options.rs` — new session vars: `enable_mv_rewrite`, `enable_mv_union_rewrite`, `mv_rewrite_min_fresh_ratio`, `mv_rewrite_max_candidates_per_group`.
- `src/meta/repository/mv.rs` — `list_mvs_by_base_table` helper.
- `src/sql/explain.rs` (if exists; otherwise wherever EXPLAIN is generated) — print MV-rewrite trace.
- `src/server/mod.rs` — wire new session vars from `SET` statements.
- `src/sql/optimizer/stats.rs` — ensure `LogicalUnion` and aggregate-rollup row-count derivation are correct.

**New test files**:
- `sql-tests/optimizer/mv_rewrite_projection_full_fresh.sql`
- `sql-tests/optimizer/mv_rewrite_projection_partial_fresh.sql`
- `sql-tests/optimizer/mv_rewrite_aggregate_rollup.sql`
- `sql-tests/optimizer/mv_rewrite_aggregate_full_fresh.sql`
- `sql-tests/optimizer/mv_rewrite_join_inner.sql`
- `sql-tests/optimizer/mv_rewrite_aggregate_join.sql`
- `sql-tests/optimizer/mv_rewrite_reject_predicate_mismatch.sql`
- `sql-tests/optimizer/mv_rewrite_reject_groupby_finer.sql`
- `sql-tests/mv-on-iceberg/rewrite/projection_full.sql`
- `sql-tests/mv-on-iceberg/rewrite/projection_partial.sql`
- `sql-tests/mv-on-iceberg/rewrite/aggregate.sql`
- `sql-tests/mv-on-iceberg/rewrite/join.sql`
- `sql-tests/mv-on-iceberg/rewrite/aggregate_join.sql`

---

## Task 1: Scaffold module + MvRewriteCtx + session vars

Goal: Empty `mv_rewrite/` module compiles. `MvRewriteCtx` plumbed through optimizer. Session vars registered. No rules yet — original optimizer behaviour unchanged. Build must be green.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/mod.rs`
- Create: `src/sql/optimizer/mv_rewrite/trace.rs`
- Create: `src/sql/optimizer/mv_rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/mod.rs` (declare module; build ctx; pass to rules factory)
- Modify: `src/sql/optimizer/rules/mod.rs` (accept ctx; thread through; rules unchanged for now)
- Modify: `src/sql/optimizer/options.rs` (add 4 new session-var fields with defaults)
- Modify: `src/server/mod.rs` (handle `SET enable_materialized_view_rewrite=...` etc. — search for existing `SET enable_*` handling to mirror)

- [ ] **Step 1.1: Write the failing test**

Add to `src/sql/optimizer/options.rs` `tests` module:

```rust
#[test]
fn default_enables_mv_rewrite() {
    let s = SessionOptimizerSettings::default();
    assert!(s.enable_mv_rewrite);
    assert!(s.enable_mv_union_rewrite);
    assert!((s.mv_rewrite_min_fresh_ratio - 0.2).abs() < 1e-9);
    assert_eq!(s.mv_rewrite_max_candidates_per_group, 3);
}
```

- [ ] **Step 1.2: Run and verify fail**

```
cargo test --lib sql::optimizer::options::tests::default_enables_mv_rewrite
```
Expected: compilation error `no field 'enable_mv_rewrite' on type 'SessionOptimizerSettings'`.

- [ ] **Step 1.3: Add session var fields**

In `src/sql/optimizer/options.rs`, extend `SessionOptimizerSettings` (note: needs custom `Default` because `f64` and bool defaults differ from `Default::default()`):

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SessionOptimizerSettings {
    pub enable_ukfk_opt: bool,
    pub enable_rbo_table_prune: bool,
    pub enable_cbo_table_prune: bool,
    pub enable_table_prune_on_update: bool,
    pub enable_eliminate_agg: bool,
    pub disabled_rules: Vec<String>,
    /// Master kill switch for MV query rewrite. Default true.
    pub enable_mv_rewrite: bool,
    /// When false, only fully-fresh MVs are eligible; partial-freshness
    /// rewrites (UNION ALL with stale-partition base scan) are disabled.
    /// Default true.
    pub enable_mv_union_rewrite: bool,
    /// Skip MV rewrite when fresh partitions cover less than this fraction.
    /// Default 0.2.
    pub mv_rewrite_min_fresh_ratio: f64,
    /// Hard cap on number of MV alternatives inserted per memo group.
    /// Default 3.
    pub mv_rewrite_max_candidates_per_group: usize,
}

impl Default for SessionOptimizerSettings {
    fn default() -> Self {
        Self {
            enable_ukfk_opt: false,
            enable_rbo_table_prune: false,
            enable_cbo_table_prune: false,
            enable_table_prune_on_update: false,
            enable_eliminate_agg: false,
            disabled_rules: Vec::new(),
            enable_mv_rewrite: true,
            enable_mv_union_rewrite: true,
            mv_rewrite_min_fresh_ratio: 0.2,
            mv_rewrite_max_candidates_per_group: 3,
        }
    }
}
```

Add to `OptimizerOptions`:

```rust
pub(crate) struct OptimizerOptions {
    disabled_rules: HashSet<String>,
    pub rbo_max_iterations: usize,
    #[allow(dead_code)]
    pub cbo_max_groups: usize,
    pub optimize_timeout: Duration,
    pub enable_mv_rewrite: bool,
    pub enable_mv_union_rewrite: bool,
    pub mv_rewrite_min_fresh_ratio: f64,
    pub mv_rewrite_max_candidates_per_group: usize,
}
```

Update `default_settings`:
```rust
pub(crate) fn default_settings() -> Self {
    Self {
        disabled_rules: HashSet::new(),
        rbo_max_iterations: 32,
        cbo_max_groups: 5000,
        optimize_timeout: Duration::from_secs(10),
        enable_mv_rewrite: true,
        enable_mv_union_rewrite: true,
        mv_rewrite_min_fresh_ratio: 0.2,
        mv_rewrite_max_candidates_per_group: 3,
    }
}
```

And `from_session` to copy the 4 fields.

- [ ] **Step 1.4: Run test, verify pass**

```
cargo test --lib sql::optimizer::options::tests::default_enables_mv_rewrite
```
Expected: PASS.

- [ ] **Step 1.5: Create empty mv_rewrite module**

Create `src/sql/optimizer/mv_rewrite/mod.rs`:

```rust
//! Materialized view query rewrite.
//!
//! See `docs/superpowers/specs/2026-05-21-mv-rewrite-ivm-design.md` for
//! design rationale. Reference: StarRocks `materialization/` rules.

pub(crate) mod rules;
pub(crate) mod trace;

use std::sync::Arc;

use super::options::OptimizerOptions;

/// Context passed to MV-rewrite rules. Built once per `optimize()` call.
#[derive(Clone)]
pub(crate) struct MvRewriteCtx {
    inner: Arc<MvRewriteCtxInner>,
}

struct MvRewriteCtxInner {
    pub enable_mv_rewrite: bool,
    pub enable_mv_union_rewrite: bool,
    pub mv_rewrite_min_fresh_ratio: f64,
    pub mv_rewrite_max_candidates_per_group: usize,
    // Catalog handle + MvCandidateRegistry will be added in later tasks.
}

impl MvRewriteCtx {
    pub(crate) fn from_options(opts: &OptimizerOptions) -> Self {
        Self {
            inner: Arc::new(MvRewriteCtxInner {
                enable_mv_rewrite: opts.enable_mv_rewrite,
                enable_mv_union_rewrite: opts.enable_mv_union_rewrite,
                mv_rewrite_min_fresh_ratio: opts.mv_rewrite_min_fresh_ratio,
                mv_rewrite_max_candidates_per_group: opts.mv_rewrite_max_candidates_per_group,
            }),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.enable_mv_rewrite
    }

    #[allow(dead_code)]
    pub(crate) fn union_enabled(&self) -> bool {
        self.inner.enable_mv_union_rewrite
    }

    #[allow(dead_code)]
    pub(crate) fn min_fresh_ratio(&self) -> f64 {
        self.inner.mv_rewrite_min_fresh_ratio
    }

    #[allow(dead_code)]
    pub(crate) fn max_candidates_per_group(&self) -> usize {
        self.inner.mv_rewrite_max_candidates_per_group
    }
}
```

Create `src/sql/optimizer/mv_rewrite/trace.rs`:

```rust
//! Observability trace records for MV rewrite attempts.

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum MvRewriteOutcome {
    Accepted { mv_name: String, fresh: usize, stale: usize },
    Rejected { mv_name: String, reason: String },
    Skipped { mv_name: String, reason: String },
}
```

Create `src/sql/optimizer/mv_rewrite/rules/mod.rs`:

```rust
//! MV rewrite cascades rules.
//!
//! Each rule is gated on `MvRewriteCtx::enabled()`. Rules are
//! registered in `super::super::rules::all_transformation_rules`
//! only when MV rewrite is enabled (see crate::sql::optimizer::rules).
```

- [ ] **Step 1.6: Wire module into optimizer**

In `src/sql/optimizer/mod.rs` add `pub(crate) mod mv_rewrite;` near the other `pub(crate) mod` lines. Then in the same file's `optimize()`:

After `let options = options::OptimizerOptions::from_session(...);`, add:
```rust
let mv_ctx = mv_rewrite::MvRewriteCtx::from_options(&options);
```

Then change the call site:
```rust
let transform_rules = rules::all_transformation_rules(&mv_ctx);
```

In `src/sql/optimizer/rules/mod.rs`, change:
```rust
pub(crate) fn all_transformation_rules(_mv_ctx: &super::mv_rewrite::MvRewriteCtx) -> Vec<Box<dyn Rule>> {
    let rules: Vec<Box<dyn Rule>> = vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_top_n::SplitTopN),
    ];
    // MV rules will be added here in later tasks.
    rules
}
```

Update `is_known_rule_name` in `src/sql/optimizer/mod.rs` to pass a dummy ctx:
```rust
pub(crate) fn is_known_rule_name(name: &str) -> bool {
    let dummy_opts = options::OptimizerOptions::default_settings();
    let dummy_ctx = mv_rewrite::MvRewriteCtx::from_options(&dummy_opts);
    rules::all_transformation_rules(&dummy_ctx)
        .iter()
        .any(|r| r.name() == name)
        || rules::all_implementation_rules()
            .iter()
            .any(|r| r.name() == name)
        || rbo::rules::predicate_pushdown_rbo_rules()
            .iter()
            .any(|r| r.name() == name)
        || rbo::rules::column_pruning_rules()
            .iter()
            .any(|r| r.name() == name)
}
```

- [ ] **Step 1.7: Wire SET handler for new session vars**

Find existing handler for `enable_eliminate_agg` (search `enable_eliminate_agg` in `src/server/`) and add adjacent handlers for the 4 new vars. Snippet pattern (replace with whatever the project uses):

```rust
"enable_materialized_view_rewrite" => settings.enable_mv_rewrite = parse_bool(value)?,
"enable_materialized_view_union_rewrite" => settings.enable_mv_union_rewrite = parse_bool(value)?,
"mv_rewrite_min_fresh_ratio" => settings.mv_rewrite_min_fresh_ratio = parse_f64(value)?,
"mv_rewrite_max_candidates_per_group" => settings.mv_rewrite_max_candidates_per_group = parse_usize(value)?,
```

If the existing pattern uses different parsers, match it.

- [ ] **Step 1.8: Build + test**

```
cargo build
cargo test --lib sql::optimizer
```
Expected: build clean, all existing tests still pass, the new `default_enables_mv_rewrite` test passes.

- [ ] **Step 1.9: Commit**

```bash
git add src/sql/optimizer/mv_rewrite src/sql/optimizer/mod.rs src/sql/optimizer/options.rs src/sql/optimizer/rules/mod.rs src/server/mod.rs
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): scaffold module + MvRewriteCtx + session vars

Adds empty src/sql/optimizer/mv_rewrite/ module with MvRewriteCtx
threaded into all_transformation_rules(). New session variables:
- enable_materialized_view_rewrite (default true)
- enable_materialized_view_union_rewrite (default true)
- mv_rewrite_min_fresh_ratio (default 0.2)
- mv_rewrite_max_candidates_per_group (default 3)

No rules registered yet — optimizer behaviour unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: MvColumnId facility + equivalence union-find

Goal: A local canonical column-identity facility usable inside the MV rewrite module. ID assignment is deterministic across query and MV trees provided they share the same base Iceberg fields.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/column_id.rs`
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs` (declare submodule)

- [ ] **Step 2.1: Define module + types skeleton (compile only, no logic)**

```rust
//! Canonical column identity for MV rewrite.
//!
//! Both the query's Operator tree and the candidate MV's Operator tree
//! are walked with the SAME MvColumnIdFactory so identical Iceberg base
//! fields and identical derived expressions produce identical
//! MvColumnIds. This is the foundation for query↔MV column matching
//! without relying on string-based ColumnRef names (which break under
//! SubqueryAlias and Project rename).
//!
//! Equivalence union-find groups columns connected by join-eq or
//! filter-eq predicates so e.g. `t1.a = t2.b` makes the two columns
//! interchangeable for matching purposes.

use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct MvColumnId(pub(crate) u32);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum MvColumnIdKey {
    /// Scan column: (base table Iceberg UUID, base field-id).
    Base { table_uuid: String, field_id: i32 },
    /// Derived column produced by a scalar expression.
    /// `expr_hash` is a stable hash of the canonical scalar expression
    /// expressed in terms of already-assigned MvColumnIds (commutative
    /// operators are sorted; constant-folded where possible).
    Derived { expr_hash: u64 },
    /// Aggregate output column.
    AggOutput {
        fn_name: String,
        args: Vec<MvColumnId>,
        group_hash: u64,
    },
}

#[derive(Default)]
pub(crate) struct MvColumnIdFactory {
    next: u32,
    forward: HashMap<MvColumnIdKey, MvColumnId>,
    display: HashMap<MvColumnId, String>,
}

impl MvColumnIdFactory {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn intern(&mut self, key: MvColumnIdKey, display: String) -> MvColumnId {
        if let Some(id) = self.forward.get(&key) {
            return *id;
        }
        let id = MvColumnId(self.next);
        self.next += 1;
        self.forward.insert(key, id);
        self.display.insert(id, display);
        id
    }

    pub(crate) fn display(&self, id: MvColumnId) -> Option<&str> {
        self.display.get(&id).map(String::as_str)
    }
}

/// Union-find of MvColumnIds representing equivalence classes (built
/// from join-eq and filter-eq predicates).
#[derive(Clone, Debug, Default)]
pub(crate) struct MvEquivalence {
    parent: HashMap<MvColumnId, MvColumnId>,
}

impl MvEquivalence {
    pub(crate) fn find(&mut self, id: MvColumnId) -> MvColumnId {
        let p = *self.parent.entry(id).or_insert(id);
        if p == id {
            return id;
        }
        let root = self.find(p);
        self.parent.insert(id, root);
        root
    }

    pub(crate) fn union(&mut self, a: MvColumnId, b: MvColumnId) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent.insert(ra, rb);
        }
    }

    pub(crate) fn equivalent(&mut self, a: MvColumnId, b: MvColumnId) -> bool {
        self.find(a) == self.find(b)
    }
}
```

In `mv_rewrite/mod.rs` add: `pub(crate) mod column_id;`

- [ ] **Step 2.2: Write tests**

Append to `column_id.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_returns_same_id() {
        let mut f = MvColumnIdFactory::new();
        let k = MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 5 };
        let a = f.intern(k.clone(), "t1.a".into());
        let b = f.intern(k, "t1.a".into());
        assert_eq!(a, b);
    }

    #[test]
    fn different_keys_return_different_ids() {
        let mut f = MvColumnIdFactory::new();
        let id1 = f.intern(
            MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 1 },
            "t1.a".into(),
        );
        let id2 = f.intern(
            MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 2 },
            "t1.b".into(),
        );
        assert_ne!(id1, id2);
    }

    #[test]
    fn cross_factory_keys_match_when_seed_matches() {
        // Two separate factories (e.g. one for query, one for MV) assign
        // distinct local IDs, but matching uses the KEY not the ID.
        let mut q = MvColumnIdFactory::new();
        let mut mv = MvColumnIdFactory::new();
        let k = MvColumnIdKey::Base { table_uuid: "u1".into(), field_id: 7 };
        let qid = q.intern(k.clone(), "t.x".into());
        let mvid = mv.intern(k.clone(), "t.x".into());
        // IDs need not be equal across factories — matching is by key.
        // The factory's intern() returns a stable ID for the same key
        // within ONE factory; cross-factory comparison goes through
        // matching the KEY, which is the contract relied on by
        // ColumnRewriter (Task 5).
        let _ = (qid, mvid);
        assert_eq!(q.forward.get(&k).copied(), Some(qid));
        assert_eq!(mv.forward.get(&k).copied(), Some(mvid));
    }

    #[test]
    fn equivalence_find_returns_self_when_uninited() {
        let mut eq = MvEquivalence::default();
        let id = MvColumnId(0);
        assert_eq!(eq.find(id), id);
    }

    #[test]
    fn equivalence_union_makes_them_equivalent() {
        let mut eq = MvEquivalence::default();
        let a = MvColumnId(0);
        let b = MvColumnId(1);
        let c = MvColumnId(2);
        assert!(!eq.equivalent(a, b));
        eq.union(a, b);
        assert!(eq.equivalent(a, b));
        eq.union(b, c);
        assert!(eq.equivalent(a, c)); // transitivity
    }

    #[test]
    fn derived_keys_canonicalize() {
        // Same hash → same ID (caller is responsible for canonicalization
        // before hashing — we just verify the factory honours equality).
        let mut f = MvColumnIdFactory::new();
        let id1 = f.intern(MvColumnIdKey::Derived { expr_hash: 0xDEAD }, "x+y".into());
        let id2 = f.intern(MvColumnIdKey::Derived { expr_hash: 0xDEAD }, "y+x".into());
        assert_eq!(id1, id2);
    }
}
```

- [ ] **Step 2.3: Run tests**

```
cargo test --lib sql::optimizer::mv_rewrite::column_id
```
Expected: all 6 tests pass.

- [ ] **Step 2.4: Commit**

```bash
git add src/sql/optimizer/mv_rewrite/column_id.rs src/sql/optimizer/mv_rewrite/mod.rs
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): MvColumnId facility + equivalence union-find

Canonical column identity local to the MV rewrite module. Designed to
retire once ARCH G1 (global ColumnId) lands — mechanical replacement.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: MvCandidateRegistry + base-table lookup + plan cache

Goal: A registry that, given a base table FQN, returns the set of MV definitions referencing it. The MV's `Operator` tree is materialized on demand (re-parsing the stored SELECT SQL) and cached.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/registry.rs`
- Modify: `src/meta/repository/mv.rs` (add `list_mvs_by_base_table` helper)
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs` (add registry submodule + store in `MvRewriteCtx`)

- [ ] **Step 3.1: Add repository helper**

In `src/meta/repository/mv.rs`, add inside the `impl MvMetaRepository` block (after `list_definitions`):

```rust
/// Return the IDs of all MVs that reference `base_table_fqn` as one of
/// their base tables. Linear scan over `list_definitions`; v1 callers
/// are responsible for caching the result for the duration of a query.
pub fn list_mvs_by_base_table(
    &self,
    txn: &mut dyn MetaReadTxn,
    base_table_fqn: &str,
) -> RepositoryResult<Vec<i64>> {
    let all = self.list_definitions(txn)?;
    let needle = base_table_fqn.to_string();
    Ok(all
        .into_iter()
        .filter(|d| d.base_table_refs.iter().any(|b| b == &needle))
        .map(|d| d.mv_id)
        .collect())
}
```

- [ ] **Step 3.2: Write the failing test for the helper**

Search for existing tests on `MvMetaRepository` (likely in the same file under a `#[cfg(test)] mod tests`). Add or extend:

```rust
#[test]
fn list_mvs_by_base_table_filters_by_base_fqn() {
    let mut txn = TestMetaTxn::new(); // use existing test harness
    let repo = MvMetaRepository::default();
    // Insert two MVs: mv1 references base_a, mv2 references base_a + base_b.
    let mv1_id = repo
        .create_definition(
            &mut txn,
            CreateMvDefinitionRequest {
                select_sql: "SELECT 1".into(),
                base_table_refs: vec!["base_a".into()],
                primary_key_columns: vec![],
                storage_engine: "iceberg".into(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                schema_contract: None,
                partition_spec: None,
                created_at_ms: 0,
            },
        )
        .unwrap()
        .mv_id;
    let mv2_id = repo
        .create_definition(
            &mut txn,
            CreateMvDefinitionRequest {
                select_sql: "SELECT 1".into(),
                base_table_refs: vec!["base_a".into(), "base_b".into()],
                primary_key_columns: vec![],
                storage_engine: "iceberg".into(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                schema_contract: None,
                partition_spec: None,
                created_at_ms: 0,
            },
        )
        .unwrap()
        .mv_id;

    let mut a = repo.list_mvs_by_base_table(&mut txn, "base_a").unwrap();
    a.sort();
    assert_eq!(a, vec![mv1_id, mv2_id]);
    let b = repo.list_mvs_by_base_table(&mut txn, "base_b").unwrap();
    assert_eq!(b, vec![mv2_id]);
    let none = repo.list_mvs_by_base_table(&mut txn, "missing").unwrap();
    assert!(none.is_empty());
}
```

If `TestMetaTxn` doesn't exist, find what existing `mv.rs` tests use (look for any `#[test]` in or near the file) and mirror.

- [ ] **Step 3.3: Run repo test, verify pass**

```
cargo test --lib meta::repository::mv:: -- list_mvs_by_base_table
```
Expected: PASS.

- [ ] **Step 3.4: Create registry.rs skeleton**

```rust
//! MV candidate registry.
//!
//! Builds per-query candidate set indexed by base-table FQN. Caches
//! reparsed MV definitions for the duration of one optimize() call.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::meta::repository::mv::StoredMvDefinition;

#[derive(Clone, Debug)]
pub(crate) struct MvCandidate {
    pub mv_id: i64,
    pub mv_name: String,
    pub definition: StoredMvDefinition,
    // The reparsed MV Operator tree is added in Task 6 when shape
    // extraction needs it. For now the registry just holds metadata.
}

#[derive(Default)]
pub(crate) struct MvCandidateRegistry {
    /// FQN → set of candidates. Filled lazily.
    by_base_table: Mutex<HashMap<String, Vec<MvCandidate>>>,
}

impl MvCandidateRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return candidates referencing `base_table_fqn` that pass v1 filters:
    /// - storage_engine == "iceberg"
    /// - refresh_in_progress == false
    /// - last_refresh_snapshots non-empty (refreshed at least once)
    pub(crate) fn candidates_for_base(
        &self,
        base_table_fqn: &str,
        // Caller hands in the snapshot of definitions to scan.
        all_defs: &[StoredMvDefinition],
    ) -> Vec<MvCandidate> {
        let mut cache = self.by_base_table.lock().unwrap();
        if let Some(c) = cache.get(base_table_fqn) {
            return c.clone();
        }
        let candidates: Vec<MvCandidate> = all_defs
            .iter()
            .filter(|d| {
                d.storage_engine == "iceberg"
                    && !d.refresh_in_progress
                    && !d.last_refresh_snapshots.is_empty()
                    && d.base_table_refs.iter().any(|b| b == base_table_fqn)
            })
            .map(|d| MvCandidate {
                mv_id: d.mv_id,
                mv_name: d
                    .target_table
                    .clone()
                    .unwrap_or_else(|| format!("mv_{}", d.mv_id)),
                definition: d.clone(),
            })
            .collect();
        cache.insert(base_table_fqn.to_string(), candidates.clone());
        candidates
    }
}
```

- [ ] **Step 3.5: Write registry tests**

Append to `registry.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv::StoredMvDefinition;
    use std::collections::BTreeMap;

    fn mk(mv_id: i64, base: &str, engine: &str, refreshed: bool, in_progress: bool) -> StoredMvDefinition {
        let mut snaps = BTreeMap::new();
        if refreshed {
            snaps.insert(base.to_string(), 1);
        }
        StoredMvDefinition {
            mv_id,
            select_sql: "SELECT 1".into(),
            base_table_refs: vec![base.into()],
            primary_key_columns: vec![],
            storage_engine: engine.into(),
            target_catalog: None,
            target_namespace: None,
            target_table: Some(format!("mv_target_{}", mv_id)),
            schema_contract: None,
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: Some(100),
            last_refresh_snapshots: snaps,
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: in_progress,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            created_at_ms: 0,
        }
    }

    #[test]
    fn filters_non_iceberg_backend() {
        let r = MvCandidateRegistry::new();
        let defs = vec![mk(1, "tbl", "managed_lake", true, false)];
        assert!(r.candidates_for_base("tbl", &defs).is_empty());
    }

    #[test]
    fn filters_refresh_in_progress() {
        let r = MvCandidateRegistry::new();
        let defs = vec![mk(1, "tbl", "iceberg", true, true)];
        assert!(r.candidates_for_base("tbl", &defs).is_empty());
    }

    #[test]
    fn filters_unrefreshed() {
        let r = MvCandidateRegistry::new();
        let defs = vec![mk(1, "tbl", "iceberg", false, false)];
        assert!(r.candidates_for_base("tbl", &defs).is_empty());
    }

    #[test]
    fn includes_eligible_iceberg_mv() {
        let r = MvCandidateRegistry::new();
        let defs = vec![mk(1, "tbl", "iceberg", true, false)];
        let cands = r.candidates_for_base("tbl", &defs);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].mv_id, 1);
    }

    #[test]
    fn excludes_unrelated_base_tables() {
        let r = MvCandidateRegistry::new();
        let defs = vec![mk(1, "tbl_a", "iceberg", true, false)];
        assert!(r.candidates_for_base("tbl_b", &defs).is_empty());
    }
}
```

- [ ] **Step 3.6: Wire registry into MvRewriteCtx**

In `mv_rewrite/mod.rs`:
- Add `pub(crate) mod registry;`
- Add `pub registry: registry::MvCandidateRegistry,` to `MvRewriteCtxInner` (wrap in `Arc<MvCandidateRegistry>` if cloning issues — registry is Send+Sync via Mutex).

```rust
struct MvRewriteCtxInner {
    pub enable_mv_rewrite: bool,
    pub enable_mv_union_rewrite: bool,
    pub mv_rewrite_min_fresh_ratio: f64,
    pub mv_rewrite_max_candidates_per_group: usize,
    pub registry: registry::MvCandidateRegistry,
}

impl MvRewriteCtx {
    pub(crate) fn from_options(opts: &OptimizerOptions) -> Self {
        Self {
            inner: Arc::new(MvRewriteCtxInner {
                enable_mv_rewrite: opts.enable_mv_rewrite,
                enable_mv_union_rewrite: opts.enable_mv_union_rewrite,
                mv_rewrite_min_fresh_ratio: opts.mv_rewrite_min_fresh_ratio,
                mv_rewrite_max_candidates_per_group: opts.mv_rewrite_max_candidates_per_group,
                registry: registry::MvCandidateRegistry::new(),
            }),
        }
    }

    pub(crate) fn registry(&self) -> &registry::MvCandidateRegistry {
        &self.inner.registry
    }
}
```

- [ ] **Step 3.7: Run all tests**

```
cargo test --lib sql::optimizer::mv_rewrite
cargo test --lib meta::repository::mv
cargo build
```
Expected: clean.

- [ ] **Step 3.8: Commit**

```bash
git add src/sql/optimizer/mv_rewrite/registry.rs src/sql/optimizer/mv_rewrite/mod.rs src/meta/repository/mv.rs
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): MvCandidateRegistry + base-table lookup

list_mvs_by_base_table helper on MvMetaRepository, registry that
filters candidates by storage_engine=iceberg + refresh state, and
session-cached lookup keyed by base FQN.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: PredicateSplit + containment + compensation derivation

Goal: Decompose a list of conjuncts into eq/range/residual, decide whether query's predicates contain MV's, and emit the compensating filter.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/predicate_split.rs`
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs` (declare submodule)

- [ ] **Step 4.1: Scan TypedExpr / Operator constants used in predicates**

Read `src/sql/analysis/mod.rs` lines 200-300 and `src/sql/analysis/scalar.rs` (search for `pub enum ScalarOp` or similar) to discover the concrete shape of `TypedExpr`. The plan uses placeholder names — adapt to the actual type.

Key things to find:
- How equality is represented (`BinaryOp::Eq`, `ScalarOp::Equal`, ...)
- How a literal is constructed (`TypedExpr::Literal(...)`, `Expr::Const(...)`, ...)
- How to compare two `TypedExpr` for structural equality (probably `PartialEq` derive or a helper like `expr_equal`)

Adapt the code below to actual names. Where the plan says `BinaryOp::Eq` and similar, replace with the real enum variants.

- [ ] **Step 4.2: Define predicate types**

```rust
//! Decompose a list of conjuncts into (equality, range, residual)
//! categories and decide containment / derive compensation.
//!
//! Reference: StarRocks PredicateSplit, PredicateExtractor.

use super::column_id::MvColumnId;
use crate::sql::analysis::TypedExpr;

#[derive(Clone, Debug)]
pub(crate) struct EqualityPred {
    pub col: MvColumnId,
    pub literal: TypedExpr, // a constant
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RangeBound {
    /// `col > literal` or `col >= literal`
    LowerBound { literal: TypedExpr, inclusive: bool },
    /// `col < literal` or `col <= literal`
    UpperBound { literal: TypedExpr, inclusive: bool },
    /// `col BETWEEN low AND high`
    Between { low: TypedExpr, high: TypedExpr },
}

#[derive(Clone, Debug)]
pub(crate) struct RangePred {
    pub col: MvColumnId,
    pub bound: RangeBound,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PredicateSplit {
    pub equality: Vec<EqualityPred>,
    pub range: Vec<RangePred>,
    pub residual: Vec<TypedExpr>,
}

impl PredicateSplit {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn from_conjuncts(
        preds: &[TypedExpr],
        resolve_col: &impl Fn(&TypedExpr) -> Option<MvColumnId>,
    ) -> Self {
        let mut out = Self::default();
        for p in preds {
            if let Some(eq) = try_as_equality(p, resolve_col) {
                out.equality.push(eq);
            } else if let Some(rg) = try_as_range(p, resolve_col) {
                out.range.push(rg);
            } else {
                out.residual.push(p.clone());
            }
        }
        out
    }
}

/// Recognise `col = literal` and `literal = col` shapes.
fn try_as_equality(
    e: &TypedExpr,
    resolve_col: &impl Fn(&TypedExpr) -> Option<MvColumnId>,
) -> Option<EqualityPred> {
    // PSEUDOCODE — adapt to actual TypedExpr enum.
    // Find BinaryOp::Eq with one side being a column reference
    // (resolve_col returns Some) and the other side a literal.
    None
}

fn try_as_range(
    e: &TypedExpr,
    resolve_col: &impl Fn(&TypedExpr) -> Option<MvColumnId>,
) -> Option<RangePred> {
    // PSEUDOCODE — adapt to actual TypedExpr enum.
    // Find <, <=, >, >=, BETWEEN with one side resolving to a column.
    None
}

/// Decide whether query's predicates are at least as restrictive as
/// MV's predicates (i.e. query ⇒ MV). Returns the compensating
/// predicate that must be applied on top of the MV scan to recover the
/// query's selectivity.
pub(crate) fn contain_and_compensate(
    query: &PredicateSplit,
    mv: &PredicateSplit,
) -> Option<Compensation> {
    // 1. Every MV equality (c, v) must appear in query equalities
    //    with the SAME value v. Otherwise reject.
    for eq in &mv.equality {
        if !query.equality.iter().any(|q| q.col == eq.col && exprs_equal(&q.literal, &eq.literal)) {
            return None;
        }
    }
    // 2. Every MV range must be a SUPERSET of query's range on that col.
    //    (MV [10,30] is OK if query is [15,25]; not OK if query is [5,25].)
    for r in &mv.range {
        let q_ranges: Vec<_> = query.range.iter().filter(|q| q.col == r.col).collect();
        // For v1: require query has a tighter or equal range on the same col.
        // Be conservative — if cannot prove, reject.
        if q_ranges.is_empty() {
            return None;
        }
        if !q_ranges.iter().any(|q| range_subset(&q.bound, &r.bound)) {
            return None;
        }
    }
    // 3. Residual: require exact set equality after canonicalization.
    //    Anything stronger requires SAT solving; v2.
    if !residual_equal(&query.residual, &mv.residual) {
        return None;
    }
    // 4. Compensation = query.equality \ mv.equality + query.range \ mv.range
    let comp_eq: Vec<EqualityPred> = query
        .equality
        .iter()
        .filter(|q| !mv.equality.iter().any(|m| m.col == q.col && exprs_equal(&m.literal, &q.literal)))
        .cloned()
        .collect();
    let comp_range: Vec<RangePred> = query
        .range
        .iter()
        .filter(|q| !mv.range.iter().any(|m| m.col == q.col && m.bound == q.bound))
        .cloned()
        .collect();
    Some(Compensation { eq: comp_eq, range: comp_range })
}

#[derive(Clone, Debug)]
pub(crate) struct Compensation {
    pub eq: Vec<EqualityPred>,
    pub range: Vec<RangePred>,
}

impl Compensation {
    pub(crate) fn is_empty(&self) -> bool {
        self.eq.is_empty() && self.range.is_empty()
    }

    pub(crate) fn into_typed_expr(self) -> Option<TypedExpr> {
        // Reassemble into a single AND-chained TypedExpr or return None
        // if empty. Build using the project's existing AND/Eq/Lt/Gt
        // constructors.
        // PSEUDOCODE — adapt to actual builders.
        None
    }
}

fn exprs_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    // For literals, structural compare suffices. Adapt to project's
    // own equality helper if one exists.
    format!("{:?}", a) == format!("{:?}", b)
}

fn range_subset(query: &RangeBound, mv: &RangeBound) -> bool {
    // True if `query` is at least as tight as `mv`. Implemented for
    // common shapes; falls back to false for unknown combinations
    // (v1 is conservative).
    use RangeBound::*;
    match (query, mv) {
        (LowerBound { literal: ql, .. }, LowerBound { literal: ml, .. }) => {
            // query: col > ql ; mv: col > ml ; subset iff ql >= ml
            literal_ge(ql, ml)
        }
        (UpperBound { literal: ql, .. }, UpperBound { literal: ml, .. }) => {
            // query: col < ql ; mv: col < ml ; subset iff ql <= ml
            literal_le(ql, ml)
        }
        _ => false,
    }
}

fn literal_ge(_a: &TypedExpr, _b: &TypedExpr) -> bool {
    // PSEUDOCODE — compare numeric or string literals.
    false
}
fn literal_le(_a: &TypedExpr, _b: &TypedExpr) -> bool {
    false
}

fn residual_equal(a: &[TypedExpr], b: &[TypedExpr]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    // Order-independent: each in a must equal one in b (after
    // canonicalization). For v1 use textual sort.
    let mut da: Vec<String> = a.iter().map(|e| format!("{:?}", e)).collect();
    let mut db: Vec<String> = b.iter().map(|e| format!("{:?}", e)).collect();
    da.sort();
    db.sort();
    da == db
}
```

- [ ] **Step 4.3: Resolve TypedExpr-specific pseudocode**

Implement `try_as_equality`, `try_as_range`, `literal_ge`, `literal_le`, and `Compensation::into_typed_expr` using the **actual** types found in `src/sql/analysis/`. Pattern: find existing call sites that detect `col = literal` (search `try_as_equality_predicate`, `match_eq_pred`, or look for how `predicate_pushdown` extracts equalities in `src/sql/optimizer/rbo/rules/`).

Use existing helpers wherever possible — do not invent a new boolean expression builder.

- [ ] **Step 4.4: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Each test constructs a few synthetic TypedExpr via the project's
    // builders. Replace pseudo-builders with real ones.
    //
    // Helper:
    //   col(id: u32) -> TypedExpr — a column ref tagged so that the
    //                                resolver returns MvColumnId(id).
    //   lit(v: i64) -> TypedExpr — integer literal.
    //   eq(a, b) -> TypedExpr     — a = b.
    //   gt(a, b) -> TypedExpr     — a > b.
    //   and(items) -> Vec<TypedExpr> — list of conjuncts.

    fn resolve_passthrough(e: &TypedExpr) -> Option<MvColumnId> {
        // Test resolver: if expr is a marker `ColumnRef("c<n>")`,
        // return MvColumnId(n).
        // ADAPT to actual TypedExpr enum.
        None
    }

    #[test]
    fn split_classifies_equality() {
        // let preds = vec![eq(col(1), lit(5))];
        // let s = PredicateSplit::from_conjuncts(&preds, &resolve_passthrough);
        // assert_eq!(s.equality.len(), 1);
        // assert!(s.range.is_empty());
        // assert!(s.residual.is_empty());
    }

    #[test]
    fn split_classifies_range() {
        // let preds = vec![gt(col(1), lit(5))];
        // let s = PredicateSplit::from_conjuncts(&preds, &resolve_passthrough);
        // assert_eq!(s.range.len(), 1);
    }

    #[test]
    fn split_classifies_residual_when_unrecognized() {
        // Use an expr the matchers don't recognise (e.g. `CASE WHEN ...`).
        // let s = PredicateSplit::from_conjuncts(&[case_expr], &resolve_passthrough);
        // assert_eq!(s.residual.len(), 1);
    }

    #[test]
    fn containment_rejects_when_mv_has_eq_query_doesnt() {
        let mv = PredicateSplit {
            equality: vec![EqualityPred { col: MvColumnId(1), literal: lit_for_test(5) }],
            ..Default::default()
        };
        let query = PredicateSplit::default();
        assert!(contain_and_compensate(&query, &mv).is_none());
    }

    #[test]
    fn containment_compensates_extra_query_eq() {
        let query = PredicateSplit {
            equality: vec![
                EqualityPred { col: MvColumnId(1), literal: lit_for_test(5) },
                EqualityPred { col: MvColumnId(2), literal: lit_for_test(7) },
            ],
            ..Default::default()
        };
        let mv = PredicateSplit {
            equality: vec![EqualityPred { col: MvColumnId(1), literal: lit_for_test(5) }],
            ..Default::default()
        };
        let comp = contain_and_compensate(&query, &mv).expect("should match");
        assert_eq!(comp.eq.len(), 1);
        assert_eq!(comp.eq[0].col, MvColumnId(2));
    }

    fn lit_for_test(v: i64) -> TypedExpr {
        // ADAPT — call the project's literal builder.
        todo!("use project's literal constructor")
    }
}
```

Replace `todo!()` with actual builders before running.

- [ ] **Step 4.5: Run, verify pass**

```
cargo test --lib sql::optimizer::mv_rewrite::predicate_split
```
Expected: 5+ tests pass. Failures most likely from TypedExpr unfamiliarity — re-read `src/sql/analysis/` and adjust.

- [ ] **Step 4.6: Wire into mod.rs and commit**

In `mv_rewrite/mod.rs` add `pub(crate) mod predicate_split;`.

```bash
git add src/sql/optimizer/mv_rewrite/predicate_split.rs src/sql/optimizer/mv_rewrite/mod.rs
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): PredicateSplit + containment + compensation

Decomposes conjuncts into equality / range / residual, checks
containment of MV predicates by query predicates, and derives the
compensating predicate to apply on top of MV scan.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: ColumnRewriter + aggregate-rollup helpers

Goal: Given a query's output column list and an MV's output column list (both tagged with MvColumnId), produce a mapping. For aggregates, support roll-up when query's GROUP BY is coarser than MV's.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/column_rewriter.rs`
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs` (declare submodule)

- [ ] **Step 5.1: Define types and core mapper**

```rust
//! Map a query's output columns onto an MV's output columns using
//! MvColumnId. Also handles aggregate rollup (query coarser than MV).
//!
//! Reference: StarRocks ColumnRewriter, EquationRewriter,
//! RewriteEquivalent (per-agg-function rollup helpers).

use std::collections::HashMap;

use super::column_id::{MvColumnId, MvEquivalence};
use crate::sql::planner::plan::AggregateCall;

/// Output column tagged with MvColumnId.
#[derive(Clone, Debug)]
pub(crate) struct TaggedOutputColumn {
    pub id: MvColumnId,
    pub display: String,
}

/// Mapping from each query output column index to the corresponding
/// MV output column index.
#[derive(Clone, Debug)]
pub(crate) struct ColumnMapping {
    pub query_to_mv: Vec<usize>,
}

pub(crate) fn try_map_outputs(
    query: &[TaggedOutputColumn],
    mv: &[TaggedOutputColumn],
    equiv: &mut MvEquivalence,
) -> Option<ColumnMapping> {
    let mut indices = Vec::with_capacity(query.len());
    let mv_index: HashMap<MvColumnId, usize> = mv
        .iter()
        .enumerate()
        .map(|(i, c)| (equiv.find(c.id), i))
        .collect();
    for q in query {
        let canonical = equiv.find(q.id);
        let idx = mv_index.get(&canonical)?;
        indices.push(*idx);
    }
    Some(ColumnMapping { query_to_mv: indices })
}

/// Roll-up plan: how to compute query's aggregates given MV's
/// precomputed aggregate columns.
#[derive(Clone, Debug)]
pub(crate) enum RollupAction {
    /// Query agg exactly matches an MV agg column — just project it.
    PassThrough { mv_col_index: usize },
    /// Query agg is a SUM rollup over the MV's per-group SUM.
    SumOverSum { mv_col_index: usize },
    /// Query agg is COUNT rollup: SUM the MV's count column.
    SumOverCount { mv_col_index: usize },
    /// Query agg is MIN rollup: MIN the MV's min column.
    MinOverMin { mv_col_index: usize },
    /// Query agg is MAX rollup over MV's max column.
    MaxOverMax { mv_col_index: usize },
    /// Query AVG: SUM(sum_col) / SUM(count_col).
    AvgFromSumCount { sum_col_index: usize, count_col_index: usize },
}

pub(crate) fn try_rollup_aggs(
    query_aggs: &[AggregateCall],
    mv_aggs: &[AggregateCall],
    mv_outputs: &[TaggedOutputColumn],
    arg_to_id: &impl Fn(&AggregateCall, usize) -> Option<MvColumnId>,
    equiv: &mut MvEquivalence,
) -> Option<Vec<RollupAction>> {
    let mut out = Vec::with_capacity(query_aggs.len());
    for q in query_aggs {
        let action = match q.fn_name.as_str() {
            "sum" => {
                let arg = arg_to_id(q, 0)?;
                find_matching(mv_aggs, mv_outputs, "sum", &[arg], equiv).map(|i| RollupAction::SumOverSum { mv_col_index: i })
            }
            "count" => {
                let arg = arg_to_id(q, 0);
                // Match either count(x) or count(*) → count(*)
                find_matching(mv_aggs, mv_outputs, "count", arg.as_slice_opt(), equiv).map(|i| RollupAction::SumOverCount { mv_col_index: i })
            }
            "min" => {
                let arg = arg_to_id(q, 0)?;
                find_matching(mv_aggs, mv_outputs, "min", &[arg], equiv).map(|i| RollupAction::MinOverMin { mv_col_index: i })
            }
            "max" => {
                let arg = arg_to_id(q, 0)?;
                find_matching(mv_aggs, mv_outputs, "max", &[arg], equiv).map(|i| RollupAction::MaxOverMax { mv_col_index: i })
            }
            "avg" => {
                let arg = arg_to_id(q, 0)?;
                let sum_idx = find_matching(mv_aggs, mv_outputs, "sum", &[arg], equiv)?;
                let cnt_idx = find_matching(mv_aggs, mv_outputs, "count", &[arg], equiv)
                    .or_else(|| find_matching(mv_aggs, mv_outputs, "count", &[], equiv))?;
                Some(RollupAction::AvgFromSumCount { sum_col_index: sum_idx, count_col_index: cnt_idx })
            }
            _ => None, // Unsupported in v1; caller bails out.
        };
        out.push(action?);
    }
    Some(out)
}

fn find_matching(
    mv_aggs: &[AggregateCall],
    mv_outputs: &[TaggedOutputColumn],
    fn_name: &str,
    args_canonical: &[MvColumnId],
    _equiv: &mut MvEquivalence,
) -> Option<usize> {
    // mv_aggs and mv_outputs are parallel slices.
    for (i, agg) in mv_aggs.iter().enumerate() {
        if agg.fn_name != fn_name {
            continue;
        }
        // Match arg count and canonical IDs. arg_to_id-style closure
        // must be applied externally; here we rely on the caller to have
        // pre-resolved the MV args via the same factory + equivalence.
        // For simplicity, this v1 helper accepts the MV agg's
        // ALREADY-TAGGED args being implicitly indexed by `i`. The
        // caller (the rule) is responsible for arg matching.
        let _ = mv_outputs;
        if args_canonical.is_empty() {
            // count(*) — no arg required.
            return Some(i);
        }
        // PSEUDOCODE — caller should pre-resolve MV agg args and pass
        // them via the resolver pattern used in try_rollup_aggs above.
        // For now, return the first match purely by name. The full
        // arg-canonical check is implemented when this is wired into
        // the aggregate-scan rule (Task 9).
        return Some(i);
    }
    None
}

trait OptionSliceExt {
    fn as_slice_opt(&self) -> &[MvColumnId];
}

impl OptionSliceExt for Option<MvColumnId> {
    fn as_slice_opt(&self) -> &[MvColumnId] {
        // Helper for count(*) where Some/None converts to a slice.
        // Implementation detail: we keep the slice empty for None so
        // callers treat "no args" as count(*).
        match self {
            Some(_) => std::slice::from_ref(self.as_ref().unwrap()),
            None => &[],
        }
    }
}
```

(Note: the `count(*)` arg-handling shim is intentionally minimal — full handling is finalized in Task 9 when wired into the aggregate-scan rule.)

- [ ] **Step 5.2: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn tagged(id: u32, name: &str) -> TaggedOutputColumn {
        TaggedOutputColumn { id: MvColumnId(id), display: name.into() }
    }

    #[test]
    fn map_outputs_succeeds_when_every_query_col_in_mv() {
        let q = vec![tagged(1, "a"), tagged(2, "b")];
        let mv = vec![tagged(1, "a"), tagged(2, "b"), tagged(3, "c")];
        let mut e = MvEquivalence::default();
        let m = try_map_outputs(&q, &mv, &mut e).unwrap();
        assert_eq!(m.query_to_mv, vec![0, 1]);
    }

    #[test]
    fn map_outputs_fails_when_query_col_missing() {
        let q = vec![tagged(1, "a"), tagged(99, "missing")];
        let mv = vec![tagged(1, "a")];
        let mut e = MvEquivalence::default();
        assert!(try_map_outputs(&q, &mv, &mut e).is_none());
    }

    #[test]
    fn map_outputs_uses_equivalence() {
        // Query references id=1; MV exposes id=2. Without equivalence, no match.
        // With union(1, 2), they're interchangeable.
        let q = vec![tagged(1, "x")];
        let mv = vec![tagged(2, "x")];
        let mut e = MvEquivalence::default();
        assert!(try_map_outputs(&q, &mv, &mut e).is_none());
        e.union(MvColumnId(1), MvColumnId(2));
        assert!(try_map_outputs(&q, &mv, &mut e).is_some());
    }

    // Rollup tests live in Task 9 where they're wired with real
    // AggregateCall instances. Stub coverage here.
    #[test]
    fn rollup_returns_some_for_supported_fns() {
        // PASS-THROUGH SKELETON — actual asserts in Task 9.
    }
}
```

- [ ] **Step 5.3: Run, verify pass**

```
cargo test --lib sql::optimizer::mv_rewrite::column_rewriter
```
Expected: 4 tests pass (1 skeleton is a no-op).

- [ ] **Step 5.4: Wire and commit**

In `mv_rewrite/mod.rs` add `pub(crate) mod column_rewriter;`.

```bash
git add src/sql/optimizer/mv_rewrite/column_rewriter.rs src/sql/optimizer/mv_rewrite/mod.rs
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): ColumnRewriter + rollup helper skeleton

Query↔MV column mapping via MvColumnId + equivalence. Aggregate
rollup helper enumerates supported functions (sum/count/min/max/avg)
and emits a RollupAction. Full agg-arg-matching wired in Task 9.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Shape extraction + MvRewriter orchestrator skeleton

Goal: Helpers that walk a Memo subgraph and extract a `QueryShape` (or `MvShape`) — root op kind + child operators in canonical order. Also lay down the `MvRewriter` skeleton with the entry method (no rules yet).

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/shape.rs`
- Create: `src/sql/optimizer/mv_rewrite/rewriter.rs`
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs`

- [ ] **Step 6.1: Define shape types**

```rust
//! Walk a Memo group and recognize one of the IVM shapes.
//!
//! ShapeKind discriminates by root operator kind. Inner ops are extracted
//! by walking the FIRST logical expression of each child group — the rule
//! is free to skip Memo groups with multiple logical exprs (rare for the
//! shapes we care about; could be enabled in v2).

use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::*;

#[derive(Clone, Debug)]
pub(crate) struct QueryShape {
    pub kind: QueryShapeKind,
    pub scan_a: LogicalScanOp,             // primary base scan
    pub scan_b: Option<LogicalScanOp>,     // optional second base for join shapes
    pub filter: Option<LogicalFilterOp>,   // optional filter between scan and project/agg/join
    pub join: Option<LogicalJoinOp>,
    pub project: Option<LogicalProjectOp>,
    pub aggregate: Option<LogicalAggregateOp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryShapeKind {
    Projection,           // Project(Filter?(Scan))
    AggregateScan,        // Aggregate(Project?(Filter?(Scan)))
    Join,                 // Join(Scan_or_Filter_or_Project, Scan_or_Filter_or_Project)
    AggregateJoin,        // Aggregate(Project?(Join(...)))
}

pub(crate) fn extract_shape(
    memo: &Memo,
    root: &MExpr,
    kind: QueryShapeKind,
) -> Option<QueryShape> {
    match kind {
        QueryShapeKind::Projection => extract_projection(memo, root),
        QueryShapeKind::AggregateScan => extract_aggregate_scan(memo, root),
        QueryShapeKind::Join => extract_join(memo, root),
        QueryShapeKind::AggregateJoin => extract_aggregate_join(memo, root),
    }
}

fn first_logical_child<'a>(memo: &'a Memo, expr: &MExpr, idx: usize) -> Option<&'a MExpr> {
    let group_id: GroupId = *expr.children.get(idx)?;
    memo.groups.get(group_id)?.logical_exprs.first()
}

fn extract_projection(memo: &Memo, root: &MExpr) -> Option<QueryShape> {
    let proj = match &root.op {
        Operator::LogicalProject(p) => p.clone(),
        _ => return None,
    };
    let mut cur = first_logical_child(memo, root, 0)?;
    let mut filter: Option<LogicalFilterOp> = None;
    if let Operator::LogicalFilter(f) = &cur.op {
        filter = Some(f.clone());
        cur = first_logical_child(memo, cur, 0)?;
    }
    let scan = match &cur.op {
        Operator::LogicalScan(s) => s.clone(),
        _ => return None,
    };
    Some(QueryShape {
        kind: QueryShapeKind::Projection,
        scan_a: scan,
        scan_b: None,
        filter,
        join: None,
        project: Some(proj),
        aggregate: None,
    })
}

fn extract_aggregate_scan(memo: &Memo, root: &MExpr) -> Option<QueryShape> {
    let agg = match &root.op {
        Operator::LogicalAggregate(a) => a.clone(),
        _ => return None,
    };
    let mut cur = first_logical_child(memo, root, 0)?;
    let mut project: Option<LogicalProjectOp> = None;
    if let Operator::LogicalProject(p) = &cur.op {
        project = Some(p.clone());
        cur = first_logical_child(memo, cur, 0)?;
    }
    let mut filter: Option<LogicalFilterOp> = None;
    if let Operator::LogicalFilter(f) = &cur.op {
        filter = Some(f.clone());
        cur = first_logical_child(memo, cur, 0)?;
    }
    let scan = match &cur.op {
        Operator::LogicalScan(s) => s.clone(),
        _ => return None,
    };
    Some(QueryShape {
        kind: QueryShapeKind::AggregateScan,
        scan_a: scan,
        scan_b: None,
        filter,
        join: None,
        project,
        aggregate: Some(agg),
    })
}

fn extract_join(memo: &Memo, root: &MExpr) -> Option<QueryShape> {
    let join = match &root.op {
        Operator::LogicalJoin(j) => j.clone(),
        _ => return None,
    };
    let left_scan = unwrap_to_scan(memo, first_logical_child(memo, root, 0)?)?;
    let right_scan = unwrap_to_scan(memo, first_logical_child(memo, root, 1)?)?;
    Some(QueryShape {
        kind: QueryShapeKind::Join,
        scan_a: left_scan,
        scan_b: Some(right_scan),
        filter: None,
        join: Some(join),
        project: None,
        aggregate: None,
    })
}

fn extract_aggregate_join(memo: &Memo, root: &MExpr) -> Option<QueryShape> {
    let agg = match &root.op {
        Operator::LogicalAggregate(a) => a.clone(),
        _ => return None,
    };
    let mut cur = first_logical_child(memo, root, 0)?;
    let mut project: Option<LogicalProjectOp> = None;
    if let Operator::LogicalProject(p) = &cur.op {
        project = Some(p.clone());
        cur = first_logical_child(memo, cur, 0)?;
    }
    let join = match &cur.op {
        Operator::LogicalJoin(j) => j.clone(),
        _ => return None,
    };
    let left_scan = unwrap_to_scan(memo, first_logical_child(memo, cur, 0)?)?;
    let right_scan = unwrap_to_scan(memo, first_logical_child(memo, cur, 1)?)?;
    Some(QueryShape {
        kind: QueryShapeKind::AggregateJoin,
        scan_a: left_scan,
        scan_b: Some(right_scan),
        filter: None,
        join: Some(join),
        project,
        aggregate: Some(agg),
    })
}

fn unwrap_to_scan(memo: &Memo, mut cur: &MExpr) -> Option<LogicalScanOp> {
    loop {
        match &cur.op {
            Operator::LogicalScan(s) => return Some(s.clone()),
            Operator::LogicalProject(_) | Operator::LogicalFilter(_) => {
                cur = first_logical_child(memo, cur, 0)?;
            }
            _ => return None,
        }
    }
}
```

- [ ] **Step 6.2: Define MvRewriter skeleton**

`src/sql/optimizer/mv_rewrite/rewriter.rs`:

```rust
//! Orchestrator for one MV rewrite attempt.
//!
//! Per-rule entry point. Each Mv*RewriteRule constructs an MvRewriter
//! with the current MvRewriteCtx, then calls try_rewrite() with the
//! query shape and candidate MV. Returns either a new GroupId for the
//! Memo (the rewritten subtree root) or an error string for trace.

use super::shape::QueryShape;
use super::MvRewriteCtx;
use crate::sql::optimizer::memo::{GroupId, Memo};

pub(crate) struct MvRewriter<'a> {
    pub ctx: &'a MvRewriteCtx,
}

impl<'a> MvRewriter<'a> {
    pub(crate) fn new(ctx: &'a MvRewriteCtx) -> Self {
        Self { ctx }
    }

    /// Attempt to rewrite the query subtree using the candidate MV.
    /// Returns Ok(Some(new_root_group_id)) on success, Ok(None) when the
    /// candidate matches structurally but is skipped (e.g. 0% fresh),
    /// or Err(reason) when match fails.
    pub(crate) fn try_rewrite(
        &self,
        memo: &mut Memo,
        query_shape: &QueryShape,
        candidate_mv_id: i64,
    ) -> Result<Option<GroupId>, String> {
        // Skeleton — body wired in Tasks 7-11.
        let _ = (memo, query_shape, candidate_mv_id);
        Err("not implemented in skeleton".into())
    }
}
```

- [ ] **Step 6.3: Tests**

```rust
// In shape.rs:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::memo::Memo;

    // Helper: build a Memo with the desired Operator tree and return
    // the root GroupId.
    // Tests construct Operator trees by hand because the project's
    // higher-level builders all require Analyzer output. For each shape
    // we assert extract_shape returns Some(...) with matching fields.

    #[test]
    fn extract_projection_recognizes_project_filter_scan() {
        // build: Project(Filter(Scan))
        // assert extract_shape(memo, root, QueryShapeKind::Projection)
        //   returns Some with scan_a, filter, project all set.
        // ADAPT to actual construction helpers.
    }

    #[test]
    fn extract_aggregate_scan_recognizes_agg_over_scan() {
        // build: Aggregate(Scan)
        // assert Some with aggregate.is_some() and scan_a set.
    }

    #[test]
    fn extract_join_recognizes_join_of_two_scans() {
        // build: Join(Scan, Scan)
        // assert Some with scan_a, scan_b, join all set.
    }

    #[test]
    fn extract_returns_none_for_wrong_kind() {
        // build: simple Scan
        // assert extract_shape(memo, root, QueryShapeKind::Projection) is None.
    }
}
```

Use the `Memo::new_group` API + `MExpr` direct construction (see `src/sql/optimizer/memo.rs` for the pattern; example in `cte_rewrite.rs` or `convert.rs`).

- [ ] **Step 6.4: Run, verify pass**

```
cargo test --lib sql::optimizer::mv_rewrite::shape
```
Expected: 4+ tests pass.

- [ ] **Step 6.5: Wire and commit**

In `mv_rewrite/mod.rs`:
```rust
pub(crate) mod shape;
pub(crate) mod rewriter;
```

```bash
git add src/sql/optimizer/mv_rewrite/shape.rs src/sql/optimizer/mv_rewrite/rewriter.rs src/sql/optimizer/mv_rewrite/mod.rs
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): shape extraction + MvRewriter skeleton

Walks a Memo subgraph to extract QueryShape (Projection / AggregateScan
/ Join / AggregateJoin). MvRewriter skeleton accepts ctx, shape, and
candidate MV id — body filled in per-rule in following tasks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: MvProjectionRewriteRule (first end-to-end wired rule)

Goal: First wired transformation rule. Handles single-base SPJF queries against single-base SPJF MVs. Implements full pipeline: shape extract → column map → predicate split → containment → emit `Project(Filter(Scan(MV)))` as Memo alternative. Excludes UNION compensation (Task 8).

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/rules/projection.rs`
- Modify: `src/sql/optimizer/mv_rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rules/mod.rs` (register the rule conditionally)
- Modify: `src/sql/optimizer/mv_rewrite/rewriter.rs` (implement try_rewrite for Projection)
- Create: `sql-tests/optimizer/mv_rewrite_projection_full_fresh.sql`
- Create: `sql-tests/optimizer/mv_rewrite_reject_predicate_mismatch.sql`
- Create: `sql-tests/mv-on-iceberg/rewrite/projection_full.sql`

- [ ] **Step 7.1: Implement try_rewrite for Projection shape**

In `rewriter.rs`, expand:

```rust
impl<'a> MvRewriter<'a> {
    pub(crate) fn try_rewrite(
        &self,
        memo: &mut Memo,
        query_shape: &QueryShape,
        candidate_mv_id: i64,
    ) -> Result<Option<GroupId>, String> {
        match query_shape.kind {
            super::shape::QueryShapeKind::Projection => self.rewrite_projection(memo, query_shape, candidate_mv_id),
            _ => Err("shape not implemented yet".into()),
        }
    }

    fn rewrite_projection(
        &self,
        memo: &mut Memo,
        query: &QueryShape,
        mv_id: i64,
    ) -> Result<Option<GroupId>, String> {
        // 1. Resolve MV candidate. (Registry lookup performed by the
        //    rule before calling rewriter; here we re-fetch metadata.)
        let mv_def = self.ctx.registry().lookup_by_id(mv_id)
            .ok_or_else(|| "mv candidate vanished".to_string())?;

        // 2. Re-parse MV definition into LogicalPlan and lower to Operator.
        //    Cached inside registry. Adapt to project's existing helper:
        //    see `src/engine/mod.rs` for the `analyze() -> plan_query()`
        //    pipeline. Reuse it.
        let mv_shape = self.ctx.registry().mv_shape(mv_id, self.ctx)?
            .ok_or_else(|| "MV is not a Projection shape".to_string())?;

        // 3. Build MvColumnIdFactory + tag both query and MV columns.
        //    Both must be tagged from the SAME factory using
        //    MvColumnIdKey::Base { table_uuid, field_id } seeded from
        //    `mv_def.schema_contract.base.base_field_records`.
        let mut factory = super::column_id::MvColumnIdFactory::new();
        let mut equiv = super::column_id::MvEquivalence::default();
        let q_tagged = tag_projection_outputs(&mut factory, &mut equiv, query, &mv_def)?;
        let mv_tagged = tag_projection_outputs(&mut factory, &mut equiv, &mv_shape, &mv_def)?;

        // 4. Column match.
        let column_map = super::column_rewriter::try_map_outputs(&q_tagged, &mv_tagged, &mut equiv)
            .ok_or_else(|| "output column mapping failed".to_string())?;

        // 5. Predicate split + containment.
        let q_split = super::predicate_split::PredicateSplit::from_conjuncts(
            &collect_conjuncts(query),
            &|e| resolve_col(&factory, e),
        );
        let mv_split = super::predicate_split::PredicateSplit::from_conjuncts(
            &collect_conjuncts(&mv_shape),
            &|e| resolve_col(&factory, e),
        );
        let compensation = super::predicate_split::contain_and_compensate(&q_split, &mv_split)
            .ok_or_else(|| "predicates not contained".to_string())?;

        // 6. Build rewritten subtree:
        //      Project(query.project, Filter(compensation, Scan(MV_target_table)))
        //    Skip the Filter wrapping when compensation is empty.
        let mv_scan_op = build_mv_scan_op(&mv_def);
        let mv_scan_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(mv_scan_op),
            children: vec![],
        });
        let filtered_group = if let Some(comp_expr) = compensation.into_typed_expr() {
            memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalFilter(LogicalFilterOp { predicate: comp_expr }),
                children: vec![mv_scan_group],
            })
        } else {
            mv_scan_group
        };

        // The project that goes on top must use the QUERY's project items
        // re-targeted at MV's output columns via column_map.
        let projected_op = retarget_project(query.project.as_ref().unwrap(), &column_map, &mv_tagged);
        let project_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalProject(projected_op),
            children: vec![filtered_group],
        });

        Ok(Some(project_group))
    }
}

// Helpers — implement using the actual TypedExpr / Operator structures.
fn tag_projection_outputs(
    _factory: &mut super::column_id::MvColumnIdFactory,
    _equiv: &mut super::column_id::MvEquivalence,
    _shape: &QueryShape,
    _mv_def: &crate::meta::repository::mv::StoredMvDefinition,
) -> Result<Vec<super::column_rewriter::TaggedOutputColumn>, String> {
    // For each output column, derive an MvColumnIdKey:
    //   - If column is a direct scan column: Base{ uuid, field_id }
    //     uuid: mv_def.schema_contract.base.base_field_records lookup
    //   - If derived: Derived{ expr_hash }
    // Then factory.intern() and equiv.find() it.
    Err("not implemented — finish in sub-agent".to_string())
}

fn collect_conjuncts(shape: &QueryShape) -> Vec<crate::sql::analysis::TypedExpr> {
    // Combine: scan_a.predicates + filter.predicate + project items'
    // built-in filter terms (if any). Returns a flat conjunction list.
    let mut out = shape.scan_a.predicates.clone();
    if let Some(f) = &shape.filter {
        out.push(f.predicate.clone());
    }
    out
}

fn resolve_col(
    _factory: &super::column_id::MvColumnIdFactory,
    _e: &crate::sql::analysis::TypedExpr,
) -> Option<super::column_id::MvColumnId> {
    // PSEUDOCODE — given a ColumnRef-shaped TypedExpr, look up its
    // (uuid, field_id) via the active scan metadata and return the
    // interned MvColumnId. Adapt to the project's ColumnRef shape.
    None
}

fn build_mv_scan_op(
    _mv_def: &crate::meta::repository::mv::StoredMvDefinition,
) -> LogicalScanOp {
    // Construct a LogicalScanOp pointing at the MV target table.
    // Use Iceberg catalog + namespace + table from mv_def.target_*.
    // Reuse the same TableDef resolution path that normal scans use
    // (look at how src/engine/query_prep.rs builds LogicalScanOp).
    panic!("not implemented")
}

fn retarget_project(
    _query_project: &LogicalProjectOp,
    _column_map: &super::column_rewriter::ColumnMapping,
    _mv_tagged: &[super::column_rewriter::TaggedOutputColumn],
) -> LogicalProjectOp {
    // Replace each ProjectItem expr referencing a query-column with
    // a reference to the matching MV output column.
    panic!("not implemented")
}
```

The bodies marked "not implemented — finish in sub-agent" must be implemented by the sub-agent reading actual codebase types. The skeleton makes the contracts explicit.

- [ ] **Step 7.2: Add registry helpers `lookup_by_id` and `mv_shape`**

Extend `registry.rs`:

```rust
impl MvCandidateRegistry {
    pub(crate) fn lookup_by_id(&self, mv_id: i64) -> Option<StoredMvDefinition> {
        let cache = self.by_base_table.lock().unwrap();
        for cands in cache.values() {
            if let Some(c) = cands.iter().find(|c| c.mv_id == mv_id) {
                return Some(c.definition.clone());
            }
        }
        None
    }

    /// Re-parse the MV's `select_sql` into a QueryShape. Cached.
    pub(crate) fn mv_shape(
        &self,
        mv_id: i64,
        ctx: &super::MvRewriteCtx,
    ) -> Result<Option<super::shape::QueryShape>, String> {
        // Cache per-mv_id under a separate field. Re-parses via
        // analyze() + plan_query() + RBO. Then runs extract_shape with
        // QueryShapeKind::Projection — caller decides whether the
        // shape matches their expected kind.
        // PSEUDOCODE — sub-agent fills in using src/engine/mod.rs as
        // the example of analyze→plan flow.
        let _ = (mv_id, ctx);
        Err("not implemented in skeleton".into())
    }
}
```

- [ ] **Step 7.3: Create the rule struct**

`src/sql/optimizer/mv_rewrite/rules/projection.rs`:

```rust
use super::super::{shape, MvRewriteCtx};
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

pub(crate) struct MvProjectionRewriteRule {
    ctx: MvRewriteCtx,
}

impl MvProjectionRewriteRule {
    pub(crate) fn new(ctx: MvRewriteCtx) -> Self {
        Self { ctx }
    }
}

impl Rule for MvProjectionRewriteRule {
    fn name(&self) -> &str {
        "MvProjectionRewrite"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalProject(_)) && self.ctx.enabled()
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        // 1. Shape extract
        let Some(query_shape) = shape::extract_shape(memo, expr, shape::QueryShapeKind::Projection) else {
            return vec![];
        };
        // 2. Look up candidate MVs by base table.
        let base_fqn = format!(
            "{}.{}",
            query_shape.scan_a.database,
            // TableDef has a name accessor — adapt.
            query_shape.scan_a.alias.clone().unwrap_or_default(),
        );
        // The all_defs sourcing path will be wired in Step 7.4. For now,
        // the registry must be pre-warmed by the optimizer entry path.
        let candidates = self.ctx.registry().candidates_for_base(&base_fqn, &[]);
        let max = self.ctx.max_candidates_per_group();
        let mut out = vec![];
        for cand in candidates.into_iter().take(max) {
            let rewriter = super::super::rewriter::MvRewriter::new(&self.ctx);
            match rewriter.try_rewrite(memo, &query_shape, cand.mv_id) {
                Ok(Some(new_group)) => {
                    // The new_group already contains the full rewritten
                    // subtree. We need to return a NewExpr whose op
                    // matches `expr`'s position in the group. We emit a
                    // pass-through Project that delegates to new_group.
                    // Adapt the NewExpr shape to your Memo conventions —
                    // an alternative is to add MExprs directly to the
                    // root group rather than via NewExpr.
                    if let Some(group) = memo.groups.get(new_group) {
                        if let Some(top) = group.logical_exprs.first() {
                            out.push(NewExpr { op: top.op.clone(), children: top.children.clone() });
                        }
                    }
                }
                Ok(None) | Err(_) => continue,
            }
        }
        out
    }
}
```

- [ ] **Step 7.4: Wire all_defs source into the rule**

The rule needs to source `StoredMvDefinition`s without exfiltrating the global catalog. The cleanest path: add a snapshot-loader closure to `MvRewriteCtx` populated by `optimize()`:

```rust
// In mv_rewrite/mod.rs:
pub(crate) struct MvRewriteCtxInner {
    // ... existing fields ...
    pub all_mv_defs: Vec<StoredMvDefinition>,
}

impl MvRewriteCtx {
    pub(crate) fn from_options_with_defs(opts: &OptimizerOptions, defs: Vec<StoredMvDefinition>) -> Self { ... }

    pub(crate) fn all_mv_defs(&self) -> &[StoredMvDefinition] {
        &self.inner.all_mv_defs
    }
}
```

In `optimize()`:
```rust
let mv_defs = load_all_mv_defs_for_session(); // see src/engine/mv_flow.rs for the existing call site that lists MVs
let mv_ctx = mv_rewrite::MvRewriteCtx::from_options_with_defs(&options, mv_defs);
```

Implementation note: `load_all_mv_defs_for_session()` should call `MvMetaRepository::list_definitions(&mut txn)` where `txn` comes from the active standalone-server session. Look at `src/engine/mv_flow.rs` for an existing read-only path.

Update rule's call:
```rust
let candidates = self.ctx.registry().candidates_for_base(&base_fqn, self.ctx.all_mv_defs());
```

- [ ] **Step 7.5: Register the rule**

In `src/sql/optimizer/rules/mod.rs`:

```rust
pub(crate) mod mv {
    pub(crate) use super::super::mv_rewrite::rules::*;
}

pub(crate) fn all_transformation_rules(mv_ctx: &super::mv_rewrite::MvRewriteCtx) -> Vec<Box<dyn Rule>> {
    let mut rules: Vec<Box<dyn Rule>> = vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_top_n::SplitTopN),
    ];
    if mv_ctx.enabled() {
        rules.push(Box::new(super::mv_rewrite::rules::projection::MvProjectionRewriteRule::new(mv_ctx.clone())));
    }
    rules
}
```

In `mv_rewrite/rules/mod.rs`:
```rust
pub(crate) mod projection;
```

- [ ] **Step 7.6: Sub-agent implements the panic!() stubs**

The remaining work in this task is to replace the `panic!()` and `Err("not implemented")` stubs with real code based on:
- `src/sql/analysis/` — `TypedExpr`, `ColumnRef`, `OutputColumn`
- `src/sql/planner/` — `LogicalPlan`, `AggregateCall`, `WindowExpr`
- `src/sql/catalog.rs` — `TableDef` (line 221)
- `src/engine/mod.rs` — the `analyze() → plan_query() → optimize()` chain
- `src/engine/query_prep.rs` — how to build `LogicalScanOp` from a `TableDef`
- `src/meta/repository/mv_contract.rs` — `MvSchemaContract`, `BaseFieldRecord`, field-id lookup

Sub-agent task per stub:
1. `tag_projection_outputs` — for each `OutputColumn` of the scan, look up its base field-id via `MvSchemaContract.base.base_field_records`. Intern with `MvColumnIdKey::Base { table_uuid, field_id }`. For project items, hash the canonicalized expression in terms of already-tagged column ids.
2. `resolve_col` — given a `TypedExpr`, peel any `Cast` wrappers, recognize `ColumnRef`, look up its base field, intern.
3. `build_mv_scan_op` — construct a `LogicalScanOp` pointing at the MV's target Iceberg table. Use the same code path as a normal SELECT against that table.
4. `retarget_project` — walk each `ProjectItem`'s expression, replace any column reference whose `MvColumnId` is in the query side with a reference to the corresponding MV output column.
5. `lookup_by_id` and `mv_shape` — registry: re-parse `select_sql` via `analyze + plan + RBO`, then `extract_shape`. Cache by `(mv_id, schema_contract_hash)`.

- [ ] **Step 7.7: Write SQL test — full-fresh projection rewrite**

`sql-tests/optimizer/mv_rewrite_projection_full_fresh.sql`:

```sql
-- @suite: optimizer
-- @explain_contains=mv_west_sales
-- @normalize_explain_timing

CREATE EXTERNAL CATALOG ice_proj1
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "${NOVAROCKS_ICEBERG_REST_URI}",
  "iceberg.catalog.warehouse" = "${NOVAROCKS_ICEBERG_REST_WAREHOUSE}"
);

USE ice_proj1.mv_rewrite_test;

CREATE TABLE base_sales (
    order_id  BIGINT,
    region    STRING,
    amount    DECIMAL(18, 2),
    sold_at   DATE
) PARTITION BY days(sold_at)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

INSERT INTO base_sales VALUES
  (1, 'west', 100.00, DATE '2026-05-10'),
  (2, 'east', 200.00, DATE '2026-05-11');

CREATE MATERIALIZED VIEW mv_west_sales
PARTITION BY days(sold_at)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
AS SELECT order_id, amount, sold_at
   FROM base_sales
   WHERE region = 'west';

REFRESH MATERIALIZED VIEW mv_west_sales;

EXPLAIN VERBOSE
SELECT order_id, amount
FROM base_sales
WHERE region = 'west' AND amount > 50;
```

- [ ] **Step 7.8: Write SQL test — rejection on predicate mismatch**

`sql-tests/optimizer/mv_rewrite_reject_predicate_mismatch.sql`:

```sql
-- @suite: optimizer
-- @explain_contains=base_sales
-- ! @explain_contains=mv_west_sales
-- (the rule should reject because query's region='east' is not contained
--  in MV's region='west')
-- @normalize_explain_timing

-- Schema/MV same as above.
EXPLAIN VERBOSE
SELECT order_id, amount
FROM base_sales
WHERE region = 'east' AND amount > 50;
```

- [ ] **Step 7.9: Write end-to-end correctness test**

`sql-tests/mv-on-iceberg/rewrite/projection_full.sql`:

```sql
-- @suite: mv-on-iceberg
-- Setup ice_proj1 catalog (same as plan-shape test) then:

CREATE TABLE base_sales (...);
INSERT INTO base_sales VALUES ...;
CREATE MATERIALIZED VIEW mv_west_sales AS SELECT ... WHERE region='west';
REFRESH MATERIALIZED VIEW mv_west_sales;

-- Query and assert result equals direct base-table query.
SELECT order_id, amount FROM base_sales WHERE region='west' AND amount > 50
ORDER BY order_id;
-- expected output (golden):
-- 1  100.00
```

- [ ] **Step 7.10: Run tests**

```
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build
cargo test --lib sql::optimizer::mv_rewrite
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only mv_rewrite_projection_full_fresh,mv_rewrite_reject_predicate_mismatch \
  --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --only rewrite/projection_full --mode verify
```

Expected: all green.

- [ ] **Step 7.11: Commit**

```bash
git add src/sql/optimizer/mv_rewrite/rules/projection.rs \
        src/sql/optimizer/mv_rewrite/rules/mod.rs \
        src/sql/optimizer/mv_rewrite/rewriter.rs \
        src/sql/optimizer/mv_rewrite/registry.rs \
        src/sql/optimizer/mv_rewrite/mod.rs \
        src/sql/optimizer/rules/mod.rs \
        src/sql/optimizer/mod.rs \
        sql-tests/optimizer/mv_rewrite_projection_full_fresh.sql \
        sql-tests/optimizer/mv_rewrite_reject_predicate_mismatch.sql \
        sql-tests/mv-on-iceberg/rewrite/projection_full.sql
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): MvProjectionRewriteRule

First wired MV rewrite rule. Handles single-base SPJF queries with
predicate compensation. No UNION compensation yet — only full-fresh
MVs match in this commit (Task 8 adds the union path).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: PartitionCompensator + UNION transparent synthesis

Goal: Compute fresh/stale partition split for an MV against a query's partition predicate; synthesize `UNION ALL(MV-scan-fresh-only, base-scan-stale-only re-projected through MV's transform)` when partial freshness applies.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/partition_compensator.rs`
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs`
- Modify: `src/sql/optimizer/mv_rewrite/rewriter.rs` (call compensator)
- Create: `sql-tests/optimizer/mv_rewrite_projection_partial_fresh.sql`
- Create: `sql-tests/mv-on-iceberg/rewrite/projection_partial.sql`

- [ ] **Step 8.1: Define types**

```rust
//! Per-partition freshness oracle and UNION ALL synthesis for partial
//! MV freshness.
//!
//! Reference: StarRocks MvPartitionCompensator.

use std::collections::HashSet;

use crate::meta::repository::mv::StoredMvDefinition;

#[derive(Clone, Debug)]
pub(crate) struct FreshnessSplit {
    /// Base partition identifiers (canonical string form, e.g.
    /// "sold_at=2026-05-15"). Fresh = MV reflects base's current state.
    pub fresh_partitions: HashSet<String>,
    /// Base partition identifiers where base has data the MV hasn't seen.
    pub stale_partitions: HashSet<String>,
}

impl FreshnessSplit {
    pub(crate) fn fresh_ratio(&self) -> f64 {
        let total = (self.fresh_partitions.len() + self.stale_partitions.len()) as f64;
        if total == 0.0 {
            return 1.0;
        }
        self.fresh_partitions.len() as f64 / total
    }
}

/// Compute fresh/stale split for `mv` against `base_table_fqn` and the
/// set of base partitions the query is going to read.
pub(crate) fn compute_freshness(
    mv: &StoredMvDefinition,
    base_table_fqn: &str,
    query_partitions: &HashSet<String>,
    iceberg_diff: &dyn IcebergSnapshotDiff,
) -> FreshnessSplit {
    let s_mv = mv.last_refresh_snapshots.get(base_table_fqn).copied();
    let s_now = iceberg_diff.current_snapshot(base_table_fqn);
    if s_mv == Some(s_now) {
        return FreshnessSplit {
            fresh_partitions: query_partitions.clone(),
            stale_partitions: HashSet::new(),
        };
    }
    let touched: HashSet<String> = match s_mv {
        Some(snap) => iceberg_diff.partitions_changed_between(base_table_fqn, snap, s_now),
        None => query_partitions.clone(),
    };
    let stale: HashSet<String> = query_partitions.intersection(&touched).cloned().collect();
    let fresh: HashSet<String> = query_partitions.difference(&stale).cloned().collect();
    FreshnessSplit { fresh_partitions: fresh, stale_partitions: stale }
}

/// Trait implemented over the Iceberg catalog client to expose
/// snapshot diff. Reuses the same primitives as IVM's IcebergDeltaScan.
pub(crate) trait IcebergSnapshotDiff {
    fn current_snapshot(&self, base_table_fqn: &str) -> i64;
    fn partitions_changed_between(
        &self,
        base_table_fqn: &str,
        from: i64,
        to: i64,
    ) -> HashSet<String>;
}
```

- [ ] **Step 8.2: UNION ALL synthesis helper**

```rust
use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::*;

/// Synthesize a UNION ALL plan over the fresh-MV branch and the
/// stale-base branch.
///
/// `mv_branch` — root group of the MV-scan branch (already filtered to
///               include only fresh_partitions).
/// `base_branch` — root group of the base-scan branch (filtered to
///                 stale_partitions, then projected through the MV's
///                 transform so its output schema matches mv_branch).
pub(crate) fn synthesize_union_all(
    memo: &mut Memo,
    mv_branch: GroupId,
    base_branch: GroupId,
) -> GroupId {
    let union_op = LogicalUnionOp { all: true };
    let id = memo.next_expr_id();
    memo.new_group(MExpr {
        id,
        op: Operator::LogicalUnion(union_op),
        children: vec![mv_branch, base_branch],
    })
}

/// Build a partition-filter predicate that constrains a scan to the
/// given set of partition keys. E.g. "sold_at IN ('2026-05-15', ...)".
///
/// Returns None when the set is empty (caller should skip this branch).
pub(crate) fn build_partition_filter(
    _partition_col: &crate::sql::analysis::TypedExpr,
    partitions: &HashSet<String>,
) -> Option<crate::sql::analysis::TypedExpr> {
    if partitions.is_empty() {
        return None;
    }
    // PSEUDOCODE — construct an IN-list TypedExpr.
    // Use the project's builder. See src/sql/analysis/scalar.rs for
    // existing IN constructors.
    None
}
```

- [ ] **Step 8.3: Wire compensator into rewriter**

In `rewrite_projection`, after computing `compensation`, add:

```rust
// 7. Compute partition freshness.
let query_partitions = self.extract_query_partitions(query);
let mv_snapshot_diff = self.ctx.iceberg_diff();
let split = super::partition_compensator::compute_freshness(
    &mv_def,
    &base_table_fqn(&query.scan_a),
    &query_partitions,
    mv_snapshot_diff,
);
if split.fresh_ratio() < self.ctx.min_fresh_ratio() {
    return Ok(None); // not worth rewriting
}
if split.stale_partitions.is_empty() {
    // All fresh → pure MV scan path (already built above).
    return Ok(Some(project_group));
}
if !self.ctx.union_enabled() {
    return Ok(None);
}
// 8. Build base-scan branch limited to stale partitions, then push the
//    MV's logical tree on top so its output schema aligns.
let base_branch = self.build_base_branch_for_stale_partitions(memo, query, &mv_shape, &split)?;
// 9. Filter the MV branch to fresh partitions only.
let mv_branch_filtered = self.filter_mv_branch_to_fresh(memo, project_group, &split, query)?;
// 10. UNION ALL them.
let union_group = super::partition_compensator::synthesize_union_all(memo, mv_branch_filtered, base_branch);
Ok(Some(union_group))
```

Implement `extract_query_partitions`, `build_base_branch_for_stale_partitions`, `filter_mv_branch_to_fresh` as additional `MvRewriter` methods. These are the densest parts of the implementation; the sub-agent should follow StarRocks `MvPartitionCompensator.compensate(...)` as the canonical reference.

- [ ] **Step 8.4: Stub the iceberg_diff() accessor on ctx**

In `MvRewriteCtx`:

```rust
struct MvRewriteCtxInner {
    // ... existing ...
    pub iceberg_diff: Arc<dyn super::partition_compensator::IcebergSnapshotDiff + Send + Sync>,
}

impl MvRewriteCtx {
    pub(crate) fn iceberg_diff(&self) -> &dyn super::partition_compensator::IcebergSnapshotDiff {
        self.inner.iceberg_diff.as_ref()
    }
}
```

Implement a concrete adapter type `IcebergCatalogSnapshotDiff` wrapping the iceberg-rust catalog client. Use the same client that IVM's `IcebergDeltaScan` uses (`src/connector/iceberg/changes.rs`).

For unit testing of the compensator, also provide a `StubIcebergSnapshotDiff` in `#[cfg(test)]`.

- [ ] **Step 8.5: Tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    struct StubDiff {
        current: i64,
        changed: HashSet<String>,
    }
    impl IcebergSnapshotDiff for StubDiff {
        fn current_snapshot(&self, _t: &str) -> i64 { self.current }
        fn partitions_changed_between(&self, _t: &str, _from: i64, _to: i64) -> HashSet<String> {
            self.changed.clone()
        }
    }

    fn mk_mv(base: &str, mv_snap: i64) -> crate::meta::repository::mv::StoredMvDefinition {
        let mut snaps = BTreeMap::new();
        snaps.insert(base.to_string(), mv_snap);
        crate::meta::repository::mv::StoredMvDefinition {
            mv_id: 1,
            select_sql: "SELECT 1".into(),
            base_table_refs: vec![base.into()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".into(),
            target_catalog: None,
            target_namespace: None,
            target_table: Some("t".into()),
            schema_contract: None,
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: Some(100),
            last_refresh_snapshots: snaps,
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            created_at_ms: 0,
        }
    }

    #[test]
    fn all_fresh_when_snapshots_match() {
        let mv = mk_mv("b", 10);
        let qp: HashSet<String> = ["p1".into(), "p2".into()].into_iter().collect();
        let diff = StubDiff { current: 10, changed: HashSet::new() };
        let s = compute_freshness(&mv, "b", &qp, &diff);
        assert_eq!(s.fresh_partitions, qp);
        assert!(s.stale_partitions.is_empty());
        assert_eq!(s.fresh_ratio(), 1.0);
    }

    #[test]
    fn stale_partitions_appear_when_diff_touches_them() {
        let mv = mk_mv("b", 10);
        let qp: HashSet<String> = ["p1".into(), "p2".into(), "p3".into()].into_iter().collect();
        let diff = StubDiff { current: 11, changed: ["p2".into()].into_iter().collect() };
        let s = compute_freshness(&mv, "b", &qp, &diff);
        let mut stale: Vec<_> = s.stale_partitions.into_iter().collect();
        stale.sort();
        assert_eq!(stale, vec!["p2".to_string()]);
        let mut fresh: Vec<_> = s.fresh_partitions.into_iter().collect();
        fresh.sort();
        assert_eq!(fresh, vec!["p1".to_string(), "p3".to_string()]);
        assert!((s.fresh_ratio() - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn fresh_ratio_one_when_query_partitions_empty() {
        let mv = mk_mv("b", 10);
        let qp = HashSet::new();
        let diff = StubDiff { current: 10, changed: HashSet::new() };
        let s = compute_freshness(&mv, "b", &qp, &diff);
        assert_eq!(s.fresh_ratio(), 1.0);
    }
}
```

- [ ] **Step 8.6: Run, verify**

```
cargo test --lib sql::optimizer::mv_rewrite::partition_compensator
cargo build
```

- [ ] **Step 8.7: Write partial-fresh SQL tests**

`sql-tests/optimizer/mv_rewrite_projection_partial_fresh.sql` — assert `UNION ALL` appears, with `mv_west_sales` on one branch and `base_sales` on the other:

```sql
-- @suite: optimizer
-- @explain_contains=UNION ALL
-- @explain_contains=mv_west_sales
-- @explain_contains=base_sales
-- @normalize_explain_timing

-- Setup base + MV like Task 7, then add a new partition that the MV
-- doesn't know about:
INSERT INTO base_sales VALUES (3, 'west', 300.00, DATE '2026-05-20');

EXPLAIN VERBOSE
SELECT order_id, amount FROM base_sales
WHERE region = 'west' AND sold_at BETWEEN '2026-05-01' AND '2026-05-31';
```

`sql-tests/mv-on-iceberg/rewrite/projection_partial.sql` — end-to-end correctness for partial freshness. Same setup; assert query result == direct base-table query result.

- [ ] **Step 8.8: Run all tests**

```
cargo build
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only mv_rewrite_projection_partial_fresh,mv_rewrite_projection_full_fresh,mv_rewrite_reject_predicate_mismatch \
  --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --only rewrite/projection_full,rewrite/projection_partial --mode verify
```

- [ ] **Step 8.9: Commit**

```bash
git add src/sql/optimizer/mv_rewrite/partition_compensator.rs \
        src/sql/optimizer/mv_rewrite/rewriter.rs \
        src/sql/optimizer/mv_rewrite/mod.rs \
        sql-tests/optimizer/mv_rewrite_projection_partial_fresh.sql \
        sql-tests/mv-on-iceberg/rewrite/projection_partial.sql
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): PartitionCompensator + UNION transparent synthesis

Computes per-partition freshness for an MV against a query's partition
predicate using Iceberg snapshot diff (reused from IVM IcebergDeltaScan
infrastructure). When partial freshness applies, synthesises a
UNION ALL of (fresh-partition MV scan) and (stale-partition base scan
re-projected through MV's transform).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: MvAggregateScanRewriteRule

Goal: SPJG single-base rule. Supports query GROUP BY ⊆ MV GROUP BY (rollup) and query GROUP BY == MV GROUP BY. Supports `SUM`, `COUNT`, `MIN`, `MAX`, `AVG` (decomposed to SUM+COUNT).

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/rules/aggregate_scan.rs`
- Modify: `src/sql/optimizer/mv_rewrite/rewriter.rs` (add `rewrite_aggregate_scan`)
- Modify: `src/sql/optimizer/mv_rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rules/mod.rs`
- Modify: `src/sql/optimizer/mv_rewrite/column_rewriter.rs` (finalize `find_matching` with real arg-matching now that we have shape context)
- Create: `sql-tests/optimizer/mv_rewrite_aggregate_full_fresh.sql`
- Create: `sql-tests/optimizer/mv_rewrite_aggregate_rollup.sql`
- Create: `sql-tests/optimizer/mv_rewrite_reject_groupby_finer.sql`
- Create: `sql-tests/mv-on-iceberg/rewrite/aggregate.sql`

- [ ] **Step 9.1: Implement rewrite_aggregate_scan in rewriter**

Mirror `rewrite_projection`. Key additions:
- Check `query.aggregate` GROUP BY ⊆ MV's GROUP BY by mapping group-by exprs to MvColumnId and using set inclusion.
- Reject if query GROUP BY is *strictly larger* than MV's (cannot un-aggregate).
- Use `try_rollup_aggs` (Task 5) to build the rollup plan.
- Emit `LogicalAggregate{group_by=query_gb, aggregates=rollup_aggs}(Project(LogicalScan(MV)))`.

- [ ] **Step 9.2: Finalize `find_matching` with real arg-matching**

In `column_rewriter.rs`, replace the v1 simplification with proper arg-canonical matching, now that the caller (the rule) can provide an `arg_to_id` closure for both query and MV.

- [ ] **Step 9.3: Define MvAggregateScanRewriteRule struct**

Same shape as `MvProjectionRewriteRule` (Task 7). `matches()`:

```rust
fn matches(&self, op: &Operator) -> bool {
    matches!(op, Operator::LogicalAggregate(_)) && self.ctx.enabled()
}
```

- [ ] **Step 9.4: Register**

In `rules/mod.rs`:
```rust
if mv_ctx.enabled() {
    rules.push(Box::new(super::mv_rewrite::rules::projection::MvProjectionRewriteRule::new(mv_ctx.clone())));
    rules.push(Box::new(super::mv_rewrite::rules::aggregate_scan::MvAggregateScanRewriteRule::new(mv_ctx.clone())));
}
```

- [ ] **Step 9.5: SQL tests**

`sql-tests/optimizer/mv_rewrite_aggregate_full_fresh.sql`:
```sql
-- MV: GROUP BY region; aggregates SUM(amount).
-- Query: same GROUP BY; assert MV scan.
CREATE MATERIALIZED VIEW mv_region_sales AS
SELECT region, SUM(amount) AS total_amount
FROM base_sales
GROUP BY region;
REFRESH MATERIALIZED VIEW mv_region_sales;

EXPLAIN VERBOSE
SELECT region, SUM(amount) FROM base_sales GROUP BY region;
-- @explain_contains=mv_region_sales
```

`sql-tests/optimizer/mv_rewrite_aggregate_rollup.sql`:
```sql
-- MV: GROUP BY region, sold_at; aggregates SUM(amount), COUNT(*).
-- Query: GROUP BY region; assert rollup over MV.
CREATE MATERIALIZED VIEW mv_region_day_sales AS
SELECT region, sold_at, SUM(amount) AS s, COUNT(*) AS c
FROM base_sales
GROUP BY region, sold_at;
REFRESH MATERIALIZED VIEW mv_region_day_sales;

EXPLAIN VERBOSE
SELECT region, SUM(amount), COUNT(*) FROM base_sales GROUP BY region;
-- @explain_contains=mv_region_day_sales
```

`sql-tests/optimizer/mv_rewrite_reject_groupby_finer.sql`:
```sql
-- MV: GROUP BY region.
-- Query: GROUP BY region, sold_at. Cannot un-aggregate; assert reject.
EXPLAIN VERBOSE
SELECT region, sold_at, SUM(amount) FROM base_sales GROUP BY region, sold_at;
-- ! @explain_contains=mv_region_sales
-- @explain_contains=base_sales
```

`sql-tests/mv-on-iceberg/rewrite/aggregate.sql` — e2e correctness, full + partial fresh + rollup.

- [ ] **Step 9.6: Run tests**

```
cargo build
cargo test --lib sql::optimizer::mv_rewrite::column_rewriter
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only mv_rewrite_aggregate_full_fresh,mv_rewrite_aggregate_rollup,mv_rewrite_reject_groupby_finer \
  --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --only rewrite/aggregate --mode verify
```

- [ ] **Step 9.7: Commit**

```bash
git add src/sql/optimizer/mv_rewrite/rules/aggregate_scan.rs \
        src/sql/optimizer/mv_rewrite/rewriter.rs \
        src/sql/optimizer/mv_rewrite/rules/mod.rs \
        src/sql/optimizer/rules/mod.rs \
        src/sql/optimizer/mv_rewrite/column_rewriter.rs \
        sql-tests/optimizer/mv_rewrite_aggregate_full_fresh.sql \
        sql-tests/optimizer/mv_rewrite_aggregate_rollup.sql \
        sql-tests/optimizer/mv_rewrite_reject_groupby_finer.sql \
        sql-tests/mv-on-iceberg/rewrite/aggregate.sql
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): MvAggregateScanRewriteRule

SPJG single-base aggregate rewrite with GROUP BY rollup support.
Supports SUM/COUNT/MIN/MAX/AVG via per-function RollupAction
(AVG decomposed to SUM+COUNT).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: MvJoinRewriteRule

Goal: SPJF two-base inner equi-join rewrite.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/rules/join.rs`
- Modify: `src/sql/optimizer/mv_rewrite/rewriter.rs` (add `rewrite_join`)
- Modify: `src/sql/optimizer/mv_rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rules/mod.rs`
- Create: `sql-tests/optimizer/mv_rewrite_join_inner.sql`
- Create: `sql-tests/mv-on-iceberg/rewrite/join.sql`

- [ ] **Step 10.1: Implement rewrite_join**

Constraints (v1):
- Both query.join and mv.join must be `JoinKind::Inner`.
- Same set of join-eq conditions (canonicalized: each eq normalized so `t1.a = t2.b` and `t2.b = t1.a` are the same — sort by `MvColumnId`).
- Same set of base tables (set equality after equivalence-class normalization).
- Predicate compensation applies on top of MV scan, same as projection.

Reject early if any condition fails. Emit:
```
Project(query_projection,
  Filter(compensation,
    Scan(MV)))
```

- [ ] **Step 10.2: Define rule struct + register**

Symmetric to Task 7/9.

`matches()`:
```rust
fn matches(&self, op: &Operator) -> bool {
    matches!(op, Operator::LogicalJoin(_)) && self.ctx.enabled()
}
```

- [ ] **Step 10.3: SQL tests**

`sql-tests/optimizer/mv_rewrite_join_inner.sql`:
```sql
CREATE TABLE base_orders (order_id BIGINT, cust_id BIGINT, amount DECIMAL(18,2))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE base_customers (cust_id BIGINT, region STRING)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE MATERIALIZED VIEW mv_join_orders_customers AS
SELECT o.order_id, o.amount, c.region
FROM base_orders o JOIN base_customers c ON o.cust_id = c.cust_id;
REFRESH MATERIALIZED VIEW mv_join_orders_customers;

EXPLAIN VERBOSE
SELECT o.order_id, o.amount, c.region
FROM base_orders o JOIN base_customers c ON o.cust_id = c.cust_id
WHERE c.region = 'west';
-- @explain_contains=mv_join_orders_customers
```

`sql-tests/mv-on-iceberg/rewrite/join.sql` — e2e.

- [ ] **Step 10.4: Run + commit**

```
cargo build
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only mv_rewrite_join_inner --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --only rewrite/join --mode verify
```

```bash
git add src/sql/optimizer/mv_rewrite/rules/join.rs \
        src/sql/optimizer/mv_rewrite/rewriter.rs \
        src/sql/optimizer/mv_rewrite/rules/mod.rs \
        src/sql/optimizer/rules/mod.rs \
        sql-tests/optimizer/mv_rewrite_join_inner.sql \
        sql-tests/mv-on-iceberg/rewrite/join.sql
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): MvJoinRewriteRule

Two-base inner equi-join rewrite. Requires identical join-eq set
(normalized by MvColumnId order) and identical base table set.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: MvAggregateJoinRewriteRule

Goal: SPJG two-base rewrite (aggregate over inner equi-join). Combines Tasks 9 and 10.

**Files:**
- Create: `src/sql/optimizer/mv_rewrite/rules/aggregate_join.rs`
- Modify: `src/sql/optimizer/mv_rewrite/rewriter.rs` (add `rewrite_aggregate_join`)
- Modify: `src/sql/optimizer/mv_rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rules/mod.rs`
- Create: `sql-tests/optimizer/mv_rewrite_aggregate_join.sql`
- Create: `sql-tests/mv-on-iceberg/rewrite/aggregate_join.sql`

- [ ] **Step 11.1: Implement**

Combine logic from Task 9 (aggregate / rollup / arg-matching) and Task 10 (join constraints). `matches()` is `LogicalAggregate` over child `LogicalJoin` (or `Project(Join)`). Use `extract_aggregate_join` shape.

- [ ] **Step 11.2: SQL tests**

Same pattern as Task 9 but with a join MV.

- [ ] **Step 11.3: Run + commit**

```bash
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): MvAggregateJoinRewriteRule

Two-base aggregate-over-join rewrite. Combines join-shape constraints
(identical base set + join-eq set) with aggregate rollup.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: EXPLAIN VERBOSE / ANALYZE + trace channel

Goal: Surface MV rewrite decisions in `EXPLAIN`. Each `LogicalScan` that came from a rewrite gets annotated with `mv=<name> fresh=N stale=M`. A query-level header line summarizes the choice. A debug session var enables full candidate-attempt trace.

**Files:**
- Modify: `src/sql/explain.rs` (or wherever EXPLAIN is built — search for `EXPLAIN VERBOSE` rendering)
- Modify: `src/sql/optimizer/mv_rewrite/trace.rs` (record outcomes)
- Modify: `src/sql/optimizer/options.rs` (add `debug_print_mv_rewrite` session var)
- Modify: `src/sql/optimizer/mv_rewrite/mod.rs` + `rules/*.rs` (collect trace)

- [ ] **Step 12.1: Add trace collection**

Extend `MvRewriteCtx` with a thread-safe trace buffer:

```rust
use std::sync::Mutex;

struct MvRewriteCtxInner {
    // ... existing ...
    pub trace: Mutex<Vec<trace::MvRewriteOutcome>>,
}

impl MvRewriteCtx {
    pub(crate) fn record_outcome(&self, outcome: trace::MvRewriteOutcome) {
        self.inner.trace.lock().unwrap().push(outcome);
    }

    pub(crate) fn drain_trace(&self) -> Vec<trace::MvRewriteOutcome> {
        std::mem::take(&mut *self.inner.trace.lock().unwrap())
    }
}
```

Each rule's `apply()` records `Accepted` / `Rejected` / `Skipped` per candidate.

- [ ] **Step 12.2: Annotate rewritten scans**

When a rule emits a rewritten `LogicalScan(MV)`, mark it. Approach: add a side-table on `MvRewriteCtx` mapping `MExprId -> RewriteAnnotation`, populated when the rule emits. The EXPLAIN renderer reads this.

```rust
#[derive(Clone, Debug)]
pub(crate) struct RewriteAnnotation {
    pub mv_name: String,
    pub fresh_partitions: usize,
    pub stale_partitions: usize,
}

// In ctx:
pub annotations: Mutex<HashMap<MExprId, RewriteAnnotation>>,
```

- [ ] **Step 12.3: Render in EXPLAIN**

Find the existing EXPLAIN render path (search for `fn explain_node` or similar, likely in `src/sql/explain.rs`). When rendering a `LogicalScan` or `PhysicalScan`, look up the MExprId in `ctx.annotations` and append:
```
... | mv=mv_west_sales | fresh_partitions=12 | stale_partitions=3
```

For `EXPLAIN ANALYZE`, prepend a header line (similar to OPT-5's Planning/Execution/Rows header):
```
MV Rewrite: mv_west_sales (fresh: 12/15)
```

When the trace shows rejections and `debug_print_mv_rewrite=true`, append a footer:
```
MV candidates considered:
  mv_west_sales: ACCEPTED (fresh: 12/15)
  mv_east_sales: REJECTED (predicates not contained)
```

- [ ] **Step 12.4: Tests**

Add a plan-shape test asserting EXPLAIN output formatting:

`sql-tests/optimizer/mv_rewrite_explain_shape.sql`:
```sql
-- Setup MV + REFRESH (full).
SET debug_print_mv_rewrite = true;
EXPLAIN VERBOSE
SELECT order_id FROM base_sales WHERE region = 'west';
-- @explain_contains=mv=mv_west_sales
-- @explain_contains=MV candidates considered
```

- [ ] **Step 12.5: Run + commit**

```bash
cargo build
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only mv_rewrite_explain_shape --mode verify
```

```bash
git add src/sql/explain.rs src/sql/optimizer/mv_rewrite/trace.rs \
        src/sql/optimizer/mv_rewrite/mod.rs src/sql/optimizer/mv_rewrite/rules/ \
        src/sql/optimizer/options.rs sql-tests/optimizer/mv_rewrite_explain_shape.sql
git commit -m "$(cat <<'EOF'
feat(mv-rewrite): EXPLAIN VERBOSE / ANALYZE + trace channel

MV rewrite annotations (mv name, fresh/stale counts) attached to
rewritten scans, surfaced through EXPLAIN VERBOSE. EXPLAIN ANALYZE
gets a query-header line. SET debug_print_mv_rewrite=true emits a
per-candidate trace footer.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Full SQL suite regression + memory snapshot

Goal: Run all relevant SQL suites in `verify` mode and confirm parity with `main`. Update memory with progress.

- [ ] **Step 13.1: Run regression suites**

```
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build --release  # for suite throughput
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --mode verify
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-h --mode verify
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --mode verify
cargo test --lib
```
Expected: all suites pass.

- [ ] **Step 13.2: Update memory file**

Add a new memory file at `/Users/harbor/.claude/projects/-Users-harbor-project-NovaRocks/memory/project_mv_rewrite_v1.md`:

```markdown
---
name: project-mv-rewrite-v1
description: MV query rewrite v1 (IVM-scoped, Iceberg-backed) landed
metadata:
  type: project
---

**Spec:** `docs/superpowers/specs/2026-05-21-mv-rewrite-ivm-design.md`
**Plan:** `docs/superpowers/plans/2026-05-21-mv-rewrite-ivm.md`
**Branch:** `claude/stupefied-turing-c93a76`

**Scope landed:** 4 IVM shapes (Projection / Aggregate / Join / AggregateJoin), Iceberg-backed only, partition-level UNION ALL compensation, CBO transformation rule + cost.

**Out of scope (push to v2):** managed-lake MV, view-delta, nested MV, text-match, HLL/BITMAP rollup, staleness budget, MV hints.

**Why:** Closes the gap docs/iceberg-v3/materialized-views.md flagged as "❌ MV 自动 query rewrite", which was the most critical missing piece on top of mature IVM execution.

**How to apply:** Future work that needs MV rewrite should extend the existing modules under src/sql/optimizer/mv_rewrite/ rather than building parallel infra. MvColumnId retires once ARCH G1 lands — replace with global ColumnId mechanically.
```

Add a one-line index entry in `MEMORY.md`:

```
- [MV rewrite v1](project_mv_rewrite_v1.md) — IVM-scoped MV query rewrite landed on claude/stupefied-turing-c93a76
```

- [ ] **Step 13.3: Commit**

```bash
git add /Users/harbor/.claude/projects/-Users-harbor-project-NovaRocks/memory/project_mv_rewrite_v1.md
# memory file is outside the worktree; commit handled separately if at all.
# Inside the worktree:
git commit --allow-empty -m "$(cat <<'EOF'
test(mv-rewrite): full SQL suite regression sweep

All target suites pass: optimizer, iceberg, iceberg-ivm, iceberg-rest,
mv-on-iceberg, tpc-h, tpc-ds. cargo test --lib green.

Memory snapshot: project_mv_rewrite_v1.md.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Plan Self-Review Checklist (for the executor)

Run these checks at task boundaries:

1. **Build green**: `cargo build` after each commit.
2. **Existing tests still pass**: `cargo test --lib` after each commit.
3. **New tests pass**: each task's listed `cargo test --lib ...` and SQL test commands.
4. **EXPLAIN output stable**: any change touching plan rendering must update relevant goldens.
5. **No StarRocks drift**: when stuck on semantics, consult `~/project/starrocks/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/` and prefer their behaviour. Record any deliberate divergence as a comment in the code.

---

## Out-of-Scope Reminders (do NOT do as part of this plan)

- Do **not** refactor existing optimizer rules to support Pattern matching (ARCH G5). The MV rewrite uses bespoke shape extraction; G5 is a separate workstream.
- Do **not** introduce global ColumnId (ARCH G1). `MvColumnId` is the local stand-in.
- Do **not** add managed-lake MV rewrite support.
- Do **not** push this branch to remote or open a PR unless explicitly asked.
- Do **not** delete or modify pre-existing IVM tests; only add to them.
