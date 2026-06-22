# Query Stats Provider Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace name-based scan row-count fallback with query-scoped catalog statistics, and land missing-aware NDV semantics in the same architecture arc.

**Architecture:** Introduce an optimizer-native `StatsRef` and `QueryStatsSnapshot` boundary, collect catalog statistics before optimizer conversion, and make every optimizer entry consume the same snapshot. Keep catalog and connector calls in engine/connector layers; `src/sql/optimizer` only reads the immutable snapshot keyed by `StatsRef`.

**Tech Stack:** Rust, NovaRocks standalone SQL optimizer, Iceberg catalog metadata, sql-test runner, cargo test.

---

## File Structure

- Create `src/sql/optimizer/stats_input.rs`
  - Owns optimizer input statistics types: `StatsRef`, `QueryStatsSnapshot`, `BaseTableStatistics`, `BaseColumnStatistics`, `StatValue`, `StatsSource`, and `StatsMissingReason`.
  - Contains only optimizer-native data structures and compatibility helpers. It must not import `engine`, `connector`, or `catalog_mgr`.

- Modify `src/sql/optimizer/mod.rs`
  - Export `stats_input`.
  - Change `optimize` and `optimize_with_root_distribution` signatures from `&HashMap<String, TableStatistics>` to `&QueryStatsSnapshot`.
  - Thread the snapshot through rewrite context, rewrite registry, memo statistics derivation, and search.

- Modify `src/sql/optimizer/operator.rs`
  - Add `stats_ref: StatsRef` to `ScanOp`.
  - Keep scan identity fields (`database`, `table`, `alias`) for display and execution only; statistics lookup must use `stats_ref`.

- Modify `src/sql/planner/optimizer_bridge/plan.rs`
  - Add `PlanScanOrdinal`, `OptimizerBridgeStatsRefs`, and a checked bridge entry that assigns `ScanOp.stats_ref` while converting each scan leaf.
  - Keep a compatibility bridge only for unit tests and immediately mark each production caller for migration in the same task.

- Create `src/engine/query_stats.rs`
  - Owns `QueryStatsCollector`, scan-leaf enumeration, `PlanScanOrdinal` allocation, provider dispatch, and warning-to-missing behavior.
  - Produces `QueryStatsPlan { snapshot, scan_refs }` for optimizer bridge calls.

- Modify `src/engine/mod.rs`
  - Replace `build_table_stats_from_plan()` call sites in SELECT, EXPLAIN, EXPLAIN ANALYZE, INSERT SELECT / Iceberg write planning, and tests with `QueryStatsCollector`.
  - Remove `collect_scan_stats()` and move Puffin loading into provider code.

- Create `src/connector/stats.rs`
  - Owns `TableStatsProvider`, `TableStatsRequest`, `TableSnapshotRef`, `ScanSourceIdentity`, and `StatsProviderError`.
  - Keeps connector-facing request/response types independent of engine session state.

- Modify `src/connector/mod.rs`
  - Export `stats`.
  - Route provider lookup from connector registry or connector-specific backends.

- Modify `src/connector/iceberg/catalog/backend.rs` and related Iceberg catalog modules
  - Implement the first real provider from current-snapshot manifests and Puffin NDV.
  - Reuse existing helpers for data-file extraction and `StatsLoader`.

- Modify `src/engine/mv_rewrite_prep.rs`
  - Stop mutating `HashMap<String, TableStatistics>`.
  - Return MV candidates plus stats extensions keyed by `StatsRef`, or accept a mutable `QueryStatsPlanBuilder` from the collector.

- Modify `src/sql/optimizer/stats.rs`
  - Change scan derivation to `scan.stats_ref -> QueryStatsSnapshot`.
  - Delete `estimate_default_row_count`.
  - Keep a single non-name-based operator fallback for missing base row count.

- Modify `src/sql/optimizer/statistics.rs`, `src/sql/optimizer/estimate/ndv.rs`, `src/sql/optimizer/estimate/selectivity.rs`, `src/sql/optimizer/estimate/join_condition.rs`, and aggregate-pushdown cost files
  - Replace `distinct_values_count: f64` sentinel usage with missing-aware NDV accessors.
  - Keep old field only as a compile-phase adapter until the final ST-2 task removes it.

- Add SQL tests under `sql-tests/optimizer/sql/` and `sql-tests/optimizer/result/`
  - Cover non-TPC table names, misleading names containing `sales`, and ordinary SELECT vs EXPLAIN consistency.

## Implementation Tasks

### Task 1: Add Optimizer Stats Input Types

**Files:**
- Create: `src/sql/optimizer/stats_input.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Test: unit tests inside `src/sql/optimizer/stats_input.rs`

- [ ] **Step 1: Add the new module export**

Add this line near the other optimizer modules in `src/sql/optimizer/mod.rs`:

```rust
pub(crate) mod stats_input;
```

- [ ] **Step 2: Create `stats_input.rs` with the input boundary types**

Create `src/sql/optimizer/stats_input.rs` with this content:

```rust
//! Query-scoped statistics input for the optimizer.
//!
//! This module is the only boundary through which base-table statistics enter
//! the optimizer. It intentionally contains no connector, catalog, or engine
//! dependencies.

use std::collections::HashMap;

use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, TableStatistics};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StatsRef(u32);

impl StatsRef {
    pub(crate) fn new(value: u32) -> Self {
        Self(value)
    }

    pub(crate) fn as_u32(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatsSource {
    IcebergManifest,
    IcebergPuffin,
    ManagedLakeMetadata,
    StarRocksTableMetadata,
    ConnectorEstimate,
    Derived,
    Fallback,
    TestFixture,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatsMissingReason {
    NoCurrentSnapshot,
    NoDataFiles,
    ManifestMissingRowCount,
    StatsFileMissing,
    ConnectorUnsupported,
    CatalogLoadError(String),
    ColumnNotReported(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StatValue<T> {
    Known {
        value: T,
        confidence: Confidence,
        source: StatsSource,
    },
    Missing {
        reason: StatsMissingReason,
    },
}

impl<T> StatValue<T> {
    pub(crate) fn known(value: T, confidence: Confidence, source: StatsSource) -> Self {
        Self::Known {
            value,
            confidence,
            source,
        }
    }

    pub(crate) fn missing(reason: StatsMissingReason) -> Self {
        Self::Missing { reason }
    }

    pub(crate) fn known_value(&self) -> Option<&T> {
        match self {
            Self::Known { value, .. } => Some(value),
            Self::Missing { .. } => None,
        }
    }

    pub(crate) fn confidence(&self) -> Confidence {
        match self {
            Self::Known { confidence, .. } => *confidence,
            Self::Missing { .. } => Confidence::Fallback,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BaseColumnStatistics {
    pub nulls_fraction: StatValue<f64>,
    pub average_row_size: StatValue<f64>,
    pub min_value: StatValue<f64>,
    pub max_value: StatValue<f64>,
    pub ndv: StatValue<f64>,
}

impl BaseColumnStatistics {
    pub(crate) fn from_legacy(stat: ColumnStatistic, source: StatsSource) -> Self {
        let confidence = stat.confidence;
        Self {
            nulls_fraction: StatValue::known(stat.nulls_fraction, confidence, source),
            average_row_size: StatValue::known(stat.average_row_size, confidence, source),
            min_value: StatValue::known(stat.min_value, confidence, source),
            max_value: StatValue::known(stat.max_value, confidence, source),
            ndv: if stat.confidence > Confidence::Fallback
                && stat.distinct_values_count.is_finite()
                && stat.distinct_values_count > 1.0
            {
                StatValue::known(stat.distinct_values_count, confidence, source)
            } else {
                StatValue::missing(StatsMissingReason::ColumnNotReported("ndv".to_string()))
            },
        }
    }

    pub(crate) fn missing(column: &str) -> Self {
        let reason = StatsMissingReason::ColumnNotReported(column.to_string());
        Self {
            nulls_fraction: StatValue::missing(reason.clone()),
            average_row_size: StatValue::missing(reason.clone()),
            min_value: StatValue::missing(reason.clone()),
            max_value: StatValue::missing(reason.clone()),
            ndv: StatValue::missing(reason),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BaseTableStatistics {
    pub row_count: StatValue<u64>,
    pub columns: HashMap<String, BaseColumnStatistics>,
    pub source: StatsSource,
}

impl BaseTableStatistics {
    pub(crate) fn missing(reason: StatsMissingReason) -> Self {
        Self {
            row_count: StatValue::missing(reason),
            columns: HashMap::new(),
            source: StatsSource::Fallback,
        }
    }

    pub(crate) fn from_legacy(table_stats: TableStatistics, source: StatsSource) -> Self {
        let columns = table_stats
            .column_stats
            .into_iter()
            .map(|(name, stat)| (name, BaseColumnStatistics::from_legacy(stat, source)))
            .collect();
        Self {
            row_count: StatValue::known(table_stats.row_count, Confidence::Exact, source),
            columns,
            source,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QueryStatsSnapshot {
    table_stats: HashMap<StatsRef, BaseTableStatistics>,
}

impl QueryStatsSnapshot {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, stats_ref: StatsRef, stats: BaseTableStatistics) {
        self.table_stats.insert(stats_ref, stats);
    }

    pub(crate) fn get(&self, stats_ref: StatsRef) -> Option<&BaseTableStatistics> {
        self.table_stats.get(&stats_ref)
    }

    pub(crate) fn len(&self) -> usize {
        self.table_stats.len()
    }

    pub(crate) fn for_test(stats_ref: StatsRef, table_stats: TableStatistics) -> Self {
        let mut snapshot = Self::empty();
        snapshot.insert(
            stats_ref,
            BaseTableStatistics::from_legacy(table_stats, StatsSource::TestFixture),
        );
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_refs_are_distinct_even_when_table_stats_are_equal() {
        let left = StatsRef::new(0);
        let right = StatsRef::new(1);
        assert_ne!(left, right);
        assert_eq!(left.as_u32(), 0);
        assert_eq!(right.as_u32(), 1);
    }

    #[test]
    fn unknown_legacy_ndv_becomes_missing() {
        let col = BaseColumnStatistics::from_legacy(
            ColumnStatistic::unknown(),
            StatsSource::TestFixture,
        );
        assert!(matches!(col.ndv, StatValue::Missing { .. }));
    }
}
```

- [ ] **Step 3: Run the focused test**

Run:

```bash
cargo test --lib sql::optimizer::stats_input
```

Expected:

```text
test sql::optimizer::stats_input::tests::stats_refs_are_distinct_even_when_table_stats_are_equal ... ok
test sql::optimizer::stats_input::tests::unknown_legacy_ndv_becomes_missing ... ok
```

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/mod.rs src/sql/optimizer/stats_input.rs
git commit -m "feat: add query stats input types"
```

### Task 2: Put `StatsRef` on Optimizer Scans

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/planner/optimizer_bridge/plan.rs`
- Test: unit tests inside `src/sql/planner/optimizer_bridge/plan.rs`

- [ ] **Step 1: Add `StatsRef` to `ScanOp`**

In `src/sql/optimizer/operator.rs`, import the type near other optimizer imports:

```rust
use crate::sql::optimizer::stats_input::StatsRef;
```

Add this field to `ScanOp` before `database`:

```rust
    pub stats_ref: StatsRef,
```

- [ ] **Step 2: Add bridge scan ordinal types**

In `src/sql/planner/optimizer_bridge/plan.rs`, add these imports:

```rust
use std::collections::HashMap;

use crate::sql::optimizer::stats_input::StatsRef;
```

Add these types below the imports:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PlanScanOrdinal(u32);

impl PlanScanOrdinal {
    pub(crate) fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OptimizerBridgeStatsRefs {
    scan_refs: HashMap<PlanScanOrdinal, StatsRef>,
}

impl OptimizerBridgeStatsRefs {
    pub(crate) fn new(scan_refs: HashMap<PlanScanOrdinal, StatsRef>) -> Self {
        Self { scan_refs }
    }

    pub(crate) fn stats_ref_for_scan(&self, ordinal: PlanScanOrdinal) -> Option<StatsRef> {
        self.scan_refs.get(&ordinal).copied()
    }
}

#[derive(Default)]
struct BridgeState {
    next_scan_ordinal: u32,
}

impl BridgeState {
    fn next_scan_ordinal(&mut self) -> PlanScanOrdinal {
        let ordinal = PlanScanOrdinal::new(self.next_scan_ordinal);
        self.next_scan_ordinal += 1;
        ordinal
    }
}
```

- [ ] **Step 3: Add the checked bridge entry**

Replace the current `try_logical_plan_to_opt_expr` body with:

```rust
pub(crate) fn try_logical_plan_to_opt_expr(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> Result<OptExpr, String> {
    let scan_refs = collect_default_scan_refs(plan);
    try_logical_plan_to_opt_expr_with_stats_refs(plan, scalars, &scan_refs)
}
```

Add this production entry:

```rust
pub(crate) fn try_logical_plan_to_opt_expr_with_stats_refs(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
    stats_refs: &OptimizerBridgeStatsRefs,
) -> Result<OptExpr, String> {
    validate_logical_plan_stage(plan)?;
    let mut state = BridgeState::default();
    logical_plan_to_opt_expr_unchecked(plan, scalars, stats_refs, &mut state)
}
```

Add this temporary compatibility helper for tests and not-yet-migrated callers:

```rust
fn collect_default_scan_refs(plan: &LogicalPlanNode) -> OptimizerBridgeStatsRefs {
    fn walk(
        plan: &LogicalPlanNode,
        next_ordinal: &mut u32,
        scan_refs: &mut HashMap<PlanScanOrdinal, StatsRef>,
    ) {
        if matches!(plan.kind, PlanNodeKind::Scan(_)) {
            let ordinal = PlanScanOrdinal::new(*next_ordinal);
            scan_refs.insert(ordinal, StatsRef::new(*next_ordinal));
            *next_ordinal += 1;
        }
        for child in &plan.children {
            walk(child, next_ordinal, scan_refs);
        }
    }

    let mut next_ordinal = 0;
    let mut scan_refs = HashMap::new();
    walk(plan, &mut next_ordinal, &mut scan_refs);
    OptimizerBridgeStatsRefs::new(scan_refs)
}
```

- [ ] **Step 4: Thread the bridge state into recursion**

Change the helper signature from:

```rust
fn logical_plan_to_opt_expr_unchecked(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
) -> OptExpr
```

to:

```rust
fn logical_plan_to_opt_expr_unchecked(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
    stats_refs: &OptimizerBridgeStatsRefs,
    state: &mut BridgeState,
) -> Result<OptExpr, String>
```

In the scan branch, allocate the ordinal and set `stats_ref`:

```rust
let ordinal = state.next_scan_ordinal();
let stats_ref = stats_refs
    .stats_ref_for_scan(ordinal)
    .ok_or_else(|| format!("missing stats ref for scan ordinal {:?}", ordinal))?;
let op = Operator::LogicalScan(ScanOp {
    stats_ref,
    database: node.database.clone(),
    table: node.table.clone(),
    alias: node.alias.clone(),
    columns: node.columns.clone(),
    predicates: intern_exprs(scalars, &node.predicates),
    required_columns: node.required_columns.clone(),
    dict_columns: node.dict_columns.clone(),
    variant_columns: node.variant_columns.clone(),
    mv_rewritten_from: None,
});
Ok(OptExpr::leaf(op))
```

For every recursive child conversion, add `?` and pass `stats_refs, state`. Example:

```rust
let child = logical_plan_to_opt_expr_unchecked(
    plan.unary_input(),
    scalars,
    stats_refs,
    state,
)?;
```

- [ ] **Step 5: Add a missing-ref regression test**

Add this test in the existing `#[cfg(test)]` module in `src/sql/planner/optimizer_bridge/plan.rs`:

```rust
#[test]
fn bridge_requires_stats_ref_for_each_scan_ordinal() {
    let plan = scan_plan_for_test("db", "t");
    let mut scalars = ScalarArena::new();
    let refs = OptimizerBridgeStatsRefs::new(HashMap::new());
    let err = try_logical_plan_to_opt_expr_with_stats_refs(&plan, &mut scalars, &refs)
        .expect_err("bridge should reject missing scan stats ref");
    assert!(err.contains("missing stats ref for scan ordinal"));
}
```

If the existing test helpers use a different constructor name than `scan_plan_for_test`, use the local helper already used by the bridge tests and keep the assertion text unchanged.

- [ ] **Step 6: Run focused bridge tests**

Run:

```bash
cargo test --lib sql::planner::optimizer_bridge::plan
```

Expected: all optimizer bridge plan tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/planner/optimizer_bridge/plan.rs
git commit -m "feat: assign stats refs to optimizer scans"
```

### Task 3: Convert Optimizer API to `QueryStatsSnapshot`

**Files:**
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/rewrite/context.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/mod.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/context.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/cost.rs`
- Modify: `src/sql/optimizer/cascades_rules/multi_join_reorder/pass.rs`
- Test: existing optimizer unit tests

- [ ] **Step 1: Replace public optimizer API parameters**

In `src/sql/optimizer/mod.rs`, replace:

```rust
use crate::sql::optimizer::statistics::TableStatistics;
```

with:

```rust
use crate::sql::optimizer::stats_input::QueryStatsSnapshot;
```

Change `optimize`, `optimize_with_root_distribution`, and `optimize_with_root_property` parameters:

```rust
query_stats: &QueryStatsSnapshot,
```

Then replace internal uses:

```rust
rewrite_ctx.set_query_stats(query_stats.clone());
let rewritten_expr = rewrite::registry::query_rewrite_pipeline(query_stats)
    .rewrite(plan_expr, &mut rewrite_ctx)?;
```

- [ ] **Step 2: Move rewrite context from table-name map to snapshot**

In `src/sql/optimizer/rewrite/context.rs`, replace the table stats field:

```rust
query_table_stats: Option<Arc<HashMap<String, TableStatistics>>>,
```

with:

```rust
query_stats: Option<Arc<QueryStatsSnapshot>>,
```

Replace the methods with:

```rust
pub(crate) fn set_query_stats(&mut self, query_stats: QueryStatsSnapshot) {
    self.query_stats = Some(Arc::new(query_stats));
}

pub(crate) fn query_stats(&self) -> Option<&QueryStatsSnapshot> {
    self.query_stats.as_deref()
}
```

Update imports:

```rust
use crate::sql::optimizer::stats_input::QueryStatsSnapshot;
```

- [ ] **Step 3: Change rewrite registry signature**

In `src/sql/optimizer/rewrite/registry.rs`, change:

```rust
pub(crate) fn query_rewrite_pipeline(
    table_stats: &HashMap<String, TableStatistics>,
) -> RewritePipeline
```

to:

```rust
pub(crate) fn query_rewrite_pipeline(query_stats: &QueryStatsSnapshot) -> RewritePipeline
```

Update imports and change aggregate pushdown construction:

```rust
rules::aggregate_pushdown::aggregate_pushdown_rules(query_stats)
```

- [ ] **Step 4: Add a temporary aggregate-pushdown adapter**

In `src/sql/optimizer/rewrite/rules/aggregate_pushdown/mod.rs`, change the exported constructor to:

```rust
pub(crate) fn aggregate_pushdown_rules(
    query_stats: &crate::sql::optimizer::stats_input::QueryStatsSnapshot,
) -> Vec<Box<dyn crate::sql::optimizer::rewrite::tree::TreeRewriteRule>> {
    vec![Box::new(rule::AggregatePushdownRule::new(query_stats.clone()))]
}
```

In the rule/context files, store `QueryStatsSnapshot` instead of `HashMap<String, TableStatistics>`. Where the old code needs table-level stats by name, use a local adapter that returns `None` until Task 6 rewires by `StatsRef`:

```rust
fn legacy_table_stats_for_scan(
    scan: &crate::sql::optimizer::operator::ScanOp,
    query_stats: &QueryStatsSnapshot,
) -> Option<crate::sql::optimizer::statistics::TableStatistics> {
    let base = query_stats.get(scan.stats_ref)?;
    crate::sql::optimizer::statistics::TableStatistics::try_from_base_stats(base)
}
```

Add `try_from_base_stats` in Task 3 Step 5.

- [ ] **Step 5: Add legacy conversion only as a migration bridge**

In `src/sql/optimizer/statistics.rs`, add this associated function:

```rust
impl TableStatistics {
    pub(crate) fn try_from_base_stats(
        base: &crate::sql::optimizer::stats_input::BaseTableStatistics,
    ) -> Option<Self> {
        use crate::sql::optimizer::stats_input::StatValue;

        let row_count = match &base.row_count {
            StatValue::Known { value, .. } => *value,
            StatValue::Missing { .. } => return None,
        };

        let mut column_stats = HashMap::new();
        for (name, col) in &base.columns {
            let mut stat = ColumnStatistic::unknown();
            if let StatValue::Known { value, confidence, .. } = &col.min_value {
                stat.min_value = *value;
                stat.confidence = stat.confidence.max(*confidence);
            }
            if let StatValue::Known { value, confidence, .. } = &col.max_value {
                stat.max_value = *value;
                stat.confidence = stat.confidence.max(*confidence);
            }
            if let StatValue::Known { value, confidence, .. } = &col.nulls_fraction {
                stat.nulls_fraction = *value;
                stat.confidence = stat.confidence.max(*confidence);
            }
            if let StatValue::Known { value, confidence, .. } = &col.average_row_size {
                stat.average_row_size = *value;
                stat.confidence = stat.confidence.max(*confidence);
            }
            if let StatValue::Known { value, confidence, .. } = &col.ndv {
                stat.distinct_values_count = *value;
                stat.confidence = stat.confidence.max(*confidence);
            }
            column_stats.insert(name.clone(), stat);
        }

        Some(Self {
            row_count,
            column_stats,
        })
    }
}
```

- [ ] **Step 6: Update all compile errors from old signatures**

Run:

```bash
cargo check --lib
```

Expected first failure pattern:

```text
expected `&QueryStatsSnapshot`, found `&HashMap<String, TableStatistics>`
```

For each failing optimizer test or production caller, use `QueryStatsSnapshot::empty()` for no-stats tests and `QueryStatsSnapshot::for_test(StatsRef::new(0), table_stats)` only when the plan has one scan with ordinal `0`.

- [ ] **Step 7: Run optimizer tests**

Run:

```bash
cargo test --lib sql::optimizer
```

Expected: all optimizer unit tests pass. Golden-plan changes are not expected in this task because scan derivation still uses the compatibility path.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer src/sql/planner
git commit -m "refactor: pass query stats snapshot into optimizer"
```

### Task 4: Add Connector Stats Provider Boundary

**Files:**
- Create: `src/connector/stats.rs`
- Modify: `src/connector/mod.rs`
- Modify: `src/connector/backend.rs`
- Test: unit tests in `src/connector/stats.rs`

- [ ] **Step 1: Export the stats module**

Add to `src/connector/mod.rs`:

```rust
pub(crate) mod stats;
```

- [ ] **Step 2: Create connector stats request/trait types**

Create `src/connector/stats.rs`:

```rust
//! Connector-side statistics provider boundary.
//!
//! Providers return planning statistics only. They do not build executable
//! scan splits and do not mutate analyzer schema metadata.

use crate::sql::optimizer::stats_input::{BaseTableStatistics, StatsMissingReason};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ScanSourceIdentity {
    IcebergTable {
        catalog: String,
        namespace: String,
        table: String,
    },
    ManagedLakeTable {
        catalog: String,
        database: String,
        table: String,
    },
    StarRocksTable {
        database: String,
        table: String,
    },
    Unsupported {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TableSnapshotRef {
    Current,
    SnapshotId(i64),
    Branch(String),
    Tag(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TableStatsRequest {
    pub catalog: Option<String>,
    pub database: String,
    pub table: String,
    pub source: ScanSourceIdentity,
    pub snapshot: Option<TableSnapshotRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatsProviderError {
    Unsupported(String),
    Catalog(String),
    Metadata(String),
}

impl StatsProviderError {
    pub(crate) fn into_missing_reason(self) -> StatsMissingReason {
        match self {
            Self::Unsupported(reason) => StatsMissingReason::ConnectorUnsupported,
            Self::Catalog(err) | Self::Metadata(err) => StatsMissingReason::CatalogLoadError(err),
        }
    }
}

pub(crate) trait TableStatsProvider: Send + Sync {
    fn estimate_table_statistics(
        &self,
        request: &TableStatsRequest,
    ) -> Result<BaseTableStatistics, StatsProviderError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_errors_convert_to_missing_reasons() {
        assert!(matches!(
            StatsProviderError::Catalog("boom".to_string()).into_missing_reason(),
            StatsMissingReason::CatalogLoadError(err) if err == "boom"
        ));
        assert!(matches!(
            StatsProviderError::Unsupported("jdbc".to_string()).into_missing_reason(),
            StatsMissingReason::ConnectorUnsupported
        ));
    }
}
```

- [ ] **Step 3: Add an optional stats-provider hook to table sources**

In `src/connector/backend.rs`, import:

```rust
use std::sync::Arc;

use crate::connector::stats::TableStatsProvider;
```

Add this default method to `TableSource`:

```rust
fn stats_provider(&self) -> Option<Arc<dyn TableStatsProvider>> {
    None
}
```

- [ ] **Step 4: Run connector stats tests**

Run:

```bash
cargo test --lib connector::stats
```

Expected: the provider error conversion test passes.

- [ ] **Step 5: Commit**

```bash
git add src/connector/mod.rs src/connector/stats.rs src/connector/backend.rs
git commit -m "feat: add connector stats provider boundary"
```

### Task 5: Implement Iceberg Provider From Manifests and Puffin NDV

**Files:**
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/connector/iceberg/mod.rs` or a new focused helper under `src/connector/iceberg/stats.rs`
- Modify: `src/sql/optimizer/statistics.rs`
- Test: unit tests near Iceberg stats helper

- [ ] **Step 1: Add base-stats builder beside the legacy builder**

In `src/sql/optimizer/statistics.rs`, add this function after `build_table_statistics_with_ndv`:

```rust
pub(crate) fn build_base_table_statistics_with_ndv(
    files: &[crate::sql::catalog::IcebergDataFileInfo],
    columns: &[crate::sql::catalog::ColumnDef],
    ndv_by_name: &HashMap<String, f64>,
    name_to_field_id: &HashMap<String, i32>,
) -> crate::sql::optimizer::stats_input::BaseTableStatistics {
    use crate::sql::optimizer::stats_input::{
        BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };

    if files.is_empty() {
        return BaseTableStatistics {
            row_count: StatValue::known(0, Confidence::Exact, StatsSource::IcebergManifest),
            columns: HashMap::new(),
            source: StatsSource::IcebergManifest,
        };
    }

    let Some(legacy) = build_table_statistics_with_ndv(
        files,
        columns,
        ndv_by_name,
        name_to_field_id,
    ) else {
        return BaseTableStatistics::missing(StatsMissingReason::ManifestMissingRowCount);
    };

    BaseTableStatistics::from_legacy(legacy, StatsSource::IcebergManifest)
}
```

This keeps old tests stable while giving the provider a missing-aware output type.

- [ ] **Step 2: Create the Iceberg stats helper**

If `src/connector/iceberg/stats.rs` does not exist, create it and add `pub(crate) mod stats;` in `src/connector/iceberg/mod.rs`.

Use this helper shape:

```rust
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use crate::connector::iceberg::catalog::{
    IcebergCatalogRegistry, load_table as reg_load_table,
};
use crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info;
use crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at;
use crate::connector::iceberg::stats_loader::StatsLoader;
use crate::connector::stats::{
    ScanSourceIdentity, StatsProviderError, TableStatsProvider, TableStatsRequest,
};
use crate::sql::optimizer::stats_input::{BaseTableStatistics, StatsMissingReason};

pub(crate) struct IcebergTableStatsProvider {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergTableStatsProvider {
    pub(crate) fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableStatsProvider for IcebergTableStatsProvider {
    fn estimate_table_statistics(
        &self,
        request: &TableStatsRequest,
    ) -> Result<BaseTableStatistics, StatsProviderError> {
        let ScanSourceIdentity::IcebergTable {
            catalog,
            namespace,
            table,
        } = &request.source
        else {
            return Err(StatsProviderError::Unsupported(format!(
                "iceberg stats provider cannot handle {:?}",
                request.source
            )));
        };

        load_iceberg_current_snapshot_stats(&self.registry, catalog, namespace, table)
    }
}

fn load_iceberg_current_snapshot_stats(
    registry: &Arc<RwLock<IcebergCatalogRegistry>>,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<BaseTableStatistics, StatsProviderError> {
    let guard = registry.read().expect("iceberg catalog read lock");
    let entry = guard
        .get(catalog)
        .map_err(StatsProviderError::Catalog)?;
    let loaded = reg_load_table(&entry, namespace, table).map_err(StatsProviderError::Catalog)?;
    let Some(snapshot_id) = loaded.table.metadata().current_snapshot_id() else {
        return Ok(BaseTableStatistics::missing(
            StatsMissingReason::NoCurrentSnapshot,
        ));
    };
    let columns = loaded.columns.clone();
    let iceberg_table = loaded.table.clone();
    let cloud_properties = entry.cloud_properties_map();
    drop(guard);

    let files = extract_data_files_with_stats_at(&iceberg_table, snapshot_id)
        .map_err(StatsProviderError::Metadata)?
        .into_iter()
        .map(data_file_with_stats_to_iceberg_data_file_info)
        .collect::<Vec<_>>();
    let (ndv_by_name, name_to_field_id) =
        load_iceberg_puffin_ndv(&iceberg_table, &cloud_properties);

    Ok(crate::sql::optimizer::statistics::build_base_table_statistics_with_ndv(
        &files,
        &columns,
        &ndv_by_name,
        &name_to_field_id,
    ))
}
```

If `data_file_with_stats_to_iceberg_data_file_info` is private in the target checkout, make that existing helper `pub(crate)` instead of duplicating conversion logic. Keep the conversion helper in the Iceberg connector module, not in `engine`.

The return path must be:

```rust
Ok(crate::sql::optimizer::statistics::build_base_table_statistics_with_ndv(
    &files,
    &columns,
    &ndv_by_name,
    &name_to_field_id,
))
```

Use `StatsLoader::load_ndv` through the same `load_iceberg_puffin_ndv` logic currently in `src/engine/mod.rs`, moved into this helper so engine no longer owns Puffin parsing.

- [ ] **Step 3: Wire Iceberg table source to the provider**

In `src/connector/iceberg/catalog/backend.rs`, import:

```rust
use crate::connector::iceberg::stats::IcebergTableStatsProvider;
```

Add this method to the existing `impl TableSource for IcebergTableSource`:

```rust
fn stats_provider(&self) -> Option<Arc<dyn crate::connector::stats::TableStatsProvider>> {
    Some(Arc::new(IcebergTableStatsProvider::new(Arc::clone(
        &self.registry,
    ))))
}
```

- [ ] **Step 4: Handle empty current snapshot and missing snapshot distinctly**

When catalog metadata has no current snapshot, return:

```rust
Ok(BaseTableStatistics::missing(StatsMissingReason::NoCurrentSnapshot))
```

When a current snapshot exists but has zero data files, return known zero from `build_base_table_statistics_with_ndv(&[], ...)`.

- [ ] **Step 5: Add provider tests for zero and missing**

Add unit tests at the bottom of the Iceberg stats helper:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::stats_input::{StatValue, StatsMissingReason};

    #[test]
    fn empty_current_snapshot_is_known_zero() {
        let stats = crate::sql::optimizer::statistics::build_base_table_statistics_with_ndv(
            &[],
            &[],
            &HashMap::new(),
            &HashMap::new(),
        );
        assert!(matches!(stats.row_count, StatValue::Known { value: 0, .. }));
    }

    #[test]
    fn missing_snapshot_reason_is_explicit() {
        let stats = BaseTableStatistics::missing(StatsMissingReason::NoCurrentSnapshot);
        assert!(matches!(
            stats.row_count,
            StatValue::Missing {
                reason: StatsMissingReason::NoCurrentSnapshot
            }
        ));
    }
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib connector::iceberg::stats sql::optimizer::statistics::tests
```

Expected: Iceberg helper tests and existing statistics tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/connector/iceberg src/sql/optimizer/statistics.rs
git commit -m "feat: derive iceberg table stats for optimizer snapshots"
```

### Task 6: Add `QueryStatsCollector`

**Files:**
- Create: `src/engine/query_stats.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/sql/planner/optimizer_bridge/plan.rs`
- Test: unit tests in `src/engine/query_stats.rs`

- [ ] **Step 1: Export the engine collector module**

Add this module declaration in `src/engine/mod.rs` near other engine submodules:

```rust
mod query_stats;
```

- [ ] **Step 2: Create collector output and builder types**

Create `src/engine/query_stats.rs`:

```rust
//! Query-scoped statistics collection before optimizer conversion.

use std::collections::HashMap;

use crate::connector::stats::{ScanSourceIdentity, TableStatsRequest};
use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::stats_input::{
    BaseTableStatistics, QueryStatsSnapshot, StatsMissingReason, StatsRef,
};
use crate::sql::planner::optimizer_bridge::plan::{OptimizerBridgeStatsRefs, PlanScanOrdinal};
use crate::sql::planner::plan::{LogicalPlanNode, PlanNodeKind};

#[derive(Clone, Debug)]
pub(crate) struct QueryStatsPlan {
    pub snapshot: QueryStatsSnapshot,
    pub scan_refs: OptimizerBridgeStatsRefs,
}

#[derive(Default)]
pub(crate) struct QueryStatsCollector {
    next_stats_ref: u32,
    next_scan_ordinal: u32,
    snapshot: QueryStatsSnapshot,
    scan_refs: HashMap<PlanScanOrdinal, StatsRef>,
}

impl QueryStatsCollector {
    pub(crate) fn collect(mut self, plan: &LogicalPlanNode) -> QueryStatsPlan {
        self.walk(plan);
        QueryStatsPlan {
            snapshot: self.snapshot,
            scan_refs: OptimizerBridgeStatsRefs::new(self.scan_refs),
        }
    }

    fn walk(&mut self, plan: &LogicalPlanNode) {
        if let PlanNodeKind::Scan(scan) = &plan.kind {
            let scan_ordinal = PlanScanOrdinal::new(self.next_scan_ordinal);
            self.next_scan_ordinal += 1;
            let stats_ref = StatsRef::new(self.next_stats_ref);
            self.next_stats_ref += 1;
            self.scan_refs.insert(scan_ordinal, stats_ref);
            let stats = self.collect_scan_stats(scan);
            self.snapshot.insert(stats_ref, stats);
        }
        for child in &plan.children {
            self.walk(child);
        }
    }

    fn collect_scan_stats(
        &self,
        scan: &crate::sql::planner::plan::LogicalScanNode,
    ) -> BaseTableStatistics {
        let Some(_request) = table_stats_request(scan) else {
            return BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported);
        };
        BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported)
    }
}

fn table_stats_request(
    scan: &crate::sql::planner::plan::LogicalScanNode,
) -> Option<TableStatsRequest> {
    match &scan.table.source {
        ScanSource::IcebergDataFiles { table, .. } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            snapshot: None,
        }),
        _ => None,
    }
}
```

This first version allocates stable refs and makes unsupported stats explicit. Provider dispatch is added in Step 4.

- [ ] **Step 3: Add collector allocation tests**

Add tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_plan_has_empty_snapshot() {
        let plan = LogicalPlanNode::values_for_test();
        let stats_plan = QueryStatsCollector::default().collect(&plan);
        assert_eq!(stats_plan.snapshot.len(), 0);
    }
}
```

If the repo has no `values_for_test`, use the existing logical-plan test helper already used in `src/sql/planner/optimizer_bridge/plan.rs`.

- [ ] **Step 4: Add provider dispatch**

Extend `QueryStatsCollector` to hold provider access:

```rust
use std::sync::Arc;

pub(crate) struct QueryStatsCollector {
    providers: QueryStatsProviders,
    next_stats_ref: u32,
    next_scan_ordinal: u32,
    snapshot: QueryStatsSnapshot,
    scan_refs: HashMap<PlanScanOrdinal, StatsRef>,
}

#[derive(Clone, Default)]
pub(crate) struct QueryStatsProviders {
    pub iceberg: Option<Arc<dyn crate::connector::stats::TableStatsProvider>>,
}
```

Change the constructor:

```rust
impl QueryStatsCollector {
    pub(crate) fn new(providers: QueryStatsProviders) -> Self {
        Self {
            providers,
            next_stats_ref: 0,
            next_scan_ordinal: 0,
            snapshot: QueryStatsSnapshot::empty(),
            scan_refs: HashMap::new(),
        }
    }
}
```

Add provider constructors:

```rust
impl QueryStatsProviders {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn from_connectors(connectors: &crate::connector::ConnectorRegistry) -> Self {
        let iceberg = connectors
            .table_source("iceberg")
            .ok()
            .and_then(|source| source.stats_provider());
        Self { iceberg }
    }

    pub(crate) fn from_standalone_state(state: &std::sync::Arc<super::StandaloneState>) -> Self {
        let connectors = state
            .connectors
            .read()
            .expect("standalone connectors read lock");
        Self::from_connectors(&connectors)
    }

    pub(crate) fn from_optional_state(
        state: Option<&std::sync::Arc<super::StandaloneState>>,
    ) -> Self {
        match state {
            Some(state) => Self::from_standalone_state(state),
            None => Self::none(),
        }
    }
}
```

In `collect_scan_stats`, dispatch Iceberg:

```rust
let Some(request) = table_stats_request(scan) else {
    return BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported);
};
match &request.source {
    ScanSourceIdentity::IcebergTable { .. } => {
        let Some(provider) = self.providers.iceberg.as_deref() else {
            return BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported);
        };
        provider.estimate_table_statistics(&request).unwrap_or_else(|err| {
            tracing::debug!("stats provider failed for {}.{}: {:?}", request.database, request.table, err);
            BaseTableStatistics::missing(err.into_missing_reason())
        })
    }
    _ => BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported),
}
```

- [ ] **Step 5: Run collector tests**

Run:

```bash
cargo test --lib engine::query_stats
```

Expected: collector tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mod.rs src/engine/query_stats.rs src/sql/planner/optimizer_bridge/plan.rs
git commit -m "feat: collect query stats before optimization"
```

### Task 7: Migrate SELECT and EXPLAIN Entrypoints to the Collector

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `src/sql/planner/optimizer_bridge/plan.rs`
- Test: targeted engine/optimizer tests and SQL optimizer suite

- [ ] **Step 1: Replace EXPLAIN stats build path**

In `explain_query`, replace:

```rust
let mut table_stats = build_table_stats_from_plan(&logical);
```

with:

```rust
let mut query_stats = crate::engine::query_stats::QueryStatsCollector::new(
    crate::engine::query_stats::QueryStatsProviders::from_optional_state(mv_rewrite_state),
)
.collect(&logical);
```

Then replace the bridge call:

```rust
let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr_with_stats_refs(
    &logical,
    &mut scalar_arena,
    &query_stats.scan_refs,
)?;
```

And the optimizer call:

```rust
let physical = crate::sql::optimizer::optimize(
    opt_expr,
    scalar_arena,
    &query_stats.snapshot,
    factory,
    None,
    mv_candidates,
)?;
```

- [ ] **Step 2: Replace EXPLAIN ANALYZE stats build path**

Apply the same replacement in `explain_analyze_query`. Keep the planning timer unchanged, so stats collection remains part of planning time.

- [ ] **Step 3: Replace ordinary SELECT stats build path**

In `execute_query_with_catalog_provider`, replace:

```rust
let table_stats = build_table_stats_from_plan(&logical);
```

with the same collector call, using the real `state` when available:

```rust
let query_stats = crate::engine::query_stats::QueryStatsCollector::new(
    crate::engine::query_stats::QueryStatsProviders::from_standalone_state(state),
)
.collect(&logical);
```

Then call the checked bridge and `optimize(..., &query_stats.snapshot, ...)`.

- [ ] **Step 4: Keep EXPLAIN COSTS stats display source-aware**

Replace the old loop over table-name stats:

```rust
for (table, stats) in &table_stats {
    lines.push(format!("TABLE STATS {} rows={}", table, stats.row_count));
}
```

with:

```rust
for entry in query_stats.snapshot.display_rows() {
    lines.push(entry);
}
```

Add this method to `QueryStatsSnapshot`:

```rust
pub(crate) fn display_rows(&self) -> Vec<String> {
    let mut rows = Vec::new();
    for (stats_ref, stats) in &self.table_stats {
        match &stats.row_count {
            StatValue::Known {
                value,
                confidence,
                source,
            } => rows.push(format!(
                "TABLE STATS ref={} rows={} confidence={:?} source={:?}",
                stats_ref.as_u32(),
                value,
                confidence,
                source
            )),
            StatValue::Missing { reason } => rows.push(format!(
                "TABLE STATS ref={} rows=missing reason={:?}",
                stats_ref.as_u32(),
                reason
            )),
        }
    }
    rows.sort();
    rows
}
```

If `table_stats` map display is asserted in existing golden files, update the result files in the same task because source/confidence is now the user-visible contract.

- [ ] **Step 5: Run compile check**

Run:

```bash
cargo check --lib
```

Expected: no remaining compile errors in migrated SELECT and EXPLAIN paths. Remaining compile errors should point only to INSERT/MV/test paths not migrated yet.

- [ ] **Step 6: Run SQL optimizer verify**

If a standalone server is already running, run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --mode verify
```

If no server is running, start one from the generated environment first:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

Expected: optimizer suite passes or only `EXPLAIN COSTS` result files change to include the new stats source lines.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mod.rs src/engine/query_stats.rs src/sql/optimizer/stats_input.rs sql-tests/optimizer
git commit -m "refactor: use query stats collector for select and explain"
```

### Task 8: Migrate INSERT SELECT and MV Rewrite Stats

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/mv_rewrite_prep.rs`
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/**`
- Test: MV rewrite unit tests and targeted Iceberg MV SQL tests

- [ ] **Step 1: Change MV prep signature**

Replace:

```rust
table_stats: &mut HashMap<String, TableStatistics>,
```

with:

```rust
query_stats: &mut crate::engine::query_stats::QueryStatsPlan,
```

in `prepare_mv_rewrite_candidates` and `try_prepare`.

- [ ] **Step 2: Add stats extension API to `QueryStatsPlan`**

In `src/engine/query_stats.rs`, add:

```rust
impl QueryStatsPlan {
    pub(crate) fn add_mv_target_stats(
        &mut self,
        stats: BaseTableStatistics,
    ) -> StatsRef {
        let stats_ref = StatsRef::new(self.snapshot.len() as u32);
        self.snapshot.insert(stats_ref, stats);
        stats_ref
    }
}
```

If `len()` is no longer a dense allocator after earlier tasks, store `next_stats_ref` in `QueryStatsPlan` and increment it there instead.

- [ ] **Step 3: Put stats refs into MV rewrite candidates**

Add a field to the MV candidate descriptor type:

```rust
pub(crate) target_stats_ref: Option<crate::sql::optimizer::stats_input::StatsRef>,
```

When `load_target_stats` succeeds in `mv_rewrite_prep.rs`, call:

```rust
let target_stats_ref = Some(query_stats.add_mv_target_stats(
    crate::sql::optimizer::statistics::build_base_table_statistics_with_ndv(
        &files,
        &table_def.columns,
        &ndv_by_name,
        &name_to_field_id,
    ),
));
```

Then set `target_stats_ref` on the candidate.

- [ ] **Step 4: Use target stats ref when MV rewrite injects scans**

In `src/sql/optimizer/cascades_rules/mv_rewrite/rule.rs`, when building the replacement MV scan, set:

```rust
stats_ref: candidate.target_stats_ref.unwrap_or_else(|| original_scan.stats_ref),
```

If the rule constructs an MV scan without an original scan in scope, require `target_stats_ref` and return the existing no-rewrite result when it is absent.

- [ ] **Step 5: Migrate INSERT SELECT / Iceberg write optimization**

In `execute_query_as_iceberg_write` and other INSERT SELECT planning helpers around the current `build_table_stats_from_plan(&logical)` calls, use the collector and checked bridge exactly as in Task 7. Keep root distribution resolution unchanged:

```rust
let query_stats = crate::engine::query_stats::QueryStatsCollector::new(
    crate::engine::query_stats::QueryStatsProviders::from_standalone_state(state),
)
.collect(&logical);
let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr_with_stats_refs(
    &logical,
    &mut scalar_arena,
    &query_stats.scan_refs,
)?;
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib mv_rewrite query_stats
```

Then run the focused SQL suite if the Iceberg REST environment is available:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify --query-timeout 120
```

Expected: MV rewrite unit tests pass; Iceberg IVM suite passes or only explain stats lines change.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mod.rs src/engine/query_stats.rs src/engine/mv_rewrite_prep.rs src/sql/optimizer/cascades_rules/mv_rewrite
git commit -m "refactor: key mv rewrite stats by stats ref"
```

### Task 9: Use `StatsRef` in Scan Statistics Derivation

**Files:**
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/statistics.rs`
- Test: unit tests in `src/sql/optimizer/stats.rs`

- [ ] **Step 1: Change derivation signatures**

Replace `&HashMap<String, TableStatistics>` parameters in `src/sql/optimizer/stats.rs` with:

```rust
&crate::sql::optimizer::stats_input::QueryStatsSnapshot
```

Update imports:

```rust
use crate::sql::optimizer::stats_input::{BaseTableStatistics, QueryStatsSnapshot, StatValue, StatsMissingReason};
```

- [ ] **Step 2: Replace table-name lookup**

In the logical and physical scan branches, replace:

```rust
derive_scan_statistics_scalar(
    &scan.columns,
    &scan.predicates,
    scalars,
    table_stats.get(&scan.table.name).or_else(|| {
        scan.alias
            .as_ref()
            .and_then(|alias| table_stats.get(alias))
    }),
    estimate_default_row_count(&scan.table.name),
)
```

with:

```rust
derive_scan_statistics_scalar(
    &scan.columns,
    &scan.predicates,
    scalars,
    query_stats.get(scan.stats_ref),
)
```

- [ ] **Step 3: Rewrite `derive_scan_statistics_scalar`**

Change the function signature to:

```rust
fn derive_scan_statistics_scalar(
    columns: &[OutputColumn],
    predicates: &[ScalarId],
    scalars: &ScalarArena,
    table_stats: Option<&BaseTableStatistics>,
) -> Statistics
```

Use this missing-row fallback constant:

```rust
const MISSING_BASE_ROW_COUNT_FALLBACK: f64 = 100_000.0;
```

At the start of the function:

```rust
let (base_rows, row_count_confidence) = match table_stats.and_then(|stats| stats.row_count.known_value()) {
    Some(row_count) => (*row_count as f64, table_stats.unwrap().row_count.confidence()),
    None => (MISSING_BASE_ROW_COUNT_FALLBACK, Confidence::Fallback),
};
```

Map column stats by name:

```rust
let mut column_statistics = match table_stats {
    Some(stats) => map_base_column_stats_to_ids(columns, stats),
    None => columns
        .iter()
        .map(|c| (c.column_id, ColumnStatistic::unknown_missing_ndv()))
        .collect(),
};
```

- [ ] **Step 4: Add base-column mapper**

Add:

```rust
fn map_base_column_stats_to_ids(
    columns: &[OutputColumn],
    table_stats: &BaseTableStatistics,
) -> HashMap<ColumnId, ColumnStatistic> {
    columns
        .iter()
        .map(|column| {
            let stat = table_stats
                .columns
                .get(&column.name.to_lowercase())
                .map(ColumnStatistic::from_base_column)
                .unwrap_or_else(ColumnStatistic::unknown_missing_ndv);
            (column.column_id, stat)
        })
        .collect()
}
```

Add the two `ColumnStatistic` helpers in `src/sql/optimizer/statistics.rs`:

```rust
impl ColumnStatistic {
    pub(crate) fn unknown_missing_ndv() -> Self {
        Self::unknown()
    }

    pub(crate) fn from_base_column(
        base: &crate::sql::optimizer::stats_input::BaseColumnStatistics,
    ) -> Self {
        use crate::sql::optimizer::stats_input::StatValue;

        let mut stat = Self::unknown_missing_ndv();
        if let StatValue::Known { value, confidence, .. } = &base.min_value {
            stat.min_value = *value;
            stat.confidence = stat.confidence.max(*confidence);
        }
        if let StatValue::Known { value, confidence, .. } = &base.max_value {
            stat.max_value = *value;
            stat.confidence = stat.confidence.max(*confidence);
        }
        if let StatValue::Known { value, confidence, .. } = &base.nulls_fraction {
            stat.nulls_fraction = *value;
            stat.confidence = stat.confidence.max(*confidence);
        }
        if let StatValue::Known { value, confidence, .. } = &base.average_row_size {
            stat.average_row_size = *value;
            stat.confidence = stat.confidence.max(*confidence);
        }
        if let StatValue::Known { value, confidence, .. } = &base.ndv {
            stat.distinct_values_count = *value;
            stat.confidence = stat.confidence.max(*confidence);
        }
        stat
    }
}
```

This is still a bridge to the old NDV field; Task 10 removes the sentinel.

- [ ] **Step 5: Add non-TPC fallback regression unit test**

In `src/sql/optimizer/stats.rs`, add a test that creates a scan named `contains_sales_but_missing_stats` with an empty `QueryStatsSnapshot` and asserts:

```rust
assert_eq!(stats.output_row_count, 100_000.0);
assert_eq!(stats.row_count_confidence, Confidence::Fallback);
```

Then create the same scan with a snapshot row count of `2` and assert:

```rust
assert_eq!(stats.output_row_count, 2.0);
assert_eq!(stats.row_count_confidence, Confidence::Exact);
```

The table name must contain `sales` so the test proves the old substring heuristic is not consulted.

- [ ] **Step 6: Run focused stats tests**

Run:

```bash
cargo test --lib sql::optimizer::stats
```

Expected: new regression test passes; existing statistics tests pass after any expected assertion updates from source-aware display.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer/stats.rs src/sql/optimizer/statistics.rs
git commit -m "refactor: derive scan stats from stats refs"
```

### Task 10: Land Missing-Aware NDV Semantics

**Files:**
- Modify: `src/sql/optimizer/statistics.rs`
- Modify: `src/sql/optimizer/estimate/ndv.rs`
- Modify: `src/sql/optimizer/estimate/selectivity.rs`
- Modify: `src/sql/optimizer/estimate/join_condition.rs`
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/cost.rs`
- Modify: tests under `src/sql/optimizer/**`

- [ ] **Step 1: Add `DistinctValueCount`**

In `src/sql/optimizer/statistics.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DistinctValueCount {
    Known {
        value: f64,
        confidence: Confidence,
        source: crate::sql::optimizer::stats_input::StatsSource,
    },
    Unknown {
        reason: crate::sql::optimizer::stats_input::StatsMissingReason,
    },
}

impl DistinctValueCount {
    pub(crate) fn known(
        value: f64,
        confidence: Confidence,
        source: crate::sql::optimizer::stats_input::StatsSource,
    ) -> Self {
        Self::Known {
            value,
            confidence,
            source,
        }
    }

    pub(crate) fn unknown(
        reason: crate::sql::optimizer::stats_input::StatsMissingReason,
    ) -> Self {
        Self::Unknown { reason }
    }

    pub(crate) fn trusted_value(&self) -> Option<(f64, Confidence)> {
        match self {
            Self::Known {
                value,
                confidence,
                ..
            } if *confidence > Confidence::Fallback && value.is_finite() && *value > 1.0 => {
                Some((*value, *confidence))
            }
            _ => None,
        }
    }
}

impl Default for DistinctValueCount {
    fn default() -> Self {
        Self::unknown(
            crate::sql::optimizer::stats_input::StatsMissingReason::ColumnNotReported(
                "ndv".to_string(),
            ),
        )
    }
}
```

- [ ] **Step 2: Add the field while keeping the old field temporarily**

Change `ColumnStatistic` to:

```rust
pub struct ColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,
    pub average_row_size: f64,
    pub distinct_values_count: f64,
    pub ndv: DistinctValueCount,
    pub confidence: Confidence,
}
```

Change `ColumnStatistic::unknown()`:

```rust
pub fn unknown() -> Self {
    Self {
        min_value: f64::NEG_INFINITY,
        max_value: f64::INFINITY,
        nulls_fraction: 0.0,
        average_row_size: 8.0,
        distinct_values_count: f64::NAN,
        ndv: DistinctValueCount::unknown(
            crate::sql::optimizer::stats_input::StatsMissingReason::ColumnNotReported(
                "ndv".to_string(),
            ),
        ),
        confidence: Confidence::Fallback,
    }
}
```

- [ ] **Step 3: Add constructors for known NDV**

Add:

```rust
impl ColumnStatistic {
    pub(crate) fn with_known_ndv(
        mut self,
        ndv: f64,
        confidence: Confidence,
        source: crate::sql::optimizer::stats_input::StatsSource,
    ) -> Self {
        self.distinct_values_count = ndv;
        self.ndv = DistinctValueCount::known(ndv, confidence, source);
        self.confidence = self.confidence.max(confidence);
        self
    }

    pub(crate) fn trusted_ndv(&self) -> Option<(f64, Confidence)> {
        self.ndv.trusted_value()
    }
}
```

- [ ] **Step 4: Update NDV consumers to use accessors**

In `src/sql/optimizer/estimate/ndv.rs`, replace direct checks:

```rust
cs.distinct_values_count.is_finite() && cs.distinct_values_count > 1.0
```

with:

```rust
cs.trusted_ndv().map(|(ndv, confidence)| (ndv, confidence))
```

In `src/sql/optimizer/estimate/selectivity.rs`, replace `trusted_distinct_values_count` with:

```rust
fn trusted_distinct_values_count(stat: &ColumnStatistic) -> Option<f64> {
    stat.trusted_ndv().map(|(ndv, _)| ndv)
}
```

Apply the same accessor in `estimate/join_condition.rs`, `stats.rs`, and aggregate-pushdown cost.

- [ ] **Step 5: Update producers to set known NDV**

Where tests or builders construct real NDV stats, change:

```rust
distinct_values_count: ndv,
```

to:

```rust
distinct_values_count: ndv,
ndv: DistinctValueCount::known(
    ndv,
    Confidence::Exact,
    crate::sql::optimizer::stats_input::StatsSource::TestFixture,
),
```

For derived operator output, use `StatsSource::Derived`. For formula defaults such as join-key fallback or expression fallback, return the fallback number from the function result but do not write it into `ColumnStatistic.ndv`.

- [ ] **Step 6: Change `cap_ndv_at_rows`**

Keep the numeric helper for formula outputs, but add a missing-aware helper:

```rust
pub(crate) fn cap_known_ndv_at_rows(
    ndv: &DistinctValueCount,
    rows: f64,
) -> DistinctValueCount {
    match ndv {
        DistinctValueCount::Known {
            value,
            confidence,
            source,
        } => DistinctValueCount::Known {
            value: cap_ndv_at_rows(*value, rows),
            confidence: *confidence,
            source: *source,
        },
        DistinctValueCount::Unknown { reason } => DistinctValueCount::Unknown {
            reason: reason.clone(),
        },
    }
}
```

Use `cap_known_ndv_at_rows` when mutating `ColumnStatistic.ndv`.

- [ ] **Step 7: Replace unknown NDV tests**

Update `get_expr_ndv_ignores_unknown_ndv` in `src/sql/optimizer/estimate/ndv.rs` so the first assertion becomes:

```rust
assert!(matches!(
    column_stats[&test_col_id("unknown_col")].ndv,
    DistinctValueCount::Unknown { .. }
));
```

The expected returned value remains `DEFAULT_EXPR_NDV`; that is now a formula fallback, not stored column stats.

- [ ] **Step 8: Run NDV tests**

Run:

```bash
cargo test --lib sql::optimizer::estimate::ndv sql::optimizer::estimate::selectivity sql::optimizer::stats
```

Expected: all NDV/selectivity/statistics tests pass, and no test asserts `ColumnStatistic::unknown().distinct_values_count == 1.0`.

- [ ] **Step 9: Commit**

```bash
git add src/sql/optimizer
git commit -m "refactor: make optimizer ndv missing-aware"
```

### Task 11: Delete Name-Based Row-Count Fallback

**Files:**
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/statistics.rs`
- Modify: affected optimizer tests
- Test: audit commands and optimizer tests

- [ ] **Step 1: Delete `estimate_default_row_count`**

Remove the function from `src/sql/optimizer/stats.rs`:

```rust
fn estimate_default_row_count(table_name: &str) -> f64
```

Remove tests named like:

```rust
test_estimate_default_row_count_*
```

Replace them with the scan-derivation test from Task 9 if it is not already in place.

- [ ] **Step 2: Add the audit test**

Add this test to `src/sql/optimizer/stats.rs`:

```rust
#[test]
fn missing_scan_stats_do_not_inspect_table_name() {
    let stats = derive_scan_for_test_with_empty_snapshot("store_sales");
    let also_stats = derive_scan_for_test_with_empty_snapshot("tiny_dim");
    assert_eq!(stats.output_row_count, also_stats.output_row_count);
    assert_eq!(stats.row_count_confidence, Confidence::Fallback);
    assert_eq!(also_stats.row_count_confidence, Confidence::Fallback);
}
```

Use existing local test helpers for scan construction; keep both table names because they were formerly special-cased.

- [ ] **Step 3: Run source audit**

Run:

```bash
rg -n "estimate_default_row_count|FACT_TABLE_PATTERNS|MEDIUM_TABLE_PATTERNS|SMALL_TABLE_PATTERNS|contains\\(\"sales\"\\)|contains\\(\"_dim\"\\)" src/sql/optimizer
```

Expected: no output.

- [ ] **Step 4: Run optimizer tests**

Run:

```bash
cargo test --lib sql::optimizer
```

Expected: all optimizer tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer
git commit -m "refactor: remove name-based scan row fallback"
```

### Task 12: Add End-to-End SQL Regression Coverage

**Files:**
- Create: `sql-tests/optimizer/sql/query_stats_provider.sql`
- Create: `sql-tests/optimizer/result/query_stats_provider.result`
- Test: SQL optimizer suite

- [ ] **Step 1: Add the SQL case**

Create `sql-tests/optimizer/sql/query_stats_provider.sql`:

```sql
-- @normalize_explain_timing
DROP TABLE IF EXISTS random_business_events;
CREATE TABLE random_business_events (
  id INT,
  category INT,
  payload VARCHAR
);
INSERT INTO random_business_events VALUES
  (1, 10, 'a'),
  (2, 20, 'b'),
  (3, 20, 'c');
EXPLAIN COSTS SELECT * FROM random_business_events;

DROP TABLE IF EXISTS misleading_sales_table;
CREATE TABLE misleading_sales_table (
  id INT,
  amount INT
);
INSERT INTO misleading_sales_table VALUES (1, 100), (2, 200);
EXPLAIN COSTS SELECT * FROM misleading_sales_table;

SELECT COUNT(*) FROM random_business_events;
SELECT COUNT(*) FROM misleading_sales_table;
```

- [ ] **Step 2: Record the result**

Start the standalone server using the generated environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In another shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only query_stats_provider --mode record
```

Expected result assertions:

```text
TABLE STATS ref=0 rows=3 confidence=Exact source=IcebergManifest
TABLE STATS ref=0 rows=2 confidence=Exact source=IcebergManifest
```

If the local optimizer suite uses in-memory Parquet instead of Iceberg for `CREATE TABLE`, place this case in the Iceberg REST-backed suite and name it `iceberg_query_stats_provider.sql`.

- [ ] **Step 3: Verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only query_stats_provider --mode verify
```

Expected: `case num: 1`, `failed: 0`.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/optimizer/sql/query_stats_provider.sql sql-tests/optimizer/result/query_stats_provider.result
git commit -m "test: cover query stats provider row counts"
```

### Task 13: Remove Compatibility Adapters and Old Stats Map Callers

**Files:**
- Modify: `src/sql/planner/optimizer_bridge/plan.rs`
- Modify: `src/sql/optimizer/statistics.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/mv_rewrite_prep.rs`
- Test: source audit and full compile

- [ ] **Step 1: Remove default bridge fallback**

Delete `collect_default_scan_refs` and change `try_logical_plan_to_opt_expr` to require explicit refs or remove it entirely if all callers use `try_logical_plan_to_opt_expr_with_stats_refs`.

Run:

```bash
rg -n "try_logical_plan_to_opt_expr\\(" src | rg -v "with_stats_refs|fn try_logical_plan_to_opt_expr"
```

Expected: no production callers. Unit tests may use a helper that constructs `OptimizerBridgeStatsRefs`.

- [ ] **Step 2: Remove legacy stats map conversion**

Delete:

```rust
TableStatistics::try_from_base_stats
```

and any aggregate-pushdown adapter that reconstructs table-name maps. Aggregate pushdown must read scan stats through `scan.stats_ref`.

- [ ] **Step 3: Remove old plan stats builder**

Delete from `src/engine/mod.rs`:

```rust
fn build_table_stats_from_plan(...)
fn collect_scan_stats(...)
fn load_iceberg_puffin_ndv(...)
```

Run:

```bash
rg -n "build_table_stats_from_plan|collect_scan_stats|load_iceberg_puffin_ndv" src
```

Expected: no output, except an Iceberg provider helper with a connector-local name if it still needs Puffin loading.

- [ ] **Step 4: Run full compile and focused tests**

Run:

```bash
cargo check --lib
cargo test --lib sql::optimizer engine::query_stats connector::stats
```

Expected: compile succeeds and focused tests pass.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "refactor: remove legacy optimizer stats adapters"
```

### Task 14: Architecture Audit and Final Verification

**Files:**
- Modify: `docs/design/specs/2026-06-22-query-stats-provider-architecture-design.md` only if implementation decisions require a factual update
- Test: audit commands, cargo tests, SQL suites

- [ ] **Step 1: Audit optimizer dependency boundary**

Run:

```bash
rg -n "crate::(engine|connector|catalog_mgr)" src/sql/optimizer
```

Expected: no output.

- [ ] **Step 2: Audit removed name heuristics**

Run:

```bash
rg -n "estimate_default_row_count|FACT_TABLE_PATTERNS|SMALL_TABLE_PATTERNS|lineitem|_dim|contains\\(\"sales\"\\)" src/sql/optimizer
```

Expected: no output related to row-count fallback. Test data names outside fallback logic are acceptable only when the assertion proves names do not affect row count.

- [ ] **Step 3: Audit old table-name stats maps**

Run:

```bash
rg -n "HashMap<String, TableStatistics>|set_query_table_stats|query_table_stats|table_stats: &HashMap|table_stats: HashMap" src/sql src/engine
```

Expected: no optimizer input or MV rewrite path still uses a table-name-keyed stats map. Local test fixtures may use `HashMap<String, ColumnStatistic>` for column stats.

- [ ] **Step 4: Run formatting and unit tests**

Run:

```bash
cargo fmt --all -- --check
cargo test --lib sql::optimizer engine::query_stats connector::stats
```

Expected: formatting check and focused unit tests pass.

- [ ] **Step 5: Run SQL regression suites**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: both suites pass.

- [ ] **Step 6: Commit final audit updates**

If Step 1-5 required only code/test changes already committed, skip this commit. If result files or docs changed, commit them:

```bash
git add docs sql-tests src
git commit -m "test: verify query stats provider architecture"
```

## Execution Notes

- Treat every compile error that mentions `HashMap<String, TableStatistics>` as useful guidance. The end state is no production optimizer path keyed by table name.
- Do not add catalog or connector imports inside `src/sql/optimizer`.
- Do not replace the old name heuristic with a different table-name heuristic. The only allowed missing-row fallback is a single operator-level constant with `Confidence::Fallback`.
- Keep provider failures advisory: log at debug/warn and return `StatsMissingReason`, but never block query execution because statistics are absent.
- Keep commit messages in English.
