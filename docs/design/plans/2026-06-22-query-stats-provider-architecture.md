# Query Stats Provider Architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace name-based scan row-count fallback with query-scoped catalog statistics, and land missing-aware NDV semantics in the same architecture arc.

**Architecture:** The planner bridge keeps a single responsibility: convert `LogicalPlanNode` into an owned optimizer `OptExpr` whose scans have `stats_ref: None`. `QueryStatsCollector` then performs one mutable traversal over that same `OptExpr`, allocating `StatsRef`, writing it into each `ScanOp`, and building `QueryStatsSnapshot`; `optimize()` rejects unbound scans before costing.

**Tech Stack:** Rust, NovaRocks standalone SQL optimizer, Iceberg catalog metadata, sql-test runner, cargo test.

---

## File Structure

- Create `src/sql/optimizer/stats_input.rs`
  - Owns optimizer input statistics types: `StatsRef`, `QueryStatsSnapshot`, `BaseTableStatistics`, `BaseColumnStatistics`, `StatValue`, `StatsSource`, and `StatsMissingReason`.
  - Contains only optimizer-native data structures and display labels. It must not import `engine`, `connector`, or `catalog_mgr`.

- Modify `src/sql/optimizer/operator.rs`
  - Add `stats_ref: Option<StatsRef>` to `ScanOp`.
  - Bridge-created scans start unbound (`None`); `QueryStatsCollector` changes them to `Some(ref)`.

- Modify `src/sql/optimizer/mod.rs`, `src/sql/optimizer/rewrite/**`, `src/sql/optimizer/search.rs`, and `src/sql/optimizer/cascades_rules/multi_join_reorder/**`
  - Replace `&HashMap<String, TableStatistics>` optimizer inputs with `&QueryStatsSnapshot`.
  - Validate all scans are bound at `optimize()` entry after the query stats collector is wired into all callers.

- Create `src/connector/stats.rs`
  - Defines `TableStatsProvider`, `TableStatsRequest`, `TableSnapshotRef`, `ScanSourceIdentity`, and `StatsProviderError`.

- Modify `src/connector/backend.rs`, `src/connector/mod.rs`, and `src/connector/iceberg/**`
  - Add an optional `TableSource::stats_provider() -> Option<Arc<dyn TableStatsProvider>>`.
  - Implement the Iceberg provider using current-snapshot manifests, existing data-file cache, and Puffin NDV.

- Create `src/engine/query_stats.rs`
  - Owns `QueryStatsCollector`, provider lookup, scan-to-request conversion, per-query cache, `StatsRef` allocation, and missing-stat conversion.
  - Traverses `&mut OptExpr`, not `LogicalPlanNode`.

- Modify `src/engine/mod.rs`
  - Replace `build_table_stats_from_plan()`, `collect_scan_stats()`, and engine-owned Puffin loading with the collector.
  - Migrate SELECT, EXPLAIN, EXPLAIN ANALYZE, and INSERT SELECT / Iceberg write planning.

- Modify `src/engine/mv_rewrite_prep.rs` and `src/sql/optimizer/cascades_rules/mv_rewrite/**`
  - Allocate independent stats refs for MV target scans.
  - Never reuse the original base scan stats ref for an MV replacement scan.

- Modify `src/sql/optimizer/stats.rs`, `src/sql/optimizer/statistics.rs`, `src/sql/optimizer/estimate/**`, and aggregate-pushdown cost code
  - Derive scan statistics through `ScanOp.stats_ref -> QueryStatsSnapshot`.
  - Replace and remove `ColumnStatistic.distinct_values_count: f64` production use.
  - Delete `estimate_default_row_count`.

- Add SQL tests under `sql-tests/optimizer/sql/` and `sql-tests/optimizer/result/`
  - Cover non-TPC names, misleading `sales` names, `_dim` names larger than the old 10k heuristic, SELECT vs EXPLAIN consistency, and Puffin NDV after `ANALYZE TABLE`.

## Implementation Tasks

### Task 1: Add Query Stats Input Types

**Files:**
- Create: `src/sql/optimizer/stats_input.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Test: unit tests inside `src/sql/optimizer/stats_input.rs`

- [ ] **Step 1: Export the module**

Add this line in `src/sql/optimizer/mod.rs` with the other optimizer modules:

```rust
pub(crate) mod stats_input;
```

- [ ] **Step 2: Add stats input types**

Create `src/sql/optimizer/stats_input.rs`:

```rust
//! Query-scoped statistics input for the optimizer.
//!
//! No engine, connector, or catalog dependencies belong in this module.

use std::collections::HashMap;

use crate::sql::optimizer::statistics::Confidence;

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
    ConnectorUnsupported(String),
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
    pub(crate) fn missing(column: &str) -> Self {
        let reason = StatsMissingReason::ColumnNotReported(column.to_ascii_lowercase());
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryStatsEntry {
    pub label: String,
    pub stats: BaseTableStatistics,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QueryStatsSnapshot {
    entries: HashMap<StatsRef, QueryStatsEntry>,
}

impl QueryStatsSnapshot {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(
        &mut self,
        stats_ref: StatsRef,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) {
        self.entries.insert(
            stats_ref,
            QueryStatsEntry {
                label: label.into(),
                stats,
            },
        );
    }

    pub(crate) fn get(&self, stats_ref: StatsRef) -> Option<&BaseTableStatistics> {
        self.entries.get(&stats_ref).map(|entry| &entry.stats)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn display_rows(&self) -> Vec<String> {
        let mut refs = self.entries.keys().copied().collect::<Vec<_>>();
        refs.sort_by_key(|stats_ref| stats_ref.as_u32());
        refs.into_iter()
            .map(|stats_ref| {
                let entry = &self.entries[&stats_ref];
                match &entry.stats.row_count {
                    StatValue::Known {
                        value,
                        confidence,
                        source,
                    } => format!(
                        "TABLE STATS ref={} table={} rows={} confidence={:?} source={:?}",
                        stats_ref.as_u32(),
                        entry.label,
                        value,
                        confidence,
                        source
                    ),
                    StatValue::Missing { reason } => format!(
                        "TABLE STATS ref={} table={} rows=missing reason={:?}",
                        stats_ref.as_u32(),
                        entry.label,
                        reason
                    ),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_rows_sort_by_numeric_ref() {
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            StatsRef::new(10),
            "db.t10",
            BaseTableStatistics::missing(StatsMissingReason::NoCurrentSnapshot),
        );
        snapshot.insert(
            StatsRef::new(2),
            "db.t2",
            BaseTableStatistics::missing(StatsMissingReason::NoCurrentSnapshot),
        );

        let rows = snapshot.display_rows();
        assert!(rows[0].contains("ref=2 table=db.t2"));
        assert!(rows[1].contains("ref=10 table=db.t10"));
    }

    #[test]
    fn connector_unsupported_preserves_reason() {
        let reason = StatsMissingReason::ConnectorUnsupported("jdbc".to_string());
        assert!(matches!(
            reason,
            StatsMissingReason::ConnectorUnsupported(ref value) if value == "jdbc"
        ));
    }
}
```

- [ ] **Step 3: Run the focused test**

Run:

```bash
cargo test --lib sql::optimizer::stats_input
```

Expected: both `stats_input` tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer/mod.rs src/sql/optimizer/stats_input.rs
git commit -m "feat: add query stats input types"
```

### Task 2: Add Optional StatsRef to ScanOp and Validate Bound Scans

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/planner/optimizer_bridge/plan.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Test: unit tests in `src/sql/optimizer/mod.rs` or `src/sql/optimizer/operator.rs`

- [ ] **Step 1: Add the scan field**

In `src/sql/optimizer/operator.rs`, import:

```rust
use crate::sql::optimizer::stats_input::StatsRef;
```

Add this field to `ScanOp`:

```rust
pub stats_ref: Option<StatsRef>,
```

- [ ] **Step 2: Initialize scans as unbound in the bridge**

In `src/sql/planner/optimizer_bridge/plan.rs`, add this field when constructing `ScanOp`:

```rust
stats_ref: None,
```

Do not change the bridge signature. Do not add any ordinal-based bridge API or a second scan-order mapping.

- [ ] **Step 3: Add the optimizer-entry validation helper**

In `src/sql/optimizer/mod.rs`, add:

```rust
fn validate_query_stats_bound(expr: &OptExpr) -> Result<(), String> {
    match &expr.op {
        Operator::LogicalScan(scan) | Operator::PhysicalScan(scan)
            if scan.stats_ref.is_none() =>
        {
            return Err(format!(
                "optimizer scan statistics are not bound for table {}",
                scan.table.name
            ));
        }
        _ => {}
    }
    for child in &expr.children {
        validate_query_stats_bound(child)?;
    }
    Ok(())
}
```

Do not call this helper from `optimize_with_root_property` until Task 6 wires
`QueryStatsCollector` into all production optimizer callers. Bridge-created
scans intentionally remain unbound in this task; enabling the validation before
the collector would make normal scan queries fail.

- [ ] **Step 4: Add validation test**

Add a unit test that builds one `LogicalScan` without collecting stats and calls `validate_query_stats_bound`:

```rust
#[test]
fn optimizer_rejects_unbound_scan_stats() {
    let scan = test_scan_opt_expr("db", "t");
    let err = validate_query_stats_bound(&scan).expect_err("scan should be unbound");
    assert!(err.contains("optimizer scan statistics are not bound"));
}
```

Use the local optimizer scan test helper already present in `src/sql/optimizer/mod.rs`; if the helper name differs, keep the assertion text exactly as above.

- [ ] **Step 5: Run tests**

Run as separate Cargo filter commands if needed:

```bash
cargo test --lib sql::optimizer::optimizer_rejects_unbound_scan_stats
cargo test --lib sql::planner::optimizer_bridge::plan
```

Expected: bridge tests still pass, and the new unbound-scan validation test passes.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/operator.rs src/sql/planner/optimizer_bridge/plan.rs src/sql/optimizer/mod.rs
git commit -m "feat: require bound scan stats refs"
```

### Task 3: Convert Optimizer API to QueryStatsSnapshot

**Files:**
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/rewrite/context.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/**`
- Modify: `src/sql/optimizer/cascades_rules/multi_join_reorder/**`
- Test: existing optimizer unit tests

- [ ] **Step 1: Change optimizer function signatures**

Replace optimizer API parameters:

```rust
table_stats: &HashMap<String, TableStatistics>,
```

with:

```rust
query_stats: &crate::sql::optimizer::stats_input::QueryStatsSnapshot,
```

Apply this to:

```rust
optimize(...)
optimize_with_root_distribution(...)
optimize_with_root_property(...)
```

- [ ] **Step 2: Change rewrite context storage**

In `src/sql/optimizer/rewrite/context.rs`, replace table-name stats storage with:

```rust
query_stats: Option<Arc<QueryStatsSnapshot>>,
```

Use these methods:

```rust
pub(crate) fn set_query_stats(&mut self, query_stats: QueryStatsSnapshot) {
    self.query_stats = Some(Arc::new(query_stats));
}

pub(crate) fn query_stats(&self) -> Option<&QueryStatsSnapshot> {
    self.query_stats.as_deref()
}
```

- [ ] **Step 3: Change rewrite pipeline input**

In `src/sql/optimizer/rewrite/registry.rs`, change:

```rust
pub(crate) fn query_rewrite_pipeline(query_stats: &QueryStatsSnapshot) -> RewritePipeline
```

and pass `query_stats` to aggregate pushdown. Remove `HashMap<String, TableStatistics>` from rewrite pipeline signatures.

- [ ] **Step 4: Use a stats-ref adapter only inside aggregate pushdown**

Where aggregate pushdown still expects legacy `TableStatistics`, use `scan.stats_ref`:

```rust
fn legacy_table_stats_for_scan(
    scan: &crate::sql::optimizer::operator::ScanOp,
    query_stats: &QueryStatsSnapshot,
) -> Option<crate::sql::optimizer::statistics::TableStatistics> {
    let stats_ref = scan.stats_ref?;
    let base = query_stats.get(stats_ref)?;
    crate::sql::optimizer::statistics::TableStatistics::try_from_base_stats(base)
}
```

This adapter is removed in the ST-2 cleanup task. It must not reconstruct a table-name-keyed map.

- [ ] **Step 5: Add the temporary legacy conversion**

In `src/sql/optimizer/statistics.rs`, add:

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
            column_stats.insert(name.to_ascii_lowercase(), stat);
        }

        Some(Self {
            row_count,
            column_stats,
        })
    }
}
```

This method is a migration adapter only. Task 9 removes production dependency on the legacy NDV field and Task 12 audits away the adapter if no caller remains.

- [ ] **Step 6: Run compile check**

Run:

```bash
cargo check --lib
```

Expected: remaining failures point to engine callers still passing old `table_stats`; do not record SQL goldens in this mid-migration state.

- [ ] **Step 7: Commit**

```bash
git add src/sql/optimizer
git commit -m "refactor: pass query stats snapshot into optimizer"
```

### Task 4: Add Connector Stats Provider and Iceberg Implementation

**Files:**
- Create: `src/connector/stats.rs`
- Modify: `src/connector/mod.rs`
- Modify: `src/connector/backend.rs`
- Modify: `src/connector/iceberg/mod.rs`
- Create: `src/connector/iceberg/stats.rs`
- Modify: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/sql/optimizer/statistics.rs`
- Test: connector and statistics unit tests

- [ ] **Step 1: Export connector stats**

Add to `src/connector/mod.rs`:

```rust
pub(crate) mod stats;
```

- [ ] **Step 2: Add provider types**

Create `src/connector/stats.rs`:

```rust
use crate::sql::optimizer::stats_input::{BaseTableStatistics, StatsMissingReason};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ScanSourceIdentity {
    IcebergTable {
        catalog: String,
        namespace: String,
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
            Self::Unsupported(reason) => StatsMissingReason::ConnectorUnsupported(reason),
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
```

- [ ] **Step 3: Add provider hook to TableSource**

In `src/connector/backend.rs`, import `Arc` and `TableStatsProvider`, then add this default method to `TableSource`:

```rust
fn stats_provider(&self) -> Option<Arc<dyn TableStatsProvider>> {
    None
}
```

- [ ] **Step 4: Add a missing-aware Iceberg stats builder**

In `src/sql/optimizer/statistics.rs`, add a helper that lowercases column keys:

```rust
pub(crate) fn build_base_table_statistics_with_ndv(
    files: &[crate::sql::catalog::IcebergDataFileInfo],
    columns: &[crate::sql::catalog::ColumnDef],
    ndv_by_name: &HashMap<String, f64>,
    name_to_field_id: &HashMap<String, i32>,
) -> crate::sql::optimizer::stats_input::BaseTableStatistics {
    use crate::sql::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };

    if files.is_empty() {
        return BaseTableStatistics {
            row_count: StatValue::known(0, Confidence::Exact, StatsSource::IcebergManifest),
            columns: HashMap::new(),
            source: StatsSource::IcebergManifest,
        };
    }

    let Some(legacy) =
        build_table_statistics_with_ndv(files, columns, ndv_by_name, name_to_field_id)
    else {
        return BaseTableStatistics::missing(StatsMissingReason::ManifestMissingRowCount);
    };

    let columns = legacy
        .column_stats
        .into_iter()
        .map(|(name, stat)| {
            let key = name.to_ascii_lowercase();
            let confidence = stat.confidence;
            let ndv = if confidence > Confidence::Fallback
                && stat.distinct_values_count.is_finite()
                && stat.distinct_values_count > 1.0
            {
                StatValue::known(
                    stat.distinct_values_count,
                    confidence,
                    StatsSource::IcebergPuffin,
                )
            } else {
                StatValue::missing(StatsMissingReason::ColumnNotReported(key.clone()))
            };
            (
                key,
                BaseColumnStatistics {
                    nulls_fraction: StatValue::known(
                        stat.nulls_fraction,
                        confidence,
                        StatsSource::IcebergManifest,
                    ),
                    average_row_size: StatValue::known(
                        stat.average_row_size,
                        confidence,
                        StatsSource::IcebergManifest,
                    ),
                    min_value: StatValue::known(
                        stat.min_value,
                        confidence,
                        StatsSource::IcebergManifest,
                    ),
                    max_value: StatValue::known(
                        stat.max_value,
                        confidence,
                        StatsSource::IcebergManifest,
                    ),
                    ndv,
                },
            )
        })
        .collect();

    BaseTableStatistics {
        row_count: StatValue::known(
            legacy.row_count,
            Confidence::Exact,
            StatsSource::IcebergManifest,
        ),
        columns,
        source: StatsSource::IcebergManifest,
    }
}
```

- [ ] **Step 5: Implement Iceberg provider with existing conversion helper**

Create `src/connector/iceberg/stats.rs` and add `pub(crate) mod stats;` in `src/connector/iceberg/mod.rs`.

Use the existing `data_file_with_stats_to_iceberg_data_file_info` helper from `src/connector/iceberg/catalog/backend.rs`; it is already `pub(crate)`.

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::connector::iceberg::catalog::backend::data_file_with_stats_to_iceberg_data_file_info;
use crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at;
use crate::connector::iceberg::catalog::{IcebergCatalogRegistry, load_table as reg_load_table};
use crate::connector::stats::{
    ScanSourceIdentity, StatsProviderError, TableSnapshotRef, TableStatsProvider,
    TableStatsRequest,
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

        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(catalog).map_err(StatsProviderError::Catalog)?;
        let loaded = reg_load_table(&entry, namespace, table).map_err(StatsProviderError::Catalog)?;
        let snapshot_id = match request.snapshot.as_ref().unwrap_or(&TableSnapshotRef::Current) {
            TableSnapshotRef::Current => loaded.table.metadata().current_snapshot_id(),
            TableSnapshotRef::SnapshotId(id) => Some(*id),
            TableSnapshotRef::Branch(_) | TableSnapshotRef::Tag(_) => None,
        };
        let Some(snapshot_id) = snapshot_id else {
            return Ok(BaseTableStatistics::missing(
                StatsMissingReason::NoCurrentSnapshot,
            ));
        };

        let cached = entry
            .cached_data_files(namespace, table, Some(snapshot_id))
            .map_err(StatsProviderError::Metadata)?;
        let data_files = match cached {
            Some(files) => files,
            None => {
                let files = extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                    .map_err(StatsProviderError::Metadata)?;
                entry
                    .cache_data_files(namespace, table, Some(snapshot_id), files.clone())
                    .map_err(StatsProviderError::Metadata)?;
                files
            }
        };
        let columns = loaded.columns.clone();
        let cloud_properties = entry.cloud_properties_map();
        let iceberg_table = loaded.table.clone();
        drop(guard);

        let files = data_files
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
}
```

Move the existing Puffin NDV logic from `src/engine/mod.rs::load_iceberg_puffin_ndv` into this module as `load_iceberg_puffin_ndv`.

- [ ] **Step 6: Wire the provider from IcebergTableSource**

In `src/connector/iceberg/catalog/backend.rs`, add:

```rust
fn stats_provider(&self) -> Option<Arc<dyn crate::connector::stats::TableStatsProvider>> {
    Some(Arc::new(crate::connector::iceberg::stats::IcebergTableStatsProvider::new(
        Arc::clone(&self.registry),
    )))
}
```

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test --lib connector::stats connector::iceberg::stats sql::optimizer::statistics
```

Expected: focused connector/statistics tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector src/sql/optimizer/statistics.rs
git commit -m "feat: provide iceberg query statistics"
```

### Task 5: Collect Stats by Mutating OptExpr

**Files:**
- Create: `src/engine/query_stats.rs`
- Modify: `src/engine/mod.rs`
- Test: unit tests in `src/engine/query_stats.rs`

- [ ] **Step 1: Export the module**

Add to `src/engine/mod.rs`:

```rust
mod query_stats;
```

- [ ] **Step 2: Add collector and provider lookup**

Create `src/engine/query_stats.rs`:

```rust
use std::sync::Arc;

use crate::connector::stats::{
    ScanSourceIdentity, TableSnapshotRef, TableStatsProvider, TableStatsRequest,
};
use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::stats_input::{
    BaseTableStatistics, QueryStatsSnapshot, StatsMissingReason, StatsRef,
};

#[derive(Clone, Default)]
pub(crate) struct QueryStatsProviders {
    iceberg: Option<Arc<dyn TableStatsProvider>>,
}

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

    pub(crate) fn from_standalone_state(state: &Arc<super::StandaloneState>) -> Self {
        let connectors = state
            .connectors
            .read()
            .expect("standalone connectors read lock");
        Self::from_connectors(&connectors)
    }

    pub(crate) fn from_optional_state(state: Option<&Arc<super::StandaloneState>>) -> Self {
        state.map(Self::from_standalone_state).unwrap_or_else(Self::none)
    }
}

pub(crate) struct QueryStatsPlan {
    pub snapshot: QueryStatsSnapshot,
    next_stats_ref: u32,
}

impl QueryStatsPlan {
    fn new(snapshot: QueryStatsSnapshot, next_stats_ref: u32) -> Self {
        Self {
            snapshot,
            next_stats_ref,
        }
    }

    pub(crate) fn add_stats(
        &mut self,
        label: impl Into<String>,
        stats: BaseTableStatistics,
    ) -> StatsRef {
        let stats_ref = StatsRef::new(self.next_stats_ref);
        self.next_stats_ref += 1;
        self.snapshot.insert(stats_ref, label, stats);
        stats_ref
    }
}

pub(crate) struct QueryStatsCollector {
    providers: QueryStatsProviders,
    next_stats_ref: u32,
    snapshot: QueryStatsSnapshot,
}

impl QueryStatsCollector {
    pub(crate) fn new(providers: QueryStatsProviders) -> Self {
        Self {
            providers,
            next_stats_ref: 0,
            snapshot: QueryStatsSnapshot::empty(),
        }
    }

    pub(crate) fn collect(mut self, opt_expr: &mut OptExpr) -> QueryStatsPlan {
        self.walk(opt_expr);
        QueryStatsPlan::new(self.snapshot, self.next_stats_ref)
    }

    fn walk(&mut self, expr: &mut OptExpr) {
        if let Operator::LogicalScan(scan) = &mut expr.op {
            let stats_ref = StatsRef::new(self.next_stats_ref);
            self.next_stats_ref += 1;
            scan.stats_ref = Some(stats_ref);
            let (label, stats) = self.collect_scan(scan);
            self.snapshot.insert(stats_ref, label, stats);
        }
        for child in &mut expr.children {
            self.walk(child);
        }
    }

    fn collect_scan(
        &self,
        scan: &crate::sql::optimizer::operator::ScanOp,
    ) -> (String, BaseTableStatistics) {
        let label = scan_label(scan);
        let Some(request) = table_stats_request(scan) else {
            return (
                label,
                BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                    "scan source does not expose query stats".to_string(),
                )),
            );
        };

        let stats = match &request.source {
            ScanSourceIdentity::IcebergTable { .. } => {
                let Some(provider) = self.providers.iceberg.as_deref() else {
                    return (
                        label,
                        BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                            "iceberg stats provider is not registered".to_string(),
                        )),
                    );
                };
                provider
                    .estimate_table_statistics(&request)
                    .unwrap_or_else(|err| BaseTableStatistics::missing(err.into_missing_reason()))
            }
            ScanSourceIdentity::Unsupported { reason } => {
                BaseTableStatistics::missing(StatsMissingReason::ConnectorUnsupported(
                    reason.clone(),
                ))
            }
        };
        (label, stats)
    }
}

fn scan_label(scan: &crate::sql::optimizer::operator::ScanOp) -> String {
    match &scan.table.source {
        ScanSource::IcebergDataFiles { table, .. }
        | ScanSource::IcebergVersionTable { table, .. }
        | ScanSource::IcebergDeltaTable { table, .. } => {
            format!("{}.{}.{}", table.catalog, table.namespace, table.table)
        }
        _ => format!("{}.{}", scan.database, scan.table.name),
    }
}

fn table_stats_request(
    scan: &crate::sql::optimizer::operator::ScanOp,
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
            snapshot: Some(TableSnapshotRef::Current),
        }),
        ScanSource::IcebergVersionTable { table, snapshot_id } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::IcebergTable {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
            },
            snapshot: Some(TableSnapshotRef::SnapshotId(*snapshot_id)),
        }),
        ScanSource::IcebergDeltaTable { table, .. } => Some(TableStatsRequest {
            catalog: Some(table.catalog.clone()),
            database: table.namespace.clone(),
            table: table.table.clone(),
            source: ScanSourceIdentity::Unsupported {
                reason: "iceberg delta scan stats are not supported".to_string(),
            },
            snapshot: None,
        }),
        _ => None,
    }
}
```

- [ ] **Step 3: Add collector invariant tests**

Add tests that build a two-scan join `OptExpr`, collect stats with `QueryStatsProviders::none()`, and assert both scans have distinct `Some(StatsRef)` values and `snapshot.len() == 2`.

```rust
#[test]
fn collector_binds_each_scan_in_the_same_opt_expr_traversal() {
    let mut expr = test_join_with_two_scans();
    let plan = QueryStatsCollector::new(QueryStatsProviders::none()).collect(&mut expr);
    let refs = collect_scan_refs_for_test(&expr);
    assert_eq!(refs.len(), 2);
    assert_ne!(refs[0], refs[1]);
    assert_eq!(plan.snapshot.len(), 2);
}
```

Use local helper names if the optimizer test module already has scan constructors.

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test --lib engine::query_stats
```

Expected: collector tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mod.rs src/engine/query_stats.rs
git commit -m "feat: collect query stats from optimizer expressions"
```

### Task 6: Migrate Query Entrypoints

**Files:**
- Modify: `src/engine/mod.rs`
- Modify: `src/sql/codegen/fragment_builder.rs` if standalone internal optimize callers exist there
- Test: compile check and optimizer unit tests

- [ ] **Step 1: Replace the common optimize sequence**

For SELECT, EXPLAIN, EXPLAIN ANALYZE, and INSERT SELECT / Iceberg write planning, use this sequence after `try_logical_plan_to_opt_expr`:

```rust
let mut opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
    &logical,
    &mut scalar_arena,
)?;
let providers = crate::engine::query_stats::QueryStatsProviders::from_standalone_state(state);
let mut query_stats = crate::engine::query_stats::QueryStatsCollector::new(providers)
    .collect(&mut opt_expr);
let physical = crate::sql::optimizer::optimize(
    opt_expr,
    scalar_arena,
    &query_stats.snapshot,
    factory,
    dictionary_provider,
    mv_candidates,
)?;
```

After every production caller has collected query stats before `optimize`, enable
the non-optional optimizer boundary check by calling
`validate_query_stats_bound(&plan_expr)?` at the start of
`optimize_with_root_property`, before rewrite runs.

For `explain_query` and `explain_analyze_query`, use:

```rust
let providers =
    crate::engine::query_stats::QueryStatsProviders::from_optional_state(mv_rewrite_state);
```

- [ ] **Step 2: Keep root-distribution paths equivalent**

For `optimize_with_root_distribution`, pass `&query_stats.snapshot` and the same `opt_expr` after collector mutation:

```rust
crate::sql::optimizer::optimize_with_root_distribution(
    opt_expr,
    scalar_arena,
    &query_stats.snapshot,
    factory,
    root_distribution,
)?;
```

- [ ] **Step 3: Replace EXPLAIN COSTS table stats display**

Use `query_stats.snapshot.display_rows()`:

```rust
if matches!(level, ExplainLevel::Costs) {
    lines.extend(query_stats.snapshot.display_rows());
}
```

The output includes both `ref` and `table`, sorted by numeric ref.

- [ ] **Step 4: Remove old engine stats builders**

Delete these from `src/engine/mod.rs` after all callers move:

```rust
fn build_table_stats_from_plan(...)
fn collect_scan_stats(...)
fn load_iceberg_puffin_ndv(...)
```

Run:

```bash
rg -n "build_table_stats_from_plan|collect_scan_stats|load_iceberg_puffin_ndv" src
```

Expected: no output except the connector-local Puffin helper in `src/connector/iceberg/stats.rs`.

- [ ] **Step 5: Run compile check**

Run:

```bash
cargo check --lib
```

Expected: compile succeeds. SQL suite output may still change until Task 10 removes the old fallback and goldens are recorded.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mod.rs src/sql/codegen/fragment_builder.rs src/connector/iceberg/stats.rs
git commit -m "refactor: use query stats collector in engine planning"
```

### Task 7: Key MV Rewrite Stats by Independent StatsRef

**Files:**
- Modify: `src/engine/mv_rewrite_prep.rs`
- Modify: `src/sql/optimizer/cascades_rules/mv_rewrite/**`
- Modify: `src/engine/query_stats.rs`
- Test: MV rewrite unit tests

- [ ] **Step 1: Change MV candidate stats field**

Add a required stats ref to MV candidates:

```rust
pub(crate) target_stats_ref: crate::sql::optimizer::stats_input::StatsRef,
```

Avoid `Option<StatsRef>` in the rule layer; candidate preparation must decide whether to add a missing stats entry or skip the candidate.

- [ ] **Step 2: Allocate target stats through QueryStatsPlan**

In `src/engine/query_stats.rs`, use the existing `QueryStatsPlan::add_stats` method:

```rust
let target_stats_ref = query_stats.add_stats(label, target_stats);
```

Do not use `snapshot.len()` as an allocator.

- [ ] **Step 3: Use missing stats for target failures**

When MV target stats cannot be loaded but the candidate is still otherwise valid, allocate:

```rust
let target_stats = BaseTableStatistics::missing(
    StatsMissingReason::CatalogLoadError(err.to_string()),
);
let target_stats_ref = query_stats.add_stats(target_label, target_stats);
```

If the MV rule cannot safely cost the candidate without target stats, skip the candidate in `prepare_mv_rewrite_candidates`. Do not borrow the original base scan's stats ref.

- [ ] **Step 4: Fill MV replacement scan with target ref**

In the MV rewrite rule, construct the replacement scan with:

```rust
stats_ref: Some(candidate.target_stats_ref),
```

Do not fall back to the original base scan's stats ref.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test --lib mv_rewrite query_stats
```

Expected: MV rewrite and collector tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv_rewrite_prep.rs src/engine/query_stats.rs src/sql/optimizer/cascades_rules/mv_rewrite
git commit -m "refactor: bind mv rewrite stats by stats ref"
```

### Task 8: Derive Scan Statistics from QueryStatsSnapshot

**Files:**
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/statistics.rs`
- Test: unit tests in `src/sql/optimizer/stats.rs`

- [ ] **Step 1: Replace table-name scan lookup**

Change scan statistics derivation to use:

```rust
let stats_ref = scan
    .stats_ref
    .expect("scan stats refs are validated before stats derivation");
let table_stats = query_stats.get(stats_ref);
```

Remove lookup by `scan.table.name` and `scan.alias`.

- [ ] **Step 2: Use a single non-name fallback**

Add:

```rust
const MISSING_BASE_ROW_COUNT_FALLBACK: f64 = 100_000.0;
```

Document the value in code:

```rust
// Matches the old unknown-table default from estimate_default_row_count to
// minimize plan churn while removing table-name heuristics.
```

This constant is only for missing base row count after a bound `StatsRef` lookup; it must not inspect table names.

- [ ] **Step 3: Normalize column stat keys**

When mapping base stats to output columns, store and lookup by lowercase column name:

```rust
let key = column.name.to_ascii_lowercase();
let stat = table_stats
    .columns
    .get(&key)
    .map(ColumnStatistic::from_base_column)
    .unwrap_or_else(ColumnStatistic::unknown);
```

Task 4 already lowercases provider-produced keys. This fixes uppercase-column silent misses.

- [ ] **Step 4: Add fallback regression tests**

Add tests that compare missing stats for special old names:

```rust
#[test]
fn missing_scan_stats_do_not_inspect_table_name() {
    let store_sales = derive_scan_for_test_with_missing_stats("store_sales");
    let tiny_dim = derive_scan_for_test_with_missing_stats("tiny_dim");
    assert_eq!(store_sales.output_row_count, 100_000.0);
    assert_eq!(tiny_dim.output_row_count, 100_000.0);
    assert_eq!(store_sales.row_count_confidence, Confidence::Fallback);
    assert_eq!(tiny_dim.row_count_confidence, Confidence::Fallback);
}
```

Add a second test with a table name containing `sales` but a snapshot row count of `2`; assert the scan rows are `2.0`.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test --lib sql::optimizer::stats
```

Expected: scan stats tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/stats.rs src/sql/optimizer/statistics.rs
git commit -m "refactor: derive scan stats from query snapshot"
```

### Task 9: Complete Missing-Aware NDV and Remove f64 Sentinel

**Files:**
- Modify: `src/sql/optimizer/statistics.rs`
- Modify: `src/sql/optimizer/estimate/ndv.rs`
- Modify: `src/sql/optimizer/estimate/selectivity.rs`
- Modify: `src/sql/optimizer/estimate/join_condition.rs`
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/cost.rs`
- Test: optimizer NDV/statistics tests

- [ ] **Step 1: Add DistinctValueCount**

In `src/sql/optimizer/statistics.rs`:

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

- [ ] **Step 2: Replace ColumnStatistic field**

Change:

```rust
pub distinct_values_count: f64,
```

to:

```rust
pub ndv: DistinctValueCount,
```

Update `ColumnStatistic::unknown()` so it stores `DistinctValueCount::Unknown`.

- [ ] **Step 3: Add NDV accessors**

Add:

```rust
impl ColumnStatistic {
    pub(crate) fn with_known_ndv(
        mut self,
        ndv: f64,
        confidence: Confidence,
        source: crate::sql::optimizer::stats_input::StatsSource,
    ) -> Self {
        self.ndv = DistinctValueCount::known(ndv, confidence, source);
        self.confidence = self.confidence.max(confidence);
        self
    }

    pub(crate) fn trusted_ndv(&self) -> Option<(f64, Confidence)> {
        self.ndv.trusted_value()
    }
}
```

- [ ] **Step 4: Replace every production direct read**

Run:

```bash
rg -n "distinct_values_count|trusted_distinct_values_count" src/sql/optimizer
```

Replace both copies of `trusted_distinct_values_count` (`estimate/selectivity.rs` and `stats.rs`) with `ColumnStatistic::trusted_ndv()`. Update `estimate/ndv.rs`, `estimate/join_condition.rs`, `stats.rs`, and aggregate-pushdown cost to use accessors.

- [ ] **Step 5: Remove the legacy conversion adapter**

After aggregate pushdown reads `QueryStatsSnapshot` directly through `scan.stats_ref`, delete:

```rust
TableStatistics::try_from_base_stats
```

from `src/sql/optimizer/statistics.rs`. No production path should reconstruct legacy table stats from base stats after this step.

- [ ] **Step 6: Update producers**

Where code used:

```rust
ColumnStatistic {
    distinct_values_count: ndv,
    ..
}
```

use:

```rust
ColumnStatistic {
    ..
}.with_known_ndv(ndv, confidence, StatsSource::Derived)
```

Use `StatsSource::IcebergPuffin` for Puffin NDV, `StatsSource::IcebergManifest` for manifest-derived fallback column stats, and `StatsSource::TestFixture` in tests.

- [ ] **Step 7: Run NDV tests and audit**

Run:

```bash
cargo test --lib sql::optimizer::estimate::ndv sql::optimizer::estimate::selectivity sql::optimizer::stats
rg -n "distinct_values_count|trusted_distinct_values_count" src/sql/optimizer
```

Expected: tests pass; `rg` has no production hits. Test names may mention old behavior only when asserting it was removed.

- [ ] **Step 8: Commit**

```bash
git add src/sql/optimizer
git commit -m "refactor: make optimizer ndv missing-aware"
```

### Task 10: Delete Name-Based Row Fallback

**Files:**
- Modify: `src/sql/optimizer/stats.rs`
- Modify: affected optimizer tests
- Test: source audit and optimizer tests

- [ ] **Step 1: Delete the heuristic function**

Remove:

```rust
fn estimate_default_row_count(table_name: &str) -> f64
```

Remove FACT/MEDIUM/SMALL pattern arrays and old tests that assert `store_sales`, `lineitem`, or `_dim` special handling.

- [ ] **Step 2: Run audit**

Run:

```bash
rg -n "estimate_default_row_count|FACT_TABLE_PATTERNS|MEDIUM_TABLE_PATTERNS|SMALL_TABLE_PATTERNS|contains\\(\"sales\"\\)|contains\\(\"_dim\"\\)" src/sql/optimizer
```

Expected: no output.

- [ ] **Step 3: Run optimizer tests**

Run:

```bash
cargo test --lib sql::optimizer
```

Expected: optimizer tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/sql/optimizer
git commit -m "refactor: remove name-based scan row fallback"
```

### Task 11: Add SQL Regression Coverage

**Files:**
- Create: `sql-tests/optimizer/sql/query_stats_provider.sql`
- Create: `sql-tests/optimizer/result/query_stats_provider.result`
- Test: optimizer SQL suite

- [ ] **Step 1: Add SQL case**

Create `sql-tests/optimizer/sql/query_stats_provider.sql`:

```sql
-- @normalize_explain_timing=true
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
SELECT COUNT(*) FROM random_business_events;

DROP TABLE IF EXISTS misleading_sales_table;
CREATE TABLE misleading_sales_table (
  id INT,
  amount INT
);
INSERT INTO misleading_sales_table VALUES (1, 100), (2, 200);
EXPLAIN COSTS SELECT * FROM misleading_sales_table;
SELECT COUNT(*) FROM misleading_sales_table;

DROP TABLE IF EXISTS oversized_dim_table;
CREATE TABLE oversized_dim_table (
  id INT,
  v INT
);
INSERT INTO oversized_dim_table
SELECT generate_series, generate_series % 17 FROM TABLE(generate_series(1, 10001));
EXPLAIN COSTS SELECT * FROM oversized_dim_table;
SELECT COUNT(*) FROM oversized_dim_table;

ANALYZE TABLE random_business_events;
EXPLAIN COSTS
SELECT category, COUNT(*) FROM random_business_events GROUP BY category;
```

Expected result assertions after record:

```text
TABLE STATS ref=0 table=iceberg_opt.<db>.random_business_events rows=3 confidence=Exact source=IcebergManifest
TABLE STATS ref=0 table=iceberg_opt.<db>.misleading_sales_table rows=2 confidence=Exact source=IcebergManifest
TABLE STATS ref=0 table=iceberg_opt.<db>.oversized_dim_table rows=10001 confidence=Exact source=IcebergManifest
```

The final `ANALYZE TABLE` block should show Puffin-backed NDV in the plan or stats debug line once EXPLAIN prints column stat source.

- [ ] **Step 2: Record result**

Start the server:

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

- [ ] **Step 3: Verify SELECT vs EXPLAIN consistency**

Inspect `sql-tests/optimizer/result/query_stats_provider.result` and confirm:

```text
rows=3
3
rows=2
2
rows=10001
10001
```

The first number in each pair is the EXPLAIN table stat; the second is `COUNT(*)`.

- [ ] **Step 4: Verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only query_stats_provider --mode verify
```

Expected: `case num: 1`, `failed: 0`.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/optimizer/sql/query_stats_provider.sql sql-tests/optimizer/result/query_stats_provider.result
git commit -m "test: cover query stats provider row counts"
```

### Task 12: Final Architecture Audit

**Files:**
- Modify: docs or result files only if audit finds drift
- Test: audit commands, cargo tests, SQL suites

- [ ] **Step 1: Audit removed ordinal path**

Run:

```bash
rg -n "PlanScanOrdinal|OptimizerBridgeStatsRefs|try_logical_plan_to_opt_expr_with_stats_refs|collect_default_scan_refs" src
```

Expected: no output.

- [ ] **Step 2: Audit optimizer dependency boundary**

Run:

```bash
rg -n "crate::(engine|connector|catalog_mgr)" src/sql/optimizer
```

Expected: no output.

- [ ] **Step 3: Audit old stats maps and row heuristics**

Run:

```bash
rg -n "HashMap<String, TableStatistics>|set_query_table_stats|query_table_stats|table_stats: &HashMap|table_stats: HashMap" src/sql src/engine
rg -n "estimate_default_row_count|FACT_TABLE_PATTERNS|SMALL_TABLE_PATTERNS|lineitem|_dim|contains\\(\"sales\"\\)" src/sql/optimizer
```

Expected: no production optimizer input still uses table-name-keyed stats, and no row-count fallback inspects table names.

- [ ] **Step 4: Audit NDV cleanup**

Run:

```bash
rg -n "distinct_values_count|trusted_distinct_values_count" src/sql/optimizer
```

Expected: no production hits.

- [ ] **Step 5: Run formatting and tests**

Run:

```bash
cargo fmt --all -- --check
cargo test --lib sql::optimizer engine::query_stats connector::stats connector::iceberg::stats
```

Expected: formatting check and focused tests pass.

- [ ] **Step 6: Run SQL suites**

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

- [ ] **Step 7: Commit audit updates**

If result files or docs changed during audit:

```bash
git add docs sql-tests src
git commit -m "test: verify query stats provider architecture"
```

## Execution Notes

- Task 3 through Task 6 are a migration window. Compile and unit tests are the gate there; avoid recording SQL goldens until query entrypoints use the collector.
- `MISSING_BASE_ROW_COUNT_FALLBACK = 100_000.0` deliberately matches the old unknown-table default to reduce plan churn while removing table-name heuristics.
- Provider failures are advisory: convert them to `StatsMissingReason`, log at debug/warn, and keep query execution unblocked.
- Do not add `engine`, `connector`, or `catalog_mgr` imports inside `src/sql/optimizer`.
- Do not replace the old name heuristic with a different name heuristic.
- Keep commit messages in English.
