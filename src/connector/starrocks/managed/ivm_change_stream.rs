use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::connector::iceberg::changes::{ChangeError, IcebergChangeBatch, plan_changes};
use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table;
use crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin;
use crate::engine::{QueryResult, StandaloneState};

// Compatibility wrapper for the older two-branch materialized change stream.
#[allow(dead_code)]
pub(crate) struct IvmChangeStream {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

#[allow(dead_code)]
pub(crate) struct MaterializedChanges {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

#[allow(dead_code)]
impl IvmChangeStream {
    pub(crate) fn from_materialized(changes: MaterializedChanges) -> Self {
        Self {
            previous_snapshot_id: changes.previous_snapshot_id,
            current_snapshot_id: changes.current_snapshot_id,
            inserts: changes.inserts,
            deletes: changes.deletes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inserts.row_count() == 0 && self.deletes.row_count() == 0
    }

    pub(crate) fn into_results(self) -> (QueryResult, QueryResult) {
        (self.inserts, self.deletes)
    }
}

/// Plan an `IcebergChangeBatch` for a single base table pinned to
/// `expected_current_snapshot_id`. Thin wrapper over Layer 1 `plan_changes`
/// with the to_snapshot_id set explicitly from the pin; the previous
/// current-snapshot post-check is no longer needed since plan_changes itself
/// now writes the requested-to into the batch.
pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    plan_changes(
        base_table,
        previous_snapshot_id,
        Some(expected_current_snapshot_id),
        pk_columns,
    )
}

/// Plan one `IcebergChangeBatch` per base table in `pin`, using
/// `last_refresh[base.fqn()]` as `from` and `pin.get(base)` as `to`.
/// Returns batches in iteration order of the pin (sorted by fqn).
///
/// Fails fast on the first base table that:
/// - is missing from `last_refresh` (no previous refresh recorded)
/// - cannot be loaded (catalog or io error)
/// - returns any `ChangeError` from `plan_changes`
#[allow(dead_code)]
pub(crate) fn plan_change_batches_for_pin(
    state: &Arc<StandaloneState>,
    pin: &RefreshSnapshotPin,
    last_refresh: &BTreeMap<String, i64>,
    pk_columns_by_base: &HashMap<IcebergTableRef, Vec<String>>,
) -> Result<Vec<(IcebergTableRef, IcebergChangeBatch)>, String> {
    let mut out = Vec::with_capacity(pin.len());
    for (fqn, pinned_snap) in pin.iter() {
        let base_ref = parse_fqn_to_iceberg_ref(fqn)?;
        let previous = last_refresh.get(fqn).copied().ok_or_else(|| {
            format!("plan_change_batches_for_pin: base table {fqn} missing from last_refresh")
        })?;
        let loaded = load_current_iceberg_base_table(state, &base_ref)?;
        let pk_default: Vec<String> = Vec::new();
        let pk_columns = pk_columns_by_base
            .iter()
            .find_map(|(base, columns)| (base == &base_ref).then_some(columns))
            .unwrap_or(&pk_default);
        let batch = plan_changes(&loaded.table, previous, Some(pinned_snap), pk_columns)
            .map_err(|e| format!("plan_change_batches_for_pin: {fqn}: {e}"))?;
        debug_assert_eq!(batch.current_snapshot_id, pinned_snap);
        out.push((base_ref, batch));
    }
    Ok(out)
}

#[allow(dead_code)]
fn parse_fqn_to_iceberg_ref(fqn: &str) -> Result<IcebergTableRef, String> {
    let parts: Vec<&str> = fqn.split('.').collect();
    if parts.len() != 3 {
        return Err(format!(
            "expected 3-part fqn '<catalog>.<namespace>.<table>', got '{fqn}'"
        ));
    }
    Ok(IcebergTableRef {
        catalog: parts[0].to_string(),
        namespace: parts[1].to_string(),
        table: parts[2].to_string(),
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_fqn_to_iceberg_ref_round_trip() {
        let parsed = super::parse_fqn_to_iceberg_ref("ice.sales.orders").expect("parse");
        assert_eq!(parsed.catalog, "ice");
        assert_eq!(parsed.namespace, "sales");
        assert_eq!(parsed.table, "orders");
        assert_eq!(parsed.fqn(), "ice.sales.orders");
    }

    #[test]
    fn parse_fqn_to_iceberg_ref_rejects_non_three_part() {
        assert!(super::parse_fqn_to_iceberg_ref("ice.sales").is_err());
        assert!(super::parse_fqn_to_iceberg_ref("ice.sales.orders.extra").is_err());
        assert!(super::parse_fqn_to_iceberg_ref("orders").is_err());
    }
}
