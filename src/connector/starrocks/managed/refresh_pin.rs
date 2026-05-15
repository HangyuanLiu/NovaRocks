//! Refresh-scoped snapshot pin for iceberg-backed materialized views.
//!
//! `RefreshSnapshotPin` captures, at the start of a refresh, the
//! `current_snapshot_id` and `uuid` of every base table. The pin is the
//! single source of truth for snapshot ids during the refresh:
//!
//! * `plan_changes` uses pin[base] as its `to_snapshot_id`
//! * `begin_mv_refresh_intent` records pin as the refresh target
//! * `update_managed_mv_refresh_summary` writes `last_refresh_snapshots = pin`
//!
//! For single-base MVs (the only shape currently supported by the DDL gate),
//! this guarantees delta computation and bookkeeping agree on the same
//! snapshot, even if the base table commits concurrently during the refresh.
//!
//! For multi-base MVs (future), the pin additionally guarantees cross-table
//! consistency: every base table is read at the snapshot it had at refresh
//! start, regardless of intervening external commits.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::engine::StandaloneState;

/// Per-refresh snapshot pin: each base table is pinned to the
/// `current_snapshot_id` it had at refresh entry time.
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct RefreshSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
}

#[allow(dead_code)]
impl RefreshSnapshotPin {
    /// Capture the current snapshot id and uuid for each base table.
    ///
    /// Fails fast if any base table has no current snapshot - refresh
    /// against an empty iceberg table is not a supported flow at this
    /// layer; the caller is expected to handle that earlier.
    pub(crate) fn capture(
        state: &Arc<StandaloneState>,
        base_refs: &[IcebergTableRef],
    ) -> Result<Self, String> {
        let mut pin = RefreshSnapshotPin::default();
        for base_ref in base_refs {
            let loaded =
                crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table(
                    state, base_ref,
                )?;
            let snapshot_id = loaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .ok_or_else(|| {
                    format!(
                        "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
                        base_ref.fqn()
                    )
                })?;
            pin.snapshots.insert(base_ref.fqn(), snapshot_id);
            pin.table_uuids
                .insert(base_ref.fqn(), loaded.table.metadata().uuid().to_string());
        }
        #[cfg(test)]
        invoke_after_capture_hook();
        Ok(pin)
    }

    pub(crate) fn get(&self, base: &IcebergTableRef) -> Option<i64> {
        self.snapshots.get(&base.fqn()).copied()
    }

    pub(crate) fn uuid(&self, base: &IcebergTableRef) -> Option<&str> {
        self.table_uuids.get(&base.fqn()).map(String::as_str)
    }

    pub(crate) fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.snapshots.iter().map(|(k, v)| (k.as_str(), *v))
    }

    pub(crate) fn to_snapshot_map(&self) -> BTreeMap<String, i64> {
        self.snapshots.clone()
    }

    pub(crate) fn to_table_uuid_map(&self) -> BTreeMap<String, String> {
        self.table_uuids.clone()
    }
}

#[cfg(test)]
pub(crate) type AfterCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
fn after_capture_hook_slot() -> &'static std::sync::Mutex<Option<AfterCaptureHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<AfterCaptureHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn invoke_after_capture_hook() {
    let hook = after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock")
        .clone();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
pub(crate) fn set_after_capture_hook(f: AfterCaptureHook) {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = Some(f);
}

#[cfg(test)]
pub(crate) fn clear_after_capture_hook() {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_get_and_iter_use_fqn_keys() {
        let mut pin = RefreshSnapshotPin::default();
        pin.snapshots.insert("ice.db.a".to_string(), 10);
        pin.snapshots.insert("ice.db.b".to_string(), 20);
        pin.table_uuids
            .insert("ice.db.a".to_string(), "uuid-a".to_string());
        pin.table_uuids
            .insert("ice.db.b".to_string(), "uuid-b".to_string());
        let a = IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "a".to_string(),
        };
        assert_eq!(pin.get(&a), Some(10));
        assert_eq!(pin.uuid(&a), Some("uuid-a"));
        assert_eq!(pin.len(), 2);
        assert!(!pin.is_empty());

        let snapshot_map = pin.to_snapshot_map();
        assert_eq!(snapshot_map.get("ice.db.a"), Some(&10));
        assert_eq!(snapshot_map.get("ice.db.b"), Some(&20));
    }

    #[test]
    fn after_capture_hook_round_trip() {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_for_hook = Arc::clone(&flag);
        set_after_capture_hook(Arc::new(move || {
            flag_for_hook.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        invoke_after_capture_hook();
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        clear_after_capture_hook();
        flag.store(false, std::sync::atomic::Ordering::SeqCst);
        invoke_after_capture_hook();
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }
}
