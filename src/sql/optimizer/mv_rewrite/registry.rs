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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv::StoredMvDefinition;
    use std::collections::BTreeMap;

    fn mk(
        mv_id: i64,
        base: &str,
        engine: &str,
        refreshed: bool,
        in_progress: bool,
    ) -> StoredMvDefinition {
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
