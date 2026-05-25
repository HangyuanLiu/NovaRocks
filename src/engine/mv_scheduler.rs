use crate::meta::repository::mv::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::novarocks_config::StandaloneServerConfig;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCoordinatorConfig {
    pub(crate) enabled: bool,
    pub(crate) tick_interval_ms: u64,
    pub(crate) max_concurrent_refreshes: usize,
    pub(crate) failure_backoff_ms: i64,
}

impl RefreshCoordinatorConfig {
    pub(crate) fn from_standalone_config(config: &StandaloneServerConfig) -> Self {
        Self {
            enabled: config.mv_refresh_scheduler_enabled,
            tick_interval_ms: config.mv_refresh_scheduler_interval_ms.max(1),
            max_concurrent_refreshes: config.mv_refresh_scheduler_max_concurrent.max(1),
            failure_backoff_ms: config.mv_refresh_scheduler_failure_backoff_ms.max(1),
        }
    }
}

impl Default for RefreshCoordinatorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tick_interval_ms: 30_000,
            max_concurrent_refreshes: 1,
            failure_backoff_ms: 60_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshTaskState {
    Pending,
    Running,
    Succeeded,
    FailedBackoff,
    BlockedRecovery,
    Paused,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCandidate {
    pub(crate) mv_id: i64,
    pub(crate) policy: StoredMvRefreshPolicy,
    pub(crate) state: RefreshTaskState,
}

#[derive(Debug)]
pub(crate) struct RefreshCoordinatorHandle {
    enabled: bool,
}

impl RefreshCoordinatorHandle {
    pub(crate) fn disabled() -> Self {
        Self { enabled: false }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }
}

pub(crate) fn start_refresh_coordinator_for_server(
    _engine: &crate::engine::StandaloneNovaRocks,
    config: RefreshCoordinatorConfig,
) -> RefreshCoordinatorHandle {
    if !config.enabled {
        return RefreshCoordinatorHandle::disabled();
    }
    RefreshCoordinatorHandle { enabled: true }
}

pub(crate) fn scan_refresh_candidates(
    definitions: &[StoredMvDefinition],
    _now_ms: i64,
) -> Vec<RefreshCandidate> {
    definitions
        .iter()
        .filter_map(|definition| {
            if definition.refresh_paused {
                return Some(RefreshCandidate {
                    mv_id: definition.mv_id,
                    policy: definition.refresh_policy.clone(),
                    state: RefreshTaskState::Paused,
                });
            }
            if matches!(definition.refresh_policy, StoredMvRefreshPolicy::Manual) {
                return None;
            }
            Some(RefreshCandidate {
                mv_id: definition.mv_id,
                policy: definition.refresh_policy.clone(),
                state: RefreshTaskState::Pending,
            })
        })
        .filter(|candidate| !matches!(candidate.state, RefreshTaskState::Paused))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv::{StoredMvDefinition, StoredMvRefreshPolicy};
    use std::collections::BTreeMap;

    fn test_definition(mv_id: i64, refresh_policy: StoredMvRefreshPolicy) -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id,
            select_sql: "SELECT 1".to_string(),
            base_table_refs: Vec::new(),
            primary_key_columns: Vec::new(),
            storage_engine: "managed_lake".to_string(),
            target_catalog: None,
            target_namespace: None,
            target_table: None,
            schema_contract: None,
            partition_spec: None,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: BTreeMap::new(),
            last_refresh_table_uuids: BTreeMap::new(),
            last_refreshed_iceberg_snapshot_id: None,
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: BTreeMap::new(),
            refresh_policy,
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    #[test]
    fn disabled_coordinator_handle_does_not_start_worker() {
        let handle = RefreshCoordinatorHandle::disabled();

        assert!(!handle.is_enabled());
    }

    #[test]
    fn scan_candidates_skips_manual_and_paused_mvs() {
        let now_ms = 1_000;
        let manual = test_definition(1, StoredMvRefreshPolicy::Manual);
        let mut paused = test_definition(2, StoredMvRefreshPolicy::AsyncOnChange);
        paused.refresh_paused = true;
        let async_mv = test_definition(3, StoredMvRefreshPolicy::AsyncOnChange);

        let candidates = scan_refresh_candidates(&[manual, paused, async_mv], now_ms);

        assert_eq!(
            candidates,
            vec![RefreshCandidate {
                mv_id: 3,
                policy: StoredMvRefreshPolicy::AsyncOnChange,
                state: RefreshTaskState::Pending,
            }]
        );
    }
}
