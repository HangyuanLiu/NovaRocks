use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::meta::repository::mv::{
    StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest,
};
use crate::novarocks_config::StandaloneServerConfig;
use crate::sql::parser::ast::{ObjectName, RefreshMaterializedViewStmt};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshTaskReason {
    Manual,
    Periodic,
    SnapshotChange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCandidate {
    pub(crate) mv_id: i64,
    pub(crate) policy: StoredMvRefreshPolicy,
    pub(crate) state: RefreshTaskState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PeriodicRefreshDecision {
    pub(crate) mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RefreshQueueEntry {
    mv_id: i64,
    reason: RefreshTaskReason,
}

pub(crate) trait RefreshExecutor {
    fn execute_refresh(&mut self, mv_id: i64) -> Result<(), String>;
}

#[derive(Debug)]
pub(crate) struct RefreshCoordinator {
    config: RefreshCoordinatorConfig,
    queue: VecDeque<RefreshQueueEntry>,
    queued_mv_ids: BTreeSet<i64>,
    running_mv_ids: BTreeSet<i64>,
    states: BTreeMap<i64, RefreshTaskState>,
}

impl RefreshCoordinator {
    fn new(config: RefreshCoordinatorConfig) -> Self {
        Self {
            config,
            queue: VecDeque::new(),
            queued_mv_ids: BTreeSet::new(),
            running_mv_ids: BTreeSet::new(),
            states: BTreeMap::new(),
        }
    }

    pub(crate) fn enqueue_refresh(&mut self, mv_id: i64, reason: RefreshTaskReason) -> bool {
        if self.queued_mv_ids.contains(&mv_id) || self.running_mv_ids.contains(&mv_id) {
            return false;
        }
        self.queue.push_back(RefreshQueueEntry { mv_id, reason });
        self.queued_mv_ids.insert(mv_id);
        self.states.insert(mv_id, RefreshTaskState::Pending);
        true
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn state_for_mv(&self, mv_id: i64) -> Option<RefreshTaskState> {
        self.states.get(&mv_id).copied()
    }

    pub(crate) fn drain_ready<E: RefreshExecutor>(
        &mut self,
        executor: &mut E,
        _now_ms: i64,
    ) -> Result<(), String> {
        let capacity = self
            .config
            .max_concurrent_refreshes
            .saturating_sub(self.running_mv_ids.len());
        for _ in 0..capacity {
            let Some(entry) = self.queue.pop_front() else {
                break;
            };
            self.queued_mv_ids.remove(&entry.mv_id);
            if self.running_mv_ids.contains(&entry.mv_id) {
                continue;
            }
            let _reason = entry.reason;
            self.running_mv_ids.insert(entry.mv_id);
            self.states.insert(entry.mv_id, RefreshTaskState::Running);
            let result = executor.execute_refresh(entry.mv_id);
            self.running_mv_ids.remove(&entry.mv_id);
            match result {
                Ok(()) => {
                    self.states.insert(entry.mv_id, RefreshTaskState::Succeeded);
                }
                Err(_) => {
                    self.states
                        .insert(entry.mv_id, RefreshTaskState::FailedBackoff);
                }
            }
        }
        Ok(())
    }

    fn tick_state(
        &mut self,
        state: &Arc<crate::engine::StandaloneState>,
        now_ms: i64,
    ) -> Result<(), String> {
        let definitions = load_scheduler_definitions(state)?;
        for decision in plan_periodic_refreshes(&definitions, now_ms) {
            self.enqueue_refresh(decision.mv_id, RefreshTaskReason::Periodic);
        }
        let capacity = self
            .config
            .max_concurrent_refreshes
            .saturating_sub(self.running_mv_ids.len());
        let mut executor = MetadataRefreshExecutor::new(Arc::clone(state));
        for _ in 0..capacity {
            let Some(entry) = self.queue.pop_front() else {
                break;
            };
            self.queued_mv_ids.remove(&entry.mv_id);
            if self.running_mv_ids.contains(&entry.mv_id) {
                continue;
            }
            let _reason = entry.reason;
            self.running_mv_ids.insert(entry.mv_id);
            self.states.insert(entry.mv_id, RefreshTaskState::Running);
            let result = executor.execute_refresh(entry.mv_id);
            self.running_mv_ids.remove(&entry.mv_id);
            match result {
                Ok(()) => {
                    self.states.insert(entry.mv_id, RefreshTaskState::Succeeded);
                    record_scheduler_success_metadata(state, entry.mv_id, now_ms)?;
                }
                Err(err) => {
                    self.states
                        .insert(entry.mv_id, RefreshTaskState::FailedBackoff);
                    record_scheduler_failure_metadata(
                        state,
                        entry.mv_id,
                        &err,
                        now_ms,
                        self.config.failure_backoff_ms,
                    )?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn plan_periodic_refreshes(
    definitions: &[StoredMvDefinition],
    now_ms: i64,
) -> Vec<PeriodicRefreshDecision> {
    definitions
        .iter()
        .filter(|definition| {
            matches!(
                definition.refresh_policy,
                StoredMvRefreshPolicy::AsyncInterval
            ) && !definition.refresh_paused
                && definition.refresh_interval_ms.is_some()
                && definition
                    .next_refresh_after_ms
                    .map(|next| next <= now_ms)
                    .unwrap_or(true)
        })
        .map(|definition| PeriodicRefreshDecision {
            mv_id: definition.mv_id,
        })
        .collect()
}

pub(crate) fn metadata_update_after_success(
    definition: &StoredMvDefinition,
    now_ms: i64,
) -> Result<UpdateMvRefreshMetadataRequest, String> {
    let next_refresh_after_ms = match definition.refresh_policy {
        StoredMvRefreshPolicy::AsyncInterval => {
            let interval = definition.refresh_interval_ms.ok_or_else(|| {
                format!(
                    "MV definition {} has ASYNC_INTERVAL policy without interval",
                    definition.mv_id
                )
            })?;
            Some(now_ms.saturating_add(interval))
        }
        _ => definition.next_refresh_after_ms,
    };
    Ok(UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy: definition.refresh_policy.clone(),
        refresh_paused: definition.refresh_paused,
        refresh_interval_ms: definition.refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms,
    })
}

pub(crate) fn metadata_update_after_failure(
    definition: &StoredMvDefinition,
    err: &str,
    now_ms: i64,
    failure_backoff_ms: i64,
) -> UpdateMvRefreshMetadataRequest {
    UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy: definition.refresh_policy.clone(),
        refresh_paused: definition.refresh_paused,
        refresh_interval_ms: definition.refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: Some(err.to_string()),
        next_refresh_after_ms: Some(now_ms.saturating_add(failure_backoff_ms.max(1))),
    }
}

fn load_scheduler_definitions(
    state: &Arc<crate::engine::StandaloneState>,
) -> Result<Vec<StoredMvDefinition>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(Vec::new());
    };
    let txn = provider
        .begin_read()
        .map_err(|e| format!("open MV scheduler read transaction failed: {e}"))?;
    state
        .mv_repo
        .list_definitions(txn.as_ref())
        .map_err(|e| format!("list MV definitions failed: {e}"))
}

fn record_scheduler_success_metadata(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    now_ms: i64,
) -> Result<(), String> {
    update_scheduler_metadata(state, mv_id, |definition| {
        metadata_update_after_success(definition, now_ms)
    })
}

fn record_scheduler_failure_metadata(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    err: &str,
    now_ms: i64,
    failure_backoff_ms: i64,
) -> Result<(), String> {
    update_scheduler_metadata(state, mv_id, |definition| {
        Ok(metadata_update_after_failure(
            definition,
            err,
            now_ms,
            failure_backoff_ms,
        ))
    })
}

fn update_scheduler_metadata<F>(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
    build_request: F,
) -> Result<(), String>
where
    F: FnOnce(&StoredMvDefinition) -> Result<UpdateMvRefreshMetadataRequest, String>,
{
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "MV refresh scheduler requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("update MV refresh scheduler metadata")
        .map_err(|e| format!("open MV scheduler write transaction failed: {e}"))?;
    let definition = state
        .mv_repo
        .load_by_id(txn.as_ref(), mv_id)
        .map_err(|e| format!("load MV definition failed: {e}"))?
        .ok_or_else(|| format!("MV definition {mv_id} not found"))?;
    let req = build_request(&definition)?;
    state
        .mv_repo
        .update_refresh_metadata(txn.as_mut(), req)
        .map_err(|e| format!("update MV scheduler metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit MV scheduler metadata failed: {e}"))?;
    Ok(())
}

pub(crate) struct MetadataRefreshExecutor {
    state: Arc<crate::engine::StandaloneState>,
}

impl MetadataRefreshExecutor {
    pub(crate) fn new(state: Arc<crate::engine::StandaloneState>) -> Self {
        Self { state }
    }
}

impl RefreshExecutor for MetadataRefreshExecutor {
    fn execute_refresh(&mut self, mv_id: i64) -> Result<(), String> {
        let target = load_refresh_execution_target(&self.state, mv_id)?;
        crate::engine::mv_flow::refresh_mv(
            &self.state,
            target.current_catalog.as_deref(),
            &target.current_database,
            &RefreshMaterializedViewStmt {
                name: target.name,
                full: false,
            },
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RefreshExecutionTarget {
    current_catalog: Option<String>,
    current_database: String,
    name: ObjectName,
}

fn load_refresh_execution_target(
    state: &Arc<crate::engine::StandaloneState>,
    mv_id: i64,
) -> Result<RefreshExecutionTarget, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "MV refresh scheduler requires metadata provider".to_string())?;
    let txn = provider
        .begin_read()
        .map_err(|e| format!("open MV refresh scheduler read transaction failed: {e}"))?;
    let definition = state
        .mv_repo
        .load_by_id(txn.as_ref(), mv_id)
        .map_err(|e| format!("load MV definition failed: {e}"))?
        .ok_or_else(|| format!("MV definition {mv_id} not found"))?;
    refresh_execution_target_for_definition(state, txn.as_ref(), &definition)
}

fn refresh_execution_target_for_definition(
    state: &Arc<crate::engine::StandaloneState>,
    txn: &dyn crate::meta::MetaReadTxn,
    definition: &StoredMvDefinition,
) -> Result<RefreshExecutionTarget, String> {
    match (
        definition.target_catalog.as_ref(),
        definition.target_namespace.as_ref(),
        definition.target_table.as_ref(),
    ) {
        (Some(catalog), Some(namespace), Some(table)) => {
            return Ok(RefreshExecutionTarget {
                current_catalog: Some(catalog.clone()),
                current_database: namespace.clone(),
                name: ObjectName {
                    parts: vec![table.clone()],
                },
            });
        }
        (None, None, None) => {}
        _ => {
            return Err(format!(
                "MV definition {} has incomplete target metadata",
                definition.mv_id
            ));
        }
    }

    let table = state
        .managed_repo
        .load_table(txn, definition.mv_id)
        .map_err(|e| format!("load managed MV table failed: {e}"))?
        .ok_or_else(|| format!("managed MV table {} not found", definition.mv_id))?;
    let database = state
        .managed_repo
        .load_database(txn, table.db_id)
        .map_err(|e| format!("load managed MV database failed: {e}"))?
        .ok_or_else(|| format!("managed database {} not found", table.db_id))?;
    Ok(RefreshExecutionTarget {
        current_catalog: None,
        current_database: database.name,
        name: ObjectName {
            parts: vec![table.name],
        },
    })
}

pub(crate) struct RefreshCoordinatorHandle {
    enabled: bool,
    stop_tx: Option<Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl RefreshCoordinatorHandle {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            stop_tx: None,
            worker: None,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Drop for RefreshCoordinatorHandle {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) fn start_refresh_coordinator_for_server(
    engine: &crate::engine::StandaloneNovaRocks,
    config: RefreshCoordinatorConfig,
) -> RefreshCoordinatorHandle {
    if !config.enabled {
        return RefreshCoordinatorHandle::disabled();
    }
    let state = Arc::clone(&engine.inner);
    let worker_config = config.clone();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::Builder::new()
        .name("novarocks-mv-refresh-scheduler".to_string())
        .spawn(move || {
            let mut coordinator = RefreshCoordinator::new(worker_config.clone());
            loop {
                if let Err(err) = coordinator.tick_state(&state, current_time_ms()) {
                    tracing::warn!(error = %err, "MV refresh scheduler tick failed");
                }
                match stop_rx.recv_timeout(Duration::from_millis(worker_config.tick_interval_ms)) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        });
    match worker {
        Ok(worker) => RefreshCoordinatorHandle {
            enabled: true,
            stop_tx: Some(stop_tx),
            worker: Some(worker),
        },
        Err(err) => {
            tracing::warn!(error = %err, "failed to start MV refresh scheduler worker");
            RefreshCoordinatorHandle::disabled()
        }
    }
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
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

    #[derive(Default)]
    struct RecordingRefreshExecutor {
        executed_mv_ids: Vec<i64>,
        failure: Option<String>,
    }

    impl RecordingRefreshExecutor {
        fn failing(message: &str) -> Self {
            Self {
                executed_mv_ids: Vec::new(),
                failure: Some(message.to_string()),
            }
        }

        fn executed_mv_ids(&self) -> Vec<i64> {
            self.executed_mv_ids.clone()
        }
    }

    impl RefreshExecutor for RecordingRefreshExecutor {
        fn execute_refresh(&mut self, mv_id: i64) -> Result<(), String> {
            self.executed_mv_ids.push(mv_id);
            match self.failure.as_ref() {
                Some(message) => Err(message.clone()),
                None => Ok(()),
            }
        }
    }

    impl RefreshCoordinatorConfig {
        fn enabled_for_test() -> Self {
            Self {
                enabled: true,
                ..Self::default()
            }
        }
    }

    impl RefreshCoordinator {
        fn new_for_test(config: RefreshCoordinatorConfig) -> Self {
            Self::new(config)
        }

        fn drain_ready_for_test<E: RefreshExecutor>(
            &mut self,
            executor: &mut E,
            now_ms: i64,
        ) -> Result<(), String> {
            self.drain_ready(executor, now_ms)
        }
    }

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

    #[test]
    fn enqueue_refresh_deduplicates_same_mv_until_drained() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());

        assert!(coordinator.enqueue_refresh(7, RefreshTaskReason::Manual));
        assert!(!coordinator.enqueue_refresh(7, RefreshTaskReason::Manual));
        assert_eq!(coordinator.pending_len(), 1);
    }

    #[test]
    fn drain_once_executes_manual_refresh_and_records_success() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
        let mut executor = RecordingRefreshExecutor::default();

        coordinator
            .drain_ready_for_test(&mut executor, 1_000)
            .expect("drain succeeds");

        assert_eq!(executor.executed_mv_ids(), vec![7]);
        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::Succeeded)
        );
    }

    #[test]
    fn drain_once_records_failure_backoff() {
        let mut coordinator =
            RefreshCoordinator::new_for_test(RefreshCoordinatorConfig::enabled_for_test());
        coordinator.enqueue_refresh(7, RefreshTaskReason::Manual);
        let mut executor = RecordingRefreshExecutor::failing("refresh failed");

        coordinator
            .drain_ready_for_test(&mut executor, 1_000)
            .expect("drain succeeds");

        assert_eq!(
            coordinator.state_for_mv(7),
            Some(RefreshTaskState::FailedBackoff)
        );
    }

    #[test]
    fn periodic_policy_enqueues_only_when_due() {
        let mut due = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);
        due.refresh_interval_ms = Some(10_000);
        due.next_refresh_after_ms = Some(1_000);
        let mut future = test_definition(2, StoredMvRefreshPolicy::AsyncInterval);
        future.refresh_interval_ms = Some(10_000);
        future.next_refresh_after_ms = Some(2_000);

        let decisions = plan_periodic_refreshes(&[due, future], 1_500);

        assert_eq!(
            decisions
                .into_iter()
                .map(|decision| decision.mv_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn periodic_success_sets_next_refresh_after() {
        let mut definition = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);
        definition.refresh_interval_ms = Some(10_000);

        let req = metadata_update_after_success(&definition, 1_500).expect("success metadata");

        assert_eq!(req.last_scheduler_error, None);
        assert_eq!(req.next_refresh_after_ms, Some(11_500));
    }

    #[test]
    fn periodic_failure_sets_backoff_and_preserves_policy() {
        let definition = test_definition(1, StoredMvRefreshPolicy::AsyncInterval);

        let req = metadata_update_after_failure(&definition, "boom", 1_500, 30_000);

        assert_eq!(req.last_scheduler_error, Some("boom".to_string()));
        assert_eq!(req.next_refresh_after_ms, Some(31_500));
        assert_eq!(req.refresh_policy, StoredMvRefreshPolicy::AsyncInterval);
    }
}
