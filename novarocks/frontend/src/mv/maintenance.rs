// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Frontend-owned policy and admission for automatic MV maintenance.
//!
//! This module intentionally consumes only [`MvMaintenanceFacts`].  Provider
//! metadata stays behind the Core background-engine port, while the frontend
//! decides retry, capacity, and the durable lifecycle route for every action.
//! A host must obtain the per-MV activity gate *before* calling
//! [`MaintenanceCoordinator::try_begin`]; a queued gate ticket therefore never
//! consumes the independent maintenance concurrency budget.

pub use novarocks_mv_application::maintenance::MaintenanceCoordinatorConfig;
pub(crate) use novarocks_mv_application::maintenance::{
    AutomaticMaintenanceRunner, MaintenanceAdmission, MaintenanceExecutionReport,
};

#[cfg(test)]
use super::background::{MvBackgroundEngineErrorKind, MvMaintenanceFacts};
#[cfg(test)]
use novarocks_mv_application::maintenance::{
    AutomaticMaintenanceAction, MaintenanceActionKind, MaintenanceAttempt, MaintenanceSkipReason,
};
#[cfg(test)]
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget, OptimizeSubmission,
};

/// Process-local maintenance policy state.  It is intentionally non-durable:
/// recovery re-evaluates current provider facts and durable action state.
#[cfg(test)]
pub(crate) struct MaintenanceCoordinator {
    inner: novarocks_mv_application::maintenance::MaintenanceCoordinator,
}

#[cfg(test)]
impl MaintenanceCoordinator {
    pub(crate) fn new(config: MaintenanceCoordinatorConfig) -> Self {
        Self {
            inner: novarocks_mv_application::maintenance::MaintenanceCoordinator::new(config),
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn config(&self) -> &MaintenanceCoordinatorConfig {
        self.inner.config()
    }

    /// Admit work only after the caller has acquired the MV activity gate.
    /// This is what keeps a FIFO gate waiter from consuming a maintenance
    /// permit.  The returned attempt holds one permit until `run_attempt` or
    /// `cancel_attempt` is called.
    pub(crate) fn try_begin(
        &mut self,
        mv_id: i64,
        target: MaintenanceTarget,
        facts: &MvMaintenanceFacts,
        now_ms: i64,
    ) -> Result<MaintenanceAttempt, MaintenanceAdmission> {
        self.inner.try_begin(mv_id, target, facts, now_ms)
    }

    /// Execute external durable actions without holding the coordinator lock.
    /// The caller must subsequently call [`Self::finish_attempt`] while
    /// retaining the attempt's activity lease.  Splitting execution from
    /// admission is what makes `max_concurrent` a real parallelism bound
    /// rather than a mutex-shaped serial queue.
    pub(crate) fn execute_attempt(
        attempt: &MaintenanceAttempt,
        runner: &mut dyn AutomaticMaintenanceRunner,
    ) -> MaintenanceExecutionReport {
        novarocks_mv_application::maintenance::MaintenanceCoordinator::execute_attempt(
            attempt, runner,
        )
    }

    /// Persist the local policy outcome and release the permit after external
    /// execution completes. This must be called exactly once for every
    /// admitted attempt, including a cancellation before dispatch.
    pub(crate) fn finish_attempt(
        &mut self,
        attempt: MaintenanceAttempt,
        report: &MaintenanceExecutionReport,
        now_ms: i64,
    ) {
        self.inner.finish_attempt(attempt, report, now_ms);
    }

    #[cfg(test)]
    fn run_attempt(
        &mut self,
        attempt: MaintenanceAttempt,
        runner: &mut dyn AutomaticMaintenanceRunner,
        now_ms: i64,
    ) -> MaintenanceExecutionReport {
        let report = Self::execute_attempt(&attempt, runner);
        self.finish_attempt(attempt, &report, now_ms);
        report
    }

    /// End a pre-dispatch or shutdown-cancelled attempt without converting it
    /// into success, a metadata observation, or an ordinary retry.
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub(crate) fn cancel_attempt(&mut self, attempt: MaintenanceAttempt) {
        self.inner.cancel_attempt(attempt);
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.inner.active_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::maintenance::MvBackgroundEngineError;

    const NOW: i64 = 1_000_000_000;

    fn target(name: &str) -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: name.to_string(),
        }
    }

    fn facts() -> MvMaintenanceFacts {
        MvMaintenanceFacts {
            current_snapshot_id: Some(3),
            total_data_files: Some(200),
            max_compactable_data_files: Some(200),
            total_delete_files: Some(0),
            total_files_size_bytes: Some(200 * 1024 * 1024),
            oldest_snapshot_timestamp_ms: Some(1_000),
            snapshot_count: 3,
            ..MvMaintenanceFacts::default()
        }
    }

    struct Runner {
        transient_expire: bool,
        calls: Vec<MaintenanceActionKind>,
    }

    impl AutomaticMaintenanceRunner for Runner {
        fn expire_snapshots_durably(
            &mut self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
            self.calls.push(MaintenanceActionKind::Expire);
            if self.transient_expire {
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    "temporary metadata lease failure",
                ));
            }
            Ok(MaintenanceActionOutcome::ExpireSnapshots {
                deleted_data_files_count: None,
                deleted_position_delete_files_count: None,
                deleted_equality_delete_files_count: None,
                deleted_manifest_files_count: None,
                deleted_manifest_lists_count: None,
                deleted_statistics_files_count: None,
            })
        }

        fn rewrite_position_deletes_durably(
            &mut self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, MvBackgroundEngineError> {
            self.calls
                .push(MaintenanceActionKind::RewritePositionDeletes);
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: 1,
                added_delete_files_count: Some(1),
                rewritten_bytes_count: 1,
                added_bytes_count: 1,
            })
        }

        fn optimize_durably(
            &mut self,
            _target: MaintenanceTarget,
        ) -> Result<OptimizeSubmission, MvBackgroundEngineError> {
            self.calls.push(MaintenanceActionKind::Optimize);
            Ok(OptimizeSubmission::Submitted { job_id: 7 })
        }
    }

    #[test]
    fn policy_prefers_durable_optimize_and_suppresses_delete_rewrite() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let attempt = coordinator
            .try_begin(1, target("mv"), &facts(), NOW)
            .expect("admit maintenance");
        assert!(
            attempt
                .evaluation()
                .actions
                .contains(&AutomaticMaintenanceAction::Optimize)
        );
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceSkipReason::SuppressedByOptimize,
        )));
        let report = coordinator.run_attempt(
            attempt,
            &mut Runner {
                transient_expire: false,
                calls: Vec::new(),
            },
            NOW,
        );
        assert!(report.completed.contains(&MaintenanceActionKind::Optimize));
    }

    #[test]
    fn transient_failure_sets_backoff_without_direct_retry() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            compaction_min_data_files: 1_000,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv"), &facts(), NOW)
            .expect("admit first pass");
        let report = coordinator.run_attempt(
            first,
            &mut Runner {
                transient_expire: true,
                calls: Vec::new(),
            },
            NOW,
        );
        assert_eq!(
            report.failures,
            vec![(
                MaintenanceActionKind::Expire,
                MvBackgroundEngineErrorKind::TransientUnavailable,
            )]
        );

        let second = coordinator
            .try_begin(1, target("mv"), &facts(), NOW + 1)
            .expect("admit policy reevaluation after gate");
        assert!(second.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::FailureBackoff,
        )));
        coordinator.cancel_attempt(second);
    }

    #[test]
    fn unchanged_snapshot_is_a_noop_after_first_completed_pass() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            compaction_min_data_files: 1_000,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv"), &facts(), NOW)
            .expect("admit first pass");
        coordinator.run_attempt(
            first,
            &mut Runner {
                transient_expire: false,
                calls: Vec::new(),
            },
            NOW,
        );

        let mut current = facts();
        current.oldest_snapshot_timestamp_ms = Some(NOW);
        let second = coordinator
            .try_begin(1, target("mv"), &current, NOW + 1)
            .expect("admit second pass");
        assert!(second.evaluation().actions.is_empty());
        assert!(second.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::SnapshotUnchanged,
        )));
        let report = coordinator.run_attempt(
            second,
            &mut Runner {
                transient_expire: false,
                calls: Vec::new(),
            },
            NOW + 1,
        );
        assert!(report.is_noop());
    }

    #[test]
    fn disabled_typed_fact_skips_every_action() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.maintenance_enabled = Some(false);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().actions.is_empty());
        for kind in [
            MaintenanceActionKind::Expire,
            MaintenanceActionKind::RewritePositionDeletes,
            MaintenanceActionKind::Optimize,
        ] {
            assert!(
                attempt
                    .evaluation()
                    .skips
                    .contains(&(kind, MaintenanceSkipReason::Disabled)),
                "missing Disabled skip for {kind:?}"
            );
        }
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn explicitly_enabled_typed_fact_evaluates_normally() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.maintenance_enabled = Some(true);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(
            attempt
                .evaluation()
                .actions
                .contains(&AutomaticMaintenanceAction::Optimize)
        );
        assert!(
            !attempt
                .evaluation()
                .skips
                .iter()
                .any(|(_, reason)| *reason == MaintenanceSkipReason::Disabled)
        );
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn declared_min_snapshots_to_keep_blocks_expire() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.expire_min_snapshots_to_keep = Some(5);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::NothingToExpire,
        )));
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn declared_target_file_size_drives_the_small_file_ratio() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        // The fixture's 1 MiB average file is "small" against the 512 MiB
        // default, but not against a 1-byte declared target.
        let mut current = facts();
        current.target_file_size_bytes = Some(1);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::Optimize,
            MaintenanceSkipReason::BelowThreshold,
        )));
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn declared_expire_max_age_keeps_recent_snapshots() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        let mut current = facts();
        current.oldest_snapshot_timestamp_ms = Some(NOW - 1_000);
        current.expire_max_snapshot_age_ms = Some(10_000);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().skips.contains(&(
            MaintenanceActionKind::Expire,
            MaintenanceSkipReason::NothingToExpire,
        )));
        coordinator.cancel_attempt(attempt);

        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig::default());
        current.expire_max_snapshot_age_ms = Some(100);
        let attempt = coordinator
            .try_begin(1, target("mv"), &current, NOW)
            .expect("admit maintenance");
        assert!(attempt.evaluation().actions.iter().any(|action| matches!(
            action,
            AutomaticMaintenanceAction::ExpireSnapshots { retain_last, .. } if *retain_last == 1
        )));
        coordinator.cancel_attempt(attempt);
    }

    #[test]
    fn admitted_attempts_enforce_real_per_mv_capacity() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenanceCoordinatorConfig {
            max_concurrent: 1,
            ..MaintenanceCoordinatorConfig::default()
        });
        let first = coordinator
            .try_begin(1, target("mv_one"), &facts(), NOW)
            .expect("admit first MV");
        assert_eq!(coordinator.active_count(), 1);
        assert_eq!(
            coordinator
                .try_begin(2, target("mv_two"), &facts(), NOW)
                .expect_err("second MV must wait for capacity"),
            MaintenanceAdmission::AtCapacity
        );
        coordinator.cancel_attempt(first);
        assert_eq!(coordinator.active_count(), 0);
    }
}
