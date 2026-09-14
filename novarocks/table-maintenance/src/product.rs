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

//! The connector-neutral table-maintenance product state machine.
//!
//! Provider and Native query adapters retain opaque sessions and execute their
//! physical effects.  This module owns the business ordering around those
//! effects: target conflict rights, cleanup maturity, rewrite cohort/commit
//! transitions, and the interpretation of terminal provider facts.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use novarocks_state_store_api::StateStore;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::activity::{
    MaintenanceActivityBusy, MaintenanceActivityFamily, MaintenanceActivityPermit,
};
use crate::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
};
use crate::job_service::{OptimizeJobRuntime, OptimizeTargetCapturePort};
use crate::runtime::{JobHandle, MaintenanceJobState, TerminalError};
use crate::worker::{OptimizeJobAdmissionPort, OptimizeJobExecutionPort};
use crate::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget, OptimizeJob,
    OptimizeSubmission,
};

/// A provider-neutral rewrite intent.  SQL lowering remains outside this crate;
/// this value is the product's execution decision after lowering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RewriteIntent {
    DataFiles {
        rewrite_all: bool,
    },
    PositionDeletes {
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RewritePlanFacts {
    pub noop: bool,
    pub cohort_count: usize,
    pub input_bytes: u64,
}

/// Receipt facts proven by the provider. `None` remains unknown, never zero.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RewriteReceiptFacts {
    pub target_snapshot_id: Option<i64>,
    pub input_data_files: u64,
    pub input_delete_files: u64,
    pub output_data_files: Option<u64>,
    pub output_delete_files: Option<u64>,
    pub output_rows: Option<u64>,
}

/// Provider commit fact after a rewrite dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RewriteCommit {
    KnownCommitted { finalization_failed: Option<String> },
    KnownUncommitted { failure: String },
    CommitUnknown { failure: String },
}

/// Opaque provider/native session for one product-owned rewrite transition.
pub trait DistributedRewriteSession: Send {
    fn plan_facts(&self) -> RewritePlanFacts;
    fn execute_cohort(&mut self, ordinal: usize) -> Result<(), String>;
    fn commit(&mut self) -> Result<RewriteCommit, String>;
    fn finalize_committed(&mut self) -> Result<RewriteReceiptFacts, String>;
    fn abort(&mut self, reason: String) -> Result<(), String>;
}

/// One owned-ref fact eligible for the GC first-observation safety window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupOwnedRefFact {
    pub table_uuid: Uuid,
    pub ref_name: String,
    pub head_snapshot_id: i64,
    pub provenance_version: u16,
    pub provenance_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupCandidate {
    Object,
    OwnedRef(CleanupOwnedRefFact),
}

/// Final provider fact for cleanup.  The product, not the adapter, decides how
/// that fact maps to a statement failure and whether retry is safe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupTerminal {
    KnownCommitted { locations: Vec<String> },
    KnownUncommitted { failure: String },
    CommitUnknown { failure: String },
    KnownCommittedFinalizationFailed { failure: String },
}

/// Opaque provider session for one cleanup operation.
pub trait CleanupSession: Send {
    fn candidates(&self) -> &[CleanupCandidate];
    fn select_owned_refs(&mut self, candidate_indexes: &[usize]) -> Result<(), String>;
    fn execute(&mut self) -> Result<CleanupTerminal, String>;
}

/// The only adapter surface needed by the table-maintenance product.
///
/// Implementations may own provider leases, Native encoders, and query clients,
/// but return only typed effect facts.  They do not own activity gates, GC
/// maturity, worker lifecycle, or commit terminal interpretation.
pub trait TableMaintenanceEffectPort {
    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String>;
    fn execute_metadata(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String>;
    fn begin_rewrite<'a>(
        &'a self,
        target: &MaintenanceTarget,
        intent: RewriteIntent,
    ) -> Result<Box<dyn DistributedRewriteSession + 'a>, String>;
    fn begin_cleanup<'a>(
        &'a self,
        target: &MaintenanceTarget,
        older_than_ms: i64,
    ) -> Result<Box<dyn CleanupSession + 'a>, String>;
}

/// Single process-local business owner for every table-maintenance operation.
pub struct TableMaintenanceProduct {
    jobs: OptimizeJobRuntime,
    observations: Option<Arc<GcOwnedRefObservationAccelerator>>,
    safe_gc_age: Option<Duration>,
}

impl TableMaintenanceProduct {
    pub async fn open(
        durable: Option<(Arc<dyn StateStore>, StateStoreRunPolicy)>,
    ) -> Result<Self, String> {
        let observations = match durable {
            Some((store, policy)) => Some(Arc::new(
                GcOwnedRefObservationAccelerator::open(store, policy)
                    .await
                    .map_err(|error| {
                        format!("open table-maintenance GC owned-ref observation accelerator failed: {error}")
                    })?,
            )),
            None => None,
        };
        Ok(Self::new(observations))
    }

    pub fn new(observations: Option<Arc<GcOwnedRefObservationAccelerator>>) -> Self {
        Self {
            jobs: OptimizeJobRuntime::new(),
            observations,
            safe_gc_age: None,
        }
    }

    pub fn with_safe_gc_age(mut self, safe_gc_age: Duration) -> Self {
        self.safe_gc_age = Some(safe_gc_age);
        self
    }

    pub fn start(
        &self,
        runtime: &Handle,
        admission: Arc<dyn OptimizeJobAdmissionPort>,
        execution: Arc<dyn OptimizeJobExecutionPort>,
    ) -> Result<(), String> {
        self.jobs.start(runtime, admission, execution)
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.jobs.shutdown_until(deadline).await
    }

    pub fn request_shutdown_for_process_exit(&self) {
        self.jobs.request_shutdown_for_process_exit();
    }

    pub fn jobs(&self) -> &OptimizeJobRuntime {
        &self.jobs
    }

    pub fn acquire_activity(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        self.jobs.acquire_activity(target, family)
    }

    pub async fn submit_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
    ) -> Result<OptimizeSubmission, String> {
        self.jobs.submit_optimize(target, capture).await
    }

    pub async fn list_optimize(&self) -> Result<Vec<OptimizeJob>, String> {
        self.jobs.list().await
    }

    pub async fn wait_optimize(&self, handle: JobHandle) -> Result<MaintenanceJobState, String> {
        self.jobs.wait_for_completion(handle).await
    }

    /// Executes a user action after the SQL adapter has resolved its target.
    pub async fn execute_user_action<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        effects.reject_user_action_on_mv(request.target())?;
        self.execute_action(effects, request).await
    }

    /// Executes an automatic action through the same target gate and effect
    /// state machine as SQL.  It deliberately does not apply the SQL-only MV
    /// mutation guard.
    pub async fn execute_action<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        match request {
            MaintenanceActionRequest::RewriteDataFiles { target, .. } => {
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                self.execute_rewrite(
                    effects,
                    &target,
                    RewriteIntent::DataFiles { rewrite_all: true },
                )
            }
            MaintenanceActionRequest::RewritePositionDeleteFiles {
                target,
                options,
                where_clause,
            } => {
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                self.execute_rewrite(
                    effects,
                    &target,
                    rewrite_position_delete_intent(&options, where_clause.as_deref())?,
                )
            }
            MaintenanceActionRequest::RemoveOrphanFiles {
                target,
                older_than_ms,
            } => self.execute_cleanup(effects, target, older_than_ms).await,
            request => {
                let target = request.target().clone();
                let _permit = self
                    .acquire_activity(&target, MaintenanceActivityFamily::Metadata)
                    .map_err(|error| error.to_string())?;
                effects.execute_metadata(request)
            }
        }
    }

    fn execute_rewrite<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        target: &MaintenanceTarget,
        intent: RewriteIntent,
    ) -> Result<MaintenanceActionOutcome, String> {
        self.execute_rewrite_terminal(effects, target, intent)
            .map_err(|error| error.message)
    }

    pub fn execute_rewrite_terminal<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        target: &MaintenanceTarget,
        intent: RewriteIntent,
    ) -> Result<MaintenanceActionOutcome, TerminalError> {
        let mut session = effects
            .begin_rewrite(target, intent.clone())
            .map_err(TerminalError::failed)?;
        let plan = session.plan_facts();
        if plan.noop {
            return rewrite_outcome(intent, None, plan).map_err(TerminalError::failed);
        }
        for ordinal in 0..plan.cohort_count {
            if let Err(error) = session.execute_cohort(ordinal) {
                return abort_rewrite_terminal(session.as_mut(), error);
            }
        }
        match session.commit().map_err(TerminalError::failed)? {
            RewriteCommit::KnownCommitted {
                finalization_failed: Some(error),
            } => Err(TerminalError::known_committed_finalization_failed(format!(
                "distributed optimize committed but finalization failed: {error}"
            ))),
            RewriteCommit::KnownCommitted {
                finalization_failed: None,
            } => rewrite_outcome(
                intent,
                Some(
                    session
                        .finalize_committed()
                        .map_err(TerminalError::failed)?,
                ),
                plan,
            )
            .map_err(TerminalError::failed),
            RewriteCommit::KnownUncommitted { failure } => {
                let message = format!("distributed rewrite commit was not applied: {failure}");
                abort_rewrite_known_uncommitted(session.as_mut(), message)
            }
            RewriteCommit::CommitUnknown { failure } => {
                Err(TerminalError::commit_unknown(format!(
                    "distributed rewrite commit outcome is unknown: {failure}; do not retry automatically"
                )))
            }
        }
    }

    async fn execute_cleanup<P: TableMaintenanceEffectPort + ?Sized>(
        &self,
        effects: &P,
        target: MaintenanceTarget,
        older_than_ms: i64,
    ) -> Result<MaintenanceActionOutcome, String> {
        let _permit = self
            .acquire_activity(&target, MaintenanceActivityFamily::Cleanup)
            .map_err(|error| error.to_string())?;
        let (now_ms, safe_gc_age_ms) = self.gc_timing(older_than_ms)?;
        let mut session = effects.begin_cleanup(&target, older_than_ms)?;
        let candidates = session.candidates();
        let has_owned = candidates
            .iter()
            .any(|candidate| matches!(candidate, CleanupCandidate::OwnedRef(_)));
        let has_objects = candidates
            .iter()
            .any(|candidate| matches!(candidate, CleanupCandidate::Object));
        if has_owned && has_objects {
            return Err("cleanup discovery mixed owned-ref and object candidates".to_string());
        }
        if has_owned {
            let observations = self.observations.as_ref().ok_or_else(|| {
                "orphan cleanup is unavailable because the GC observation accelerator is not configured"
                    .to_string()
            })?;
            let indexes =
                mature_owned_ref_indexes(observations, candidates, now_ms, safe_gc_age_ms).await?;
            if indexes.is_empty() {
                return Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
                    orphan_file_locations: Vec::new(),
                });
            }
            session.select_owned_refs(&indexes)?;
        }
        match session.execute()? {
            CleanupTerminal::KnownCommitted { locations } => {
                Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
                    orphan_file_locations: locations,
                })
            }
            CleanupTerminal::KnownUncommitted { failure } => {
                Err(format!("orphan cleanup was not applied: {failure}"))
            }
            CleanupTerminal::CommitUnknown { failure } => Err(format!(
                "orphan cleanup dispatch outcome is unknown: {failure}; do not retry automatically"
            )),
            CleanupTerminal::KnownCommittedFinalizationFailed { failure } => Err(format!(
                "orphan cleanup committed but finalization failed: {failure}"
            )),
        }
    }

    fn gc_timing(&self, older_than_ms: i64) -> Result<(i64, i64), String> {
        let safe_gc_age = self.safe_gc_age.ok_or_else(|| {
            "orphan cleanup is unsupported until the lake publication runtime policy is installed"
                .to_string()
        })?;
        let now_ms = now_unix_millis()?;
        let safe_gc_age_ms = i64::try_from(safe_gc_age.as_millis())
            .map_err(|_| "orphan cleanup is unsupported because safe GC age exceeds i64")?;
        let cutoff = now_ms.checked_sub(safe_gc_age_ms).ok_or_else(|| {
            "orphan cleanup is unsupported because safe GC cutoff underflows".to_string()
        })?;
        if older_than_ms <= 0 || older_than_ms > cutoff {
            return Err(format!(
                "orphan cleanup cutoff {older_than_ms} is newer than the shared safe GC boundary {cutoff}"
            ));
        }
        Ok((now_ms, safe_gc_age_ms))
    }
}

async fn mature_owned_ref_indexes(
    observations: &GcOwnedRefObservationAccelerator,
    candidates: &[CleanupCandidate],
    now_ms: i64,
    safe_gc_age_ms: i64,
) -> Result<Vec<usize>, String> {
    let mut indexes = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        let CleanupCandidate::OwnedRef(candidate) = candidate else {
            continue;
        };
        let observation = GcOwnedRefObservation::try_new(
            candidate.table_uuid,
            candidate.ref_name.clone(),
            candidate.head_snapshot_id,
            candidate.provenance_version,
            candidate.provenance_digest,
            now_ms,
        )
        .map_err(|error| format!("build GC owned-ref observation failed: {error}"))?;
        if matches!(
            observations
                .observe(observation, now_ms, safe_gc_age_ms)
                .await
                .map_err(|error| format!("record GC owned-ref observation failed: {error}"))?,
            GcOwnedRefObservationDecision::Mature { .. }
        ) {
            indexes.push(index);
        }
    }
    Ok(indexes)
}

fn abort_rewrite_terminal(
    session: &mut dyn DistributedRewriteSession,
    error: String,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    match session.abort(error.clone()) {
        Ok(()) => Err(TerminalError::failed(error)),
        Err(abort) => Err(TerminalError::failed(format!(
            "{error}; distributed rewrite abort failed: {abort}"
        ))),
    }
}

fn abort_rewrite_known_uncommitted(
    session: &mut dyn DistributedRewriteSession,
    error: String,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    match session.abort(error.clone()) {
        Ok(()) => Err(TerminalError::known_uncommitted(error)),
        Err(abort) => Err(TerminalError::known_uncommitted(format!(
            "{error}; distributed rewrite abort failed: {abort}"
        ))),
    }
}

fn rewrite_outcome(
    intent: RewriteIntent,
    receipt: Option<RewriteReceiptFacts>,
    plan: RewritePlanFacts,
) -> Result<MaintenanceActionOutcome, String> {
    let receipt = receipt.unwrap_or_default();
    match intent {
        RewriteIntent::DataFiles { .. } => Ok(MaintenanceActionOutcome::RewriteDataFiles {
            target_snapshot_id: receipt.target_snapshot_id,
            rewritten_data_files_count: i32::try_from(receipt.input_data_files)
                .map_err(|_| "distributed rewrite input data file count exceeds i32")?,
            added_data_files_count: receipt
                .output_data_files
                .map(|count| {
                    i32::try_from(count).map_err(|_| {
                        "distributed rewrite output data file count exceeds i32".to_string()
                    })
                })
                .transpose()?,
            added_delete_files_count: receipt
                .output_delete_files
                .map(|count| {
                    i32::try_from(count).map_err(|_| {
                        "distributed rewrite output delete file count exceeds i32".to_string()
                    })
                })
                .transpose()?,
            rewritten_bytes_count: i64::try_from(plan.input_bytes)
                .map_err(|_| "distributed rewrite input byte count exceeds i64")?,
            failed_data_files_count: 0,
            removed_delete_files_count: i32::try_from(receipt.input_delete_files)
                .map_err(|_| "distributed rewrite input delete file count exceeds i32")?,
            output_record_count: receipt
                .output_rows
                .map(|count| {
                    i64::try_from(count)
                        .map_err(|_| "distributed rewrite output row count exceeds i64".to_string())
                })
                .transpose()?,
        }),
        RewriteIntent::PositionDeletes { .. } => {
            Ok(MaintenanceActionOutcome::RewritePositionDeleteFiles {
                rewritten_delete_files_count: i32::try_from(receipt.input_delete_files)
                    .map_err(|_| "distributed rewrite input delete file count exceeds i32")?,
                added_delete_files_count: receipt
                    .output_delete_files
                    .map(|count| {
                        i32::try_from(count).map_err(|_| {
                            "distributed rewrite output delete file count exceeds i32".to_string()
                        })
                    })
                    .transpose()?,
                rewritten_bytes_count: i64::try_from(plan.input_bytes)
                    .map_err(|_| "distributed rewrite input byte count exceeds i64")?,
                added_bytes_count: 0,
            })
        }
    }
}

fn rewrite_position_delete_intent(
    options: &std::collections::BTreeMap<String, String>,
    where_clause: Option<&str>,
) -> Result<RewriteIntent, String> {
    if where_clause.is_some() {
        return Err(
            "rewrite_position_delete_files where is not supported in NovaRocks yet".to_string(),
        );
    }
    let mut rewrite_all = false;
    let mut min_input_files = None;
    for (key, value) in options {
        match key.as_str() {
            "rewrite-all" if value.eq_ignore_ascii_case("true") => rewrite_all = true,
            "rewrite-all" => {
                return Err(
                    "rewrite_position_delete_files option `rewrite-all` must be `true`".to_string(),
                );
            }
            "min-input-files" => {
                min_input_files = Some(value.parse::<u32>().map_err(|_| {
                    "rewrite_position_delete_files option `min-input-files` must be a positive integer".to_string()
                })?);
                if min_input_files == Some(0) {
                    return Err("rewrite_position_delete_files option `min-input-files` must be a positive integer".to_string());
                }
            }
            other => {
                return Err(format!(
                    "unsupported rewrite_position_delete_files option `{other}`"
                ));
            }
        }
    }
    Ok(RewriteIntent::PositionDeletes {
        rewrite_all,
        min_input_files,
    })
}

fn now_unix_millis() -> Result<i64, String> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "orphan cleanup is unsupported because wall clock is unsafe")?
            .as_millis(),
    )
    .map_err(|_| "orphan cleanup is unsupported because wall clock exceeds i64".to_string())
}

trait ActionTarget {
    fn target(&self) -> &MaintenanceTarget;
}

impl ActionTarget for MaintenanceActionRequest {
    fn target(&self) -> &MaintenanceTarget {
        match self {
            Self::RewriteDataFiles { target, .. }
            | Self::RewriteManifests { target, .. }
            | Self::ExpireSnapshots { target, .. }
            | Self::RemoveOrphanFiles { target, .. }
            | Self::RewritePositionDeleteFiles { target, .. } => target,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MaintenanceJobState;

    struct FixedRewritePort {
        commit: RewriteCommit,
    }

    struct FixedRewriteSession {
        commit: RewriteCommit,
        aborted: bool,
    }

    impl DistributedRewriteSession for FixedRewriteSession {
        fn plan_facts(&self) -> RewritePlanFacts {
            RewritePlanFacts {
                noop: false,
                cohort_count: 1,
                input_bytes: 0,
            }
        }

        fn execute_cohort(&mut self, _ordinal: usize) -> Result<(), String> {
            Ok(())
        }

        fn commit(&mut self) -> Result<RewriteCommit, String> {
            Ok(self.commit.clone())
        }

        fn finalize_committed(&mut self) -> Result<RewriteReceiptFacts, String> {
            Ok(RewriteReceiptFacts::default())
        }

        fn abort(&mut self, _reason: String) -> Result<(), String> {
            self.aborted = true;
            Ok(())
        }
    }

    impl TableMaintenanceEffectPort for FixedRewritePort {
        fn reject_user_action_on_mv(&self, _target: &MaintenanceTarget) -> Result<(), String> {
            Ok(())
        }

        fn execute_metadata(
            &self,
            _request: MaintenanceActionRequest,
        ) -> Result<MaintenanceActionOutcome, String> {
            unreachable!("rewrite test never executes metadata")
        }

        fn begin_rewrite<'a>(
            &'a self,
            _target: &MaintenanceTarget,
            _intent: RewriteIntent,
        ) -> Result<Box<dyn DistributedRewriteSession + 'a>, String> {
            Ok(Box::new(FixedRewriteSession {
                commit: self.commit.clone(),
                aborted: false,
            }))
        }

        fn begin_cleanup<'a>(
            &'a self,
            _target: &MaintenanceTarget,
            _older_than_ms: i64,
        ) -> Result<Box<dyn CleanupSession + 'a>, String> {
            unreachable!("rewrite test never starts cleanup")
        }
    }

    fn target() -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "catalog".to_string(),
            namespace: "namespace".to_string(),
            table: "table".to_string(),
        }
    }

    #[test]
    fn rewrite_unknown_output_is_not_converted_to_zero() {
        let outcome = rewrite_outcome(
            RewriteIntent::DataFiles { rewrite_all: true },
            Some(RewriteReceiptFacts {
                output_data_files: None,
                output_delete_files: Some(0),
                output_rows: None,
                ..RewriteReceiptFacts::default()
            }),
            RewritePlanFacts {
                noop: false,
                cohort_count: 1,
                input_bytes: 0,
            },
        )
        .expect("receipt is valid");
        let MaintenanceActionOutcome::RewriteDataFiles {
            added_data_files_count,
            added_delete_files_count,
            output_record_count,
            ..
        } = outcome
        else {
            panic!("expected data rewrite")
        };
        assert_eq!(added_data_files_count, None);
        assert_eq!(added_delete_files_count, Some(0));
        assert_eq!(output_record_count, None);
    }

    #[test]
    fn position_delete_options_reject_zero_minimum() {
        let mut options = std::collections::BTreeMap::new();
        options.insert("min-input-files".to_string(), "0".to_string());
        assert!(rewrite_position_delete_intent(&options, None).is_err());
    }

    #[test]
    fn rewrite_commit_unknown_is_a_product_terminal_not_a_retry() {
        let product = TableMaintenanceProduct::new(None);
        let error = product
            .execute_rewrite_terminal(
                &FixedRewritePort {
                    commit: RewriteCommit::CommitUnknown {
                        failure: "receipt lost".to_string(),
                    },
                },
                &target(),
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .expect_err("unknown commit cannot be reported as success");
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        assert!(error.message.contains("do not retry automatically"));
    }

    #[test]
    fn known_committed_finalization_failure_keeps_its_terminal_fact() {
        let product = TableMaintenanceProduct::new(None);
        let error = product
            .execute_rewrite_terminal(
                &FixedRewritePort {
                    commit: RewriteCommit::KnownCommitted {
                        finalization_failed: Some("projection unavailable".to_string()),
                    },
                },
                &target(),
                RewriteIntent::DataFiles { rewrite_all: true },
            )
            .expect_err("known committed finalization failure is not plain failure");
        assert_eq!(
            error.state,
            MaintenanceJobState::KnownCommittedFinalizationFailed
        );
        assert!(error.message.contains("projection unavailable"));
    }
}
