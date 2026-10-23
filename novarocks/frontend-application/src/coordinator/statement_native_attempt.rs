// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original statement Native attempt, borrowed in foreground and returned
//! to its pre-registered process supervision slot at the foreground boundary.
//! Publication and the statement's immutable business verdict stay with their
//! original write/COW/CTAS owner. A drain budget only limits foreground waiting.

use super::{
    execution::{FinalTaskInfoCollector, RootResultPolls},
    internal_result::{InternalDomainFailure, InternalDomainSlot},
    query_registry::ActiveQueryGuard,
};
use crate::{
    query_execution::{
        artifact::TaskExecutionPreparedQuery,
        internal_result_cpu::{InternalResultCpuJob, InternalResultValue},
        lifecycle_diagnostics::{
            FrontendLifecycleDiagnostics, QueryLifecycleConvergenceErrorSource,
            QueryLifecycleConvergenceSnapshot, RuntimeFilterTerminalRollupSnapshot,
            RuntimeFilterTerminalRollupUnavailable,
        },
        lifecycle_plan::AttemptCredentialStorage,
        runtime_filter_terminal_rollup::rollup_from_release_contributions,
        split_assignment_round::SplitAssignmentRoundGuard,
    },
    runtime_filter::feedback::RuntimeFilterFeedbackState,
    task_execution::{
        attempt_convergence::{
            NativeAttemptConvergenceMode, NativeAttemptConvergenceOwner, advance_task_round,
        },
        round::TaskRound,
        split_transport::SplitDeliveryBridge,
    },
};
use novarocks_execution_contract::{QueryContextRef, root_lifetime::RootReadConvergence};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{
        BoundedRootReadPort, NativeActiveConvergenceClosure, StatementNativeAttemptOwner,
        StatementNativeClosure, StatementNativeConvergenceFuture,
        StatementNativeConvergenceRequest, StatementNativeHealthFailure, StatementNativeLocalExit,
        StatementNativeLocalHolder, StatementNativeRemoteClosure,
    },
    execution_error::ExecutionFailureInput,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::WorkScope;
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

type InternalJob = InternalResultCpuJob<Result<InternalDomainSlot, InternalDomainFailure>>;
const LOCAL_HOLDERS: [StatementNativeLocalHolder; 5] = [
    StatementNativeLocalHolder::RootPolls,
    StatementNativeLocalHolder::InternalCpu,
    StatementNativeLocalHolder::SplitSources,
    StatementNativeLocalHolder::NativeOperations,
    StatementNativeLocalHolder::StatusSubscriptions,
];

pub(super) struct StatementNativeAttempt {
    pub execution: QueryExecutionId,
    pub scope: WorkScope,
    pub contexts: Box<[QueryContextRef]>,
    // These original backing owners cannot be reconstructed from wire facts.
    pub _prepared: TaskExecutionPreparedQuery,
    pub _query_registration: ActiveQueryGuard,
    pub wake: Arc<crate::task_execution::status_intake::CondvarWake>,
    pub _attempt_storage: Option<Arc<AttemptCredentialStorage>>,
    pub round: TaskRound,
    pub split_delivery: Arc<SplitDeliveryBridge>,
    pub convergence: NativeAttemptConvergenceOwner,
    pub split_assignment: Option<SplitAssignmentRoundGuard>,
    pub feedback_state: Arc<RuntimeFilterFeedbackState>,
    pub relayed_reader: Option<Arc<dyn BoundedRootReadPort>>,
    pub root_result_polls: Option<RootResultPolls>,
    pub pending_internal: Option<InternalJob>,
    pub retiring_internal: Option<InternalJob>,
    pub internal_domain: Option<InternalResultValue<InternalDomainSlot>>,
    pub internal_capacity: Option<QueryResultCapacityBinding>,
    pub final_task_info: FinalTaskInfoCollector,
    pub diagnostics: Arc<FrontendLifecycleDiagnostics>,
    pub accepted_failure: Option<ExecutionFailureInput>,
    pub mode: Option<NativeAttemptConvergenceMode>,
    pub profile_waits_for_terminals: bool,
    pub health: Option<StatementNativeHealthFailure>,
    pub remote_closed: Option<bool>,
    pub close_started: bool,
    pub close_turn_error_reported: bool,
}

impl StatementNativeAttempt {
    pub fn begin_terminal(&mut self, mode: NativeAttemptConvergenceMode, profile: bool) {
        if self.close_started
            && (self.mode == Some(NativeAttemptConvergenceMode::Cleanup) || self.mode == Some(mode))
        {
            return;
        }
        self.mode = Some(mode);
        self.profile_waits_for_terminals = profile && mode == NativeAttemptConvergenceMode::Normal;
        self.feedback_state.close();
        self.round.begin_input_close();
        if let Some(reader) = self.relayed_reader.as_ref()
            && mode == NativeAttemptConvergenceMode::Cleanup
        {
            let mut root = RootReadConvergence::new(self.round.root_task());
            if let Err(error) = reader.seal(root.seal_terminated()) {
                tracing::warn!(%error, "Terminal root close diagnostic");
            }
        }
        if let Some(polls) = self.root_result_polls.as_mut() {
            polls.begin_close();
        }
        if let Some(job) = self.pending_internal.as_mut() {
            job.begin_close();
        }
        if let Some(job) = self.retiring_internal.as_mut() {
            job.begin_close();
        }
        self.close_started = true;
        if mode == NativeAttemptConvergenceMode::Cleanup {
            self.final_task_info.stop_starting();
        }
        if !self.profile_waits_for_terminals {
            self.start_remote_close(mode);
            if let Some(source) = &self.split_assignment {
                source.request_stop();
            }
        }
    }

    fn request_context_stop(&mut self) {
        for context in self.contexts.iter().copied() {
            if let Err(error) = self.round.execution_mut().abort_context(
                context,
                novarocks_execution_contract::AbortCause::QueryFailed,
            ) && !self.close_turn_error_reported
            {
                tracing::warn!(%error, "Native terminal context stop diagnostic");
                self.close_turn_error_reported = true;
            }
        }
    }
    fn start_remote_close(&mut self, mode: NativeAttemptConvergenceMode) {
        self.convergence
            .begin(&mut self.round, &self.split_delivery, mode);
        if mode == NativeAttemptConvergenceMode::Cleanup {
            self.request_context_stop();
        }
    }

    /// Retire the same completed forwarder before allowing another CPU job.
    /// Early cancellation may deliver an error while that original job runs.
    pub fn poll_retiring_internal(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(job) = self.retiring_internal.as_mut() else {
            return true;
        };
        match job.poll_closed(cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(())) => {
                self.retiring_internal.take();
                true
            }
            Poll::Ready(Err(_)) => {
                let cause = job.join_failure().expect("original CPU join error");
                self.health.get_or_insert_with(|| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "Original Internal CPU forwarder exit is unproven",
                        cause,
                    )
                });
                true // unproven family remains held; progress healthy siblings
            }
        }
    }

    pub(super) fn publish(&mut self, converged: bool) {
        self.remote_closed = Some(converged);
        let contributions = self
            .round
            .execution()
            .released_runtime_filter_contributions();
        let runtime_filter = if converged && contributions.is_complete() {
            RuntimeFilterTerminalRollupSnapshot::Available(rollup_from_release_contributions(
                contributions.contributions(),
            ))
        } else {
            RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            )
        };
        self.diagnostics.publish(QueryLifecycleConvergenceSnapshot {
            execution_id: self.execution,
            error_source: self.accepted_failure.as_ref().map(|input| match input.site() {
                novarocks_query_application::execution_error::ExecutionFailureSite::Task { .. } | novarocks_query_application::execution_error::ExecutionFailureSite::TaskControl { .. } => QueryLifecycleConvergenceErrorSource::BackendAttestation,
                _ => QueryLifecycleConvergenceErrorSource::FrontendLiveness,
            }),
            primary_error: self.accepted_failure.as_ref().map(|input| input.detail().as_str().to_owned()),
            failure: self.accepted_failure.clone(),
            worker_contexts_converged: converged,
            runtime_filter,
        });
    }

    fn close_turn(
        &mut self,
        cx: &mut Context<'_>,
        request: &StatementNativeConvergenceRequest,
    ) -> Result<Option<StatementNativeClosure>, StatementNativeHealthFailure> {
        if !self.close_started {
            // An unexpected foreground unwind still returns this same owner.
            self.begin_terminal(NativeAttemptConvergenceMode::Cleanup, false);
        }
        if request.cancellation().reason().is_some() {
            self.begin_terminal(NativeAttemptConvergenceMode::Cleanup, false);
        }
        if self.profile_waits_for_terminals
            && std::time::Instant::now() >= request.remote_observation_deadline()
        {
            self.begin_terminal(NativeAttemptConvergenceMode::Cleanup, false);
        }
        let wake = Arc::clone(&self.wake);
        let waker = std::task::Waker::from(wake);
        let mut read_cx = Context::from_waker(&waker);
        self.final_task_info.poll_observe(&self.round, &mut read_cx);
        if self.profile_waits_for_terminals
            && self
                .round
                .execution()
                .attempt_drain_facts()
                .all_tasks_terminal()
            && self.final_task_info.all_terminal_observed()
        {
            self.profile_waits_for_terminals = false;
            self.convergence.begin(
                &mut self.round,
                &self.split_delivery,
                self.mode.expect("terminal mode"),
            );
            if let Some(source) = &self.split_assignment {
                source.request_stop();
            }
        }
        if self.mode == Some(NativeAttemptConvergenceMode::Cleanup) {
            self.request_context_stop();
        }
        // Always turn local close owners even if the remote receipts already
        // exist. A remote Ready is not a local CPU/source/poll exit receipt.
        if let Err(error) = advance_task_round(&mut self.round, &self.split_delivery) {
            if !self.close_turn_error_reported {
                tracing::warn!(%error, execution = ?self.execution, "Native terminal turn diagnostic");
                self.close_turn_error_reported = true;
            }
            self.profile_waits_for_terminals = false;
            self.mode = Some(NativeAttemptConvergenceMode::Cleanup);
            self.final_task_info.stop_starting();
            {
                self.convergence.begin(
                    &mut self.round,
                    &self.split_delivery,
                    NativeAttemptConvergenceMode::Cleanup,
                );
            }
        }
        while let Some(error) = self.round.take_pump_close_error() {
            tracing::warn!(%error, execution = ?self.execution, "Native source close diagnostic after fixed outcome");
        }
        if let Some(unresolved) = self.round.take_pump_close_unresolved() {
            let health = StatementNativeHealthFailure::actual_exit_unproven(
                "Original split source close exit is unproven",
                unresolved.cause,
            );
            self.health.get_or_insert(health);
        }
        let root_closed = match self.root_result_polls.as_mut() {
            None => true,
            Some(polls) => match polls.poll_closed(cx) {
                Poll::Pending => false,
                Poll::Ready(Ok(())) => true,
                Poll::Ready(Err(error)) if error.is_cancelled() => true,
                Poll::Ready(Err(_)) => {
                    let health = StatementNativeHealthFailure::actual_exit_unproven(
                        "Original Root poll exit is unproven",
                        polls.join_failure().expect("original Root join error"),
                    );
                    self.health.get_or_insert(health);
                    true // original unproven receipt stays in this owner
                }
            },
        };
        let cpu_closed = match self.pending_internal.as_mut() {
            None => true,
            Some(job) => match job.poll_closed(cx) {
                Poll::Pending => false,
                Poll::Ready(Ok(())) => true,
                Poll::Ready(Err(_)) => {
                    let health = StatementNativeHealthFailure::actual_exit_unproven(
                        "Original Internal CPU forwarder exit is unproven",
                        job.join_failure().expect("original CPU join error"),
                    );
                    self.health.get_or_insert(health);
                    true // original unproven receipt stays in this owner
                }
            },
        };
        let retiring_closed = self.poll_retiring_internal(cx);
        if self.health.is_some() {
            // A genuine unknown freezes optional production, but the same
            // owner still joins every independent healthy local family.
            self.begin_terminal(NativeAttemptConvergenceMode::Cleanup, false);
        }
        let activity_closed = self.internal_capacity.as_ref().is_none_or(|binding| {
            use std::future::Future;
            let future = binding.result_activities_exited();
            tokio::pin!(future);
            future.as_mut().poll(cx).is_ready()
        });
        // Freeze the original remote observation independently of local joins.
        // A held local job cannot renew the remote budget or upgrade Unknown.
        let remote_closure = self
            .convergence
            .observe_remote(&self.round, request.remote_observation_deadline());
        if let Some(health) = &self.health {
            let cpu_unproven = self
                .pending_internal
                .as_ref()
                .and_then(|job| job.join_failure())
                .is_some()
                || self
                    .retiring_internal
                    .as_ref()
                    .and_then(|job| job.join_failure())
                    .is_some();
            let healthy_local_done = root_closed
                && cpu_closed
                && retiring_closed
                && (activity_closed || cpu_unproven)
                && self.final_task_info.is_closed()
                && self.round.pump_close_progress_settled();
            let remote_observation_done = remote_closure.is_some();
            if healthy_local_done && remote_observation_done {
                // Healthy Native caller leases return to their original
                // process records even while this exact failed local family
                // remains retained. This is custody, never an exit receipt.
                self.round.return_native_observation_custody();
                return Err(health.clone());
            }
            return Ok(None);
        }
        if !(root_closed
            && self.final_task_info.is_closed()
            && cpu_closed
            && retiring_closed
            && activity_closed
            && self.round.pumps_closed())
        {
            return Ok(None);
        }
        let remote = match remote_closure {
            Some(NativeActiveConvergenceClosure::Evidence(evidence)) => {
                self.publish(true);
                StatementNativeRemoteClosure::Evidence(evidence)
            }
            Some(NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts }) => {
                self.publish(false);
                StatementNativeRemoteClosure::TrackingEnded { exact_contexts }
            }
            None => return Ok(None),
        };
        // Caller capabilities return to records which the original process
        // Native owners registered BEFORE dispatch. Their original child work,
        // raw future/cause retirement and Host join remain there until exit.
        self.round.return_native_observation_custody();
        let local = LOCAL_HOLDERS
            .into_iter()
            .map(|holder| match holder {
                StatementNativeLocalHolder::NativeOperations
                | StatementNativeLocalHolder::StatusSubscriptions => {
                    StatementNativeLocalExit::after_original_process_owner_return(holder)
                        .expect("Native original process custody")
                }
                _ => StatementNativeLocalExit::after_actual_exit(holder),
            })
            .collect::<Vec<_>>();
        Ok(Some(StatementNativeClosure::after_actual_exit(
            self.execution,
            self.scope.id(),
            remote,
            local,
        )))
    }
}

impl StatementNativeAttemptOwner for StatementNativeAttempt {
    fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    fn contexts(&self) -> &[QueryContextRef] {
        &self.contexts
    }
    fn local_holders(&self) -> &[StatementNativeLocalHolder] {
        &LOCAL_HOLDERS
    }
    fn request_shutdown_cleanup(&mut self) {
        self.begin_terminal(NativeAttemptConvergenceMode::Cleanup, false);
    }
    fn converge(
        &mut self,
        request: StatementNativeConvergenceRequest,
    ) -> StatementNativeConvergenceFuture<'_> {
        Box::pin(async move {
            loop {
                let wake = Arc::clone(&self.wake);
                let notified = wake.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let closed =
                    std::future::poll_fn(|cx| Poll::Ready(self.close_turn(cx, &request))).await?;
                if let Some(closed) = closed {
                    return Ok(closed);
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                }
            }
        })
    }
}
