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

//! TaskRound assembly from one exact application-bound Task manifest.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_query_application::api::{
    NativeActiveConvergenceClosure, NativeActiveConvergenceRequest,
    NativeAttemptPreparationFailure, NativeAttemptTerminal, StatementNativeHealthFailure,
};
use novarocks_query_application::coordination::DispatchBudget;
use novarocks_query_application::coordination::{
    AcceptedRootStatusSource, NativeAttemptDrive, ReplacementWorkerAdmissionEvidence,
};
use novarocks_task_codec::TransportBudget;
use novarocks_workload_control::{CancellationReason, CancellationView};

use super::abort_effect::NativeAbortEffectIntake;
use super::actor_gate::{ActorGateOwner, ActorGatedTaskOperationSink};
use super::attempt_convergence::{
    NativeAttemptConvergenceMode, NativeAttemptConvergenceOwner, NativeAttemptConvergenceTarget,
    advance_task_round,
};
use super::clock::{ProcessMonotonicClock, TaskProtocolClock};
#[cfg(test)]
use super::error::ParticipantObservationFailure;
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::graph::build_task_graph_from_manifest;
use super::intent::TaskOperationSink;
use super::round::{AcknowledgementObserver, TaskRound};
use super::sources::{AttemptEstablishFacts, SubmissionFragmentPlans};
use super::split_transport::SplitDeliveryBridge;
use super::status_intake::{NotifyWake, ObservationIntake, StatusIntake, StatusIntakeWake};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::task_transport::{
    AttemptWireFacts, CoveredTaskStatusSubscriber, NativeTaskOperationSink, TaskAckIntake,
};
use crate::query_execution::artifact::{TaskManifestBinding, ValidatedNativeSubmission};
use crate::query_execution::split_assignment::TaskUpdateTransport;
use crate::query_execution::split_assignment_round::{
    RoundSplitAssignmentPlan, SplitAssignmentRoundGuard,
};
use novarocks_query_application::api::BackendProcessObservationService;

const STATUS_INTAKE_CAPACITY: usize = 4096;
const IDLE_RECHECK: std::time::Duration = std::time::Duration::from_millis(5);

/// Process-owned runtime policy used to instantiate one exact manifest.
pub(crate) struct ManifestAttemptTransport {
    pub(crate) dispatch_budget: DispatchBudget,
    pub(crate) transport_budget: TransportBudget,
    pub(crate) status_subscription_error_budget: u32,
    pub(crate) wire: AttemptWireFacts,
    pub(crate) data_runtime: FrontendDataRuntime,
    pub(crate) subscription_scope: novarocks_workload_control::WorkScope,
    /// Exact process evidence bounds active pre-ready observation and residual
    /// convergence. It cannot change the frozen manifest or schedule a successor.
    pub(crate) convergence_source: BackendProcessObservationService,
}

/// One active Task protocol owner assembled without a scheduling capability.
pub(crate) struct ManifestAssembledRound {
    pub(crate) round: TaskRound,
    pub(crate) split_delivery: Arc<SplitDeliveryBridge>,
    pub(crate) actor_gate: ActorGateOwner,
    convergence: NativeAttemptConvergenceOwner,
    notify: Arc<tokio::sync::Notify>,
    terminal: Option<NativeAttemptTerminal>,
    pending_terminal: Option<NativeAttemptTerminal>,
    /// Only an exact Establish transport-unknown may await independent
    /// process replacement within its original admission receipt horizon.
    pending_pre_ready_unknown: Option<novarocks_types::BackendProcessId>,
}

/// The completion fact this Task owner must wait for before it reports the
/// physical attempt terminal to Query Application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManifestAttemptCompletion {
    /// A row result pump owns output and asks TaskRound to seal the exact root
    /// success after it has consumed Worker EOS.
    AcceptedRootSuccessSeal,
}

pub(crate) fn assemble_manifest_round(
    manifest: &TaskManifestBinding,
    submissions: Vec<ValidatedNativeSubmission>,
    establish: AttemptEstablishFacts,
    replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
    transport: ManifestAttemptTransport,
) -> Result<ManifestAssembledRound, TaskExecutionError> {
    let notify = Arc::new(tokio::sync::Notify::new());
    let wake = Arc::new(NotifyWake::new(Arc::clone(&notify))) as Arc<dyn StatusIntakeWake>;
    let mut plans = SubmissionFragmentPlans::index_manifest(submissions, manifest)?;
    let graph = build_task_graph_from_manifest(manifest, &mut plans, transport.transport_budget)?;
    let split_delivery = SplitDeliveryBridge::for_graph(&graph);
    let connector_blocking_io = transport.data_runtime.connector_blocking_io().clone();
    let convergence_source = Arc::clone(&transport.convergence_source);

    let mut backends = Vec::with_capacity(manifest.contexts().len());
    let mut admission_epochs = BTreeMap::new();
    let mut preparing_positions = BTreeMap::new();
    let mut convergence_targets = Vec::with_capacity(manifest.contexts().len());
    for context in manifest.contexts() {
        let backend = context.backend();
        if backend.native_compatibility_id() != transport.wire.native_compatibility_id {
            return Err(TaskExecutionError::Schedule(format!(
                "manifest backend {} compatibility differs from the attempt wire identity",
                backend.process_id()
            )));
        }
        backends.push(backend.target().descriptor().clone());
        preparing_positions.insert(
            backend.process_id(),
            backend.target().descriptor().preparing_positions(),
        );
        convergence_targets.push(NativeAttemptConvergenceTarget::new(
            context.context(),
            backend.endpoint().clone(),
        ));
        if admission_epochs
            .insert(backend.process_id(), backend.admission_epoch_capability())
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "manifest repeats admission authority for backend {}",
                backend.process_id()
            )));
        }
    }

    let acks = TaskAckIntake::new(Arc::clone(&wake));
    let native = Arc::new(
        NativeTaskOperationSink::new(
            &backends,
            transport.transport_budget,
            transport.wire.clone(),
            acks.handle(),
            transport.data_runtime.clone(),
        )
        .map_err(TaskExecutionError::NativeTransport)?,
    ) as Arc<dyn TaskOperationSink>;
    let (actor_sink, actor_gate, actor_observer) = ActorGatedTaskOperationSink::pair(
        native,
        Arc::clone(&wake),
        transport.wire.native_compatibility_id,
    );
    let sink = split_delivery.sink(Arc::new(actor_sink) as Arc<dyn TaskOperationSink>);

    let intake = StatusIntake::new(STATUS_INTAKE_CAPACITY, Arc::clone(&wake));
    let clock = Arc::new(ProcessMonotonicClock::new()) as Arc<dyn TaskProtocolClock>;
    let observation = Arc::new(
        ObservationIntake::for_task_attempt(Arc::clone(&wake), Arc::clone(&clock)).map_err(
            |error| {
                TaskExecutionError::Schedule(format!(
                    "covered observation capacity is invalid: {error:?}"
                ))
            },
        )?,
    );
    let covered_subscriber = Arc::new(
        CoveredTaskStatusSubscriber::new(
            &backends,
            Arc::clone(&observation),
            transport.status_subscription_error_budget,
            transport.data_runtime,
            transport.subscription_scope,
        )
        .map_err(TaskExecutionError::NativeTransport)?,
    );
    let mut execution = QueryTaskExecution::new(
        graph,
        transport.dispatch_budget,
        transport.transport_budget,
        transport.wire.native_compatibility_id,
        &admission_epochs,
        &preparing_positions,
        clock,
        sink,
        intake,
    )?;
    if let Some(admissions) = replacement_admissions {
        execution.adopt_replacement_admissions(&admissions)?;
    }
    let round = TaskRound::new_covered(
        execution,
        acks,
        Box::new(establish),
        covered_subscriber,
        observation,
    )
    .observing(Arc::clone(&split_delivery) as Arc<dyn AcknowledgementObserver>)
    .observing(actor_observer)
    .with_connector_blocking_io(connector_blocking_io);
    let convergence =
        NativeAttemptConvergenceOwner::new(&round, convergence_source, convergence_targets)?;
    Ok(ManifestAssembledRound {
        round,
        split_delivery,
        actor_gate,
        convergence,
        notify,
        terminal: None,
        pending_terminal: None,
        pending_pre_ready_unknown: None,
    })
}

impl ManifestAssembledRound {
    /// Read only the terminal selected by this original attempt owner.
    pub(crate) fn observed_failure(
        &self,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        match self.terminal.as_ref() {
            Some(NativeAttemptTerminal::Failed(failure)) => Some(failure.input().clone()),
            _ => None,
        }
    }

    pub(crate) fn install_split_assignment(
        &mut self,
        execution: novarocks_types::QueryExecutionId,
        plan: RoundSplitAssignmentPlan,
        data_runtime: FrontendDataRuntime,
    ) -> Option<SplitAssignmentRoundGuard> {
        SplitAssignmentRoundGuard::install(
            &mut self.round,
            execution,
            plan,
            Arc::clone(&self.split_delivery) as Arc<dyn TaskUpdateTransport>,
            data_runtime,
            Arc::new(NotifyWake::new(Arc::clone(&self.notify))) as Arc<dyn StatusIntakeWake>,
        )
    }

    pub(crate) fn abort_wake(&self) -> Arc<dyn StatusIntakeWake> {
        Arc::new(NotifyWake::new(Arc::clone(&self.notify)))
    }

    /// Installs the actor's one bounded Abort-effect intake before the attempt
    /// starts. The intake is part of C3's manifest-validated runtime resource
    /// projection; this owner never constructs a second actor port.
    pub(crate) fn install_abort_effect_intake(&mut self, intake: NativeAbortEffectIntake) {
        self.round.install_abort_effect_intake(intake);
    }

    pub(crate) fn take_root_status_source(&mut self) -> Option<AcceptedRootStatusSource> {
        self.round.take_root_status_source()
    }

    /// Drives one fair Task-protocol turn and then releases at most one batch
    /// through the async actor gate. ACK observation intentionally happens in
    /// the round first, so actor settlement precedes authorization of the next
    /// batch while Task state remains single-owner.
    async fn advance(&mut self, drive: &NativeAttemptDrive) -> Result<bool, TaskExecutionError> {
        let report = advance_task_round(&mut self.round, &self.split_delivery);
        // ACK observers run inside TaskRound before a state-machine error is
        // returned. Settle all observed ACKs before publishing that error,
        // without authorizing another batch for the terminal attempt.
        match report {
            Ok(report) => {
                let gated = self.actor_gate.drive(drive).await?;
                Ok(!report.is_idle() || gated != 0)
            }
            Err(error) => {
                let _ = self
                    .actor_gate
                    .settle_terminal_acknowledgements(drive)
                    .await;
                Err(error)
            }
        }
    }

    async fn receive_pending(
        &mut self,
        drive: &NativeAttemptDrive,
    ) -> Result<bool, TaskExecutionError> {
        let report = self.round.receive_without_dispatch();
        // ACK observers are still settled by their original actor owner.
        // No authorize_intents call is legal while evidence is pending.
        let settled = self
            .actor_gate
            .settle_terminal_acknowledgements(drive)
            .await;
        let report = report?;
        Ok(!report.is_idle() || settled? != 0)
    }

    fn record_protocol_failure(&mut self, error: TaskExecutionError) {
        let unknown = match &error {
            TaskExecutionError::PreReadyEstablishTransportUnknown { backend } => Some(*backend),
            _ => None,
        };
        if self.pending_terminal.is_none()
            || (self.pending_pre_ready_unknown.is_some() && unknown.is_none())
        {
            self.pending_pre_ready_unknown = unknown;
            self.pending_terminal = Some(task_protocol_failure(error));
        }
    }

    /// A positive exact replacement is a separate originating fact. Neither
    /// expiry nor uncertainty can supply it, and no recovery policy lives here.
    fn pending_pre_ready_must_wait(&mut self) -> bool {
        let Some(backend) = self.pending_pre_ready_unknown else {
            return false;
        };
        if self.convergence.process_replaced(backend) {
            use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
            use novarocks_query_application::execution_error::{
                ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
            };
            self.pending_terminal = Some(NativeAttemptTerminal::Failed(
                NativeAttemptPreparationFailure::from_input(ExecutionFailureInput::new(
                    QueryFailureReason::BackendLost.failure_fact(),
                    SafeDetail::truncating("the frozen pre-ready backend process was replaced"),
                    ExecutionFailureSite::Backend { process: backend },
                )),
            ));
            self.pending_pre_ready_unknown = None;
            return false;
        }
        self.round
            .execution()
            .pre_ready_observation_remaining(backend)
            .is_some_and(|remaining| !remaining.is_zero())
    }

    /// Runs the active attempt without occupying an OS thread. The method is
    /// borrowed and memoizes its terminal, so cancellation or panic of the
    /// caller's future cannot move the Task owner out of its supervisor.
    pub(crate) async fn run(
        &mut self,
        drive: &NativeAttemptDrive,
        cancellation: CancellationView,
        completion: ManifestAttemptCompletion,
    ) -> NativeAttemptTerminal {
        if let Some(terminal) = &self.terminal {
            return terminal.clone();
        }
        let mut process_changes: Option<tokio::sync::watch::Receiver<u64>> = None;
        let mut process_changes_open = true;
        loop {
            let notify = Arc::clone(&self.notify);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let was_pending_pre_ready = self.pending_pre_ready_unknown.is_some();
            let advanced = if was_pending_pre_ready {
                self.receive_pending(drive).await
            } else {
                self.advance(drive).await
            };
            let moved = match advanced {
                Ok(moved) => moved,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        "Active Task protocol failed before attempt terminal"
                    );
                    self.record_protocol_failure(error);
                    false
                }
            };
            let success_sealed = completion == ManifestAttemptCompletion::AcceptedRootSuccessSeal
                && self.round.accepted_root_success_sealed();
            if success_sealed {
                if let Some(residual) = self.pending_terminal.take() {
                    tracing::warn!(
                        ?residual,
                        "Task failure after the accepted root seal belongs to cleanup"
                    );
                }
                if self.actor_gate.prepare_convergence() {
                    let terminal = NativeAttemptTerminal::Completed;
                    self.terminal = Some(terminal.clone());
                    return terminal;
                }
            }
            if !success_sealed
                && (self.pending_terminal.is_none()
                    || self.pending_pre_ready_unknown.is_some())
                && let Some(input) = self.round.execution().selected_task_failure().and_then(novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task)
            {
                // Peer placeholders still wait for their original author.
                // Task/control inputs retain their real identity and phase.
                let terminal=NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(input));
                let pending_unknown = self.pending_pre_ready_unknown.is_some();
                if self.actor_gate.prepare_convergence() || pending_unknown {
                    self.terminal=Some(terminal.clone());
                    return terminal;
                }
            }
            if !success_sealed
                && self.pending_pre_ready_unknown.is_some()
                && let Some(reason) = cancellation.reason()
            {
                // Publish control without waiting for residual transports
                // whose abort/close owner is the enclosing supervisor.
                let _ = self.actor_gate.prepare_convergence();
                let terminal = cancellation_failure(reason);
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if !success_sealed
                && self.pending_pre_ready_unknown.is_some()
                && process_changes.is_none()
            {
                // Subscribe only for this confirmed unknown, before its first
                // process read. Ordinary attempts never acquire this observer.
                process_changes = Some(self.convergence.subscribe_process_changes());
            }
            let waiting_for_process = !success_sealed && self.pending_pre_ready_must_wait();
            if !success_sealed && self.pending_terminal.is_some() && !waiting_for_process {
                // A Task-protocol failure is already an authoritative attempt
                // terminal. Closing the actor gate prevents any further
                // admission, but outstanding actor-owned transport effects
                // are residual convergence work: waiting for them here would
                // deadlock a failure that needs Query Application to issue its
                // Abort effects first.
                let _ = self.actor_gate.prepare_convergence();
                let terminal = self
                    .pending_terminal
                    .take()
                    .expect("pending Task terminal was checked");
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            let completed = match completion {
                ManifestAttemptCompletion::AcceptedRootSuccessSeal => {
                    self.round.accepted_root_success_sealed()
                }
            };
            if !waiting_for_process && completed && self.actor_gate.prepare_convergence() {
                let terminal = NativeAttemptTerminal::Completed;
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if !success_sealed
                && let Some(reason) = cancellation.reason()
                && self.actor_gate.prepare_convergence()
            {
                let terminal = cancellation_failure(reason);
                self.terminal = Some(terminal.clone());
                return terminal;
            }
            if moved {
                continue;
            }
            tokio::select! {
                _ = notified => {}
                reason = cancellation.cancelled(), if !success_sealed => {
                    if self.actor_gate.prepare_convergence() || waiting_for_process {
                        let terminal = cancellation_failure(reason);
                        self.terminal = Some(terminal.clone());
                        return terminal;
                    }
                }
                changed = async {
                    process_changes.as_mut()
                        .expect("a pending process read subscribed before observation")
                        .changed().await
                }, if waiting_for_process && process_changes_open => {
                    process_changes_open = changed.is_ok();
                }
                _ = tokio::time::sleep(IDLE_RECHECK) => {}
            }
        }
    }

    /// Keeps the original local pumps until their real close/join exits. The
    /// original remote deadline only ends remote tracking; it never supplies a
    /// Worker stop fact or substitutes for a local pump exit.
    pub(crate) async fn converge(
        &mut self,
        request: NativeActiveConvergenceRequest,
    ) -> Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure> {
        let mode = if matches!(self.terminal, Some(NativeAttemptTerminal::Completed)) {
            NativeAttemptConvergenceMode::Normal
        } else {
            NativeAttemptConvergenceMode::Cleanup
        };
        self.convergence
            .drive(
                &mut self.round,
                &self.split_delivery,
                mode,
                request,
                &self.notify,
            )
            .await
    }

    #[cfg(test)]
    fn context_convergence(
        &self,
    ) -> Option<Box<[novarocks_query_application::api::NativeContextConvergence]>> {
        self.convergence.observed_contexts(&self.round)
    }
}

// The original raw TaskRound fixture still starts its normal drain here.
// Production convergence is owned by NativeAttemptConvergenceOwner.
#[cfg(test)]
pub(super) fn begin_convergence(
    round: &mut TaskRound,
    split_delivery: &SplitDeliveryBridge,
    terminal: Option<&NativeAttemptTerminal>,
) {
    let mode = if matches!(terminal, Some(NativeAttemptTerminal::Completed)) {
        NativeAttemptConvergenceMode::Normal
    } else {
        NativeAttemptConvergenceMode::Cleanup
    };
    super::attempt_convergence::begin_convergence(round, split_delivery, mode);
}

fn task_protocol_failure(error: TaskExecutionError) -> NativeAttemptTerminal {
    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
        error.preparation_input(),
    ))
}

fn cancellation_failure(reason: CancellationReason) -> NativeAttemptTerminal {
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
    };
    let fact = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => QueryFailureReason::DeadlineExceeded,
        _ => QueryFailureReason::Cancelled,
    }
    .failure_fact();
    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
        ExecutionFailureInput::new(
            fact,
            SafeDetail::truncating(&format!("Native Task attempt cancelled: {reason:?}")),
            ExecutionFailureSite::Coordinator,
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::api::BackendProcessObservation;
    use novarocks_query_application::{
        api::QueryExecutionErrorKind, coordination::AttemptFailureClass,
    };

    fn protocol_context(
        backend: novarocks_types::BackendProcessId,
    ) -> novarocks_execution_contract::QueryContextRef {
        use novarocks_types::{AttemptId, FrontendProcessId, QueryExecutionId, QueryId};
        novarocks_execution_contract::QueryContextRef::new(
            QueryExecutionId::new(QueryId::new(31, 32), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            backend,
        )
    }

    #[test]
    fn preparation_task_capability_uses_the_same_client_identity_as_result_observation() {
        use novarocks_failure_contract::FailureProjection;
        let backend = novarocks_types::BackendProcessId::new_v7();
        let query = novarocks_types::QueryExecutionId::new(
            novarocks_types::QueryId::new(31, 32),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let identity = novarocks_execution_contract::TaskIdentity::new(
            query,
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            backend,
        );
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "FULL OUTER hash join".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("FULL OUTER hash join".into()),
        ] {
            let fact = original.failure_fact();
            let selected = novarocks_query_application::coordination::SelectedTaskTermination::new(
                identity,
                novarocks_execution_contract::TerminationDetail::Failed(
                    novarocks_execution_contract::TaskFailure::new_in_phase(
                        fact,
                        novarocks_execution_contract::SafeDetail::truncating(
                            "FULL OUTER hash join",
                        ),
                        novarocks_execution_contract::TaskFailurePhase::Preparation,
                    ),
                ),
            );
            let input = novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap();
            assert_eq!(input.fact(), fact);
            assert_eq!(
                input.site(),
                novarocks_query_application::execution_error::ExecutionFailureSite::Task {
                    identity,
                    phase: novarocks_execution_contract::TaskFailurePhase::Preparation
                }
            );
            let failure = NativeAttemptPreparationFailure::from_input(input.clone());
            assert_eq!(failure.input(), &input);
            let observed =
                novarocks_query_application::coordination::RootResultFetchFailure::from_input(
                    input.clone(),
                );
            assert_eq!(failure.class(), observed.class());
            assert_eq!(failure.error().execution_failure(), Some(&input));
            assert_eq!(observed.input().descriptor(), input.descriptor());
            assert!(failure.error().message().contains("FULL OUTER hash join"));
        }
    }

    struct UnusedConvergenceSource;

    impl novarocks_query_application::api::BackendProcessObservationPort for UnusedConvergenceSource {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            panic!(
                "ordinary, sealed, Task-failed or cancelled attempts must not subscribe to process observation"
            );
        }

        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            panic!("attempt terminal arbitration must not inspect residual convergence");
        }
    }

    async fn terminal_regression_drive(
        context: novarocks_execution_contract::QueryContextRef,
    ) -> (
        novarocks_query_application::test_support::LogicalExecutionTestHarness,
        NativeAttemptDrive,
        CancellationView,
        novarocks_workload_control::WorkCancellationRequester,
        super::super::abort_effect::NativeAbortEffectIntake,
    ) {
        use novarocks_query_application::coordination::{
            AbortQueryContextEffectPort, ExecutionEffect, LogicalExecutionActorConfig,
        };
        use novarocks_query_application::test_support::LogicalExecutionTestHarness;
        use novarocks_workload_control::{
            ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };
        use std::num::NonZeroUsize;
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: (1 << 20) - (1 << 10),
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let governed = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = governed.owner.scope().cancellation().unwrap();
        let requester = governed.owner.cancellation_requester();
        let execution_stage = governed
            .owner
            .scope()
            .try_acquire(Stage::Execution)
            .unwrap();
        let (adapter, intake) = super::super::abort_effect::NativeAbortEffectAdapter::bounded(
            NonZeroUsize::new(2).unwrap(),
            Arc::new(super::super::status_intake::CountingWake::default()),
        );
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            context.query_execution_id(),
            ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            governed.owner,
            execution_stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(
            adapter as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            context.query_execution_id(),
            vec![context],
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let drive = logical.native_attempt_drive();
        (logical, drive, cancellation, requester, intake)
    }

    #[derive(Debug)]
    struct BackpressuredGateTransport;

    impl TaskOperationSink for BackpressuredGateTransport {
        fn try_reserve_queue(
            &self,
            _: super::super::intent::TaskOperationQueueRequest,
        ) -> super::super::intent::TaskOperationQueueAdmission {
            super::super::intent::TaskOperationQueueAdmission::Admitted(
                super::super::intent::test_queue_permit(),
            )
        }

        fn try_submit(
            &self,
            batch: super::super::intent::DispatchBatch,
        ) -> super::super::intent::TaskOperationSubmit {
            super::super::intent::TaskOperationSubmit::Backpressured(batch)
        }
    }

    #[test]
    fn abort_closure_is_not_worker_exit_and_only_the_original_receipt_closes_resources() {
        use super::super::status_intake::{ObservationFrame, ObservationIntakeEntry};
        use novarocks_execution_contract::{
            QueryContextConvergenceReceipt, QueryContextConvergenceState,
            QueryContextConvergenceVersion,
        };
        use novarocks_task_codec::operation::{CoveredStatusStreamEvent, CoveredStatusStreamFact};
        struct CurrentProcess;
        impl novarocks_query_application::api::BackendProcessObservationPort for CurrentProcess {
            fn observe_process_at_endpoint(
                &self,
                _: novarocks_types::BackendProcessId,
                _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
            ) -> Result<
                BackendProcessObservation,
                novarocks_query_application::api::BackendTopologyError,
            > {
                Ok(BackendProcessObservation::Current)
            }
        }
        let (round, _) = super::super::tests::manifest_terminal_regression_fixture();
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence = NativeAttemptConvergenceOwner::new(
            &round,
            Arc::new(CurrentProcess),
            vec![NativeAttemptConvergenceTarget::new(
                context,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut assembled = ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: None,
            pending_terminal: None,
            pending_pre_ready_unknown: None,
        };
        assembled.convergence.begin(
            &mut assembled.round,
            &assembled.split_delivery,
            NativeAttemptConvergenceMode::Cleanup,
        );
        assembled
            .round
            .execution_mut()
            .observe_actor_abort_context_closed(context)
            .unwrap();
        assert!(
            assembled
                .round
                .execution()
                .owner(context)
                .unwrap()
                .is_released()
        );
        assert!(assembled.context_convergence().is_none());
        assert!(!assembled.round.execution().worker_contexts_stopped());
        let receipt = QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
        assembled
            .round
            .execution_mut()
            .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
                ObservationFrame::Covered {
                    context,
                    generation: 1,
                    event: CoveredStatusStreamEvent {
                        fact: CoveredStatusStreamFact::ContextConvergence(receipt),
                        source_revision: None,
                    },
                },
            ))])
            .unwrap();
        assert_eq!(
            assembled
                .round
                .execution()
                .context_convergence_receipt(context),
            Some(receipt)
        );
        assert!(assembled.round.execution().worker_contexts_stopped());
        assert_eq!(assembled.context_convergence().unwrap().len(), 1);
    }

    #[derive(Clone)]
    struct ReleaseJoinObservation(BackendProcessObservation);

    impl novarocks_query_application::api::BackendProcessObservationPort for ReleaseJoinObservation {
        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            Ok(self.0)
        }
    }

    fn release_join_round(
        round: TaskRound,
        observation: BackendProcessObservation,
    ) -> ManifestAssembledRound {
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence = NativeAttemptConvergenceOwner::new(
            &round,
            Arc::new(ReleaseJoinObservation(observation)),
            vec![NativeAttemptConvergenceTarget::new(
                context,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut assembled = ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: Some(NativeAttemptTerminal::Completed),
            pending_terminal: None,
            pending_pre_ready_unknown: None,
        };
        assembled.convergence.begin(
            &mut assembled.round,
            &assembled.split_delivery,
            NativeAttemptConvergenceMode::Normal,
        );
        assembled
    }

    fn publish_release_join_exit(assembled: &mut ManifestAssembledRound) {
        use super::super::status_intake::{ObservationFrame, ObservationIntakeEntry};
        use novarocks_execution_contract::{
            QueryContextConvergenceReceipt, QueryContextConvergenceState,
            QueryContextConvergenceVersion,
        };
        use novarocks_task_codec::operation::{CoveredStatusStreamEvent, CoveredStatusStreamFact};
        let context = *assembled
            .round
            .execution()
            .graph()
            .contexts()
            .next()
            .unwrap();
        let receipt = QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
        assembled
            .round
            .execution_mut()
            .apply_covered_entries(vec![ObservationIntakeEntry::Frame(Box::new(
                ObservationFrame::Covered {
                    context,
                    generation: 1,
                    event: CoveredStatusStreamEvent {
                        fact: CoveredStatusStreamFact::ContextConvergence(receipt),
                        source_revision: None,
                    },
                },
            ))])
            .unwrap();
    }

    #[test]
    fn completed_convergence_joins_covered_exit_and_original_release_in_both_orders() {
        use super::super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
        use novarocks_execution_contract::{
            OperationKind, OperationOutcome, QueryContextReceipt, QueryContextState, ReleaseOutcome,
        };
        let expected = super::super::tests::fixture_runtime_filter_contribution();
        let mut outcomes = Vec::new();
        for covered_first in [true, false] {
            let (round, acks, release) =
                super::super::tests::manifest_normal_release_ack_regression_fixture();
            let mut assembled = release_join_round(round, BackendProcessObservation::Current);
            let context = *assembled
                .round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap();
            let OperationIntent::ReleaseQueryContext(request) = &release else {
                unreachable!()
            };
            let ack = OperationAcknowledgement::new(
                release.operation_id(),
                OperationKind::ReleaseQueryContext,
                OperationOutcome::Accepted,
                AckPayload::Release {
                    receipt: QueryContextReceipt::new(
                        request.context(),
                        QueryContextState::TerminalRetained,
                    ),
                    outcome: ReleaseOutcome::Released,
                    runtime_filter: Some(expected.clone()),
                },
            );
            assert!(assembled.context_convergence().is_none());
            if covered_first {
                publish_release_join_exit(&mut assembled);
                assert!(assembled.round.execution().worker_contexts_stopped());
                assert!(
                    !assembled
                        .round
                        .execution()
                        .owner(context)
                        .unwrap()
                        .is_released()
                );
                assert!(
                    assembled.context_convergence().is_none(),
                    "physical exit must not discard the pending Release acknowledgement"
                );
                acks.publish(ack);
                assembled.round.turn().unwrap();
            } else {
                acks.publish(ack);
                assembled.round.turn().unwrap();
                assert!(
                    assembled
                        .round
                        .execution()
                        .owner(context)
                        .unwrap()
                        .is_released()
                );
                assert!(!assembled.round.execution().worker_contexts_stopped());
                assert!(
                    assembled.context_convergence().is_none(),
                    "Release acknowledgement is not physical exit evidence"
                );
                publish_release_join_exit(&mut assembled);
            }
            assert_eq!(assembled.context_convergence().unwrap().len(), 1);
            let contributions = assembled
                .round
                .execution()
                .released_runtime_filter_contributions();
            assert!(contributions.is_complete());
            assert_eq!(contributions.contexts(), 1);
            assert_eq!(
                contributions.contributions(),
                &[(context.backend_process_id(), expected.clone())]
            );
            outcomes.push(
                contributions
                    .contributions()
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(
            outcomes[0], outcomes[1],
            "arrival order cannot change the sealed contribution"
        );
    }

    #[test]
    fn cleanup_convergence_keeps_physical_exit_without_requiring_normal_release() {
        let (round, _acks, _release) =
            super::super::tests::manifest_normal_release_ack_regression_fixture();
        let mut assembled = release_join_round(round, BackendProcessObservation::Current);
        let context = *assembled
            .round
            .execution()
            .graph()
            .contexts()
            .next()
            .unwrap();
        assembled.convergence.begin(
            &mut assembled.round,
            &assembled.split_delivery,
            NativeAttemptConvergenceMode::Cleanup,
        );
        publish_release_join_exit(&mut assembled);
        assert!(
            !assembled
                .round
                .execution()
                .owner(context)
                .unwrap()
                .is_released()
        );
        assert_eq!(assembled.context_convergence().unwrap().len(), 1);
        let contributions = assembled
            .round
            .execution()
            .released_runtime_filter_contributions();
        assert!(!contributions.is_complete());
        assert!(contributions.contributions().is_empty());
    }

    #[test]
    fn exact_process_replacement_does_not_wait_for_release_or_invent_a_contribution() {
        use novarocks_query_application::api::NativeContextConvergenceKind;
        for covered_first in [false, true] {
            let (round, _acks, _release) =
                super::super::tests::manifest_normal_release_ack_regression_fixture();
            let mut assembled = release_join_round(
                round,
                BackendProcessObservation::Replaced {
                    current_process: novarocks_types::BackendProcessId::new_v7(),
                },
            );
            if covered_first {
                publish_release_join_exit(&mut assembled);
            }
            let convergence = assembled.context_convergence().unwrap();
            assert_eq!(convergence.len(), 1);
            assert_eq!(
                convergence[0].kind(),
                NativeContextConvergenceKind::WorkerProcessReplaced
            );
            let contributions = assembled
                .round
                .execution()
                .released_runtime_filter_contributions();
            assert!(!contributions.is_complete());
            assert!(contributions.contributions().is_empty());
        }
    }

    #[tokio::test]
    async fn accepted_root_seal_wins_residual_native_advance_error_before_reply_consumption() {
        use super::super::intent::OperationAcknowledgement;
        use novarocks_execution::task_execution::{OperationKind, TaskOperationId};
        for (accept_before_error, cancel_with_pending_gate) in
            [(true, false), (false, false), (true, true)]
        {
            let (mut round, acks) = super::super::tests::manifest_terminal_regression_fixture();
            let context = *round.execution().graph().contexts().next().unwrap();
            let (mut logical, drive, cancellation, requester, _abort_intake) =
                terminal_regression_drive(context).await;
            let source = round.take_root_status_source().unwrap();
            let mut reply = source.begin_success_seal_request().unwrap();
            if accept_before_error {
                round.turn().unwrap();
                assert!(round.accepted_root_success_sealed());
            }
            // This Native ACK is deliberately not a sent operation. It takes
            // the actual advance error path before run inspects the seal; the
            // result pump still holds its unread seal reply throughout run.
            acks.publish(OperationAcknowledgement::transport_unknown(
                TaskOperationId::new_v7(),
                OperationKind::UpdateTask,
            ));
            let (sink, actor_gate, _) = ActorGatedTaskOperationSink::pair(
                Arc::new(BackpressuredGateTransport),
                Arc::new(super::super::status_intake::CountingWake::default()),
                novarocks_types::NativeCompatibilityId::new([0x41; 32]),
            );
            let cancellation_after_gate_closes = if cancel_with_pending_gate {
                use super::super::intent::{DispatchBatch, OperationIntent, TaskOperationSubmit};
                use novarocks_execution::task_execution::{
                    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, LeaseValidFor,
                };
                use novarocks_query_application::coordination::DispatchLane;
                let operation = OperationIntent::AcquireQueryContextAdmissionTicket(
                    AcquireQueryContextAdmissionTicket::new(
                        TaskOperationId::new_v7(),
                        context,
                        LeaseValidFor::new(std::time::Duration::from_secs(30)).unwrap(),
                        novarocks_types::NativeCompatibilityId::new([0x41; 32]),
                        AdmissionEpochCapability::try_from_bytes([9; 16]).unwrap(),
                    ),
                );
                let bytes = operation.queue_request().queued_bytes();
                let batch = DispatchBatch::test_fixture(
                    context.backend_process_id(),
                    DispatchLane::Lifecycle,
                    vec![operation],
                    bytes,
                );
                let TaskOperationSubmit::Backpressured(mut retained) = sink.try_submit(batch)
                else {
                    panic!("actor authorization must retain the unsent carrier");
                };
                Some(tokio::spawn(async move {
                    loop {
                        tokio::task::yield_now().await;
                        match sink.try_submit(retained) {
                            TaskOperationSubmit::Backpressured(batch) => retained = batch,
                            TaskOperationSubmit::Rejected { reason, .. } => {
                                assert!(reason.contains("closing"));
                                // This rejection removes the authorized carrier only
                                // after run's first convergence attempt returned false.
                                requester.request(CancellationReason::Requested).unwrap();
                                break;
                            }
                            other => panic!("unsent carrier unexpectedly escaped: {other:?}"),
                        }
                    }
                }))
            } else {
                None
            };
            let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
            let convergence = NativeAttemptConvergenceOwner::new(
                &round,
                Arc::new(UnusedConvergenceSource),
                round
                    .execution()
                    .graph()
                    .contexts()
                    .map(|&context| {
                        NativeAttemptConvergenceTarget::new(
                            context,
                            novarocks_execution::runtime::endpoint::RuntimeEndpoint::new(
                                "127.0.0.1",
                                1,
                            )
                            .unwrap(),
                        )
                    })
                    .collect(),
            )
            .unwrap();
            let mut assembled = ManifestAssembledRound {
                round,
                split_delivery,
                actor_gate,
                convergence,
                notify: Arc::new(tokio::sync::Notify::new()),
                terminal: None,
                pending_terminal: None,
                pending_pre_ready_unknown: None,
            };
            let terminal = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal,
                ),
            )
            .await
            .expect("terminal arbitration must finish without residual cleanup");
            if let Some(release) = cancellation_after_gate_closes {
                release.await.unwrap();
            }
            if accept_before_error {
                assert!(matches!(terminal, NativeAttemptTerminal::Completed));
                assert_eq!(reply.try_recv().unwrap(), Ok(()));
            } else {
                assert!(matches!(terminal, NativeAttemptTerminal::Failed(_)));
                assert!(!assembled.round.accepted_root_success_sealed());
                assert!(!matches!(reply.try_recv(), Ok(Ok(()))));
            }
            drop(assembled);
            drop(drive);
            logical.abandon_running_attempt();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while logical
                    .stand_down_snapshot(context)
                    .await
                    .unwrap()
                    .is_none()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // This closes only the fixture actor's residual scope during
            // explicit teardown, after the terminal assertions above.
            logical
                .observe_worker_process_replaced(context)
                .await
                .unwrap();
            logical
                .finish_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
                .await
                .unwrap();
        }
    }

    #[test]
    fn foreign_status_identity_violation_refuses_attempt_recovery() {
        let terminal = task_protocol_failure(TaskExecutionError::ParticipantUnobservable {
            backend: novarocks_types::identity::BackendProcessId::new_v7(),
            state: ParticipantObservationFailure::IdentityViolation("process_mismatch"),
        });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("task protocol refusal must fail the attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
    }

    #[test]
    fn pre_ready_establish_unknown_is_observation_loss_without_backend_loss_evidence() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let terminal =
            task_protocol_failure(TaskExecutionError::PreReadyEstablishTransportUnknown {
                backend,
            });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("transport uncertainty must terminate the old attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    #[test]
    fn pre_ready_establish_rejection_retains_its_protocol_fact() {
        let terminal = task_protocol_failure(TaskExecutionError::PreReadyEstablishRejected {
            context: protocol_context(novarocks_types::identity::BackendProcessId::new_v7()),
            operation_id: novarocks_execution_contract::TaskOperationId::new_v7(),
            rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                outcome:
                    novarocks_execution::task_execution::OperationOutcome::InvalidStateOrRequest,
                detail: None,
                acknowledgement: None,
            },
        });
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("pre-ready Establish rejection must terminate the old attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(failure.error().kind(), QueryExecutionErrorKind::Failed);
    }
    #[test]
    fn pre_ready_identity_mismatch_is_an_operation_fact_without_process_loss() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let NativeAttemptTerminal::Failed(failure) =
            task_protocol_failure(TaskExecutionError::PreReadyEstablishRejected {
                context: protocol_context(backend),
                operation_id: novarocks_execution_contract::TaskOperationId::new_v7(),
                rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                    outcome: novarocks_execution_contract::OperationOutcome::IdentityMismatch,
                    detail: None,
                    acknowledgement: None,
                },
            })
        else {
            panic!("identity mismatch must fail the attempt");
        };
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }
    #[test]
    fn lost_status_transport_does_not_manufacture_failed_process_evidence() {
        let backend = novarocks_types::identity::BackendProcessId::new_v7();
        let NativeAttemptTerminal::Failed(failure) =
            task_protocol_failure(TaskExecutionError::ParticipantUnobservable {
                backend,
                state: ParticipantObservationFailure::Transport("stream_lost"),
            })
        else {
            panic!("lost participant must fail the attempt");
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    struct PendingProcessObservation {
        expected: novarocks_types::BackendProcessId,
        observation: std::sync::Mutex<
            Result<
                BackendProcessObservation,
                novarocks_query_application::api::BackendTopologyError,
            >,
        >,
        changes: tokio::sync::watch::Sender<u64>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl PendingProcessObservation {
        fn new(
            expected: novarocks_types::BackendProcessId,
            observation: BackendProcessObservation,
        ) -> Self {
            let (changes, _) = tokio::sync::watch::channel(0);
            Self {
                expected,
                observation: std::sync::Mutex::new(Ok(observation)),
                changes,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn replace(&self) {
            *self.observation.lock().unwrap() = Ok(BackendProcessObservation::Replaced {
                current_process: novarocks_types::BackendProcessId::new_v7(),
            });
            self.changes.send_modify(|epoch| *epoch += 1);
        }
    }
    impl novarocks_query_application::api::BackendProcessObservationPort for PendingProcessObservation {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.changes.subscribe()
        }
        fn observe_process_at_endpoint(
            &self,
            expected: novarocks_types::BackendProcessId,
            endpoint: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, novarocks_query_application::api::BackendTopologyError>
        {
            assert_eq!(expected, self.expected);
            assert_eq!(
                *endpoint,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap()
            );
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.observation.lock().unwrap().clone()
        }
    }

    fn pending_pre_ready_round(
        round: TaskRound,
        source: Arc<dyn novarocks_query_application::api::BackendProcessObservationPort>,
    ) -> ManifestAssembledRound {
        let context = *round.execution().graph().contexts().next().unwrap();
        let split_delivery = SplitDeliveryBridge::for_graph(round.execution().graph());
        let (_, actor_gate, _) = ActorGatedTaskOperationSink::pair(
            Arc::new(BackpressuredGateTransport),
            Arc::new(super::super::status_intake::CountingWake::default()),
            novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        );
        let convergence = NativeAttemptConvergenceOwner::new(
            &round,
            source,
            vec![NativeAttemptConvergenceTarget::new(
                context,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                    .unwrap(),
            )],
        )
        .unwrap();
        let mut assembled = ManifestAssembledRound {
            round,
            split_delivery,
            actor_gate,
            convergence,
            notify: Arc::new(tokio::sync::Notify::new()),
            terminal: None,
            pending_terminal: None,
            pending_pre_ready_unknown: None,
        };
        assembled.record_protocol_failure(TaskExecutionError::PreReadyEstablishTransportUnknown {
            backend: context.backend_process_id(),
        });
        assembled
    }

    #[test]
    fn pending_unknown_uses_original_ticket_horizon_and_only_positive_exact_replacement() {
        for state in [
            BackendProcessObservation::Current,
            BackendProcessObservation::Unobservable,
            BackendProcessObservation::Replaced {
                current_process: novarocks_types::BackendProcessId::new_v7(),
            },
        ] {
            let (round, clock) = super::super::tests::pre_ready_observation_round();
            let backend = round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap()
                .backend_process_id();
            let source = Arc::new(PendingProcessObservation::new(backend, state));
            let mut assembled = pending_pre_ready_round(round, source.clone());
            assert!(
                !assembled
                    .convergence
                    .process_replaced(novarocks_types::BackendProcessId::new_v7())
            );
            assert_eq!(source.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let replaced = matches!(state, BackendProcessObservation::Replaced { .. });
            assert_eq!(assembled.pending_pre_ready_must_wait(), !replaced);
            if !replaced {
                clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
                assert!(!assembled.pending_pre_ready_must_wait());
            }
            let Some(NativeAttemptTerminal::Failed(failure)) = assembled.pending_terminal else {
                panic!("expected original failure")
            };
            assert_eq!(
                failure.class(),
                if replaced {
                    AttemptFailureClass::RecoverableInfrastructure
                } else {
                    AttemptFailureClass::ExecutionFailure
                }
            );
        }
        // The observer itself cannot authorize a foreign or equal process.
        for error in [false, true] {
            let (round, _) = super::super::tests::pre_ready_observation_round();
            let backend = round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap()
                .backend_process_id();
            let source = Arc::new(PendingProcessObservation::new(
                backend,
                BackendProcessObservation::Replaced {
                    current_process: backend,
                },
            ));
            if error {
                *source.observation.lock().unwrap() = Err(
                    novarocks_query_application::api::BackendTopologyError::Unavailable {
                        message: "original observer".into(),
                    },
                );
            }
            let mut assembled = pending_pre_ready_round(round, source);
            assert!(assembled.pending_pre_ready_must_wait());
            let Some(NativeAttemptTerminal::Failed(failure)) = &assembled.pending_terminal else {
                panic!("expected original failure")
            };
            assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        }
    }

    async fn finish_pending_actor(
        mut logical: novarocks_query_application::test_support::LogicalExecutionTestHarness,
        context: novarocks_execution_contract::QueryContextRef,
    ) {
        logical.abandon_running_attempt();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while logical
                .stand_down_snapshot(context)
                .await
                .unwrap()
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        logical
            .observe_worker_process_replaced(context)
            .await
            .unwrap();
        logical
            .finish_until(std::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pending_unknown_without_query_deadline_observes_replacement_before_original_ticket_expiry()
     {
        let (round, _) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        assert_eq!(cancellation.deadline(), None);
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Unobservable,
        ));
        let mut assembled = pending_pre_ready_round(round, source.clone());
        let update = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            source.replace();
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                update,
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected backend loss")
        };
        assert_eq!(
            failure.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::ExcludeProcess(
                context.backend_process_id()
            )
        );
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_unknown_cancellation_returns_original_owner_without_waiting_for_replacement() {
        let (round, _) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, requester, _abort) =
            terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        requester.request(CancellationReason::Requested).unwrap();
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected cancellation")
        };
        assert_eq!(failure.class(), AttemptFailureClass::Cancelled);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_unknown_without_query_deadline_stops_at_original_ticket_horizon() {
        let (round, clock) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        assert_eq!(cancellation.deadline(), None);
        let source = Arc::new(PendingProcessObservation::new(
            context.backend_process_id(),
            BackendProcessObservation::Current,
        ));
        let mut assembled = pending_pre_ready_round(round, source.clone());
        let expire = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                expire,
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected original uncertainty")
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_unknown_cannot_hide_an_accepted_originating_task_failure() {
        let round = super::super::tests::manifest_originating_failure_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        let root = assembled.round.execution().graph().root_identity();
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected original Task failure")
        };
        assert_eq!(
            failure.input().detail().as_str(),
            "original preparing Task failure"
        );
        assert_eq!(
            failure.input().site(),
            novarocks_query_application::execution_error::ExecutionFailureSite::Task {
                identity: root,
                phase: novarocks_execution_contract::TaskFailurePhase::Preparation,
            }
        );
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[test]
    fn pending_unknown_without_original_establish_window_never_invents_a_deadline() {
        for replaced in [false, true] {
            let round = super::super::tests::manifest_preparing_observation_round();
            let backend = round
                .execution()
                .graph()
                .contexts()
                .next()
                .unwrap()
                .backend_process_id();
            assert_eq!(
                round.execution().pre_ready_observation_remaining(backend),
                None
            );
            let state = if replaced {
                BackendProcessObservation::Replaced {
                    current_process: novarocks_types::BackendProcessId::new_v7(),
                }
            } else {
                BackendProcessObservation::Unobservable
            };
            let source = Arc::new(PendingProcessObservation::new(backend, state));
            let mut assembled = pending_pre_ready_round(round, source);
            assert!(!assembled.pending_pre_ready_must_wait());
            let Some(NativeAttemptTerminal::Failed(failure)) = assembled.pending_terminal else {
                panic!("expected failure")
            };
            assert_eq!(
                failure.class(),
                if replaced {
                    AttemptFailureClass::RecoverableInfrastructure
                } else {
                    AttemptFailureClass::ExecutionFailure
                }
            );
        }
    }
    #[test]
    fn a_real_unsent_replay_deadline_stops_unknown_observation_without_backend_loss() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureSite, QueryFailureReason,
        };
        let (round, error) = super::super::tests::pre_ready_expired_replay_observation_round();
        assert!(matches!(
            error,
            TaskExecutionError::QueueResidenceExpired {
                kind: novarocks_execution_contract::OperationKind::UpdateQueryContext,
                ..
            }
        ));
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        assembled.record_protocol_failure(error);
        assert_eq!(assembled.pending_pre_ready_unknown, None);
        assert!(!assembled.pending_pre_ready_must_wait());
        let Some(NativeAttemptTerminal::Failed(failure)) = assembled.pending_terminal else {
            panic!("a definitive local deadline must retain its original author");
        };
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::TransportDeadline.failure_fact()
        );
        assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
    }

    #[tokio::test]
    async fn pending_unknown_closed_process_watch_keeps_original_ticket_and_failure() {
        struct ClosedProcessObservation {
            expected: novarocks_types::BackendProcessId,
            calls: std::sync::atomic::AtomicUsize,
        }
        impl novarocks_query_application::api::BackendProcessObservationPort for ClosedProcessObservation {
            // The default subscription is closed. Its closure is not a
            // replacement and must not grant a fresh observation horizon.
            fn observe_process_at_endpoint(
                &self,
                expected: novarocks_types::BackendProcessId,
                endpoint: &novarocks_execution::runtime::endpoint::RuntimeEndpoint,
            ) -> Result<
                BackendProcessObservation,
                novarocks_query_application::api::BackendTopologyError,
            > {
                assert_eq!(expected, self.expected);
                assert_eq!(
                    *endpoint,
                    novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 1)
                        .unwrap()
                );
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(BackendProcessObservation::Unobservable)
            }
        }
        let (round, clock) = super::super::tests::pre_ready_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let source = Arc::new(ClosedProcessObservation {
            expected: context.backend_process_id(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut assembled = pending_pre_ready_round(round, source.clone());
        let expire = async {
            while source.calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            clock.advance(super::super::context_owner::ADMISSION_TICKET_VALID_FOR);
        };
        let (terminal, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(
                assembled.run(
                    &drive,
                    cancellation,
                    ManifestAttemptCompletion::AcceptedRootSuccessSeal
                ),
                expire,
            )
        })
        .await
        .unwrap();
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected original uncertainty")
        };
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
        assert_eq!(
            failure.topology_requirement(),
            novarocks_query_application::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }
    #[tokio::test]
    async fn pending_receiver_definitive_error_keeps_normal_priority_over_cancel_view() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::QueryFailureReason;
        let round = super::super::tests::pre_ready_invalid_ack_observation_round();
        let context = *round.execution().graph().contexts().next().unwrap();
        let (logical, drive, cancellation, requester, _abort) =
            terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        requester.request(CancellationReason::Requested).unwrap();
        // The receiver sees a genuine protocol invariant in this same turn.
        // Existing non-unknown error priority must not depend on whether an
        // earlier transport candidate had been awaiting process evidence.
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("expected protocol failure")
        };
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }

    #[tokio::test]
    async fn pending_receiver_task_failure_cannot_hide_a_later_same_turn_protocol_error() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        let round =
            super::super::round::pending_task_failure_then_invalid_covered_frame_round().await;
        let context = *round.execution().graph().contexts().next().unwrap();
        let root = round.execution().graph().root_identity();
        let (logical, drive, cancellation, _, _abort) = terminal_regression_drive(context).await;
        let mut assembled = pending_pre_ready_round(round, Arc::new(UnusedConvergenceSource));
        let terminal = assembled
            .run(
                &drive,
                cancellation,
                ManifestAttemptCompletion::AcceptedRootSuccessSeal,
            )
            .await;
        // The first valid frame really reached its original Task owner before
        // the second frame failed. No transaction rollback erases that fact.
        let selected = ExecutionFailureInput::from_selected_task(
            assembled.round.execution().selected_task_failure().unwrap(),
        )
        .unwrap();
        assert_eq!(
            selected.fact(),
            QueryFailureReason::PreparationFailed.failure_fact()
        );
        assert_eq!(
            selected.site(),
            ExecutionFailureSite::Task {
                identity: root,
                phase: novarocks_execution_contract::TaskFailurePhase::Preparation,
            }
        );
        assert_eq!(
            selected.detail().as_str(),
            "original Task before invalid frame"
        );
        let NativeAttemptTerminal::Failed(failure) = terminal else {
            panic!("the invalid covered frame must preserve the protocol failure");
        };
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        drop(assembled);
        drop(drive);
        finish_pending_actor(logical, context).await;
    }
}
