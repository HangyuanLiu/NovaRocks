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

//! Actor-owned root-result pumping.
//!
//! Role adapters supply bounded Backend-encoded root items. This module owns
//! ordered relay, acknowledgement, status progression and terminal handoff.
//! It has no Arrow decoder or execution-kernel dependency.

use std::{future::Future, num::NonZeroU64, pin::Pin, sync::Arc};

use novarocks_execution_contract::root_result::RootResultRead;
use novarocks_execution_contract::{
    AbortCause, QueryContextRef, ResultPacketSequence, TaskFailureCategory, TaskIdentity,
    TaskState, TaskStatus, TerminationDetail,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{CancellationReason, CancellationView, WorkError, WorkScope};
use tokio::sync::{oneshot, watch};

use crate::api::{BoundedRootReadPort, RetainedRootReply};
use crate::api::{NativeAttemptTerminal, NativeAttemptTopologyRequirement};
use crate::api::{QueryExecutionError, QueryExecutionErrorKind};

use super::{
    AttemptFailureClass, LogicalConclusion, LogicalExecutionActor, LogicalExecutionActorError,
    ReplacementQualification, RootResultObserver, RootTerminalFailure, RunningAttemptHandoffError,
    RunningAttemptPermit,
};

pub(crate) struct NativeAttemptTerminalSender {
    sender: Option<oneshot::Sender<NativeAttemptTerminal>>,
}

pub(crate) struct NativeAttemptTerminalSource {
    receiver: Option<oneshot::Receiver<NativeAttemptTerminal>>,
}

pub(crate) fn native_attempt_terminal_channel()
-> (NativeAttemptTerminalSender, NativeAttemptTerminalSource) {
    let (sender, receiver) = oneshot::channel();
    (
        NativeAttemptTerminalSender {
            sender: Some(sender),
        },
        NativeAttemptTerminalSource {
            receiver: Some(receiver),
        },
    )
}

impl NativeAttemptTerminalSender {
    pub(crate) fn publish(mut self, terminal: NativeAttemptTerminal) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(terminal);
        }
    }
}

impl NativeAttemptTerminalSource {
    async fn next(&mut self) -> Result<NativeAttemptTerminal, QueryExecutionError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| contract_error("Native attempt terminal was consumed more than once"))?;
        let terminal = receiver.await.map_err(|_| {
            contract_error("Native attempt terminal owner dropped without a verdict")
        })?;
        self.receiver = None;
        Ok(terminal)
    }

    fn try_next(&mut self) -> Result<Option<NativeAttemptTerminal>, QueryExecutionError> {
        let receiver = self
            .receiver
            .as_mut()
            .ok_or_else(|| contract_error("Native attempt terminal was consumed more than once"))?;
        match receiver.try_recv() {
            Ok(terminal) => {
                self.receiver = None;
                Ok(Some(terminal))
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(None),
            Err(oneshot::error::TryRecvError::Closed) => Err(contract_error(
                "Native attempt terminal owner dropped without a verdict",
            )),
        }
    }
}

/// Typed adapter failure and its attempt-level recovery class.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultFetchFailure {
    topology_requirement: NativeAttemptTopologyRequirement,
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl RootResultFetchFailure {
    pub const fn with_topology_requirement(
        mut self,
        requirement: NativeAttemptTopologyRequirement,
    ) -> Self {
        self.topology_requirement = requirement;
        self
    }

    pub fn new(class: AttemptFailureClass, error: QueryExecutionError) -> Self {
        Self {
            class,
            error,
            topology_requirement: NativeAttemptTopologyRequirement::LiveSnapshot,
        }
    }

    pub const fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.topology_requirement
    }

    pub const fn class(&self) -> AttemptFailureClass {
        self.class
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

/// Single accepted-status projection. Frontend publishes the complete
/// snapshot in the same serial turn that its TaskRound accepts it.
#[doc(hidden)]
pub struct AcceptedRootStatusSender {
    root: TaskIdentity,
    sender: watch::Sender<Option<AcceptedRootProjection>>,
}

#[doc(hidden)]
pub struct AcceptedRootStatusSource {
    root: TaskIdentity,
    receiver: watch::Receiver<Option<AcceptedRootProjection>>,
    observed: Option<AcceptedRootProjection>,
    root_control_port: Option<Arc<dyn AcceptedRootControlPort>>,
}

/// Frontend's single serialized path for ordering result-control requirements
/// against accepted Task status. Implementations enqueue the request beside status events and
/// must not decide it on the caller's async task.
#[doc(hidden)]
pub trait AcceptedRootControlPort: std::fmt::Debug + Send + Sync {
    fn enqueue_root_control(
        &self,
        request: AcceptedRootControlRequest,
    ) -> Result<(), AcceptedRootControlRequest>;
}

/// Move-only result-control request. Success sealing consumes its status
/// publisher; terminal-control registration records a nonrenewable evidence
/// requirement in that same serialized owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct AcceptedRootControlRequest {
    kind: AcceptedRootControlRequestKind,
    reply: oneshot::Sender<Result<(), QueryExecutionError>>,
}

#[derive(Clone, Copy, Debug)]
enum AcceptedRootControlRequestKind {
    SuccessSeal,
    AwaitTerminalControl(TaskIdentity),
}

impl AcceptedRootControlRequest {
    pub fn terminal_control_root(&self) -> Option<TaskIdentity> {
        match self.kind {
            AcceptedRootControlRequestKind::AwaitTerminalControl(root) => Some(root),
            AcceptedRootControlRequestKind::SuccessSeal => None,
        }
    }

    pub fn accept_terminal_control(self) -> Result<(), QueryExecutionError> {
        if self.terminal_control_root().is_none() {
            return Err(contract_error(
                "success request cannot register terminal control",
            ));
        }
        let _ = self.reply.send(Ok(()));
        Ok(())
    }

    pub fn accept(self, sender: AcceptedRootStatusSender) -> Result<(), QueryExecutionError> {
        if self.terminal_control_root().is_some() {
            let error = contract_error("terminal control request cannot seal success");
            let _ = self.reply.send(Err(error.clone()));
            return Err(error);
        }
        let result = sender.seal_success();
        let _ = self.reply.send(result.clone());
        result
    }

    pub fn reject(self, error: QueryExecutionError) {
        let _ = self.reply.send(Err(error));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AcceptedRootProjection {
    Observation(AcceptedRootObservation),
    SuccessSealed(TaskStatus),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AcceptedRootObservation {
    status: TaskStatus,
    attempt_failure: AcceptedAttemptFailure,
}

#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AcceptedAttemptFailure {
    None,
    DerivedPending,
    Authoritative(TerminationDetail),
}

#[cfg(test)]
fn accepted_root_status_projection(
    root: TaskIdentity,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    accepted_root_status_projection_inner(root, None)
}

#[doc(hidden)]
pub fn accepted_root_status_projection_with_control_port(
    root: TaskIdentity,
    root_control_port: Arc<dyn AcceptedRootControlPort>,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    accepted_root_status_projection_inner(root, Some(root_control_port))
}

fn accepted_root_status_projection_inner(
    root: TaskIdentity,
    root_control_port: Option<Arc<dyn AcceptedRootControlPort>>,
) -> (AcceptedRootStatusSender, AcceptedRootStatusSource) {
    let (sender, receiver) = watch::channel(None);
    (
        AcceptedRootStatusSender { root, sender },
        AcceptedRootStatusSource {
            root,
            receiver,
            observed: None,
            root_control_port,
        },
    )
}

impl AcceptedRootStatusSender {
    #[cfg(test)]
    fn publish(&self, status: TaskStatus) -> Result<(), QueryExecutionError> {
        let attempt_failure = if status
            .termination()
            .is_some_and(|detail| !detail.is_success_compatible())
        {
            AcceptedAttemptFailure::DerivedPending
        } else {
            AcceptedAttemptFailure::None
        };
        self.publish_attempt_observation(status, attempt_failure)
    }

    /// Atomically publishes the exact root status and the serialized
    /// attempt-level failure latch observed in the same owner turn.
    pub fn publish_attempt_observation(
        &self,
        status: TaskStatus,
        attempt_failure: AcceptedAttemptFailure,
    ) -> Result<(), QueryExecutionError> {
        if let AcceptedAttemptFailure::Authoritative(authoritative) = &attempt_failure
            && (authoritative.is_derived() || authoritative.is_success_compatible())
        {
            return Err(contract_error(
                "authoritative attempt failure must be a non-derived failure cause",
            ));
        }
        self.publish_observation(AcceptedRootObservation {
            status,
            attempt_failure,
        })
    }

    /// Publishes the current root status together with the non-derived cause
    /// selected by the same serialized attempt status owner.
    ///
    /// The authoritative attempt failure may originate from another Task, so
    /// the root itself need not be terminal or failed.
    pub fn publish_with_attempt_failure(
        &self,
        status: TaskStatus,
        authoritative_attempt_failure: TerminationDetail,
    ) -> Result<(), QueryExecutionError> {
        self.publish_attempt_observation(
            status,
            AcceptedAttemptFailure::Authoritative(authoritative_attempt_failure),
        )
    }

    fn publish_observation(
        &self,
        observation: AcceptedRootObservation,
    ) -> Result<(), QueryExecutionError> {
        let status = &observation.status;
        if status.identity() != self.root {
            return Err(contract_error("accepted root status names another Task"));
        }
        if let Some(AcceptedRootProjection::Observation(held)) = self.sender.borrow().as_ref() {
            let cause_refinement = held.status == observation.status
                && matches!(
                    (&held.attempt_failure, &observation.attempt_failure),
                    (
                        AcceptedAttemptFailure::None,
                        AcceptedAttemptFailure::DerivedPending
                            | AcceptedAttemptFailure::Authoritative(_)
                    ) | (
                        AcceptedAttemptFailure::DerivedPending,
                        AcceptedAttemptFailure::Authoritative(_)
                    )
                );
            if status.version() < held.status.version()
                || (status.version() == held.status.version()
                    && observation != *held
                    && !cause_refinement)
                || (held.status.is_terminal() && observation.status != held.status)
            {
                return Err(contract_error(
                    "accepted root status regressed or overwrote a published version",
                ));
            }
            if observation == *held {
                return Ok(());
            }
        } else if self.sender.borrow().is_some() {
            return Err(contract_error(
                "accepted root status cannot be published after success was sealed",
            ));
        }
        self.sender
            .send_replace(Some(AcceptedRootProjection::Observation(observation)));
        Ok(())
    }

    /// Consumes the only publisher and seals that the serialized attempt
    /// owner proved success. The carried Finished snapshot lets a watch
    /// receiver observe status and seal atomically even if it skipped the
    /// immediately preceding observation.
    fn seal_success(self) -> Result<(), QueryExecutionError> {
        let status = match self.sender.borrow().as_ref() {
            Some(AcceptedRootProjection::Observation(observation))
                if observation.status.state() == TaskState::Finished
                    && matches!(observation.attempt_failure, AcceptedAttemptFailure::None) =>
            {
                observation.status.clone()
            }
            _ => {
                return Err(contract_error(
                    "success seal requires a Finished root and no attempt failure",
                ));
            }
        };
        self.sender
            .send_replace(Some(AcceptedRootProjection::SuccessSealed(status)));
        Ok(())
    }
}

impl AcceptedRootStatusSource {
    fn try_next(&mut self) -> Option<AcceptedRootProjection> {
        let current = self.receiver.borrow_and_update().clone();
        if let Some(status) = current
            && self.observed.as_ref() != Some(&status)
        {
            self.observed = Some(status.clone());
            Some(status)
        } else {
            None
        }
    }

    async fn next(&mut self) -> Result<AcceptedRootProjection, QueryExecutionError> {
        loop {
            let current = self.receiver.borrow_and_update().clone();
            if let Some(status) = current
                && self.observed.as_ref() != Some(&status)
            {
                self.observed = Some(status.clone());
                return Ok(status);
            }
            self.receiver.changed().await.map_err(|_| {
                contract_error(
                    "accepted root status source closed without an explicit success seal",
                )
            })?;
        }
    }

    pub fn begin_terminal_control_request(
        &self,
    ) -> Result<oneshot::Receiver<Result<(), QueryExecutionError>>, QueryExecutionError> {
        let Some(port) = self.root_control_port.as_ref() else {
            return Err(contract_error(
                "accepted root source has no serialized terminal control port",
            ));
        };
        let (reply, receiver) = oneshot::channel();
        port.enqueue_root_control(AcceptedRootControlRequest {
            kind: AcceptedRootControlRequestKind::AwaitTerminalControl(self.root),
            reply,
        })
        .map_err(|_| contract_error("terminal control request intake is closed"))?;
        Ok(receiver)
    }

    /// Requests the serialized attempt owner's decision after the result owner
    /// consumed exact EOS. This request carries no authority to seal success.
    #[doc(hidden)]
    pub fn begin_success_seal_request(
        &self,
    ) -> Result<oneshot::Receiver<Result<(), QueryExecutionError>>, QueryExecutionError> {
        let Some(port) = self.root_control_port.as_ref() else {
            return Err(contract_error(
                "accepted root status source has no serialized success-seal port",
            ));
        };
        let (reply, outcome) = oneshot::channel();
        port.enqueue_root_control(AcceptedRootControlRequest {
            kind: AcceptedRootControlRequestKind::SuccessSeal,
            reply,
        })
        .map_err(|_| contract_error("success-seal request intake is closed"))?;
        Ok(outcome)
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub enum ResultPumpFailure {
    DecisionPending(ResultPumpDecision),
    Concluded(ConcludedResultPumpFailure),
    ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure),
}

/// An attempt failure whose final retry-or-fail decision still belongs to the
/// running permit owner.
#[doc(hidden)]
#[derive(Debug)]
pub struct ResultPumpDecision {
    topology_requirement: NativeAttemptTopologyRequirement,
    permit: RunningAttemptPermit,
    observer: Option<RootResultObserver>,
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl ResultPumpDecision {
    pub const fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.topology_requirement
    }

    pub const fn class(&self) -> AttemptFailureClass {
        self.class
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }

    /// Installs the final typed failure before releasing the last root
    /// observer, so observer loss cannot race and replace the chosen result.
    pub async fn fail_logical(
        self,
        actor: &LogicalExecutionActor,
    ) -> Result<LogicalConclusion, LogicalExecutionActorError> {
        let permit = self.permit;
        let result = actor
            .fail_attempt_with_error(permit, self.error.clone())
            .await;
        drop(self.observer);
        result
    }

    /// Transfers the retained attempt authority into replacement while the
    /// root observer remains alive through the actor's decision turn.
    pub async fn begin_replacement(
        self,
        actor: &LogicalExecutionActor,
        replacement: QueryExecutionId,
        replacement_contexts: Vec<QueryContextRef>,
    ) -> Result<(ReplacementQualification, QueryExecutionError), LogicalExecutionActorError> {
        let permit = self.permit;
        let qualification = actor
            .begin_replacement_with_error(
                permit,
                self.class,
                self.error.clone(),
                replacement,
                replacement_contexts,
            )
            .await?;
        drop(self.observer);
        Ok((qualification, self.error))
    }
}

/// The actor already fixed the logical terminal state; no attempt authority is
/// returned and no caller can ask this value to begin replacement.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcludedResultPumpFailure {
    conclusion: LogicalConclusion,
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl ConcludedResultPumpFailure {
    pub const fn conclusion(&self) -> LogicalConclusion {
        self.conclusion
    }

    pub const fn class(&self) -> AttemptFailureClass {
        self.class
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

/// The actor accepted the running-attempt authority, but no actor reply proved
/// which logical conclusion, if any, was fixed.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorOutcomeUnknownResultPumpFailure {
    error: QueryExecutionError,
}

impl ActorOutcomeUnknownResultPumpFailure {
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

enum PumpInterruption {
    Decision(RootResultFetchFailure),
    Cancellation(RootResultFetchFailure),
    Concluded(LogicalConclusion, RootResultFetchFailure),
}

struct PumpRuntime {
    observer: RootResultObserver,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
    cancellation: CancellationView,
    root_finished: bool,
    success_sealed: bool,
    native_completed: bool,
    pending_root_failure: Option<TaskStatus>,
}

impl PumpRuntime {
    fn observe_ready_native_terminal(&mut self) -> Result<(), PumpInterruption> {
        if self.native_completed {
            return Ok(());
        }
        if let Some(terminal) = self
            .native_terminal
            .try_next()
            .map_err(|error| PumpInterruption::Decision(contract_failure(error)))?
        {
            self.observe_native_terminal(Ok(terminal))?;
        }
        Ok(())
    }

    fn observe_native_terminal(
        &mut self,
        terminal: Result<NativeAttemptTerminal, QueryExecutionError>,
    ) -> Result<(), PumpInterruption> {
        match terminal {
            Ok(NativeAttemptTerminal::Completed) => {
                self.native_completed = true;
                Ok(())
            }
            Ok(NativeAttemptTerminal::Failed(failure)) => {
                Err(PumpInterruption::Decision(RootResultFetchFailure {
                    class: failure.class(),
                    error: failure.error().clone(),
                    topology_requirement: failure.topology_requirement(),
                }))
            }
            Err(error) => Err(PumpInterruption::Decision(contract_failure(error))),
        }
    }

    async fn observe_projection(
        &mut self,
        projection: AcceptedRootProjection,
    ) -> Result<(), PumpInterruption> {
        match projection {
            AcceptedRootProjection::Observation(observation) => {
                self.observe_status(observation).await
            }
            AcceptedRootProjection::SuccessSealed(status) => {
                if self.pending_root_failure.is_some() || status.state() != TaskState::Finished {
                    return Err(PumpInterruption::Decision(contract_failure(
                        contract_error("invalid success seal for the accepted root status"),
                    )));
                }
                self.observe_status(AcceptedRootObservation {
                    status,
                    attempt_failure: AcceptedAttemptFailure::None,
                })
                .await?;
                if !self.root_finished {
                    return Err(PumpInterruption::Decision(contract_failure(
                        contract_error("success seal did not carry a Finished root status"),
                    )));
                }
                self.success_sealed = true;
                Ok(())
            }
        }
    }

    async fn observe_status(
        &mut self,
        observation: AcceptedRootObservation,
    ) -> Result<(), PumpInterruption> {
        let AcceptedRootObservation {
            status,
            attempt_failure,
        } = observation;
        if self.pending_root_failure.is_some() {
            if matches!(attempt_failure, AcceptedAttemptFailure::DerivedPending) {
                // Pending causal authority does not freeze the root's Task version.
                // The actor's existing cursor/transition owner validates each
                // exact-root progression and still forbids terminal overwrite.
                return match self
                    .observer
                    .observe_status_with_failure(status.clone(), RootTerminalFailure::Pending)
                    .await
                {
                    Err(LogicalExecutionActorError::RootAttemptTerminal) => {
                        self.pending_root_failure = Some(status);
                        Ok(())
                    }
                    Err(error) => Err(PumpInterruption::Decision(actor_failure(
                        "advance pending root Task status",
                        error,
                    ))),
                    Ok(()) => Err(PumpInterruption::Decision(contract_failure(
                        contract_error(
                            "pending root failure progression did not retain a terminal disposition",
                        ),
                    ))),
                };
            }
            let AcceptedAttemptFailure::Authoritative(authoritative) = attempt_failure else {
                return Err(PumpInterruption::Decision(contract_failure(
                    contract_error(
                        "pending attempt failure cannot lose its causal authority requirement",
                    ),
                )));
            };
            let failure =
                classify_root_termination(&status, Some(&authoritative)).ok_or_else(|| {
                    PumpInterruption::Decision(contract_failure(contract_error(
                        "authoritative attempt failure did not classify the pending root terminal",
                    )))
                })?;
            return match self
                .observer
                .observe_status_with_failure(
                    status,
                    RootTerminalFailure::Authoritative(failure.error.clone()),
                )
                .await
            {
                Err(LogicalExecutionActorError::RootAttemptTerminal) => {
                    Err(PumpInterruption::Decision(failure))
                }
                Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
                    Err(PumpInterruption::Concluded(conclusion, failure))
                }
                Err(error) => Err(PumpInterruption::Decision(actor_failure(
                    "refine root Task failure",
                    error,
                ))),
                Ok(()) => Err(PumpInterruption::Decision(contract_failure(
                    contract_error(
                        "authoritative root failure refinement did not retain a terminal disposition",
                    ),
                ))),
            };
        }
        let finished = status.state() == TaskState::Finished;
        let terminal_failure = match &attempt_failure {
            AcceptedAttemptFailure::None => classify_root_termination(&status, None),
            AcceptedAttemptFailure::DerivedPending => None,
            AcceptedAttemptFailure::Authoritative(authoritative) => {
                classify_root_termination(&status, Some(authoritative))
            }
        };
        let terminal_actor_fact = match (&attempt_failure, terminal_failure.as_ref()) {
            (AcceptedAttemptFailure::DerivedPending, _) => RootTerminalFailure::Pending,
            (_, Some(failure)) => RootTerminalFailure::Authoritative(failure.error.clone()),
            _ => RootTerminalFailure::Unspecified,
        };
        match self
            .observer
            .observe_status_with_failure(status.clone(), terminal_actor_fact)
            .await
        {
            Ok(()) => {
                self.root_finished |= finished;
                Ok(())
            }
            Err(LogicalExecutionActorError::RootAttemptTerminal) if terminal_failure.is_some() => {
                Err(PumpInterruption::Decision(terminal_failure.expect(
                    "terminal failure was checked before actor observation",
                )))
            }
            Err(LogicalExecutionActorError::RootAttemptTerminal)
                if matches!(attempt_failure, AcceptedAttemptFailure::DerivedPending) =>
            {
                self.pending_root_failure = Some(status);
                Ok(())
            }
            Err(LogicalExecutionActorError::ExecutionConcluded(conclusion))
                if terminal_failure.is_some() =>
            {
                Err(PumpInterruption::Concluded(
                    conclusion,
                    terminal_failure
                        .expect("terminal failure was checked before actor observation"),
                ))
            }
            Err(error @ LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
                Err(PumpInterruption::Concluded(
                    conclusion,
                    actor_failure("observe root Task status", error),
                ))
            }
            Err(error) => Err(PumpInterruption::Decision(actor_failure(
                "observe root Task status",
                error,
            ))),
        }
    }

    /// Waits until the serialized Task owner has accepted at least one status
    /// for the exact root. The Frontend publishes this projection only after
    /// Installed proves result capability, or a terminal preparation failure
    /// makes a fetch unnecessary. A result fetch therefore cannot overtake
    /// root installation on its owning Worker.
    async fn await_initial_root_status(&mut self) -> Result<(), PumpInterruption> {
        loop {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                    return Ok(());
                }
            }
        }
    }

    async fn await_step<F, T>(&mut self, future: F) -> Result<T, PumpInterruption>
    where
        F: Future<Output = T>,
    {
        tokio::pin!(future);
        loop {
            if self.pending_root_failure.is_some() {
                tokio::select! {
                    biased;
                    reason = self.cancellation.cancelled() => {
                        return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                    }
                    terminal = self.native_terminal.next(), if !self.native_completed => {
                        self.observe_native_terminal(terminal)?;
                    }
                    status = self.statuses.next() => {
                        let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                        self.observe_projection(projection).await?;
                    }
                }
                continue;
            }
            if self.success_sealed {
                return tokio::select! {
                    biased;
                    reason = self.cancellation.cancelled() => Err(PumpInterruption::Cancellation(cancellation_failure(reason))),
                    terminal = self.native_terminal.next(), if !self.native_completed => {
                        self.observe_native_terminal(terminal)?;
                        continue;
                    }
                    output = &mut future => {
                        self.observe_ready_native_terminal()?;
                        Ok(output)
                    },
                };
            }
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                }
                output = &mut future => {
                    self.observe_ready_native_terminal()?;
                    return Ok(output);
                },
            }
        }
    }

    async fn await_terminal_control(&mut self) -> PumpInterruption {
        if let Some(projection) = self.statuses.try_next()
            && let Err(interruption) = self.observe_projection(projection).await
        {
            return interruption;
        }
        if self.native_completed || self.success_sealed {
            return PumpInterruption::Decision(contract_failure(contract_error(
                "revoked root output has no pending native terminal authority",
            )));
        }
        let reply = match self.statuses.begin_terminal_control_request() {
            Ok(reply) => reply,
            Err(error) => return PumpInterruption::Decision(contract_failure(error)),
        };
        match self
            .await_step(async move {
                reply
                    .await
                    .map_err(|_| contract_error("terminal control registration owner dropped"))?
            })
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return PumpInterruption::Decision(contract_failure(error)),
            Err(interruption) => return interruption,
        }
        if let Some(projection) = self.statuses.try_next()
            && let Err(interruption) = self.observe_projection(projection).await
        {
            return interruption;
        }
        if self.native_completed || self.success_sealed {
            return PumpInterruption::Decision(contract_failure(contract_error(
                "revoked root output has no pending native terminal authority",
            )));
        }
        // No further result fetch or ACK is issued. The existing Task owner
        // applies its bounded observation/transport recovery and publishes
        // either an originating failure or an explicit recovery failure.
        loop {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return PumpInterruption::Cancellation(cancellation_failure(reason));
                }
                status = self.statuses.next() => {
                    let projection = match status {
                        Ok(projection) => projection,
                        Err(error) => return PumpInterruption::Decision(contract_failure(error)),
                    };
                    if let Err(interruption) = self.observe_projection(projection).await {
                        return interruption;
                    }
                    if self.success_sealed {
                        return PumpInterruption::Decision(contract_failure(contract_error(
                            "revoked root output received a success seal",
                        )));
                    }
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    if let Err(interruption) = self.observe_native_terminal(terminal) {
                        return interruption;
                    }
                    return PumpInterruption::Decision(contract_failure(contract_error(
                        "revoked root output completed without an originating Task failure",
                    )));
                }
            }
        }
    }

    async fn await_success_seal(&mut self) -> Result<(), PumpInterruption> {
        // The serialized Task owner publishes SuccessSealed only after the
        // exact root is Finished, no authoritative attempt failure exists,
        // and every required Task creation was accepted. That is the logical
        // result boundary. Native terminal convergence may still be waiting
        // for transport ownership or residual Worker cleanup and must remain
        // observable without delaying the actor-owned success EOF.
        while !self.success_sealed {
            tokio::select! {
                biased;
                reason = self.cancellation.cancelled() => {
                    return Err(PumpInterruption::Cancellation(cancellation_failure(reason)));
                }
                terminal = self.native_terminal.next(), if !self.native_completed => {
                    self.observe_native_terminal(terminal)?;
                }
                status = self.statuses.next() => {
                    let projection = status.map_err(|error| PumpInterruption::Decision(contract_failure(error)))?;
                    self.observe_projection(projection).await?;
                }
            }
        }
        Ok(())
    }
}

/// One root stream relayed to its consumer without decode: the read port,
/// the frontier that decides each read, and the window alias every reply
/// retains until its delivery exits.
pub struct RootRelayBinding {
    pub port: Arc<dyn BoundedRootReadPort>,
    pub frontier: super::RootRelayFrontier,
    pub window: novarocks_workload_control::ResultWindowAlias,
    pub max_wait: std::time::Duration,
}

enum RelayEvent {
    Delivered(Result<(), LogicalExecutionActorError>),
    Read(Result<RetainedRootReply, RootResultFetchFailure>),
}

enum RelayExit {
    EndConsumed(
        super::RootRelayRead,
        novarocks_execution_contract::root_result::RootResultEnd,
    ),
    Interruption(PumpInterruption),
    Failure(RootResultFetchFailure),
    Actor(&'static str, LogicalExecutionActorError),
    AwaitTerminalControl,
}

type DeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), LogicalExecutionActorError>> + Send + 'a>>;
type ReadFuture =
    Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>>;

/// Relays one exact attempt's Backend-encoded root stream in order. The next
/// item is read while the previous one is written; the cumulative ACK on
/// each read is the prefix whose delivery receipts completed. Success seal
/// follows the local End consumption proof and the root's terminal facts; it
/// never waits for the Backend to acknowledge the final item.
#[doc(hidden)]
pub(crate) async fn run_root_relay(
    permit: RunningAttemptPermit,
    root: TaskIdentity,
    scope: WorkScope,
    binding: RootRelayBinding,
    statuses: AcceptedRootStatusSource,
    native_terminal: NativeAttemptTerminalSource,
) -> Result<LogicalConclusion, ResultPumpFailure> {
    let RootRelayBinding {
        port,
        mut frontier,
        window,
        max_wait,
    } = binding;
    if statuses.root != root
        || frontier.root() != root
        || permit.identity().execution() != root.query_execution_id()
        || !window.is_for_scope(&scope)
        || window.class()
            != match frontier.kind() {
                novarocks_result_contract::RootOutputKind::ClientRows => {
                    novarocks_workload_control::ResultWindowClass::Client
                }
                _ => novarocks_workload_control::ResultWindowClass::Internal,
            }
    {
        return Err(pump_failure(
            permit,
            contract_failure(contract_error(
                "root relay identity does not match its attempt, status source, frontier or admitted window",
            )),
        ));
    }
    let cancellation = match scope.cancellation() {
        Ok(cancellation) => cancellation,
        Err(error) => return Err(pump_failure(permit, work_failure(error))),
    };
    if let Some(reason) = cancellation.reason() {
        return Err(conclude_cancellation(None, permit, cancellation_failure(reason)).await);
    }
    let observer = match permit.bind_root_result(root).await {
        Ok(observer) => observer,
        Err(error) => {
            if let Some(reason) = cancellation.reason() {
                return Err(
                    conclude_cancellation(None, permit, cancellation_failure(reason)).await,
                );
            }
            return Err(pump_actor_failure(permit, "bind root result", error));
        }
    };
    let observer_owner = observer;
    let mut runtime = PumpRuntime {
        observer: observer_owner.clone(),
        statuses,
        native_terminal,
        cancellation,
        root_finished: false,
        success_sealed: false,
        native_completed: false,
        pending_root_failure: None,
    };
    if let Err(interruption) = runtime.await_initial_root_status().await {
        return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
    }

    let exit = {
        // This slot survives a pump cancellation while its delivered item
        // still owns the window; only the protocol cut may take it for closing.
        let resident = crate::api::RootRelayResidentWindow::default();
        let mut delivering: Option<(NonZeroU64, DeliveryFuture<'_>)> = None;
        let mut reading: Option<ReadFuture> = None;
        loop {
            if delivering.is_none()
                && let Some(crate::api::ResidentRootSegment {
                    sequence,
                    reply,
                    client_rows,
                    rows,
                }) = resident.take()
            {
                let packet = ResultPacketSequence::new(sequence.get() - 1);
                delivering = Some((
                    sequence,
                    Box::pin(permit.deliver_root_segment(
                        packet,
                        reply,
                        client_rows,
                        rows,
                        resident.clone(),
                    )),
                ));
            }
            if let Some(end) = frontier.end_consumed()
                && delivering.is_none()
                && resident.is_empty()
                && reading.is_none()
            {
                // The final ACK-only read is optional and never awaited here.
                let read = frontier.next_read().unwrap_or(super::RootRelayRead {
                    wanted: None,
                    consumed: frontier.consumed_through(),
                });
                break RelayExit::EndConsumed(read, end);
            }
            if reading.is_none()
                && let Some(read) = frontier.next_read()
                && read.wanted.is_some()
            {
                let request = match RootResultRead::try_new(
                    root,
                    frontier.profile(),
                    frontier.kind(),
                    read.wanted,
                    read.consumed,
                    max_wait,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        break RelayExit::Failure(contract_failure(contract_error(format!(
                            "root relay read is invalid: {error}"
                        ))));
                    }
                };
                frontier.sent(read);
                reading = Some(port.read(request, window.clone()));
            }
            if delivering.is_none() && reading.is_none() {
                break RelayExit::Failure(contract_failure(contract_error(
                    "root relay has neither a delivery nor a read in progress",
                )));
            }
            let event = runtime
                .await_step(std::future::poll_fn(|cx| {
                    if let Some((_, future)) = delivering.as_mut()
                        && let std::task::Poll::Ready(result) = future.as_mut().poll(cx)
                    {
                        return std::task::Poll::Ready(RelayEvent::Delivered(result));
                    }
                    if let Some(future) = reading.as_mut()
                        && let std::task::Poll::Ready(result) = future.as_mut().poll(cx)
                    {
                        return std::task::Poll::Ready(RelayEvent::Read(result));
                    }
                    std::task::Poll::Pending
                }))
                .await;
            match event {
                Err(interruption) => break RelayExit::Interruption(interruption),
                Ok(RelayEvent::Delivered(result)) => {
                    let (sequence, _) = delivering.take().expect("a delivery completed");
                    if let Err(error) = result {
                        break RelayExit::Actor("deliver root segment", error);
                    }
                    resident.retire(sequence);
                    if let Err(error) = frontier.receipt(sequence) {
                        break RelayExit::Failure(contract_failure(contract_error(
                            error.to_string(),
                        )));
                    }
                }
                Ok(RelayEvent::Read(result)) => {
                    reading = None;
                    let reply = match result {
                        Ok(reply) => reply,
                        Err(failure) => break RelayExit::Failure(failure),
                    };
                    let client_rows = frontier.client_rows();
                    let body = match reply.outcome() {
                        crate::api::RootReplyView::Data { body, .. } => Some(body),
                        _ => None,
                    };
                    let step = frontier.accept(reply.reply(), body);
                    match step {
                        Err(error) => {
                            break RelayExit::Failure(contract_failure(contract_error(
                                error.to_string(),
                            )));
                        }
                        Ok(super::RootRelayStep::Deliver { sequence, rows, .. }) => {
                            if !resident.publish(crate::api::ResidentRootSegment {
                                sequence,
                                reply: Arc::new(reply),
                                client_rows,
                                rows,
                            }) {
                                break RelayExit::AwaitTerminalControl;
                            }
                        }
                        Ok(super::RootRelayStep::AwaitTerminalControl) => {
                            break RelayExit::AwaitTerminalControl;
                        }
                        Ok(super::RootRelayStep::NotReady) => {
                            // A port that answers NotReady without its long
                            // poll must not starve the delivery or the runtime.
                            tokio::task::yield_now().await;
                        }
                        Ok(
                            super::RootRelayStep::EndKnown(_) | super::RootRelayStep::Acknowledged,
                        ) => {}
                    }
                }
            }
        }
    };
    let (final_read, end) = match exit {
        RelayExit::EndConsumed(read, end) => (read, end),
        RelayExit::Interruption(interruption) => {
            return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
        }
        RelayExit::Failure(failure) => {
            return Err(bound_pump_failure(&observer_owner, permit, failure).await);
        }
        RelayExit::Actor(operation, error) => {
            return Err(bound_actor_failure(&observer_owner, permit, operation, error).await);
        }
        RelayExit::AwaitTerminalControl => {
            let interruption = runtime.await_terminal_control().await;
            return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
        }
    };
    // Optional early retirement of the consumed prefix on the Backend. It is
    // neither a seal precondition nor awaited for success.
    if final_read.wanted.is_none()
        && let Ok(request) = RootResultRead::try_new(
            root,
            frontier.profile(),
            frontier.kind(),
            None,
            final_read.consumed,
            max_wait,
        )
    {
        let ack = port.read(request, window.clone());
        tokio::spawn(async move {
            let _ = ack.await;
        });
    }
    // The local End consumption proof: every data item's delivery completed.
    if let Err(error) = runtime.observer.observe_local_root_end(root, end).await {
        return Err(bound_actor_failure(
            &observer_owner,
            permit,
            "observe local root End consumption",
            error,
        )
        .await);
    }
    if !runtime.success_sealed {
        let seal_reply = match runtime.statuses.begin_success_seal_request() {
            Ok(reply) => reply,
            Err(error) => {
                return Err(
                    bound_pump_failure(&observer_owner, permit, contract_failure(error)).await,
                );
            }
        };
        let seal_result = runtime
            .await_step(async move {
                seal_reply.await.map_err(|_| {
                    contract_error("success-seal request owner dropped without a verdict")
                })?
            })
            .await;
        match seal_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(
                    bound_pump_failure(&observer_owner, permit, contract_failure(error)).await,
                );
            }
            Err(interruption) => {
                return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
            }
        }
    }
    if let Err(interruption) = runtime.await_success_seal().await {
        return Err(bound_pump_interruption(&observer_owner, permit, interruption).await);
    }
    match permit.finish_result_stream().await {
        Ok(conclusion) => Ok(conclusion),
        Err(error) => Err(finish_handoff_failure(observer_owner, error).await),
    }
}

fn pump_failure(
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    ResultPumpFailure::DecisionPending(ResultPumpDecision {
        topology_requirement: failure.topology_requirement,
        permit,
        observer: None,
        class: failure.class,
        error: failure.error,
    })
}

fn pump_actor_failure(
    permit: RunningAttemptPermit,
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    let conclusion = actor_error_conclusion(error);
    let failure = actor_failure(operation, error);
    if let Some(conclusion) = conclusion {
        drop(permit);
        concluded_pump_failure(conclusion, failure)
    } else {
        pump_failure(permit, failure)
    }
}

async fn bound_pump_failure(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    match permit
        .freeze_result_attempt_failure(failure.error.clone())
        .await
    {
        Ok(()) => ResultPumpFailure::DecisionPending(ResultPumpDecision {
            topology_requirement: failure.topology_requirement,
            permit,
            observer: Some(observer.clone()),
            class: failure.class,
            error: failure.error,
        }),
        Err(LogicalExecutionActorError::ExecutionConcluded(conclusion)) => {
            drop(permit);
            ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
                conclusion,
                class: failure.class,
                error: failure.error,
            })
        }
        Err(error) => {
            drop(permit);
            ResultPumpFailure::ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure {
                error: actor_failure("freeze result attempt failure", error).error,
            })
        }
    }
}

async fn bound_actor_failure(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    let conclusion = actor_error_conclusion(error);
    let failure = actor_failure(operation, error);
    if let Some(conclusion) = conclusion {
        drop(permit);
        concluded_pump_failure(conclusion, failure)
    } else {
        bound_pump_failure(observer, permit, failure).await
    }
}

async fn bound_pump_interruption(
    observer: &RootResultObserver,
    permit: RunningAttemptPermit,
    interruption: PumpInterruption,
) -> ResultPumpFailure {
    match interruption {
        PumpInterruption::Decision(failure) => bound_pump_failure(observer, permit, failure).await,
        PumpInterruption::Cancellation(failure) => {
            conclude_cancellation(Some(observer.clone()), permit, failure).await
        }
        PumpInterruption::Concluded(conclusion, failure) => {
            drop(permit);
            ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
                conclusion,
                class: failure.class,
                error: failure.error,
            })
        }
    }
}

async fn conclude_cancellation(
    observer: Option<RootResultObserver>,
    permit: RunningAttemptPermit,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    match permit.await_work_cancellation().await {
        Ok(conclusion) | Err(RunningAttemptHandoffError::ExecutionConcluded(conclusion)) => {
            drop(observer);
            concluded_pump_failure(conclusion, failure)
        }
        Err(RunningAttemptHandoffError::NotSubmitted { permit, .. }) => {
            if let Some(observer) = observer {
                bound_pump_failure(&observer, permit, failure).await
            } else {
                pump_failure(permit, failure)
            }
        }
        Err(RunningAttemptHandoffError::ActorOutcomeUnknown(error)) => {
            drop(observer);
            actor_outcome_unknown("await work cancellation", error)
        }
    }
}

fn concluded_pump_failure(
    conclusion: LogicalConclusion,
    failure: RootResultFetchFailure,
) -> ResultPumpFailure {
    ResultPumpFailure::Concluded(ConcludedResultPumpFailure {
        conclusion,
        class: failure.class,
        error: failure.error,
    })
}

async fn finish_handoff_failure(
    observer: RootResultObserver,
    error: RunningAttemptHandoffError,
) -> ResultPumpFailure {
    match error {
        RunningAttemptHandoffError::ExecutionConcluded(conclusion) => {
            drop(observer);
            concluded_pump_failure(
                conclusion,
                actor_failure(
                    "finish root result stream",
                    LogicalExecutionActorError::ExecutionConcluded(conclusion),
                ),
            )
        }
        RunningAttemptHandoffError::NotSubmitted { permit, error } => {
            bound_actor_failure(&observer, permit, "finish root result stream", error).await
        }
        RunningAttemptHandoffError::ActorOutcomeUnknown(error) => {
            drop(observer);
            actor_outcome_unknown("finish root result stream", error)
        }
    }
}

fn actor_error_conclusion(error: LogicalExecutionActorError) -> Option<LogicalConclusion> {
    match error {
        LogicalExecutionActorError::ExecutionConcluded(conclusion) => Some(conclusion),
        _ => None,
    }
}

fn actor_outcome_unknown(
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> ResultPumpFailure {
    ResultPumpFailure::ActorOutcomeUnknown(ActorOutcomeUnknownResultPumpFailure {
        error: actor_failure(operation, error).error,
    })
}

fn contract_error(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message)
}

fn contract_failure(error: QueryExecutionError) -> RootResultFetchFailure {
    RootResultFetchFailure::new(AttemptFailureClass::ContractViolation, error)
}

fn actor_failure(
    operation: &'static str,
    error: LogicalExecutionActorError,
) -> RootResultFetchFailure {
    RootResultFetchFailure::new(
        AttemptFailureClass::ContractViolation,
        QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            format!("{operation} failed: {error}"),
        ),
    )
}

fn work_failure(error: WorkError) -> RootResultFetchFailure {
    match error {
        WorkError::Cancelled(reason) => cancellation_failure(reason),
        WorkError::Capacity(_) | WorkError::CapacityWaitTimeout => RootResultFetchFailure::new(
            AttemptFailureClass::ResourceGovernance,
            QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                format!("result capacity is unavailable: {error}"),
            ),
        ),
        error => contract_failure(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            format!("result capacity contract failed: {error}"),
        )),
    }
}

fn cancellation_failure(reason: CancellationReason) -> RootResultFetchFailure {
    let (class, kind, message) = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => (
            AttemptFailureClass::DeadlineExceeded,
            QueryExecutionErrorKind::DeadlineExceeded,
            "logical execution deadline expired while pumping results",
        ),
        _ => (
            AttemptFailureClass::Cancelled,
            QueryExecutionErrorKind::Cancelled,
            "logical execution was cancelled while pumping results",
        ),
    };
    RootResultFetchFailure::new(class, QueryExecutionError::new(kind, message))
}

fn classify_root_termination(
    status: &TaskStatus,
    authoritative_attempt_failure: Option<&TerminationDetail>,
) -> Option<RootResultFetchFailure> {
    let detail = authoritative_attempt_failure.or_else(|| status.termination())?;
    let (class, message) = match detail {
        TerminationDetail::Canceled(reason) => (
            AttemptFailureClass::ExecutionFailure,
            format!("attempt ended without stable result success: CANCELED(reason={reason})"),
        ),
        TerminationDetail::Aborted(AbortCause::LeaseExpired) => (
            AttemptFailureClass::RecoverableInfrastructure,
            "attempt ended without stable result success: ABORTED(cause=LEASE_EXPIRED)".to_owned(),
        ),
        TerminationDetail::Aborted(AbortCause::PeerTaskFailed) => {
            return Some(contract_failure(contract_error(
                "derived root termination reached classification without an authoritative attempt failure",
            )));
        }
        TerminationDetail::Aborted(cause) => (
            AttemptFailureClass::ExecutionFailure,
            format!("attempt ended without stable result success: ABORTED(cause={cause})"),
        ),
        TerminationDetail::Failed(failure) => {
            let class = match failure.category() {
                TaskFailureCategory::ResourceExhausted => {
                    AttemptFailureClass::RecoverableInfrastructure
                }
                TaskFailureCategory::Execution | TaskFailureCategory::Exchange => {
                    AttemptFailureClass::ExecutionFailure
                }
                TaskFailureCategory::Protocol | TaskFailureCategory::Internal => {
                    AttemptFailureClass::ContractViolation
                }
            };
            (
                class,
                format!(
                    "attempt ended without stable result success: FAILED(category={}, detail={})",
                    failure.category(),
                    failure.detail()
                ),
            )
        }
    };
    Some(RootResultFetchFailure::new(
        class,
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, message),
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        num::NonZeroUsize,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use arrow::datatypes::DataType;
    use novarocks_execution_contract::{
        SafeDetail, TaskFailure, TaskOutputFacts, TaskState, TaskStatusVersion,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryId, StageId, TaskId, identity::QueryExecutionId,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, WorkClass, WorkOwner, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use tokio::{runtime::Handle, sync::Notify};

    use crate::{
        api::{ExecutionOutput, ResultDelivery, ResultField, ResultSchema},
        coordination::{
            ExecutionEffect, LogicalExecutionActor, LogicalExecutionActorConfig,
            spawn_logical_execution_actor,
        },
    };

    use super::*;

    #[derive(Debug, Default)]
    struct TestSuccessSealPort {
        requests: Mutex<VecDeque<AcceptedRootControlRequest>>,
        ready: Notify,
        terminal_roots: Mutex<VecDeque<TaskIdentity>>,
        terminal_ready: Notify,
    }

    impl AcceptedRootControlPort for TestSuccessSealPort {
        fn enqueue_root_control(
            &self,
            request: AcceptedRootControlRequest,
        ) -> Result<(), AcceptedRootControlRequest> {
            if let Some(root) = request.terminal_control_root() {
                request.accept_terminal_control().unwrap();
                self.terminal_roots.lock().unwrap().push_back(root);
                self.terminal_ready.notify_one();
                return Ok(());
            }
            self.requests.lock().unwrap().push_back(request);
            self.ready.notify_one();
            Ok(())
        }
    }

    impl TestSuccessSealPort {
        async fn next_terminal_control(&self) -> TaskIdentity {
            loop {
                if let Some(root) = self.terminal_roots.lock().unwrap().pop_front() {
                    return root;
                }
                self.terminal_ready.notified().await;
            }
        }

        async fn next(&self) -> AcceptedRootControlRequest {
            loop {
                if let Some(request) = self.requests.lock().unwrap().pop_front() {
                    return request;
                }
                self.ready.notified().await;
            }
        }
    }

    async fn poll_once<F: Future + ?Sized>(mut future: Pin<&mut F>) -> std::task::Poll<F::Output> {
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await
    }

    async fn wait_for_terminal_control_registration<F: Future + ?Sized>(
        pump: Pin<&mut F>,
        port: &TestSuccessSealPort,
        root: TaskIdentity,
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = pump => panic!("the revoked result finished before control registration"),
                registered = port.next_terminal_control() => assert_eq!(registered, root),
            }
        })
        .await
        .expect("the exact revoked root must register terminal control");
    }

    async fn poll_pending_after_actor_turn<F: Future + ?Sized>(
        mut future: Pin<&mut F>,
        actor: &LogicalExecutionActor,
    ) {
        assert!(matches!(
            poll_once(future.as_mut()).await,
            std::task::Poll::Pending
        ));
        // This exact actor mailbox barrier acknowledges the previously sent
        // root observation; advancing the test does not depend on scheduling.
        actor.snapshot().await.unwrap();
        assert!(matches!(
            poll_once(future.as_mut()).await,
            std::task::Poll::Pending
        ));
    }

    fn execution(tag: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(77, tag), AttemptId::new(1).unwrap()).unwrap()
    }

    fn root_task(execution: QueryExecutionId) -> TaskIdentity {
        TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    fn result_schema() -> ResultSchema {
        ResultSchema::new(vec![ResultField::new(
            "value",
            DataType::Int64,
            false,
            None,
        )])
    }

    fn finished(root: TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap()
    }

    fn running(root: TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(1).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap()
    }

    struct Harness {
        control: WorkloadControl,
        scope: WorkScope,
        actor: LogicalExecutionActor,
        owner: super::super::LogicalExecutionActorOwner,
        permit: RunningAttemptPermit,
        stream: crate::api::QueryResultStream,
        root: TaskIdentity,
    }

    #[tokio::test]
    async fn relay_refuses_foreign_scope_or_wrong_window_class_before_reading() {
        for foreign_scope in [false, true] {
            let (
                Harness {
                    control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(56).await;
            let (foreign, foreign_window) = relay_harness(57).await;
            let wrong_class = scope
                .result_capacity()
                .unwrap()
                .try_acquire(&scope, ResultWindowClass::Local)
                .unwrap();
            let (sender, statuses) = accepted_root_status_projection(root);
            sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(Vec::new());
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let mut binding = relay_binding(root, port.clone(), &window);
            binding.window = if foreign_scope {
                foreign_window.retain_alias()
            } else {
                wrong_class.retain_alias()
            };
            let ResultPumpFailure::DecisionPending(decision) =
                run_root_relay(permit, root, scope, binding, statuses, terminal)
                    .await
                    .unwrap_err()
            else {
                panic!("invalid window must be refused before root binding");
            };
            assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
            assert_eq!(
                decision.fail_logical(&actor).await.unwrap(),
                LogicalConclusion::Failed
            );
            assert!(stream.next().await.is_err());
            assert!(port.requests().is_empty());
            drop((
                sender,
                stream,
                actor,
                owner,
                window,
                wrong_class,
                foreign,
                foreign_window,
            ));
        }
    }

    #[tokio::test]
    async fn relay_revoked_output_registers_exact_control_and_never_polls_or_acks_again() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(54).await;
        let control = Arc::new(TestSuccessSealPort::default());
        let (sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            control.clone() as Arc<dyn AcceptedRootControlPort>,
        );
        sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(vec![RootReadOutcome::AwaitTerminalControl]);
        let (_terminal_sender, terminal) = native_attempt_terminal_channel();
        let mut relay = Box::pin(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, port.clone(), &window),
            statuses,
            terminal,
        ));
        wait_for_terminal_control_registration(relay.as_mut(), &control, root).await;
        let status = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        sender
            .publish_with_attempt_failure(
                status,
                TerminationDetail::Failed(TaskFailure::new(
                    TaskFailureCategory::Exchange,
                    SafeDetail::new("revoked root original cause").unwrap(),
                )),
            )
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap_err();
        let ResultPumpFailure::DecisionPending(decision) = error else {
            panic!("revoked output must retain its original attempt authority");
        };
        assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            decision
                .error()
                .message()
                .contains("revoked root original cause")
        );
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        assert_eq!(port.requests(), vec![(Some(1), 0)]);
        drop((sender, stream, actor, owner, window));
    }

    #[tokio::test]
    async fn relay_finished_and_native_completed_cannot_replace_an_explicit_success_seal() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(55).await;
        let control = Arc::new(TestSuccessSealPort::default());
        let (sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            control.clone() as Arc<dyn AcceptedRootControlPort>,
        );
        sender.publish(finished(root)).unwrap();
        let port = ScriptedRootPort::new(vec![rows_data(1, &[1, 0, 0, 0, b'a'], Some((2, 1)))]);
        let (terminal_sender, terminal) = native_attempt_terminal_channel();
        terminal_sender.publish(NativeAttemptTerminal::Completed);
        let relay = tokio::spawn(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, port, &window),
            statuses,
            terminal,
        ));
        next_segment(&mut stream).await.complete();
        let seal_request = tokio::time::timeout(Duration::from_secs(1), control.next())
            .await
            .unwrap();
        drop(sender);
        drop(seal_request);
        let error = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        match error {
            ResultPumpFailure::DecisionPending(decision) => {
                assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
                assert_eq!(
                    decision.fail_logical(&actor).await.unwrap(),
                    LogicalConclusion::Failed
                );
            }
            ResultPumpFailure::Concluded(concluded) => {
                assert_eq!(concluded.conclusion(), LogicalConclusion::Failed);
                assert_eq!(concluded.class(), AttemptFailureClass::ContractViolation);
            }
            _ => panic!("missing success seal must settle a typed logical failure"),
        }
        assert!(stream.next().await.is_err());
        drop((stream, actor, owner, window));
    }

    #[tokio::test]
    async fn relay_authoritative_failure_interrupts_a_read_or_held_delivery_without_ack() {
        for hold_delivery in [false, true] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(50 + i64::from(hold_delivery)).await;
            let (sender, statuses) = accepted_root_status_projection(root);
            sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(if hold_delivery {
                vec![rows_data(1, &[1, 0, 0, 0, b'a'], None)]
            } else {
                Vec::new()
            });
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let mut relay = tokio::spawn(run_root_relay(
                permit,
                root,
                scope,
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal,
            ));
            let held = if hold_delivery {
                Some(next_segment(&mut stream).await)
            } else {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while port.requests().is_empty() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                None
            };
            let status = TaskStatus::try_new(
                root,
                TaskStatusVersion::new(3).unwrap(),
                TaskState::Failed,
                Some(TerminationDetail::Failed(TaskFailure::new(
                    TaskFailureCategory::Execution,
                    SafeDetail::new("originating root failure").unwrap(),
                ))),
                TaskOutputFacts::new(false),
            )
            .unwrap();
            sender
                .publish_with_attempt_failure(status.clone(), status.termination().unwrap().clone())
                .unwrap();
            let error = match tokio::time::timeout(Duration::from_secs(1), &mut relay).await {
                Ok(result) => result.unwrap().unwrap_err(),
                Err(_) => {
                    let snapshot = actor.snapshot().await.unwrap();
                    _control.cancel_active_roots(CancellationReason::Requested);
                    let _ = tokio::time::timeout(Duration::from_secs(1), relay).await;
                    panic!(
                        "root failure stuck: held={hold_delivery}, phase={:?}, conclusion={:?}, visible={}, reads={}",
                        snapshot.phase,
                        snapshot.conclusion,
                        snapshot.output_visible,
                        port.requests().len()
                    );
                }
            };
            match error {
                ResultPumpFailure::DecisionPending(decision) if !hold_delivery => {
                    assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
                    assert!(
                        decision
                            .error()
                            .message()
                            .contains("originating root failure")
                    );
                    assert_eq!(
                        decision.fail_logical(&actor).await.unwrap(),
                        LogicalConclusion::Failed
                    );
                }
                ResultPumpFailure::Concluded(concluded) if hold_delivery => {
                    assert_eq!(concluded.conclusion(), LogicalConclusion::Failed);
                    assert_eq!(concluded.class(), AttemptFailureClass::ExecutionFailure);
                    assert!(
                        concluded
                            .error()
                            .message()
                            .contains("originating root failure")
                    );
                }
                _ => panic!("visibility must decide whether failure returns attempt authority"),
            }
            if let Some(held) = held {
                held.complete();
            }
            assert!(stream.next().await.is_err());
            assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
            drop((sender, stream, actor, owner, window));
        }
    }

    #[tokio::test]
    async fn relay_derived_failure_requires_originating_refinement_before_settling() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(52).await;
        let control = Arc::new(TestSuccessSealPort::default());
        let (sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            control.clone() as Arc<dyn AcceptedRootControlPort>,
        );
        sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(Vec::new());
        let (_terminal_sender, terminal) = native_attempt_terminal_channel();
        let mut relay = Box::pin(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, port.clone(), &window),
            statuses,
            terminal,
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while port.requests().is_empty() {
                poll_pending_after_actor_turn(relay.as_mut(), &actor).await;
            }
        })
        .await
        .unwrap();
        let pending = TaskStatus::try_new(
            root,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            TaskOutputFacts::new(false),
        )
        .unwrap();
        sender
            .publish_attempt_observation(pending.clone(), AcceptedAttemptFailure::DerivedPending)
            .unwrap();
        poll_pending_after_actor_turn(relay.as_mut(), &actor).await;
        sender
            .publish_with_attempt_failure(
                pending,
                TerminationDetail::Failed(TaskFailure::new(
                    TaskFailureCategory::Exchange,
                    SafeDetail::new("originating peer exchange failure").unwrap(),
                )),
            )
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .unwrap()
            .unwrap_err();
        let ResultPumpFailure::DecisionPending(decision) = error else {
            panic!("refinement must retain its attempt decision");
        };
        assert_eq!(decision.class(), AttemptFailureClass::ExecutionFailure);
        assert!(
            decision
                .error()
                .message()
                .contains("originating peer exchange failure")
        );
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
        drop((sender, stream, actor, owner, window));
    }

    #[tokio::test]
    async fn relay_native_failure_without_root_status_retains_recovery_authority() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(53).await;
        let (_sender, statuses) = accepted_root_status_projection(root);
        let port = ScriptedRootPort::new(Vec::new());
        let (terminal_sender, terminal) = native_attempt_terminal_channel();
        terminal_sender.publish(NativeAttemptTerminal::Failed(
            crate::api::NativeAttemptPreparationFailure::new(
                AttemptFailureClass::RecoverableInfrastructure,
                contract_error("exact backend process replaced"),
            ),
        ));
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            run_root_relay(
                permit,
                root,
                scope,
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        let ResultPumpFailure::DecisionPending(decision) = error else {
            panic!("native failure must retain its attempt decision");
        };
        assert_eq!(
            decision.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(decision.error().message(), "exact backend process replaced");
        assert_eq!(
            decision.fail_logical(&actor).await.unwrap(),
            LogicalConclusion::Failed
        );
        assert!(stream.next().await.is_err());
        assert!(port.requests().is_empty());
        drop((stream, actor, owner, window));
    }

    // ---- Backend-encoded root relay ----

    use novarocks_execution_contract::root_result::{
        RootReadOutcome, RootResultData, RootResultEnd, RootResultReply,
    };
    use novarocks_result_contract::{
        ClientRowProfile, RootOutputKind, RootProfileId, RootProfileV1,
    };
    use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass, ResultWindowGrant};

    /// Answers each read from a script and records it. An ACK-only read with
    /// no scripted answer is acknowledged.
    struct ScriptedRootPort {
        requests: Mutex<Vec<RootResultRead>>,
        outcomes: Mutex<VecDeque<RootReadOutcome>>,
    }
    impl ScriptedRootPort {
        fn new(outcomes: Vec<RootReadOutcome>) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                outcomes: Mutex::new(outcomes.into()),
            })
        }
        fn requests(&self) -> Vec<(Option<u64>, u64)> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .map(|read| (read.wanted().map(|wanted| wanted.get()), read.consumed()))
                .collect()
        }
    }
    impl crate::api::BoundedRootReadPort for ScriptedRootPort {
        fn read(
            &self,
            request: RootResultRead,
            physical_guard: novarocks_workload_control::ResultWindowAlias,
        ) -> Pin<Box<dyn Future<Output = Result<RetainedRootReply, RootResultFetchFailure>> + Send>>
        {
            self.requests.lock().unwrap().push(request.clone());
            let outcome = self.outcomes.lock().unwrap().pop_front().unwrap_or(
                if request.wanted().is_none() {
                    RootReadOutcome::AckOnly
                } else {
                    RootReadOutcome::NotReady
                },
            );
            let reply = RootResultReply {
                root_task: request.root_task(),
                profile: request.profile(),
                kind: request.kind(),
                accepted_consumed: request.consumed(),
                outcome,
            };
            let long_poll = matches!(reply.outcome, RootReadOutcome::NotReady);
            Box::pin(async move {
                if long_poll {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                RetainedRootReply::try_new(reply, physical_guard, 64 * 1024).map_err(|error| {
                    contract_failure(contract_error(format!("retain root reply: {error}")))
                })
            })
        }
        fn seal(
            &self,
            _sealed: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    fn rows_data(sequence: u64, body: &'static [u8], end: Option<(u64, u64)>) -> RootReadOutcome {
        RootReadOutcome::Data(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                NonZeroU64::new(sequence).unwrap(),
                bytes::Bytes::from_static(body),
                end.map(|(sequence, rows)| RootResultEnd {
                    sequence: NonZeroU64::new(sequence).unwrap(),
                    output_rows: rows,
                }),
            )
            .unwrap(),
        )
    }

    /// The ordinary harness plus a configured result window for the root.
    async fn relay_harness(tag: i64) -> (Harness, ResultWindowGrant) {
        relay_harness_with(tag, relay_carrier()).await
    }

    async fn relay_harness_with(
        tag: i64,
        carrier: crate::api::ResultRowCarrier,
    ) -> (Harness, ResultWindowGrant) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = work.owner.scope();
        let class = match carrier {
            crate::api::ResultRowCarrier::Relayed {
                kind: RootOutputKind::InternalFacts(_) | RootOutputKind::CountOnly,
                ..
            } => ResultWindowClass::Internal,
            _ => ResultWindowClass::Client,
        };
        let window = capacity.try_acquire(&scope, class).unwrap();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        let execution = execution(tag);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            Vec::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work.owner,
            stage,
        )
        .unwrap()
        .with_result_stream(result_schema(), NonZeroUsize::new(1).unwrap())
        .with_result_row_carrier(carrier);
        let (owner, initial, output) = spawn_logical_execution_actor(&Handle::current(), config)
            .unwrap()
            .into_parts();
        let ExecutionOutput::Rows(mut stream) = output.into_output() else {
            panic!("result actor must expose a stream");
        };
        let schema = stream.begin_schema().unwrap();
        assert_eq!(schema.row_carrier(), carrier);
        schema.complete();
        let actor = owner.actor().clone();
        let permit = actor.activate(initial.ready()).await.unwrap();
        (
            Harness {
                control,
                scope,
                actor,
                owner,
                permit,
                stream,
                root: root_task(execution),
            },
            window,
        )
    }

    fn relay_profile() -> ClientRowProfile {
        ClientRowProfile::try_new(RootProfileV1::SEGMENT_BYTES, 1 << 20).unwrap()
    }

    fn relay_carrier() -> crate::api::ResultRowCarrier {
        crate::api::ResultRowCarrier::relayed(RootOutputKind::ClientRows, Some(relay_profile()))
            .unwrap()
    }

    fn relay_binding(
        root: TaskIdentity,
        port: Arc<ScriptedRootPort>,
        window: &ResultWindowGrant,
    ) -> RootRelayBinding {
        RootRelayBinding {
            port,
            frontier: super::super::RootRelayFrontier::new(
                root,
                RootProfileId::V1,
                RootOutputKind::ClientRows,
                Some(relay_profile()),
            )
            .unwrap(),
            window: window.retain_alias(),
            max_wait: Duration::from_millis(5),
        }
    }

    async fn next_segment(
        stream: &mut crate::api::QueryResultStream,
    ) -> crate::api::RootSegmentDelivery {
        match stream.next().await.unwrap().unwrap() {
            ResultDelivery::Segment(segment) => segment,
            _ => panic!("expected a relayed root segment"),
        }
    }

    #[tokio::test]
    async fn relay_cancel_cut_retains_validated_coverage_across_actor_handoff_without_ack() {
        for complete_first in [false, true] {
            let (
                Harness {
                    control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(40 + i64::from(complete_first)).await;
            let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
                root,
                Arc::new(TestSuccessSealPort::default()),
            );
            status_sender.publish(finished(root)).unwrap();
            let port = ScriptedRootPort::new(vec![
                rows_data(1, &[5, 0, 0, 0, b'a', b'b'], None),
                rows_data(2, &[b'c', b'd', b'e', 1, 0, 0, 0, b'f'], Some((3, 2))),
            ]);
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let relay = tokio::spawn(run_root_relay(
                permit,
                root,
                scope.clone(),
                relay_binding(root, port.clone(), &window),
                statuses,
                terminal,
            ));
            let mut first = Some(next_segment(&mut stream).await);
            let coverage = first.as_ref().unwrap().resident_window().unwrap();
            // Barrier: the second response must have passed frontier validation,
            // not merely have been dispatched by the read port.
            while coverage.is_empty() {
                tokio::task::yield_now().await;
            }
            if complete_first {
                first.take().unwrap().complete();
                // Force the second item out of ready and through the actor's
                // queue before the protocol has acquired its next delivery.
                while !coverage.is_empty() {
                    tokio::task::yield_now().await;
                }
            }
            assert_eq!(
                control.cancel_active_roots(CancellationReason::Requested),
                1
            );
            let items = coverage.freeze();
            let payloads = items
                .iter()
                .flatten()
                .flat_map(|item| {
                    item.client_rows()
                        .unwrap()
                        .payload_spans()
                        .map(|span| span.bytes.to_vec())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            assert!(payloads.iter().any(|part| part == b"cde"));
            assert!(coverage.freeze().into_iter().all(|item| item.is_none()));
            if let Some(first) = first {
                first.fail(contract_error("cancelled during a partial write"));
            }
            assert!(relay.await.unwrap().is_err());
            assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
            drop(items);
            drop(stream);
            drop(actor);
            drop(owner);
        }
    }

    #[tokio::test]
    async fn relay_prefetches_delivers_in_order_and_seals_without_a_final_backend_ack() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(31).await;
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootControlPort>,
        );
        status_sender.publish(finished(root)).unwrap();
        // A two-segment row, then a one-row segment carrying End.
        let port = ScriptedRootPort::new(vec![
            rows_data(1, &[5, 0, 0, 0, b'a', b'b'], None),
            rows_data(2, &[b'c', b'd', b'e', 1, 0, 0, 0, b'f'], Some((3, 2))),
        ]);
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let relay = tokio::spawn(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, Arc::clone(&port), &window),
            statuses,
            terminal_source,
        ));
        let first = next_segment(&mut stream).await;
        assert_eq!(first.sequence(), ResultPacketSequence::new(0));
        assert_eq!(first.rows(), 0);
        // Seq 2 is read while seq 1 is still being written; both carry ACK 0.
        while port.requests().len() < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(port.requests(), vec![(Some(1), 0), (Some(2), 0)]);
        first.complete();
        let second = next_segment(&mut stream).await;
        assert_eq!(second.rows(), 2);
        let spans = second
            .client_rows()
            .unwrap()
            .payload_spans()
            .map(|span| (span.bytes.to_vec(), span.completes_row))
            .collect::<Vec<_>>();
        assert_eq!(spans, vec![(b"cde".to_vec(), true), (b"f".to_vec(), true)]);
        second.complete();
        // Success needs the seal verdict, not a Backend final ACK.
        seal_port.next().await.accept(status_sender).unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("local End consumption and root Finished produce EOF");
        };
        assert_eq!(end.root_output_rows(), Some(2));
        end.complete();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(&port.requests()[..2], &[(Some(1), 0), (Some(2), 0)]);
        drop((stream, actor, owner, window));
    }

    #[tokio::test]
    async fn scalar_relay_delivers_value_and_no_rows_counts_before_success_seal() {
        use novarocks_result_contract::{
            BorrowedScalarLeaf, InternalResultDomain, ScalarField, ScalarLeafCursor, ScalarRecord,
            ScalarSchema, ScalarValueType,
        };
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::SignedInteger(64),
        })
        .unwrap();
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1);
        for (tag, value) in [
            (71, BorrowedScalarLeaf::NoRows),
            (72, BorrowedScalarLeaf::Null),
            (73, BorrowedScalarLeaf::SignedInteger { bits: 64, value: 7 }),
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness_with(
                tag,
                crate::api::ResultRowCarrier::relayed(kind, None).unwrap(),
            )
            .await;
            let cursor = ScalarLeafCursor::try_new(&schema, value).unwrap();
            let rows = cursor.rows();
            let mut body = vec![0; cursor.encoded_len()];
            cursor.copy_range(0, &mut body).unwrap();
            let port = ScriptedRootPort::new(vec![RootReadOutcome::Data(
                RootResultData::try_new(
                    kind,
                    NonZeroU64::MIN,
                    bytes::Bytes::from(body),
                    Some(RootResultEnd {
                        sequence: NonZeroU64::new(2).unwrap(),
                        output_rows: rows,
                    }),
                )
                .unwrap(),
            )]);
            let seal_port = Arc::new(TestSuccessSealPort::default());
            let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
                root,
                Arc::clone(&seal_port) as Arc<dyn AcceptedRootControlPort>,
            );
            status_sender.publish(finished(root)).unwrap();
            let (_terminal_sender, terminal) = native_attempt_terminal_channel();
            let binding = RootRelayBinding {
                port: port.clone(),
                window: window.retain_alias(),
                frontier: super::super::RootRelayFrontier::new(root, RootProfileId::V1, kind, None)
                    .unwrap(),
                max_wait: Duration::from_millis(5),
            };
            let relay = tokio::spawn(run_root_relay(
                permit, root, scope, binding, statuses, terminal,
            ));
            let segment = next_segment(&mut stream).await;
            assert_eq!(segment.rows(), rows);
            let record = ScalarRecord::decode_owned(&schema, segment.body()).unwrap();
            assert_eq!(
                u64::from(matches!(record, ScalarRecord::Value(_))),
                segment.rows()
            );
            drop(record);
            assert!(seal_port.requests.lock().unwrap().is_empty());
            segment.complete();
            seal_port.next().await.accept(status_sender).unwrap();
            let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
                panic!("end")
            };
            assert_eq!(end.root_output_rows(), Some(rows));
            end.complete();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), relay)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                LogicalConclusion::Succeeded
            );
            drop((stream, actor, owner, window));
        }
    }

    #[tokio::test]
    async fn count_only_relay_preserves_end_rows_after_success_seal() {
        let kind = RootOutputKind::CountOnly;
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness_with(
            74,
            crate::api::ResultRowCarrier::relayed(kind, None).unwrap(),
        )
        .await;
        let port = ScriptedRootPort::new(vec![RootReadOutcome::End(RootResultEnd {
            sequence: NonZeroU64::MIN,
            output_rows: 42,
        })]);
        let seal_port = Arc::new(TestSuccessSealPort::default());
        let (status_sender, statuses) = accepted_root_status_projection_with_control_port(
            root,
            Arc::clone(&seal_port) as Arc<dyn AcceptedRootControlPort>,
        );
        status_sender.publish(finished(root)).unwrap();
        let (_terminal_sender, terminal) = native_attempt_terminal_channel();
        let binding = RootRelayBinding {
            port: port.clone(),
            window: window.retain_alias(),
            frontier: super::super::RootRelayFrontier::new(root, RootProfileId::V1, kind, None)
                .unwrap(),
            max_wait: Duration::from_millis(5),
        };
        let relay = tokio::spawn(run_root_relay(
            permit, root, scope, binding, statuses, terminal,
        ));
        let seal = seal_port.next().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err()
        );
        seal.accept(status_sender).unwrap();
        let ResultDelivery::End(end) = stream.next().await.unwrap().unwrap() else {
            panic!("CountOnly must produce End without a data segment");
        };
        assert_eq!(end.root_output_rows(), Some(42));
        end.complete();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            LogicalConclusion::Succeeded
        );
        let requests = port.requests();
        assert_eq!(requests[0], (Some(1), 0));
        assert!(requests[1..].iter().all(|request| *request == (None, 0)));
        drop((stream, actor, owner, window));
    }

    #[tokio::test]
    async fn relay_refuses_exact_prefix_only_and_complete_row_prefix_suffix_without_ack() {
        for (body, label) in [
            (&[1, 0, 0, 0][..], "exact four-byte prefix-only body"),
            (
                &[1, 0, 0, 0, b'a', 1, 0, 0, 0][..],
                "complete valid row followed by a four-byte prefix-only suffix",
            ),
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(75).await;
            let capacity = scope.result_capacity().unwrap();
            assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
            let (status_sender, statuses) = accepted_root_status_projection(root);
            status_sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(vec![rows_data(1, body, None)]);
            let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
            let error = tokio::time::timeout(
                Duration::from_secs(1),
                run_root_relay(
                    permit,
                    root,
                    scope,
                    relay_binding(root, port.clone(), &window),
                    statuses,
                    terminal_source,
                ),
            )
            .await
            .unwrap()
            .unwrap_err();
            let ResultPumpFailure::DecisionPending(decision) = error else {
                panic!("{label} must retain its failure decision");
            };
            assert_eq!(decision.class(), AttemptFailureClass::ContractViolation);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), decision.fail_logical(&actor))
                    .await
                    .unwrap()
                    .unwrap(),
                LogicalConclusion::Failed
            );
            // Whole-body validation must reject the valid row too; the first
            // consumer observation is the failure, never a Segment delivery.
            assert!(
                tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .unwrap()
                    .is_err(),
                "{label} must not deliver any segment"
            );
            let requests = port.requests();
            assert_eq!(requests, vec![(Some(1), 0)], "{label}");
            assert!(
                requests
                    .iter()
                    .all(|(wanted, consumed)| wanted.is_some() && *consumed == 0),
                "{label} must neither ACK-only nor acknowledge consumption"
            );
            // The test's original grant still holds its one position. After
            // dropping it, the existing capacity oracle detects leaked relay
            // window aliases; this is not a physical-allocation exit oracle.
            assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
            drop((stream, actor, owner, window, status_sender));
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        }
    }

    #[tokio::test]
    async fn relay_refuses_malformed_rows_and_end_mismatch_before_delivery() {
        for (outcomes, label) in [
            (
                vec![rows_data(1, &[1, 0, 0, 0, b'a', 1, 0], None)],
                "prefix without payload",
            ),
            (
                vec![rows_data(1, &[1, 0, 0, 0, b'a'], Some((2, 3)))],
                "End row count",
            ),
            (
                vec![rows_data(2, &[1, 0, 0, 0, b'a'], None)],
                "sequence gap",
            ),
        ] {
            let (
                Harness {
                    control: _control,
                    scope,
                    actor,
                    owner,
                    permit,
                    mut stream,
                    root,
                },
                window,
            ) = relay_harness(32).await;
            let (status_sender, statuses) = accepted_root_status_projection(root);
            status_sender.publish(running(root)).unwrap();
            let port = ScriptedRootPort::new(outcomes);
            let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                run_root_relay(
                    permit,
                    root,
                    scope,
                    relay_binding(root, port, &window),
                    statuses,
                    terminal_source,
                ),
            )
            .await
            .unwrap();
            assert!(result.is_err(), "{label} must fail the attempt");
            // Nothing was handed to the consumer.
            assert!(
                tokio::time::timeout(Duration::from_millis(20), stream.next())
                    .await
                    .map_or(true, |next| !matches!(
                        next,
                        Ok(Some(ResultDelivery::Segment(_)))
                    )),
                "{label} must not deliver any segment"
            );
            drop((stream, actor, owner, window, status_sender));
        }
    }

    #[tokio::test]
    async fn a_client_segment_on_a_count_only_result_is_refused() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness_with(
            33,
            crate::api::ResultRowCarrier::relayed(
                novarocks_result_contract::RootOutputKind::CountOnly,
                None,
            )
            .unwrap(),
        )
        .await;
        // Keep the native root and admitted window valid; only the stream's
        // frozen consumer purpose conflicts with the ClientRows delivery.
        drop(window);
        let window = scope
            .result_capacity()
            .unwrap()
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(vec![rows_data(1, &[1, 0, 0, 0, b'a'], None)]);
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            run_root_relay(
                permit,
                root,
                scope,
                relay_binding(root, port, &window),
                statuses,
                terminal_source,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(error, ResultPumpFailure::Concluded(_)),
            "unexpected error: {error:?}"
        );
        // A ClientRows segment cannot reach a CountOnly consumer.
        let Err(consumer) = stream.next().await else {
            panic!("the CountOnly consumer must see the refusal");
        };
        assert!(
            consumer.to_string().contains("row carrier"),
            "unexpected consumer error: {consumer}"
        );
        drop((stream, actor, owner, window, status_sender));
    }

    #[tokio::test]
    async fn relay_failed_delivery_fails_the_attempt_without_acknowledging_it() {
        let (
            Harness {
                control: _control,
                scope,
                actor,
                owner,
                permit,
                mut stream,
                root,
            },
            window,
        ) = relay_harness(33).await;
        let (status_sender, statuses) = accepted_root_status_projection(root);
        status_sender.publish(running(root)).unwrap();
        let port = ScriptedRootPort::new(vec![rows_data(1, &[1, 0, 0, 0, b'a'], None)]);
        let (_terminal_sender, terminal_source) = native_attempt_terminal_channel();
        let relay = tokio::spawn(run_root_relay(
            permit,
            root,
            scope,
            relay_binding(root, Arc::clone(&port), &window),
            statuses,
            terminal_source,
        ));
        let segment = next_segment(&mut stream).await;
        segment.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client went away",
        ));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        // The failed item was never acknowledged as consumed.
        assert!(port.requests().iter().all(|(_, consumed)| *consumed == 0));
        drop((stream, actor, owner, window, status_sender));
    }
}
