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

//! Process-owned logical execution supervision.
//!
//! The supervisor is the sole constructor of [`QueryExecutionClient`]. A
//! start mailbox transfers one governed work owner before returning a future to
//! the caller. Warehouse concurrency admission happened before this handoff,
//! so the mailbox has no independent capacity policy. Accepted requests remain
//! in the supervisor's JoinSet even when that caller drops its reply future.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use novarocks_types::identity::{
    AttemptId, FrontendProcessId, LocalQuerySequence, QueryExecutionId, QueryIdAttribution,
    QueryProcessNamespace,
};
use novarocks_workload_control::{CancellationReason, Stage, WorkError, WorkOwner};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::api::{
    ActiveNativeAttemptOwner, DormantNativeAttemptOwner, ExecutionHandle,
    LogicalExecutionNativePort, LogicalNativeOpenError, LogicalNativeOpenRequest,
    LogicalNativeSession, NativeActiveConvergenceClosure, NativeActiveConvergenceRequest,
    NativeAttemptConvergence, NativeAttemptPreparationError, NativeAttemptPreparationFailure,
    NativeAttemptTerminal, NativeContextConvergenceKind, NativeRowsAttemptRuntime,
    QueryExecutionClient, QueryExecutionDriver, QueryExecutionError, QueryExecutionErrorKind,
    QueryExecutionFuture, QueryExecutionRequest, ResultSchema, StatementNativeHealthFailure,
};
use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
use crate::preparation::OutputContract;
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};

use super::{
    LogicalExecutionActorConfig, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, NativeAttemptDrive, RecoveryDecision, RecoveryInput,
    ResultPumpFailure, SupervisedActiveNativeOwner, assert_shutdown_complete,
    build_attempt_schedule, evaluate_recovery,
};
use novarocks_coordination_contract::RecoveryMode;

static NEXT_PROCESS_QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const NATIVE_CONVERGENCE_PANIC_BACKOFF: Duration = Duration::from_millis(10);
const DEFAULT_REMOTE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Explicit process bounds used by the logical execution supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionSupervisorConfig {
    actor_mailbox_capacity: NonZeroUsize,
    max_admission_issues_per_context: NonZeroUsize,
    max_establish_authorizations_per_context: NonZeroUsize,
    rows: LogicalExecutionRowsConfig,
    remote_cleanup_timeout: Duration,
}

/// Process-fixed bounds for every row-producing logical execution.
///
/// These limits are role composition facts. A frozen query description cannot
/// raise them, and the supervisor supplies the same bounds to each replacement
/// attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionRowsConfig {
    delivery_capacity: NonZeroUsize,
    max_attempts: NonZeroU32,
    replacement_reservation_valid_for: Duration,
}

impl LogicalExecutionRowsConfig {
    pub const fn new(
        delivery_capacity: NonZeroUsize,
        max_attempts: NonZeroU32,
        replacement_reservation_valid_for: Duration,
    ) -> Self {
        Self {
            delivery_capacity,
            max_attempts,
            replacement_reservation_valid_for,
        }
    }
}

impl LogicalExecutionSupervisorConfig {
    pub const fn new(
        actor_mailbox_capacity: NonZeroUsize,
        max_admission_issues_per_context: NonZeroUsize,
        max_establish_authorizations_per_context: NonZeroUsize,
        rows: LogicalExecutionRowsConfig,
    ) -> Self {
        Self {
            actor_mailbox_capacity,
            max_admission_issues_per_context,
            max_establish_authorizations_per_context,
            rows,
            remote_cleanup_timeout: DEFAULT_REMOTE_CLEANUP_TIMEOUT,
        }
    }

    pub const fn replacement_reservation_valid_for(self) -> Duration {
        self.rows.replacement_reservation_valid_for
    }

    /// Sets the one absolute observation budget that starts after the FE has
    /// reached a logical terminal outcome. It governs FE tracking only; it
    /// does not assert a Worker stop deadline.
    pub const fn with_remote_cleanup_timeout(mut self, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "remote cleanup timeout must be nonzero");
        self.remote_cleanup_timeout = timeout;
        self
    }

    pub const fn remote_cleanup_timeout(self) -> Duration {
        self.remote_cleanup_timeout
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalExecutionSupervisorShutdownError {
    DeadlineExceeded,
    Registry(LogicalExecutionRuntimeRegistryError),
    SupervisorFailed(QueryExecutionError),
    SupervisorPanicked,
    StatementNativeUnresolved(QueryExecutionError),
}

impl fmt::Display for LogicalExecutionSupervisorShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded => {
                formatter.write_str("logical execution supervisor shutdown deadline exceeded")
            }
            Self::Registry(error) => write!(
                formatter,
                "logical execution supervisor Registry shutdown failed: {error}"
            ),
            Self::SupervisorFailed(error) => {
                write!(formatter, "logical execution supervisor failed: {error}")
            }
            Self::StatementNativeUnresolved(error) => {
                write!(
                    formatter,
                    "statement Native owner has unproven actual exit: {error}"
                )
            }
            Self::SupervisorPanicked => {
                formatter.write_str("logical execution supervisor task panicked")
            }
        }
    }
}

impl std::error::Error for LogicalExecutionSupervisorShutdownError {}

/// Unique process owner for logical execution admission, tasks, and Registry.
///
/// Shutdown borrows this owner. A timed-out or cancelled wait leaves the exact
/// join handle and Registry owner here so a later call resumes the same
/// convergence.
// Design: ADR-0146 (docs/adr/ADR-0146-logical-execution-owns-attempts-and-result-visibility.md)
pub struct LogicalExecutionSupervisor {
    shutdown: watch::Sender<bool>,
    join: Option<JoinHandle<Result<(), QueryExecutionError>>>,
    run_outcome: Option<SupervisorRunOutcome>,
    registry: LogicalExecutionRuntimeRegistry,
    statement_native_residual_port: super::StatementNativeResidualPort,
    shutdown_complete: bool,
}

enum SupervisorRunOutcome {
    Completed,
    Failed(Box<QueryExecutionError>),
    Panicked,
}

impl fmt::Debug for LogicalExecutionSupervisor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionSupervisor")
            .field("shutdown_requested", &*self.shutdown.borrow())
            .field("shutdown_complete", &self.shutdown_complete)
            .finish_non_exhaustive()
    }
}

impl Drop for LogicalExecutionSupervisor {
    fn drop(&mut self) {
        assert_shutdown_complete(self.shutdown_complete, "logical execution supervisor");
    }
}

impl LogicalExecutionSupervisor {
    /// Constructs the sole bounded client together with its process owner.
    pub fn new(
        runtime: Handle,
        native: Arc<dyn LogicalExecutionNativePort>,
        namespace: QueryProcessNamespace,
        frontend_process_id: FrontendProcessId,
        config: LogicalExecutionSupervisorConfig,
    ) -> (Self, QueryExecutionClient) {
        // Query admission is the sole warehouse scheduling queue. This
        // unbounded handoff only transfers already admitted work to the
        // supervisor; it must never reject a query with a second capacity.
        let (starts, start_rx) = mpsc::unbounded_channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let registry = LogicalExecutionRuntimeRegistry::new(runtime.clone());
        let statement_native_residual_port =
            registry.statement_native_residual_port(config.remote_cleanup_timeout());
        let registry_handle = registry.handle();
        let driver = QueryExecutionDriverHandle {
            starts,
            shutdown: shutdown_rx.clone(),
        };
        let join = runtime.spawn(run_supervisor(
            registry_handle,
            native,
            namespace,
            frontend_process_id,
            config,
            start_rx,
            shutdown_rx,
        ));
        (
            Self {
                shutdown,
                join: Some(join),
                run_outcome: None,
                registry,
                statement_native_residual_port,
                shutdown_complete: false,
            },
            QueryExecutionClient::new(driver),
        )
    }

    /// Registers only original statement Native responsibility; no actor gate,
    /// logical success, or second business admission is granted by this port.
    pub fn statement_native_residual_port(&self) -> super::StatementNativeResidualPort {
        self.statement_native_residual_port.clone()
    }

    pub async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionSupervisorShutdownError> {
        self.shutdown.send_replace(true);
        if self.shutdown_complete {
            return Ok(());
        }
        if self.run_outcome.is_none() {
            let join = self
                .join
                .as_mut()
                .expect("unfinished supervisor retains its join handle");
            self.run_outcome = match tokio::time::timeout_at(deadline.into(), join).await {
                Err(_) => {
                    // Preserve the original healthy/error boundary while the
                    // same JoinHandle/Registry still own unresolved local work.
                    return Err(match self.registry.statement_native_health() {
                        Some(error) => {
                            LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(
                                error,
                            )
                        }
                        None => LogicalExecutionSupervisorShutdownError::DeadlineExceeded,
                    });
                }
                Ok(Err(error)) => {
                    self.registry.record_native_supervision_join_failure(error);
                    Some(SupervisorRunOutcome::Panicked)
                }
                Ok(Ok(Err(error))) => Some(SupervisorRunOutcome::Failed(Box::new(error))),
                Ok(Ok(Ok(()))) => Some(SupervisorRunOutcome::Completed),
            };
            self.join.take();
        }

        match self.registry.shutdown_until(deadline).await {
            Ok(()) => {}
            Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded { .. }) => {
                return Err(match self.registry.statement_native_health() {
                    Some(error) => {
                        LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(error)
                    }
                    None => LogicalExecutionSupervisorShutdownError::DeadlineExceeded,
                });
            }
            Err(LogicalExecutionRuntimeShutdownError::Registry(error)) => {
                return Err(LogicalExecutionSupervisorShutdownError::Registry(error));
            }
        }

        if let Some(error) = self.registry.statement_native_health()
            && matches!(self.run_outcome, Some(SupervisorRunOutcome::Completed))
        {
            self.run_outcome = Some(SupervisorRunOutcome::Failed(Box::new(error)));
        }
        self.shutdown_complete = true;
        match self
            .run_outcome
            .take()
            .expect("registry shutdown follows a collected supervisor outcome")
        {
            SupervisorRunOutcome::Completed => Ok(()),
            SupervisorRunOutcome::Failed(error) => Err(
                LogicalExecutionSupervisorShutdownError::SupervisorFailed(*error),
            ),
            SupervisorRunOutcome::Panicked => {
                Err(LogicalExecutionSupervisorShutdownError::SupervisorPanicked)
            }
        }
    }

    /// Ends process-local ownership after bounded graceful convergence failed
    /// and the FE runner has irrevocably committed to process exit.
    ///
    /// This does not abort Tokio tasks. Their join handles are released only at
    /// the process boundary, where the runtime and every process-local task are
    /// about to cease together. Ordinary callers must keep using
    /// [`Self::shutdown_until`] and may not use this as a detach path.
    pub fn abandon_for_process_exit(&mut self) {
        self.shutdown.send_replace(true);
        self.join.take();
        self.run_outcome.take();
        self.registry.abandon_for_process_exit();
        self.shutdown_complete = true;
    }

    #[cfg(test)]
    fn inject_registry_shutdown_error_once(&mut self, error: LogicalExecutionRuntimeRegistryError) {
        self.registry.inject_shutdown_error_once(error);
    }
}

#[derive(Debug)]
struct QueryExecutionDriverHandle {
    starts: mpsc::UnboundedSender<StartCommand>,
    shutdown: watch::Receiver<bool>,
}

impl QueryExecutionDriver for QueryExecutionDriverHandle {
    fn start(&self, request: QueryExecutionRequest, owner: WorkOwner) -> QueryExecutionFuture {
        if *self.shutdown.borrow() {
            return rejected_start(owner, "logical execution supervisor is shutting down");
        }
        let cancellation = match owner.scope().cancellation() {
            Ok(cancellation) => cancellation,
            Err(error) => {
                let error = work_error(error);
                owner.complete_after_terminal_cancel_settled();
                return Box::pin(async move { Err(error) });
            }
        };
        let (reply, response) = oneshot::channel();
        let command = StartCommand {
            request,
            owner,
            reply,
        };
        match self.starts.send(command) {
            Ok(()) => Box::pin(async move {
                tokio::select! {
                    biased;
                    result = response => result.unwrap_or_else(|_| {
                        Err(QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "logical execution supervisor closed without a start verdict",
                        ))
                    }),
                    reason = cancellation.cancelled() => Err(start_cancellation_error(
                        cancellation.reason().unwrap_or(reason),
                    )),
                }
            }),
            Err(mpsc::error::SendError(command)) => {
                rejected_start(command.owner, "logical execution supervisor is closed")
            }
        }
    }
}

/// Why a start never reached its verdict.
///
/// A statement whose deadline ran out and a statement someone killed reach
/// this point the same way, and the caller acts on the difference: one is the
/// query's own budget, the other is a decision about it. Naming both
/// `Cancelled` here made the answer depend on which of two futures the
/// runtime happened to poll first, since the same expiry also reaches the
/// pre-install wait, which has always classified it.
fn start_cancellation_error(reason: CancellationReason) -> QueryExecutionError {
    cancellation_error(
        reason,
        "logical execution start was cancelled before its start verdict",
    )
}

/// Project only the actual workload cancellation verdict, without inventing a Task phase.
fn cancellation_error(reason: CancellationReason, detail: &'static str) -> QueryExecutionError {
    let reason = match reason {
        CancellationReason::DeadlineExceeded
        | CancellationReason::FrontendDrainDeadlineExceeded => QueryFailureReason::DeadlineExceeded,
        _ => QueryFailureReason::Cancelled,
    };
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

fn rejected_start(owner: WorkOwner, message: &'static str) -> QueryExecutionFuture {
    owner.complete_after_terminal_cancel_settled();
    Box::pin(async move {
        Err(QueryExecutionError::new(
            QueryExecutionErrorKind::Rejected,
            message,
        ))
    })
}

struct StartCommand {
    request: QueryExecutionRequest,
    owner: WorkOwner,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
}

struct ProcessQueryIdAllocator {
    namespace: QueryProcessNamespace,
}

impl ProcessQueryIdAllocator {
    const fn new(namespace: QueryProcessNamespace) -> Self {
        Self { namespace }
    }

    fn first_execution(&self) -> Result<QueryExecutionId, QueryExecutionError> {
        let previous = NEXT_PROCESS_QUERY_SEQUENCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= i64::MAX as u64)
            })
            .map_err(|_| {
                QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    "logical execution query id sequence is exhausted",
                )
            })?;
        let sequence = LocalQuerySequence::new(previous + 1).expect("validated nonzero sequence");
        QueryExecutionId::new(
            QueryIdAttribution::new(self.namespace, sequence).into_query_id(),
            AttemptId::new(1).expect("initial attempt id is nonzero"),
        )
        .map_err(|error| {
            QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
        })
    }
}

async fn run_supervisor(
    registry: LogicalExecutionRuntimeRegistryHandle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    mut starts: mpsc::UnboundedReceiver<StartCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), QueryExecutionError> {
    let query_ids = Arc::new(ProcessQueryIdAllocator::new(namespace));
    let mut logical_tasks = JoinSet::new();
    let mut task_failure = None;
    loop {
        if *shutdown.borrow() {
            break;
        }
        for task in registry.statement_native_ready_tasks() {
            logical_tasks.spawn(task);
        }
        if let Some(error) = registry.statement_native_health() {
            task_failure.get_or_insert(error);
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = registry.statement_native_changed() => {}
            command = starts.recv() => {
                let Some(command) = command else { break };
                logical_tasks.spawn(run_logical_execution(
                    command,
                    Arc::clone(&native),
                    Arc::clone(&query_ids),
                    frontend_process_id,
                    config,
                    registry.clone(),
                    shutdown.clone(),
                ));
            }
            result = logical_tasks.join_next(), if !logical_tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) => {
                        task_failure.get_or_insert(error);
                    }
                    Some(Err(error)) => {
                        registry.record_native_supervision_join_failure(error);
                        task_failure.get_or_insert_with(|| QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "logical execution supervision task panicked",
                        ));
                    }
                }
            }
        }
    }

    registry.close_statement_native_admission();
    starts.close();
    while let Ok(command) = starts.try_recv() {
        command.owner.complete_after_terminal_cancel_settled();
        let _ = command.reply.send(Err(QueryExecutionError::new(
            QueryExecutionErrorKind::Rejected,
            "logical execution supervisor shut down before starting the accepted request",
        )));
    }
    loop {
        for task in registry.statement_native_ready_tasks() {
            logical_tasks.spawn(task);
        }
        if let Some(error) = registry.statement_native_health() {
            task_failure.get_or_insert(error);
        }
        if logical_tasks.is_empty() && registry.statement_native_count() == 0 {
            break;
        }
        tokio::select! {
            _ = registry.statement_native_changed() => {}
            result = logical_tasks.join_next(), if !logical_tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) => { task_failure.get_or_insert(error); }
                    Some(Err(error)) => {
                        registry.record_native_supervision_join_failure(error);
                        task_failure.get_or_insert_with(|| QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "logical execution supervision task panicked",
                        ));
                    }
                }
            }
        }
    }
    task_failure.map_or(Ok(()), Err)
}

async fn run_logical_execution(
    command: StartCommand,
    native: Arc<dyn LogicalExecutionNativePort>,
    query_ids: Arc<ProcessQueryIdAllocator>,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    registry: super::LogicalExecutionRuntimeRegistryHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), QueryExecutionError> {
    let StartCommand {
        request,
        owner,
        reply,
    } = command;
    let mut pending_owner = PendingWorkOwner(Some(owner));
    let requester = pending_owner.owner().cancellation_requester();
    let scope = pending_owner.owner().scope();
    let initial_execution = match query_ids.first_execution() {
        Ok(execution) => execution,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, error),
    };
    let (description, native_seed) = request.into_parts();
    let description = Arc::new(description);
    // Activation is one-shot: the plan chosen by preparation and completion is
    // the one every attempt of this execution runs.
    let active_plan = super::ActiveLogicalPlan::activate(description.completed_candidate().clone());
    let result_schema = match description.output() {
        OutputContract::Rows(fields) => Some(ResultSchema::new(Arc::clone(fields))),
        OutputContract::CompletionOnly => None,
    };
    if result_schema.is_none() && description.recovery() != RecoveryMode::NoRecovery {
        return fail_uninstalled_start(
            pending_owner,
            reply,
            QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "completion-only logical execution cannot restart as a row attempt",
            ),
        );
    }
    let cancellation = match scope.cancellation() {
        Ok(cancellation) => cancellation,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let (open_request, open_acceptance) = LogicalNativeOpenRequest::issue(
        initial_execution,
        Arc::clone(&description),
        scope.id(),
        cancellation.clone(),
        native_seed,
    );
    let opened = match await_before_actor_install(
        native.open(open_request),
        cancellation.clone(),
        &mut shutdown,
        &requester,
    )
    .await
    {
        PreInstallWait::Completed(opened) => opened,
        PreInstallWait::Cancelled(reason) => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                pre_install_cancellation_error(reason),
            );
        }
    };
    let mut session =
        match opened.and_then(|session| open_acceptance.accept(session).map_err(Into::into)) {
            Ok(session) => session,
            Err(error) => {
                return fail_uninstalled_start(pending_owner, reply, native_open_error(error));
            }
        };
    let (attempt_request, attempt_acceptance) = match session.issue_attempt(initial_execution) {
        Ok(pair) => pair,
        Err(error) => {
            return fail_uninstalled_start(pending_owner, reply, contract_error(error));
        }
    };
    let prepared = match await_before_actor_install(
        session.prepare(attempt_request),
        cancellation.clone(),
        &mut shutdown,
        &requester,
    )
    .await
    {
        PreInstallWait::Completed(prepared) => prepared,
        PreInstallWait::Cancelled(reason) => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                pre_install_cancellation_error(reason),
            );
        }
    };
    let prepared = match prepared
        .and_then(|prepared| attempt_acceptance.accept(prepared).map_err(Into::into))
    {
        Ok(prepared) => prepared,
        Err(error) => {
            return fail_uninstalled_start(pending_owner, reply, native_prepare_error(error));
        }
    };
    debug_assert_eq!(prepared.execution, initial_execution);
    let schedule = match build_attempt_schedule(
        initial_execution,
        frontend_process_id,
        &prepared.eligible_backends,
        &prepared.scheduling,
        #[cfg(novarocks_dev_controls)]
        prepared
            .owner
            .test_preferred_backend_process(initial_execution),
    ) {
        Ok(schedule) => schedule,
        Err(error) => {
            return fail_uninstalled_start(
                pending_owner,
                reply,
                QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()),
            );
        }
    };
    let stage = match scope.track_stage(Stage::Execution) {
        Ok(stage) => stage,
        Err(error) => return fail_uninstalled_start(pending_owner, reply, work_error(error)),
    };
    let contexts = schedule.contexts().to_vec();
    let reservation = match registry.reserve(initial_execution, contexts.clone()) {
        Ok(reservation) => reservation,
        Err(error) => {
            drop(stage);
            return fail_uninstalled_start(pending_owner, reply, registry_error(error));
        }
    };
    let row_carrier = if result_schema.is_some() {
        match description.row_carrier() {
            Ok(carrier) => Some(carrier),
            Err(error) => {
                drop(stage);
                return fail_uninstalled_start(
                    pending_owner,
                    reply,
                    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, error),
                );
            }
        }
    } else {
        None
    };
    let max_establish_authorizations = config.max_establish_authorizations_per_context;
    let actor_config = match (&result_schema, description.recovery()) {
        (None, RecoveryMode::NoRecovery) => LogicalExecutionActorConfig::no_recovery_completion(
            initial_execution,
            description.effect(),
            config.actor_mailbox_capacity,
            contexts,
            config.max_admission_issues_per_context,
            max_establish_authorizations,
            pending_owner.take(),
            stage,
        ),
        (Some(schema), RecoveryMode::NoRecovery) => {
            LogicalExecutionActorConfig::single_attempt_read_rows(
                initial_execution,
                config.actor_mailbox_capacity,
                contexts,
                config.max_admission_issues_per_context,
                max_establish_authorizations,
                pending_owner.take(),
                stage,
                schema.clone(),
                config.rows.delivery_capacity,
                row_carrier.expect("row output carrier was validated"),
            )
        }
        (Some(schema), RecoveryMode::RestartAttemptBeforeVisibility) => {
            let replacement = session
                .replacement_effect_port()
                .ok_or(super::LogicalExecutionActorError::InvariantViolation);
            match replacement {
                Ok(replacement) => {
                    LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
                        initial_execution,
                        config.actor_mailbox_capacity,
                        contexts,
                        config.max_admission_issues_per_context,
                        max_establish_authorizations,
                        config.rows.max_attempts,
                        replacement,
                        pending_owner.take(),
                        stage,
                        config.rows.replacement_reservation_valid_for,
                        schema.clone(),
                        config.rows.delivery_capacity,
                        row_carrier.expect("row output carrier was validated"),
                    )
                }
                Err(error) => Err(error),
            }
        }
        (None, RecoveryMode::RestartAttemptBeforeVisibility) => {
            unreachable!("completion-only recovery was rejected before actor construction")
        }
    };
    let actor_config = match actor_config {
        Ok(actor_config) => actor_config.with_abort_query_context_effect_port(
            session.abort_effect_port(),
            max_establish_authorizations,
        ),
        Err(error) => {
            let _ = reply.send(Err(actor_error(error)));
            return Ok(());
        }
    };
    let installed = match reservation.spawn_and_install(actor_config) {
        Ok(installed) => installed,
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
    };
    let (registration, initial, output) = installed.into_parts();
    let actor = match registry.actor(&registration) {
        Ok(actor) => actor,
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            return Err(error);
        }
    };
    let mut dormant = prepared.owner;
    let mut active_custody = match registry.reserve_active_native_owner(
        &registration,
        schedule.execution(),
        schedule.contexts(),
    ) {
        Ok(reservation) => Some(reservation),
        Err(error) => {
            let error = registry_error(error);
            let _ = reply.send(Err(error.clone()));
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            converge_dormant(dormant.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(dormant);
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(actor_result, retire_result),
            );
        }
    };
    let activation = await_with_shutdown(
        catch_future_panic(dormant.activate(&schedule, None, cancellation.clone())),
        &mut shutdown,
        &requester,
    )
    .await;
    let active = match activation {
        Ok(Ok(active)) => {
            drop(dormant);
            active
        }
        Ok(Err(failure)) => {
            drop(active_custody.take());
            session.retire_preparation();
            let error = failure.into_failure().error().clone();
            // Initialization failure may cancel the root while the actor starts
            // converging. Publish the native verdict first so that derived
            // cancellation cannot replace the direct start failure.
            let _ = reply.send(Err(error));
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            converge_dormant(dormant.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(dormant);
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(actor_result, retire_result);
        }
        Err(()) => {
            drop(active_custody.take());
            session.retire_preparation();
            let error = native_future_panicked("Native attempt activation panicked");
            // As above, the direct activation verdict precedes any derived
            // cancellation emitted while the installed actor converges.
            let _ = reply.send(Err(error.clone()));
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            converge_dormant(dormant.as_mut(), cancellation, &mut shutdown, &requester).await;
            drop(dormant);
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(actor_result, retire_result),
            );
        }
    };
    let (original_active, rows_runtime) = active.into_parts();
    let mut active = active_custody
        .take()
        .expect("reserved before activation")
        .bind(original_active);
    match (&result_schema, rows_runtime) {
        (Some(_), Some(rows_runtime)) => {
            let running = match actor.activate(initial.ready()).await {
                Ok(running) => running,
                Err(error) => {
                    session.retire_preparation();
                    let supervision = actor_terminal_refusal(error);
                    if supervision.is_err() {
                        let _ = requester.request(CancellationReason::Requested);
                    }
                    let error = actor_error(error);
                    let convergence = converge_supervised_active(
                        &mut active,
                        cancellation,
                        &mut shutdown,
                        &requester,
                        config.remote_cleanup_timeout(),
                    )
                    .await;
                    drop(active);
                    let convergence_result = record_reported_active_convergence(
                        &registry,
                        &registration,
                        schedule.contexts(),
                        convergence,
                    )
                    .await;
                    let _ = reply.send(Err(error.clone()));
                    let retire_result = retire_logical(&registry, registration).await;
                    return first_supervision_error(
                        supervision,
                        first_supervision_error(convergence_result, retire_result),
                    );
                }
            };
            let handle = ExecutionHandle::new(requester.clone(), output.into_output());
            let _ = reply.send(Ok(handle));
            let actor_result = supervise_rows(
                &actor,
                &mut session,
                Arc::clone(&description),
                scope,
                frontend_process_id,
                config,
                &registry,
                &registration,
                &mut shutdown,
                &requester,
                active_plan,
                RowsAttempt {
                    schedule,
                    active,
                    running,
                    runtime: rows_runtime,
                },
            )
            .await;
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(actor_result, retire_result);
        }
        (Some(_), None) => {
            session.retire_preparation();
            let error = result_protocol_error(
                "Native activated a row execution without its root result runtime",
            );
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            let _ = requester.request(CancellationReason::Requested);
            let convergence = converge_supervised_active(
                &mut active,
                cancellation,
                &mut shutdown,
                &requester,
                config.remote_cleanup_timeout(),
            )
            .await;
            drop(active);
            let convergence_result = record_reported_active_convergence(
                &registry,
                &registration,
                schedule.contexts(),
                convergence,
            )
            .await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(
                    actor_result,
                    first_supervision_error(convergence_result, retire_result),
                ),
            );
        }
        (None, Some(_)) => {
            session.retire_preparation();
            let error = result_protocol_error(
                "Native attached a root result runtime to a completion-only execution",
            );
            let actor_result = actor
                .initialization_failed(initial)
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            let _ = requester.request(CancellationReason::Requested);
            let convergence = converge_supervised_active(
                &mut active,
                cancellation,
                &mut shutdown,
                &requester,
                config.remote_cleanup_timeout(),
            )
            .await;
            drop(active);
            let convergence_result = record_reported_active_convergence(
                &registry,
                &registration,
                schedule.contexts(),
                convergence,
            )
            .await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                Err(error),
                first_supervision_error(
                    actor_result,
                    first_supervision_error(convergence_result, retire_result),
                ),
            );
        }
        (None, None) => {}
    }
    let running = match actor.activate(initial.ready()).await {
        Ok(running) => running,
        Err(error) => {
            session.retire_preparation();
            let supervision = actor_terminal_refusal(error);
            if supervision.is_err() {
                let _ = requester.request(CancellationReason::Requested);
            }
            let error = actor_error(error);
            let convergence = converge_supervised_active(
                &mut active,
                cancellation,
                &mut shutdown,
                &requester,
                config.remote_cleanup_timeout(),
            )
            .await;
            drop(active);
            let convergence_result = record_reported_active_convergence(
                &registry,
                &registration,
                schedule.contexts(),
                convergence,
            )
            .await;
            let _ = reply.send(Err(error.clone()));
            let retire_result = retire_logical(&registry, registration).await;
            return first_supervision_error(
                supervision,
                first_supervision_error(convergence_result, retire_result),
            );
        }
    };
    let drive = NativeAttemptDrive::new(&running);
    let terminal = await_with_shutdown(
        catch_future_panic(active.as_mut().run(&drive, cancellation.clone())),
        &mut shutdown,
        &requester,
    )
    .await;
    active.begin_terminal_cut(Instant::now());
    session.retire_preparation();
    let (actor_result, convergence) = match terminal {
        Ok(NativeAttemptTerminal::Completed) => {
            // The logical conclusion is the client-visible terminal boundary.
            // It must precede bounded remote observation so the statement
            // owner can release its warehouse query permit while FE cleanup
            // continues independently.
            let actor_result = match actor.complete_attempt(running).await {
                Ok(super::LogicalConclusion::Succeeded) => {
                    let handle = ExecutionHandle::new(requester.clone(), output.into_output());
                    let _ = reply.send(Ok(handle));
                    Ok(())
                }
                Ok(conclusion) => {
                    let error = QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        format!("completion execution concluded as {conclusion:?}"),
                    );
                    let _ = reply.send(Err(error.clone()));
                    Err(error)
                }
                Err(error) => {
                    let supervision = actor_terminal_refusal(error);
                    let _ = reply.send(Err(actor_error(error)));
                    supervision
                }
            };
            let convergence = converge_supervised_active(
                &mut active,
                cancellation,
                &mut shutdown,
                &requester,
                config.remote_cleanup_timeout(),
            )
            .await;
            drop(active);
            (actor_result, convergence)
        }
        Ok(NativeAttemptTerminal::Failed(failure)) => {
            let client_error = failure.error().clone();
            let actor_result = actor
                .fail_attempt_with_error(running, client_error.clone())
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            let _ = reply.send(Err(client_error));
            let convergence = converge_supervised_active(
                &mut active,
                cancellation,
                &mut shutdown,
                &requester,
                config.remote_cleanup_timeout(),
            )
            .await;
            drop(active);
            (actor_result, convergence)
        }
        Err(()) => {
            let error = native_future_panicked("Native attempt run panicked");
            let actor_result = actor
                .fail_attempt_with_error(running, error.clone())
                .await
                .map(|_| ())
                .or_else(actor_terminal_refusal);
            let _ = reply.send(Err(error.clone()));
            let convergence = converge_supervised_active(
                &mut active,
                cancellation,
                &mut shutdown,
                &requester,
                config.remote_cleanup_timeout(),
            )
            .await;
            drop(active);
            (
                first_supervision_error(Err(error), actor_result),
                convergence,
            )
        }
    };
    let convergence_result = record_reported_active_convergence(
        &registry,
        &registration,
        schedule.contexts(),
        convergence,
    )
    .await;
    let retire_result = retire_logical(&registry, registration).await;
    first_supervision_error(
        actor_result,
        first_supervision_error(convergence_result, retire_result),
    )
}

struct RowsAttempt {
    schedule: super::AttemptSchedule,
    active: SupervisedActiveNativeOwner,
    running: super::RunningAttemptPermit,
    runtime: NativeRowsAttemptRuntime,
}

type ResidualRowsConvergence = JoinSet<(
    Box<[novarocks_execution_contract::QueryContextRef]>,
    ActiveAttemptConvergence,
)>;

/// The supervisor's local outcome for an active Native attempt. Native owns
/// only positive Worker/process closure evidence; bounded FE observation is a
/// Query Application fact and therefore has a separate variant.
#[derive(Debug)]
enum ActiveAttemptConvergence {
    Evidence(NativeAttemptConvergence),
    RemoteTrackingEnded {
        exact_contexts: Box<[novarocks_execution_contract::QueryContextRef]>,
    },
    Unresolved(StatementNativeHealthFailure),
    Recorded(Box<Result<(), QueryExecutionError>>),
}

fn supervise_residual_rows_attempt(
    residuals: &mut ResidualRowsConvergence,
    mut active: SupervisedActiveNativeOwner,
    contexts: Box<[novarocks_execution_contract::QueryContextRef]>,
    cancellation: novarocks_workload_control::CancellationView,
    mut shutdown: watch::Receiver<bool>,
    requester: novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) {
    residuals.spawn(async move {
        let convergence = converge_supervised_active(
            &mut active,
            cancellation,
            &mut shutdown,
            &requester,
            cleanup_timeout,
        )
        .await;
        (contexts, convergence)
    });
}

async fn converge_rows_attempts(
    active: &mut SupervisedActiveNativeOwner,
    active_contexts: &[novarocks_execution_contract::QueryContextRef],
    residuals: &mut ResidualRowsConvergence,
    cancellation: novarocks_workload_control::CancellationView,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) -> Result<(), QueryExecutionError> {
    let active_convergence =
        converge_supervised_active(active, cancellation, shutdown, requester, cleanup_timeout)
            .await;
    let result = record_reported_active_convergence(
        registry,
        registration,
        active_contexts,
        active_convergence,
    )
    .await;
    first_supervision_error(
        result,
        converge_residual_rows_attempts(residuals, registry, registration).await,
    )
}

async fn converge_residual_rows_attempts(
    residuals: &mut ResidualRowsConvergence,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
) -> Result<(), QueryExecutionError> {
    let mut result = Ok(());
    while let Some(joined) = residuals.join_next().await {
        let convergence = match joined {
            Ok((contexts, convergence)) => {
                record_reported_active_convergence(registry, registration, &contexts, convergence)
                    .await
            }
            Err(error) => {
                let diagnostic = QueryExecutionError::new(
                    QueryExecutionErrorKind::Failed,
                    format!("residual Native attempt convergence task failed: {error}"),
                );
                registry.record_native_supervision_join_failure(error);
                Err(diagnostic)
            }
        };
        result = first_supervision_error(result, convergence);
    }
    result
}

async fn drive_rows_attempt_until_pump_decision(
    active: &mut dyn ActiveNativeAttemptOwner,
    drive: &NativeAttemptDrive,
    cancellation: novarocks_workload_control::CancellationView,
    running: super::RunningAttemptPermit,
    root: novarocks_execution_contract::TaskIdentity,
    scope: novarocks_workload_control::WorkScope,
    runtime: NativeRowsAttemptRuntime,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> (Result<super::LogicalConclusion, ResultPumpFailure>,) {
    let (terminal_sender, terminal_source) = super::native_attempt_terminal_channel();
    let mut terminal_sender = Some(terminal_sender);
    let native_run = catch_future_panic(active.run(drive, cancellation));
    let pump = super::run_root_relay(
        running,
        root,
        scope,
        runtime.binding,
        runtime.statuses,
        terminal_source,
    );
    tokio::pin!(native_run);
    tokio::pin!(pump);
    let pump_result = loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
            outcome = &mut native_run, if terminal_sender.is_some() => {
                let terminal = outcome.unwrap_or_else(|()| {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        native_future_panic_input("Native row attempt run panicked"),
                    ))
                });
                terminal_sender
                    .take()
                    .expect("Native terminal sender is present while run is polled")
                    .publish(terminal);
            }
            outcome = &mut pump => break outcome,
        }
    };
    (pump_result,)
}

#[expect(
    clippy::too_many_arguments,
    reason = "the attempt supervisor takes exact per-query capabilities and active-attempt facts from the single coordinator assembly boundary"
)]
async fn supervise_rows(
    actor: &super::LogicalExecutionActor,
    session: &mut LogicalNativeSession,
    description: Arc<crate::preparation::FrozenExecutionDescription>,
    scope: novarocks_workload_control::WorkScope,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    active_plan: super::ActiveLogicalPlan,
    mut attempt: RowsAttempt,
) -> Result<(), QueryExecutionError> {
    let mut residuals = JoinSet::new();
    // One recovery window covers every replacement preparation. A failed
    // successor cannot renew the topology wait by starting another attempt.
    let mut replacement_preparation_deadline = None;
    loop {
        // Every attempt, the first and each replacement, runs the activated
        // plan itself: the one the description froze, by identity.
        if !Arc::ptr_eq(active_plan.plan(), description.completed_candidate().plan()) {
            return Err(QueryExecutionError::from_execution(
                ExecutionFailureInput::new(
                    QueryFailureReason::PreparationProtocol.failure_fact(),
                    SafeDetail::truncating("replacement attempt lost the activated physical plan"),
                    ExecutionFailureSite::Coordinator,
                ),
            ));
        }
        let drive = NativeAttemptDrive::new(&attempt.running);
        let (pump_result,) = drive_rows_attempt_until_pump_decision(
            attempt.active.as_mut(),
            &drive,
            scope.cancellation().map_err(work_error)?,
            attempt.running,
            attempt.schedule.root(),
            scope.clone(),
            attempt.runtime,
            shutdown,
            requester,
        )
        .await;
        attempt.active.begin_terminal_cut(Instant::now());
        drop(drive);

        match pump_result {
            Ok(super::LogicalConclusion::Succeeded) => {
                session.retire_preparation();
                return converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
            }
            Ok(conclusion) => {
                session.retire_preparation();
                let convergence = converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                return first_supervision_error(
                    Err(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        format!("row execution concluded as {conclusion:?}"),
                    )),
                    convergence,
                );
            }
            Err(ResultPumpFailure::Concluded(failure)) => {
                session.retire_preparation();
                let convergence = converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                return if matches!(
                    failure.conclusion(),
                    super::LogicalConclusion::Cancelled | super::LogicalConclusion::Failed
                ) {
                    convergence
                } else {
                    first_supervision_error(Err(failure.error().clone()), convergence)
                };
            }
            Err(ResultPumpFailure::ActorOutcomeUnknown(failure)) => {
                session.retire_preparation();
                let convergence = converge_rows_attempts(
                    &mut attempt.active,
                    attempt.schedule.contexts(),
                    &mut residuals,
                    scope.cancellation().map_err(work_error)?,
                    registry,
                    registration,
                    shutdown,
                    requester,
                    config.remote_cleanup_timeout(),
                )
                .await;
                return first_supervision_error(Err(failure.error().clone()), convergence);
            }
            Err(ResultPumpFailure::DecisionPending(decision)) => {
                let cancellation = scope.cancellation().map_err(work_error)?;
                let attempts_started =
                    u32::try_from(attempt.schedule.execution().attempt_id().get())
                        .unwrap_or(u32::MAX);
                let recovery = evaluate_recovery(RecoveryInput {
                    mode: description.recovery(),
                    effect: description.effect(),
                    failure: decision.class(),
                    output_visible: false,
                    attempts_started,
                    max_attempts: config.rows.max_attempts.get(),
                    deadline_reached: matches!(
                        cancellation.reason(),
                        Some(CancellationReason::DeadlineExceeded)
                    ),
                });
                if recovery != RecoveryDecision::BeginReplacement {
                    session.retire_preparation();
                    let actor_result = decision
                        .fail_logical(actor)
                        .await
                        .map(|_| ())
                        .or_else(actor_terminal_refusal);
                    let convergence = converge_rows_attempts(
                        &mut attempt.active,
                        attempt.schedule.contexts(),
                        &mut residuals,
                        cancellation,
                        registry,
                        registration,
                        shutdown,
                        requester,
                        config.remote_cleanup_timeout(),
                    )
                    .await;
                    return first_supervision_error(actor_result, convergence);
                }

                let replacement = match next_attempt(attempt.schedule.execution()) {
                    Ok(replacement) => replacement,
                    Err(error) => {
                        session.retire_preparation();
                        let actor_result = decision
                            .fail_logical(actor)
                            .await
                            .map(|_| ())
                            .or_else(actor_terminal_refusal);
                        let convergence = converge_rows_attempts(
                            &mut attempt.active,
                            attempt.schedule.contexts(),
                            &mut residuals,
                            cancellation,
                            registry,
                            registration,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(actor_result, convergence),
                        );
                    }
                };
                let preparation_deadline =
                    *replacement_preparation_deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now() + config.rows.replacement_reservation_valid_for
                    });
                let prepared = prepare_native_attempt(
                    session,
                    replacement,
                    decision.topology_requirement(),
                    frontend_process_id,
                    shutdown,
                    requester,
                    cancellation.clone(),
                    preparation_deadline,
                )
                .await;
                let (replacement_schedule, mut dormant) = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        session.retire_preparation();
                        let actor_outcome = decision.fail_logical(actor).await;
                        let supervision = preparation_supervision_result(error, actor_outcome);
                        let convergence = converge_rows_attempts(
                            &mut attempt.active,
                            attempt.schedule.contexts(),
                            &mut residuals,
                            cancellation,
                            registry,
                            registration,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        return first_supervision_error(supervision, convergence);
                    }
                };
                let qualification = match decision
                    .begin_replacement(actor, replacement, replacement_schedule.contexts().to_vec())
                    .await
                {
                    Ok((qualification, _)) => qualification,
                    Err(error) => {
                        session.retire_preparation();
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence = converge_rows_attempts(
                            &mut attempt.active,
                            attempt.schedule.contexts(),
                            &mut residuals,
                            scope.cancellation().map_err(work_error)?,
                            registry,
                            registration,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };

                supervise_residual_rows_attempt(
                    &mut residuals,
                    attempt.active,
                    attempt.schedule.contexts().to_vec().into_boxed_slice(),
                    cancellation.clone(),
                    shutdown.clone(),
                    requester.clone(),
                    config.remote_cleanup_timeout(),
                );
                let instantiation = match await_with_shutdown(
                    actor.activate_replacement(qualification),
                    shutdown,
                    requester,
                )
                .await
                {
                    Ok(instantiation) => instantiation,
                    Err(error) => {
                        session.retire_preparation();
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                let replacement_admissions = match await_with_shutdown(
                    instantiation.take_replacement_admission_evidence(),
                    shutdown,
                    requester,
                )
                .await
                {
                    Ok(admissions) => admissions,
                    Err(error) => {
                        session.retire_preparation();
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                let mut replacement_custody = match registry.reserve_active_native_owner(
                    registration,
                    replacement_schedule.execution(),
                    replacement_schedule.contexts(),
                ) {
                    Ok(reservation) => Some(reservation),
                    Err(error) => {
                        session.retire_preparation();
                        let actor_result = actor
                            .initialization_failed(instantiation)
                            .await
                            .map(|_| ())
                            .or_else(actor_terminal_refusal);
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            Err(registry_error(error)),
                            first_supervision_error(
                                actor_result,
                                first_supervision_error(replacement_convergence, old_convergence),
                            ),
                        );
                    }
                };
                let activated = await_with_shutdown(
                    catch_future_panic(dormant.activate(
                        &replacement_schedule,
                        replacement_admissions,
                        cancellation.clone(),
                    )),
                    shutdown,
                    requester,
                )
                .await;
                let activated = match activated {
                    Ok(Ok(activated)) => activated,
                    Ok(Err(failure)) => {
                        drop(replacement_custody.take());
                        session.retire_preparation();
                        let error = failure.into_failure().error().clone();
                        let actor_outcome = actor.initialization_failed(instantiation).await;
                        let supervision = preparation_supervision_result(error, actor_outcome);
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            supervision,
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                    Err(()) => {
                        drop(replacement_custody.take());
                        session.retire_preparation();
                        let error = native_future_panicked(
                            "Native replacement attempt activation panicked",
                        );
                        let actor_result = actor
                            .initialization_failed(instantiation)
                            .await
                            .map(|_| ())
                            .or_else(actor_terminal_refusal);
                        converge_dormant(dormant.as_mut(), cancellation, shutdown, requester).await;
                        let replacement_convergence = record_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            Err(error),
                            first_supervision_error(
                                actor_result,
                                first_supervision_error(replacement_convergence, old_convergence),
                            ),
                        );
                    }
                };
                drop(dormant);
                let (original_active, rows_runtime) = activated.into_parts();
                let active = replacement_custody
                    .take()
                    .expect("reserved before replacement activation")
                    .bind(original_active);
                let Some(rows_runtime) = rows_runtime else {
                    session.retire_preparation();
                    let error =
                        result_protocol_error("Native replacement omitted its root result runtime");
                    let actor_result = actor
                        .initialization_failed(instantiation)
                        .await
                        .map(|_| ())
                        .or_else(actor_terminal_refusal);
                    let mut active = active;
                    let convergence = converge_supervised_active(
                        &mut active,
                        cancellation,
                        shutdown,
                        requester,
                        config.remote_cleanup_timeout(),
                    )
                    .await;
                    drop(active);
                    let replacement_convergence = record_reported_active_convergence(
                        registry,
                        registration,
                        replacement_schedule.contexts(),
                        convergence,
                    )
                    .await;
                    let old_convergence =
                        converge_residual_rows_attempts(&mut residuals, registry, registration)
                            .await;
                    return first_supervision_error(
                        Err(error),
                        first_supervision_error(
                            actor_result,
                            first_supervision_error(replacement_convergence, old_convergence),
                        ),
                    );
                };
                let running = match actor.activate(instantiation.ready()).await {
                    Ok(running) => running,
                    Err(error) => {
                        session.retire_preparation();
                        let mut active = active;
                        let convergence = converge_supervised_active(
                            &mut active,
                            cancellation,
                            shutdown,
                            requester,
                            config.remote_cleanup_timeout(),
                        )
                        .await;
                        let replacement_convergence = record_reported_active_convergence(
                            registry,
                            registration,
                            replacement_schedule.contexts(),
                            convergence,
                        )
                        .await;
                        let old_convergence =
                            converge_residual_rows_attempts(&mut residuals, registry, registration)
                                .await;
                        return first_supervision_error(
                            actor_terminal_refusal(error),
                            first_supervision_error(replacement_convergence, old_convergence),
                        );
                    }
                };
                attempt = RowsAttempt {
                    schedule: replacement_schedule,
                    active,
                    running,
                    runtime: rows_runtime,
                };
            }
        }
    }
}

async fn prepare_native_attempt(
    session: &mut LogicalNativeSession,
    execution: QueryExecutionId,
    topology_requirement: crate::api::NativeAttemptTopologyRequirement,
    frontend_process_id: FrontendProcessId,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cancellation: novarocks_workload_control::CancellationView,
    deadline: tokio::time::Instant,
) -> Result<(super::AttemptSchedule, Box<dyn DormantNativeAttemptOwner>), QueryExecutionError> {
    let (request, acceptance) = session.issue_attempt(execution).map_err(contract_error)?;
    let preparation = await_with_shutdown(
        session.prepare(request.require_topology(topology_requirement)),
        shutdown,
        requester,
    );
    tokio::pin!(preparation);
    let prepared = tokio::select! {
        biased;
        reason = cancellation.cancelled() => {
            return Err(cancellation_error(
                cancellation.reason().unwrap_or(reason),
                "Native replacement preparation was cancelled",
            ));
        }
        _ = tokio::time::sleep_until(deadline) => {
            return Err(QueryExecutionError::from_execution(ExecutionFailureInput::new(
                QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact(),
                SafeDetail::truncating("Native replacement preparation exceeded its recovery budget"),
                ExecutionFailureSite::Coordinator,
            )));
        }
        prepared = &mut preparation => prepared,
    }
    .and_then(|prepared| acceptance.accept(prepared).map_err(Into::into))
    .map_err(native_prepare_error)?;
    let schedule = build_attempt_schedule(
        execution,
        frontend_process_id,
        &prepared.eligible_backends,
        &prepared.scheduling,
        #[cfg(novarocks_dev_controls)]
        prepared.owner.test_preferred_backend_process(execution),
    )
    .map_err(|error| {
        QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string())
    })?;
    Ok((schedule, prepared.owner))
}

fn next_attempt(execution: QueryExecutionId) -> Result<QueryExecutionId, QueryExecutionError> {
    let next = execution.attempt_id().get().checked_add(1).ok_or_else(|| {
        QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "logical execution attempt id space is exhausted",
        )
    })?;
    QueryExecutionId::new(
        execution.query_id(),
        AttemptId::new(next).expect("checked successor attempt id is nonzero"),
    )
    .map_err(|error| QueryExecutionError::new(QueryExecutionErrorKind::Failed, error.to_string()))
}

async fn record_active_convergence(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    contexts: &[novarocks_execution_contract::QueryContextRef],
) -> Result<(), QueryExecutionError> {
    let mut first_error = None;
    for &context in contexts {
        if let Err(error) = registry
            .observe_worker_stopped_and_context_fenced(registration, context)
            .await
        {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn record_reported_active_convergence(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    expected_contexts: &[novarocks_execution_contract::QueryContextRef],
    convergence: ActiveAttemptConvergence,
) -> Result<(), QueryExecutionError> {
    let convergence = match convergence {
        ActiveAttemptConvergence::Recorded(result) => return *result,
        ActiveAttemptConvergence::Unresolved(failure) => {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                failure.detail(),
            ));
        }
        ActiveAttemptConvergence::RemoteTrackingEnded { exact_contexts } => {
            let expected = expected_contexts.iter().copied().collect::<BTreeSet<_>>();
            let reported = exact_contexts.iter().copied().collect::<BTreeSet<_>>();
            if expected != reported || reported.len() != exact_contexts.len() {
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "Native remote tracking omitted or duplicated original contexts",
                ));
            }
            return record_remote_tracking_ended(registry, registration, expected_contexts).await;
        }
        ActiveAttemptConvergence::Evidence(convergence) => convergence,
    };
    let NativeAttemptConvergence::Contexts(facts) = convergence else {
        return record_active_convergence(registry, registration, expected_contexts).await;
    };
    let expected = expected_contexts.iter().copied().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for fact in &facts {
        if !expected.contains(&fact.context()) || !observed.insert(fact.context()) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "Native active convergence returned duplicate or foreign context evidence",
            ));
        }
    }
    if observed != expected {
        return Err(QueryExecutionError::new(
            QueryExecutionErrorKind::InvalidRequest,
            "Native active convergence omitted scheduled context evidence",
        ));
    }
    let mut first_error = None;
    for fact in facts {
        let result = match fact.kind() {
            NativeContextConvergenceKind::NeverEstablished => {
                registry
                    .observe_context_never_established(registration, fact.context())
                    .await
            }
            NativeContextConvergenceKind::WorkerStoppedAndContextFenced => {
                registry
                    .observe_worker_stopped_and_context_fenced(registration, fact.context())
                    .await
            }
            NativeContextConvergenceKind::WorkerProcessReplaced => {
                registry
                    .observe_worker_process_replaced(registration, fact.context())
                    .await
            }
        };
        if let Err(error) = result {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn record_remote_tracking_ended(
    registry: &LogicalExecutionRuntimeRegistryHandle,
    registration: &super::LogicalExecutionRegistration,
    contexts: &[novarocks_execution_contract::QueryContextRef],
) -> Result<(), QueryExecutionError> {
    let mut first_error = None;
    for &context in contexts {
        if let Err(error) = registry
            .end_remote_context_tracking(registration, context)
            .await
        {
            first_error.get_or_insert_with(|| registry_error(error));
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn converge_dormant(
    owner: &mut dyn DormantNativeAttemptOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) {
    loop {
        let convergence = async { owner.converge(cancellation.clone()).await };
        if await_with_shutdown(catch_future_panic(convergence), shutdown, requester)
            .await
            .is_ok()
        {
            return;
        }
        let _ = requester.request(CancellationReason::Requested);
        tokio::time::sleep(NATIVE_CONVERGENCE_PANIC_BACKOFF).await;
    }
}

async fn converge_supervised_active(
    owner: &mut SupervisedActiveNativeOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) -> ActiveAttemptConvergence {
    let request = owner.request(cancellation, cleanup_timeout);
    let (outcome, deadline) =
        converge_active_request(owner.as_mut(), request, shutdown, requester).await;
    owner.shorten_remote_deadline(deadline);
    let result = match outcome {
        ActiveAttemptConvergence::Evidence(evidence) => {
            owner
                .record_and_close(NativeActiveConvergenceClosure::Evidence(evidence))
                .await
        }
        ActiveAttemptConvergence::RemoteTrackingEnded { exact_contexts } => {
            owner
                .record_and_close(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_contexts,
                })
                .await
        }
        ActiveAttemptConvergence::Unresolved(failure) => Err(owner.retain_unresolved(failure)),
        ActiveAttemptConvergence::Recorded(result) => *result,
    };
    ActiveAttemptConvergence::Recorded(Box::new(result))
}

#[cfg(test)]
async fn converge_active(
    owner: &mut dyn ActiveNativeAttemptOwner,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
    cleanup_timeout: Duration,
) -> ActiveAttemptConvergence {
    converge_active_request(
        owner,
        NativeActiveConvergenceRequest::new(cancellation, Instant::now() + cleanup_timeout),
        shutdown,
        requester,
    )
    .await
    .0
}

async fn converge_active_request(
    owner: &mut dyn ActiveNativeAttemptOwner,
    request: NativeActiveConvergenceRequest,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> (ActiveAttemptConvergence, Instant) {
    let cancellation = request.cancellation().clone();
    let mut deadline = request.remote_observation_deadline();
    let mut shutdown_cut = false;
    loop {
        if *shutdown.borrow() && !shutdown_cut {
            let _ = requester.request(CancellationReason::ServerShutdown);
            deadline = deadline.min(Instant::now());
            shutdown_cut = true;
        }
        enum Turn {
            Closed(
                Result<
                    Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure>,
                    StatementNativeHealthFailure,
                >,
            ),
            Shutdown,
        }
        let turn_deadline = deadline;
        let turn = {
            let convergence = super::catch_native_convergence_panic(async {
                owner
                    .converge(NativeActiveConvergenceRequest::new(
                        cancellation.clone(),
                        turn_deadline,
                    ))
                    .await
            });
            tokio::pin!(convergence);
            tokio::select! {
                result = &mut convergence => Turn::Closed(result),
                changed = shutdown.changed(), if !shutdown_cut => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = requester.request(CancellationReason::ServerShutdown);
                        deadline = deadline.min(Instant::now());
                        shutdown_cut = true;
                    }
                    Turn::Shutdown
                }
            }
        };
        let result = match turn {
            Turn::Shutdown => continue,
            Turn::Closed(result) => result,
        };
        let outcome = match result {
            Ok(Ok(NativeActiveConvergenceClosure::Evidence(evidence))) => {
                ActiveAttemptConvergence::Evidence(evidence)
            }
            Ok(Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts })) => {
                ActiveAttemptConvergence::RemoteTrackingEnded { exact_contexts }
            }
            Ok(Err(health)) | Err(health) => ActiveAttemptConvergence::Unresolved(health),
        };
        return (outcome, deadline);
    }
}

async fn catch_future_panic<F>(future: F) -> Result<F::Output, ()>
where
    F: Future,
{
    let mut future = Box::pin(future);
    std::future::poll_fn(move |context| {
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_payload) => Poll::Ready(Err(())),
        }
    })
    .await
}

async fn await_with_shutdown<F>(
    future: F,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> F::Output
where
    F: Future,
{
    tokio::pin!(future);
    let mut shutdown_open = true;
    loop {
        if *shutdown.borrow() {
            let _ = requester.request(CancellationReason::ServerShutdown);
            return future.await;
        }
        tokio::select! {
            output = &mut future => return output,
            changed = shutdown.changed(), if shutdown_open => {
                shutdown_open = changed.is_ok();
                if !shutdown_open || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
        }
    }
}

/// Outcome of waiting for an operation before its work owner transfers to a
/// logical-execution actor.
enum PreInstallWait<T> {
    Completed(T),
    Cancelled(CancellationReason),
}

/// Waits for a pre-install operation without letting a deadline strand the
/// only work owner behind an unbounded Native or admission wait.
///
/// This is deliberately limited to the interval before actor installation:
/// there is then no Task, output, or effect owner whose convergence must be
/// observed. Once installed, cancellation remains actor-owned and ordinary
/// control delivery cannot be acknowledged as a substitute for terminal
/// facts. Shutdown retains its existing behavior and lets the in-flight
/// operation observe its shutdown cancellation before returning.
async fn await_before_actor_install<F>(
    future: F,
    cancellation: novarocks_workload_control::CancellationView,
    shutdown: &mut watch::Receiver<bool>,
    requester: &novarocks_workload_control::WorkCancellationRequester,
) -> PreInstallWait<F::Output>
where
    F: Future,
{
    if let Some(reason) = cancellation.reason() {
        return PreInstallWait::Cancelled(reason);
    }
    tokio::pin!(future);
    let mut shutdown_open = true;
    loop {
        if *shutdown.borrow() {
            let _ = requester.request(CancellationReason::ServerShutdown);
            return PreInstallWait::Completed(future.await);
        }
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                let _ = requester.request(reason.clone());
                return PreInstallWait::Cancelled(
                    cancellation.reason().unwrap_or(reason),
                );
            }
            output = &mut future => return PreInstallWait::Completed(output),
            changed = shutdown.changed(), if shutdown_open => {
                shutdown_open = changed.is_ok();
                if !shutdown_open || *shutdown.borrow() {
                    let _ = requester.request(CancellationReason::ServerShutdown);
                }
            }
        }
    }
}

struct PendingWorkOwner(Option<WorkOwner>);

impl PendingWorkOwner {
    fn owner(&self) -> &WorkOwner {
        self.0
            .as_ref()
            .expect("pending start retains its work owner")
    }

    fn take(&mut self) -> WorkOwner {
        self.0
            .take()
            .expect("pending start transfers its work owner once")
    }

    /// A start that failed before installing an actor has no Task, output, or
    /// external-effect owner left to deliver its cancellation control. Its
    /// terminal error is therefore the exact settlement boundary for a queued
    /// cancel intent.
    fn complete_uninstalled_failure(mut self) {
        self.take().complete_after_terminal_cancel_settled();
    }
}

impl Drop for PendingWorkOwner {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            owner.complete_after_terminal_cancel_settled();
        }
    }
}

fn fail_uninstalled_start(
    owner: PendingWorkOwner,
    reply: oneshot::Sender<Result<ExecutionHandle, QueryExecutionError>>,
    error: QueryExecutionError,
) -> Result<(), QueryExecutionError> {
    owner.complete_uninstalled_failure();
    let _ = reply.send(Err(error));
    Ok(())
}

async fn retire_logical(
    registry: &super::LogicalExecutionRuntimeRegistryHandle,
    registration: super::LogicalExecutionRegistration,
) -> Result<(), QueryExecutionError> {
    if let Some(error) = registry.active_native_registration_health(&registration) {
        return Err(error);
    }
    registry
        .join_and_retire(registration)
        .await
        .map_err(registry_error)
}

fn first_supervision_error(
    first: Result<(), QueryExecutionError>,
    convergence: Result<(), QueryExecutionError>,
) -> Result<(), QueryExecutionError> {
    first.and(convergence)
}

fn contract_error(error: crate::api::NativeExecutionContractError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        ExecutionFailureSite::Coordinator,
    ))
}

fn result_protocol_error(detail: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::ResultProtocol.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

fn native_open_error(error: LogicalNativeOpenError) -> QueryExecutionError {
    match error {
        LogicalNativeOpenError::Contract(error) => contract_error(error),
        LogicalNativeOpenError::Runtime(failure) => failure.error().clone(),
    }
}

fn native_prepare_error(error: NativeAttemptPreparationError) -> QueryExecutionError {
    match error {
        NativeAttemptPreparationError::Contract(error) => contract_error(error),
        NativeAttemptPreparationError::Runtime(failure) => failure.error().clone(),
    }
}

/// Only a terminal disposition authored by the actor proves this command
/// lost to an already accepted conclusion. Other refusals remain failures.
fn actor_terminal_refusal(
    error: super::LogicalExecutionActorError,
) -> Result<(), QueryExecutionError> {
    match error {
        super::LogicalExecutionActorError::ExecutionConcluded(_) => Ok(()),
        error => Err(actor_error(error)),
    }
}

fn preparation_supervision_result(
    error: QueryExecutionError,
    actor_outcome: Result<super::LogicalConclusion, super::LogicalExecutionActorError>,
) -> Result<(), QueryExecutionError> {
    let accepted_control_conclusion = matches!(
        &actor_outcome,
        Err(super::LogicalExecutionActorError::ExecutionConcluded(_))
    ) && matches!(
        error.kind(),
        QueryExecutionErrorKind::Cancelled | QueryExecutionErrorKind::DeadlineExceeded
    );
    let actor_result = actor_outcome.map(|_| ()).or_else(actor_terminal_refusal);
    if accepted_control_conclusion {
        actor_result
    } else {
        first_supervision_error(Err(error), actor_result)
    }
}

fn actor_error(error: super::LogicalExecutionActorError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        ExecutionFailureSite::Coordinator,
    ))
}

fn registry_error(error: LogicalExecutionRuntimeRegistryError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        error.safe_detail(),
        ExecutionFailureSite::Coordinator,
    ))
}

fn work_error(error: WorkError) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        error.failure_fact(),
        SafeDetail::truncating("query workload control refused the operation"),
        ExecutionFailureSite::Coordinator,
    ))
}

fn pre_install_cancellation_error(reason: CancellationReason) -> QueryExecutionError {
    cancellation_error(
        reason,
        "logical execution cancelled before actor installation",
    )
}

fn native_future_panic_input(message: &'static str) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        QueryFailureReason::NativeFuturePanicked.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    )
}

fn native_future_panicked(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(native_future_panic_input(message))
}

#[cfg(test)]
mod tests {
    #[test]
    fn terminal_actor_disposition_does_not_hide_invalid_phase_or_service_invariants() {
        use super::super::{LogicalConclusion, LogicalExecutionActorError};
        assert!(
            actor_terminal_refusal(LogicalExecutionActorError::ExecutionConcluded(
                LogicalConclusion::Cancelled
            ))
            .is_ok()
        );
        for error in [
            LogicalExecutionActorError::WrongPhase,
            LogicalExecutionActorError::StaleAuthority,
            LogicalExecutionActorError::WrongExecution,
            LogicalExecutionActorError::InvariantViolation,
            LogicalExecutionActorError::MailboxClosed,
        ] {
            let failure = actor_terminal_refusal(error).unwrap_err();
            assert_eq!(
                failure.execution_failure().unwrap().fact(),
                error.failure_fact()
            );
            let cancellation = cancellation_error(
                CancellationReason::Requested,
                "the preparation observed workload cancellation",
            );
            assert!(preparation_supervision_result(cancellation, Err(error)).is_err());
        }
    }

    #[test]
    fn producer_failures_keep_their_facts_instead_of_claiming_query_cancellation_or_deadline() {
        use novarocks_execution_contract::failure::{FailureClass, FailureEvidence};
        let source = super::super::LogicalExecutionActorError::WrongPhase;
        let error = super::actor_error(source);
        let input = error.execution_failure().unwrap();
        assert_eq!(input.fact(), source.failure_fact());
        assert_eq!(input.detail(), &source.safe_detail());
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            input.attempt_class(),
            novarocks_coordination_contract::AttemptFailureClass::ExecutionFailure
        );

        let source = WorkError::CapacityWaitTimeout;
        let error = super::work_error(source.clone());
        let input = error.execution_failure().unwrap();
        assert_eq!(input.fact(), source.failure_fact());
        assert_eq!(input.fact().class(), FailureClass::Resource);
        assert_eq!(input.fact().resource(), None);
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);

        let source = WorkError::Cancelled(CancellationReason::Requested);
        let error = super::work_error(source.clone());
        let input = error.execution_failure().unwrap();
        assert_eq!(input.fact(), source.failure_fact());
        assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
    }

    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    use novarocks_execution_contract::{
        AdmissionEpochCapability, AdmissionTicketId, CodecOwnedContent, ConfidentialContent,
        ContentFingerprint, CredentialEpoch, CredentialLeaseId, CredentialUpdate,
        EstablishQueryContext, LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt,
        QueryContextRef, TaskOperationId, TaskOutputFacts, TaskState, TaskStatus,
        TaskStatusVersion,
    };
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{BackendProcessId, FrontendProcessId};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    use crate::api::{
        ActivatedNativeAttempt, ActiveNativeAttemptOwner, DormantNativeAttemptOwner,
        LogicalNativeOpenFailure, LogicalNativeOpenFuture, NativeActiveAttemptConvergenceFuture,
        NativeAttemptActivationFailure, NativeAttemptActivationFuture, NativeAttemptConvergence,
        NativeAttemptConvergenceFuture, NativeAttemptPreparationFailure,
        NativeAttemptPreparationFuture, NativeAttemptPreparationPort,
        NativeAttemptPreparationRequest, NativeAttemptRunFuture,
    };
    use crate::coordination::{
        AdmissionIssueSettlement, EstablishIssueIdentity, EstablishIssueSubmit,
        EstablishTransportAdmission, EstablishTransportReservation, EstablishTransportSink,
        EstablishTransportSubmission,
    };
    use crate::coordination::{
        AttemptSchedule, PermanentlyBackpressuredAbortEffectPort,
        ReplacementWorkerAdmissionEvidence,
    };
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescription, OutputContract,
    };
    use novarocks_coordination_contract::AttemptFailureClass;

    use super::*;

    /// A test-owned injected failure is a legal execution failure, not a
    /// production protocol violation or a fictitious Task failure.
    #[derive(Clone, Copy, Debug)]
    enum FixtureFailureReason {
        InjectedExecution,
    }
    impl FailureProjection for FixtureFailureReason {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
            match self {
                Self::InjectedExecution => FailureFact::new(
                    FailureClass::Unexpected,
                    FailureReason::new(
                        std::num::NonZeroU16::MAX,
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
            }
        }
    }
    fn fixture_execution_input(message: &'static str) -> ExecutionFailureInput {
        ExecutionFailureInput::new(
            FixtureFailureReason::InjectedExecution.failure_fact(),
            SafeDetail::truncating(message),
            ExecutionFailureSite::Coordinator,
        )
    }
    fn fixture_cancellation_input(
        reason: CancellationReason,
        message: &'static str,
    ) -> ExecutionFailureInput {
        let reason = match reason {
            CancellationReason::DeadlineExceeded
            | CancellationReason::FrontendDrainDeadlineExceeded => {
                QueryFailureReason::DeadlineExceeded
            }
            _ => QueryFailureReason::Cancelled,
        };
        ExecutionFailureInput::new(
            reason.failure_fact(),
            SafeDetail::truncating(message),
            ExecutionFailureSite::Coordinator,
        )
    }
    fn fixture_root_reply_failure(
        root: novarocks_execution_contract::TaskIdentity,
        error: WorkError,
    ) -> super::super::RootResultFetchFailure {
        tracing::debug!(error = %error, "fixture root retained reply violated its contract");
        super::super::RootResultFetchFailure::from_input(ExecutionFailureInput::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("fixture root reply violated its retained window contract"),
            ExecutionFailureSite::RootRead { root },
        ))
    }

    /// Scheduling facts for a request whose plan reads no provider.
    ///
    /// The shape comes from the description's own plan, so a test schedules
    /// the fragments its statement actually has.
    fn no_scan_scheduling(
        request: &crate::api::NativeAttemptPreparationRequest,
    ) -> crate::api::ExecutionSchedulingFacts {
        crate::api::ExecutionSchedulingFacts::from_frozen_description(
            request.description(),
            &std::collections::BTreeMap::new(),
        )
        .expect("a plan with no provider read needs no enumerated work")
    }

    fn supervisor_config() -> LogicalExecutionSupervisorConfig {
        LogicalExecutionSupervisorConfig::new(
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            LogicalExecutionRowsConfig::new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU32::new(2).unwrap(),
                Duration::from_secs(30),
            ),
        )
    }

    #[derive(Debug)]
    struct NeverConverges;

    impl ActiveNativeAttemptOwner for NeverConverges {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            // No local work exists in this fixture; only remote observation is
            // stuck. The true owner, rather than the supervisor, ends it.
            Box::pin(async move {
                tokio::time::sleep_until(request.remote_observation_deadline().into()).await;
                Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_contexts: Box::new([]),
                })
            })
        }
    }

    #[tokio::test]
    async fn active_cleanup_deadline_ends_fe_tracking_without_fabricating_stop_evidence() {
        let (_control, root) = governance();
        let cancellation = root.owner.scope().cancellation().unwrap();
        let requester = root.owner.cancellation_requester();
        let (_shutdown_sender, mut shutdown) = watch::channel(false);
        let mut owner = NeverConverges;

        let outcome = converge_active(
            &mut owner,
            cancellation,
            &mut shutdown,
            &requester,
            Duration::from_millis(1),
        )
        .await;

        assert!(matches!(
            outcome,
            ActiveAttemptConvergence::RemoteTrackingEnded { .. }
        ));
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test]
    async fn supervisor_shutdown_ends_active_remote_tracking_without_waiting_for_cleanup_window() {
        let (_control, root) = governance();
        let cancellation = root.owner.scope().cancellation().unwrap();
        let requester = root.owner.cancellation_requester();
        let (shutdown_sender, mut shutdown) = watch::channel(false);
        let mut owner = NeverConverges;
        shutdown_sender.send_replace(true);

        let outcome = tokio::time::timeout(
            Duration::from_millis(10),
            converge_active(
                &mut owner,
                cancellation,
                &mut shutdown,
                &requester,
                Duration::from_secs(30),
            ),
        )
        .await
        .expect("shutdown must not wait for the normal cleanup window");

        assert!(matches!(
            outcome,
            ActiveAttemptConvergence::RemoteTrackingEnded { .. }
        ));
        root.owner.complete();
        root.business.release();
    }

    #[derive(Debug)]
    struct HeldLocalConvergence(Arc<tokio::sync::Notify>);
    impl ActiveNativeAttemptOwner for HeldLocalConvergence {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancel: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }
        fn converge<'a>(
            &'a mut self,
            _request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.0.notified().await;
                Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                    exact_contexts: Box::new([]),
                })
            })
        }
    }
    #[tokio::test]
    async fn shutdown_remote_budget_never_closes_a_held_original_local_owner() {
        let (_control, root) = governance();
        let cancellation = root.owner.scope().cancellation().unwrap();
        let requester = root.owner.cancellation_requester();
        let (shutdown_sender, mut shutdown) = watch::channel(false);
        shutdown_sender.send_replace(true);
        let release = Arc::new(tokio::sync::Notify::new());
        let mut owner = HeldLocalConvergence(Arc::clone(&release));
        let convergence = converge_active(
            &mut owner,
            cancellation,
            &mut shutdown,
            &requester,
            Duration::from_secs(30),
        );
        tokio::pin!(convergence);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut convergence)
                .await
                .is_err()
        );
        release.notify_one();
        assert!(matches!(
            convergence.await,
            ActiveAttemptConvergence::RemoteTrackingEnded { .. }
        ));
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
    }

    #[tokio::test]
    async fn accepted_start_returns_on_cancellation_before_supervisor_verdict() {
        let (starts, mut start_rx) = mpsc::unbounded_channel();
        let (_shutdown, shutdown) = watch::channel(false);
        let client = QueryExecutionClient::new(QueryExecutionDriverHandle { starts, shutdown });
        let (_control, root) = governance();
        let requester = root.owner.cancellation_requester();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
        let command = start_rx
            .recv()
            .await
            .expect("accepted start reaches the supervisor queue");

        requester
            .request(CancellationReason::DeadlineExceeded)
            .expect("active root accepts deadline cancellation");
        let error = match tokio::time::timeout(Duration::from_secs(1), start)
            .await
            .expect("accepted start returns after cancellation")
        {
            Ok(_) => panic!("cancellation wins before a start verdict"),
            Err(error) => error,
        };
        // The reason survives the race: this start was cancelled by its own
        // deadline, and says so rather than reporting the generic outcome
        // both causes share.
        assert_eq!(
            error.kind(),
            QueryExecutionErrorKind::DeadlineExceeded,
            "{}",
            error.message()
        );

        command.owner.complete_after_terminal_cancel_settled();
        root.business.release();
    }

    #[tokio::test]
    async fn uninstalled_failure_settles_its_known_cancel_control() {
        let (control, root) = governance();
        let scope = root.owner.scope();
        root.owner.cancel(CancellationReason::Requested);

        PendingWorkOwner(Some(root.owner)).complete_uninstalled_failure();
        root.business.release();

        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(
            control.next_control().is_none(),
            "a failure before actor installation has no effect owner left to acknowledge"
        );
    }

    fn fixture_version() -> [u8; 16] {
        static NEXT_VERSION: AtomicU64 = AtomicU64::new(1);
        let sequence = NEXT_VERSION.fetch_add(1, Ordering::Relaxed);
        let mut version = [72; 16];
        version[8..].copy_from_slice(&sequence.to_be_bytes());
        version
    }

    fn completion_request(attempts: impl NativeAttemptPreparationPort) -> QueryExecutionRequest {
        let plan = crate::completed_plan_fixture::completed_noop_plan(fixture_version());
        let description = FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Maintenance,
            plan.candidate().clone(),
            Vec::new(),
            OutputContract::CompletionOnly,
            novarocks_coordination_contract::ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            attempts,
        )
    }

    fn rows_request(
        _control: &WorkloadControl,
        scope: &novarocks_workload_control::WorkScope,
        recovery: RecoveryMode,
        replacements: Option<Arc<dyn super::super::ReplacementQualificationEffectPort>>,
        mut attempts: impl TestRowsPreparationPort,
    ) -> QueryExecutionRequest {
        let plan = crate::completed_plan_fixture::completed_values_plan_blocking(fixture_version());
        use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
        let render =
            novarocks_sql::compiler::client_render_schema(plan.candidate().plan(), 0).unwrap();
        let candidate = crate::preparation::CompletedPhysicalPlanCandidate::for_program(
            plan.candidate()
                .plan()
                .as_ref()
                .clone()
                .with_root_output(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::ClientRows(render),
                ))
                .unwrap(),
            &novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let window = scope
            .result_capacity()
            .unwrap()
            .try_acquire(scope, novarocks_workload_control::ResultWindowClass::Client)
            .unwrap();
        attempts.bind_window(window.retain_alias());
        let output = OutputContract::from_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate.plan(),
        )
        .unwrap();
        let description = FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Read,
            candidate,
            Vec::new(),
            output,
            novarocks_coordination_contract::ExecutionEffect::None,
            recovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap();
        QueryExecutionRequest::bind_native(
            description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            replacements,
            attempts,
        )
    }

    #[derive(Clone)]
    struct TestWindow(novarocks_workload_control::ResultWindowAlias);
    impl std::fmt::Debug for TestWindow {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestWindow")
                .field("scope", &self.0.scope_id())
                .finish()
        }
    }

    trait TestRowsPreparationPort: NativeAttemptPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias);
    }

    struct TestRootReadPort {
        sent: AtomicBool,
        payload: Option<&'static [u8]>,
    }
    impl crate::api::BoundedRootReadPort for TestRootReadPort {
        fn read(
            &self,
            request: novarocks_execution_contract::root_result::RootResultRead,
            guard: novarocks_workload_control::ResultWindowAlias,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            crate::api::RetainedRootReply,
                            super::super::RootResultFetchFailure,
                        >,
                    > + Send,
            >,
        > {
            use novarocks_execution_contract::root_result::{
                RootReadOutcome, RootResultData, RootResultReply,
            };
            let payload = if !self.sent.swap(true, Ordering::SeqCst) {
                self.payload
            } else {
                None
            };
            Box::pin(async move {
                let Some(payload) = payload else {
                    return std::future::pending().await;
                };
                let reply = RootResultReply {
                    root_task: request.root_task(),
                    profile: request.profile(),
                    kind: request.kind(),
                    accepted_consumed: request.consumed(),
                    outcome: RootReadOutcome::Data(
                        RootResultData::try_new(
                            request.kind(),
                            std::num::NonZeroU64::MIN,
                            bytes::Bytes::from_static(payload),
                            None,
                        )
                        .unwrap(),
                    ),
                };
                crate::api::RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    guard,
                    64 * 1024,
                )
                .map_err(|error| fixture_root_reply_failure(request.root_task(), error))
            })
        }
        fn seal(
            &self,
            _: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    fn test_root_binding(
        root: novarocks_execution_contract::TaskIdentity,
        window: novarocks_workload_control::ResultWindowAlias,
        payload: Option<&'static [u8]>,
    ) -> super::super::RootRelayBinding {
        use novarocks_result_contract::{
            ClientRowProfile, RootOutputKind, RootProfileId, RootProfileV1,
        };
        super::super::RootRelayBinding {
            port: Arc::new(TestRootReadPort {
                sent: AtomicBool::new(false),
                payload,
            }),
            frontier: super::super::RootRelayFrontier::new(
                root,
                RootProfileId::V1,
                RootOutputKind::ClientRows,
                Some(
                    ClientRowProfile::try_new(
                        RootProfileV1::SEGMENT_BYTES,
                        RootProfileV1::ROW_PAYLOAD_BYTES,
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
            window,
            max_wait: Duration::from_millis(10),
        }
    }

    fn running_status(root: novarocks_execution_contract::TaskIdentity) -> TaskStatus {
        TaskStatus::try_new(
            root,
            TaskStatusVersion::new(1).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::new(false),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct UnreachablePreparationPort;

    impl NativeAttemptPreparationPort for UnreachablePreparationPort {
        fn prepare(
            &mut self,
            _request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            Box::pin(async { panic!("unreachable Native attempt preparation") })
        }
    }

    #[derive(Debug)]
    struct DropTrackedPreparationPort {
        drops: Arc<AtomicU64>,
    }

    impl Drop for DropTrackedPreparationPort {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl NativeAttemptPreparationPort for DropTrackedPreparationPort {
        fn prepare(
            &mut self,
            _request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            Box::pin(async { panic!("rejected Native seed must never prepare an attempt") })
        }
    }

    fn drop_tracked_request(drops: &Arc<AtomicU64>) -> QueryExecutionRequest {
        completion_request(DropTrackedPreparationPort {
            drops: Arc::clone(drops),
        })
    }

    fn governance() -> (WorkloadControl, RootWork) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: (1 << 20) - (1 << 12),
            },
        )
        .unwrap();
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        (control, root)
    }

    #[tokio::test]
    async fn cancelled_rejected_start_settles_uninstalled_control() {
        let (control, root) = governance();
        let cancellation = root.owner.scope().cancellation().unwrap();
        root.owner.cancel(CancellationReason::DeadlineExceeded);
        let error = match rejected_start(root.owner, "supervisor closed").await {
            Err(error) => error,
            Ok(_) => panic!("closed supervisor cannot accept the start"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            error.execution_failure().is_none(),
            "local admission is not a Task failure"
        );
        assert_eq!(
            cancellation.reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        root.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(control.next_control().is_none());
    }

    fn acknowledge_all_control(control: &WorkloadControl) {
        while let Some(permit) = control.next_control() {
            permit.acknowledge();
        }
    }

    #[derive(Debug)]
    struct FailingActivationNativePort {
        opened_execution_low: Arc<AtomicU64>,
    }

    impl LogicalExecutionNativePort for FailingActivationNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let execution = request.initial_execution();
            self.opened_execution_low
                .store(execution.query_id().low() as u64, Ordering::SeqCst);
            Box::pin(async move { request.bind().map_err(Into::into) })
        }
    }

    #[derive(Debug)]
    struct FailingActivationPreparationPort {
        backend: BackendProcessId,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl NativeAttemptPreparationPort for FailingActivationPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            let dormant = FailingDormantOwner {
                backend,
                activated: Arc::clone(&self.activated),
                residual_converged: Arc::clone(&self.residual_converged),
            };
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request.bind(scheduling, dormant).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct FailingDormantOwner {
        backend: BackendProcessId,
        activated: Arc<AtomicBool>,
        residual_converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for FailingDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            self.activated.store(true, Ordering::SeqCst);
            Box::pin(async move {
                Err(NativeAttemptActivationFailure::new(
                    NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                        "injected Native activation failure",
                    )),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.residual_converged.store(true, Ordering::SeqCst);
            })
        }
    }

    type FailingNativeFixture = (
        Arc<dyn LogicalExecutionNativePort>,
        FailingActivationPreparationPort,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    );

    fn failing_native() -> FailingNativeFixture {
        let opened = Arc::new(AtomicU64::new(0));
        let activated = Arc::new(AtomicBool::new(false));
        let residual = Arc::new(AtomicBool::new(false));
        (
            Arc::new(FailingActivationNativePort {
                opened_execution_low: Arc::clone(&opened),
            }),
            FailingActivationPreparationPort {
                backend: BackendProcessId::new_v7(),
                activated: Arc::clone(&activated),
                residual_converged: Arc::clone(&residual),
            },
            opened,
            activated,
            residual,
        )
    }

    /// An owner abandoned by an unwind must not replace the panic that
    /// abandoned it. Without the guard in its destructor this test does not
    /// fail: it aborts the whole binary and takes every other test's result
    /// with it.
    #[tokio::test]
    async fn an_owner_abandoned_by_an_unwind_reports_the_original_panic() {
        let (supervisor, _client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            failing_native().0,
            QueryProcessNamespace::new(0x4f),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let payload = catch_unwind(AssertUnwindSafe(move || {
            let _abandoned = supervisor;
            panic!("the assertion that actually failed");
        }))
        .expect_err("the closure panics");

        assert_eq!(
            *payload
                .downcast_ref::<&str>()
                .expect("the original panic payload survives the unwind"),
            "the assertion that actually failed"
        );
    }

    #[test]
    fn native_contract_wrappers_keep_original_fact_and_safe_detail() {
        use crate::api::NativeExecutionContractError;
        for source in [
            NativeExecutionContractError::ForeignAttemptTicket,
            NativeExecutionContractError::AttemptPreparationRetired,
        ] {
            for error in [
                native_open_error(source.into()),
                native_prepare_error(source.into()),
            ] {
                let input = error
                    .execution_failure()
                    .expect("native contract author must retain its fact");
                assert_eq!(input.fact(), source.failure_fact());
                assert_eq!(input.detail(), &source.safe_detail());
                assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
                assert_eq!(
                    input.topology_requirement(),
                    crate::api::NativeAttemptTopologyRequirement::LiveSnapshot
                );
                assert_eq!(
                    input.user_error().phase(),
                    novarocks_user_error::ErrorPhase::Execution
                );
                assert_eq!(input.user_error().location(), None);
            }
        }
    }

    #[tokio::test]
    async fn activation_failure_retains_residual_and_retires_exact_registry_entry() {
        let (native, attempts, opened, activated, residual) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x45),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(attempts), root.owner).await {
            Ok(_) => panic!("activation failure must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.message(), "injected Native activation failure");
        assert_eq!(
            error.execution_failure(),
            Some(&fixture_execution_input(
                "injected Native activation failure"
            ))
        );
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_ne!(opened.load(Ordering::SeqCst), 0);
        assert!(activated.load(Ordering::SeqCst));
        assert!(residual.load(Ordering::SeqCst));
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn dropping_an_accepted_reply_does_not_drop_its_work_owner() {
        let (native, attempts, _opened, activated, residual) = failing_native();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x46),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let future = client.start(completion_request(attempts), root.owner);
        drop(future);
        root.business.release();

        tokio::time::timeout(Duration::from_secs(1), async {
            while !activated.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("accepted request continues after its reply receiver is dropped");

        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert!(activated.load(Ordering::SeqCst));
        assert!(residual.load(Ordering::SeqCst));
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum AttemptPanicPhase {
        Activate,
        Run,
        Converge,
    }

    #[derive(Debug)]
    struct BindingNativePort;

    impl LogicalExecutionNativePort for BindingNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            Box::pin(async move { request.bind().map_err(Into::into) })
        }
    }

    #[derive(Debug, Default)]
    struct ClosedSuccessSealPort;

    impl super::super::AcceptedRootControlPort for ClosedSuccessSealPort {
        fn enqueue_root_control(
            &self,
            request: super::super::AcceptedRootControlRequest,
        ) -> Result<(), super::super::AcceptedRootControlRequest> {
            Err(request)
        }
    }

    #[derive(Debug)]
    struct RowsAttemptPreparationPort {
        relay_window: Option<TestWindow>,
        backend: BackendProcessId,
        converged: Arc<AtomicBool>,
    }

    impl TestRowsPreparationPort for RowsAttemptPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.relay_window = Some(TestWindow(window));
        }
    }

    impl NativeAttemptPreparationPort for RowsAttemptPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let owner = RowsDormantOwner {
                relay_window: self
                    .relay_window
                    .as_ref()
                    .expect("fixture window was admitted")
                    .clone(),
                backend: self.backend,
                converged: Arc::clone(&self.converged),
            };
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request.bind(scheduling, owner).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct RowsDormantOwner {
        relay_window: TestWindow,
        backend: BackendProcessId,
        converged: Arc<AtomicBool>,
    }

    impl DormantNativeAttemptOwner for RowsDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let converged = Arc::clone(&self.converged);
            let window = self.relay_window.clone();
            Box::pin(async move {
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                status_sender
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let binding = test_root_binding(root, window.0, None);
                Ok(ActivatedNativeAttempt::relayed_rows(
                    RowsActiveOwner {
                        converged,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.converged.store(true, Ordering::SeqCst);
            })
        }
    }

    struct RowsActiveOwner {
        converged: Arc<AtomicBool>,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for RowsActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("RowsActiveOwner")
        }
    }

    impl ActiveNativeAttemptOwner for RowsActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                let reason = cancellation.cancelled().await;
                NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                    fixture_cancellation_input(reason, "Rows test attempt was cancelled"),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.converged.store(true, Ordering::SeqCst);
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[derive(Default)]
    struct PreparationExitSignals {
        factory_exited: AtomicBool,
        convergence_entered: AtomicBool,
        convergence_completed: AtomicBool,
        reply_captured: AtomicBool,
        release_convergence: tokio::sync::Notify,
        trigger_failure: tokio::sync::Notify,
        reply: std::sync::Mutex<Option<crate::api::RetainedRootReply>>,
        transport: std::sync::Mutex<Option<novarocks_workload_control::ResultWindowAlias>>,
    }
    impl fmt::Debug for PreparationExitSignals {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("PreparationExitSignals")
        }
    }

    #[derive(Debug)]
    struct PreparationExitState {
        window: Option<TestWindow>,
        signals: Arc<PreparationExitSignals>,
    }
    impl Drop for PreparationExitState {
        fn drop(&mut self) {
            // Witness actual state/window destruction, not just wrapper destruction.
            drop(self.window.take());
            self.signals.factory_exited.store(true, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct PreparationExitPort {
        state: Arc<PreparationExitState>,
        backend: BackendProcessId,
    }
    impl TestRowsPreparationPort for PreparationExitPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            Arc::get_mut(&mut self.state)
                .expect("window admission precedes the first preparation future")
                .window = Some(TestWindow(window));
        }
    }
    impl NativeAttemptPreparationPort for PreparationExitPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            // Match the production Arc-owned preparation state: the future can also
            // retain the factory, independently of LogicalNativeSession.attempts.
            let state = Arc::clone(&self.state);
            let backend = self.backend;
            Box::pin(async move {
                let dormant = PreparationExitDormant {
                    inner: RowsDormantOwner {
                        relay_window: state.window.as_ref().expect("admitted window").clone(),
                        backend,
                        converged: Arc::new(AtomicBool::new(false)),
                    },
                    signals: Arc::clone(&state.signals),
                };
                let scheduling = no_scan_scheduling(&request);
                let prepared = request.bind(scheduling, dormant).map_err(Into::into);
                drop(state);
                prepared
            })
        }
    }

    #[derive(Debug)]
    struct PreparationExitDormant {
        inner: RowsDormantOwner,
        signals: Arc<PreparationExitSignals>,
    }
    impl DormantNativeAttemptOwner for PreparationExitDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let signals = Arc::clone(&self.signals);
            let activation = self.inner.activate(schedule, admissions, cancellation);
            Box::pin(async move {
                let activated = activation.await?;
                let (inner, runtime) = activated.into_parts();
                let mut runtime = runtime.expect("original Rows fixture carries its runtime");
                runtime.binding.port = Arc::new(PreparationExitReadPort {
                    signals: Arc::clone(&signals),
                });
                Ok(ActivatedNativeAttempt::relayed_rows(
                    PreparationExitActive { inner, signals },
                    runtime.binding,
                    runtime.statuses,
                ))
            })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.inner.converge(cancellation)
        }
    }

    #[derive(Debug)]
    struct PreparationExitActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        signals: Arc<PreparationExitSignals>,
    }
    impl ActiveNativeAttemptOwner for PreparationExitActive {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            let signals = Arc::clone(&self.signals);
            let run = self.inner.run(drive, cancellation);
            Box::pin(async move {
                tokio::select! {
                    terminal = run => terminal,
                    _ = signals.trigger_failure.notified() => {
                        NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(fixture_execution_input("held-convergence fixture execution failure")))
                    }
                }
            })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                let result = self.inner.converge(cancellation).await;
                signals.convergence_completed.store(true, Ordering::SeqCst);
                result
            })
        }
    }

    #[derive(Debug)]
    struct PreparationExitReadPort {
        signals: Arc<PreparationExitSignals>,
    }
    impl crate::api::BoundedRootReadPort for PreparationExitReadPort {
        fn read(
            &self,
            request: novarocks_execution_contract::root_result::RootResultRead,
            guard: novarocks_workload_control::ResultWindowAlias,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            crate::api::RetainedRootReply,
                            super::super::RootResultFetchFailure,
                        >,
                    > + Send,
            >,
        > {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                use novarocks_execution_contract::root_result::{
                    RootReadOutcome, RootResultData, RootResultReply,
                };
                // This reply is deliberately held by an independent decode owner.
                // It is never delivered as a synthetic valid MySQL row or ACKed.
                let reply = RootResultReply {
                    root_task: request.root_task(),
                    profile: request.profile(),
                    kind: request.kind(),
                    accepted_consumed: request.consumed(),
                    outcome: RootReadOutcome::Data(
                        RootResultData::try_new(
                            request.kind(),
                            std::num::NonZeroU64::MIN,
                            bytes::Bytes::from_static(b"\x00"),
                            None,
                        )
                        .expect("one nonempty byte is a bounded root data body"),
                    ),
                };
                let retained = crate::api::RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    guard,
                    64 * 1024,
                )
                .map_err(|error| fixture_root_reply_failure(request.root_task(), error))?;
                let transport = retained.retain_physical_guard();
                *signals.transport.lock().expect("fixture transport lock") = Some(transport);
                *signals.reply.lock().expect("fixture reply lock") = Some(retained);
                signals.reply_captured.store(true, Ordering::SeqCst);
                std::future::pending().await
            })
        }
        fn seal(
            &self,
            _: novarocks_execution_contract::root_lifetime::RootReadSealed,
        ) -> Result<(), QueryExecutionError> {
            Ok(())
        }
    }

    async fn wait_preparation_exit_fact(flag: &AtomicBool) -> Result<(), &'static str> {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !flag.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "fixture fact was not observed within its bound")
    }

    #[derive(Debug)]
    struct PreparationExitFacts {
        factory_exited: bool,
        convergence_still_held: bool,
        held_with_reply_and_transport: usize,
        held_after_reply_exit: usize,
        held_after_last_transport_exit: usize,
    }

    async fn terminal_preparation_exit_fixture(fail: bool) {
        let (control, root) = governance();
        let scope = root.owner.scope();
        let signals = Arc::new(PreparationExitSignals::default());
        let attempts = PreparationExitPort {
            state: Arc::new(PreparationExitState {
                window: None,
                signals: Arc::clone(&signals),
            }),
            backend: BackendProcessId::new_v7(),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x74),
            FrontendProcessId::new_v7(),
            supervisor_config().with_remote_cleanup_timeout(Duration::from_secs(10)),
        );
        let request = rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts);
        let capacity = scope
            .result_capacity()
            .expect("original scope result capacity");
        let original_client_positions = capacity.snapshot().held_positions[0];
        // Capture every negative observation without asserting while the actual
        // convergence owner is blocked. The outer cleanup always releases it.
        let observations: Result<PreparationExitFacts, &'static str> = async {
            let mut handle =
                tokio::time::timeout(Duration::from_secs(1), client.start(request, root.owner))
                    .await
                    .map_err(|_| "Rows handle handoff timed out")?
                    .map_err(|_| "Rows handle handoff failed")?;
            let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
                return Err("original request did not hand off Rows");
            };
            wait_preparation_exit_fact(&signals.reply_captured).await?;
            if fail {
                signals.trigger_failure.notify_one();
            } else {
                handle
                    .request_cancel()
                    .map_err(|_| "original cancel request failed")?;
            }
            let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .map_err(|_| "Rows terminal result timed out")?;
            if terminal.is_ok() {
                return Err("terminal Rows result did not preserve a typed error");
            }
            drop(stream);
            drop(handle);
            wait_preparation_exit_fact(&signals.convergence_entered).await?;
            let factory_exited = signals.factory_exited.load(Ordering::SeqCst);
            let held_with_reply_and_transport = capacity.snapshot().held_positions[0];
            let reply = signals
                .reply
                .lock()
                .map_err(|_| "fixture reply lock poisoned")?
                .take();
            drop(reply);
            let held_after_reply_exit = capacity.snapshot().held_positions[0];
            let transport = signals
                .transport
                .lock()
                .map_err(|_| "fixture transport lock poisoned")?
                .take();
            drop(transport);
            let held_after_last_transport_exit = capacity.snapshot().held_positions[0];
            let convergence_still_held = !signals.convergence_completed.load(Ordering::SeqCst);
            Ok(PreparationExitFacts {
                factory_exited,
                convergence_still_held,
                held_with_reply_and_transport,
                held_after_reply_exit,
                held_after_last_transport_exit,
            })
        }
        .await;

        // Preserve cleanup even when an observation failed. Poisoned fixture locks
        // retain their actual slots until explicitly taken; do not abandon owners.
        drop(
            signals
                .reply
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take(),
        );
        drop(
            signals
                .transport
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take(),
        );
        signals.release_convergence.notify_one();
        // Process shutdown may legitimately end optional remote tracking.
        // Observe this original fixture's convergence before requesting it.
        let convergence_exit = wait_preparation_exit_fact(&signals.convergence_completed).await;
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        // A timed-out handoff may have continued in the original supervisor.
        // Once shutdown joins it, drain slots again before checking scope release.
        drop(
            signals
                .reply
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take(),
        );
        drop(
            signals
                .transport
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take(),
        );
        acknowledge_all_control(&control);
        let settled = tokio::time::timeout(Duration::from_secs(2), scope.wait_released()).await;

        // All assertions happen after the original owner has been released/joined.
        assert!(
            shutdown.is_ok(),
            "original supervisor did not shut down: {shutdown:?}"
        );
        assert!(settled.is_ok(), "original scope did not settle");
        assert!(
            convergence_exit.is_ok(),
            "original convergence did not exit"
        );
        let facts = observations.expect("all bounded lifetime observations must complete");
        assert_eq!(original_client_positions, 1);
        assert!(
            facts.factory_exited,
            "terminal supervisor retained its preparation factory: {facts:?}"
        );
        assert!(
            facts.convergence_still_held,
            "fixture released remote convergence too early"
        );
        assert_eq!(facts.held_with_reply_and_transport, 1);
        assert_eq!(
            facts.held_after_reply_exit, 1,
            "actual transport alias must keep the window"
        );
        assert_eq!(
            facts.held_after_last_transport_exit, 0,
            "last actual alias must return capacity before remote convergence"
        );
        assert!(signals.convergence_completed.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_cancel_retires_preparation_before_held_convergence_but_preserves_reply_aliases()
     {
        terminal_preparation_exit_fixture(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_failure_retires_preparation_before_held_convergence_but_preserves_reply_aliases()
     {
        terminal_preparation_exit_fixture(true).await;
    }

    #[derive(Clone, Copy, Debug)]
    enum InitialTerminalPhase {
        ActivateFailure,
        ActivatePanic,
        CompletionSuccess,
        CompletionFailure,
        CompletionPanic,
    }

    #[derive(Debug)]
    struct InitialTerminalPreparationPort {
        state: Arc<PreparationExitState>,
        backend: BackendProcessId,
        frontend: FrontendProcessId,
        phase: InitialTerminalPhase,
    }
    impl NativeAttemptPreparationPort for InitialTerminalPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let state = Arc::clone(&self.state);
            let backend = self.backend;
            let frontend = self.frontend;
            let phase = self.phase;
            Box::pin(async move {
                let dormant = InitialTerminalDormant {
                    inner: SuccessfulCompletionDormantOwner {
                        backend,
                        expected_frontend: frontend,
                    },
                    // The actual attempt guard is independent of the factory alias.
                    guard: state
                        .window
                        .as_ref()
                        .expect("original admitted window")
                        .clone(),
                    signals: Arc::clone(&state.signals),
                    phase,
                };
                let scheduling = no_scan_scheduling(&request);
                let result = request.bind(scheduling, dormant).map_err(Into::into);
                drop(state);
                result
            })
        }
    }

    #[derive(Debug)]
    struct InitialTerminalDormant {
        inner: SuccessfulCompletionDormantOwner,
        guard: TestWindow,
        signals: Arc<PreparationExitSignals>,
        phase: InitialTerminalPhase,
    }
    impl DormantNativeAttemptOwner for InitialTerminalDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            match self.phase {
                InitialTerminalPhase::ActivateFailure => Box::pin(async {
                    Err(NativeAttemptActivationFailure::new(
                        NativeAttemptPreparationFailure::from_input(fixture_execution_input(
                            "initial terminal fixture activation failure",
                        )),
                    ))
                }),
                InitialTerminalPhase::ActivatePanic => {
                    Box::pin(async { panic!("initial terminal fixture activation panic") })
                }
                phase => {
                    let activation = self.inner.activate(schedule, admissions, cancellation);
                    let guard = self.guard.clone();
                    let signals = Arc::clone(&self.signals);
                    Box::pin(async move {
                        let activated = activation.await?;
                        let (inner, runtime) = activated.into_parts();
                        debug_assert!(runtime.is_none());
                        Ok(ActivatedNativeAttempt::completion(InitialTerminalActive {
                            inner,
                            _guard: guard,
                            signals,
                            phase,
                        }))
                    })
                }
            }
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                self.inner.converge(cancellation).await;
                signals.convergence_completed.store(true, Ordering::SeqCst);
            })
        }
    }

    #[derive(Debug)]
    struct InitialTerminalActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        _guard: TestWindow,
        signals: Arc<PreparationExitSignals>,
        phase: InitialTerminalPhase,
    }
    impl ActiveNativeAttemptOwner for InitialTerminalActive {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            match self.phase {
                // Delegate the existing exact Establish/admission fixture rather
                // than returning an unqualified synthetic Completed verdict.
                InitialTerminalPhase::CompletionSuccess => self.inner.run(drive, cancellation),
                InitialTerminalPhase::CompletionFailure => Box::pin(async {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        fixture_execution_input("initial terminal fixture run failure"),
                    ))
                }),
                InitialTerminalPhase::CompletionPanic => {
                    Box::pin(async { panic!("initial terminal fixture run panic") })
                }
                _ => unreachable!("activation failure has no active owner"),
            }
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                let result = self.inner.converge(cancellation).await;
                signals.convergence_completed.store(true, Ordering::SeqCst);
                result
            })
        }
    }

    async fn initial_terminal_preparation_fixture(phase: InitialTerminalPhase) {
        let (control, root) = governance();
        let scope = root.owner.scope();
        let capacity = scope
            .result_capacity()
            .expect("original scope result capacity");
        let window = capacity
            .try_acquire(
                &scope,
                novarocks_workload_control::ResultWindowClass::Internal,
            )
            .expect("original Internal window admission");
        let signals = Arc::new(PreparationExitSignals::default());
        let frontend = FrontendProcessId::new_v7();
        let attempts = InitialTerminalPreparationPort {
            state: Arc::new(PreparationExitState {
                window: Some(TestWindow(window.retain_alias())),
                signals: Arc::clone(&signals),
            }),
            backend: BackendProcessId::new_v7(),
            frontend,
            phase,
        };
        drop(window);
        let original_internal_positions = capacity.snapshot().held_positions[2];
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x78),
            frontend,
            supervisor_config().with_remote_cleanup_timeout(Duration::from_secs(10)),
        );
        // Capture failure observations without asserting while a real owner waits.
        let observations: Result<(bool, bool, usize), &'static str> = async {
            let reply = tokio::time::timeout(
                Duration::from_secs(1),
                client.start(completion_request(attempts), root.owner),
            )
            .await
            .map_err(|_| "initial terminal reply timed out")?;
            match phase {
                InitialTerminalPhase::CompletionSuccess => {
                    let mut handle = reply.map_err(|_| "exact Establish completion failed")?;
                    if !matches!(
                        handle.take_output(),
                        Some(crate::api::ExecutionOutput::Completion)
                    ) {
                        return Err("success did not preserve original Completion output");
                    }
                    drop(handle);
                }
                phase => {
                    let error = match reply {
                        Err(error) => error,
                        Ok(handle) => {
                            drop(handle);
                            return Err("terminal failure returned success");
                        }
                    };
                    let expected = match phase {
                        InitialTerminalPhase::ActivateFailure => {
                            "initial terminal fixture activation failure"
                        }
                        InitialTerminalPhase::ActivatePanic => "Native attempt activation panicked",
                        InitialTerminalPhase::CompletionFailure => {
                            "initial terminal fixture run failure"
                        }
                        InitialTerminalPhase::CompletionPanic => "Native attempt run panicked",
                        _ => unreachable!(),
                    };
                    if error.message() != expected {
                        return Err("original terminal failure source/order changed");
                    }
                }
            }
            wait_preparation_exit_fact(&signals.convergence_entered).await?;
            Ok((
                signals.factory_exited.load(Ordering::SeqCst),
                !signals.convergence_completed.load(Ordering::SeqCst),
                capacity.snapshot().held_positions[2],
            ))
        }
        .await;
        // Release and observe the same original convergence before shutdown can
        // legitimately end its optional remote tracking. Then settle actual scope.
        signals.release_convergence.notify_one();
        let convergence_exit = wait_preparation_exit_fact(&signals.convergence_completed).await;
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        acknowledge_all_control(&control);
        let settled = tokio::time::timeout(Duration::from_secs(2), scope.wait_released()).await;
        assert!(
            convergence_exit.is_ok(),
            "original convergence did not exit: {phase:?}"
        );
        let joined_with_expected_outcome = match (&shutdown, phase) {
            (
                Ok(()),
                InitialTerminalPhase::ActivateFailure
                | InitialTerminalPhase::CompletionSuccess
                | InitialTerminalPhase::CompletionFailure,
            ) => true,
            (
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)),
                InitialTerminalPhase::ActivatePanic,
            ) => error.message() == "Native attempt activation panicked",
            (
                Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)),
                InitialTerminalPhase::CompletionPanic,
            ) => error.message() == "Native attempt run panicked",
            _ => false,
        };
        assert!(
            joined_with_expected_outcome,
            "original supervisor owner did not join with its expected outcome: {phase:?} {shutdown:?}"
        );
        assert!(settled.is_ok(), "original scope did not settle: {phase:?}");
        let (factory_exited, convergence_held, held_positions) =
            observations.expect("bounded terminal facts");
        assert_eq!(original_internal_positions, 1);
        assert!(
            factory_exited,
            "terminal caller retained preparation state: {phase:?}"
        );
        assert!(
            convergence_held,
            "original convergence released before observation: {phase:?}"
        );
        // The real Dormant/Active guard must remain live even after the unused
        // factory drops. Zero here would be a false release, not a desired result.
        assert_eq!(
            held_positions, 1,
            "original attempt guard lost its window: {phase:?}"
        );
        assert_eq!(capacity.snapshot().held_positions[2], 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn initial_activation_failure_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::ActivateFailure).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn initial_activation_panic_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::ActivatePanic).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn completion_success_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionSuccess).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn completion_failure_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionFailure).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn completion_panic_retires_only_unused_preparation_before_convergence() {
        initial_terminal_preparation_fixture(InitialTerminalPhase::CompletionPanic).await;
    }

    #[tokio::test]
    async fn rows_handle_is_handed_off_before_the_attempt_converges() {
        let (control, root) = governance();
        let scope = root.owner.scope();
        let converged = Arc::new(AtomicBool::new(false));
        let attempts = RowsAttemptPreparationPort {
            relay_window: None,
            backend: BackendProcessId::new_v7(),
            converged: Arc::clone(&converged),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x52),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let mut handle = tokio::time::timeout(
            Duration::from_secs(1),
            client.start(
                rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts),
                root.owner,
            ),
        )
        .await
        .expect("Rows execution must hand off its bounded result consumer")
        .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("Rows execution must return its result stream");
        };
        assert!(!converged.load(Ordering::SeqCst));
        handle.request_cancel().unwrap();
        let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("scope cancellation must terminate the result stream")
        else {
            panic!("cancelled Rows stream must end with a typed error");
        };
        assert!(!error.message().is_empty());
        drop(stream);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert!(converged.load(Ordering::SeqCst));
    }

    #[derive(Debug)]
    struct ImmediateReplacementPort {
        capacity: watch::Sender<u64>,
    }

    impl Default for ImmediateReplacementPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(1);
            Self { capacity }
        }
    }

    #[derive(Debug)]
    struct ImmediateReplacementEffectReservation;

    impl super::super::ReplacementQualificationEffectReservation
        for ImmediateReplacementEffectReservation
    {
        fn submit(
            self: Box<Self>,
            submission: super::super::ReplacementQualificationEffectSubmission,
        ) {
            let request = submission.request();
            let admissions: Box<[_]> = request
                .replacement_contexts()
                .iter()
                .enumerate()
                .map(|(index, context)| {
                    let tag = u8::try_from(index + 1).unwrap();
                    let valid_for = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
                    let admission_request =
                        novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
                            TaskOperationId::new_v7(),
                            *context,
                            valid_for,
                            NativeCompatibilityId::new([tag; 32]),
                            AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
                        );
                    let ticket = QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
                        *context,
                        valid_for,
                    );
                    super::super::QualifiedWorkerAdmission::try_new(
                        request.identity().replacement(),
                        admission_request,
                        ticket,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            let reservation = ImmediateIsolationReservation {
                identity: request.identity(),
                failed_contexts: request.failed_contexts().iter().copied().collect(),
                replacement_contexts: request.replacement_contexts().iter().copied().collect(),
                admissions,
            };
            let qualified = super::super::QualifiedReplacementReservation::try_new(
                request,
                Box::new(reservation),
            )
            .unwrap();
            submission.qualified(qualified).unwrap();
        }
    }

    impl super::super::ReplacementQualificationEffectPort for ImmediateReplacementPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _request: &super::super::ReplacementQualificationRequest,
        ) -> super::super::ReplacementQualificationEffectAdmission {
            super::super::ReplacementQualificationEffectAdmission::Admitted(Box::new(
                ImmediateReplacementEffectReservation,
            ))
        }
    }

    #[derive(Debug)]
    struct ImmediateIsolationReservation {
        identity: super::super::ReplacementQualificationIdentity,
        failed_contexts: BTreeSet<QueryContextRef>,
        replacement_contexts: BTreeSet<QueryContextRef>,
        admissions: Box<[super::super::QualifiedWorkerAdmission]>,
    }

    impl super::super::AttemptIsolationReservation for ImmediateIsolationReservation {
        fn identity(&self) -> super::super::ReplacementQualificationIdentity {
            self.identity
        }

        fn topology_revision(&self) -> u64 {
            1
        }

        fn admissions(&self) -> &[super::super::QualifiedWorkerAdmission] {
            &self.admissions
        }

        fn validate_binding(
            &self,
            failed_contexts: &[QueryContextRef],
        ) -> Result<(), super::super::ReplacementQualificationFailure> {
            let failed: BTreeSet<_> = failed_contexts.iter().copied().collect();
            let replacements: BTreeSet<_> = self
                .admissions
                .iter()
                .map(super::super::QualifiedWorkerAdmission::context)
                .collect();
            (failed == self.failed_contexts && replacements == self.replacement_contexts)
                .then_some(())
                .ok_or(super::super::ReplacementQualificationFailure::InvalidReservation)
        }

        fn activate(
            self: Box<Self>,
        ) -> Result<
            Box<dyn super::super::ActiveAttemptIsolationOwner>,
            super::super::AttemptIsolationActivationFailure,
        > {
            Ok(Box::new(ImmediateActiveIsolation {
                execution: self.identity.replacement(),
            }))
        }

        fn abandon(self: Box<Self>) {}
    }

    #[derive(Debug)]
    struct ImmediateActiveIsolation {
        execution: QueryExecutionId,
    }

    impl super::super::ActiveAttemptIsolationOwner for ImmediateActiveIsolation {
        fn execution(&self) -> QueryExecutionId {
            self.execution
        }

        fn finish(self: Box<Self>) {}

        fn abandon(self: Box<Self>) {}
    }

    fn fixture_recovery_backends(processes: usize) -> Box<[BackendProcessId]> {
        (0..processes).map(|_| BackendProcessId::new_v7()).collect()
    }

    #[derive(Debug)]
    struct RecoveringRowsPreparationPort {
        relay_window: Option<TestWindow>,
        backends: Box<[BackendProcessId]>,
        prepares: Arc<AtomicU64>,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        replacement_missing_rows: bool,
        failures_before_success: u64,
    }

    impl TestRowsPreparationPort for RecoveringRowsPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.relay_window = Some(TestWindow(window));
        }
    }

    impl NativeAttemptPreparationPort for RecoveringRowsPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let ordinal = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
            // Every process identity was created with this fixture, before
            // any BackendLost input. Previously failed processes are not reused.
            let requirement = request.topology_requirement();
            let backend = self
                .backends
                .iter()
                .copied()
                .skip(usize::try_from(ordinal - 1).unwrap())
                .find(|process| match requirement {
                    crate::api::NativeAttemptTopologyRequirement::LiveSnapshot => true,
                    crate::api::NativeAttemptTopologyRequirement::ExcludeProcess(excluded) => {
                        *process != excluded
                    }
                })
                .expect("fixture has enough original nonexcluded process identities");
            if let crate::api::NativeAttemptTopologyRequirement::ExcludeProcess(excluded) =
                requirement
            {
                assert_ne!(backend, excluded);
            }
            let owner = RecoveringRowsDormantOwner {
                relay_window: self
                    .relay_window
                    .as_ref()
                    .expect("fixture window was admitted")
                    .clone(),
                backend,
                ordinal,
                runs: Arc::clone(&self.runs),
                convergences: Arc::clone(&self.convergences),
                allow_initial_convergence: Arc::clone(&self.allow_initial_convergence),
                block_initial_convergence: self.block_initial_convergence,
                replacement_missing_rows: self.replacement_missing_rows,
                failures_before_success: self.failures_before_success,
            };
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request.bind(scheduling, owner).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct RecoveringRowsDormantOwner {
        relay_window: TestWindow,
        backend: BackendProcessId,
        ordinal: u64,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        replacement_missing_rows: bool,
        failures_before_success: u64,
    }

    impl DormantNativeAttemptOwner for RecoveringRowsDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let context = schedule.contexts()[0];
            let ordinal = self.ordinal;
            let runs = Arc::clone(&self.runs);
            let convergences = Arc::clone(&self.convergences);
            let allow_initial_convergence = Arc::clone(&self.allow_initial_convergence);
            let block_initial_convergence = self.block_initial_convergence;
            let replacement_missing_rows = self.replacement_missing_rows;
            let failures_before_success = self.failures_before_success;
            let window = self.relay_window.clone();
            Box::pin(async move {
                if replacement_missing_rows && ordinal > 1 {
                    return Ok(ActivatedNativeAttempt::completion(PanickingActiveOwner {
                        phase: AttemptPanicPhase::Run,
                        convergence_calls: convergences,
                    }));
                }
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                status_sender
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let binding = test_root_binding(
                    root,
                    window.0,
                    Some(&[
                        14, 0, 0, 0, 1, b'2', 11, b'r', b'e', b'p', b'l', b'a', b'c', b'e', b'm',
                        b'e', b'n', b't',
                    ]),
                );
                Ok(ActivatedNativeAttempt::relayed_rows(
                    RecoveringRowsActiveOwner {
                        ordinal,
                        failures_before_success,
                        context,
                        runs,
                        convergences,
                        allow_initial_convergence,
                        block_initial_convergence,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.convergences.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    struct RecoveringRowsActiveOwner {
        ordinal: u64,
        failures_before_success: u64,
        context: QueryContextRef,
        runs: Arc<AtomicU64>,
        convergences: Arc<AtomicU64>,
        allow_initial_convergence: Arc<tokio::sync::Notify>,
        block_initial_convergence: bool,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for RecoveringRowsActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RecoveringRowsActiveOwner")
                .field("ordinal", &self.ordinal)
                .finish_non_exhaustive()
        }
    }

    impl ActiveNativeAttemptOwner for RecoveringRowsActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                self.runs.fetch_add(1, Ordering::SeqCst);
                if self.ordinal <= self.failures_before_success {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        ExecutionFailureInput::new(
                            QueryFailureReason::BackendLost.failure_fact(),
                            SafeDetail::truncating(
                                "injected pre-visibility infrastructure failure",
                            ),
                            ExecutionFailureSite::Backend {
                                process: self.context.backend_process_id(),
                            },
                        ),
                    ))
                } else {
                    let reason = cancellation.cancelled().await;
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        fixture_cancellation_input(reason, "replacement attempt was cancelled"),
                    ))
                }
            })
        }

        fn converge<'a>(
            &'a mut self,
            request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                if self.ordinal == 1 && self.block_initial_convergence {
                    // This fixture has no local close work: the held fact is
                    // only its original remote observation. Shutdown can end
                    // that wait without fabricating WorkerStopped evidence.
                    tokio::select! {
                        biased;
                        _ = self.allow_initial_convergence.notified() => {}
                        _ = tokio::time::sleep_until(request.remote_observation_deadline().into()) => {
                            return Ok(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                                exact_contexts: vec![self.context].into_boxed_slice(),
                            });
                        }
                    }
                }
                self.convergences.fetch_add(1, Ordering::SeqCst);
                Ok(NativeActiveConvergenceClosure::Evidence(
                    if self.ordinal == 1 {
                        NativeAttemptConvergence::contexts(vec![
                            crate::api::NativeContextConvergence::worker_process_replaced(
                                self.context,
                            ),
                        ])
                    } else {
                        NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced()
                    },
                ))
            })
        }
    }

    #[derive(Debug)]
    struct HeldReplacementPreparationPort {
        inner: RecoveringRowsPreparationPort,
        release: Arc<tokio::sync::Notify>,
        exits: Arc<AtomicU64>,
        entered: Arc<AtomicU64>,
    }

    impl TestRowsPreparationPort for HeldReplacementPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.inner.bind_window(window);
        }
    }

    impl NativeAttemptPreparationPort for HeldReplacementPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let replacement = request.execution().attempt_id().get() > 1;
            let prepared = self.inner.prepare(request);
            let release = Arc::clone(&self.release);
            let exits = Arc::clone(&self.exits);
            let entered = Arc::clone(&self.entered);
            Box::pin(async move {
                struct PreparationExit(Arc<AtomicU64>);
                impl Drop for PreparationExit {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::SeqCst);
                    }
                }
                if replacement {
                    let _exit = PreparationExit(exits);
                    entered.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    prepared.await
                } else {
                    prepared.await
                }
            })
        }
    }

    async fn exercise_held_replacement_preparation(
        cancel_reason: Option<CancellationReason>,
        release: bool,
    ) {
        let cancel = cancel_reason.is_some();
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let exits = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(tokio::sync::Notify::new());
        let attempts = HeldReplacementPreparationPort {
            inner: RecoveringRowsPreparationPort {
                relay_window: None,
                backends: fixture_recovery_backends(2),
                prepares: Arc::clone(&prepares),
                runs: Arc::clone(&runs),
                convergences: Arc::clone(&convergences),
                allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                block_initial_convergence: false,
                replacement_missing_rows: false,
                failures_before_success: 1,
            },
            release: Arc::clone(&gate),
            exits: Arc::clone(&exits),
            entered: Arc::clone(&entered),
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let requester = root.owner.cancellation_requester();
        let mut config = supervisor_config();
        config.rows.replacement_reservation_valid_for = if cancel || release {
            Duration::from_secs(30)
        } else {
            Duration::from_millis(50)
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x59),
            FrontendProcessId::new_v7(),
            config,
        );
        let mut handle = client
            .start(
                rows_request(
                    &control,
                    &scope,
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must retain its result consumer");
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while entered.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the failed attempt must enter replacement preparation");
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        stream.begin_schema().unwrap().complete();
        if release {
            gate.notify_one();
            let crate::api::ResultDelivery::Segment(delivery) =
                tokio::time::timeout(Duration::from_secs(1), stream.next())
                    .await
                    .expect("replacement released within the budget must deliver")
                    .unwrap()
                    .unwrap()
            else {
                panic!("replacement must deliver its actual result batch");
            };
            delivery.complete();
            assert_eq!(runs.load(Ordering::SeqCst), 2);
        }
        if let Some(reason) = cancel_reason.clone() {
            requester.request(reason.clone()).unwrap();
            requester.request(CancellationReason::Requested).unwrap();
            assert_eq!(scope.cancellation().unwrap().reason(), Some(reason));
        } else if release {
            handle.request_cancel().unwrap();
        }
        let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("bounded preparation or cancellation must terminate the public stream");
        let Err(error) = terminal else {
            panic!("the stream must expose the failed logical execution");
        };
        if cancel || release {
            let expected = match cancel_reason.as_ref() {
                Some(
                    CancellationReason::DeadlineExceeded
                    | CancellationReason::FrontendDrainDeadlineExceeded,
                ) => QueryFailureReason::DeadlineExceeded,
                _ => QueryFailureReason::Cancelled,
            };
            let input = error
                .execution_failure()
                .expect("public cancellation retains canonical input");
            assert_eq!(input.fact(), expected.failure_fact());
            assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
            assert_eq!(
                error.kind(),
                if matches!(expected, QueryFailureReason::DeadlineExceeded) {
                    QueryExecutionErrorKind::DeadlineExceeded
                } else {
                    QueryExecutionErrorKind::Cancelled
                },
            );
            if matches!(expected, QueryFailureReason::Cancelled) {
                assert_eq!(
                    error.to_string(),
                    "logical execution was cancelled before success EOF"
                );
            }
        } else {
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            assert_eq!(
                error.to_string(),
                "injected pre-visibility infrastructure failure"
            );
            let original = error
                .execution_failure()
                .expect("the originating attempt failure remains canonical");
            assert_eq!(
                original.fact(),
                QueryFailureReason::BackendLost.failure_fact()
            );
            assert!(matches!(
                original.site(),
                ExecutionFailureSite::Backend { .. }
            ));
        }
        if !release {
            assert_eq!(
                runs.load(Ordering::SeqCst),
                1,
                "a held prepare cannot activate"
            );
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while exits.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the pending preparation must really exit before shutdown is requested");
        drop(stream);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        if release {
            shutdown.unwrap();
        } else if cancel {
            shutdown.expect("accepted query cancellation is not a supervisor health failure");
        } else {
            let Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)) = shutdown
            else {
                panic!("the supervisor must retain the precise preparation failure");
            };
            assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
            assert_eq!(
                error.to_string(),
                "Native replacement preparation exceeded its recovery budget"
            );
            assert_recovery_preparation_budget_input(&error);
        }
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert_eq!(
            convergences.load(Ordering::SeqCst),
            if release { 2 } else { 1 }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_recovery_does_not_refresh_the_preparation_deadline() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let exits = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(tokio::sync::Notify::new());
        let attempts = HeldReplacementPreparationPort {
            inner: RecoveringRowsPreparationPort {
                relay_window: None,
                backends: fixture_recovery_backends(3),
                prepares: Arc::clone(&prepares),
                runs: Arc::clone(&runs),
                convergences: Arc::clone(&convergences),
                allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
                block_initial_convergence: false,
                replacement_missing_rows: false,
                failures_before_success: 2,
            },
            release: Arc::clone(&gate),
            exits: Arc::clone(&exits),
            entered: Arc::clone(&entered),
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let mut config = supervisor_config();
        config.rows.max_attempts = NonZeroU32::new(3).unwrap();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x5a),
            FrontendProcessId::new_v7(),
            config,
        );
        let mut handle = client
            .start(
                rows_request(
                    &control,
                    &scope,
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recovery must retain the public stream");
        };
        stream.begin_schema().unwrap().complete();
        while entered.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(20)).await;
        gate.notify_one();
        while entered.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while exits.load(Ordering::SeqCst) != 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the held third prepare must exit at the original deadline");
        let terminal = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("the third prepare must expire at the first recovery deadline");
        let Err(error) = terminal else {
            panic!("the expired replacement must retain the originating failure");
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            error.to_string(),
            "injected pre-visibility infrastructure failure"
        );
        assert_eq!(exits.load(Ordering::SeqCst), 2);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "expired third prepare cannot activate"
        );
        drop(stream);
        root.business.release();
        // shutdown_until converts the supplied std Instant back into Tokio
        // time. Use the advanced clock so this is a future deadline, rather
        // than the wall clock's already-past instant in this paused test.
        let shutdown_deadline = tokio::time::Instant::now().into_std() + Duration::from_secs(1);
        let shutdown = supervisor.shutdown_until(shutdown_deadline).await;
        let Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(error)) = shutdown else {
            panic!("the preparation budget failure must survive owned shutdown: {shutdown:?}");
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            error.message(),
            "Native replacement preparation exceeded its recovery budget"
        );
        assert_recovery_preparation_budget_input(&error);
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergences.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn replacement_preparation_budget_expires_and_converges_the_original_owner() {
        exercise_held_replacement_preparation(None, false).await;
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_replacement_preparation() {
        exercise_held_replacement_preparation(Some(CancellationReason::Requested), false).await;
    }

    #[tokio::test]
    async fn accepted_deadline_interrupts_pending_replacement_preparation() {
        exercise_held_replacement_preparation(Some(CancellationReason::DeadlineExceeded), false)
            .await;
    }

    fn assert_recovery_preparation_budget_input(error: &QueryExecutionError) {
        let input = error
            .execution_failure()
            .expect("recovery budget retains its own canonical input");
        assert_eq!(
            input.fact(),
            QueryFailureReason::RecoveryPreparationBudgetExceeded.failure_fact()
        );
        assert_eq!(
            input.fact().class(),
            novarocks_execution_contract::failure::FailureClass::Dependency
        );
        assert_eq!(input.fact().reason().code(), 151);
        assert_eq!(input.fact().resource(), None);
        assert_eq!(input.fact().representation(), None);
        assert_eq!(input.fact().row_position(), None);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            input.topology_requirement(),
            crate::api::NativeAttemptTopologyRequirement::LiveSnapshot
        );
    }

    #[tokio::test]
    async fn replacement_preparation_released_within_budget_can_activate() {
        exercise_held_replacement_preparation(None, true).await;
    }

    #[tokio::test]
    async fn recoverable_pre_visibility_failure_activates_a_rows_replacement() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let allow_initial_convergence = Arc::new(tokio::sync::Notify::new());
        let attempts = RecoveringRowsPreparationPort {
            relay_window: None,
            backends: fixture_recovery_backends(2),
            prepares: Arc::clone(&prepares),
            runs: Arc::clone(&runs),
            convergences: Arc::clone(&convergences),
            allow_initial_convergence: Arc::clone(&allow_initial_convergence),
            block_initial_convergence: true,
            replacement_missing_rows: false,
            failures_before_success: 1,
        };
        let replacements = Arc::new(ImmediateReplacementPort::default());
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x54),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let mut handle = client
            .start(
                rows_request(
                    &control,
                    &scope,
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(replacements),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must keep one result consumer");
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while runs.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successor activation must not wait for old attempt convergence");
        assert_eq!(prepares.load(Ordering::SeqCst), 2);
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(convergences.load(Ordering::SeqCst), 0);
        stream
            .begin_schema()
            .expect("replacement Rows execution must retain the frozen schema")
            .complete();
        let crate::api::ResultDelivery::Segment(delivery) =
            tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("replacement must deliver while old convergence is blocked")
                .unwrap()
                .unwrap()
        else {
            panic!("replacement must deliver a result batch");
        };
        delivery.complete();
        assert_eq!(convergences.load(Ordering::SeqCst), 0);
        allow_initial_convergence.notify_one();
        handle.request_cancel().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("replacement cancellation must terminate the result stream");
        drop(stream);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergences.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shutdown_finishes_remote_tracking_for_a_lost_residual_attempt() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let allow_initial_convergence = Arc::new(tokio::sync::Notify::new());
        let attempts = RecoveringRowsPreparationPort {
            relay_window: None,
            backends: fixture_recovery_backends(2),
            prepares,
            runs: Arc::clone(&runs),
            convergences: Arc::clone(&convergences),
            allow_initial_convergence: Arc::clone(&allow_initial_convergence),
            block_initial_convergence: true,
            replacement_missing_rows: false,
            failures_before_success: 1,
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x55),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let mut handle = client
            .start(
                rows_request(
                    &control,
                    &scope,
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must keep one result consumer");
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while runs.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successor activation must not wait for the lost residual attempt");
        stream.begin_schema().unwrap().complete();
        let crate::api::ResultDelivery::Segment(delivery) =
            tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("successor result must remain available")
                .unwrap()
                .unwrap()
        else {
            panic!("successor must deliver a result batch");
        };
        delivery.complete();

        handle.request_cancel().unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("successor cancellation must terminate the result stream");
        drop(stream);
        root.business.release();

        supervisor
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect("shutdown must end FE tracking without waiting for a lost residual attempt");
        assert_eq!(convergences.load(Ordering::SeqCst), 1);
        drop(supervisor);

        // A later worker fact cannot revive the retired FE observation.
        allow_initial_convergence.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                acknowledge_all_control(&control);
                tokio::select! {
                    () = scope.wait_released() => break,
                    () = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            }
        })
        .await
        .expect("detached logical work must release after the late residual fact");
    }

    #[tokio::test]
    async fn replacement_activation_rejects_a_missing_rows_runtime() {
        let prepares = Arc::new(AtomicU64::new(0));
        let runs = Arc::new(AtomicU64::new(0));
        let convergences = Arc::new(AtomicU64::new(0));
        let attempts = RecoveringRowsPreparationPort {
            relay_window: None,
            backends: fixture_recovery_backends(2),
            prepares: Arc::clone(&prepares),
            runs,
            convergences: Arc::clone(&convergences),
            allow_initial_convergence: Arc::new(tokio::sync::Notify::new()),
            block_initial_convergence: false,
            replacement_missing_rows: true,
            failures_before_success: 1,
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x55),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let mut handle = client
            .start(
                rows_request(
                    &control,
                    &scope,
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("recoverable Rows execution must keep one result consumer");
        };
        let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("invalid replacement runtime must terminate the stream")
        else {
            panic!("invalid replacement runtime must fail the stream");
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        let input = error
            .execution_failure()
            .expect("the output guard authors an execution contract fact");
        assert_eq!(
            input.fact(),
            QueryFailureReason::ResultProtocol.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            input.user_error().phase(),
            novarocks_user_error::ErrorPhase::Execution
        );
        assert_eq!(input.user_error().location(), None);
        assert_eq!(prepares.load(Ordering::SeqCst), 2);
        drop(stream);
        root.business.release();
        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergences.load(Ordering::SeqCst), 2);
    }

    #[derive(Debug)]
    struct VisibleFailurePreparationPort {
        relay_window: Option<TestWindow>,
        backend: BackendProcessId,
        prepares: Arc<AtomicU64>,
        fail: Arc<tokio::sync::Notify>,
    }

    impl TestRowsPreparationPort for VisibleFailurePreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            self.relay_window = Some(TestWindow(window));
        }
    }

    impl NativeAttemptPreparationPort for VisibleFailurePreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            self.prepares.fetch_add(1, Ordering::SeqCst);
            let owner = VisibleFailureDormantOwner {
                relay_window: self
                    .relay_window
                    .as_ref()
                    .expect("fixture window was admitted")
                    .clone(),
                backend: self.backend,
                fail: Arc::clone(&self.fail),
            };
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request.bind(scheduling, owner).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct VisibleFailureDormantOwner {
        relay_window: TestWindow,
        backend: BackendProcessId,
        fail: Arc<tokio::sync::Notify>,
    }

    impl DormantNativeAttemptOwner for VisibleFailureDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let root = schedule.root();
            let fail = Arc::clone(&self.fail);
            let window = self.relay_window.clone();
            Box::pin(async move {
                let (status_sender, statuses) =
                    super::super::accepted_root_status_projection_with_control_port(
                        root,
                        Arc::new(ClosedSuccessSealPort),
                    );
                status_sender
                    .publish_attempt_observation(
                        running_status(root),
                        super::super::AcceptedAttemptFailure::None,
                    )
                    .unwrap();
                let binding = test_root_binding(
                    root,
                    window.0,
                    Some(&[
                        10, 0, 0, 0, 1, b'1', 7, b'v', b'i', b's', b'i', b'b', b'l', b'e',
                    ]),
                );
                Ok(ActivatedNativeAttempt::relayed_rows(
                    VisibleFailureActiveOwner {
                        backend: root.backend_process_id(),
                        fail,
                        _status_sender: status_sender,
                    },
                    binding,
                    statuses,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    struct VisibleFailureActiveOwner {
        backend: BackendProcessId,
        fail: Arc<tokio::sync::Notify>,
        _status_sender: super::super::AcceptedRootStatusSender,
    }

    impl fmt::Debug for VisibleFailureActiveOwner {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("VisibleFailureActiveOwner")
        }
    }

    impl ActiveNativeAttemptOwner for VisibleFailureActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                self.fail.notified().await;
                NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                    ExecutionFailureInput::new(
                        QueryFailureReason::BackendLost.failure_fact(),
                        SafeDetail::truncating("injected failure after visible output"),
                        ExecutionFailureSite::Backend {
                            process: self.backend,
                        },
                    ),
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn visible_rows_failure_does_not_prepare_a_replacement() {
        let prepares = Arc::new(AtomicU64::new(0));
        let fail = Arc::new(tokio::sync::Notify::new());
        let attempts = VisibleFailurePreparationPort {
            relay_window: None,
            backend: BackendProcessId::new_v7(),
            prepares: Arc::clone(&prepares),
            fail: Arc::clone(&fail),
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x56),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let mut handle = client
            .start(
                rows_request(
                    &control,
                    &scope,
                    RecoveryMode::RestartAttemptBeforeVisibility,
                    Some(Arc::new(ImmediateReplacementPort::default())),
                    attempts,
                ),
                root.owner,
            )
            .await
            .unwrap();
        let Some(crate::api::ExecutionOutput::Rows(mut stream)) = handle.take_output() else {
            panic!("Rows execution must return one result stream");
        };
        stream
            .begin_schema()
            .expect("Rows execution must begin with its frozen result schema")
            .complete();
        let crate::api::ResultDelivery::Segment(delivery) = stream.next().await.unwrap().unwrap()
        else {
            panic!("the first delivery must be a result batch");
        };
        delivery.complete();
        fail.notify_one();
        let Err(error) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("visible failure must terminate the stream")
        else {
            panic!("visible failure must be caller-visible");
        };
        assert_eq!(error.message(), "injected failure after visible output");
        assert_eq!(prepares.load(Ordering::SeqCst), 1);
        drop(stream);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[derive(Debug)]
    struct PanickingAttemptPreparationPort {
        backend: BackendProcessId,
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl TestRowsPreparationPort for PanickingAttemptPreparationPort {
        fn bind_window(&mut self, window: novarocks_workload_control::ResultWindowAlias) {
            drop(window);
        }
    }

    impl NativeAttemptPreparationPort for PanickingAttemptPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            let owner = PanickingDormantOwner {
                backend,
                phase: self.phase,
                convergence_calls: Arc::clone(&self.convergence_calls),
            };
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request.bind(scheduling, owner).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PanickingDormantOwner {
        backend: BackendProcessId,
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl DormantNativeAttemptOwner for PanickingDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            match self.phase {
                AttemptPanicPhase::Activate => {
                    Box::pin(async { panic!("injected Native activation panic") })
                }
                AttemptPanicPhase::Run | AttemptPanicPhase::Converge => {
                    let convergence_calls = Arc::clone(&self.convergence_calls);
                    let phase = self.phase;
                    Box::pin(async move {
                        Ok(ActivatedNativeAttempt::completion(PanickingActiveOwner {
                            phase,
                            convergence_calls,
                        }))
                    })
                }
            }
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.convergence_calls.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    #[derive(Debug)]
    struct PanickingActiveOwner {
        phase: AttemptPanicPhase,
        convergence_calls: Arc<AtomicU64>,
    }

    impl ActiveNativeAttemptOwner for PanickingActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            match self.phase {
                AttemptPanicPhase::Run => Box::pin(async { panic!("injected Native run panic") }),
                AttemptPanicPhase::Converge => Box::pin(async {
                    NativeAttemptTerminal::Failed(NativeAttemptPreparationFailure::from_input(
                        fixture_execution_input("injected Native attempt failure"),
                    ))
                }),
                AttemptPanicPhase::Activate => unreachable!("activation panic has no active owner"),
            }
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                let previous = self.convergence_calls.fetch_add(1, Ordering::SeqCst);
                if self.phase == AttemptPanicPhase::Converge && previous == 0 {
                    panic!("injected Native convergence panic");
                }
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    async fn assert_attempt_panic_converges(
        phase: AttemptPanicPhase,
        namespace: u64,
        expected_message: &'static str,
    ) {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let attempts = PanickingAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            phase,
            convergence_calls: Arc::clone(&convergence_calls),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(namespace),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(attempts), root.owner).await {
            Ok(_) => panic!("a panicking Native attempt must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.message(), expected_message);
        let input = error
            .execution_failure()
            .expect("real Native future panic retains its input");
        assert_eq!(
            input.fact(),
            QueryFailureReason::NativeFuturePanicked.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert_eq!(convergence_calls.load(Ordering::SeqCst), 1);
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn activation_future_panic_retains_owner_for_convergence() {
        assert_attempt_panic_converges(
            AttemptPanicPhase::Activate,
            0x4b,
            "Native attempt activation panicked",
        )
        .await;
    }

    #[tokio::test]
    async fn run_future_panic_retains_owner_for_convergence() {
        assert_attempt_panic_converges(AttemptPanicPhase::Run, 0x4c, "Native attempt run panicked")
            .await;
    }

    #[tokio::test]
    async fn rows_activation_rejects_a_missing_root_result_runtime() {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let attempts = PanickingAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            phase: AttemptPanicPhase::Run,
            convergence_calls: Arc::clone(&convergence_calls),
        };
        let (control, root) = governance();
        let scope = root.owner.scope();
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x53),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let error = match client
            .start(
                rows_request(&control, &scope, RecoveryMode::NoRecovery, None, attempts),
                root.owner,
            )
            .await
        {
            Ok(_) => panic!("Rows activation must transfer its exact pump runtime"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        let input = error
            .execution_failure()
            .expect("the output guard authors an execution contract fact");
        assert_eq!(
            input.fact(),
            QueryFailureReason::ResultProtocol.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            input.user_error().phase(),
            novarocks_user_error::ErrorPhase::Execution
        );
        assert_eq!(input.user_error().location(), None);
        assert_eq!(
            error.message(),
            "Native activated a row execution without its root result runtime"
        );
        root.business.release();
        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(convergence_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn convergence_poll_panic_retains_same_owner_and_health_until_process_exit() {
        let convergence_calls = Arc::new(AtomicU64::new(0));
        let attempts = PanickingAttemptPreparationPort {
            backend: BackendProcessId::new_v7(),
            phase: AttemptPanicPhase::Converge,
            convergence_calls: Arc::clone(&convergence_calls),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x50),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();

        let error = match client.start(completion_request(attempts), root.owner).await {
            Ok(_) => panic!("a failed Native attempt must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.message(), "injected Native attempt failure");
        let input = error
            .execution_failure()
            .expect("original injected failure is preserved");
        assert_eq!(
            input.fact(),
            FixtureFailureReason::InjectedExecution.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(input.attempt_class(), AttemptFailureClass::ExecutionFailure);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::StatementNativeUnresolved(_))
        ));
        assert_eq!(
            convergence_calls.load(Ordering::SeqCst),
            1,
            "unproven convergence is never re-polled into fabricated closure"
        );
        assert_eq!(supervisor.registry.active_native_custody_count(), 1);
        assert_eq!(control.snapshot().root_responsibilities, 1);
        assert!(
            scope.cancellation().is_ok(),
            "original actor scope remains present before actual exit"
        );
        supervisor.abandon_for_process_exit();
    }

    #[derive(Debug)]
    struct TestContent(u8);

    impl CodecOwnedContent for TestContent {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([self.0; 16])
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    #[derive(Debug)]
    struct TestSecret;

    impl ConfidentialContent for TestSecret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }

    #[derive(Debug)]
    struct ImmediateEstablishTransport;

    impl EstablishTransportSink for ImmediateEstablishTransport {
        fn try_reserve(&self, _identity: EstablishIssueIdentity) -> EstablishTransportAdmission {
            EstablishTransportAdmission::Admitted(Box::new(ImmediateEstablishReservation))
        }
    }

    #[derive(Debug)]
    struct ImmediateEstablishReservation;

    impl EstablishTransportReservation for ImmediateEstablishReservation {
        fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
            submission
                .worker_settled(OperationOutcome::Accepted)
                .expect("test transport publishes exact Establish success");
        }
    }

    #[derive(Debug, Default)]
    struct ActivationCancellationSignals {
        entered: AtomicBool,
        release_activation: tokio::sync::Notify,
        convergence_entered: AtomicBool,
        release_convergence: tokio::sync::Notify,
        converged: AtomicBool,
    }

    #[derive(Debug)]
    struct ActivationCancellationPreparationPort {
        backend: BackendProcessId,
        frontend: FrontendProcessId,
        signals: Arc<ActivationCancellationSignals>,
    }

    impl NativeAttemptPreparationPort for ActivationCancellationPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let dormant = ActivationCancellationDormant {
                inner: SuccessfulCompletionDormantOwner {
                    backend: self.backend,
                    expected_frontend: self.frontend,
                },
                signals: Arc::clone(&self.signals),
            };
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request.bind(scheduling, dormant).map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct ActivationCancellationDormant {
        inner: SuccessfulCompletionDormantOwner,
        signals: Arc<ActivationCancellationSignals>,
    }

    impl DormantNativeAttemptOwner for ActivationCancellationDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            self.inner.eligible_backends()
        }
        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            let signals = Arc::clone(&self.signals);
            let activation = self.inner.activate(schedule, admissions, cancellation);
            Box::pin(async move {
                signals.entered.store(true, Ordering::SeqCst);
                signals.release_activation.notified().await;
                let activated = activation.await?;
                let (inner, rows) = activated.into_parts();
                assert!(rows.is_none());
                Ok(ActivatedNativeAttempt::completion(
                    ActivationCancellationActive { inner, signals },
                ))
            })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            self.inner.converge(cancellation)
        }
    }

    #[derive(Debug)]
    struct ActivationCancellationActive {
        inner: Box<dyn ActiveNativeAttemptOwner>,
        signals: Arc<ActivationCancellationSignals>,
    }

    impl ActiveNativeAttemptOwner for ActivationCancellationActive {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("an already concluded activation must never run") })
        }
        fn converge<'a>(
            &'a mut self,
            cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            let signals = Arc::clone(&self.signals);
            Box::pin(async move {
                signals.convergence_entered.store(true, Ordering::SeqCst);
                signals.release_convergence.notified().await;
                let convergence = self.inner.converge(cancellation).await;
                signals.converged.store(true, Ordering::SeqCst);
                convergence
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn accepted_cancel_during_native_activation_retires_the_original_owner_without_poisoning_supervisor()
     {
        for reason in [
            CancellationReason::Requested,
            CancellationReason::DeadlineExceeded,
        ] {
            let frontend = FrontendProcessId::new_v7();
            let signals = Arc::new(ActivationCancellationSignals::default());
            let (control, root) = governance();
            let scope = root.owner.scope();
            let requester = root.owner.cancellation_requester();
            let (mut supervisor, client) = LogicalExecutionSupervisor::new(
                Handle::current(),
                Arc::new(BindingNativePort),
                QueryProcessNamespace::new(0x771),
                frontend,
                supervisor_config(),
            );
            let start = client.start(
                completion_request(ActivationCancellationPreparationPort {
                    backend: BackendProcessId::new_v7(),
                    frontend,
                    signals: Arc::clone(&signals),
                }),
                root.owner,
            );
            wait_preparation_exit_fact(&signals.entered).await.unwrap();
            requester.request(reason.clone()).unwrap();
            let error = match start.await {
                Ok(_) => panic!("cancelled start cannot become success"),
                Err(error) => error,
            };
            assert_eq!(
                error.kind(),
                if matches!(reason, CancellationReason::DeadlineExceeded) {
                    QueryExecutionErrorKind::DeadlineExceeded
                } else {
                    QueryExecutionErrorKind::Cancelled
                }
            );
            signals.release_activation.notify_one();
            wait_preparation_exit_fact(&signals.convergence_entered)
                .await
                .unwrap();
            assert!(!signals.converged.load(Ordering::SeqCst));
            assert_ne!(control.snapshot().root_responsibilities, 0);
            // A terminal query does not poison the process or block a sibling start.
            let (sibling_control, sibling) = governance();
            let sibling_scope = sibling.owner.scope();
            let mut sibling_handle = client
                .start(
                    completion_request(SuccessfulCompletionPreparationPort {
                        backend: BackendProcessId::new_v7(),
                        expected_frontend: frontend,
                    }),
                    sibling.owner,
                )
                .await
                .unwrap();
            assert!(matches!(
                sibling_handle.take_output(),
                Some(crate::api::ExecutionOutput::Completion)
            ));
            sibling.business.release();
            signals.release_convergence.notify_one();
            wait_preparation_exit_fact(&signals.converged)
                .await
                .unwrap();
            root.business.release();
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
            acknowledge_all_control(&control);
            acknowledge_all_control(&sibling_control);
            scope.wait_released().await;
            sibling_scope.wait_released().await;
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionPreparationPort {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl NativeAttemptPreparationPort for SuccessfulCompletionPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            let expected_frontend = self.expected_frontend;
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request
                    .bind(
                        scheduling,
                        SuccessfulCompletionDormantOwner {
                            backend,
                            expected_frontend,
                        },
                    )
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionDormantOwner {
        backend: BackendProcessId,
        expected_frontend: FrontendProcessId,
    }

    impl DormantNativeAttemptOwner for SuccessfulCompletionDormantOwner {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            assert!(
                schedule
                    .contexts()
                    .iter()
                    .all(|context| context.frontend_process_id() == self.expected_frontend)
            );
            let contexts = schedule.contexts().to_vec();
            Box::pin(async move {
                Ok(ActivatedNativeAttempt::completion(
                    SuccessfulCompletionActiveOwner { contexts },
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct SuccessfulCompletionActiveOwner {
        contexts: Vec<QueryContextRef>,
    }

    impl ActiveNativeAttemptOwner for SuccessfulCompletionActiveOwner {
        fn run<'a>(
            &'a mut self,
            drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async move {
                let valid_for = LeaseValidFor::new(Duration::from_secs(30)).unwrap();
                for (index, &context) in self.contexts.iter().enumerate() {
                    let tag = u8::try_from(index + 1).unwrap();
                    let compatibility = NativeCompatibilityId::new([tag; 32]);
                    let issue = drive
                        .begin_admission_issue(
                            novarocks_execution_contract::AcquireQueryContextAdmissionTicket::new(
                                TaskOperationId::new_v7(),
                                context,
                                valid_for,
                                compatibility,
                                AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
                            ),
                        )
                        .await
                        .unwrap();
                    let ticket = QueryContextAdmissionTicketReceipt::new(
                        AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
                        context,
                        valid_for,
                    );
                    drive
                        .settle_admission_issue(
                            issue,
                            AdmissionIssueSettlement::applied(
                                issue.operation_id(),
                                OperationOutcome::Accepted,
                                ticket,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    let content = |offset: u8| {
                        Arc::new(TestContent(tag + offset)) as Arc<dyn CodecOwnedContent>
                    };
                    let establish = Arc::new(EstablishQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        ticket.ticket_id(),
                        content(0),
                        content(1),
                        content(2),
                        CredentialUpdate::new(
                            CredentialLeaseId::new(u64::from(tag)),
                            CredentialEpoch::new(1).unwrap(),
                            Arc::new(TestSecret),
                        ),
                        valid_for,
                    ));
                    let permit = drive
                        .authorize_establish(establish, compatibility)
                        .await
                        .unwrap();
                    assert_eq!(
                        permit.try_submit(&ImmediateEstablishTransport).unwrap(),
                        EstablishIssueSubmit::Accepted
                    );
                }
                tokio::task::yield_now().await;
                NativeAttemptTerminal::Completed
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn completion_becomes_visible_only_after_exact_establish_success() {
        let frontend = FrontendProcessId::new_v7();
        let attempts = SuccessfulCompletionPreparationPort {
            backend: BackendProcessId::new_v7(),
            expected_frontend: frontend,
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x50),
            frontend,
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let mut handle = client
            .start(completion_request(attempts), root.owner)
            .await
            .expect("exact Establish success permits completion visibility");
        assert!(matches!(
            handle.take_output(),
            Some(crate::api::ExecutionOutput::Completion)
        ));
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[derive(Debug)]
    struct PrematureCompletionPreparationPort {
        backend: BackendProcessId,
    }

    impl NativeAttemptPreparationPort for PrematureCompletionPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            let backend = self.backend;
            Box::pin(async move {
                let scheduling = no_scan_scheduling(&request);
                request
                    .bind(scheduling, PrematureCompletionDormant { backend })
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionDormant {
        backend: BackendProcessId,
    }

    impl DormantNativeAttemptOwner for PrematureCompletionDormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            std::slice::from_ref(&self.backend)
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async {
                Ok(ActivatedNativeAttempt::completion(
                    PrematureCompletionActive,
                ))
            })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    #[derive(Debug)]
    struct PrematureCompletionActive;

    impl ActiveNativeAttemptOwner for PrematureCompletionActive {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn completion_is_not_visible_without_exact_establish_success() {
        let attempts = PrematureCompletionPreparationPort {
            backend: BackendProcessId::new_v7(),
        };
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x4a),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let error = match client.start(completion_request(attempts), root.owner).await {
            Ok(_) => panic!("Native completion without Establish proof must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[tokio::test]
    async fn start_handoff_has_no_second_capacity_and_closed_supervisor_rejects_work() {
        let seed_drops = Arc::new(AtomicU64::new(0));
        let (starts, mut start_rx) = mpsc::unbounded_channel();
        let (_shutdown_sender, shutdown) = watch::channel(false);
        let client = QueryExecutionClient::new(QueryExecutionDriverHandle { starts, shutdown });

        let (first_control, first) = governance();
        let first_scope = first.owner.scope();
        let first_start = client.start(drop_tracked_request(&seed_drops), first.owner);
        let (second_control, second) = governance();
        let second_scope = second.owner.scope();
        let second_cancellation = second_scope.cancellation().unwrap();
        let second_start = client.start(drop_tracked_request(&seed_drops), second.owner);

        let first_command = start_rx
            .recv()
            .await
            .expect("first admitted query reaches the handoff");
        let second_command = start_rx
            .recv()
            .await
            .expect("second admitted query reaches the same unbounded handoff");
        assert!(
            second_control.next_control().is_none(),
            "handoff must not manufacture cancellation work"
        );
        assert_eq!(second_cancellation.reason(), None);
        assert!(
            first_control.next_control().is_none(),
            "handoff must not manufacture cancellation work"
        );

        let StartCommand {
            request: first_request,
            owner: first_owner,
            reply: first_reply,
        } = first_command;
        let StartCommand {
            request: second_request,
            owner: second_owner,
            reply: second_reply,
        } = second_command;
        first_owner.complete();
        second_owner.complete();
        let start_error = || {
            QueryExecutionError::new(
                QueryExecutionErrorKind::Rejected,
                "injected supervisor handoff completion",
            )
        };
        assert!(first_reply.send(Err(start_error())).is_ok());
        assert!(second_reply.send(Err(start_error())).is_ok());
        let first_error = match first_start.await {
            Ok(_) => panic!("injected handoff reply must fail"),
            Err(error) => error,
        };
        let second_error = match second_start.await {
            Ok(_) => panic!("injected handoff reply must fail"),
            Err(error) => error,
        };
        assert_eq!(first_error.kind(), QueryExecutionErrorKind::Rejected);
        assert_eq!(second_error.kind(), QueryExecutionErrorKind::Rejected);
        drop(first_request);
        drop(second_request);
        assert_eq!(seed_drops.load(Ordering::SeqCst), 2);

        first.business.release();
        second.business.release();
        first_scope.wait_released().await;
        second_scope.wait_released().await;

        drop(start_rx);
        let (closed_control, closed) = governance();
        let closed_scope = closed.owner.scope();
        let closed_cancellation = closed_scope.cancellation().unwrap();
        let closed_error = match client
            .start(drop_tracked_request(&seed_drops), closed.owner)
            .await
        {
            Ok(_) => panic!("a closed supervisor must reject new work"),
            Err(error) => error,
        };
        assert_eq!(closed_error.kind(), QueryExecutionErrorKind::Rejected);
        assert!(
            closed_control.next_control().is_none(),
            "closed admission rejection must not manufacture cancellation work"
        );
        assert_eq!(closed_cancellation.reason(), None);
        assert_eq!(seed_drops.load(Ordering::SeqCst), 3);
        closed.business.release();
        closed_scope.wait_released().await;
    }

    #[derive(Debug)]
    struct PanickingOpenNativePort;

    impl LogicalExecutionNativePort for PanickingOpenNativePort {
        fn open(&self, _request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            Box::pin(async { panic!("injected Native open panic") })
        }
    }

    #[derive(Debug)]
    struct IsolatedTaskPanicNativePort {
        opens: AtomicU64,
        first_opened: Arc<AtomicBool>,
        second_opened: Arc<AtomicBool>,
        second_gate: Arc<tokio::sync::Notify>,
    }

    impl LogicalExecutionNativePort for IsolatedTaskPanicNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            if self.opens.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_opened.store(true, Ordering::SeqCst);
                return Box::pin(async { panic!("injected isolated logical task panic") });
            }
            self.second_opened.store(true, Ordering::SeqCst);
            let gate = Arc::clone(&self.second_gate);
            Box::pin(async move {
                gate.notified().await;
                request.bind().map_err(Into::into)
            })
        }
    }

    #[tokio::test]
    async fn one_logical_task_panic_does_not_cancel_or_stop_another_execution() {
        let frontend = FrontendProcessId::new_v7();
        let first_opened = Arc::new(AtomicBool::new(false));
        let second_opened = Arc::new(AtomicBool::new(false));
        let second_gate = Arc::new(tokio::sync::Notify::new());
        let native = Arc::new(IsolatedTaskPanicNativePort {
            opens: AtomicU64::new(0),
            first_opened: Arc::clone(&first_opened),
            second_opened: Arc::clone(&second_opened),
            second_gate: Arc::clone(&second_gate),
        });
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            native,
            QueryProcessNamespace::new(0x51),
            frontend,
            supervisor_config(),
        );
        let (first_control, first) = governance();
        let first_scope = first.owner.scope();
        let first_cancellation = first_scope.cancellation().unwrap();
        let first_result =
            client.start(completion_request(UnreachablePreparationPort), first.owner);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !first_opened.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first logical task reached its isolated panic");

        let (second_control, second) = governance();
        let second_scope = second.owner.scope();
        let second_cancellation = second_scope.cancellation().unwrap();
        let second_result = client.start(
            completion_request(SuccessfulCompletionPreparationPort {
                backend: BackendProcessId::new_v7(),
                expected_frontend: frontend,
            }),
            second.owner,
        );

        let first_error = match first_result.await {
            Ok(_) => panic!("the panicking logical task must fail its own request"),
            Err(error) => error,
        };
        assert_eq!(first_error.kind(), QueryExecutionErrorKind::Failed);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !second_opened.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the sibling logical task remains admitted after the panic");
        assert_eq!(first_cancellation.reason(), None);
        assert_eq!(second_cancellation.reason(), None);
        assert!(first_control.next_control().is_none());
        assert!(second_control.next_control().is_none());

        second_gate.notify_one();
        let mut second_handle = second_result
            .await
            .expect("a sibling execution remains supervised after the panic");
        assert!(matches!(
            second_handle.take_output(),
            Some(crate::api::ExecutionOutput::Completion)
        ));
        assert_eq!(first_cancellation.reason(), None);
        assert!(first_control.next_control().is_none());

        first.business.release();
        second.business.release();
        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert_eq!(first_cancellation.reason(), None);
        acknowledge_all_control(&first_control);
        acknowledge_all_control(&second_control);
        first_scope.wait_released().await;
        second_scope.wait_released().await;
    }

    #[tokio::test]
    async fn logical_task_panic_completes_uninstalled_work_and_drains_supervisor() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x49),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
        let error = match start.await {
            Ok(_) => panic!("panicking Native open must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(matches!(
            shutdown,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        acknowledge_all_control(&control);
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }

    #[derive(Debug)]
    struct DelayedShutdownNativePort;

    impl LogicalExecutionNativePort for DelayedShutdownNativePort {
        fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let cancellation = request.cancellation();
            Box::pin(async move {
                cancellation.cancelled().await;
                tokio::time::sleep(Duration::from_millis(25)).await;
                Err(
                    LogicalNativeOpenFailure::cancelled("server shutdown interrupted Native open")
                        .into(),
                )
            })
        }
    }

    #[derive(Debug)]
    struct PendingOpenNativePort {
        entered: Arc<tokio::sync::Notify>,
    }

    impl LogicalExecutionNativePort for PendingOpenNativePort {
        fn open(&self, _request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
            let entered = Arc::clone(&self.entered);
            Box::pin(async move {
                entered.notify_one();
                std::future::pending::<Result<LogicalNativeSession, LogicalNativeOpenError>>().await
            })
        }
    }

    #[tokio::test]
    async fn deadline_before_actor_install_settles_its_cancel_control() {
        let (control, root) = {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1 << 20,
                    control_bytes: 1 << 12,
                    per_scope_bytes: (1 << 20) - (1 << 12),
                },
            )
            .unwrap();
            control.mark_ready().unwrap();
            let root = control
                .try_begin_root(WorkRequest {
                    class: WorkClass::Query,
                    deadline: Some((Instant::now() + Duration::from_millis(25)).into()),
                })
                .unwrap();
            (control, root)
        };
        let scope = root.owner.scope();
        let entered = Arc::new(tokio::sync::Notify::new());
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PendingOpenNativePort {
                entered: Arc::clone(&entered),
            }),
            QueryProcessNamespace::new(0x4a),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );

        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("Native open did not begin");
        tokio::time::sleep(Duration::from_millis(30)).await;
        control.expire_deadlines();

        let start = tokio::time::timeout(Duration::from_secs(1), start)
            .await
            .expect("deadline must stop a pre-install Native open");
        let Err(error) = start else {
            panic!("deadline must not return an execution handle");
        };
        assert_eq!(
            error.kind(),
            QueryExecutionErrorKind::DeadlineExceeded,
            "{}",
            error.message()
        );
        let input = error
            .execution_failure()
            .expect("pre-install deadline retains canonical input");
        assert_eq!(
            input.fact(),
            QueryFailureReason::DeadlineExceeded.failure_fact()
        );
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert_eq!(
            scope.cancellation().unwrap().reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        scope.wait_released().await;
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert!(
            control.next_control().is_none(),
            "the terminal pre-install failure must settle its cancellation control"
        );
    }

    #[tokio::test]
    async fn shutdown_deadline_keeps_the_same_supervisor_join_for_retry() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(DelayedShutdownNativePort),
            QueryProcessNamespace::new(0x47),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (control, root) = governance();
        let scope = root.owner.scope();
        let start = client.start(completion_request(UnreachablePreparationPort), root.owner);
        tokio::task::yield_now().await;

        assert_eq!(
            supervisor.shutdown_until(Instant::now()).await,
            Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded)
        );
        let error = match start.await {
            Ok(_) => panic!("shutdown during Native open must not return an execution handle"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        root.business.release();
        supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        scope.wait_released().await;
    }

    #[test]
    fn query_sequence_is_not_reused_by_a_rebuilt_supervisor_allocator() {
        let namespace = QueryProcessNamespace::new(0x4e);
        let first = ProcessQueryIdAllocator::new(namespace)
            .first_execution()
            .unwrap();
        let rebuilt = ProcessQueryIdAllocator::new(namespace)
            .first_execution()
            .unwrap();

        assert_ne!(first, rebuilt);
    }

    #[tokio::test]
    async fn registry_shutdown_error_retains_owner_and_run_failure_for_retry() {
        let (mut supervisor, client) = LogicalExecutionSupervisor::new(
            Handle::current(),
            Arc::new(PanickingOpenNativePort),
            QueryProcessNamespace::new(0x4f),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        let (_control, root) = governance();
        let scope = root.owner.scope();
        let error = match client
            .start(completion_request(UnreachablePreparationPort), root.owner)
            .await
        {
            Ok(_) => panic!("panicking Native open must fail the logical task"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        root.business.release();
        supervisor.inject_registry_shutdown_error_once(
            LogicalExecutionRuntimeRegistryError::JoinNotReady,
        );

        assert_eq!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::Registry(
                LogicalExecutionRuntimeRegistryError::JoinNotReady,
            ))
        );
        assert!(!supervisor.shutdown_complete);

        assert!(matches!(
            supervisor
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionSupervisorShutdownError::SupervisorFailed(_))
        ));
        assert!(supervisor.shutdown_complete);
        scope.wait_released().await;
    }
}
