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

use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use bytes::Bytes;
use foundationdb::{Database, FdbError, Transaction};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use super::codec::{DurableCommitState, KeyspaceCodec, REVISION_BYTES};
use super::txn::create_raw_transaction_with_observer;
use crate::state_store::runtime::OperationHandle;
use crate::state_store::{
    CommitOutcome, CommitReceipt, CommitResolution, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StateStoreMetrics, StateStoreOperation, StateStoreOutcome, StoreRevision,
    TransactionId,
};

const AUXILIARY_MAX_ATTEMPTS: usize = 5;
const AUXILIARY_DEADLINE: Duration = Duration::from_secs(4);
const NOT_COMMITTED_ERROR_CODE: i32 = 1020;
const DETERMINISTIC_COMMIT_ERROR_CODES: &[i32] = &[
    2000, // client_invalid_operation (including malformed versionstamp operands)
    2002, // commit_read_incomplete
    2004, // key_outside_legal_range
    2006, // invalid_option_value
    2007, // invalid_option
    2018, // invalid_mutation_type
    2020, // transaction_invalid_version
    2023, // transaction_read_only
    2101, // transaction_too_large
    2102, // key_too_large
    2103, // value_too_large
    2108, // unsupported_operation
    2109, // too_many_tags
    2110, // tag_too_long
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReservationDecision {
    Reserve,
    Dispatch,
    ReturnCommitted([u8; REVISION_BYTES]),
    ReturnNotCommitted,
    ForeignPending,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PreDispatchFailureDecision {
    PersistNotCommitted,
    RejectReuse,
    ReturnCommitted([u8; REVISION_BYTES]),
    PendingUnknown,
}

enum ReservationCommitFailureDecision {
    Retry {
        reservation_may_exist: bool,
        foreign_state_may_exist: bool,
    },
    Outcome(CommitOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeCommitDisposition {
    ConflictNotCommitted,
    RetryableNotCommitted,
    DefiniteNotCommitted,
    Unknown,
}

impl NativeCommitDisposition {
    const fn category(self) -> &'static str {
        match self {
            Self::ConflictNotCommitted => "conflict_not_committed",
            Self::RetryableNotCommitted => "retryable_not_committed",
            Self::DefiniteNotCommitted => "definite_not_committed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitStatePhase {
    Reservation,
    DataCommit,
    Versionstamp,
    PreDispatchTombstone,
    Terminalization,
    Resolve,
}

impl CommitStatePhase {
    #[cfg(test)]
    const ALL: [Self; 6] = [
        Self::Reservation,
        Self::DataCommit,
        Self::Versionstamp,
        Self::PreDispatchTombstone,
        Self::Terminalization,
        Self::Resolve,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Reservation => "reservation",
            Self::DataCommit => "data_commit",
            Self::Versionstamp => "versionstamp",
            Self::PreDispatchTombstone => "pre_dispatch_tombstone",
            Self::Terminalization => "terminalization",
            Self::Resolve => "resolve",
        }
    }
}

struct NativeErrorLogFields {
    transaction_id: String,
    phase: &'static str,
    native_error_code: i32,
    category: &'static str,
}

fn native_error_log_fields(
    transaction_id: TransactionId,
    phase: CommitStatePhase,
    error: FdbError,
    disposition: NativeCommitDisposition,
) -> NativeErrorLogFields {
    NativeErrorLogFields {
        transaction_id: transaction_id.as_uuid().to_string(),
        phase: phase.as_str(),
        native_error_code: error.code(),
        category: disposition.category(),
    }
}

fn log_native_error(
    transaction_id: TransactionId,
    phase: CommitStatePhase,
    error: FdbError,
    disposition: NativeCommitDisposition,
) {
    let fields = native_error_log_fields(transaction_id, phase, error, disposition);
    tracing::warn!(
        transaction_id = %fields.transaction_id,
        phase = fields.phase,
        native_error_code = fields.native_error_code,
        category = fields.category,
        "FoundationDB commit-state native error"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuxiliaryMetricEvent {
    Attempt,
    Deadline,
    BlockingFailure,
}

#[derive(Debug)]
struct AuxiliaryAttemptBudget {
    attempts: usize,
}

impl AuxiliaryAttemptBudget {
    fn new() -> Self {
        Self { attempts: 0 }
    }

    fn try_consume(&mut self) -> bool {
        if self.attempts == AUXILIARY_MAX_ATTEMPTS {
            return false;
        }
        self.attempts += 1;
        true
    }

    fn has_remaining(&self) -> bool {
        self.attempts < AUXILIARY_MAX_ATTEMPTS
    }
}

fn record_auxiliary_metric(metrics: &StateStoreMetrics, event: AuxiliaryMetricEvent) {
    match event {
        AuxiliaryMetricEvent::Attempt => metrics.record_retry(),
        AuxiliaryMetricEvent::Deadline => metrics.record_deadline(),
        AuxiliaryMetricEvent::BlockingFailure => metrics.record_blocking_failure(),
    }
}

fn begin_auxiliary_attempt(
    budget: &mut AuxiliaryAttemptBudget,
    deadline: Instant,
    metrics: &StateStoreMetrics,
) -> Result<(), StateStoreError> {
    if Instant::now() >= deadline {
        record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
        return Err(deadline_error());
    }
    if !budget.try_consume() {
        return Err(auxiliary_attempts_exhausted());
    }
    record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Attempt);
    Ok(())
}

fn record_auxiliary_error(metrics: &StateStoreMetrics, error: &StateStoreError) {
    match error.kind() {
        StateStoreErrorKind::DeadlineExceeded => {
            record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
        }
        StateStoreErrorKind::Transient | StateStoreErrorKind::ProviderUnavailable => {
            record_auxiliary_metric(metrics, AuxiliaryMetricEvent::BlockingFailure);
        }
        _ => {}
    }
}

fn is_retryable_auxiliary_error(error: &StateStoreError) -> bool {
    matches!(
        error.kind(),
        StateStoreErrorKind::Transient
            | StateStoreErrorKind::ProviderUnavailable
            | StateStoreErrorKind::DeadlineExceeded
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalizationDecision {
    WriteNotCommitted,
    AlreadyNotCommitted,
    PreserveUnknown,
}

fn decide_terminalization(
    observed: Option<&DurableCommitState>,
    reservation_token: [u8; 16],
) -> TerminalizationDecision {
    match observed {
        Some(DurableCommitState::Pending(token)) if *token == reservation_token => {
            TerminalizationDecision::WriteNotCommitted
        }
        Some(DurableCommitState::NotCommitted) => TerminalizationDecision::AlreadyNotCommitted,
        None | Some(DurableCommitState::Pending(_)) | Some(DurableCommitState::Committed(_)) => {
            TerminalizationDecision::PreserveUnknown
        }
    }
}

fn classify_native_commit_error(error: FdbError) -> NativeCommitDisposition {
    if error.code() == NOT_COMMITTED_ERROR_CODE {
        NativeCommitDisposition::ConflictNotCommitted
    } else if error.is_retryable_not_committed() {
        NativeCommitDisposition::RetryableNotCommitted
    } else if DETERMINISTIC_COMMIT_ERROR_CODES.contains(&error.code()) {
        NativeCommitDisposition::DefiniteNotCommitted
    } else {
        NativeCommitDisposition::Unknown
    }
}

fn should_retry_auxiliary_commit(error: FdbError) -> bool {
    error.code() == NOT_COMMITTED_ERROR_CODE
        || error.code() == 1031
        || error.is_retryable_not_committed()
        || error.is_maybe_committed()
}

fn classify_auxiliary_native_error(error: FdbError) -> StateStoreError {
    if error.code() == 1031 {
        deadline_error()
    } else if DETERMINISTIC_COMMIT_ERROR_CODES.contains(&error.code()) {
        deterministic_commit_error(error.code())
    } else if error.is_retryable() || error.is_maybe_committed() {
        provider_error()
    } else {
        StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB auxiliary transaction returned an unclassified native error",
        )
    }
}

fn should_record_native_blocking_failure(disposition: NativeCommitDisposition) -> bool {
    matches!(
        disposition,
        NativeCommitDisposition::RetryableNotCommitted | NativeCommitDisposition::Unknown
    )
}

fn native_error_metric_event(
    error: FdbError,
    disposition: NativeCommitDisposition,
) -> Option<AuxiliaryMetricEvent> {
    if error.code() == 1031 {
        Some(AuxiliaryMetricEvent::Deadline)
    } else if should_record_native_blocking_failure(disposition) {
        Some(AuxiliaryMetricEvent::BlockingFailure)
    } else {
        None
    }
}

fn record_native_error(
    metrics: &StateStoreMetrics,
    transaction_id: TransactionId,
    phase: CommitStatePhase,
    error: FdbError,
    disposition: NativeCommitDisposition,
) {
    log_native_error(transaction_id, phase, error, disposition);
    if let Some(event) = native_error_metric_event(error, disposition) {
        record_auxiliary_metric(metrics, event);
    }
}

fn decide_pre_dispatch_failure(
    observed: Option<&DurableCommitState>,
) -> PreDispatchFailureDecision {
    match observed {
        None => PreDispatchFailureDecision::PersistNotCommitted,
        Some(DurableCommitState::NotCommitted) => PreDispatchFailureDecision::RejectReuse,
        Some(DurableCommitState::Committed(revision)) => {
            PreDispatchFailureDecision::ReturnCommitted(*revision)
        }
        Some(DurableCommitState::Pending(_)) => PreDispatchFailureDecision::PendingUnknown,
    }
}

fn decide_reservation_commit_failure(
    error: FdbError,
    has_remaining_attempt: bool,
) -> ReservationCommitFailureDecision {
    let disposition = classify_native_commit_error(error);
    if disposition == NativeCommitDisposition::DefiniteNotCommitted {
        return ReservationCommitFailureDecision::Outcome(CommitOutcome::DefiniteFailure(
            deterministic_commit_error(error.code()),
        ));
    }
    if has_remaining_attempt && should_retry_auxiliary_commit(error) {
        return ReservationCommitFailureDecision::Retry {
            reservation_may_exist: disposition == NativeCommitDisposition::Unknown,
            foreign_state_may_exist: disposition == NativeCommitDisposition::ConflictNotCommitted,
        };
    }
    let outcome = match disposition {
        NativeCommitDisposition::ConflictNotCommitted => {
            CommitOutcome::CommitUnknown(conflict_error())
        }
        NativeCommitDisposition::RetryableNotCommitted => {
            CommitOutcome::TransientBeforeCommit(provider_transient())
        }
        NativeCommitDisposition::Unknown => {
            CommitOutcome::TransientBeforeCommit(provider_transient())
        }
        NativeCommitDisposition::DefiniteNotCommitted => unreachable!(),
    };
    ReservationCommitFailureDecision::Outcome(outcome)
}

pub(super) fn decide_reservation(
    observed: Option<&DurableCommitState>,
    reservation_token: [u8; 16],
) -> ReservationDecision {
    match observed {
        None => ReservationDecision::Reserve,
        Some(DurableCommitState::Pending(token)) if *token == reservation_token => {
            ReservationDecision::Dispatch
        }
        Some(DurableCommitState::Pending(_)) => ReservationDecision::ForeignPending,
        Some(DurableCommitState::Committed(revision)) => {
            ReservationDecision::ReturnCommitted(*revision)
        }
        Some(DurableCommitState::NotCommitted) => ReservationDecision::ReturnNotCommitted,
    }
}

pub(super) struct PreparedCommit {
    pub database: Arc<Database>,
    pub transaction: Transaction,
    pub codec: KeyspaceCodec,
    pub limits: StateStoreLimits,
    pub deadline: Instant,
    pub metrics: Arc<StateStoreMetrics>,
    pub _operation: OperationHandle,
    pub transaction_id: TransactionId,
}

pub(super) struct PreparedFailure {
    pub database: Arc<Database>,
    pub codec: KeyspaceCodec,
    pub limits: StateStoreLimits,
    pub deadline: Instant,
    pub metrics: Arc<StateStoreMetrics>,
    pub _operation: OperationHandle,
    pub transaction_id: TransactionId,
}

pub(super) async fn supervise_commit(
    prepared: PreparedCommit,
    started: StdInstant,
) -> CommitOutcome {
    let metrics = Arc::clone(&prepared.metrics);
    #[cfg(feature = "state-store-test-hooks")]
    let waiter_drop_guard = super::test_support::arm_commit_waiter_drop_guard();
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let outcome = run_commit_owner(prepared).await;
        record_commit(&metrics, started, &outcome);
        let _ = sender.send(outcome);
    });
    let outcome = receiver.await.unwrap_or_else(|_| {
        CommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB commit supervisor stopped before reporting an outcome",
        ))
    });
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(waiter_drop_guard) = waiter_drop_guard {
        waiter_drop_guard.complete();
    }
    outcome
}

pub(super) async fn supervise_pre_dispatch_failure(
    prepared: PreparedFailure,
    local_outcome: CommitOutcome,
    started: StdInstant,
) -> CommitOutcome {
    let metrics = Arc::clone(&prepared.metrics);
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let outcome = persist_pre_dispatch_failure(prepared, local_outcome).await;
        record_commit(&metrics, started, &outcome);
        let _ = sender.send(outcome);
    });
    receiver.await.unwrap_or_else(|_| {
        CommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB failure supervisor stopped before reporting an outcome",
        ))
    })
}

async fn persist_pre_dispatch_failure(
    prepared: PreparedFailure,
    local_outcome: CommitOutcome,
) -> CommitOutcome {
    let deadline = auxiliary_deadline(prepared.deadline);
    let mut budget = AuxiliaryAttemptBudget::new();
    let state_key = prepared
        .codec
        .commit_state_key(*prepared.transaction_id.as_uuid().as_bytes());
    loop {
        let transaction = match create_auxiliary_transaction(
            prepared.database.as_ref(),
            &prepared.limits,
            deadline,
            &mut budget,
            prepared.metrics.as_ref(),
            prepared.transaction_id,
            CommitStatePhase::PreDispatchTombstone,
        ) {
            Ok(transaction) => transaction,
            Err(_) => return CommitOutcome::CommitUnknown(provider_unknown()),
        };
        let state = match load_commit_state(
            &transaction,
            &prepared.codec,
            &state_key,
            deadline,
            prepared.metrics.as_ref(),
            prepared.transaction_id,
            CommitStatePhase::PreDispatchTombstone,
        )
        .await
        {
            Ok(state) => state,
            Err(error) if is_retryable_auxiliary_error(&error) && Instant::now() < deadline => {
                continue;
            }
            Err(error) => return CommitOutcome::CommitUnknown(error),
        };
        match decide_pre_dispatch_failure(state.as_ref()) {
            PreDispatchFailureDecision::ReturnCommitted(revision) => {
                return committed(prepared.transaction_id, revision);
            }
            PreDispatchFailureDecision::PendingUnknown => {
                return CommitOutcome::CommitUnknown(foreign_pending());
            }
            PreDispatchFailureDecision::RejectReuse => {
                return CommitOutcome::DefiniteFailure(invalid_reuse());
            }
            PreDispatchFailureDecision::PersistNotCommitted => {
                transaction.set(&state_key, &prepared.codec.not_committed_value());
                match timeout_at(deadline, transaction.commit()).await {
                    Ok(Ok(committed)) => {
                        drop(committed);
                        return local_outcome;
                    }
                    Ok(Err(error)) => {
                        let error = *error;
                        let disposition = classify_native_commit_error(error);
                        record_native_error(
                            prepared.metrics.as_ref(),
                            prepared.transaction_id,
                            CommitStatePhase::PreDispatchTombstone,
                            error,
                            disposition,
                        );
                        if should_retry_auxiliary_commit(error)
                            && budget.has_remaining()
                            && Instant::now() < deadline
                        {
                            continue;
                        }
                        return CommitOutcome::CommitUnknown(provider_unknown());
                    }
                    Err(_) => {
                        record_auxiliary_metric(
                            prepared.metrics.as_ref(),
                            AuxiliaryMetricEvent::Deadline,
                        );
                        return CommitOutcome::CommitUnknown(deadline_unknown());
                    }
                }
            }
        }
    }
}

async fn run_commit_owner(prepared: PreparedCommit) -> CommitOutcome {
    let reservation_token = *Uuid::new_v4().as_bytes();
    let reservation = reserve_commit_state(
        prepared.database.as_ref(),
        &prepared.codec,
        &prepared.limits,
        prepared.transaction_id,
        reservation_token,
        prepared.deadline,
        prepared.metrics.as_ref(),
    )
    .await;
    match reservation {
        ReservationResult::Dispatch => {}
        ReservationResult::Committed(revision) => {
            return committed(prepared.transaction_id, revision);
        }
        ReservationResult::Outcome(outcome) => return outcome,
    }

    #[cfg(feature = "state-store-test-hooks")]
    let gates = super::test_support::take_commit_gates();
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(gates) = gates.as_ref() {
        gates.before_native_commit().await;
    }

    let versionstamp = prepared.transaction.get_versionstamp();
    let native_result = timeout_at(prepared.deadline, prepared.transaction.commit()).await;
    let outcome = match native_result {
        Ok(Ok(committed_transaction)) => {
            drop(committed_transaction);
            match timeout_at(prepared.deadline, versionstamp).await {
                Ok(Ok(versionstamp)) => match revision_from_bytes(versionstamp.as_ref()) {
                    Ok(revision) => CommitOutcome::Committed(CommitReceipt {
                        transaction_id: prepared.transaction_id,
                        revision,
                    }),
                    Err(error) => CommitOutcome::CommitUnknown(error),
                },
                Ok(Err(error)) => {
                    record_native_error(
                        prepared.metrics.as_ref(),
                        prepared.transaction_id,
                        CommitStatePhase::Versionstamp,
                        error,
                        classify_native_commit_error(error),
                    );
                    CommitOutcome::CommitUnknown(provider_unknown())
                }
                Err(_) => {
                    record_auxiliary_metric(
                        prepared.metrics.as_ref(),
                        AuxiliaryMetricEvent::Deadline,
                    );
                    CommitOutcome::CommitUnknown(deadline_unknown())
                }
            }
        }
        Ok(Err(error)) => {
            let error = *error;
            let disposition = classify_native_commit_error(error);
            record_native_error(
                prepared.metrics.as_ref(),
                prepared.transaction_id,
                CommitStatePhase::DataCommit,
                error,
                disposition,
            );
            match disposition {
                NativeCommitDisposition::ConflictNotCommitted
                | NativeCommitDisposition::RetryableNotCommitted
                | NativeCommitDisposition::DefiniteNotCommitted => {
                    let terminalized = terminalize_matching_pending(
                        prepared.database.as_ref(),
                        &prepared.codec,
                        &prepared.limits,
                        prepared.transaction_id,
                        reservation_token,
                        prepared.deadline,
                        prepared.metrics.as_ref(),
                    )
                    .await;
                    if !terminalized {
                        CommitOutcome::CommitUnknown(provider_unknown())
                    } else if disposition == NativeCommitDisposition::ConflictNotCommitted {
                        CommitOutcome::Conflict(conflict_error())
                    } else if disposition == NativeCommitDisposition::DefiniteNotCommitted {
                        CommitOutcome::DefiniteFailure(deterministic_commit_error(error.code()))
                    } else {
                        CommitOutcome::TransientBeforeCommit(provider_transient())
                    }
                }
                NativeCommitDisposition::Unknown => {
                    CommitOutcome::CommitUnknown(provider_unknown())
                }
            }
        }
        Err(_) => {
            record_auxiliary_metric(prepared.metrics.as_ref(), AuxiliaryMetricEvent::Deadline);
            CommitOutcome::CommitUnknown(deadline_unknown())
        }
    };

    #[cfg(feature = "state-store-test-hooks")]
    if let Some(gates) = gates {
        return gates.before_response(outcome).await;
    }
    outcome
}

enum ReservationResult {
    Dispatch,
    Committed([u8; REVISION_BYTES]),
    Outcome(CommitOutcome),
}

async fn reserve_commit_state(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    transaction_id: TransactionId,
    reservation_token: [u8; 16],
    public_deadline: Instant,
    metrics: &StateStoreMetrics,
) -> ReservationResult {
    let deadline = auxiliary_deadline(public_deadline);
    let mut budget = AuxiliaryAttemptBudget::new();
    let mut reservation_may_exist = false;
    let mut foreign_state_may_exist = false;
    let state_key = codec.commit_state_key(*transaction_id.as_uuid().as_bytes());
    loop {
        let transaction = match create_auxiliary_transaction(
            database,
            limits,
            deadline,
            &mut budget,
            metrics,
            transaction_id,
            CommitStatePhase::Reservation,
        ) {
            Ok(transaction) => transaction,
            Err(error) => {
                return ReservationResult::Outcome(classify_reservation_read_error(
                    error,
                    reservation_may_exist,
                    foreign_state_may_exist,
                ));
            }
        };
        let observed = match load_commit_state(
            &transaction,
            codec,
            &state_key,
            deadline,
            metrics,
            transaction_id,
            CommitStatePhase::Reservation,
        )
        .await
        {
            Ok(observed) => observed,
            Err(error) => {
                if is_retryable_auxiliary_error(&error) && Instant::now() < deadline {
                    continue;
                }
                return ReservationResult::Outcome(classify_reservation_read_error(
                    error,
                    reservation_may_exist,
                    foreign_state_may_exist,
                ));
            }
        };
        reservation_may_exist = false;
        foreign_state_may_exist = false;
        match decide_reservation(observed.as_ref(), reservation_token) {
            ReservationDecision::Dispatch => return ReservationResult::Dispatch,
            ReservationDecision::ReturnCommitted(revision) => {
                return ReservationResult::Committed(revision);
            }
            ReservationDecision::ReturnNotCommitted => {
                return ReservationResult::Outcome(CommitOutcome::DefiniteFailure(invalid_reuse()));
            }
            ReservationDecision::ForeignPending => {
                return ReservationResult::Outcome(CommitOutcome::CommitUnknown(foreign_pending()));
            }
            ReservationDecision::Reserve => {
                transaction.set(&state_key, &codec.pending_value(reservation_token));
                match timeout_at(deadline, transaction.commit()).await {
                    Ok(Ok(committed)) => {
                        drop(committed);
                        return ReservationResult::Dispatch;
                    }
                    Ok(Err(error)) => {
                        let error = *error;
                        let disposition = classify_native_commit_error(error);
                        record_native_error(
                            metrics,
                            transaction_id,
                            CommitStatePhase::Reservation,
                            error,
                            disposition,
                        );
                        let has_remaining_attempt =
                            budget.has_remaining() && Instant::now() < deadline;
                        match decide_reservation_commit_failure(error, has_remaining_attempt) {
                            ReservationCommitFailureDecision::Retry {
                                reservation_may_exist: own_state_may_exist,
                                foreign_state_may_exist: competing_state_may_exist,
                            } => {
                                reservation_may_exist |= own_state_may_exist;
                                foreign_state_may_exist |= competing_state_may_exist;
                                continue;
                            }
                            ReservationCommitFailureDecision::Outcome(outcome) => {
                                return ReservationResult::Outcome(outcome);
                            }
                        }
                    }
                    Err(_) => {
                        record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
                        return ReservationResult::Outcome(CommitOutcome::TransientBeforeCommit(
                            deadline_error(),
                        ));
                    }
                }
            }
        }
    }
}

pub(super) async fn resolve_commit(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    transaction_id: TransactionId,
    metrics: &StateStoreMetrics,
) -> Result<CommitResolution, StateStoreError> {
    let deadline = Instant::now() + limits.transaction_deadline.min(AUXILIARY_DEADLINE);
    let mut budget = AuxiliaryAttemptBudget::new();
    let state_key = codec.commit_state_key(*transaction_id.as_uuid().as_bytes());
    loop {
        let transaction = create_auxiliary_transaction(
            database,
            limits,
            deadline,
            &mut budget,
            metrics,
            transaction_id,
            CommitStatePhase::Resolve,
        )?;
        let state = match load_commit_state(
            &transaction,
            codec,
            &state_key,
            deadline,
            metrics,
            transaction_id,
            CommitStatePhase::Resolve,
        )
        .await
        {
            Ok(state) => state,
            Err(error) if is_retryable_auxiliary_error(&error) && Instant::now() < deadline => {
                continue;
            }
            Err(error) => return Err(error),
        };
        match state {
            Some(DurableCommitState::Committed(revision)) => {
                return Ok(CommitResolution::Committed(receipt(
                    transaction_id,
                    revision,
                )?));
            }
            Some(DurableCommitState::NotCommitted) => return Ok(CommitResolution::NotCommitted),
            Some(DurableCommitState::Pending(_)) => return Ok(CommitResolution::Unresolved),
            None => {
                transaction.set(&state_key, &codec.not_committed_value());
                match timeout_at(deadline, transaction.commit()).await {
                    Ok(Ok(committed)) => {
                        drop(committed);
                        return Ok(CommitResolution::NotCommitted);
                    }
                    Ok(Err(error)) => {
                        let error = *error;
                        let disposition = classify_native_commit_error(error);
                        record_native_error(
                            metrics,
                            transaction_id,
                            CommitStatePhase::Resolve,
                            error,
                            disposition,
                        );
                        if should_retry_auxiliary_commit(error)
                            && budget.has_remaining()
                            && Instant::now() < deadline
                        {
                            continue;
                        }
                        return Err(classify_auxiliary_native_error(error));
                    }
                    Err(_) => {
                        record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
                        return Err(deadline_error());
                    }
                }
            }
        }
    }
}

async fn terminalize_matching_pending(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    transaction_id: TransactionId,
    reservation_token: [u8; 16],
    public_deadline: Instant,
    metrics: &StateStoreMetrics,
) -> bool {
    let deadline = auxiliary_deadline(public_deadline);
    let mut budget = AuxiliaryAttemptBudget::new();
    let state_key = codec.commit_state_key(*transaction_id.as_uuid().as_bytes());
    loop {
        let transaction = match create_auxiliary_transaction(
            database,
            limits,
            deadline,
            &mut budget,
            metrics,
            transaction_id,
            CommitStatePhase::Terminalization,
        ) {
            Ok(transaction) => transaction,
            Err(_) => return false,
        };
        let state = match load_commit_state(
            &transaction,
            codec,
            &state_key,
            deadline,
            metrics,
            transaction_id,
            CommitStatePhase::Terminalization,
        )
        .await
        {
            Ok(state) => state,
            Err(error) if is_retryable_auxiliary_error(&error) && Instant::now() < deadline => {
                continue;
            }
            Err(_) => return false,
        };
        match decide_terminalization(state.as_ref(), reservation_token) {
            TerminalizationDecision::WriteNotCommitted => {
                transaction.set(&state_key, &codec.not_committed_value());
                match timeout_at(deadline, transaction.commit()).await {
                    Ok(Ok(committed)) => {
                        drop(committed);
                        return true;
                    }
                    Ok(Err(error)) => {
                        let error = *error;
                        let disposition = classify_native_commit_error(error);
                        record_native_error(
                            metrics,
                            transaction_id,
                            CommitStatePhase::Terminalization,
                            error,
                            disposition,
                        );
                        if should_retry_auxiliary_commit(error)
                            && budget.has_remaining()
                            && Instant::now() < deadline
                        {
                            continue;
                        }
                        return false;
                    }
                    Err(_) => {
                        record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
                        return false;
                    }
                }
            }
            TerminalizationDecision::AlreadyNotCommitted => return true,
            TerminalizationDecision::PreserveUnknown => return false,
        }
    }
}

async fn load_commit_state(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    state_key: &[u8],
    deadline: Instant,
    metrics: &StateStoreMetrics,
    transaction_id: TransactionId,
    phase: CommitStatePhase,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    let value = match timeout_at(deadline, transaction.get(state_key, false)).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            log_native_error(
                transaction_id,
                phase,
                error,
                classify_native_commit_error(error),
            );
            let error = classify_auxiliary_native_error(error);
            record_auxiliary_error(metrics, &error);
            return Err(error);
        }
        Err(_) => {
            record_auxiliary_metric(metrics, AuxiliaryMetricEvent::Deadline);
            return Err(deadline_error());
        }
    };
    value
        .map(|value| codec.decode_commit_state(value.as_ref()))
        .transpose()
}

fn create_auxiliary_transaction(
    database: &Database,
    limits: &StateStoreLimits,
    deadline: Instant,
    budget: &mut AuxiliaryAttemptBudget,
    metrics: &StateStoreMetrics,
    transaction_id: TransactionId,
    phase: CommitStatePhase,
) -> Result<Transaction, StateStoreError> {
    begin_auxiliary_attempt(budget, deadline, metrics)?;
    create_raw_transaction_with_observer(database, limits, deadline, |error| {
        log_native_error(
            transaction_id,
            phase,
            error,
            classify_native_commit_error(error),
        );
    })
    .inspect_err(|error| {
        record_auxiliary_error(metrics, error);
    })
}

fn auxiliary_deadline(public_deadline: Instant) -> Instant {
    public_deadline.min(Instant::now() + AUXILIARY_DEADLINE)
}

fn auxiliary_attempts_exhausted() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB auxiliary transaction attempt budget was exhausted",
    )
}

fn classify_reservation_read_error(
    error: StateStoreError,
    reservation_commit_unknown: bool,
    foreign_state_may_exist: bool,
) -> CommitOutcome {
    if foreign_state_may_exist {
        return CommitOutcome::CommitUnknown(error);
    }
    match error.kind() {
        StateStoreErrorKind::Transient
        | StateStoreErrorKind::ProviderUnavailable
        | StateStoreErrorKind::DeadlineExceeded => CommitOutcome::TransientBeforeCommit(error),
        _ if reservation_commit_unknown => CommitOutcome::CommitUnknown(error),
        _ => CommitOutcome::DefiniteFailure(error),
    }
}

fn committed(transaction_id: TransactionId, revision: [u8; REVISION_BYTES]) -> CommitOutcome {
    match receipt(transaction_id, revision) {
        Ok(receipt) => CommitOutcome::Committed(receipt),
        Err(error) => CommitOutcome::CommitUnknown(error),
    }
}

fn receipt(
    transaction_id: TransactionId,
    revision: [u8; REVISION_BYTES],
) -> Result<CommitReceipt, StateStoreError> {
    Ok(CommitReceipt {
        transaction_id,
        revision: revision_from_bytes(&revision)?,
    })
}

fn revision_from_bytes(value: &[u8]) -> Result<StoreRevision, StateStoreError> {
    if value.len() != REVISION_BYTES {
        return Err(StateStoreError::new(
            StateStoreErrorKind::Corruption,
            "FoundationDB commit revision is malformed",
        ));
    }
    StoreRevision::try_from(Bytes::copy_from_slice(value))
}

fn record_commit(metrics: &StateStoreMetrics, started: StdInstant, outcome: &CommitOutcome) {
    let metric = match outcome {
        CommitOutcome::Committed(_) => StateStoreOutcome::Success,
        CommitOutcome::Conflict(_) => StateStoreOutcome::Conflict,
        CommitOutcome::TransientBeforeCommit(_) => StateStoreOutcome::TransientBeforeCommit,
        CommitOutcome::DefiniteFailure(_) => StateStoreOutcome::DefiniteFailure,
        CommitOutcome::CommitUnknown(_) => StateStoreOutcome::CommitUnknown,
    };
    metrics.record_operation(StateStoreOperation::Commit, metric, started.elapsed());
}

fn conflict_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Conflict,
        "FoundationDB transaction conflicted",
    )
}

fn foreign_pending() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Conflict,
        "FoundationDB transaction id is owned by another pending commit",
    )
}

fn invalid_reuse() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "FoundationDB transaction id is durably not committed and cannot be reused",
    )
}

fn provider_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "FoundationDB durable commit state operation failed",
    )
}

fn provider_transient() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Transient,
        "FoundationDB commit reservation was not completed",
    )
}

fn deterministic_commit_error(code: i32) -> StateStoreError {
    let kind = match code {
        2101 | 2102 | 2103 | 2109 | 2110 => StateStoreErrorKind::LimitExceeded,
        2006 | 2007 => StateStoreErrorKind::InvalidConfiguration,
        _ => StateStoreErrorKind::InvalidRequest,
    };
    StateStoreError::new(
        kind,
        "FoundationDB rejected the transaction with a deterministic client or limit error",
    )
}

fn provider_unknown() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Transient,
        "FoundationDB transaction commit outcome is unknown",
    )
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB durable commit state deadline exceeded",
    )
}

fn deadline_unknown() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "FoundationDB transaction commit timed out after dispatch",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_state_machine_never_steals_foreign_pending() {
        let ours = [0x11; 16];
        let theirs = [0x22; 16];
        assert_eq!(decide_reservation(None, ours), ReservationDecision::Reserve);
        assert_eq!(
            decide_reservation(Some(&DurableCommitState::Pending(ours)), ours),
            ReservationDecision::Dispatch
        );
        assert_eq!(
            decide_reservation(Some(&DurableCommitState::Pending(theirs)), ours),
            ReservationDecision::ForeignPending
        );
        assert_eq!(
            decide_reservation(
                Some(&DurableCommitState::Committed([0x33; REVISION_BYTES])),
                ours
            ),
            ReservationDecision::ReturnCommitted([0x33; REVISION_BYTES])
        );
        assert_eq!(
            decide_reservation(Some(&DurableCommitState::NotCommitted), ours),
            ReservationDecision::ReturnNotCommitted
        );
    }

    #[test]
    fn reservation_reads_only_retry_genuine_pre_dispatch_transients() {
        for kind in [
            StateStoreErrorKind::Transient,
            StateStoreErrorKind::ProviderUnavailable,
            StateStoreErrorKind::DeadlineExceeded,
        ] {
            assert!(matches!(
                classify_reservation_read_error(StateStoreError::new(kind, "test"), false, false),
                CommitOutcome::TransientBeforeCommit(_)
            ));
            assert!(matches!(
                classify_reservation_read_error(StateStoreError::new(kind, "test"), true, false),
                CommitOutcome::TransientBeforeCommit(_)
            ));
        }
        for kind in [
            StateStoreErrorKind::Corruption,
            StateStoreErrorKind::InvalidConfiguration,
            StateStoreErrorKind::InvalidRequest,
            StateStoreErrorKind::LimitExceeded,
            StateStoreErrorKind::Internal,
        ] {
            assert!(matches!(
                classify_reservation_read_error(StateStoreError::new(kind, "test"), false, false),
                CommitOutcome::DefiniteFailure(_)
            ));
            assert!(matches!(
                classify_reservation_read_error(StateStoreError::new(kind, "test"), true, false),
                CommitOutcome::CommitUnknown(_)
            ));
        }
    }

    #[test]
    fn reservation_conflict_requires_authoritative_reload_before_safe_retry() {
        let ReservationCommitFailureDecision::Retry {
            reservation_may_exist,
            foreign_state_may_exist,
        } = decide_reservation_commit_failure(FdbError::from_code(1020), true)
        else {
            panic!("reservation conflict with reload budget must retry");
        };
        assert!(!reservation_may_exist);
        assert!(foreign_state_may_exist);
        assert!(matches!(
            classify_reservation_read_error(
                provider_error(),
                reservation_may_exist,
                foreign_state_may_exist
            ),
            CommitOutcome::CommitUnknown(ref error)
                if error.kind() == StateStoreErrorKind::ProviderUnavailable
        ));
        assert!(matches!(
            classify_reservation_read_error(
                deadline_error(),
                reservation_may_exist,
                foreign_state_may_exist
            ),
            CommitOutcome::CommitUnknown(ref error)
                if error.kind() == StateStoreErrorKind::DeadlineExceeded
        ));
    }

    #[test]
    fn pre_dispatch_failure_never_regresses_a_concurrent_durable_state() {
        assert_eq!(
            decide_pre_dispatch_failure(None),
            PreDispatchFailureDecision::PersistNotCommitted
        );
        assert_eq!(
            decide_pre_dispatch_failure(Some(&DurableCommitState::NotCommitted)),
            PreDispatchFailureDecision::RejectReuse
        );
        assert_eq!(
            decide_pre_dispatch_failure(Some(&DurableCommitState::Committed([0x44; 10]))),
            PreDispatchFailureDecision::ReturnCommitted([0x44; 10])
        );
        assert_eq!(
            decide_pre_dispatch_failure(Some(&DurableCommitState::Pending([0x55; 16]))),
            PreDispatchFailureDecision::PendingUnknown
        );
    }

    #[test]
    fn exhausted_reservation_conflict_fails_closed_without_widening_safe_transients() {
        let mut budget = AuxiliaryAttemptBudget::new();
        for _ in 0..AUXILIARY_MAX_ATTEMPTS {
            assert!(budget.try_consume());
        }
        assert!(!budget.has_remaining());

        assert!(matches!(
            decide_reservation_commit_failure(
                FdbError::from_code(1020),
                budget.has_remaining()
            ),
            ReservationCommitFailureDecision::Outcome(CommitOutcome::CommitUnknown(ref error))
                if error.kind() == StateStoreErrorKind::Conflict
        ));
        assert!(matches!(
            decide_reservation_commit_failure(
                FdbError::from_code(1007),
                budget.has_remaining()
            ),
            ReservationCommitFailureDecision::Outcome(
                CommitOutcome::TransientBeforeCommit(ref error)
            ) if error.kind() == StateStoreErrorKind::Transient
        ));
        assert!(matches!(
            decide_reservation_commit_failure(
                FdbError::from_code(1021),
                budget.has_remaining()
            ),
            ReservationCommitFailureDecision::Outcome(
                CommitOutcome::TransientBeforeCommit(ref error)
            ) if error.kind() == StateStoreErrorKind::Transient
        ));
        assert!(matches!(
            decide_reservation_commit_failure(FdbError::from_code(1007), true),
            ReservationCommitFailureDecision::Retry {
                reservation_may_exist: false,
                foreign_state_may_exist: false,
            }
        ));
    }

    #[test]
    fn native_data_commit_errors_fail_closed_after_dispatch() {
        assert_eq!(
            classify_native_commit_error(FdbError::from_code(1020)),
            NativeCommitDisposition::ConflictNotCommitted
        );
        let retryable_not_committed = FdbError::from_code(1007);
        assert!(retryable_not_committed.is_retryable_not_committed());
        assert_eq!(
            classify_native_commit_error(retryable_not_committed),
            NativeCommitDisposition::RetryableNotCommitted
        );
        for code in [
            2000, 2002, 2004, 2006, 2007, 2018, 2020, 2023, 2101, 2102, 2103, 2108, 2109, 2110,
        ] {
            assert_eq!(
                classify_native_commit_error(FdbError::from_code(code)),
                NativeCommitDisposition::DefiniteNotCommitted,
                "deterministic limit error {code} must be terminalized"
            );
        }
        for code in [1021, 1039, 1031, 9999] {
            assert_eq!(
                classify_native_commit_error(FdbError::from_code(code)),
                NativeCommitDisposition::Unknown,
                "error code {code} must fail closed"
            );
        }
    }

    #[test]
    fn auxiliary_attempt_budget_allows_exactly_five_raw_transactions() {
        let mut budget = AuxiliaryAttemptBudget::new();
        let metrics = StateStoreMetrics::new("foundationdb");
        let deadline = Instant::now() + Duration::from_secs(60);
        for attempt in 1..=AUXILIARY_MAX_ATTEMPTS {
            assert!(
                begin_auxiliary_attempt(&mut budget, deadline, &metrics).is_ok(),
                "attempt {attempt} must be available"
            );
        }
        let error = begin_auxiliary_attempt(&mut budget, deadline, &metrics)
            .expect_err("a sixth raw transaction must be rejected");
        assert_eq!(error.kind(), StateStoreErrorKind::ProviderUnavailable);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.retry_count, 5);
        assert_eq!(snapshot.deadline_count, 0);
    }

    #[test]
    fn auxiliary_deadline_is_terminal_and_counted_once_before_creation() {
        let mut budget = AuxiliaryAttemptBudget::new();
        let metrics = StateStoreMetrics::new("foundationdb");
        let error = begin_auxiliary_attempt(&mut budget, Instant::now(), &metrics)
            .expect_err("expired deadline must reject before raw transaction creation");
        assert_eq!(error.kind(), StateStoreErrorKind::DeadlineExceeded);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.retry_count, 0);
        assert_eq!(snapshot.deadline_count, 1);
    }

    #[test]
    fn auxiliary_state_errors_retry_only_operational_blockers() {
        for kind in [
            StateStoreErrorKind::Transient,
            StateStoreErrorKind::ProviderUnavailable,
            StateStoreErrorKind::DeadlineExceeded,
        ] {
            assert!(is_retryable_auxiliary_error(&StateStoreError::new(
                kind, "test"
            )));
        }
        for kind in [
            StateStoreErrorKind::Corruption,
            StateStoreErrorKind::InvalidConfiguration,
            StateStoreErrorKind::InvalidRequest,
            StateStoreErrorKind::LimitExceeded,
            StateStoreErrorKind::Internal,
        ] {
            assert!(!is_retryable_auxiliary_error(&StateStoreError::new(
                kind, "test"
            )));
        }
    }

    #[test]
    fn auxiliary_native_commits_retry_only_safe_operational_failures() {
        for code in [1020, 1007, 1021, 1039, 1031] {
            assert!(
                should_retry_auxiliary_commit(FdbError::from_code(code)),
                "auxiliary error {code} should retry"
            );
        }
        for code in [2101, 2102, 2103, 9999] {
            assert!(
                !should_retry_auxiliary_commit(FdbError::from_code(code)),
                "auxiliary error {code} must fail closed"
            );
        }
    }

    #[test]
    fn auxiliary_native_errors_preserve_deterministic_and_internal_boundaries() {
        assert_eq!(
            classify_auxiliary_native_error(FdbError::from_code(2101)).kind(),
            StateStoreErrorKind::LimitExceeded
        );
        assert_eq!(
            classify_auxiliary_native_error(FdbError::from_code(2006)).kind(),
            StateStoreErrorKind::InvalidConfiguration
        );
        assert_eq!(
            classify_auxiliary_native_error(FdbError::from_code(9999)).kind(),
            StateStoreErrorKind::Internal
        );
    }

    #[test]
    fn auxiliary_metric_events_are_counted_without_public_commit_duplication() {
        let metrics = StateStoreMetrics::new("foundationdb");
        record_auxiliary_metric(&metrics, AuxiliaryMetricEvent::Attempt);
        record_auxiliary_metric(&metrics, AuxiliaryMetricEvent::Attempt);
        record_auxiliary_metric(&metrics, AuxiliaryMetricEvent::Deadline);
        record_auxiliary_metric(&metrics, AuxiliaryMetricEvent::BlockingFailure);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.retry_count, 2);
        assert_eq!(snapshot.deadline_count, 1);
        assert_eq!(snapshot.blocking_failure_count, 1);
        assert_eq!(snapshot.commit_count, 0);
        assert_eq!(
            snapshot.operation_duration_observations(StateStoreOperation::Commit),
            0
        );
    }

    #[test]
    fn native_error_metrics_distinguish_deadline_blocking_and_expected_failures() {
        assert_eq!(
            native_error_metric_event(
                FdbError::from_code(1020),
                NativeCommitDisposition::ConflictNotCommitted
            ),
            None
        );
        assert_eq!(
            native_error_metric_event(
                FdbError::from_code(1007),
                NativeCommitDisposition::RetryableNotCommitted
            ),
            Some(AuxiliaryMetricEvent::BlockingFailure)
        );
        assert_eq!(
            native_error_metric_event(FdbError::from_code(1031), NativeCommitDisposition::Unknown),
            Some(AuxiliaryMetricEvent::Deadline)
        );
        assert_eq!(
            native_error_metric_event(
                FdbError::from_code(2101),
                NativeCommitDisposition::DefiniteNotCommitted
            ),
            None
        );
    }

    #[test]
    fn native_error_log_fields_are_structured_phase_complete_and_secret_free() {
        let transaction_id = TransactionId::from(Uuid::from_bytes([0x5a; 16]));
        let fields = native_error_log_fields(
            transaction_id,
            CommitStatePhase::DataCommit,
            FdbError::from_code(1007),
            NativeCommitDisposition::RetryableNotCommitted,
        );
        assert_eq!(fields.transaction_id, transaction_id.as_uuid().to_string());
        assert_eq!(fields.phase, "data_commit");
        assert_eq!(fields.native_error_code, 1007);
        assert_eq!(fields.category, "retryable_not_committed");

        assert_eq!(
            CommitStatePhase::ALL.map(CommitStatePhase::as_str),
            [
                "reservation",
                "data_commit",
                "versionstamp",
                "pre_dispatch_tombstone",
                "terminalization",
                "resolve",
            ]
        );

        let source = include_str!("commit.rs");
        let helper = source
            .split("fn log_native_error")
            .nth(1)
            .expect("structured native error log helper")
            .split("\nfn ")
            .next()
            .expect("helper body");
        for required in ["transaction_id", "phase", "native_error_code", "category"] {
            assert!(helper.contains(required), "missing log field {required}");
        }
        for forbidden in [
            "logical_key",
            "logical_value",
            "cluster_file",
            "tls",
            "path",
            "secret",
        ] {
            assert!(
                !helper.contains(forbidden),
                "sensitive field {forbidden} must not enter commit-state logs"
            );
        }

        let txn_source = include_str!("txn.rs");
        assert!(
            txn_source.contains("fn create_raw_transaction_with_observer"),
            "raw transaction creation must expose native failures to commit-state logging"
        );
        assert!(
            txn_source.contains("create_raw_transaction_with_observer(")
                && txn_source.contains("|_| {}"),
            "ordinary state transactions must retain a no-op native-error observer"
        );
        let raw_helper = txn_source
            .split("fn create_raw_transaction_with_observer")
            .nth(1)
            .expect("raw transaction observer helper")
            .split("impl FoundationDbReadTransaction")
            .next()
            .expect("raw transaction helper body");
        assert!(
            raw_helper
                .matches("classify_native_read_error(error)")
                .count()
                >= 4,
            "raw create/options errors must preserve native fail-closed classification"
        );
        assert!(
            !raw_helper.contains("provider_error()"),
            "raw create/options must not flatten deterministic native errors into provider failures"
        );
        assert!(
            source.contains("create_raw_transaction_with_observer(")
                && source.contains("log_native_error(transaction_id, phase"),
            "commit-state raw creation must preserve transaction and phase context"
        );
    }

    #[test]
    fn terminalization_only_writes_a_matching_pending_token() {
        let ours = [0x66; 16];
        assert_eq!(
            decide_terminalization(Some(&DurableCommitState::Pending(ours)), ours),
            TerminalizationDecision::WriteNotCommitted
        );
        assert_eq!(
            decide_terminalization(Some(&DurableCommitState::NotCommitted), ours),
            TerminalizationDecision::AlreadyNotCommitted
        );
        for state in [
            None,
            Some(DurableCommitState::Pending([0x77; 16])),
            Some(DurableCommitState::Committed([0x88; REVISION_BYTES])),
        ] {
            assert_eq!(
                decide_terminalization(state.as_ref(), ours),
                TerminalizationDecision::PreserveUnknown
            );
        }
    }
}
