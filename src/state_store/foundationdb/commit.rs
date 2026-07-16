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
use super::txn::create_raw_transaction;
use crate::state_store::runtime::OperationHandle;
use crate::state_store::{
    CommitOutcome, CommitReceipt, CommitResolution, StateStoreError, StateStoreErrorKind,
    StateStoreLimits, StateStoreMetrics, StateStoreOperation, StateStoreOutcome, StoreRevision,
    TransactionId,
};

const AUXILIARY_MAX_ATTEMPTS: usize = 5;
const AUXILIARY_DEADLINE: Duration = Duration::from_secs(4);
const NOT_COMMITTED_ERROR_CODE: i32 = 1020;

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
    ReturnLocal,
    ReturnCommitted([u8; REVISION_BYTES]),
    PendingUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeCommitDisposition {
    ConflictNotCommitted,
    RetryableNotCommitted,
    Unknown,
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
    } else {
        NativeCommitDisposition::Unknown
    }
}

fn decide_pre_dispatch_failure(
    observed: Option<&DurableCommitState>,
) -> PreDispatchFailureDecision {
    match observed {
        None => PreDispatchFailureDecision::PersistNotCommitted,
        Some(DurableCommitState::NotCommitted) => PreDispatchFailureDecision::ReturnLocal,
        Some(DurableCommitState::Committed(revision)) => {
            PreDispatchFailureDecision::ReturnCommitted(*revision)
        }
        Some(DurableCommitState::Pending(_)) => PreDispatchFailureDecision::PendingUnknown,
    }
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
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let outcome = run_commit_owner(prepared).await;
        record_commit(&metrics, started, &outcome);
        let _ = sender.send(outcome);
    });
    receiver.await.unwrap_or_else(|_| {
        CommitOutcome::CommitUnknown(StateStoreError::new(
            StateStoreErrorKind::Internal,
            "FoundationDB commit supervisor stopped before reporting an outcome",
        ))
    })
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
    let state_key = prepared
        .codec
        .commit_state_key(*prepared.transaction_id.as_uuid().as_bytes());
    for attempt in 0..AUXILIARY_MAX_ATTEMPTS {
        if attempt > 0 {
            prepared.metrics.record_retry();
        }
        let transaction =
            match create_raw_transaction(prepared.database.as_ref(), &prepared.limits, deadline) {
                Ok(transaction) => transaction,
                Err(error) => return CommitOutcome::CommitUnknown(error),
            };
        let state =
            match load_commit_state(&transaction, &prepared.codec, &state_key, deadline).await {
                Ok(state) => state,
                Err(error) => return CommitOutcome::CommitUnknown(error),
            };
        match decide_pre_dispatch_failure(state.as_ref()) {
            PreDispatchFailureDecision::ReturnCommitted(revision) => {
                return committed(prepared.transaction_id, revision);
            }
            PreDispatchFailureDecision::PendingUnknown => {
                return CommitOutcome::CommitUnknown(foreign_pending());
            }
            PreDispatchFailureDecision::ReturnLocal => return local_outcome,
            PreDispatchFailureDecision::PersistNotCommitted => {
                transaction.set(&state_key, &prepared.codec.not_committed_value());
                match timeout_at(deadline, transaction.commit()).await {
                    Ok(Ok(committed)) => {
                        drop(committed);
                        return local_outcome;
                    }
                    Ok(Err(error))
                        if (error.code() == NOT_COMMITTED_ERROR_CODE
                            || error.is_retryable_not_committed())
                            && attempt + 1 < AUXILIARY_MAX_ATTEMPTS
                            && Instant::now() < deadline =>
                    {
                        continue;
                    }
                    Ok(Err(_)) | Err(_) => {
                        match authoritative_state(
                            prepared.database.as_ref(),
                            &prepared.codec,
                            &prepared.limits,
                            &state_key,
                            deadline,
                        )
                        .await
                        {
                            Ok(Some(DurableCommitState::Committed(revision))) => {
                                return committed(prepared.transaction_id, revision);
                            }
                            Ok(Some(DurableCommitState::NotCommitted)) => return local_outcome,
                            Ok(Some(DurableCommitState::Pending(_))) => {
                                return CommitOutcome::CommitUnknown(foreign_pending());
                            }
                            Ok(None) => {
                                return CommitOutcome::CommitUnknown(provider_unknown());
                            }
                            Err(error) => return CommitOutcome::CommitUnknown(error),
                        }
                    }
                }
            }
        }
    }
    CommitOutcome::CommitUnknown(provider_unknown())
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
                Ok(Err(_)) => CommitOutcome::CommitUnknown(provider_unknown()),
                Err(_) => CommitOutcome::CommitUnknown(deadline_unknown()),
            }
        }
        Ok(Err(error)) => {
            let error = *error;
            let disposition = classify_native_commit_error(error);
            match disposition {
                NativeCommitDisposition::ConflictNotCommitted
                | NativeCommitDisposition::RetryableNotCommitted => {
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
                    } else {
                        CommitOutcome::TransientBeforeCommit(provider_transient())
                    }
                }
                NativeCommitDisposition::Unknown => {
                    CommitOutcome::CommitUnknown(provider_unknown())
                }
            }
        }
        Err(_) => CommitOutcome::CommitUnknown(deadline_unknown()),
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
    let state_key = codec.commit_state_key(*transaction_id.as_uuid().as_bytes());
    for attempt in 0..AUXILIARY_MAX_ATTEMPTS {
        if attempt > 0 {
            metrics.record_retry();
        }
        let transaction = match create_raw_transaction(database, limits, deadline) {
            Ok(transaction) => transaction,
            Err(error) => {
                return ReservationResult::Outcome(classify_reservation_read_error(error, false));
            }
        };
        let observed = match load_commit_state(&transaction, codec, &state_key, deadline).await {
            Ok(observed) => observed,
            Err(error) => {
                let outcome = classify_reservation_read_error(error, false);
                if matches!(outcome, CommitOutcome::TransientBeforeCommit(_))
                    && attempt + 1 < AUXILIARY_MAX_ATTEMPTS
                    && Instant::now() < deadline
                {
                    continue;
                }
                return ReservationResult::Outcome(outcome);
            }
        };
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
                    Ok(Err(error))
                        if (error.code() == NOT_COMMITTED_ERROR_CODE
                            || error.is_retryable_not_committed())
                            && attempt + 1 < AUXILIARY_MAX_ATTEMPTS
                            && Instant::now() < deadline =>
                    {
                        continue;
                    }
                    Ok(Err(_)) | Err(_) => {
                        match authoritative_state(database, codec, limits, &state_key, deadline)
                            .await
                        {
                            Ok(Some(DurableCommitState::Pending(token)))
                                if token == reservation_token =>
                            {
                                return ReservationResult::Dispatch;
                            }
                            Ok(Some(DurableCommitState::Committed(revision))) => {
                                return ReservationResult::Committed(revision);
                            }
                            Ok(Some(DurableCommitState::NotCommitted)) => {
                                return ReservationResult::Outcome(CommitOutcome::DefiniteFailure(
                                    invalid_reuse(),
                                ));
                            }
                            Ok(Some(DurableCommitState::Pending(_))) => {
                                return ReservationResult::Outcome(CommitOutcome::CommitUnknown(
                                    foreign_pending(),
                                ));
                            }
                            Ok(None)
                                if attempt + 1 < AUXILIARY_MAX_ATTEMPTS
                                    && Instant::now() < deadline =>
                            {
                                continue;
                            }
                            Ok(None) => {
                                return ReservationResult::Outcome(
                                    CommitOutcome::TransientBeforeCommit(provider_transient()),
                                );
                            }
                            Err(error) => {
                                return ReservationResult::Outcome(
                                    classify_reservation_read_error(error, true),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    ReservationResult::Outcome(CommitOutcome::TransientBeforeCommit(provider_transient()))
}

pub(super) async fn resolve_commit(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    transaction_id: TransactionId,
    metrics: &StateStoreMetrics,
) -> Result<CommitResolution, StateStoreError> {
    let deadline = Instant::now() + limits.transaction_deadline.min(AUXILIARY_DEADLINE);
    let state_key = codec.commit_state_key(*transaction_id.as_uuid().as_bytes());
    for attempt in 0..AUXILIARY_MAX_ATTEMPTS {
        if attempt > 0 {
            metrics.record_retry();
        }
        let transaction = create_raw_transaction(database, limits, deadline)?;
        let state = load_commit_state(&transaction, codec, &state_key, deadline).await?;
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
                    Ok(Err(error))
                        if (error.code() == NOT_COMMITTED_ERROR_CODE
                            || error.is_retryable_not_committed())
                            && attempt + 1 < AUXILIARY_MAX_ATTEMPTS
                            && Instant::now() < deadline =>
                    {
                        continue;
                    }
                    Ok(Err(_)) => return Err(provider_error()),
                    Err(_) => return Err(deadline_error()),
                }
            }
        }
    }
    Err(provider_error())
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
    let state_key = codec.commit_state_key(*transaction_id.as_uuid().as_bytes());
    for attempt in 0..AUXILIARY_MAX_ATTEMPTS {
        if attempt > 0 {
            metrics.record_retry();
        }
        let Ok(transaction) = create_raw_transaction(database, limits, deadline) else {
            return false;
        };
        let Ok(state) = load_commit_state(&transaction, codec, &state_key, deadline).await else {
            continue;
        };
        match decide_terminalization(state.as_ref(), reservation_token) {
            TerminalizationDecision::WriteNotCommitted => {
                transaction.set(&state_key, &codec.not_committed_value());
                match timeout_at(deadline, transaction.commit()).await {
                    Ok(Ok(committed)) => {
                        drop(committed);
                        return true;
                    }
                    Ok(Err(error))
                        if (error.code() == NOT_COMMITTED_ERROR_CODE
                            || error.is_retryable_not_committed())
                            && attempt + 1 < AUXILIARY_MAX_ATTEMPTS =>
                    {
                        continue;
                    }
                    _ => return false,
                }
            }
            TerminalizationDecision::AlreadyNotCommitted => return true,
            TerminalizationDecision::PreserveUnknown => return false,
        }
    }
    false
}

async fn authoritative_state(
    database: &Database,
    codec: &KeyspaceCodec,
    limits: &StateStoreLimits,
    state_key: &[u8],
    deadline: Instant,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    let transaction = create_raw_transaction(database, limits, deadline)?;
    load_commit_state(&transaction, codec, state_key, deadline).await
}

async fn load_commit_state(
    transaction: &Transaction,
    codec: &KeyspaceCodec,
    state_key: &[u8],
    deadline: Instant,
) -> Result<Option<DurableCommitState>, StateStoreError> {
    let value = timeout_at(deadline, transaction.get(state_key, false))
        .await
        .map_err(|_| deadline_error())?
        .map_err(|_| provider_error())?;
    value
        .map(|value| codec.decode_commit_state(value.as_ref()))
        .transpose()
}

fn auxiliary_deadline(public_deadline: Instant) -> Instant {
    public_deadline.min(Instant::now() + AUXILIARY_DEADLINE)
}

fn classify_reservation_read_error(
    error: StateStoreError,
    reservation_commit_unknown: bool,
) -> CommitOutcome {
    if reservation_commit_unknown {
        return CommitOutcome::CommitUnknown(error);
    }
    match error.kind() {
        StateStoreErrorKind::Transient
        | StateStoreErrorKind::ProviderUnavailable
        | StateStoreErrorKind::DeadlineExceeded => CommitOutcome::TransientBeforeCommit(error),
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
                classify_reservation_read_error(StateStoreError::new(kind, "test"), false),
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
                classify_reservation_read_error(StateStoreError::new(kind, "test"), false),
                CommitOutcome::DefiniteFailure(_)
            ));
            assert!(matches!(
                classify_reservation_read_error(StateStoreError::new(kind, "test"), true),
                CommitOutcome::CommitUnknown(_)
            ));
        }
    }

    #[test]
    fn pre_dispatch_failure_never_regresses_a_concurrent_durable_state() {
        assert_eq!(
            decide_pre_dispatch_failure(None),
            PreDispatchFailureDecision::PersistNotCommitted
        );
        assert_eq!(
            decide_pre_dispatch_failure(Some(&DurableCommitState::NotCommitted)),
            PreDispatchFailureDecision::ReturnLocal
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
        for code in [1021, 1039, 1031, 9999] {
            assert_eq!(
                classify_native_commit_error(FdbError::from_code(code)),
                NativeCommitDisposition::Unknown,
                "error code {code} must fail closed"
            );
        }
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
