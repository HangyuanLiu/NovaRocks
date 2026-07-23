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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::BindingId;
use crate::runtime_filter::port::final_domain::{
    CompletionFenceAuthority, FinalDomainFreezeCapability, FrozenFinalDomainPayload,
};
use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence};
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, ProducerFailureReason, RuntimeContractViolation,
    RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::value_domain::ValueDomainDelta;

pub(crate) struct FinalDomainCompletionSession {
    inner: Arc<FinalDomainCompletionSessionInner>,
    _owner_lease: FinalDomainCompletionOwnerLease,
}

impl fmt::Debug for FinalDomainCompletionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FinalDomainCompletionSession")
    }
}

pub(crate) struct FinalDomainPartitionCommitter {
    inner: Arc<FinalDomainCompletionSessionInner>,
    partition_id: PartitionId,
    capability: Option<FinalDomainFreezeCapability>,
    closed: bool,
}

impl fmt::Debug for FinalDomainPartitionCommitter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FinalDomainPartitionCommitter")
            .field("partition_id", &self.partition_id)
            .field("sealed", &self.capability.is_none())
            .field("closed", &self.closed)
            .finish()
    }
}

pub(super) struct FinalDomainCompletionSessionWeak(Weak<FinalDomainCompletionSessionInner>);

#[derive(Default)]
pub(super) struct FinalDomainCompletionSessionRegistry {
    sessions: Mutex<BTreeMap<(BindingId, UniqueId), FinalDomainCompletionSessionWeak>>,
}

struct FinalDomainCompletionOwnerLease {
    inner: Arc<FinalDomainCompletionSessionInner>,
}

struct FinalDomainCompletionSessionInner {
    producer: Arc<dyn FinalDomainProducerAdapter>,
    operation: Mutex<()>,
    state: Mutex<FinalDomainCompletionState>,
}

struct FinalDomainCompletionState {
    lifecycle: FinalDomainCompletionLifecycle,
    authority: Option<CompletionFenceAuthority>,
    partitions: Vec<FinalDomainPartitionState>,
    fail_sent: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FinalDomainCompletionLifecycle {
    Collecting,
    Issuing,
    Completed,
    Failed,
}

struct FinalDomainPartitionState {
    claimed: bool,
    capability: Option<FinalDomainFreezeCapability>,
    payload: Option<FrozenFinalDomainPayload>,
    closed: bool,
}

impl FinalDomainCompletionSession {
    pub(super) fn new(
        authority: CompletionFenceAuthority,
        producer: Arc<dyn FinalDomainProducerAdapter>,
        partition_count: u32,
    ) -> Result<Self, RuntimeContractViolation> {
        if partition_count == 0 {
            return Err(violation(
                RuntimeContractViolationKind::InvalidPartitionCount,
                "a final-domain completion session requires at least one partition",
            ));
        }
        let partitions = (0..partition_count)
            .map(|partition| FinalDomainPartitionState {
                claimed: false,
                capability: Some(authority.freeze_capability(PartitionId::new(partition))),
                payload: None,
                closed: false,
            })
            .collect();
        let inner = Arc::new(FinalDomainCompletionSessionInner {
            producer,
            operation: Mutex::new(()),
            state: Mutex::new(FinalDomainCompletionState {
                lifecycle: FinalDomainCompletionLifecycle::Collecting,
                authority: Some(authority),
                partitions,
                fail_sent: false,
            }),
        });
        Ok(Self {
            inner: Arc::clone(&inner),
            _owner_lease: FinalDomainCompletionOwnerLease { inner },
        })
    }

    pub(crate) fn partition(
        &self,
        partition_id: PartitionId,
    ) -> Result<FinalDomainPartitionCommitter, RuntimeContractViolation> {
        let result = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != FinalDomainCompletionLifecycle::Collecting {
                return Err(session_unavailable());
            }
            let Some(partition) = state.partitions.get_mut(partition_id.get() as usize) else {
                drop(state);
                self.inner.fail_contract();
                return Err(violation(
                    RuntimeContractViolationKind::InvalidPartition,
                    "final-domain partition is outside the declared local partition set",
                ));
            };
            if partition.claimed {
                Err(violation(
                    RuntimeContractViolationKind::ConflictingReplay,
                    "final-domain partition committer was already created",
                ))
            } else {
                partition.claimed = true;
                Ok(partition
                    .capability
                    .take()
                    .expect("an unclaimed partition owns its freeze capability"))
            }
        };
        match result {
            Ok(capability) => Ok(FinalDomainPartitionCommitter {
                inner: Arc::clone(&self.inner),
                partition_id,
                capability: Some(capability),
                closed: false,
            }),
            Err(error) => {
                self.inner.fail_contract();
                Err(error)
            }
        }
    }

    pub(crate) fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.inner.fail_once(reason)
    }

    pub(super) fn weak(&self) -> FinalDomainCompletionSessionWeak {
        FinalDomainCompletionSessionWeak(Arc::downgrade(&self.inner))
    }
}

impl FinalDomainPartitionCommitter {
    pub(crate) fn seal(
        &mut self,
        domain: ValueDomainDelta,
    ) -> Result<(), RuntimeContractViolation> {
        if self.closed || self.capability.is_none() {
            self.inner.fail_contract();
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "final-domain partition may be sealed exactly once before close",
            ));
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != FinalDomainCompletionLifecycle::Collecting {
            return Err(session_unavailable());
        }
        let partition = &mut state.partitions[self.partition_id.get() as usize];
        if partition.payload.is_some() || partition.closed {
            drop(state);
            self.inner.fail_contract();
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "final-domain partition may be sealed exactly once before close",
            ));
        }
        let capability = self
            .capability
            .take()
            .expect("an unsealed partition committer owns its capability");
        partition.payload = Some(capability.seal(domain));
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<(), RuntimeContractViolation> {
        if self.closed {
            self.inner.fail_contract();
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "final-domain partition committer may close exactly once",
            ));
        }
        let terminal = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != FinalDomainCompletionLifecycle::Collecting {
                return Err(session_unavailable());
            }
            let partition = &mut state.partitions[self.partition_id.get() as usize];
            if partition.payload.is_none() {
                drop(state);
                self.inner.fail_contract();
                return Err(violation(
                    RuntimeContractViolationKind::FinalDomainMissing,
                    "final-domain partition must be sealed before close",
                ));
            }
            partition.closed = true;
            self.closed = true;
            let terminal = state
                .partitions
                .iter()
                .all(|partition| partition.payload.is_some() && partition.closed);
            if terminal {
                state.lifecycle = FinalDomainCompletionLifecycle::Issuing;
            }
            terminal
        };
        if terminal {
            self.inner.issue_all()
        } else {
            Ok(())
        }
    }
}

impl FinalDomainCompletionSessionRegistry {
    pub(super) fn ensure_vacant(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<(), RuntimeContractViolation> {
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if sessions
            .get(&(binding_id, fragment_instance_id))
            .is_some_and(|existing| existing.0.upgrade().is_some())
        {
            return Err(session_already_open());
        }
        Ok(())
    }

    pub(super) fn register(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        session: FinalDomainCompletionSessionWeak,
    ) -> Result<(), RuntimeContractViolation> {
        let key = (binding_id, fragment_instance_id);
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if sessions
            .get(&key)
            .is_some_and(|existing| existing.0.upgrade().is_some())
        {
            return Err(session_already_open());
        }
        sessions.insert(key, session);
        Ok(())
    }
}

impl FinalDomainCompletionSessionInner {
    fn fail_contract(&self) {
        let _ = self.fail_once(ProducerFailureReason::ExecutionFailed);
    }

    fn fail_once(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.fail_while_holding_operation(reason)
    }

    fn fail_while_holding_operation(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let should_send = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(
                state.lifecycle,
                FinalDomainCompletionLifecycle::Completed | FinalDomainCompletionLifecycle::Failed
            ) {
                false
            } else {
                state.lifecycle = FinalDomainCompletionLifecycle::Failed;
                if state.fail_sent {
                    false
                } else {
                    state.fail_sent = true;
                    true
                }
            }
        };
        if should_send {
            self.producer.fail(reason)
        } else {
            Ok(SubmitOutcome::TerminalNoop)
        }
    }

    fn issue_all(&self) -> Result<(), RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (authority, payloads) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != FinalDomainCompletionLifecycle::Issuing {
                return Err(session_unavailable());
            }
            let authority = state
                .authority
                .take()
                .expect("the terminal transition owns the completion authority");
            let payloads = state
                .partitions
                .iter_mut()
                .map(|partition| {
                    partition
                        .payload
                        .take()
                        .expect("the terminal transition owns every frozen payload")
                })
                .collect::<Vec<_>>();
            (authority, payloads)
        };
        let issuer = match authority.freeze(&payloads) {
            Ok(issuer) => issuer,
            Err(error) => {
                let violation = violation(
                    RuntimeContractViolationKind::TypeMismatch,
                    error.to_string(),
                );
                let _ = self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                return Err(violation);
            }
        };
        for payload in payloads {
            let partition_id = payload.partition_id();
            let shard = match issuer.issue(payload, ProducerSequence::new(0)) {
                Ok(shard) => shard,
                Err(error) => {
                    let violation = violation(
                        RuntimeContractViolationKind::TypeMismatch,
                        error.to_string(),
                    );
                    let _ =
                        self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                    return Err(violation);
                }
            };
            if let Err(error) =
                self.producer
                    .complete(partition_id, ProducerSequence::new(0), shard)
            {
                let _ = self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
            if let Err(error) = self
                .producer
                .close_partition(partition_id, ProducerSequence::new(1))
            {
                let _ = self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.lifecycle == FinalDomainCompletionLifecycle::Issuing {
            state.lifecycle = FinalDomainCompletionLifecycle::Completed;
            Ok(())
        } else {
            Err(session_unavailable())
        }
    }
}

impl Drop for FinalDomainCompletionOwnerLease {
    fn drop(&mut self) {
        let all_claimed = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.partitions.iter().all(|partition| partition.claimed)
        };
        if !all_claimed {
            self.inner.fail_contract();
        }
    }
}

impl Drop for FinalDomainPartitionCommitter {
    fn drop(&mut self) {
        if !self.closed {
            self.inner.fail_contract();
        }
    }
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

fn session_unavailable() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::ServiceUnavailable,
        "final-domain completion session is not collecting",
    )
}

fn session_already_open() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::ConflictingReplay,
        "a final-domain completion session is already open for this producer instance",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, CompletionFenceKind, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::final_domain::{
        CompletionFenceAuthority, FinalDomainShard, RuntimeCompletionFenceContract,
    };
    use crate::runtime_filter::port::identity::{DeploymentEpoch, PartitionId, ProducerSequence};
    use crate::runtime_filter::port::producer::{
        FinalDomainProducerAdapter, ProducerFailureReason, RuntimeContractViolation,
        RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

    use super::*;

    const BINDING: BindingId = BindingId::new(7);
    const INSTANCE: UniqueId = UniqueId { hi: 8, lo: 9 };

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum AdapterCall {
        Complete(PartitionId, ProducerSequence),
        Close(PartitionId, ProducerSequence),
        Fail(ProducerFailureReason),
    }

    struct RecordingAdapter {
        calls: Mutex<Vec<AdapterCall>>,
        complete_calls: AtomicUsize,
        fail_complete_call: Option<usize>,
    }

    impl RecordingAdapter {
        fn new(fail_complete_call: Option<usize>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                complete_calls: AtomicUsize::new(0),
                fail_complete_call,
            }
        }

        fn calls(&self) -> Vec<AdapterCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl FinalDomainProducerAdapter for RecordingAdapter {
        fn complete(
            &self,
            partition_id: PartitionId,
            sequence: ProducerSequence,
            _shard: FinalDomainShard,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.calls
                .lock()
                .unwrap()
                .push(AdapterCall::Complete(partition_id, sequence));
            let call = self.complete_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_complete_call == Some(call) {
                return Err(RuntimeContractViolation::new(
                    RuntimeContractViolationKind::ServiceUnavailable,
                    "injected final-domain submit failure",
                ));
            }
            Ok(SubmitOutcome::Applied)
        }

        fn close_partition(
            &self,
            partition_id: PartitionId,
            terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.calls
                .lock()
                .unwrap()
                .push(AdapterCall::Close(partition_id, terminal_sequence));
            Ok(SubmitOutcome::Applied)
        }

        fn fail(
            &self,
            reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.calls.lock().unwrap().push(AdapterCall::Fail(reason));
            Ok(SubmitOutcome::Applied)
        }
    }

    fn authority() -> CompletionFenceAuthority {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NullSafeEqual).unwrap();
        let contract = Arc::new(
            RuntimeCompletionFenceContract::try_from_install(
                UniqueId { hi: 1, lo: 2 },
                DeploymentEpoch::new(3),
                ChannelId::new(4),
                CompletionFenceKind::CommittedDomainFrozen,
                &schema,
            )
            .unwrap(),
        );
        CompletionFenceAuthority::try_new(contract, BINDING, INSTANCE).unwrap()
    }

    fn domain(value: i64) -> ValueDomainDelta {
        ValueDomainDelta::new(MembershipValues::int64([value]), false)
    }

    fn session(
        partition_count: u32,
        fail_complete_call: Option<usize>,
    ) -> (FinalDomainCompletionSession, Arc<RecordingAdapter>) {
        let adapter = Arc::new(RecordingAdapter::new(fail_complete_call));
        let typed: Arc<dyn FinalDomainProducerAdapter> = adapter.clone();
        (
            FinalDomainCompletionSession::new(authority(), typed, partition_count).unwrap(),
            adapter,
        )
    }

    #[test]
    fn collecting_completion_session_cannot_issue() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();
        let _partition_1 = session.partition(PartitionId::new(1)).unwrap();

        partition_0.seal(domain(10)).unwrap();
        partition_0.close().unwrap();

        assert!(adapter.calls().is_empty());
    }

    #[test]
    fn partition_must_freeze_before_close() {
        let (session, adapter) = session(1, None);
        let mut partition = session.partition(PartitionId::new(0)).unwrap();

        let error = partition.close().unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::FinalDomainMissing
        );
        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn last_local_partition_enables_issuance() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();
        let mut partition_1 = session.partition(PartitionId::new(1)).unwrap();

        partition_1.seal(domain(11)).unwrap();
        partition_1.close().unwrap();
        assert!(adapter.calls().is_empty());

        partition_0.seal(domain(10)).unwrap();
        partition_0.close().unwrap();
        assert_eq!(
            adapter.calls(),
            vec![
                AdapterCall::Complete(PartitionId::new(0), ProducerSequence::new(0)),
                AdapterCall::Close(PartitionId::new(0), ProducerSequence::new(1)),
                AdapterCall::Complete(PartitionId::new(1), ProducerSequence::new(0)),
                AdapterCall::Close(PartitionId::new(1), ProducerSequence::new(1)),
            ]
        );
    }

    #[test]
    fn unknown_or_duplicate_partition_is_contract_violation() {
        let (unknown_session, unknown_adapter) = session(2, None);
        let unknown = unknown_session.partition(PartitionId::new(2)).unwrap_err();
        assert_eq!(
            unknown.kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(
            unknown_adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );

        let (duplicate_session, duplicate_adapter) = session(2, None);
        let _partition = duplicate_session.partition(PartitionId::new(0)).unwrap();
        let duplicate = duplicate_session
            .partition(PartitionId::new(0))
            .unwrap_err();
        assert_eq!(
            duplicate.kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        assert_eq!(
            duplicate_adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn same_binding_instance_cannot_open_two_sessions() {
        let fixture = super::super::tests::fixture();
        fixture
            .service
            .install(super::super::tests::compiled_fenced_final_install())
            .unwrap();
        let _first = fixture
            .service
            .open_final_aggregate_producer(BindingId::new(10), UniqueId { hi: 70, lo: 10 }, 1)
            .unwrap();

        let error = fixture
            .service
            .open_final_aggregate_producer(BindingId::new(10), UniqueId { hi: 70, lo: 10 }, 1)
            .unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
    }

    #[test]
    fn failed_session_cannot_publish_late_partitions() {
        let (session, adapter) = session(1, None);
        let mut partition = session.partition(PartitionId::new(0)).unwrap();

        session
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap();
        let error = partition.seal(domain(10)).unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ServiceUnavailable
        );
        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn owner_drop_before_all_partition_committers_are_created_fails_session() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();

        drop(session);

        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
        let error = partition_0.seal(domain(10)).unwrap_err();
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ServiceUnavailable
        );
    }

    #[test]
    fn nth_partition_submit_failure_stops_and_fails_without_materializing_subset() {
        let (session, adapter) = session(3, Some(2));
        let mut partitions = [
            session.partition(PartitionId::new(0)).unwrap(),
            session.partition(PartitionId::new(1)).unwrap(),
            session.partition(PartitionId::new(2)).unwrap(),
        ];
        for (partition, value) in partitions.iter_mut().zip([10, 11, 12]) {
            partition.seal(domain(value)).unwrap();
        }
        partitions[0].close().unwrap();
        partitions[1].close().unwrap();

        let error = partitions[2].close().unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ServiceUnavailable
        );
        assert_eq!(
            adapter.calls(),
            vec![
                AdapterCall::Complete(PartitionId::new(0), ProducerSequence::new(0)),
                AdapterCall::Close(PartitionId::new(0), ProducerSequence::new(1)),
                AdapterCall::Complete(PartitionId::new(1), ProducerSequence::new(0)),
                AdapterCall::Fail(ProducerFailureReason::ExecutionFailed),
            ]
        );
    }
}
