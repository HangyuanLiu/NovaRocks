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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::runtime_filter::core::channel::ChannelAction;
use crate::runtime_filter::model::contract::*;
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerArtifactProfile};
use crate::runtime_filter::port::events::{
    FinalDomainRejectionKind, RuntimeFilterEvent, RuntimeFilterEventSink,
};
use crate::runtime_filter::port::final_domain::{
    FinalDomainTestIssuerTransition, FrozenFinalDomainTestIssuer,
};
use crate::runtime_filter::port::identity::*;
use crate::runtime_filter::port::install::*;
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, ProducerHandle, ProducerPortKind, RuntimeContractViolationKind,
    SubmitOutcome,
};
use crate::runtime_filter::port::subscription::{LivePollOutcome, LiveTerminal, SubscriptionKind};
use crate::runtime_filter::port::support::{
    MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const CHANNEL: ChannelId = ChannelId::new(1);
const PRODUCER_A: BindingId = BindingId::new(10);
const PRODUCER_B: BindingId = BindingId::new(20);
const CONSUMER: BindingId = BindingId::new(30);

fn uid(lo: i64) -> UniqueId {
    UniqueId { hi: 91, lo }
}

struct Clock(Instant);

impl RuntimeFilterClock for Clock {
    fn now(&self) -> Instant {
        self.0
    }
}

#[derive(Default)]
struct Events(Mutex<Vec<RuntimeFilterEvent>>);

impl Events {
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    fn snapshot(&self) -> Vec<RuntimeFilterEvent> {
        self.0.lock().unwrap().clone()
    }
}

impl RuntimeFilterEventSink for Events {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[derive(Default)]
struct ArmableMemoryAccount {
    rejecting: AtomicBool,
    current: AtomicUsize,
}

impl RuntimeFilterMemoryAccount for ArmableMemoryAccount {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        if self.rejecting.load(Ordering::SeqCst) {
            return Err(MemoryAccountError::CapacityExceeded);
        }
        self.current.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
        assert!(previous >= bytes);
    }
}

fn deployment(
    producers: &[(BindingId, CoverageWitnessId, UniqueId)],
) -> RuntimeFilterChannelDeployment {
    let coverage = Coverage::AllOf(
        producers
            .iter()
            .map(|(_, witness, _)| Coverage::Leaf(*witness))
            .collect(),
    );
    RuntimeFilterChannelDeployment::new(
        CHANNEL,
        RuntimeFilterLogicalDomain::Membership {
            value_type: DataType::Int64,
            null_semantics: NullSemantics::NullSafeEqual,
        },
        RuntimeFilterLifecycle::CompleteOnce,
        coverage.clone(),
        coverage,
        ReductionRequirement::SetUnion,
        BTreeSet::from([
            ContributionKind::FinalDomainShard,
            ContributionKind::ProducerClosed,
        ]),
        CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 4096,
            max_artifact_bytes: 4096,
            deadline_ms: 100,
            max_retries: 0,
        },
        RuntimeFilterCoreBudget::new(16 * 1024),
        MaterializationPolicy::for_test(),
        producers
            .iter()
            .map(|(binding, witness, instance)| {
                (
                    *binding,
                    ProducerDeployment::new(*witness, BTreeSet::from([*instance])),
                )
            })
            .collect(),
        BTreeMap::from([(
            CONSUMER,
            ConsumerDeployment::with_profile(
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                ConsumerArtifactProfile::new(
                    BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                    None,
                )
                .unwrap(),
                RouteEdgeId::new(40),
                BTreeSet::from([uid(30)]),
            ),
        )]),
    )
}

fn installed_service(
    producers: &[(BindingId, CoverageWitnessId, UniqueId)],
    events: Arc<dyn RuntimeFilterEventSink>,
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Arc<RuntimeFilterService> {
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        uid(0),
        Arc::new(Clock(Instant::now())),
        events,
        memory,
    ));
    service
        .install(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(1),
            RuntimeFilterParticipantId::new(1),
            BTreeMap::from([(CHANNEL, deployment(producers))]),
        ))
        .unwrap();
    service
}

fn open_final(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
) -> Arc<dyn FinalDomainProducerAdapter> {
    let ProducerHandle::FinalDomain(producer) = service
        .open_producer(binding, instance, 1, ProducerPortKind::FinalDomain)
        .unwrap()
    else {
        panic!("fenced-final install must expose only the typed final-domain port")
    };
    producer
}

fn frozen_issuer(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
    open_drivers: u32,
) -> FrozenFinalDomainTestIssuer {
    let collecting = service
        .final_domain_test_issuer(binding, instance, open_drivers)
        .expect("private service adapter owns the test-only authority");
    let mut transition = FinalDomainTestIssuerTransition::Collecting(collecting);
    loop {
        transition = match transition {
            FinalDomainTestIssuerTransition::Collecting(collecting) => collecting.close_driver(),
            FinalDomainTestIssuerTransition::Frozen(frozen) => return frozen,
        };
    }
}

fn shard(
    issuer: &FrozenFinalDomainTestIssuer,
    binding: BindingId,
    instance: UniqueId,
    sequence: u64,
    values: &[i64],
) -> crate::runtime_filter::port::final_domain::FinalDomainShard {
    issuer
        .issue_shard(
            ProducerStreamId::new(binding, instance, PartitionId::new(0)),
            ProducerSequence::new(sequence),
            ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false),
        )
        .unwrap()
}

fn assert_coordinate(
    identity: ContributionIdentity,
    binding: BindingId,
    instance: UniqueId,
    sequence: u64,
) {
    assert_eq!(identity.query_id(), uid(0));
    assert_eq!(
        identity.participant_id(),
        RuntimeFilterParticipantId::new(1)
    );
    assert_eq!(identity.channel_id(), CHANNEL);
    assert_eq!(identity.epoch(), DeploymentEpoch::new(1));
    assert_eq!(identity.stream().binding_id(), binding);
    assert_eq!(identity.stream().fragment_instance_id(), instance);
    assert_eq!(identity.stream().partition_id(), PartitionId::new(0));
    assert_eq!(identity.sequence(), ProducerSequence::new(sequence));
}

#[test]
fn public_final_port_freezes_after_all_local_drivers_and_publishes_once_after_allof() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[
            (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
            (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
        ],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-public-final"),
    );
    let producer_a = open_final(&service, PRODUCER_A, uid(10));
    let producer_b = open_final(&service, PRODUCER_B, uid(20));
    let live = service
        .subscribe(CONSUMER, uid(30), SubscriptionKind::NonBlockingLive)
        .unwrap()
        .into_live()
        .unwrap();

    let issuer_a = frozen_issuer(&service, PRODUCER_A, uid(10), 2);
    let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 2);
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&issuer_a, PRODUCER_A, uid(10), 0, &[7]),
            )
            .unwrap(),
        SubmitOutcome::Applied
    );
    producer_a
        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
        .unwrap();
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None
        }
    ));

    producer_b
        .complete(
            PartitionId::new(0),
            ProducerSequence::new(0),
            shard(&issuer_b, PRODUCER_B, uid(20), 0, &[9]),
        )
        .unwrap();
    assert_eq!(
        producer_b
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );
    let LivePollOutcome::Updated { bundle, terminal } = live.poll_after(None) else {
        panic!("AllOf completion must publish one terminal bundle")
    };
    assert_eq!(terminal, Some(LiveTerminal::Completed));
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    assert_eq!(bundle.artifacts().len(), 1);
    assert_eq!(bundle.artifacts()[0].0, ArtifactKind::ValueSet);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed)
        }
    ));
    let recorded = events.snapshot();
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCompleted { .. }))
            .count(),
        1
    );
}

#[test]
fn final_input_events_keep_coordinates_order_and_resource_rejection_precedes_unavailable() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[
            (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
            (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
        ],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-causal-events"),
    );
    let producer_a = open_final(&service, PRODUCER_A, uid(10));
    let _producer_b = open_final(&service, PRODUCER_B, uid(20));
    let issuer_a = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 1);
    events.clear();

    let accepted = shard(&issuer_a, PRODUCER_A, uid(10), 0, &[1]);
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                accepted.clone(),
            )
            .unwrap(),
        SubmitOutcome::Applied
    );
    assert_eq!(
        producer_a
            .complete(PartitionId::new(0), ProducerSequence::new(0), accepted,)
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&issuer_a, PRODUCER_A, uid(10), 0, &[2]),
            )
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::ConflictingReplay
    );
    assert_eq!(
        producer_a
            .complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                shard(&issuer_b, PRODUCER_B, uid(20), 0, &[3]),
            )
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::UnauthorizedBinding
    );

    let typed = events
        .snapshot()
        .into_iter()
        .filter(|event| {
            matches!(
                event,
                RuntimeFilterEvent::FinalDomainShardAccepted { .. }
                    | RuntimeFilterEvent::FinalDomainShardDuplicate { .. }
                    | RuntimeFilterEvent::FinalDomainShardRejected { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(typed.len(), 4);
    for event in &typed {
        let identity = match event {
            RuntimeFilterEvent::FinalDomainShardAccepted { identity }
            | RuntimeFilterEvent::FinalDomainShardDuplicate { identity }
            | RuntimeFilterEvent::FinalDomainShardRejected { identity, .. } => *identity,
            _ => unreachable!(),
        };
        assert_coordinate(identity, PRODUCER_A, uid(10), 0);
    }
    assert!(matches!(
        typed[0],
        RuntimeFilterEvent::FinalDomainShardAccepted { .. }
    ));
    assert!(matches!(
        typed[1],
        RuntimeFilterEvent::FinalDomainShardDuplicate { .. }
    ));
    assert!(matches!(
        typed[2],
        RuntimeFilterEvent::FinalDomainShardRejected {
            rejection: FinalDomainRejectionKind::Contract(
                RuntimeContractViolationKind::ConflictingReplay
            ),
            ..
        }
    ));
    assert!(matches!(
        typed[3],
        RuntimeFilterEvent::FinalDomainShardRejected {
            rejection: FinalDomainRejectionKind::Contract(
                RuntimeContractViolationKind::UnauthorizedBinding
            ),
            ..
        }
    ));

    let resource_events = Arc::new(Events::default());
    let memory = Arc::new(ArmableMemoryAccount::default());
    let resource_service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        resource_events.clone(),
        memory.clone(),
    );
    let resource_producer = open_final(&resource_service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&resource_service, PRODUCER_A, uid(10), 1);
    let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[1]);
    resource_events.clear();
    memory.rejecting.store(true, Ordering::SeqCst);
    assert_eq!(
        resource_producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input,)
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    let recorded = resource_events.snapshot();
    let rejected = recorded
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeFilterEvent::FinalDomainShardRejected {
                    rejection: FinalDomainRejectionKind::ResourceLimit,
                    ..
                }
            )
        })
        .expect("resource failure emits typed rejection");
    let unavailable = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ChannelUnavailable { .. }))
        .expect("resource failure emits unavailable terminal");
    assert!(rejected < unavailable);
    assert_eq!(memory.current.load(Ordering::SeqCst), 0);
}

#[test]
fn final_semantic_rejection_linearizes_before_a_competing_terminal() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-semantic-rejection-order"),
    );
    let producer = open_final(&service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    producer
        .complete(
            PartitionId::new(0),
            ProducerSequence::new(0),
            shard(&issuer, PRODUCER_A, uid(10), 0, &[1]),
        )
        .unwrap();
    events.clear();

    let channel = service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1
        .clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    channel.set_before_final_semantic_rejection_hook(Arc::new(move |next_dispatch_order| {
        entered_tx.send(next_dispatch_order).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    }));

    let rejected_producer = producer.clone();
    let rejected = shard(&issuer, PRODUCER_A, uid(10), 0, &[2]);
    let (result_tx, result_rx) = mpsc::channel();
    let rejection_thread = std::thread::spawn(move || {
        result_tx
            .send(rejected_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                rejected,
            ))
            .unwrap();
    });
    let observed_next_order = entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("semantic rejection did not reach the in-lock linearization hook");

    let cancelling_service = service.clone();
    let (cancel_started_tx, cancel_started_rx) = mpsc::channel();
    let cancel_thread = std::thread::spawn(move || {
        cancel_started_tx.send(()).unwrap();
        cancelling_service.cancel();
    });
    cancel_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("competing terminal thread did not start");
    std::thread::sleep(Duration::from_millis(50));
    release_tx.send(()).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::ConflictingReplay
    );
    rejection_thread.join().unwrap();
    cancel_thread.join().unwrap();
    assert_eq!(
        observed_next_order, 2,
        "Core must reserve rejection order while the semantic-decision lock is still held"
    );

    let recorded = events.snapshot();
    let rejected = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
        .expect("semantic rejection event must be delivered");
    let cancelled = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
        .expect("competing terminal event must be delivered");
    assert!(
        rejected < cancelled,
        "semantic rejection must linearize before the terminal that waited on its Core lock"
    );
    assert_eq!(service.dispatcher.pending_action_count(CHANNEL), 0);
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .count(),
        1
    );
    assert!(matches!(
        channel.terminal_action(),
        ChannelAction::Cancelled { .. }
    ));
}

#[test]
fn final_semantic_rejection_follows_a_terminal_that_linearized_first() {
    let events = Arc::new(Events::default());
    let service = installed_service(
        &[
            (PRODUCER_A, CoverageWitnessId::new(10), uid(10)),
            (PRODUCER_B, CoverageWitnessId::new(20), uid(20)),
        ],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-terminal-first-order"),
    );
    let producer_a = open_final(&service, PRODUCER_A, uid(10));
    let _producer_b = open_final(&service, PRODUCER_B, uid(20));
    let issuer_b = frozen_issuer(&service, PRODUCER_B, uid(20), 1);
    events.clear();

    let channel = service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1
        .clone();
    let terminal = channel.cancel();
    assert!(matches!(terminal, ChannelAction::Cancelled { .. }));

    let invalid = shard(&issuer_b, PRODUCER_B, uid(20), 0, &[7]);
    let (result_tx, result_rx) = mpsc::channel();
    let rejection_thread = std::thread::spawn(move || {
        result_tx
            .send(producer_a.complete(PartitionId::new(0), ProducerSequence::new(0), invalid))
            .unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while service.dispatcher.pending_action_count(CHANNEL) == 0 {
        assert!(
            Instant::now() < deadline,
            "later semantic rejection never queued behind the terminal action"
        );
        std::thread::yield_now();
    }
    service.dispatcher.dispatch(CHANNEL, terminal).unwrap();
    assert_eq!(
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err()
            .kind(),
        RuntimeContractViolationKind::UnauthorizedBinding
    );
    rejection_thread.join().unwrap();
    assert_eq!(service.dispatcher.pending_action_count(CHANNEL), 0);

    let recorded = events.snapshot();
    let cancelled = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
        .expect("terminal event must be delivered");
    let rejected = recorded
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
        .expect("later semantic rejection event must be delivered");
    assert!(cancelled < rejected);
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::FinalDomainShardRejected { .. }))
            .count(),
        1
    );
    assert!(matches!(
        channel.terminal_action(),
        ChannelAction::Cancelled { .. }
    ));
}

#[derive(Default)]
struct AdversarialEvents {
    recorded: Mutex<Vec<RuntimeFilterEvent>>,
    service: Mutex<Option<Weak<RuntimeFilterService>>>,
    panicked: AtomicBool,
    cancelled: AtomicBool,
}

impl RuntimeFilterEventSink for AdversarialEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.recorded.lock().unwrap().push(event.clone());
        if matches!(event, RuntimeFilterEvent::FinalDomainShardAccepted { .. })
            && !self.panicked.swap(true, Ordering::SeqCst)
        {
            panic!("intentional final-domain event sink panic");
        }
        if matches!(event, RuntimeFilterEvent::FinalDomainShardDuplicate { .. })
            && !self.cancelled.swap(true, Ordering::SeqCst)
        {
            if let Some(service) = self
                .service
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
            {
                service.cancel();
            }
        }
    }
}

#[test]
fn sink_panic_reentry_cancel_and_weak_handle_drop_are_safe_and_single_publish() {
    let events = Arc::new(AdversarialEvents::default());
    let service = installed_service(
        &[(PRODUCER_A, CoverageWitnessId::new(10), uid(10))],
        events.clone(),
        MemTrackerMemoryAccount::new_root_for_test("m3c-adversarial-events"),
    );
    *events.service.lock().unwrap() = Some(Arc::downgrade(&service));
    let producer = open_final(&service, PRODUCER_A, uid(10));
    let issuer = frozen_issuer(&service, PRODUCER_A, uid(10), 1);
    let input = shard(&issuer, PRODUCER_A, uid(10), 0, &[1]);
    assert_eq!(
        producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input.clone(),)
            .unwrap(),
        SubmitOutcome::Applied
    );
    assert!(events.panicked.load(Ordering::SeqCst));
    let duplicate_producer = producer.clone();
    let duplicate_input = input.clone();
    let (duplicate_tx, duplicate_rx) = mpsc::channel();
    let duplicate_worker = std::thread::spawn(move || {
        duplicate_tx
            .send(duplicate_producer.complete(
                PartitionId::new(0),
                ProducerSequence::new(0),
                duplicate_input,
            ))
            .unwrap();
    });
    assert_eq!(
        duplicate_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("sink-to-cancel reentry deadlocked")
            .unwrap(),
        SubmitOutcome::Duplicate
    );
    duplicate_worker.join().unwrap();
    assert!(events.cancelled.load(Ordering::SeqCst));
    assert_eq!(
        producer
            .complete(PartitionId::new(0), ProducerSequence::new(0), input,)
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    let recorded = events.recorded.lock().unwrap().clone();
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ChannelCancelled { .. }))
            .count(),
        1
    );
    assert_eq!(
        recorded
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
            .count(),
        0
    );

    let weak = Arc::downgrade(&producer);
    drop(producer);
    assert!(weak.upgrade().is_none());
    let service_weak = Arc::downgrade(&service);
    drop(service);
    assert!(service_weak.upgrade().is_none());
}
