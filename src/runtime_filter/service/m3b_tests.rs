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
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::*;
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::artifact::ConsumerArtifactProfile;
use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
use crate::runtime_filter::port::identity::*;
use crate::runtime_filter::port::install::*;
use crate::runtime_filter::port::ordered_bound::{
    COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, RuntimeOrderContract,
    comparator_digest_for_test,
};
use crate::runtime_filter::port::producer::{
    ProducerHandle, ProducerPortKind, RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::subscription::{
    LivePollOutcome, LiveTerminal, SubscriptionHandle, SubscriptionKind,
};
use crate::runtime_filter::port::support::{
    MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount, TemporaryContributionLease,
};
use crate::runtime_filter::port::topk_summary::{RuntimeTopKSummaryContract, TopKSummary};

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const PRODUCER_A: BindingId = BindingId::new(1);
const CONSUMER: BindingId = BindingId::new(2);
const PRODUCER_B: BindingId = BindingId::new(3);

fn uid(lo: i64) -> UniqueId {
    UniqueId { hi: 80, lo }
}

#[derive(Default)]
struct Events(Mutex<Vec<RuntimeFilterEvent>>);

impl RuntimeFilterEventSink for Events {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

struct Clock(Instant);

impl RuntimeFilterClock for Clock {
    fn now(&self) -> Instant {
        self.0
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

struct Fixture {
    service: Arc<RuntimeFilterService>,
    contract: Arc<RuntimeTopKSummaryContract>,
    events: Arc<Events>,
}

fn fixture_with_account(memory: Arc<dyn RuntimeFilterMemoryAccount>) -> Fixture {
    let events = Arc::new(Events::default());
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        uid(0),
        Arc::new(Clock(Instant::now())),
        events.clone(),
        memory,
    ));
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction: SortDirection::Ascending,
        null_order: NullOrder::Last,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let requirement = TopKSummaryRequirement::try_new(4).unwrap();
    let contract = Arc::new(RuntimeTopKSummaryContract::try_from_plan(&plan, requirement).unwrap());
    let range_contract = RuntimeOrderContract::try_from_plan(&plan).unwrap();
    let witnesses = [CoverageWitnessId::new(1), CoverageWitnessId::new(2)];
    let coverage = Coverage::AllOf(witnesses.into_iter().map(Coverage::Leaf).collect());
    let deployment = RuntimeFilterChannelDeployment::new(
        ChannelId::new(1),
        RuntimeFilterLogicalDomain::OrderedBound(plan),
        RuntimeFilterLifecycle::MonotonicUpdates,
        coverage.clone(),
        coverage,
        ReductionRequirement::MergeTopKSummary(requirement),
        BTreeSet::from([
            ContributionKind::TopKSummary,
            ContributionKind::ProducerClosed,
        ]),
        CompletionRequirement::ProducerClosed,
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 4096,
            max_artifact_bytes: 4096,
            deadline_ms: 100,
            max_retries: 0,
        },
        RuntimeFilterCoreBudget::new(16 * 1024),
        MaterializationPolicy::for_test(),
        BTreeMap::from([
            (
                PRODUCER_A,
                ProducerDeployment::new(witnesses[0], BTreeSet::from([uid(1)])),
            ),
            (
                PRODUCER_B,
                ProducerDeployment::new(witnesses[1], BTreeSet::from([uid(3)])),
            ),
        ]),
        BTreeMap::from([(
            CONSUMER,
            ConsumerDeployment::with_profile(
                ConsumerActivation::NonBlockingLive {
                    late_apply: LateApplyGranularity::Batch,
                },
                BTreeSet::from([ArtifactCapability::OrderedRange]),
                ConsumerArtifactProfile::new_ordered_range(range_contract.digest()).unwrap(),
                RouteEdgeId::new(1),
                BTreeSet::from([uid(2)]),
            ),
        )]),
    );
    service
        .install(RuntimeFilterInstallView::new(
            DeploymentEpoch::new(9),
            RuntimeFilterParticipantId::new(3),
            BTreeMap::from([(ChannelId::new(1), deployment)]),
        ))
        .unwrap();
    Fixture {
        service,
        contract,
        events,
    }
}

fn fixture() -> Fixture {
    fixture_with_account(MemTrackerMemoryAccount::new_root_for_test(
        "topk-summary-service-test",
    ))
}

fn summary(contract: &RuntimeTopKSummaryContract, values: &[i64]) -> TopKSummary {
    TopKSummary::try_new(
        contract,
        values
            .iter()
            .map(|value| {
                OrderedTuple::try_new(contract.order(), [Some(OrderedScalar::Int64(*value))])
                    .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn summary_producer(
    service: &RuntimeFilterService,
    binding: BindingId,
    instance: UniqueId,
) -> Arc<dyn crate::runtime_filter::port::producer::TopKSummaryProducerAdapter> {
    let ProducerHandle::TopKSummary(producer) = service
        .open_producer(binding, instance, 1, ProducerPortKind::TopKSummary)
        .unwrap()
    else {
        panic!("top-k route must return a summary producer")
    };
    producer
}

fn live(
    service: &RuntimeFilterService,
) -> Arc<dyn crate::runtime_filter::port::subscription::NonBlockingLiveSubscription> {
    let SubscriptionHandle::Live(live) = service
        .subscribe(CONSUMER, uid(2), SubscriptionKind::NonBlockingLive)
        .unwrap()
    else {
        panic!("top-k range consumer must be live")
    };
    live
}

fn range_value(outcome: LivePollOutcome) -> (LogicalVersion, i64, Option<LiveTerminal>) {
    let LivePollOutcome::Updated { bundle, terminal } = outcome else {
        panic!("expected a live range update")
    };
    let [(crate::runtime_filter::port::artifact::ArtifactKind::Range, artifact)] =
        bundle.artifacts()
    else {
        panic!("expected exactly one range artifact")
    };
    let [Some(OrderedScalar::Int64(value))] = artifact.range().unwrap().bound().values() else {
        panic!("expected an int64 range bound")
    };
    (bundle.version(), *value, terminal)
}

#[test]
fn topk_open_returns_summary_handle_and_rejects_logical_domain_ports() {
    let fixture = fixture();
    for wrong in [ProducerPortKind::Membership, ProducerPortKind::OrderedBound] {
        assert_eq!(
            fixture
                .service
                .open_producer(PRODUCER_A, uid(1), 1, wrong)
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ProducerPortMismatch
        );
    }
    assert!(matches!(
        fixture
            .service
            .open_producer(PRODUCER_A, uid(1), 1, ProducerPortKind::TopKSummary)
            .unwrap(),
        ProducerHandle::TopKSummary(_)
    ));
}

#[test]
fn topk_service_reuses_live_range_versions_and_exposes_terminal() {
    let fixture = fixture();
    let live = live(&fixture.service);
    let first = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let second = summary_producer(&fixture.service, PRODUCER_B, uid(3));

    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&fixture.contract, &[1, 4]),
            )
            .unwrap(),
        SubmitOutcome::StreamAcceptedNoGlobalChange
    );
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None
        }
    ));
    assert_eq!(
        second
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(0),
                summary(&fixture.contract, &[2, 2]),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        range_value(live.poll_after(None)),
        (LogicalVersion::FIRST, 4, None)
    );

    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(1),
                summary(&fixture.contract, &[0, 1, 3, 4]),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        range_value(live.poll_after(Some(LogicalVersion::FIRST))),
        (LogicalVersion::new(2), 2, None)
    );
    assert_eq!(
        first
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(2),
                summary(&fixture.contract, &[0, 1, 2, 4]),
            )
            .unwrap(),
        SubmitOutcome::StreamAcceptedNoGlobalChange
    );
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::new(2))),
        LivePollOutcome::Idle {
            latest_version: Some(version),
            terminal: None
        } if version == LogicalVersion::new(2)
    ));

    assert_ne!(
        first
            .close_partition(PartitionId::new(0), ProducerSequence::new(3))
            .unwrap(),
        SubmitOutcome::Completed
    );
    assert_eq!(
        second
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::new(2))),
        LivePollOutcome::Idle {
            latest_version: Some(version),
            terminal: Some(LiveTerminal::Completed)
        } if version == LogicalVersion::new(2)
    ));
}

#[test]
fn topk_service_emits_typed_input_events_and_keeps_global_event_names() {
    let fixture = fixture();
    let first = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let second = summary_producer(&fixture.service, PRODUCER_B, uid(3));
    fixture.events.0.lock().unwrap().clear();

    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(3),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(2),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    first
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(4),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    second
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[2, 2]),
        )
        .unwrap();

    let events = fixture.events.0.lock().unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKStreamUpdated { identity }
            if identity.sequence() == ProducerSequence::new(3)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryStale { identity }
            if identity.sequence() == ProducerSequence::new(2)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryEqual { identity }
            if identity.sequence() == ProducerSequence::new(4)
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::TopKSummaryApplied { identity }
            if identity.sequence() == ProducerSequence::new(0)
                && identity.stream().binding_id() == PRODUCER_B
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::OrderedGlobalTightened { version, .. }
            if *version == LogicalVersion::FIRST
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        RuntimeFilterEvent::LogicalVersionPublished { version, .. }
            if *version == LogicalVersion::FIRST
    )));
}

#[test]
fn topk_authorize_preflight_and_core_rejections_are_typed_before_return() {
    let account = Arc::new(ArmableMemoryAccount::default());
    let fixture = fixture_with_account(account.clone());
    let producer = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    fixture.events.0.lock().unwrap().clear();

    let unauthorized = producer
        .submit_summary(
            PartitionId::new(1),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap_err();
    assert_eq!(
        unauthorized.kind(),
        RuntimeContractViolationKind::InvalidPartition
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.stream().partition_id() == PartitionId::new(1)
                        && *violation == RuntimeContractViolationKind::InvalidPartition
            ))
    );

    fixture.events.0.lock().unwrap().clear();
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction: SortDirection::Descending,
        null_order: NullOrder::Last,
    }];
    let wrong_plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let wrong_contract = RuntimeTopKSummaryContract::try_from_plan(
        &wrong_plan,
        TopKSummaryRequirement::try_new(4).unwrap(),
    )
    .unwrap();
    account.rejecting.store(true, Ordering::SeqCst);
    let preflight = producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&wrong_contract, &[4, 1]),
        )
        .unwrap_err();
    account.rejecting.store(false, Ordering::SeqCst);
    assert_eq!(
        preflight.kind(),
        RuntimeContractViolationKind::OrderedContractMismatch
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.sequence() == ProducerSequence::new(0)
                        && *violation == RuntimeContractViolationKind::OrderedContractMismatch
            ))
    );

    fixture.events.0.lock().unwrap().clear();
    producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 4]),
        )
        .unwrap();
    fixture.events.0.lock().unwrap().clear();
    let conflict = producer
        .submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            summary(&fixture.contract, &[1, 3]),
        )
        .unwrap_err();
    assert_eq!(
        conflict.kind(),
        RuntimeContractViolationKind::ConflictingReplay
    );
    assert!(
        fixture
            .events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| matches!(
                event,
                RuntimeFilterEvent::TopKSummaryRejected { identity, violation }
                    if identity.sequence() == ProducerSequence::new(0)
                        && *violation == RuntimeContractViolationKind::ConflictingReplay
            ))
    );
}

#[test]
fn topk_rejection_cannot_overtake_an_earlier_accepted_dispatch() {
    let fixture = fixture();
    let producer = summary_producer(&fixture.service, PRODUCER_A, uid(1));
    let channel = fixture
        .service
        .registry
        .active_installation()
        .unwrap()
        .channels()
        .next()
        .unwrap()
        .1;
    fixture.events.0.lock().unwrap().clear();

    let accepted_summary = summary(&fixture.contract, &[1, 4]);
    let accepted_bytes = accepted_summary.canonical_contribution_bytes().unwrap();
    let accepted = channel
        .submit_topk_summary(
            PRODUCER_A,
            uid(1),
            PartitionId::new(0),
            ProducerSequence::new(0),
            accepted_summary,
            TemporaryContributionLease::new(
                MemTrackerMemoryAccount::new_root_for_test("topk-event-order-test"),
                accepted_bytes,
            ),
        )
        .unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let contract = fixture.contract.clone();
    let rejected = std::thread::spawn(move || {
        let error = producer
            .submit_summary(
                PartitionId::new(0),
                ProducerSequence::new(1),
                summary(&contract, &[1, 5]),
            )
            .unwrap_err();
        done_tx.send(error.kind()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(1);
    while fixture
        .service
        .dispatcher
        .pending_action_count(ChannelId::new(1))
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "rejection never reached dispatcher"
        );
        std::thread::yield_now();
    }
    assert!(fixture.events.0.lock().unwrap().is_empty());

    fixture
        .service
        .dispatcher
        .dispatch(ChannelId::new(1), accepted)
        .unwrap();
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RuntimeContractViolationKind::OrderedBoundLoosened
    );
    rejected.join().unwrap();
    let typed = fixture
        .events
        .0
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| match event {
            RuntimeFilterEvent::TopKSummaryApplied { identity } => {
                Some((identity.sequence(), None))
            }
            RuntimeFilterEvent::TopKSummaryRejected {
                identity,
                violation,
            } => Some((identity.sequence(), Some(*violation))),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        typed,
        vec![
            (ProducerSequence::new(0), None),
            (
                ProducerSequence::new(1),
                Some(RuntimeContractViolationKind::OrderedBoundLoosened),
            ),
        ]
    );
}
