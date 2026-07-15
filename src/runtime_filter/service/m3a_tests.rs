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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{LogicalVersion, PartitionId, ProducerSequence};
use crate::runtime_filter::port::producer::{
    ProducerFailureReason, ProducerHandle, ProducerPortKind, RuntimeContractViolationKind,
    SubmitOutcome,
};
use crate::runtime_filter::port::subscription::{
    ArtifactDeliveryOutcome, LivePollOutcome, LiveTerminal, SubscriptionHandle, SubscriptionKind,
    UnavailableReason,
};
use crate::runtime_filter::port::support::{MemoryAccountError, RuntimeFilterMemoryAccount};

use super::tests::{
    installed_ordered_service_fixture, installed_ordered_service_with_account, ordered_update,
};

fn uid(lo: i64) -> crate::common::types::UniqueId {
    crate::common::types::UniqueId { hi: 70, lo }
}

fn live_handle(
    service: &super::RuntimeFilterService,
) -> Arc<dyn crate::runtime_filter::port::subscription::NonBlockingLiveSubscription> {
    match service
        .subscribe(BindingId::new(2), uid(2), SubscriptionKind::NonBlockingLive)
        .unwrap()
    {
        SubscriptionHandle::Live(live) => live,
        SubscriptionHandle::Blocking(_) => panic!("ordered consumer returned blocking handle"),
    }
}

fn updated_version(outcome: LivePollOutcome) -> LogicalVersion {
    match outcome {
        LivePollOutcome::Updated { bundle, .. } => bundle.version(),
        other => panic!("expected live update, got {other:?}"),
    }
}

#[test]
fn ordered_service_live_poll_has_no_shared_cursor_and_skips_to_latest() {
    let service = installed_ordered_service_fixture();
    let first = live_handle(&service);
    let second = live_handle(&service);
    assert!(matches!(
        first.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: None
        }
    ));
    let (_, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test("unused-ordered-contract"),
    );
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };

    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        updated_version(first.poll_after(None)),
        LogicalVersion::FIRST
    );
    assert_eq!(
        updated_version(second.poll_after(None)),
        LogicalVersion::FIRST
    );

    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 70),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(
        updated_version(first.poll_after(Some(LogicalVersion::FIRST))),
        LogicalVersion::new(2)
    );
    assert_eq!(
        updated_version(second.poll_after(None)),
        LogicalVersion::new(2)
    );
}

#[test]
fn ordered_service_update_and_completed_terminal_are_one_live_snapshot() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test("ordered-live-completed"),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    assert_eq!(
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))
            .unwrap(),
        SubmitOutcome::Completed
    );

    let outcome = live.poll_after(None);
    assert!(
        matches!(
            outcome,
            LivePollOutcome::Updated {
                terminal: Some(LiveTerminal::Completed),
                ..
            }
        ),
        "unexpected live completion snapshot: {outcome:?}"
    );
}

#[test]
fn ordered_service_completed_without_artifact_is_exact_live_terminal() {
    let service = installed_ordered_service_fixture();
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    assert_eq!(
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(0))
            .unwrap(),
        SubmitOutcome::CompletedWithoutArtifact
    );
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::CompletedWithoutArtifact)
        }
    ));
}

#[test]
fn ordered_live_activation_mismatch_is_typed_and_does_not_poison_live_handle() {
    let service = installed_ordered_service_fixture();
    let error = service
        .subscribe(
            BindingId::new(2),
            uid(2),
            SubscriptionKind::BlockingSnapshot,
        )
        .unwrap_err();
    assert_eq!(
        error.kind(),
        RuntimeContractViolationKind::SubscriptionActivationMismatch
    );
    assert!(matches!(
        service
            .subscribe(BindingId::new(2), uid(2), SubscriptionKind::NonBlockingLive,)
            .unwrap(),
        SubscriptionHandle::Live(_)
    ));
}

#[derive(Default)]
struct ArmableLargeAllocationRejector {
    armed: AtomicBool,
    current: AtomicUsize,
}

impl RuntimeFilterMemoryAccount for ArmableLargeAllocationRejector {
    fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
        if self.armed.load(Ordering::SeqCst) && bytes > 256 {
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

#[test]
fn ordered_final_artifact_failure_retains_latest_and_degraded_terminal() {
    let account = Arc::new(ArmableLargeAllocationRejector::default());
    let (service, contract) = installed_ordered_service_with_account(account.clone());
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);

    account.armed.store(true, Ordering::SeqCst);
    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(1),
                ordered_update(&contract, 70),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::DegradedArtifact(
                UnavailableReason::ResourceLimit
            ))
        }
    ));

    account.armed.store(false, Ordering::SeqCst);
    assert_eq!(
        producer
            .close_partition(PartitionId::new(0), ProducerSequence::new(2))
            .unwrap(),
        SubmitOutcome::Completed
    );
    let outcome = live.poll_after(Some(LogicalVersion::FIRST));
    assert!(
        matches!(
            outcome,
            LivePollOutcome::Idle {
                latest_version: Some(LogicalVersion::FIRST),
                terminal: Some(LiveTerminal::DegradedArtifact(
                    UnavailableReason::ResourceLimit
                ))
            }
        ),
        "unexpected live completion after artifact degradation: {outcome:?}"
    );
}

#[test]
fn ordered_service_first_materialization_failure_is_unavailable() {
    let account = Arc::new(ArmableLargeAllocationRejector::default());
    let (service, contract) = installed_ordered_service_with_account(account.clone());
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    account.armed.store(true, Ordering::SeqCst);

    assert_eq!(
        producer
            .submit_bound(
                PartitionId::new(0),
                ProducerSequence::new(0),
                ordered_update(&contract, 100),
            )
            .unwrap(),
        SubmitOutcome::Published
    );
    assert!(live.snapshot().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ResourceLimit))
        }
    ));
}

#[test]
fn ordered_service_first_route_failure_is_unavailable() {
    let service = installed_ordered_service_fixture();
    let live = live_handle(&service);
    let installed = service.registry.active_installation().unwrap();
    let routes = installed.artifact_plan(ChannelId::new(1)).unwrap().groups()[0]
        .route_edges()
        .to_vec();

    assert_eq!(
        installed.router().route_live(
            &routes,
            Some(&ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::RouteUnavailable,
            )),
            None,
        ),
        routes
    );
    assert!(live.snapshot().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(
                UnavailableReason::RouteUnavailable
            ))
        }
    ));
}

#[test]
fn ordered_service_cancellation_retains_latest_live_snapshot() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test("ordered-live-cancelled"),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);

    service.cancel();

    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Cancelled)
        }
    ));
}

#[test]
fn ordered_service_cancel_overrides_completed_and_retains_latest() {
    let (service, contract) = installed_ordered_service_with_account(
        super::memory::MemTrackerMemoryAccount::new_root_for_test(
            "ordered-live-completed-then-cancelled",
        ),
    );
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    producer
        .submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ordered_update(&contract, 100),
        )
        .unwrap();
    producer
        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
        .unwrap();
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            terminal: Some(LiveTerminal::Completed),
            ..
        }
    ));

    service.cancel();

    assert_eq!(live.snapshot().unwrap().version(), LogicalVersion::FIRST);
    assert!(matches!(
        live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            terminal: Some(LiveTerminal::Cancelled),
            ..
        }
    ));
}

#[test]
fn ordered_service_cancel_overrides_unavailable_without_artifact() {
    let service = installed_ordered_service_fixture();
    let live = live_handle(&service);
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(BindingId::new(1), uid(1), 1, ProducerPortKind::OrderedBound)
        .unwrap()
    else {
        panic!("ordered fixture returned membership producer")
    };
    assert_eq!(
        producer
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap(),
        SubmitOutcome::CoverageStillPossible
    );
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ProducerFailed))
        }
    ));

    service.cancel();

    assert!(live.snapshot().is_none());
    assert!(matches!(
        live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Cancelled)
        }
    ));
}
