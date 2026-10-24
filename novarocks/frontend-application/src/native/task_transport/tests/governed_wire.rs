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

//! Original fixed-WC source for actual Native wire fixtures. This fixture
//! proves request/physical/retained lifetime, not complete Native protection.
use super::*;
use crate::native::task_transport::queue_input::queued_header_quote;
use crate::task_execution::intent::TaskOperationQueuePreparation;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub(super) struct GovernedWireRole {
    control: WorkloadControl,
    driver: Mutex<JointProgressDriver>,
    _root: novarocks_workload_control::RootWork,
    window: novarocks_workload_control::ResultWindowGrant,
}
impl GovernedWireRole {
    pub(super) fn new(peer: &Loopback) -> Self {
        let count = 8;
        let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 4, 2, &[]);
        let mut config = JointCapacityConfig {
            global: JointResourceLimits::default(),
            classes: [JointResourceLimits::default(); 5],
            lanes: [JointResourceLimits::default(); 3],
            class_lanes: [[JointResourceLimits::default(); 3]; 5],
            max_dependencies_per_claim: 2,
            max_registered_dependencies: 1,
            max_claims: 512,
            max_requests: 32,
            max_changes_per_request: 2,
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_scratch_bytes: 64 * 1024 * 1024,
            max_residual_arcs: 100000,
            max_total_proof_work: 10000000,
            work_per_turn: NonZeroUsize::new(31).unwrap(),
        };
        config.max_registered_dependencies = storage.minimum_dependency_records(&config).unwrap();
        let base = storage.base_charge(&config).unwrap();
        let metadata = base
            .get(JointResource::MetadataBytes)
            .checked_mul(64)
            .unwrap();
        let mut custody = JointResources::ZERO
            .with(JointResource::ScopeRecords, count as u64)
            .with(JointResource::ClaimRecords, 256)
            .with(JointResource::ReplyRecords, 512)
            .with(JointResource::QueueRecords, 128)
            .with(JointResource::MetadataBytes, metadata)
            .with(JointResource::PayloadBytes, 16 * 1024 * 1024 * 1024);
        let execution = JointResources::ZERO
            .with(JointResource::ClaimRecords, 128)
            .with(JointResource::MetadataBytes, metadata)
            .with(JointResource::PayloadBytes, 16 * 1024 * 1024 * 1024)
            .with(JointResource::AsyncIo, 64)
            .with(JointResource::PhysicalQueueRecords, 64)
            .with(JointResource::PhysicalQueueBytes, 1024 * 1024 * 1024);
        // Explicit finite support of this wire fixture, independent of the
        // encoded-byte limit. This is not a complete production profile.
        let native = NativeTaskOperationSink::minimum_progress_for_composition(
            TransportBudget::DEFAULT,
            std::num::NonZeroU64::new(128 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        fn sum(a: JointResources, b: JointResources) -> JointResources {
            let mut result = JointResources::ZERO;
            for resource in [
                JointResource::ScopeRecords,
                JointResource::ClaimRecords,
                JointResource::ReplyRecords,
                JointResource::QueueRecords,
                JointResource::MetadataBytes,
                JointResource::PayloadBytes,
                JointResource::UnreceivedProductions,
                JointResource::Publications,
                JointResource::Residuals,
                JointResource::Computations,
                JointResource::AsyncIo,
                JointResource::BlockingJobs,
                JointResource::InternalCpuJobs,
                JointResource::PhysicalQueueRecords,
                JointResource::PhysicalQueueBytes,
            ] {
                result = result.with(
                    resource,
                    a.get(resource).checked_add(b.get(resource)).unwrap(),
                );
            }
            result
        }
        let custody_progress = sum(base, native.custody());
        let execution_progress = native.foreground();
        assert_eq!(execution_progress, native.settlement());
        let all_progress = sum(
            custody_progress,
            sum(execution_progress, execution_progress),
        );
        let custody_limits = JointResourceLimits {
            global: custody,
            per_dependency: custody,
            protected_progress: custody_progress,
        };
        let execution_limits = JointResourceLimits {
            global: execution,
            per_dependency: execution,
            protected_progress: execution_progress,
        };
        config.lanes = [custody_limits, execution_limits, execution_limits];
        config.class_lanes[0] = config.lanes;
        for r in [
            JointResource::ScopeRecords,
            JointResource::ClaimRecords,
            JointResource::ReplyRecords,
            JointResource::QueueRecords,
            JointResource::MetadataBytes,
            JointResource::PayloadBytes,
            JointResource::UnreceivedProductions,
            JointResource::Publications,
            JointResource::Residuals,
            JointResource::Computations,
            JointResource::AsyncIo,
            JointResource::BlockingJobs,
            JointResource::InternalCpuJobs,
            JointResource::PhysicalQueueRecords,
            JointResource::PhysicalQueueBytes,
        ] {
            custody = custody.with(
                r,
                custody
                    .get(r)
                    .checked_add(execution.get(r).checked_mul(2).unwrap())
                    .unwrap(),
            );
        }
        config.global = JointResourceLimits {
            global: custody,
            per_dependency: custody,
            protected_progress: all_progress,
        };
        config.classes[0] = config.global;
        let mut required = RequiredJointProgress {
            global: all_progress,
            classes: [JointResources::ZERO; 5],
            lanes: [custody_progress, execution_progress, execution_progress],
            class_lanes: [[JointResources::ZERO; 3]; 5],
        };
        required.classes[0] = all_progress;
        required.class_lanes[0] = required.lanes;
        // This is the finite original Native source for wire/lifetime tests.
        // Other production sources and a complete Native profile are absent.
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_joint_capacity(config.validate(&required).unwrap())
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let mut proposal = scope.begin_control_binding().unwrap();
        for endpoint in [&peer.endpoint, &peer.control_endpoint] {
            let origin = novarocks_native_adapter::FrontendNativeTransport::plaintext()
                .selected_network_origin(endpoint.native_endpoint())
                .unwrap()
                .into_owned();
            proposal
                .push_dependency(&novarocks_resource_contract::SharedDependency::Network(
                    origin,
                ))
                .unwrap();
        }
        proposal.end_change().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive_original(&mut driver);
        let mut ready = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("original control binding must grant"),
        };
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop(ready);
        drive_original(&mut driver);
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        Self {
            control,
            driver: Mutex::new(driver),
            _root: root,
            window,
        }
    }
    pub(super) fn authority(&self) -> NativeAsyncActionAuthority {
        let authority = self
            .window
            .retain_alias()
            .native_action_authority()
            .unwrap();
        assert_eq!(
            authority
                .maximum_owned_increment(
                    std::num::NonZeroU64::new(
                        2 * TransportBudget::DEFAULT.max_batch_encoded_bytes() as u64
                    )
                    .unwrap()
                )
                .unwrap()
                .get(),
            128 * 1024 * 1024
        );
        assert!(
            authority
                .maximum_owned_increment(std::num::NonZeroU64::new(u64::MAX).unwrap())
                .is_err()
        );
        authority
    }
    pub(super) fn drive(&self) {
        drive_original(&mut self.driver.lock().unwrap())
    }
    pub(super) fn reserve(
        &self,
        sink: &NativeTaskOperationSink,
        intent: &OperationIntent,
    ) -> Box<dyn crate::task_execution::intent::TaskOperationQueuePermit> {
        let mut preparation = TaskOperationQueuePreparation::new();
        match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
            TaskOperationQueueAdmission::Admitted(permit) => return permit,
            TaskOperationQueueAdmission::Rejected(error) => {
                panic!("original paid input refused: {error}")
            }
            _ => {}
        }
        self.drive();
        match sink.try_reserve_queue(intent.queue_request(), &mut preparation) {
            TaskOperationQueueAdmission::Admitted(permit) => permit,
            result => panic!("original paid wire fixture input remains unavailable: {result:?}"),
        }
    }
}
fn drive_original(driver: &mut JointProgressDriver) {
    for _ in 0..100000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("finite original wire fixture proof driver failed to reach idle")
}

/// Exercise the original paid preparation which RemoteTask withdraws before
/// publication. Physical saturation is real; no operation is sent to the peer.
#[tokio::test(flavor = "multi_thread")]
async fn unpublished_paid_input_withdrawal_recycles_queue_but_keeps_retained_payload() {
    use crate::task_execution::intent::TaskOperationSink;
    use novarocks_resource_contract::ExistingOwnerRetentionLease;
    use novarocks_workload_control::WorkloadShutdownError;

    let backend = BackendProcessId::new_v7();
    let mut peer = Loopback::start().await;
    let governed = GovernedWireRole::new(&peer);
    let mut runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let acks = TaskAckIntake::new(Arc::new(CountingWake::default()));
    let sink = NativeTaskOperationSink::new(
        &[peer.descriptor(backend)],
        TransportBudget::DEFAULT,
        test_attempt_facts(),
        acks.handle(),
        runtime.clone(),
        governed.authority(),
    )
    .unwrap();
    let supervisor = runtime.task_transport_supervisor().clone();
    let physical = supervisor.capacity_sources();
    let blocker = supervisor
        .try_reserve_queue(
            &sink.transport_waiter,
            backend,
            NativeTransportLane::Ordinary,
            physical.target_items,
            1,
        )
        .expect("the original target window is genuinely occupied");
    assert!(matches!(
        supervisor.try_reserve_queue(
            &sink.transport_waiter,
            backend,
            NativeTransportLane::Ordinary,
            1,
            1,
        ),
        Err(NativeTransportBackpressure::Target)
    ));

    // The existing fixture has exactly 128 Custody QueueRecords. Exceed that
    // total without changing any limit: a leaked queued record eventually
    // prevents a later paid preparation, even though no request was published.
    let mut retained_input: Option<(Box<[u8; 16]>, ExistingOwnerRetentionLease)> = None;
    for ordinal in 0..129u32 {
        let intent = update_intent(ordinal + 1, backend, 1);
        let request = intent
            .queue_request()
            .with_native_input(queued_header_quote(std::mem::size_of::<[u8; 16]>()).unwrap());
        let mut original_intent = Some(intent);
        let mut preparation = TaskOperationQueuePreparation::new();
        assert!(matches!(
            sink.try_reserve_queue(request, &mut preparation),
            TaskOperationQueueAdmission::TargetFull
        ));
        governed.drive();
        assert!(matches!(
            sink.try_reserve_queue(request, &mut preparation),
            TaskOperationQueueAdmission::TargetFull
        ));
        assert!(preparation.admission.is_none());
        assert!(preparation.ready.is_none());
        assert!(preparation.failure.is_none());
        let grant = preparation
            .grant
            .as_ref()
            .expect("same paid Q completed before the genuine physical refusal");
        // Construct only after the paid grant. The first immutable input is
        // handed to an independent retained receiver before its queue source
        // is removed; subsequent never-published inputs actually drop here.
        let input = Box::new([ordinal as u8; 16]);
        if ordinal == 0 {
            retained_input = Some((input, grant.retain_payload().unwrap()));
        } else {
            drop(input);
        }
        drop(original_intent.take());
        assert!(original_intent.is_none());
        preparation.withdraw_unpublished();
        assert!(preparation.grant.is_none());
        assert!(preparation.request.is_none());
        assert!(preparation.admission.is_none());
        assert!(preparation.ready.is_none());
        assert!(preparation.failure.is_none());
        governed.drive();
        let retained = retained_input.as_ref().unwrap();
        assert_eq!(retained.0.as_slice(), &[0; 16]);
        assert_eq!(retained.1.capacity_bytes().unwrap(), 16);
    }

    // Re-enter the same actual sink after the original physical holder exits.
    // Its returned permit performs both real physical and paid queue release.
    drop(blocker);
    let final_intent = update_intent(130, backend, 1);
    let final_permit = governed.reserve(&sink, &final_intent);
    drop(final_intent);
    drop(final_permit);
    governed.drive();
    assert!(peer.peer.applied().is_empty());
    assert_eq!(peer.peer.control_requests(), 0);

    drop(sink);
    drop(acks);
    drop(supervisor);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    runtime
        .drain_original_subscriptions_until(deadline)
        .await
        .unwrap();
    runtime.drain_original_apply_until(deadline).await.unwrap();
    runtime.drain_native_outgoing_until(deadline).await.unwrap();
    assert!(peer.settle().await);
    drop(runtime);
    drop(peer);

    let GovernedWireRole {
        control,
        driver,
        _root: root,
        window,
    } = governed;
    let mut driver = driver.into_inner().unwrap();
    drop(window);
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    control.close_admission();
    drive_original(&mut driver);
    // Completed root and withdrawn queue are insufficient to release the
    // independent original payload. The real unique process owner is returned.
    let failure = control
        .shutdown()
        .err()
        .expect("retained input still owns its paid record");
    assert_eq!(failure.error(), WorkloadShutdownError::NotDrained);
    let (_, control) = failure.into_parts();
    drop(retained_input.take());
    drive_original(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "same original governor truly drains after the final payload exit"
    );
}
