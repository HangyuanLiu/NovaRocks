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

//! Actual bound-parent original subscription start; not a Native profile certificate.
use super::*;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, PollRoleSourceBacking, ResultCapacityConfig, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::{
    num::NonZeroUsize,
    task::{Context, Waker},
    time::Duration,
};

fn fixture() -> (
    WorkloadControl,
    JointProgressDriver,
    novarocks_workload_control::ResultCapacityHandle,
) {
    let count = 8;
    let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 2, 2, &[])
        .with_process_source_backing(
            PollRoleSourceBacking::from_layouts(
                &[std::alloc::Layout::from_size_align(
                    NativeSubscriptionOwner::resident_storage_bytes(count).unwrap(),
                    8,
                )
                .unwrap()],
                &[],
            )
            .unwrap(),
        );
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 2,
        max_registered_dependencies: 1,
        max_claims: 32,
        max_requests: 16,
        max_changes_per_request: 2,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    config.max_registered_dependencies = storage.minimum_dependency_records(&config).unwrap();
    let base = storage.base_charge(&config).unwrap();
    let metadata = base.get(JointResource::MetadataBytes);
    let encoding =
        2 * novarocks_task_codec::TransportBudget::DEFAULT.max_batch_encoded_bytes() as u64;
    let support = encoding + 1024 * 1024;
    let progress = JointResources::ZERO.with(JointResource::PayloadBytes, support);
    let custody = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, count as u64)
            .with(JointResource::ClaimRecords, 32)
            .with(JointResource::MetadataBytes, metadata * 128),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, (count - 1) as u64)
            .with(JointResource::ClaimRecords, 16)
            .with(JointResource::MetadataBytes, metadata * 64),
        protected_progress: base,
    };
    let execution = JointResourceLimits {
        global: JointResources::ZERO.with(JointResource::PayloadBytes, support * 8),
        per_dependency: JointResources::ZERO.with(JointResource::PayloadBytes, support * 4),
        protected_progress: progress,
    };
    let all_progress = base.with(JointResource::PayloadBytes, support * 2);
    let total = JointResourceLimits {
        global: custody
            .global
            .with(JointResource::PayloadBytes, support * 16),
        per_dependency: custody
            .per_dependency
            .with(JointResource::PayloadBytes, support * 8),
        protected_progress: all_progress,
    };
    config.global = total;
    config.classes[0] = total;
    config.lanes = [custody, execution, execution];
    config.class_lanes[0] = config.lanes;
    let mut required = RequiredJointProgress {
        global: all_progress,
        classes: [JointResources::ZERO; 5],
        lanes: [base, progress, progress],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.classes[0] = all_progress;
    required.class_lanes[0] = required.lanes;
    // This finite component supports only the control substrate/policy
    // read, not the complete production Native action/physical profile.
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    (control, driver, capacity)
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..100_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite driver did not become idle");
}

#[derive(Debug)]
struct Wake(std::sync::atomic::AtomicUsize);
impl StatusIntakeWake for Wake {
    fn wake(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct OriginalProbe {
    polled: Arc<std::sync::atomic::AtomicUsize>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}
impl Future for OriginalProbe {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.polled
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Poll::Ready(())
    }
}
impl Drop for OriginalProbe {
    fn drop(&mut self) {
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_parent_subscription_starts_after_same_child_receipt_and_q_reuse() {
    use std::sync::atomic::Ordering::SeqCst;
    let (control, mut driver, capacity) = fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = root.owner.scope();
    let mut proposal = scope.begin_control_binding().unwrap();
    let transport = novarocks_native_adapter::FrontendNativeTransport::plaintext();
    let endpoints = [
        novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 19010).unwrap(),
        novarocks_execution::runtime::endpoint::RuntimeEndpoint::new("control.test.invalid", 19061)
            .unwrap(),
    ];
    for endpoint in &endpoints {
        proposal
            .push_selected_network(
                transport
                    .selected_network_origin(endpoint.native_endpoint())
                    .unwrap(),
            )
            .unwrap();
    }
    proposal.end_change().unwrap();
    let mut first_bind = proposal.submit().unwrap();
    drive(&mut driver);
    let Poll::Ready(Ok(mut reply)) =
        Pin::new(&mut first_bind).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("actual original control binding must grant");
    };
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    drop(reply);
    drop(first_bind);
    drive(&mut driver);
    assert_eq!(
        scope.child_inherited().err(),
        Some(WorkError::NotReady),
        "the original production synchronous constructor rejects this exact bound parent"
    );
    let (mut owner, port) = NativeSubscriptionOwner::new(Handle::current(), 8).unwrap();
    owner.start_reaper().unwrap();
    let blockers = [
        scope.begin_control_binding().unwrap(),
        scope.begin_control_binding().unwrap(),
    ];
    let reservation = port.prepare(&scope, None, None).unwrap();
    let wake_impl = Arc::new(Wake(std::sync::atomic::AtomicUsize::new(0)));
    let wake: Arc<dyn StatusIntakeWake> = wake_impl.clone();
    assert!(!reservation.ready(&wake).unwrap());
    port.notify();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    tokio::time::timeout_at(deadline, async {
        loop {
            let waiting = {
                let state = owner.core.state.lock().unwrap();
                let row = state.records[reservation.index].as_ref().unwrap();
                let pending = row.input.child.lock().unwrap();
                let waiting = pending
                    .as_ref()
                    .is_some_and(|child| child.wait.is_some() && child.proposal.is_none());
                assert!(row.backing.owner.lock().unwrap().is_none());
                assert!(row.input.future.lock().unwrap().is_none());
                waiting
            };
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        control.snapshot().scopes.len(),
        1,
        "Q Full minted no child/ticket"
    );
    assert_eq!(
        wake_impl.0.load(SeqCst),
        0,
        "Pending is not original readiness"
    );
    for _ in 0..8 {
        assert!(!reservation.ready(&wake).unwrap());
        tokio::task::yield_now().await;
    }
    assert_eq!(
        wake_impl.0.load(SeqCst),
        0,
        "level reads do not wake their own runner"
    );
    drop(blockers);
    tokio::time::timeout_at(deadline, async {
        loop {
            drive(&mut driver);
            if reservation.ready(&wake).unwrap() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let child_scope = {
        let state = owner.core.state.lock().unwrap();
        let row = state.records[reservation.index].as_ref().unwrap();
        let child = row.backing.owner.lock().unwrap();
        child.as_ref().unwrap().scope()
    };
    let snap = control.snapshot();
    let child = snap
        .scopes
        .iter()
        .find(|s| s.id == child_scope.id())
        .unwrap();
    assert_eq!(child.parent, Some(scope.id()));
    assert_eq!(child.class, WorkClass::Query);
    assert!(
        wake_impl.0.load(SeqCst) > 0,
        "the original intake runner received readiness"
    );
    let polled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let lease = reservation
        .start(OriginalProbe {
            polled: polled.clone(),
            dropped: dropped.clone(),
        })
        .unwrap();
    tokio::time::timeout_at(deadline, async {
        loop {
            drive(&mut driver);
            if owner
                .core
                .state
                .lock()
                .unwrap()
                .records
                .iter()
                .all(Option::is_none)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(polled.load(SeqCst), 1);
    assert_eq!(dropped.load(SeqCst), 1, "actual same F destructor exited");
    drop(lease);
    assert_eq!(
        owner
            .drain_until(Instant::now() + Duration::from_secs(3))
            .await,
        DrainObservation::Joined
    );
    drop((child_scope, capacity));
    root.owner.complete_after_terminal_cancel_settled();
    root.business.release();
    control.close_admission();
    while let Some(original) = control.next_control() {
        original.acknowledge();
    }
    drive(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "same original child, slot and ledger must truly drain"
    );
}
