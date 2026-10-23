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

//! Real original paid claims/driver; no Native profile qualification.
use super::*;
use novarocks_resource_contract::SharedDependency;
use novarocks_workload_control::joint_admission::*;
use novarocks_workload_control::{
    ControlStorageConfig, WorkClass, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
    WorkloadControl,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new([0, 0, count, 0, 0], 1, 2, &[]);
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 2,
        max_registered_dependencies: 402,
        max_claims: 16,
        max_requests: 16,
        max_changes_per_request: 4,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    let base = storage.base_charge(&config).unwrap();
    let m = base.get(JointResource::MetadataBytes);
    let custody = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, 8)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::PayloadBytes, 64)
            .with(JointResource::UnreceivedProductions, 1),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, 7)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::PayloadBytes, 64)
            .with(JointResource::UnreceivedProductions, 1),
        protected_progress: base,
    };
    let foreground = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::PayloadBytes, 32)
            .with(JointResource::Computations, 1),
        per_dependency: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::PayloadBytes, 32)
            .with(JointResource::Computations, 1),
        protected_progress: JointResources::ZERO,
    };
    let global = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, 8)
            .with(JointResource::ClaimRecords, 16)
            .with(JointResource::MetadataBytes, m * 32)
            .with(JointResource::PayloadBytes, 96)
            .with(JointResource::UnreceivedProductions, 1)
            .with(JointResource::Computations, 1),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, 7)
            .with(JointResource::ClaimRecords, 16)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::PayloadBytes, 96)
            .with(JointResource::UnreceivedProductions, 1)
            .with(JointResource::Computations, 1),
        protected_progress: base,
    };
    config.global = global;
    config.classes[2] = global;
    config.lanes[0] = custody;
    config.lanes[1] = foreground;
    config.class_lanes[2][0] = custody;
    config.class_lanes[2][1] = foreground;
    // Only the original fixed substrate requires progress in this fixture.
    // CPU/complete Native action protection is deliberately not asserted.
    let mut required = RequiredJointProgress {
        global: base,
        classes: [JointResources::ZERO; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.classes[2] = base;
    required.lanes[0] = base;
    required.class_lanes[2][0] = base;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (handle, driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite driver failed to reach idle");
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("expected original paid reply"),
    }
}
fn bind(scope: &WorkScope, facts: &[SharedDependency], driver: &mut JointProgressDriver) {
    let mut proposal = scope.begin_control_binding().unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(driver);
    let mut reply = receive(&mut pending);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop(reply);
    drive(driver);
}
fn amounts(resource: JointResource, amount: u64) -> JointResources {
    JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::MetadataBytes, 0)
        .with(resource, amount)
}
fn request(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    lane: JointLane,
    amount: JointResources,
) -> JointAdmission {
    let mut proposal = handle.begin_paid_proposal(work).unwrap();
    proposal.begin_new(lane, amount).unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    proposal.submit().unwrap()
}
fn claim(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    lane: JointLane,
    amount: JointResources,
    driver: &mut JointProgressDriver,
) -> JointClaim {
    let mut pending = request(handle, work, facts, lane, amount);
    drive(driver);
    let mut reply = receive(&mut pending);
    let result = reply.new_claims.pop().unwrap();
    drop(reply);
    drive(driver);
    result
}

#[tokio::test]
async fn original_live_job_receives_the_root_grant_and_transfers_only_exact_stage_compute() {
    let (control, handle, mut driver) = fixture();
    let work = control
        .root_admission()
        .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
        .unwrap();
    let root_scope = work.owner.scope();
    let facts = [SharedDependency::LocalHost];
    bind(&root_scope, &facts, &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&root_scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let mut query = Some(root_scope.admit_query().unwrap().await.unwrap());
    let mut owner = Some(work.owner);
    let mut admission = None;
    StatisticsJobAdmission::install_internal_in(
        &mut owner,
        &mut query,
        &mut payload,
        &mut admission,
    )
    .unwrap();
    assert!(owner.is_none() && query.is_none() && payload.is_none());
    let repository = StatisticsJobRepository::new();
    let (job, notification) = repository
        .install_internal_job_in(
            StatisticsJobCreate {
                target: StatisticsTarget {
                    catalog: Arc::from("iceberg"),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                    object_id: Arc::from(&b"orders-v1"[..]),
                },
                columns: StatisticsColumns::All,
                submitted_at_ms: 1,
            },
            &mut admission,
        )
        .unwrap();
    assert!(admission.is_none());
    notification.notify();
    repository.claim_next(2).await.unwrap().unwrap();
    let stage = joint_child(&root_scope, &mut driver);
    let scope = stage.scope();
    let observer = stage.retirement_observer().unwrap();
    // This source is a pure capture under the original LiveJob lock. Body
    // allocation and proof polling happen only after that lock has exited.
    let mut proposal = repository
        .root_stage_source(job.id, &stage)
        .unwrap()
        .begin(4, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("expected the original closed batch"),
    };
    repository
        .install_root_stage_in(job.id, &stage, &mut ready)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive(&mut driver);
    let root_binding = repository.lock().unwrap().active[&job.id]
        .result_capacity
        .as_ref()
        .unwrap()
        .try_clone()
        .unwrap();
    let binding = root_binding.for_child(&scope).unwrap();
    let mut computation = None;
    let foreign = control
        .root_admission()
        .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
        .unwrap();
    assert!(
        repository
            .transfer_stage_computation_in(job.id, &foreign.owner, &mut computation)
            .is_err()
    );
    assert!(computation.is_none());
    assert!(
        repository.lock().unwrap().active[&job.id]
            .stage_computation
            .is_some()
    );
    foreign.owner.complete();
    drive(&mut driver);
    repository
        .transfer_stage_computation_in(job.id, &stage, &mut computation)
        .unwrap();
    let old = computation.as_ref().unwrap().retain_alias().unwrap();
    assert!(
        repository
            .transfer_stage_computation_in(job.id, &stage, &mut computation)
            .is_err()
    );
    assert!(old.shares_computation_with(&computation.as_ref().unwrap().retain_alias().unwrap()));
    let activity = binding.begin_internal_result_activity(old).unwrap();
    drop(computation);
    stage.complete();
    drive(&mut driver);
    assert!(!observer.is_reclaimed().unwrap());
    drop(activity);
    drive(&mut driver);
    assert!(observer.is_reclaimed().unwrap());
    // This test has no Native/provider operation. These are actual local
    // fixture exits, not a production terminal-to-convergence projection.
    repository
        .conclude(
            job.id,
            StatisticsJobPhase::Preparing,
            StatisticsJobConclusion::Failed,
            StatisticsPublicationFact::NotStarted,
            None,
            3,
        )
        .await
        .unwrap();
    repository
        .record_convergence(
            job.id,
            StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            },
            4,
        )
        .await
        .unwrap();
    drive(&mut driver);
    root_binding
        .try_window_alias()
        .unwrap()
        .check_backing_total(12)
        .unwrap();
    drop(binding);
    drop(root_binding);
    drive(&mut driver);
    assert!(root_scope.check().is_err());
}

fn joint_child(scope: &WorkScope, driver: &mut JointProgressDriver) -> WorkOwner {
    let mut pending = scope.request_joint_child().unwrap();
    drive(driver);
    let mut ready = receive(&mut pending);
    let mut target = None;
    ready
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(scope, &mut target)
        .unwrap()
        .notify();
    drop(ready);
    drop(pending);
    drive(driver);
    target.unwrap()
}
