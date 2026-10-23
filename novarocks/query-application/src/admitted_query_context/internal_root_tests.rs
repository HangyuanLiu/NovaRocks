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
    ControlStorageConfig, WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
    WorkloadControl,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};
fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 1, 2, &[]);
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
    config.classes[0] = global;
    config.lanes[0] = custody;
    config.lanes[1] = foreground;
    config.class_lanes[0][0] = custody;
    config.class_lanes[0][1] = foreground;
    // Only the original fixed substrate requires progress in this fixture.
    // CPU/complete Native action protection is deliberately not asserted.
    let mut required = RequiredJointProgress {
        global: base,
        classes: [JointResources::ZERO; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.classes[0] = base;
    required.lanes[0] = base;
    required.class_lanes[0][0] = base;
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
fn fact(_name: &str) -> SharedDependency {
    SharedDependency::LocalHost
}
fn root(control: &WorkloadControl) -> novarocks_workload_control::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
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
fn finish(work: novarocks_workload_control::RootWork, driver: &mut JointProgressDriver) {
    work.business.release();
    work.owner.complete();
    drive(driver);
}

#[test]
fn actual_job_activity_keeps_only_its_stage_compute_while_root_payload_remains_live() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let root_scope = work.owner.scope();
    let facts = [fact("host")];
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
    let stage = joint_child(&root_scope, &mut driver);
    let stage_scope = stage.scope();
    bind(&stage_scope, &facts, &mut driver);
    let observer = stage.retirement_observer().unwrap();
    let mut raw_compute = Some(claim(
        &handle,
        &stage,
        &facts,
        JointLane::Foreground,
        amounts(JointResource::Computations, 1),
        &mut driver,
    ));
    let mut compute = None;
    InternalComputationGrant::adopt_in(&stage_scope, &mut raw_compute, &mut compute)
        .unwrap()
        .notify();
    let binding = QueryResultCapacityBinding::from_internal_payload(
        &root_scope,
        &stage_scope,
        payload.as_ref().unwrap(),
    )
    .unwrap();
    assert!(
        matches!(binding.begin_result_activity(), Err(WorkError::Conflict)),
        "payload-only binding cannot mint an activity"
    );
    let lease = binding
        .begin_internal_result_activity(compute.as_ref().unwrap().retain_alias().unwrap())
        .unwrap();
    let mut exit = Box::pin(binding.result_activities_exited());
    assert!(
        exit.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(compute);
    stage.complete();
    drive(&mut driver);
    assert!(!observer.is_reclaimed().unwrap());
    drop(lease);
    drive(&mut driver);
    assert!(
        exit.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(
        observer.is_reclaimed().unwrap(),
        "Root bytes must not pin old stage control after actual activity exit"
    );
    drop(exit);
    binding
        .try_window_alias()
        .unwrap()
        .check_backing_total(8)
        .unwrap();
    let next = joint_child(&root_scope, &mut driver);
    next.complete();
    drive(&mut driver);
    drop(binding);
    drop(payload);
    finish(work, &mut driver);
}

#[test]
fn successive_real_closed_batches_reuse_the_root_payload_after_each_actual_activity_exit() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
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
    let grant = payload.as_ref().unwrap();
    let original =
        QueryResultCapacityBinding::from_internal_payload(&root_scope, &root_scope, grant).unwrap();
    let old = original.try_window_alias().unwrap();
    for expected_bytes in [12, 16] {
        let stage = joint_child(&root_scope, &mut driver);
        let scope = stage.scope();
        let observer = stage.retirement_observer().unwrap();
        let mut proposal = grant
            .root_stage_source(&root_scope, &stage)
            .unwrap()
            .begin(4, JointLane::Foreground)
            .unwrap();
        proposal.end_payload().unwrap();
        proposal.end_stage().unwrap();
        let mut admission = proposal.submit().unwrap();
        drive(&mut driver);
        let mut ready = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("the same Root batch must receive only after the previous activity exits"),
        };
        let mut computation = None;
        ready
            .install_in(&root_scope, grant, &stage, &mut computation)
            .unwrap()
            .notify();
        drop(ready);
        drop(admission);
        drive(&mut driver);
        let binding = original.for_child(&scope).unwrap();
        let view = binding.try_window_alias().unwrap();
        assert!(old.shares_capacity_with(&view));
        old.check_backing_total(expected_bytes).unwrap();
        assert!(matches!(
            old.check_backing_total(expected_bytes + 1),
            Err(WorkError::Capacity(_))
        ));
        let activity = binding
            .begin_internal_result_activity(computation.as_ref().unwrap().retain_alias().unwrap())
            .unwrap();
        drop(computation);
        stage.complete();
        drive(&mut driver);
        assert!(!observer.is_reclaimed().unwrap());
        drop(activity);
        drive(&mut driver);
        assert!(observer.is_reclaimed().unwrap());
        // Both the old Root bytes and this stage's payload-only projection
        // remain alive while the original slot becomes reusable.
        assert!(old.shares_capacity_with(&view));
        drop(view);
        drop(binding);
    }
    drop(old);
    drop(original);
    drop(payload);
    finish(work, &mut driver);
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
