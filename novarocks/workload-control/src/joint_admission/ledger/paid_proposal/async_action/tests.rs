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

//! Real original fixed ledger/proof; no physical worker/profile qualification.
use super::*;
use crate::{ControlStorageConfig, WorkloadConfig};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], 2, 2, &[]);
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: 4,
        max_registered_dependencies: (4 * 16 + 2 * 8 * 2 * 4 + 2 * 4 + 1) * 4,
        max_claims: 16,
        max_requests: 16,
        max_changes_per_request: 4,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    config.max_registered_dependencies =
        crate::joint_admission::ledger::metadata_geometry::maximum_fact_roots(&config, count * 2)
            .unwrap()
            .checked_mul(config.max_dependencies_per_claim)
            .unwrap();
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
            .with(JointResource::AsyncIo, 4)
            .with(JointResource::PhysicalQueueRecords, 4)
            .with(JointResource::PhysicalQueueBytes, 4096),
        per_dependency: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::AsyncIo, 4)
            .with(JointResource::PhysicalQueueRecords, 4)
            .with(JointResource::PhysicalQueueBytes, 4096),
        protected_progress: JointResources::ZERO,
    };
    let settlement = foreground;
    let global = JointResourceLimits {
        global: custody
            .global
            .checked_add(foreground.global)
            .unwrap()
            .checked_add(settlement.global)
            .unwrap(),
        per_dependency: custody
            .per_dependency
            .checked_add(foreground.per_dependency)
            .unwrap()
            .checked_add(settlement.per_dependency)
            .unwrap(),
        protected_progress: base,
    };
    config.global = global;
    config.classes[0] = global;
    config.lanes[0] = custody;
    config.lanes[1] = foreground;
    config.class_lanes[0][0] = custody;
    config.class_lanes[0][1] = foreground;
    config.lanes[2] = settlement;
    config.class_lanes[0][2] = settlement;
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
    crate::joint_admission::tests::drive(driver);
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("expected original paid reply"),
    }
}
fn fact(name: &str) -> SharedDependency {
    SharedDependency::Network(
        novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn root(control: &WorkloadControl) -> crate::RootWork {
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

fn install(
    action: &mut AsyncActionAdmission,
    driver: &mut JointProgressDriver,
) -> AsyncActionGrant {
    drive(driver);
    let mut target = None;
    assert!(matches!(
        action.poll_install_in(&mut Context::from_waker(Waker::noop()), &mut target),
        Poll::Ready(Ok(()))
    ));
    target.unwrap()
}
#[test]
fn async_source_preserves_complete_facts_and_occupied_destination() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let a = fact("catalog");
    let b = fact("storage");
    bind(&scope, &[a.clone(), b.clone()], &mut driver);
    let mut first = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    let mut target = Some(install(&mut first, &mut driver));
    let id = target.as_ref().unwrap().claims[0].id;
    let mut second = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    assert!(matches!(
        second.poll_install_in(&mut Context::from_waker(Waker::noop()), &mut target),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Conflict
        }))
    ));
    assert_eq!(target.as_ref().unwrap().claims[0].id, id);
    let state = control.inner.state.lock().unwrap();
    let row = &state.joint.as_ref().unwrap().claims[&id.unwrap()];
    assert_eq!(row.owner, ClaimOwner::PhysicalWork(scope.id));
    assert_eq!(row.active_amounts.get(JointResource::AsyncIo), 1);
    assert_eq!(row.active_facts.dependencies.len(), 2);
    drop(state);
    drop(target);
    drop(second);
    drive(&mut driver);
}
#[test]
fn original_async_fence_refuses_stale_foreground_and_preserves_settlement() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("catalog")], &mut driver);
    let stale = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap();
    bind(&scope, &[fact("storage")], &mut driver);
    assert!(matches!(
        stale.begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap()),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    assert!(
        work.owner
            .async_action_source(JointLane::Foreground)
            .is_err()
    );
    let mut admitted = work
        .owner
        .async_action_source(JointLane::Settlement)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    let grant = install(&mut admitted, &mut driver);
    assert_eq!(grant.claims.len(), 1);
    drop(grant);
    drive(&mut driver);
}
#[test]
fn root_payload_rekeys_live_async_rows_without_child_or_payload_pin_replacement() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("catalog")], &mut driver);
    let mut initial = work
        .owner
        .begin_root_payload(NonZeroU64::new(1).unwrap(), NonZeroU64::new(8).unwrap())
        .unwrap();
    drive(&mut driver);
    let mut payload = None;
    match initial.poll_install_in(
        &mut Context::from_waker(Waker::noop()),
        &scope,
        &mut payload,
    ) {
        Poll::Ready(Ok(n)) => n.notify(),
        _ => panic!("initial original payload"),
    }
    drop(initial);
    drive(&mut driver);
    let payload = payload.unwrap();
    let alias = payload.retain_alias().unwrap();
    let mut action = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    let grant = install(&mut action, &mut driver);
    drop(action);
    drive(&mut driver);
    let id = grant.claims[0].id.unwrap();
    let before = {
        let state = control.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .unwrap()
            .claims
            .iter()
            .find_map(|(id, r)| match r.custody {
                ClaimCustody::Payload { epoch, pins } => Some((*id, epoch, pins)),
                _ => None,
            })
            .unwrap()
    };
    let active = [&grant];
    let mut proposal = payload
        .root_payload_source(&scope, JointLane::Foreground, &active)
        .unwrap()
        .begin(4)
        .unwrap();
    proposal.push_dependency(&fact("new-storage")).unwrap();
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original Root batch"),
    };
    ready
        .install_in(&scope, &payload, &active)
        .unwrap()
        .notify();
    assert_eq!(alias.capacity_bytes().unwrap(), 12);
    let state = control.inner.state.lock().unwrap();
    let ledger = state.joint.as_ref().unwrap();
    let row = &ledger.claims[&before.0];
    assert!(
        matches!(row.custody,ClaimCustody::Payload{epoch,pins} if epoch==before.1&&pins==before.2)
    );
    assert_eq!(ledger.claims[&id].active_facts.dependencies.len(), 2);
    assert_eq!(state.nodes.len(), 1);
    drop(state);
    drop(ready);
    drop(pending);
    drop(grant);
    drop(alias);
    drop(payload);
    drive(&mut driver);
}
