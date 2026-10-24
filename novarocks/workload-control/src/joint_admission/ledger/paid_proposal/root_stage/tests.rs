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

//! Original paid builder/proof/claim behavior; no production profile claim.
use super::*;
use crate::{ControlStorageConfig, WorkloadConfig};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn fixture_with_productions(
    changes: usize,
    production: bool,
) -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    fixture_with_native_resources(changes, production, false)
}
fn fixture_with_native_resources(
    changes: usize,
    production: bool,
    native: bool,
) -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
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
        max_changes_per_request: changes,
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
            .with(JointResource::UnreceivedProductions, 1)
            .with(JointResource::Publications, u64::from(production))
            .with(JointResource::ReplyRecords, if native { 2 } else { 0 })
            .with(JointResource::QueueRecords, if native { 2 } else { 0 }),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, 7)
            .with(JointResource::ClaimRecords, 12)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::PayloadBytes, 64)
            .with(JointResource::UnreceivedProductions, 1)
            .with(JointResource::Publications, u64::from(production))
            .with(JointResource::ReplyRecords, if native { 2 } else { 0 })
            .with(JointResource::QueueRecords, if native { 2 } else { 0 }),
        protected_progress: base,
    };
    let foreground = JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 16)
            .with(JointResource::PayloadBytes, 32)
            .with(JointResource::Computations, 1)
            .with(JointResource::AsyncIo, if native { 3 } else { 0 })
            .with(
                JointResource::PhysicalQueueRecords,
                if native { 2 } else { 0 },
            )
            .with(
                JointResource::PhysicalQueueBytes,
                if native { 8 } else { 0 },
            ),
        per_dependency: JointResources::ZERO
            .with(JointResource::ClaimRecords, 4)
            .with(JointResource::MetadataBytes, m * 8)
            .with(JointResource::PayloadBytes, 32)
            .with(JointResource::Computations, 1)
            .with(JointResource::AsyncIo, if native { 3 } else { 0 })
            .with(
                JointResource::PhysicalQueueRecords,
                if native { 2 } else { 0 },
            )
            .with(
                JointResource::PhysicalQueueBytes,
                if native { 8 } else { 0 },
            ),
        protected_progress: JointResources::ZERO,
    };
    let global = JointResourceLimits {
        global: custody.global.checked_add(foreground.global).unwrap(),
        per_dependency: custody
            .per_dependency
            .checked_add(foreground.per_dependency)
            .unwrap(),
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
    let mut result = crate::ResultCapacityConfig::V1;
    result.all_objects_bytes[0] = 8;
    control.configure_result_capacity(result).unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}
pub(in crate::joint_admission::ledger::paid_proposal) fn drive(driver: &mut JointProgressDriver) {
    crate::joint_admission::tests::drive(driver);
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("expected original paid reply"),
    }
}
pub(in crate::joint_admission::ledger::paid_proposal) fn fact(name: &str) -> SharedDependency {
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
fn setup(
    unbound_stage: bool,
) -> (
    WorkloadControl,
    JointAdmissionHandle,
    JointProgressDriver,
    crate::RootWork,
    WorkOwner,
    Option<RetainedPayloadGrant>,
) {
    setup_with_production(unbound_stage, 4, false)
}
pub(in crate::joint_admission::ledger::paid_proposal) fn setup_with_production(
    unbound_stage: bool,
    changes: usize,
    production: bool,
) -> (
    WorkloadControl,
    JointAdmissionHandle,
    JointProgressDriver,
    crate::RootWork,
    WorkOwner,
    Option<RetainedPayloadGrant>,
) {
    let (control, handle, mut driver) = fixture_with_productions(changes, production);
    let work = root(&control);
    // Exercise the existing empty-base child only while its parent is empty.
    // Neither branch starts a stage or makes an I/O/execution claim.
    let early = if unbound_stage {
        Some(work.owner.scope().child_inherited().unwrap())
    } else {
        None
    };
    let a = fact("a");
    bind(&work.owner.scope(), &[a.clone()], &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &[a],
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&work.owner.scope(), &mut raw, &mut payload)
        .unwrap()
        .notify();
    let stage = early.unwrap_or_else(|| child(&work.owner.scope(), &mut driver));
    (control, handle, driver, work, stage, payload)
}
pub(in crate::joint_admission::ledger::paid_proposal) fn child(
    root: &WorkScope,
    driver: &mut JointProgressDriver,
) -> WorkOwner {
    let mut pending = root.request_joint_child().unwrap();
    drive(driver);
    let mut ready = receive(&mut pending);
    let mut target = None;
    ready
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(root, &mut target)
        .unwrap()
        .notify();
    drop(ready);
    drive(driver);
    target.unwrap()
}
pub(in crate::joint_admission::ledger::paid_proposal) fn batch(
    payload: &RetainedPayloadGrant,
    work: &crate::RootWork,
    stage: &WorkOwner,
    delta: u64,
) -> RootStageAdmission {
    let mut proposal = payload
        .root_stage_source(&work.owner.scope(), stage)
        .unwrap()
        .begin(delta, JointLane::Foreground)
        .unwrap();
    proposal.push_payload_dependency(&fact("b")).unwrap();
    proposal.end_payload().unwrap();
    proposal.push_stage_dependency(&fact("c")).unwrap();
    proposal.end_stage().unwrap();
    proposal.submit().unwrap()
}
pub(in crate::joint_admission::ledger::paid_proposal) fn ready(
    admission: &mut RootStageAdmission,
) -> RootStageReady {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(value)) => value,
        _ => panic!("closed original batch verdict"),
    }
}
fn capacity(control: &WorkloadControl, resource: JointResource) -> u64 {
    control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims
        .values()
        .map(|row| row.amounts.get(resource))
        .sum()
}
fn payload_state(control: &WorkloadControl, id: u64) -> Option<(u64, usize, u64, usize)> {
    let state = control.inner.state.lock().unwrap();
    state.joint.as_ref().unwrap().claims.get(&id).map(|row| {
        let ClaimCustody::Payload { epoch, pins } = row.custody else {
            panic!("same typed original record")
        };
        (
            epoch,
            pins,
            row.active_amounts.get(JointResource::PayloadBytes),
            row.active_facts.dependencies.len(),
        )
    })
}

#[test]
fn same_original_payload_and_exact_child_install_once_with_full_control_union() {
    for unbound_stage in [false, true] {
        let (control, _, mut driver, work, stage, payload) = setup(unbound_stage);
        let payload = payload.unwrap();
        let alias = payload.retain_alias().unwrap();
        let source = payload
            .root_stage_source(&work.owner.scope(), &stage)
            .unwrap();
        let id = source.fence.payload;
        let before = payload_state(&control, id).unwrap();
        drop(source);
        let mut admission = batch(&payload, &work, &stage, 4);
        drive(&mut driver);
        let mut reply = ready(&mut admission);
        assert_eq!(capacity(&control, JointResource::PayloadBytes), 12);
        assert_eq!(alias.capacity_bytes().unwrap(), 8);
        assert_eq!(payload_state(&control, id).unwrap(), before);
        let mut compute = None;
        let notification = reply
            .install_in(&work.owner.scope(), &payload, &stage, &mut compute)
            .unwrap();
        let after = payload_state(&control, id).unwrap();
        assert_eq!(
            (after.0, after.1, after.2, after.3),
            (before.0, before.1, 12, 2)
        );
        assert_eq!(alias.capacity_bytes().unwrap(), 12);
        let compute_alias = compute.as_ref().unwrap().retain_alias().unwrap();
        assert!(compute_alias.is_for_scope(&stage.scope()));
        assert!(!compute_alias.is_for_scope(&work.owner.scope()));
        {
            let state = control.inner.state.lock().unwrap();
            for owner in [work.owner.scope().id, stage.scope().id] {
                let claim = state.cancellations.slots[state.nodes[&owner].cancellation.0]
                    .claim
                    .unwrap();
                assert_eq!(
                    state.joint.as_ref().unwrap().claims[&claim]
                        .active_facts
                        .dependencies
                        .len(),
                    3
                );
            }
            assert_eq!(
                state
                    .joint
                    .as_ref()
                    .unwrap()
                    .claims
                    .values()
                    .filter(
                        |row| row.owner == ClaimOwner::PhysicalWork(stage.scope().id)
                            && matches!(row.custody, ClaimCustody::Computation { .. })
                    )
                    .count(),
                1
            );
        }
        notification.notify();
        drop(reply);
        drop(compute);
        assert_eq!(capacity(&control, JointResource::Computations), 1);
        drop(compute_alias);
        assert_eq!(capacity(&control, JointResource::Computations), 0);
        assert_eq!(alias.capacity_bytes().unwrap(), 12);
        drop(alias);
        drop(payload);
        stage.complete();
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
        assert_eq!(capacity(&control, JointResource::PayloadBytes), 0);
    }
}

#[test]
fn ready_drop_and_actual_last_payload_pin_rollback_original_typed_record_exactly() {
    for scenario in 0..3 {
        let last_pin = scenario != 0;
        let (control, _, mut driver, work, stage, mut payload) = setup(false);
        let source = payload
            .as_ref()
            .unwrap()
            .root_stage_source(&work.owner.scope(), &stage)
            .unwrap();
        let id = source.fence.payload;
        let before = payload_state(&control, id).unwrap();
        drop(source);
        let mut admission = batch(payload.as_ref().unwrap(), &work, &stage, 4);
        if last_pin {
            let reply = if scenario == 2 {
                drive(&mut driver);
                Some(ready(&mut admission))
            } else {
                None
            };
            // The capture/proposal adds no typed pin. The actual last pin
            // exits while Waiting or while its private Delivered reply lives.
            drop(payload.take());
            if scenario == 1 {
                drive(&mut driver);
                assert!(matches!(
                    Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Err(_))
                ));
            }
            drop(reply);
            assert!(payload_state(&control, id).is_none());
        } else {
            drive(&mut driver);
            let reply = ready(&mut admission);
            drop(reply);
            assert_eq!(payload_state(&control, id).unwrap(), before);
        }
        assert_eq!(capacity(&control, JointResource::Computations), 0);
        assert_eq!(
            capacity(&control, JointResource::PayloadBytes),
            if last_pin { 0 } else { 8 }
        );
        assert_eq!(
            control.inner.state.lock().unwrap().nodes[&stage.scope().id].resource_waiters,
            0
        );
        drop(payload);
        stage.complete();
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
    }
}

#[test]
fn stale_capture_stage_cancel_and_driver_close_cannot_publish_a_new_stage() {
    // A captured weak source does not keep an already-completed child alive.
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let source = payload
        .root_stage_source(&work.owner.scope(), &stage)
        .unwrap();
    stage.complete();
    drive(&mut driver);
    assert!(source.begin(4, JointLane::Foreground).is_err());
    drop(payload);
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    drop(control);
    // Root completion/cancellation are checked again after pure capture.
    let (_process, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let source = payload
        .root_stage_source(&work.owner.scope(), &stage)
        .unwrap();
    work.owner.complete();
    assert!(source.begin(4, JointLane::Foreground).is_err());
    drop(payload);
    stage.complete();
    work.business.release();
    drive(&mut driver);
    let (_process, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let source = payload
        .root_stage_source(&work.owner.scope(), &stage)
        .unwrap();
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    assert!(source.begin(4, JointLane::Foreground).is_err());
    drop(payload);
    stage.complete_after_terminal_cancel_settled();
    work.business.release();
    work.owner.complete_after_terminal_cancel_settled();
    drive(&mut driver);
    for after_grant in [false, true] {
        let (control, _, mut driver, work, stage, payload) = setup(false);
        let payload = payload.unwrap();
        let mut admission = batch(&payload, &work, &stage, 4);
        if after_grant {
            drive(&mut driver);
            let mut reply = ready(&mut admission);
            stage.cancel(crate::CancellationReason::ServerShutdown);
            let mut target = None;
            assert!(
                reply
                    .install_in(&work.owner.scope(), &payload, &stage, &mut target)
                    .is_err()
            );
            assert!(target.is_none());
            drop(reply);
        } else {
            stage.cancel(crate::CancellationReason::ServerShutdown);
            drive(&mut driver);
            assert!(matches!(
                Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(_))
            ));
        }
        assert_eq!(capacity(&control, JointResource::PayloadBytes), 8);
        assert_eq!(capacity(&control, JointResource::Computations), 0);
        drop(payload);
        stage.complete_after_terminal_cancel_settled();
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
    }
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let mut admission = batch(&payload, &work, &stage, 4);
    drive(&mut driver);
    let mut reply = ready(&mut admission);
    drop(driver);
    let mut target = None;
    assert!(matches!(
        reply.install_in(&work.owner.scope(), &payload, &stage, &mut target),
        Err(WorkError::Closed)
    ));
    assert!(target.is_none());
    drop(reply);
    assert_eq!(capacity(&control, JointResource::PayloadBytes), 8);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
    drop(payload);
    stage.complete();
    work.business.release();
    work.owner.complete();
}

#[test]
fn complete_finite_refusal_keeps_root_payload_and_existing_compute_without_partial_growth() {
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let mut first = batch(&payload, &work, &stage, 4);
    drive(&mut driver);
    let mut first = ready(&mut first);
    let mut compute = None;
    first
        .install_in(&work.owner.scope(), &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop(first);
    drive(&mut driver);
    let other = child(&work.owner.scope(), &mut driver);
    let mut refused = batch(&payload, &work, &other, 4);
    drive(&mut driver);
    assert!(matches!(
        Pin::new(&mut refused).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::JointCapacity(_),
            ..
        }))
    ));
    assert_eq!(capacity(&control, JointResource::PayloadBytes), 12);
    assert_eq!(capacity(&control, JointResource::Computations), 1);
    assert_eq!(
        payload.retain_alias().unwrap().capacity_bytes().unwrap(),
        12
    );
    // The original epoch survives both a successful growth and withdrawal.
    // Release the old actual position, then reuse the same Root and child.
    let alias = payload.retain_alias().unwrap();
    let original = payload
        .root_stage_source(&work.owner.scope(), &other)
        .unwrap();
    let id = original.fence.payload;
    let before_reuse = payload_state(&control, id).unwrap();
    drop(original);
    drop(compute);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
    let mut next = batch(&payload, &work, &other, 4);
    drive(&mut driver);
    let mut next = ready(&mut next);
    let mut next_compute = None;
    next.install_in(&work.owner.scope(), &payload, &other, &mut next_compute)
        .unwrap()
        .notify();
    let after_reuse = payload_state(&control, id).unwrap();
    assert_eq!(
        (after_reuse.0, after_reuse.1),
        (before_reuse.0, before_reuse.1)
    );
    assert_eq!(alias.capacity_bytes().unwrap(), 16);
    assert_eq!(capacity(&control, JointResource::Computations), 1);
    drop(next);
    drop(next_compute);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
    drop(payload);
    assert_eq!(capacity(&control, JointResource::PayloadBytes), 16);
    drop(alias);
    assert_eq!(capacity(&control, JointResource::PayloadBytes), 0);
    other.complete();
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
}

#[test]
fn root_stage_original_batch_also_installs_its_roots_unknown_production_domains() {
    let (control, _, mut driver, work, stage, payload) = setup_with_production(false, 5, true);
    let payload = payload.unwrap();
    let scope = work.owner.scope();
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let quote = UnreceivedProductionQuote::new(
        std::num::NonZeroU64::new(1).unwrap(),
        std::num::NonZeroUsize::new(8).unwrap(),
    )
    .unwrap();
    let mut pending = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote)
        .unwrap();
    drive(&mut driver);
    let mut production_ready =
        match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("actual production Ready"),
        };
    let mut facts = Some(vec![45; 8].into_boxed_slice());
    let mut production = None;
    production_ready
        .register_in(&mut facts, &mut production)
        .unwrap()
        .notify();
    drop((production_ready, pending));
    let source = payload.root_stage_source(&scope, &stage).unwrap();
    assert_eq!(source.required_change_records().unwrap(), 5);
    drop(source);
    let mut admission = batch(&payload, &work, &stage, 4);
    drive(&mut driver);
    let mut reply = ready(&mut admission);
    assert!(matches!(
        production.as_ref().unwrap().check_receive(),
        Err(WorkError::Conflict)
    ));
    let mut compute = None;
    reply
        .install_in(&scope, &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    {
        let state = scope.inner.state.lock().unwrap();
        let row = state
            .joint
            .as_ref()
            .unwrap()
            .claims
            .values()
            .find(|row| {
                row.owner == ClaimOwner::PhysicalWork(scope.id)
                    && matches!(row.custody, ClaimCustody::Production { .. })
            })
            .unwrap();
        assert_eq!(
            row.active_facts.dependencies.len(),
            3,
            "a/b/c follow the complete control union, not narrow payload a/b"
        );
        assert_eq!(row.amounts, row.active_amounts);
        assert!(row.pending.is_none());
    }
    production.as_ref().unwrap().receive_in().unwrap().notify();
    production
        .as_ref()
        .unwrap()
        .owner()
        .settle_received_in()
        .unwrap()
        .notify();
    drop((production, reply, admission, compute, payload));
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
}

#[test]
fn complete_payload_increment_refuses_before_paid_body_and_preserves_same_source_for_retry() {
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let alias = payload.retain_alias().unwrap();
    let source = payload
        .root_stage_source(&work.owner.scope(), &stage)
        .unwrap();
    let id = source.fence.payload;
    let bound = source.maximum_protected_payload_increment().unwrap();
    assert_eq!(bound, 16);
    let before = payload_state(&control, id).unwrap();
    let before_claims = {
        let state = control.inner.state.lock().unwrap();
        state.joint.as_ref().unwrap().claims.len()
    };
    assert!(matches!(
        source.begin(bound + 1, JointLane::Foreground),
        Err(WorkError::Capacity(
            "Root/stage protected payload increment"
        ))
    ));
    assert_eq!(payload_state(&control, id).unwrap(), before);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.joint.as_ref().unwrap().claims.len(), before_claims);
        assert_eq!(state.nodes[&work.owner.scope().id].resource_waiters, 0);
    }
    let mut admission = batch(&payload, &work, &stage, 4);
    drive(&mut driver);
    let mut reply = ready(&mut admission);
    let mut compute = None;
    let notification = reply
        .install_in(&work.owner.scope(), &payload, &stage, &mut compute)
        .unwrap();
    let after = payload_state(&control, id).unwrap();
    assert_eq!((after.0, after.1), (before.0, before.1));
    assert_eq!(alias.capacity_bytes().unwrap(), 12);
    notification.notify();
    drop(reply);
    drop(compute);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
    drop(alias);
    drop(payload);
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(capacity(&control, JointResource::PayloadBytes), 0);
}

fn installed_binding_computation(
    payload: &RetainedPayloadGrant,
    work: &crate::RootWork,
    stage: &WorkOwner,
    driver: &mut JointProgressDriver,
) -> InternalComputationGrant {
    let mut admission = batch(payload, work, stage, 4);
    drive(driver);
    let mut ready = ready(&mut admission);
    let mut computation = None;
    ready
        .install_in(&work.owner.scope(), payload, stage, &mut computation)
        .unwrap()
        .notify();
    drop((ready, admission));
    drive(driver);
    computation.unwrap()
}
fn internal_binding_batch(
    binding: &InternalManifestBinding,
    selected: &SharedDependency,
) -> RootStageAdmission {
    let mut proposal = match binding.try_begin().unwrap() {
        JointTryBegin::Started(proposal) => proposal,
        JointTryBegin::Full(_) => panic!("original Q must be reaped before this component step"),
    };
    proposal.push_payload_dependency(selected).unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    proposal.submit().unwrap()
}
fn typed_rows(control: &WorkloadControl) -> Vec<(u64, JointResources, u64, usize, Vec<u64>)> {
    let state = control.inner.state.lock().unwrap();
    state
        .joint
        .as_ref()
        .unwrap()
        .claims
        .iter()
        .filter_map(|(&id, row)| {
            let (epoch, pins) = match row.custody {
                ClaimCustody::Payload { epoch, pins }
                | ClaimCustody::Computation { epoch, pins } => (epoch, pins),
                _ => return None,
            };
            Some((
                id,
                row.active_amounts,
                epoch,
                pins,
                row.active_facts
                    .dependencies
                    .iter()
                    .map(|d| d.ordinal)
                    .collect(),
            ))
        })
        .collect()
}

#[test]
fn internal_manifest_binding_reuses_all_four_originals_and_productions_without_new_claims() {
    for received in [false, true] {
        let (control, _, mut driver, work, stage, payload) = setup_with_production(false, 5, true);
        let payload = payload.unwrap();
        let scope = work.owner.scope();
        let origin: Arc<dyn Send + Sync> = Arc::new(());
        let quote = UnreceivedProductionQuote::new(
            std::num::NonZeroU64::new(1).unwrap(),
            std::num::NonZeroUsize::new(8).unwrap(),
        )
        .unwrap();
        let mut admission = scope
            .begin_unreceived_productions(Arc::downgrade(&origin), quote)
            .unwrap();
        drive(&mut driver);
        let Poll::Ready(Ok(mut production_ready)) =
            Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("original production admission")
        };
        let mut facts = Some(vec![45; 8].into_boxed_slice());
        let mut production = None;
        production_ready
            .register_in(&mut facts, &mut production)
            .unwrap()
            .notify();
        drop((production_ready, admission));
        drive(&mut driver);
        let compute = installed_binding_computation(&payload, &work, &stage, &mut driver);
        if received {
            production.as_ref().unwrap().receive_in().unwrap().notify();
        }
        let alias =
            crate::ResultWindowAlias::from_internal_computation(&scope, &stage, &payload, &compute)
                .unwrap();
        let retained = alias.try_clone().unwrap();
        let binding = alias.internal_manifest_binding().unwrap().unwrap().clone();
        let compute_alias = compute.retain_alias().unwrap();
        let before = typed_rows(&control);
        let mut refused = match binding.try_begin().unwrap() {
            JointTryBegin::Started(proposal) => proposal,
            JointTryBegin::Full(_) => panic!("the actual Q has been reaped"),
        };
        refused.push_payload_dependency(&fact("d")).unwrap();
        assert!(
            matches!(
                refused.push_payload_dependency(&fact("e")),
                Err(WorkError::Capacity(_))
            ),
            "full old/new union rejects the fifth actual origin before formation"
        );
        drop(refused);
        drive(&mut driver);
        assert_eq!(typed_rows(&control), before);
        let before_ids: Vec<_> = control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .claims
            .keys()
            .copied()
            .collect();
        assert_ne!(
            before[0].4, before[1].4,
            "original payload was narrower than its stage computation"
        );
        let blockers = [
            scope.begin_control_binding().unwrap(),
            scope.begin_control_binding().unwrap(),
        ];
        let pending_before = control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .requests
            .len();
        assert!(matches!(
            binding.try_begin().unwrap(),
            JointTryBegin::Full(_)
        ));
        assert!(
            matches!(
                payload
                    .root_stage_source(&scope, &stage)
                    .unwrap()
                    .try_begin(0, JointLane::Foreground)
                    .unwrap(),
                JointTryBegin::Full(_)
            ),
            "the unchanged New mode also defers only its original unacquired Q"
        );
        assert_eq!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .requests
                .len(),
            pending_before,
            "Full has no new ticket"
        );
        assert_eq!(
            typed_rows(&control),
            before,
            "pure capability/Q Full adds no typed pins"
        );
        drop(blockers);
        drive(&mut driver);
        let mut admission = internal_binding_batch(&binding, &fact("d"));
        drive(&mut driver);
        let mut ready = ready(&mut admission);
        assert!(
            ready.batch.new_claims.is_empty(),
            "the existing mode cannot mint a second computation"
        );
        let before_install = typed_rows(&control);
        assert_eq!(
            before_install, before,
            "provisional D must not publish active D"
        );
        let notification = ready.install_binding_in(&binding).unwrap();
        let after = typed_rows(&control);
        assert_eq!(
            before
                .iter()
                .map(|r| (&r.0, &r.1, &r.2, &r.3))
                .collect::<Vec<_>>(),
            after
                .iter()
                .map(|r| (&r.0, &r.1, &r.2, &r.3))
                .collect::<Vec<_>>(),
            "amounts/typed keys/epochs/pins are preserved"
        );
        assert_eq!(after[0].4, after[1].4);
        assert_eq!(
            after[0].4.len(),
            4,
            "old a/b/c and actual manifest d form the complete union"
        );
        {
            let state = control.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().unwrap();
            assert_eq!(
                ledger.claims.keys().copied().collect::<Vec<_>>(),
                before_ids
            );
            for row in ledger.claims.values().filter(|row| match row.owner {
                ClaimOwner::PhysicalWork(id) => id == scope.id || id == stage.scope().id,
                ClaimOwner::ControlSlot(slot) => [scope.id, stage.scope().id]
                    .iter()
                    .any(|id| state.nodes[id].cancellation == slot),
                _ => false,
            }) {
                assert_eq!(
                    row.active_facts
                        .dependencies
                        .iter()
                        .map(|d| d.ordinal)
                        .collect::<Vec<_>>(),
                    after[0].4
                );
                assert_eq!(row.amounts, row.active_amounts);
                assert!(row.pending.is_none());
            }
        }
        notification.notify();
        assert!(
            ready.install_binding_in(&binding).is_err(),
            "same receipt installs once"
        );
        assert!(retained.internal_manifest_binding().unwrap().is_some());
        assert!(
            crate::ResultWindowAlias::from_internal_payload(&scope, &stage.scope(), &payload)
                .unwrap()
                .internal_manifest_binding()
                .unwrap()
                .is_none(),
            "retention alone never grants binding authority"
        );
        if !received {
            production.as_ref().unwrap().receive_in().unwrap().notify();
        }
        production
            .as_ref()
            .unwrap()
            .owner()
            .settle_received_in()
            .unwrap()
            .notify();
        drop((
            ready,
            admission,
            production,
            origin,
            retained,
            alias,
            compute_alias,
            compute,
            payload,
        ));
        stage.complete();
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }
}

#[test]
fn internal_manifest_binding_cannot_retarget_or_reanimate_cancelled_or_unpinned_sources() {
    for delivered in [false, true] {
        let (control, _, mut driver, work, stage, payload) = setup(false);
        let payload = payload.unwrap();
        let compute = installed_binding_computation(&payload, &work, &stage, &mut driver);
        let alias = crate::ResultWindowAlias::from_internal_computation(
            &work.owner.scope(),
            &stage,
            &payload,
            &compute,
        )
        .unwrap();
        let binding = alias.internal_manifest_binding().unwrap().unwrap().clone();
        let other = child(&work.owner.scope(), &mut driver);
        assert!(
            crate::ResultWindowAlias::from_internal_computation(
                &work.owner.scope(),
                &other,
                &payload,
                &compute
            )
            .is_err()
        );
        let before = typed_rows(&control);
        let mut admission = internal_binding_batch(&binding, &fact("d"));
        let mut reply = if delivered {
            drive(&mut driver);
            Some(ready(&mut admission))
        } else {
            None
        };
        stage.cancel(crate::CancellationReason::ServerShutdown);
        if let Some(reply) = reply.as_mut() {
            assert!(reply.install_binding_in(&binding).is_err());
        } else {
            drive(&mut driver);
            assert!(matches!(
                Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(_))
            ));
        }
        drop((reply, admission));
        drive(&mut driver);
        assert_eq!(
            typed_rows(&control),
            before,
            "refusal/rollback does not change active D or pins"
        );
        assert!(binding.try_begin().is_err());
        drop((alias, compute, payload));
        other.complete();
        stage.complete_after_terminal_cancel_settled();
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }
    for computation in [false, true] {
        let (control, _, mut driver, work, stage, payload) = setup(false);
        let payload = payload.unwrap();
        let compute = installed_binding_computation(&payload, &work, &stage, &mut driver);
        let alias = crate::ResultWindowAlias::from_internal_computation(
            &work.owner.scope(),
            &stage,
            &payload,
            &compute,
        )
        .unwrap();
        let binding = alias.internal_manifest_binding().unwrap().unwrap().clone();
        let mut admission = internal_binding_batch(&binding, &fact("d"));
        let mut payload = Some(payload);
        let mut compute = Some(compute);
        drop(alias);
        if computation {
            drop(compute.take());
        } else {
            drop(payload.take());
        }
        drive(&mut driver);
        assert!(
            matches!(
                Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(_))
            ),
            "a weak capture cannot keep a last typed pin alive"
        );
        assert!(binding.try_begin().is_err());
        drop((admission, payload, compute));
        stage.complete();
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }
}

#[test]
fn qualification_union_keeps_old_stage_only_s_and_cancel_preserves_typed_sources() {
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let computation = installed_binding_computation(&payload, &work, &stage, &mut driver);
    let window = crate::ResultWindowAlias::from_internal_computation(
        &work.owner.scope(),
        &stage,
        &payload,
        &computation,
    )
    .unwrap();
    let binding = window.internal_manifest_binding().unwrap().unwrap().clone();
    let before = typed_rows(&control);
    assert_ne!(
        before[0].4, before[1].4,
        "stage-only S is absent from the original payload"
    );
    // Old a/b/c come from the original payload/stage preparation. Here c is
    // S, an old compute-only responsibility. Only successor B=d is selected.
    let mut admission = internal_binding_batch(&binding, &fact("d"));
    drive(&mut driver);
    let mut reply = ready(&mut admission);
    assert_eq!(
        typed_rows(&control),
        before,
        "received proof is not installed active D"
    );
    reply.install_binding_in(&binding).unwrap().notify();
    let after = typed_rows(&control);
    assert_eq!(
        before
            .iter()
            .map(|r| (&r.0, &r.1, &r.2, &r.3))
            .collect::<Vec<_>>(),
        after
            .iter()
            .map(|r| (&r.0, &r.1, &r.2, &r.3))
            .collect::<Vec<_>>()
    );
    assert_eq!(after[0].4, after[1].4);
    assert_eq!(after[0].4.len(), 4);
    for (_, _, _, _, old) in &before {
        assert!(old.iter().all(|d| after[0].4.contains(d)));
    }
    {
        let state = control.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        for row in ledger.claims.values().filter(|row| match row.owner {
            ClaimOwner::PhysicalWork(id) => id == work.owner.scope().id || id == stage.scope().id,
            ClaimOwner::ControlSlot(slot) => [work.owner.scope().id, stage.scope().id]
                .iter()
                .any(|id| state.nodes[id].cancellation == slot),
            _ => false,
        }) {
            assert_eq!(
                row.active_facts
                    .dependencies
                    .iter()
                    .map(|d| d.ordinal)
                    .collect::<Vec<_>>(),
                after[0].4
            );
            for expected in ["a", "b", "c", "d"] {
                assert!(
                    row.active_facts
                        .dependencies
                        .iter()
                        .any(|record| record.fact == fact(expected))
                );
            }
            assert!(
                !row.active_facts
                    .dependencies
                    .iter()
                    .any(|record| record.fact == fact("unselected-c"))
            );
            assert_eq!(row.amounts, row.active_amounts);
            assert!(row.pending.is_none());
        }
    }
    drop((reply, admission));
    drive(&mut driver);
    // Cancel a new uninstalled exact batch while old typed holders stay live.
    let mut pending = internal_binding_batch(&binding, &fact("d"));
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    stage.cancel(crate::CancellationReason::Requested);
    drive(&mut driver);
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(_))
    ));
    drop(pending);
    drive(&mut driver);
    assert_eq!(
        typed_rows(&control),
        after,
        "cancel cannot refund or revert any original held payload/compute"
    );
    drop((window, computation, payload));
    stage.complete_after_terminal_cancel_settled();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    control.close_admission();
    assert!(control.shutdown().is_ok());
}

#[test]
fn client_stage_window_manifest_retains_bytes_without_pinning_or_reminting_compute() {
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let window = control
        .inner
        .update(|state| {
            crate::result_window::reserve_window(
                state,
                &work.owner.scope(),
                crate::ResultWindowClass::Client,
            )
        })
        .unwrap();
    let original = window.retain_alias();
    assert_eq!(original.client_all_objects_bytes().unwrap(), 8);
    assert!(original.internal_manifest_binding().unwrap().is_none());
    let mut admission = batch(&payload, &work, &stage, 4);
    drive(&mut driver);
    let mut proof = ready(&mut admission);
    let mut compute = None;
    proof
        .install_in(&work.owner.scope(), &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop(proof);
    let client = crate::ResultWindowAlias::from_client_computation(
        &work.owner.scope(),
        &stage,
        &original,
        &payload,
        compute.as_ref().unwrap(),
    )
    .unwrap();
    assert_eq!(client.class(), crate::ResultWindowClass::Client);
    assert!(client.shares_capacity_with(&original));
    assert!(client.is_for_scope(&stage.scope()));
    assert!(client.internal_manifest_binding().unwrap().is_some());
    let consumer = client.try_clone().unwrap();
    let binding = client.internal_manifest_binding().unwrap().unwrap().clone();
    let authority = client.native_action_authority().unwrap();
    let quote = crate::joint_admission::NativeAsyncActionQuote::for_original_native_transport(
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
        std::num::NonZeroU64::MIN,
    );
    drop(client);
    assert_eq!(capacity(&control, JointResource::Computations), 1);
    drop(compute);
    assert_eq!(
        capacity(&control, JointResource::Computations),
        0,
        "the sole actual compute owner exits independently of Native/window tails"
    );
    assert!(
        binding.try_begin().is_err(),
        "an old manifest cannot remint P"
    );
    assert!(
        authority
            .action_source(JointLane::Foreground, quote)
            .is_err()
    );
    assert!(
        consumer
            .native_action_source(JointLane::Foreground, quote)
            .is_err()
    );
    assert!(
        authority
            .action_source(JointLane::Settlement, quote)
            .is_ok()
    );
    assert_eq!(consumer.client_all_objects_bytes().unwrap(), 8);
    drop(consumer);
    assert!(
        original.internal_manifest_binding().unwrap().is_none(),
        "plain Client alias cannot gain stage authority"
    );
    drop((original, window, payload));
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
}
#[test]
fn client_stage_rejects_foreign_original_window_without_releasing_either_owner() {
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let foreign = root(&control);
    let window = control
        .inner
        .update(|state| {
            crate::result_window::reserve_window(
                state,
                &foreign.owner.scope(),
                crate::ResultWindowClass::Client,
            )
        })
        .unwrap();
    let mut admission = batch(&payload, &work, &stage, 4);
    drive(&mut driver);
    let mut proof = ready(&mut admission);
    let mut compute = None;
    proof
        .install_in(&work.owner.scope(), &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop(proof);
    assert!(matches!(
        crate::ResultWindowAlias::from_client_computation(
            &work.owner.scope(),
            &stage,
            &window.retain_alias(),
            &payload,
            compute.as_ref().unwrap()
        ),
        Err(WorkError::Conflict)
    ));
    assert_eq!(capacity(&control, JointResource::Computations), 1);
    drop((window, compute, payload));
    stage.complete();
    work.business.release();
    work.owner.complete();
    foreign.business.release();
    foreign.owner.complete();
    drive(&mut driver);
}
#[test]
fn metadata_continuation_requires_same_original_positive_root_payload() {
    let (control, _, mut driver, work, stage, payload) = setup(false);
    let payload = payload.unwrap();
    let source = payload
        .async_continuation_source(&work.owner.scope(), JointLane::Foreground)
        .unwrap();
    drop(source);
    assert!(
        payload
            .async_continuation_source(&stage.scope(), JointLane::Foreground)
            .is_err()
    );
    let foreign = root(&control);
    assert!(
        payload
            .async_continuation_source(&foreign.owner.scope(), JointLane::Foreground)
            .is_err()
    );
    drop(payload);
    stage.complete();
    work.business.release();
    work.owner.complete();
    foreign.business.release();
    foreign.owner.complete();
    drive(&mut driver);
}

#[path = "canonical_tests.rs"]
mod canonical_tests;
