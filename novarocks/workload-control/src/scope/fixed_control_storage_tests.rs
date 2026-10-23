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

//! Original control inventory, binding and child-custody regressions.

use super::*;
use crate::control_storage::{ControlStorageConfig, SlotPhase};
use crate::joint_admission::{
    JointCapacityConfig, JointProgress, JointProgressDriver, JointResource, JointResourceLimits,
    JointResources, RequiredJointProgress,
};
use std::{
    future::Future,
    num::NonZeroUsize,
    task::{Context, Poll, Waker},
};

fn prototype(count: usize) -> (WorkloadControl, JointProgressDriver) {
    prototype_with_fanout(count, 1)
}

fn prototype_with_fanout(
    count: usize,
    children_per_slot: usize,
) -> (WorkloadControl, JointProgressDriver) {
    let (control, _, driver) = prototype_full(count, children_per_slot);
    (control, driver)
}

fn prototype_full(
    count: usize,
    children_per_slot: usize,
) -> (
    WorkloadControl,
    crate::joint_admission::JointAdmissionHandle,
    JointProgressDriver,
) {
    prototype_profile(count, children_per_slot, 1, 1)
}

pub(crate) fn prototype_profile(
    count: usize,
    children_per_slot: usize,
    max_dependencies: usize,
    max_changes: usize,
) -> (
    WorkloadControl,
    crate::joint_admission::JointAdmissionHandle,
    JointProgressDriver,
) {
    prototype_profile_with_roles(count, children_per_slot, max_dependencies, max_changes, &[])
}

pub(crate) fn prototype_profile_with_roles(
    count: usize,
    children_per_slot: usize,
    max_dependencies: usize,
    max_changes: usize,
    poll_roles: &[crate::role_poll::PollRoleGeometry],
) -> (
    WorkloadControl,
    crate::joint_admission::JointAdmissionHandle,
    JointProgressDriver,
) {
    let storage = ControlStorageConfig::new([count, 0, 0, 0, 0], children_per_slot, 2, poll_roles);
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: max_dependencies,
        max_registered_dependencies: count,
        max_claims: count * 2,
        max_requests: count * 2,
        max_changes_per_request: max_changes,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    config.max_registered_dependencies = (4 * config.max_claims
        + 2 * count * storage.proposals_per_slot * max_changes
        + 2 * max_changes
        + 1)
        * max_dependencies;
    let base = storage.base_charge(&config).unwrap();
    let metadata = base.get(JointResource::MetadataBytes);
    let global = JointResources::ZERO
        .with(JointResource::ScopeRecords, count as u64)
        .with(JointResource::ClaimRecords, (count * 2) as u64)
        .with(JointResource::MetadataBytes, metadata * count as u64 * 4);
    let domain = JointResources::ZERO
        .with(JointResource::ScopeRecords, (count - 1) as u64)
        .with(JointResource::ClaimRecords, count as u64)
        .with(JointResource::MetadataBytes, metadata * count as u64 * 2);
    let limit = JointResourceLimits {
        global,
        per_dependency: domain,
        protected_progress: base,
    };
    let mut classes = [JointResourceLimits::default(); 5];
    classes[0] = limit;
    let mut lanes = [JointResourceLimits::default(); 3];
    lanes[0] = limit;
    let mut class_lanes = [[JointResourceLimits::default(); 3]; 5];
    class_lanes[0][0] = limit;
    config.global = limit;
    config.classes = classes;
    config.lanes = lanes;
    config.class_lanes = class_lanes;
    // This minimum is only the storage substrate, never a complete role.
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
    if poll_roles.is_empty() {
        control.mark_ready().unwrap();
    }
    (control, handle, driver)
}

fn root(control: &WorkloadControl) -> RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn drain(driver: &mut JointProgressDriver) -> WorkloadProgressRevision {
    for _ in 0..10000 {
        if let JointProgress::Idle(marker) = driver.step() {
            return marker;
        }
    }
    panic!("storage driver did not reach a finite idle pass");
}
fn slot(control: &WorkloadControl, id: WorkId) -> SlotId {
    control.inner.state.lock().unwrap().nodes[&id].cancellation
}
fn snapshot(control: &WorkloadControl) -> (usize, usize, JointResources, usize) {
    control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .control_snapshot_for_test()
}

fn selected_dependency(
    handle: &crate::joint_admission::JointAdmissionHandle,
    name: &str,
) -> crate::joint_admission::JointDependency {
    use novarocks_resource_contract::{NetworkServiceOrigin, SharedDependency};
    handle
        .register_dependency(SharedDependency::Network(
            NetworkServiceOrigin::from_selected_url(
                &url::Url::parse(&format!("https://{name}.example")).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap()
}

fn first_bind(
    handle: &crate::joint_admission::JointAdmissionHandle,
    owner: &WorkOwner,
    dependency: &crate::joint_admission::JointDependency,
) -> crate::joint_admission::JointAdmission {
    handle
        .request(
            &owner.scope(),
            vec![crate::joint_admission::JointClaimChange::FirstBind {
                source: owner.control_claim_view().unwrap(),
                dependencies: vec![dependency.clone()],
            }],
        )
        .unwrap()
}

fn binding_receipt(
    request: &mut crate::joint_admission::JointAdmission,
) -> crate::joint_admission::JointAdjustmentReceipt {
    let Poll::Ready(Ok(batch)) =
        std::pin::Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the original proof must have granted the binding");
    };
    assert!(
        batch.new_claims.is_empty(),
        "binding cannot mint a physical guard"
    );
    batch.adjustment.unwrap()
}

#[test]
fn first_bind_preserves_base_and_requires_the_exact_original_owner_at_install() {
    let (control, handle, mut driver) = prototype_full(4, 1);
    let dependency = selected_dependency(&handle, "a");
    let work = root(&control);
    let other = root(&control);
    let original_slot = slot(&control, work.owner.scope().id);
    let initial = snapshot(&control);
    let original_claim =
        control.inner.state.lock().unwrap().cancellations.slots[original_slot.0].claim;
    let mut request = first_bind(&handle, &work.owner, &dependency);
    drain(&mut driver);
    let charged = snapshot(&control);
    assert_eq!(
        (charged.0, charged.1, charged.2),
        (initial.0, initial.1, initial.2)
    );
    assert_eq!(charged.3, 1);
    let receipt = binding_receipt(&mut request);
    assert!(matches!(
        receipt.install_control(&other.owner, &[]),
        Err(WorkError::Conflict)
    ));
    assert_eq!(
        snapshot(&control),
        initial,
        "a refused install withdraws only this binding"
    );

    let mut request = first_bind(&handle, &work.owner, &dependency);
    drain(&mut driver);
    assert!(matches!(
        binding_receipt(&mut request).install(&[]),
        Err(WorkError::Conflict)
    ));
    assert_eq!(
        snapshot(&control),
        initial,
        "ordinary physical install cannot forge control authority"
    );

    let mut request = first_bind(&handle, &work.owner, &dependency);
    drain(&mut driver);
    binding_receipt(&mut request)
        .install_control(&work.owner, &[])
        .unwrap();
    let bound = snapshot(&control);
    assert_eq!(bound, charged);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[original_slot.0].claim,
        original_claim
    );
    let view = work.owner.scope().cancellation().unwrap();
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert_eq!(
        snapshot(&control),
        bound,
        "retained backing keeps the installed domain fee"
    );
    drop(view);
    other.business.release();
    other.owner.complete();
    drain(&mut driver);
    assert_eq!(snapshot(&control), initial);
}

#[test]
fn first_bind_is_finite_under_one_bad_domain_and_preserves_healthy_progress() {
    let (control, handle, mut driver) = prototype_full(3, 1);
    let bad = selected_dependency(&handle, "bad");
    let healthy = selected_dependency(&handle, "healthy");
    let a = root(&control);
    let b = root(&control);
    let c = root(&control);
    for owner in [&a.owner, &b.owner] {
        let mut request = first_bind(&handle, owner, &bad);
        drain(&mut driver);
        binding_receipt(&mut request)
            .install_control(owner, &[])
            .unwrap();
    }
    let before = snapshot(&control);
    let mut rejected = first_bind(&handle, &c.owner, &bad);
    drain(&mut driver);
    let Poll::Ready(Err(failure)) =
        std::pin::Pin::new(&mut rejected).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("a self-held first binding must fail rather than park");
    };
    assert!(matches!(failure.error, WorkError::Capacity(_)));
    assert_eq!(snapshot(&control), before);
    let mut request = first_bind(&handle, &c.owner, &healthy);
    drain(&mut driver);
    binding_receipt(&mut request)
        .install_control(&c.owner, &[])
        .unwrap();
    for work in [a, b, c] {
        work.business.release();
        work.owner.complete();
    }
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn first_bind_and_physical_growth_install_atomically_with_both_original_owners() {
    use crate::joint_admission::{JointClaimChange, JointLane};
    let (control, handle, mut driver) = prototype_profile(4, 1, 2, 2);
    let a = selected_dependency(&handle, "a");
    let b = selected_dependency(&handle, "b");
    let work = root(&control);
    let scope = work.owner.scope();
    let mut request = handle
        .request(
            &scope,
            vec![JointClaimChange::New {
                dependencies: vec![a.clone()],
                lane: JointLane::Custody,
                amounts: JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(JointResource::MetadataBytes, 1),
            }],
        )
        .unwrap();
    drain(&mut driver);
    let Poll::Ready(Ok(mut batch)) =
        std::pin::Pin::new(&mut request).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("physical base grant");
    };
    let physical = batch.new_claims.pop().unwrap();
    drop(batch);
    drain(&mut driver);
    let before = snapshot(&control);
    // Both source kinds share exact original claim deduplication.
    let view = work.owner.control_claim_view().unwrap();
    assert!(
        handle
            .request(
                &scope,
                vec![
                    JointClaimChange::FirstBind {
                        source: view.clone(),
                        dependencies: vec![a.clone()]
                    },
                    JointClaimChange::FirstBind {
                        source: view,
                        dependencies: vec![b.clone()]
                    },
                ]
            )
            .is_err()
    );
    for omit_physical in [true, false] {
        drain(&mut driver);
        let mut request = handle
            .request(
                &scope,
                vec![
                    JointClaimChange::FirstBind {
                        source: work.owner.control_claim_view().unwrap(),
                        dependencies: vec![a.clone(), b.clone()],
                    },
                    JointClaimChange::grow_by(
                        physical.view().unwrap(),
                        JointResources::ZERO.with(JointResource::MetadataBytes, 1),
                        vec![b.clone()],
                    ),
                ],
            )
            .unwrap();
        drain(&mut driver);
        let receipt = binding_receipt(&mut request);
        assert_eq!(
            physical
                .amounts()
                .unwrap()
                .get(JointResource::MetadataBytes),
            1,
            "charged delta is not installed permission"
        );
        if omit_physical {
            assert!(matches!(
                receipt.install_control(&work.owner, &[]),
                Err(WorkError::Conflict)
            ));
            assert_eq!(
                snapshot(&control),
                before,
                "mixed withdrawal keeps the physical original"
            );
        } else {
            receipt.install_control(&work.owner, &[&physical]).unwrap();
            assert_eq!(
                physical
                    .amounts()
                    .unwrap()
                    .get(JointResource::MetadataBytes),
                2
            );
            assert_eq!(snapshot(&control).3, 1);
        }
    }
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::FirstBind {
                source: physical.view().unwrap(),
                dependencies: vec![a],
            }]
        ),
        Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drop(physical);
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn control_source_cannot_escape_through_grow_empty_binding_or_reused_slot() {
    use crate::joint_admission::JointClaimChange;
    let (control, handle, mut driver) = prototype_full(3, 1);
    let dependency = selected_dependency(&handle, "a");
    let work = root(&control);
    let old_scope = work.owner.scope();
    let old_slot = slot(&control, old_scope.id);
    let old = work.owner.control_claim_view().unwrap();
    for change in [
        JointClaimChange::FirstBind {
            source: old.clone(),
            dependencies: vec![],
        },
        JointClaimChange::grow_by(old.clone(), JointResources::ZERO, vec![dependency.clone()]),
        JointClaimChange::grow_by(
            old.clone(),
            JointResources::ZERO.with(JointResource::MetadataBytes, 1),
            vec![dependency.clone()],
        ),
    ] {
        drain(&mut driver);
        assert!(matches!(
            handle.request(&old_scope, vec![change]),
            Err(crate::joint_admission::JointAdmissionFailure {
                error: WorkError::Conflict
            })
        ));
    }
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    let replacement = root(&control);
    let replacement_scope = replacement.owner.scope();
    assert_eq!(slot(&control, replacement_scope.id), old_slot);
    assert!(matches!(
        handle.request(
            &replacement_scope,
            vec![JointClaimChange::FirstBind {
                source: old,
                dependencies: vec![dependency]
            }]
        ),
        Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    replacement.business.release();
    replacement.owner.complete();
    drain(&mut driver);
}

#[test]
fn control_domain_extension_preserves_the_union_and_invalidates_an_old_child_receipt() {
    use crate::joint_admission::JointClaimChange;
    let (control, handle, mut driver) = prototype_profile(4, 1, 2, 1);
    let a = selected_dependency(&handle, "a");
    let b = selected_dependency(&handle, "b");
    let expected = [a.fact().clone(), b.fact().clone()];
    let parent = root(&control);
    let scope = parent.owner.scope();
    let mut binding = first_bind(&handle, &parent.owner, &a);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&parent.owner, &[])
        .unwrap();
    let parent_slot = slot(&control, scope.id);
    let parent_claim = control.inner.state.lock().unwrap().cancellations.slots[parent_slot.0]
        .claim
        .unwrap();
    let mut child_request = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    drain(&mut driver);
    let old_child = binding_receipt(&mut child_request);
    let before = snapshot(&control);
    let stale = parent.owner.control_claim_view().unwrap();
    let mut extension = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                stale.clone(),
                JointResources::ZERO,
                vec![b.clone()],
            )],
        )
        .unwrap();
    drain(&mut driver);
    binding_receipt(&mut extension)
        .install_control(&parent.owner, &[])
        .unwrap();
    assert_eq!(
        snapshot(&control),
        before,
        "domain union keeps the same original global vector"
    );
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .control_has_domains_for_test(parent_claim, &expected)
    );
    assert!(
        matches!(
            old_child.install_child(&parent.owner),
            Err(WorkError::Conflict)
        ),
        "a child proved for the old parent cannot adopt the changed domain facts"
    );
    assert_eq!(
        snapshot(&control),
        before,
        "a refused reply retains its old D until actual reset"
    );
    assert!(
        handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    stale,
                    JointResources::ZERO,
                    vec![b.clone()]
                )]
            )
            .is_err()
    );
    drain(&mut driver);
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                parent.owner.control_claim_view().unwrap(),
                JointResources::ZERO.with(JointResource::MetadataBytes, 1),
                vec![]
            )]
        ),
        Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drain(&mut driver);
    let mut request = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    drain(&mut driver);
    let child = binding_receipt(&mut request)
        .install_child(&parent.owner)
        .unwrap();
    let child_slot = slot(&control, child.scope().id);
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.joint.as_ref().unwrap().control_has_domains_for_test(
            state.cancellations.slots[child_slot.0].claim.unwrap(),
            &expected
        ));
    }
    child.complete();
    parent.business.release();
    parent.owner.complete();
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn control_union_refusal_is_finite_and_never_discards_its_old_domain() {
    use crate::joint_admission::JointClaimChange;
    let (control, handle, mut driver) = prototype_profile(4, 1, 2, 1);
    let a = selected_dependency(&handle, "a");
    let b = selected_dependency(&handle, "b");
    let parent = root(&control);
    let mut binding = first_bind(&handle, &parent.owner, &a);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&parent.owner, &[])
        .unwrap();
    let scope = parent.owner.scope();
    let parent_slot = slot(&control, scope.id);
    let claim = control.inner.state.lock().unwrap().cancellations.slots[parent_slot.0]
        .claim
        .unwrap();
    let mut others = Vec::new();
    for _ in 0..3 {
        let work = root(&control);
        let mut binding = first_bind(&handle, &work.owner, &b);
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&work.owner, &[])
            .unwrap();
        others.push(work);
    }
    let before = snapshot(&control);
    let mut extension = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                parent.owner.control_claim_view().unwrap(),
                JointResources::ZERO,
                vec![b.clone()],
            )],
        )
        .unwrap();
    drain(&mut driver);
    assert!(matches!(
        std::pin::Pin::new(&mut extension).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Capacity(_)
        }))
    ));
    assert_eq!(snapshot(&control), before);
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .control_has_domains_for_test(claim, &[a.fact().clone()])
    );
    let exited = others.pop().unwrap();
    exited.business.release();
    exited.owner.complete();
    drain(&mut driver);
    let mut extension = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                parent.owner.control_claim_view().unwrap(),
                JointResources::ZERO,
                vec![b.clone()],
            )],
        )
        .unwrap();
    drain(&mut driver);
    binding_receipt(&mut extension)
        .install_control(&parent.owner, &[])
        .unwrap();
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .control_has_domains_for_test(claim, &[a.fact().clone(), b.fact().clone()])
    );
    for work in others.into_iter().chain(std::iter::once(parent)) {
        work.business.release();
        work.owner.complete();
    }
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn waiting_pool_proposals_do_not_reserve_a_slot_or_block_a_new_root_binding() {
    let (control, handle, mut driver) = prototype_full(4, 1);
    let a = selected_dependency(&handle, "a");
    let b = selected_dependency(&handle, "b");
    let parent = root(&control);
    let mut binding = first_bind(&handle, &parent.owner, &a);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&parent.owner, &[])
        .unwrap();
    drain(&mut driver);
    let mut first = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    let mut second = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    let newcomer = root(&control);
    let mut binding = first_bind(&handle, &newcomer.owner, &b);
    drain(&mut driver);
    for request in [&mut first, &mut second] {
        assert!(matches!(
            std::pin::Pin::new(request).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
                error: WorkError::Conflict
            }))
        ));
    }
    binding_receipt(&mut binding)
        .install_control(&newcomer.owner, &[])
        .unwrap();
    assert_eq!(snapshot(&control).3, 2);
    for work in [parent, newcomer] {
        work.business.release();
        work.owner.complete();
    }
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn pool_grant_publishes_one_child_with_the_full_parent_domain_and_same_claim() {
    use crate::joint_admission::JointClaimChange;
    let (control, handle, mut driver) = prototype_profile(4, 1, 2, 2);
    let a = selected_dependency(&handle, "a");
    let b = selected_dependency(&handle, "b");
    let expected = [a.fact().clone(), b.fact().clone()];
    let parent = root(&control);
    let parent_scope = parent.owner.scope();
    let mut binding = handle
        .request(
            &parent_scope,
            vec![JointClaimChange::FirstBind {
                source: parent.owner.control_claim_view().unwrap(),
                dependencies: vec![a, b],
            }],
        )
        .unwrap();
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&parent.owner, &[])
        .unwrap();
    drain(&mut driver);
    let before = snapshot(&control);
    let candidate = control.inner.state.lock().unwrap().cancellations.free_heads[0].unwrap();
    let claim = control.inner.state.lock().unwrap().cancellations.slots[candidate.0].claim;
    let mut first = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    let mut second = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    drain(&mut driver);
    let receipt = binding_receipt(&mut first);
    assert!(matches!(
        std::pin::Pin::new(&mut second).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Conflict
        }))
    ));
    let granted = snapshot(&control);
    assert_eq!(
        (granted.0, granted.1, granted.2),
        (before.0, before.1, before.2)
    );
    assert_eq!(granted.3, before.3 + 1);
    {
        let state = control.inner.state.lock().unwrap();
        assert!(matches!(
            state.cancellations.slots[candidate.0].phase,
            SlotPhase::ChildGranted { .. }
        ));
        assert_eq!(
            state.nodes[&parent_scope.id].children, 0,
            "grant is not a fake Work"
        );
        assert_eq!(state.nodes[&parent_scope.id].resource_waiters, 1);
        assert!(
            state
                .joint
                .as_ref()
                .unwrap()
                .control_has_domains_for_test(claim.unwrap(), &expected)
        );
    }
    let child = receipt.install_child(&parent.owner).unwrap();
    let child_scope = child.scope();
    assert_eq!(slot(&control, child_scope.id), candidate);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[candidate.0].claim,
        claim
    );
    assert_eq!(snapshot(&control), granted);
    let retained = child_scope.cancellation().unwrap();
    parent.business.release();
    parent.owner.complete();
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&parent_scope.id)
    );
    child.complete();
    assert!(
        !control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&parent_scope.id)
    );
    drain(&mut driver);
    assert_eq!(
        snapshot(&control),
        granted,
        "real child backing retains both domain liabilities"
    );
    drop(retained);
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn granted_and_delivered_child_replies_keep_domain_fees_until_their_actual_exit() {
    for delivered in [false, true] {
        let (control, handle, mut driver) = prototype_full(3, 1);
        let a = selected_dependency(&handle, "a");
        let parent = root(&control);
        let scope = parent.owner.scope();
        let mut binding = first_bind(&handle, &parent.owner, &a);
        drain(&mut driver);
        binding_receipt(&mut binding)
            .install_control(&parent.owner, &[])
            .unwrap();
        let mut request = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        drain(&mut driver);
        let receipt = delivered.then(|| binding_receipt(&mut request));
        let charged = snapshot(&control);
        assert_eq!(charged.3, 2);
        parent.owner.cancel(CancellationReason::Requested);
        parent.business.release();
        parent.owner.complete_after_terminal_cancel_settled();
        drain(&mut driver);
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id)
        );
        assert_eq!(
            snapshot(&control),
            charged,
            "cancellation is not reply exit"
        );
        drop(receipt);
        drop(request);
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id)
        );
        assert_eq!(
            snapshot(&control),
            charged,
            "reply exit retires without washing the domain fee"
        );
        drain(&mut driver);
        let reset = snapshot(&control);
        assert_eq!(
            (reset.0, reset.1, reset.2),
            (charged.0, charged.1, charged.2)
        );
        assert_eq!(reset.3, 0);
    }
}

#[test]
fn refused_pool_proof_preserves_the_real_free_head_for_a_healthy_root() {
    let (control, handle, mut driver) = prototype_full(4, 2);
    let a = selected_dependency(&handle, "bad");
    let b = selected_dependency(&handle, "healthy");
    let parent = root(&control);
    let mut binding = first_bind(&handle, &parent.owner, &a);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&parent.owner, &[])
        .unwrap();
    let mut children = Vec::new();
    for _ in 0..2 {
        let mut request = handle
            .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
            .unwrap();
        drain(&mut driver);
        children.push(
            binding_receipt(&mut request)
                .install_child(&parent.owner)
                .unwrap(),
        );
    }
    let free = control.inner.state.lock().unwrap().cancellations.free_heads[0];
    let mut rejected = handle
        .request_child(&parent.owner, WorkRequest::new(WorkClass::Query))
        .unwrap();
    drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.free_heads[0],
        free
    );
    let healthy = root(&control);
    let mut binding = first_bind(&handle, &healthy.owner, &b);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&healthy.owner, &[])
        .unwrap();
    assert!(matches!(
        std::pin::Pin::new(&mut rejected).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Capacity(_)
        }))
    ));
    for child in children {
        child.complete();
    }
    for work in [parent, healthy] {
        work.business.release();
        work.owner.complete();
    }
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn child_registration_failure_detaches_and_retires_the_exact_committed_reply() {
    let (control, handle, mut driver) = prototype_full(3, 1);
    let a = selected_dependency(&handle, "a");
    let parent = root(&control);
    let scope = parent.owner.scope();
    let mut binding = first_bind(&handle, &parent.owner, &a);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&parent.owner, &[])
        .unwrap();
    let mut request = handle
        .request_child(
            &parent.owner,
            WorkRequest {
                class: WorkClass::Query,
                deadline: Some(Instant::now()),
            },
        )
        .unwrap();
    drain(&mut driver);
    let charged = snapshot(&control);
    assert!(matches!(
        binding_receipt(&mut request).install_child(&parent.owner),
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    ));
    {
        let state = control.inner.state.lock().unwrap();
        let node = &state.nodes[&scope.id];
        assert_eq!(node.children, 0);
        assert_eq!(node.resource_waiters, 0);
        assert_eq!(state.nodes.len(), 1);
        assert_eq!(
            state
                .cancellations
                .cell(node.cancellation)
                .linked_children_for_test(),
            0
        );
    }
    assert_eq!(snapshot(&control), charged);
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 1);
    parent.business.release();
    parent.owner.complete();
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn retained_view_collects_work_but_preserves_original_bound_claim_until_reset() {
    let (control, mut driver) = prototype(4);
    let prepaid = snapshot(&control);
    let work = root(&control);
    let scope = work.owner.scope();
    let original = slot(&control, scope.id);
    assert_eq!(
        snapshot(&control),
        prepaid,
        "same-key take preserves members and all coordinates"
    );
    let view = scope.cancellation().unwrap();
    let claim = {
        let mut state = control.inner.state.lock().unwrap();
        let claim = state.cancellations.slots[original.0].claim.unwrap();
        let ordinal = state.next_id().unwrap();
        state
            .joint
            .as_mut()
            .unwrap()
            .bind_control_fixture(original, claim, ordinal);
        claim
    };
    let bound = snapshot(&control);
    assert_eq!(bound.3, 1);
    work.business.release();
    work.owner.complete();
    assert!(matches!(scope.check(), Err(WorkError::Released)));
    drain(&mut driver);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(
            state.cancellations.slots[original.0].phase,
            SlotPhase::Retired
        );
        assert_eq!(state.cancellations.slots[original.0].claim, Some(claim));
    }
    assert_eq!(
        snapshot(&control),
        bound,
        "logical collect never washes the original D"
    );
    drop(view);
    drain(&mut driver);
    let freed = snapshot(&control);
    assert_eq!(
        (freed.0, freed.1, freed.2),
        (prepaid.0, prepaid.1, prepaid.2)
    );
    assert_eq!(freed.3, 0);
}

#[tokio::test]
async fn last_view_exit_wakes_the_original_idle_driver_without_a_business_event() {
    let (control, mut driver) = prototype(2);
    let work = root(&control);
    let scope = work.owner.scope();
    let original = slot(&control, scope.id);
    let view = scope.cancellation().unwrap();
    work.business.release();
    work.owner.complete();
    let marker = drain(&mut driver);
    let mut waiter = Box::pin(control.wait_progress(marker));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(view);
    assert!(matches!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(WorkloadProgress::StateChanged(_))
    ));
    drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
        SlotPhase::Free
    );
}

#[test]
fn an_actual_raw_alias_prevents_free_until_its_exit_guard_notifies() {
    let (control, mut driver) = prototype(2);
    let work = root(&control);
    let scope = work.owner.scope();
    let original = slot(&control, scope.id);
    let wake_on_exit = ReclaimWakeOnExit::new(&control.inner);
    let raw = {
        let state = control.inner.state.lock().unwrap();
        Arc::clone(state.cancellations.cell(original))
    };
    work.business.release();
    work.owner.complete();
    let marker = drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
        SlotPhase::Retired
    );
    drop(raw);
    drop(wake_on_exit);
    assert_ne!(control.progress_revision(), marker);
    drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
        SlotPhase::Free
    );
}

#[test]
fn observing_unset_cached_and_sealed_reasons_does_not_wake_the_reaper() {
    let (control, mut driver) = prototype(3);
    let work = root(&control);
    let scope = work.owner.scope();
    let root_view = scope.cancellation().unwrap();
    let child = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
    let child_view = child.scope().cancellation().unwrap();
    let sealed = root(&control);
    let sealed_view = sealed.owner.scope().cancellation().unwrap();
    let before = drain(&mut driver);
    assert_eq!(root_view.reason(), None);
    assert_eq!(child_view.reason(), None);
    assert_eq!(
        control.progress_revision(),
        before,
        "a negative read has no alias exit to publish"
    );
    assert_eq!(
        sealed.owner.success_sealer().seal().unwrap(),
        WorkSuccessSealOutcome::Sealed
    );
    work.owner.cancel(CancellationReason::Requested);
    let after = drain(&mut driver);
    for _ in 0..3 {
        assert_eq!(root_view.reason(), Some(CancellationReason::Requested));
        assert_eq!(child_view.reason(), Some(CancellationReason::Requested));
        assert_eq!(
            sealed_view.reason(),
            None,
            "sealed success remains immutable"
        );
    }
    assert_eq!(
        control.progress_revision(),
        after,
        "cached and sealed reads cannot manufacture progress"
    );
    // Inherited cancellation is ready for the expiry owner even when it
    // predates marker capture. Observing readiness must remain mutation-free.
    let deadlines = control.deadline_expiry_handle();
    let mut ready = Box::pin(deadlines.wait_state_change(after));
    assert!(
        ready
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(ready);
    assert_eq!(control.progress_revision(), after);
    deadlines.expire_deadlines();
    let settled = drain(&mut driver);
    let mut progress = Box::pin(deadlines.wait_state_change(settled));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(progress);
    child.complete_after_terminal_cancel_settled();
    work.business.release();
    work.owner.complete_after_terminal_cancel_settled();
    sealed.business.release();
    sealed.owner.complete();
    drop(sealed_view);
    drop(child_view);
    drop(root_view);
    drain(&mut driver);
}

struct PausedCancellationWake {
    armed: std::sync::atomic::AtomicBool,
    entered: std::sync::mpsc::SyncSender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl std::task::Wake for PausedCancellationWake {
    fn wake(self: Arc<Self>) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        self.entered.send(()).unwrap();
        self.resume
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .expect("the test must release the actual cancellation wake");
    }
}

#[tokio::test(start_paused = true)]
async fn retained_root_reason_reports_descendant_alias_exit_to_idle_reaper() {
    let (control, mut driver) = prototype(3);
    let work = control
        .try_begin_root(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(Instant::now() + Duration::from_secs(1)),
        })
        .unwrap();
    let scope = work.owner.scope();
    let root_slot = slot(&control, scope.id);
    let root_view = scope.cancellation().unwrap();
    let child = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
    let child_scope = child.scope();
    let child_slot = slot(&control, child_scope.id);
    let original_claim =
        control.inner.state.lock().unwrap().cancellations.slots[child_slot.0].claim;
    let child_view = child_scope.cancellation().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
    let paused = Arc::new(PausedCancellationWake {
        armed: std::sync::atomic::AtomicBool::new(false),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    });
    let waker = Waker::from(Arc::clone(&paused));
    let mut observer = Box::pin(async move { child_view.cancelled().await });
    assert!(
        observer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    // Advancing the timer is not the DFS notification being inspected.
    paused.armed.store(true, Ordering::Release);
    let runtime = tokio::runtime::Handle::current();
    let (before, after, waiting, phase, aliases, claim, reason) = std::thread::scope(|threads| {
        let request = threads.spawn(|| {
            let _runtime = runtime.enter();
            root_view.reason()
        });
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        // Tokio has released its waiter lock before invoking this Waker.
        // The real observer can exit while the DFS still holds its child Arc.
        drop(observer);
        child.complete_after_terminal_cancel_settled();
        let before = drain(&mut driver);
        let (phase, aliases, claim) = {
            let state = control.inner.state.lock().unwrap();
            let slot = &state.cancellations.slots[child_slot.0];
            (
                slot.phase,
                Arc::strong_count(state.cancellations.cell(child_slot)),
                slot.claim,
            )
        };
        let mut waiter = Box::pin(control.wait_progress(before));
        let waiting = waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        resume_tx.send(()).unwrap();
        let reason = request.join().unwrap();
        let after = control.progress_revision();
        let notified = waiter
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        (
            before,
            after,
            (waiting, notified),
            phase,
            aliases,
            claim,
            reason,
        )
    });
    assert_eq!(reason, Some(CancellationReason::DeadlineExceeded));
    assert_eq!(phase, SlotPhase::Retired);
    assert_eq!(aliases, 2, "only the pool and actual DFS retain the child");
    assert_eq!(claim, original_claim);
    assert!(waiting.0.is_pending());
    assert_ne!(
        after, before,
        "temporary descendant exit must publish its own wake"
    );
    assert!(waiting.1.is_ready());
    drain(&mut driver);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(
            state.cancellations.slots[child_slot.0].phase,
            SlotPhase::Free
        );
        assert_eq!(
            state.cancellations.slots[child_slot.0].claim,
            original_claim
        );
        assert_ne!(
            state.cancellations.slots[root_slot.0].phase,
            SlotPhase::Free
        );
    }
    // Keep this original view alive through every assertion above: its Drop
    // must not stand in for the already exited descendant traversal alias.
    drop(root_view);
    work.business.release();
    work.owner.complete_after_terminal_cancel_settled();
    drain(&mut driver);
}

#[test]
fn weak_observation_is_not_mistaken_for_exclusive_reuse() {
    let (control, mut driver) = prototype(2);
    let work = root(&control);
    let scope = work.owner.scope();
    let original = slot(&control, scope.id);
    let weak = {
        let state = control.inner.state.lock().unwrap();
        Arc::downgrade(state.cancellations.cell(original))
    };
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert_eq!(
        weak.strong_count(),
        1,
        "the pool anchor alone does not imply get_mut succeeds"
    );
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
        SlotPhase::Retired
    );
    drop(weak);
    // There is no public production Weak escape; publish the fixture's
    // private-edge drop just as the real detach/reset author does.
    control.inner.notify_reclaim_progress();
    drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[original.0].phase,
        SlotPhase::Free
    );
}

#[test]
fn leaf_parent_hint_reclaims_a_retired_chain_in_linear_cell_checks() {
    let count = 32;
    let (control, mut driver) = prototype(count);
    let work = root(&control);
    let root_scope = work.owner.scope();
    let mut parent = root_scope.clone();
    let mut owners = vec![work.owner];
    for _ in 1..count {
        let child = parent.child(WorkRequest::new(WorkClass::Query)).unwrap();
        parent = child.scope();
        owners.push(child);
    }
    let view = parent.cancellation().unwrap();
    let (root_slot, (limit, shared, edges)) = {
        let state = control.inner.state.lock().unwrap();
        let root_slot = state.nodes[&root_scope.id].cancellation;
        (
            root_slot,
            state
                .cancellations
                .cell(root_slot)
                .fixed_child_facts_for_test(),
        )
    };
    assert_eq!(limit, 1);
    assert_eq!(edges, count - 1);
    work.business.release();
    for owner in owners {
        owner.complete();
    }
    assert_eq!(control.snapshot().root_responsibilities, 0);
    drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.retired,
        count
    );
    let before = driver.reclaim_checks;
    drop(view);
    drain(&mut driver);
    assert!(driver.reclaim_checks - before <= count * 4);
    let state = control.inner.state.lock().unwrap();
    assert_eq!(state.cancellations.retired, 0);
    assert!(
        state
            .cancellations
            .slots
            .iter()
            .all(|slot| slot.phase == SlotPhase::Free)
    );
    assert_eq!(
        state
            .cancellations
            .cell(root_slot)
            .fixed_child_facts_for_test(),
        (limit, shared, 0)
    );
}

#[test]
fn unpublished_child_failure_detaches_and_retires_its_real_fixed_slot() {
    let (control, mut driver) = prototype(2);
    let work = root(&control);
    let scope = work.owner.scope();
    let original = slot(&control, scope.id);
    assert!(matches!(
        scope.child(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(Instant::now())
        }),
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    ));
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.nodes[&scope.id].children, 0);
        assert_eq!(
            state
                .cancellations
                .cell(original)
                .linked_children_for_test(),
            0
        );
        assert_eq!(state.cancellations.retired, 1);
    }
    drain(&mut driver);
    assert_eq!(control.inner.state.lock().unwrap().cancellations.retired, 0);
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
}

#[test]
fn same_cell_reuse_changes_original_work_and_claim_generation_without_refunding_base() {
    let (control, mut driver) = prototype(2);
    let prepaid = snapshot(&control);
    let first = root(&control);
    let scope = first.owner.scope();
    let requester = first.owner.cancellation_requester();
    let original = slot(&control, scope.id);
    let (address, generation, revision, claim) = {
        let state = control.inner.state.lock().unwrap();
        let cell = &state.cancellations.slots[original.0];
        (
            Arc::as_ptr(cell.cell.as_ref().unwrap()),
            cell.generation,
            state
                .joint
                .as_ref()
                .unwrap()
                .control_revision_for_test(cell.claim.unwrap()),
            cell.claim.unwrap(),
        )
    };
    first.business.release();
    first.owner.complete();
    drain(&mut driver);
    let second = root(&control);
    let second_scope = second.owner.scope();
    assert_ne!(scope.id, second_scope.id);
    assert_eq!(slot(&control, second_scope.id), original);
    assert!(matches!(
        requester.request(CancellationReason::Requested),
        Err(WorkError::Released)
    ));
    assert!(second_scope.cancellation().unwrap().reason().is_none());
    {
        let state = control.inner.state.lock().unwrap();
        let cell = &state.cancellations.slots[original.0];
        assert_eq!(Arc::as_ptr(cell.cell.as_ref().unwrap()), address);
        assert!(cell.generation > generation);
        assert_eq!(cell.claim, Some(claim));
        assert!(
            state
                .joint
                .as_ref()
                .unwrap()
                .control_revision_for_test(claim)
                > revision
        );
    }
    assert_eq!(snapshot(&control), prepaid);
    second.business.release();
    second.owner.complete();
    drain(&mut driver);
}

#[test]
fn shutdown_requires_exact_free_inventory_but_keeps_its_prepaid_profiles() {
    let (mut control, mut driver) = prototype(2);
    let prepaid = snapshot(&control);
    let work = root(&control);
    let view = work.owner.scope().cancellation().unwrap();
    work.business.release();
    work.owner.complete();
    control.close_admission();
    drain(&mut driver);
    let failure = control
        .shutdown()
        .err()
        .expect("retained backing is not drained");
    assert_eq!(failure.error(), WorkloadShutdownError::NotDrained);
    control = failure.into_parts().1;
    drop(view);
    drain(&mut driver);
    assert_eq!(snapshot(&control), prepaid);
    assert!(
        control.shutdown().is_ok(),
        "Free process claims remain charged through real teardown"
    );
}

#[test]
fn bounded_fanout_failure_preserves_the_free_head_and_later_reuses_returned_storage() {
    let (control, mut driver) = prototype_with_fanout(4, 1);
    let work = root(&control);
    let scope = work.owner.scope();
    let first = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
    let first_slot = slot(&control, first.scope().id);
    let (heads, prepaid) = {
        let state = control.inner.state.lock().unwrap();
        assert!(state.cancellations.free_inventory_valid());
        (
            state.cancellations.free_heads,
            state.joint.as_ref().unwrap().control_snapshot_for_test(),
        )
    };
    assert!(matches!(
        scope.child(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::Capacity("prepaid parent child backing"))
    ));
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.cancellations.free_heads, heads);
        assert!(state.cancellations.free_inventory_valid());
        assert_eq!(state.nodes[&scope.id].children, 1);
    }
    assert_eq!(snapshot(&control), prepaid);
    first.complete();
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(
            state.cancellations.slots[first_slot.0].phase,
            SlotPhase::Retired
        );
        assert_eq!(
            state.cancellations.free_heads, heads,
            "Retired is never free inventory"
        );
        assert!(state.cancellations.free_inventory_valid());
    }
    drain(&mut driver);
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.free_heads[0],
        Some(first_slot)
    );
    let second = scope.child(WorkRequest::new(WorkClass::Query)).unwrap();
    assert_eq!(slot(&control, second.scope().id), first_slot);
    second.complete();
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().cancellations.drained());
}

#[test]
fn rejected_fixed_root_deadline_leaves_its_head_and_original_claim_untouched() {
    let (control, mut driver) = prototype(2);
    let heads = control.inner.state.lock().unwrap().cancellations.free_heads;
    let prepaid = snapshot(&control);
    assert!(matches!(
        control.try_begin_root(WorkRequest {
            class: WorkClass::Query,
            deadline: Some(Instant::now())
        }),
        Err(WorkError::Cancelled(CancellationReason::DeadlineExceeded))
    ));
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.cancellations.free_heads, heads);
        assert!(state.cancellations.drained());
        assert!(state.nodes.is_empty());
    }
    assert_eq!(snapshot(&control), prepaid);
    let work = root(&control);
    assert_eq!(slot(&control, work.owner.scope().id), heads[0].unwrap());
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
}

#[test]
fn drained_requires_complete_acyclic_class_correct_free_chains() {
    let (control, _driver) = prototype(4);
    control.close_admission();
    {
        let mut state = control.inner.state.lock().unwrap();
        assert!(state.is_drained());
        let heads = state.cancellations.free_heads;
        let head = heads[0].unwrap();
        let next = state.cancellations.slots[head.0].next_free;
        state.cancellations.free_heads[0] = None;
        assert!(
            !state.is_drained(),
            "unreachable Free entries cannot certify drain"
        );
        state.cancellations.free_heads = heads;
        state.cancellations.slots[head.0].next_free = Some(head);
        assert!(
            !state.is_drained(),
            "a cycle must be rejected within the visit budget"
        );
        state.cancellations.slots[head.0].next_free = None;
        assert!(
            !state.is_drained(),
            "a disconnected suffix cannot certify drain"
        );
        state.cancellations.slots[head.0].next_free = next;
        state.cancellations.free_heads[1] = Some(head);
        assert!(
            !state.is_drained(),
            "one slot cannot appear in a foreign class chain"
        );
        state.cancellations.free_heads = heads;
        state.cancellations.free_heads[0] = Some(SlotId(state.cancellations.slots.len()));
        assert!(
            !state.is_drained(),
            "out-of-inventory links are not followed"
        );
        state.cancellations.free_heads = heads;
        state.cancellations.slots[head.0].phase = SlotPhase::Retired;
        assert!(!state.is_drained(), "a linked Retired entry is never Free");
        state.cancellations.slots[head.0].phase = SlotPhase::Free;
        assert!(state.is_drained());
    }
    assert!(control.shutdown().is_ok());
}

fn legacy_control() -> WorkloadControl {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    control.mark_ready().unwrap();
    control
}

#[test]
fn legacy_vacant_head_reuses_the_slot_without_reusing_retained_observer_backing() {
    let control = legacy_control();
    let first = root(&control);
    let first_scope = first.owner.scope();
    let first_slot = slot(&control, first_scope.id);
    let view = first_scope.cancellation().unwrap();
    let first_address = {
        let state = control.inner.state.lock().unwrap();
        Arc::as_ptr(state.cancellations.cell(first_slot))
    };
    first.business.release();
    first.owner.complete();
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.cancellations.legacy_vacant, Some(first_slot));
        assert!(state.cancellations.drained());
    }
    let second = root(&control);
    let second_scope = second.owner.scope();
    assert_eq!(slot(&control, second_scope.id), first_slot);
    assert_ne!(first_scope.id, second_scope.id);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.cancellations.slots.len(), 1);
        assert_eq!(state.cancellations.legacy_vacant, None);
        assert!(state.cancellations.free_inventory_valid());
        assert_ne!(
            Arc::as_ptr(state.cancellations.cell(first_slot)),
            first_address
        );
        assert_eq!(
            state.cancellations.slots[first_slot.0].phase,
            SlotPhase::Node(second_scope.id)
        );
    }
    assert!(
        view.reason().is_none(),
        "the old observer still sees its original cell"
    );
    second.business.release();
    second.owner.complete();
    {
        let mut state = control.inner.state.lock().unwrap();
        assert!(state.cancellations.drained());
        let next = state.cancellations.slots[first_slot.0].next_free;
        state.cancellations.slots[first_slot.0].next_free = Some(first_slot);
        assert!(
            !state.cancellations.drained(),
            "legacy cycles also have a finite visit budget"
        );
        state.cancellations.slots[first_slot.0].next_free = next;
        assert!(state.cancellations.drained());
    }
    drop(view);
}

#[test]
fn legacy_generation_exhaustion_does_not_pop_or_lose_the_vacant_head() {
    let control = legacy_control();
    let work = root(&control);
    let original = slot(&control, work.owner.scope().id);
    work.business.release();
    work.owner.complete();
    {
        let mut state = control.inner.state.lock().unwrap();
        state.cancellations.slots[original.0].generation = u64::MAX;
    }
    assert!(matches!(
        control.try_begin_root(WorkRequest::new(WorkClass::Query)),
        Err(WorkError::ArithmeticOverflow)
    ));
    let state = control.inner.state.lock().unwrap();
    assert_eq!(state.cancellations.legacy_vacant, Some(original));
    assert_eq!(state.cancellations.slots.len(), 1);
    assert!(state.cancellations.drained());
    assert!(state.nodes.is_empty());
}

#[test]
fn storage_fanout_is_finite_configurable_and_charged_from_actual_layout() {
    let narrow = ControlStorageConfig {
        slots_per_class: [4, 0, 0, 0, 0],
        children_per_slot: 1,
        proposals_per_slot: 2,
        poll_roles: &[],
    };
    let wider = ControlStorageConfig {
        children_per_slot: 2,
        ..narrow
    };
    let impossible = ControlStorageConfig {
        children_per_slot: 4,
        ..narrow
    };
    let (control, _, _driver) = prototype_profile(4, 1, 1, 1);
    let geometry = {
        let state = control.inner.state.lock().unwrap();
        state.joint.as_ref().unwrap().config_for_test()
    };
    let narrow = crate::joint_admission::ledger::control_storage_base(narrow, &geometry).unwrap();
    let wider = crate::joint_admission::ledger::control_storage_base(wider, &geometry).unwrap();
    assert_eq!(
        wider, narrow,
        "fanout uses the same original N-slot edge inventory"
    );
    assert!(matches!(
        crate::joint_admission::ledger::control_storage_base(impossible, &geometry),
        Err(WorkError::InvalidConfig("fixed control storage geometry"))
    ));
}

#[test]
fn constructor_cells_are_reserved_before_build_and_reaped_while_the_work_is_live() {
    let (control, handle, mut driver) = prototype_full(3, 1);
    let work = root(&control);
    let scope = work.owner.scope();
    let first = handle.reserve_proposal(&scope).unwrap();
    let second = handle.reserve_proposal(&scope).unwrap();
    assert!(matches!(
        handle.reserve_proposal(&scope),
        Err(WorkError::Capacity("fixed proposal and reply cells"))
    ));
    let own_slot = slot(&control, scope.id());
    drop(first);
    drain(&mut driver);
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&scope.id())
    );
    let reused = handle.reserve_proposal(&scope).unwrap();
    {
        let state = control.inner.state.lock().unwrap();
        let cells = &state.cancellations.slots[own_slot.0].proposals;
        assert_eq!(cells[0].generation, 2);
        assert_eq!(cells[1].generation, 1);
    }
    drop((second, reused));
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn aborted_constructor_keeps_the_original_node_until_its_actual_exit() {
    let (control, handle, mut driver) = prototype_full(3, 1);
    let work = root(&control);
    let scope = work.owner.scope();
    let building = handle.reserve_proposal(&scope).unwrap();
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert_eq!(control.inner.state.lock().unwrap().nodes.len(), 1);
    drop(building);
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn withdrawn_request_cannot_reuse_a_body_still_held_by_the_original_driver() {
    use crate::joint_admission::{JointClaimChange, JointLane};
    let (control, handle, mut driver) = prototype_full(3, 1);
    let work = root(&control);
    let scope = work.owner.scope();
    let request = || {
        handle.request(
            &scope,
            vec![JointClaimChange::New {
                dependencies: Vec::new(),
                lane: JointLane::Custody,
                amounts: JointResources::ZERO,
            }],
        )
    };
    let first = request().unwrap();
    assert_eq!(driver.step(), JointProgress::More); // Original Turn takes the body.
    drop(first);
    let second = request().unwrap();
    assert!(matches!(
        request(),
        Err(crate::joint_admission::JointAdmissionFailure {
            error: WorkError::Capacity("fixed proposal and reply cells")
        })
    ));
    drain(&mut driver);
    let third = request().unwrap();
    drop((second, third));
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

#[test]
fn empty_delivered_container_and_its_iterator_retain_the_original_body_fee() {
    use crate::joint_admission::{JointClaimChange, JointLane};
    let (control, handle, mut driver) = prototype_full(3, 1);
    let dependency = selected_dependency(&handle, "reply");
    let work = root(&control);
    let mut binding = first_bind(&handle, &work.owner, &dependency);
    drain(&mut driver);
    binding_receipt(&mut binding)
        .install_control(&work.owner, &[])
        .unwrap();
    drain(&mut driver);
    let mut request = handle
        .request(
            &work.owner.scope(),
            vec![JointClaimChange::New {
                dependencies: vec![dependency.clone()],
                lane: JointLane::Custody,
                amounts: JointResources::ZERO.with(JointResource::ClaimRecords, 1),
            }],
        )
        .unwrap();
    drain(&mut driver);
    let Poll::Ready(Ok(mut batch)) =
        std::pin::Pin::new(&mut request).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("original reply")
    };
    assert!(batch.adjustment.is_none());
    drop(batch.new_claims.pop().unwrap());
    work.business.release();
    work.owner.complete();
    drain(&mut driver);
    assert!(
        control.inner.state.lock().unwrap().nodes.is_empty(),
        "logical Node has exited"
    );
    assert_eq!(snapshot(&control).3, 1, "old bound control fee remains");
    let mut iter = batch.new_claims.into_iter();
    assert!(iter.next().is_none());
    let marker = drain(&mut driver);
    assert_eq!(snapshot(&control).3, 1, "empty iterator still owns backing");
    drop(iter);
    assert_ne!(marker, control.progress_revision());
    drain(&mut driver);
    assert_eq!(snapshot(&control).3, 0);
}

struct WithdrawAndPanicReplyWake {
    request: Mutex<Option<crate::joint_admission::JointAdmission>>,
    entered: std::sync::mpsc::SyncSender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl std::task::Wake for WithdrawAndPanicReplyWake {
    fn wake(self: Arc<Self>) {
        drop(self.request.lock().unwrap().take());
        self.entered.send(()).unwrap();
        self.resume
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        panic!("intentional reply callback unwind after actual withdrawal");
    }
}

#[test]
fn driver_turn_exit_notifies_after_a_reply_callback_withdraws_and_panics() {
    use crate::joint_admission::{JointClaimChange, JointLane};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let (control, handle, mut driver) = prototype_full(3, 1);
    let work = root(&control);
    let own_slot = slot(&control, work.owner.scope().id());
    let mut request = handle
        .request(
            &work.owner.scope(),
            vec![JointClaimChange::New {
                dependencies: Vec::new(),
                lane: JointLane::Custody,
                amounts: JointResources::ZERO,
            }],
        )
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
    let wake = Arc::new(WithdrawAndPanicReplyWake {
        request: Mutex::new(None),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    });
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        std::pin::Pin::new(&mut request)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    *wake.request.lock().unwrap() = Some(request);
    let worker = std::thread::spawn(move || {
        let panic = catch_unwind(AssertUnwindSafe(|| drain(&mut driver)));
        assert!(panic.is_err(), "the real reply callback must unwind");
        driver
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    work.business.release();
    work.owner.complete();
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.nodes.is_empty());
        assert_eq!(state.cancellations.retired_proposals, 1);
        assert_eq!(
            state.cancellations.slots[own_slot.0].phase,
            SlotPhase::Retired
        );
        assert_eq!(
            Arc::strong_count(&state.cancellations.slots[own_slot.0].proposals[0].body),
            2,
            "only the fixed anchor and paused original Turn remain"
        );
    }
    let before_exit = control.progress_revision();
    resume_tx.send(()).unwrap();
    let mut driver = worker.join().unwrap();
    assert_ne!(
        before_exit,
        control.progress_revision(),
        "actual Turn exit must publish a later generation"
    );
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().cancellations.drained());
}

struct PanicOnceProgressWake(std::sync::atomic::AtomicBool);
impl std::task::Wake for PanicOnceProgressWake {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional progress wake panic during reply handoff");
        }
    }
}

#[test]
fn reply_handoff_panic_releases_undelivered_claims_for_new_and_mixed_batches() {
    use crate::joint_admission::{JointClaimChange, JointLane};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    for mixed in [false, true] {
        let (control, handle, mut driver) = prototype_profile(3, 1, 1, 2);
        let dependency = selected_dependency(&handle, "handoff");
        let work = root(&control);
        let scope = work.owner.scope();
        let mut changes = vec![JointClaimChange::New {
            dependencies: vec![dependency.clone()],
            lane: JointLane::Custody,
            amounts: JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        }];
        if mixed {
            changes.push(JointClaimChange::FirstBind {
                source: work.owner.control_claim_view().unwrap(),
                dependencies: vec![dependency],
            });
        }
        let mut request = handle.request(&scope, changes).unwrap();
        drain(&mut driver);
        let wake = Waker::from(Arc::new(PanicOnceProgressWake(
            std::sync::atomic::AtomicBool::new(true),
        )));
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&wake))
                .is_pending()
        );
        let panic = catch_unwind(AssertUnwindSafe(|| {
            std::pin::Pin::new(&mut request).poll(&mut Context::from_waker(Waker::noop()))
        }));
        assert!(
            panic.is_err(),
            "the actual progress notification must unwind"
        );
        drop((waiter, request));
        let (members, holders, waiters) = {
            let state = control.inner.state.lock().unwrap();
            let node = &state.nodes[&scope.id()];
            (
                state.joint.as_ref().unwrap().control_snapshot_for_test().1,
                node.resource_holders,
                node.resource_waiters,
            )
        };
        assert_eq!(members, 3, "no undelivered physical claim may escape");
        assert_eq!(holders, 0);
        assert_eq!(waiters, 0);
        work.business.release();
        work.owner.complete();
        drain(&mut driver);
        assert!(control.inner.state.lock().unwrap().cancellations.drained());
    }
}

#[test]
fn metadata_reservation_tracks_actual_service_geometry_and_rejects_overflow_before_build() {
    let (control, _, _driver) = prototype_profile(4, 1, 1, 1);
    let geometry = {
        let state = control.inner.state.lock().unwrap();
        state.joint.as_ref().unwrap().config_for_test()
    };
    let storage = ControlStorageConfig {
        slots_per_class: [4, 0, 0, 0, 0],
        children_per_slot: 1,
        proposals_per_slot: 2,
        poll_roles: &[],
    };
    let quote = |geometry: &JointCapacityConfig| {
        crate::joint_admission::ledger::control_storage_base(storage, geometry)
    };
    let baseline = quote(&geometry).unwrap().get(JointResource::MetadataBytes);
    for larger in [
        JointCapacityConfig {
            max_registered_dependencies: geometry.max_registered_dependencies * 2,
            ..geometry.clone()
        },
        JointCapacityConfig {
            max_dependencies_per_claim: 2,
            ..geometry.clone()
        },
        JointCapacityConfig {
            max_changes_per_request: 2,
            ..geometry.clone()
        },
        JointCapacityConfig {
            max_claims: 16,
            ..geometry.clone()
        },
    ] {
        assert!(
            quote(&larger).unwrap().get(JointResource::MetadataBytes) > baseline,
            "fixed cells cannot reuse a smaller metadata fee for a larger service"
        );
    }
    for overflow in [
        JointCapacityConfig {
            max_dependencies_per_claim: usize::MAX,
            ..geometry.clone()
        },
        JointCapacityConfig {
            max_registered_dependencies: usize::MAX,
            ..geometry.clone()
        },
        JointCapacityConfig {
            max_changes_per_request: usize::MAX,
            ..geometry.clone()
        },
        JointCapacityConfig {
            max_claims: usize::MAX,
            ..geometry.clone()
        },
    ] {
        assert!(matches!(
            quote(&overflow),
            Err(WorkError::ArithmeticOverflow)
        ));
    }
}
