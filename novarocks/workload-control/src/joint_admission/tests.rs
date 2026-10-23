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

use super::*;
use crate::{WorkRequest, WorkScope, WorkloadConfig, WorkloadControl};
use novarocks_resource_contract::{NetworkServiceOrigin, SharedDependency};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn bytes(amount: u64) -> JointResources {
    JointResources::ZERO.with(JointResource::PayloadBytes, amount)
}
fn limits(global: u64, dependency: u64, progress: u64) -> JointResourceLimits {
    JointResourceLimits {
        global: bytes(global),
        per_dependency: bytes(dependency),
        protected_progress: bytes(progress),
    }
}
fn profile() -> (JointCapacityConfig, RequiredJointProgress) {
    let mut classes = [JointResourceLimits::default(); CLASS_COUNT];
    let mut lanes = [JointResourceLimits::default(); LANE_COUNT];
    let mut class_lanes = [[JointResourceLimits::default(); LANE_COUNT]; CLASS_COUNT];
    classes[4] = limits(6, 4, 2);
    lanes[1] = limits(3, 2, 1);
    lanes[2] = limits(3, 2, 1);
    class_lanes[4][1] = limits(3, 2, 1);
    class_lanes[4][2] = limits(3, 2, 1);
    (
        JointCapacityConfig {
            global: limits(6, 4, 2),
            classes,
            lanes,
            class_lanes,
            max_dependencies_per_claim: 2,
            max_registered_dependencies: 8,
            max_claims: 32,
            max_requests: 16,
            max_changes_per_request: 4,
            max_snapshot_bytes: 4 * 1024 * 1024,
            max_scratch_bytes: 4 * 1024 * 1024,
            max_residual_arcs: 10000,
            max_total_proof_work: 10_000_000,
            work_per_turn: NonZeroUsize::new(17).unwrap(),
        },
        RequiredJointProgress {
            global: bytes(2),
            classes: [JointResources::ZERO; CLASS_COUNT],
            lanes: [JointResources::ZERO; LANE_COUNT],
            class_lanes: [[JointResources::ZERO; LANE_COUNT]; CLASS_COUNT],
        },
    )
}
fn control() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let (profile, required) = profile();
    let (handle, driver) = control
        .configure_joint_capacity(profile.validate(&required).unwrap())
        .unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}
fn dependency(handle: &JointAdmissionHandle, name: &str) -> JointDependency {
    handle
        .register_dependency(SharedDependency::Network(
            NetworkServiceOrigin::from_selected_url(
                &url::Url::parse(&format!("https://{name}.example")).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap()
}
fn request(
    handle: &JointAdmissionHandle,
    scope: &WorkScope,
    deps: &[&JointDependency],
    lane: JointLane,
    amount: u64,
) -> JointAdmission {
    handle
        .request(
            scope,
            vec![JointClaimChange::New {
                dependencies: deps.iter().map(|dep| (*dep).clone()).collect(),
                lane,
                amounts: bytes(amount),
            }],
        )
        .unwrap()
}
fn poll(request: &mut JointAdmission) -> Poll<Result<JointGrantedBatch, JointAdmissionFailure>> {
    Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..200_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("joint progression did not become idle");
}
fn granted(request: &mut JointAdmission) -> Vec<JointClaim> {
    match poll(request) {
        Poll::Ready(Ok(batch)) => {
            assert!(batch.adjustment.is_none());
            batch.new_claims
        }
        other => panic!("expected original grant: {other:?}"),
    }
}
fn held(control: &WorkloadControl) -> (usize, u64) {
    let state = control.inner.state.lock().unwrap();
    let ledger = state.joint.as_ref().unwrap();
    (
        ledger.claims.len(),
        ledger
            .claims
            .values()
            .map(|claim| claim.amounts.get(JointResource::PayloadBytes))
            .sum(),
    )
}

#[test]
fn static_reservations_preserve_dependency_and_parent_intersection_headroom() {
    let (config, required) = profile();
    config.clone().validate(&required).unwrap();
    let mut bad = config.clone();
    bad.global.per_dependency = bytes(3);
    assert!(bad.validate(&required).is_err());
    let mut bad = config.clone();
    bad.global.protected_progress = bytes(3);
    assert!(bad.validate(&required).is_err());
    let mut bad = config.clone();
    bad.classes[4].per_dependency = bytes(3);
    assert!(bad.validate(&required).is_err());
    let mut bad = config;
    bad.lanes[1].protected_progress = bytes(0);
    assert!(bad.validate(&required).is_err());
}

#[test]
fn saturated_bad_dependency_does_not_take_healthy_or_settlement_reservations() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let mut original = granted(&mut first);
    let mut blocked = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    let mut healthy = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    let mut settlement = request(&handle, &scope, &[&a], JointLane::Settlement, 1);
    drive(&mut driver);
    assert!(poll(&mut blocked).is_pending());
    let healthy = granted(&mut healthy);
    let settlement = granted(&mut settlement);
    assert_eq!(held(&control), (3, 4));
    // A release wakes the same queued request; no fresh identity or class reset.
    original[0].release_part(bytes(1)).unwrap();
    drive(&mut driver);
    let blocked = granted(&mut blocked);
    assert_eq!(held(&control), (4, 4));
    drop((original, healthy, settlement, blocked));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn exact_dependency_batch_is_atomic_and_sequential_not_independent() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let c = dependency(&handle, "c");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut batch = handle
        .request(
            &scope,
            vec![
                JointClaimChange::New {
                    dependencies: vec![b],
                    lane: JointLane::Foreground,
                    amounts: bytes(1),
                },
                JointClaimChange::New {
                    dependencies: vec![c],
                    lane: JointLane::Foreground,
                    amounts: bytes(2),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    assert!(poll(&mut batch).is_pending());
    assert_eq!(held(&control), (1, 1));
    drop(original);
    drive(&mut driver);
    let claims = granted(&mut batch);
    assert_eq!(claims.len(), 2);
    assert_eq!(held(&control), (2, 3));
}

#[test]
fn concurrent_release_is_not_resurrected_by_positive_shadow_commit() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut second = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    assert_eq!(driver.step(), JointProgress::More);
    assert_eq!(driver.step(), JointProgress::More); // Arena allocated.
    assert_eq!(driver.step(), JointProgress::More); // Original copied before release.
    drop(original);
    drive(&mut driver);
    let second = granted(&mut second);
    assert_eq!(held(&control), (1, 1));
    drop(second);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn release_during_negative_proof_is_reobserved_instead_of_cached_as_current() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut second = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    assert_eq!(driver.step(), JointProgress::More);
    assert_eq!(driver.step(), JointProgress::More);
    assert_eq!(driver.step(), JointProgress::More); // Observe the old charge first.
    drop(original);
    drive(&mut driver);
    let second = granted(&mut second);
    assert_eq!(held(&control), (1, 1));
    drop(second);
}

#[test]
fn granted_but_unreceived_drop_returns_exact_capacity_without_releasing_another_claim() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let original = granted(&mut first);
    let replace = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original[0].view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(held(&control), (1, 2));
    drop(replace);
    assert_eq!(held(&control), (1, 1));
    assert_eq!(original[0].amounts().unwrap(), bytes(1));
    drop(original);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn foreign_authority_rejection_leaves_original_capability_in_place() {
    let (control, handle, mut driver) = control();
    let (foreign, foreign_handle, _foreign_driver) = self::control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let other = foreign
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let a = dependency(&handle, "a");
    let foreign_a = dependency(&foreign_handle, "a");
    let foreign_request = handle.request(
        &scope,
        vec![JointClaimChange::New {
            dependencies: vec![foreign_a],
            lane: JointLane::Foreground,
            amounts: bytes(1),
        }],
    );
    assert!(matches!(
        foreign_request,
        Err(JointAdmissionFailure {
            error: WorkError::ForeignAuthority,
            ..
        })
    ));
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let claims = granted(&mut first);
    let failed = handle.request(
        &other.owner.scope(),
        vec![JointClaimChange::grow_by(
            claims[0].view().unwrap(),
            bytes(1),
            vec![],
        )],
    );
    let Err(failed) = failed else {
        panic!("foreign scope accepted");
    };
    assert_eq!(failed.error, WorkError::ForeignAuthority);
    assert_eq!(claims[0].amounts().unwrap(), bytes(1));
}

#[test]
fn request_drop_clears_fence_only_after_the_original_driver_observes_it() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let request = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    driver.step();
    drop(request);
    assert!(matches!(
        handle.register_dependency(SharedDependency::LocalHost),
        Err(WorkError::Capacity(_))
    ));
    drive(&mut driver);
    handle
        .register_dependency(SharedDependency::LocalHost)
        .unwrap();
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn driver_exit_keeps_original_holder_and_never_mints_growth() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let claims = granted(&mut first);
    let mut pending = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                claims[0].view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    driver.step();
    drop(driver);
    match poll(&mut pending) {
        Poll::Ready(Err(failed)) => {
            assert_eq!(failed.error, WorkError::Closed);
            assert_eq!(claims[0].amounts().unwrap(), bytes(1));
        }
        _ => panic!("driver exit did not reject"),
    }
    assert_eq!(held(&control), (1, 1));
    drop(claims);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn cancelled_infeasible_request_retires_without_another_capacity_proof() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut blocked = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    assert!(poll(&mut blocked).is_pending());
    root.owner.cancel(crate::CancellationReason::Requested);
    // Selection and the next turn observe cancellation before rebuilding input.
    driver.step();
    driver.step();
    match poll(&mut blocked) {
        Poll::Ready(Err(failed)) => assert_eq!(
            failed.error,
            WorkError::Cancelled(crate::CancellationReason::Requested)
        ),
        other => panic!("cancelled capacity request was not retired: {other:?}"),
    }
    assert_eq!(held(&control), (1, 2));
    let mut settlement = request(&handle, &scope, &[&a], JointLane::Settlement, 1);
    drive(&mut driver);
    let settlement = granted(&mut settlement);
    drop((original, settlement));
}

#[tokio::test]
async fn idle_revision_has_no_self_wake_and_covers_both_notify_orderings() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let JointProgress::Idle(revision) = driver.step() else {
        panic!("empty authority was not idle");
    };
    assert_eq!(
        control.progress_revision(),
        revision,
        "an idle observation must not publish its own event"
    );
    let expiry = control.deadline_expiry_handle();
    let waiting = expiry.wait_state_change(revision);
    tokio::pin!(waiting);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(first);
    drive(&mut driver);
    let JointProgress::Idle(revision) = driver.step() else {
        panic!("retired request did not become idle");
    };
    // Event between the same-lock idle verdict and subscription cannot be lost.
    let second = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    let waiting = expiry.wait_state_change(revision);
    tokio::pin!(waiting);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(second);
    drive(&mut driver);
}

#[test]
fn snapshot_builder_shares_the_finite_total_work_ceiling() {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let (mut profile, required) = profile();
    profile.max_total_proof_work = 8;
    let (handle, mut driver) = control
        .configure_joint_capacity(profile.validate(&required).unwrap())
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut pending = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    match poll(&mut pending) {
        Poll::Ready(Err(failed)) => assert!(matches!(failed.error, WorkError::Capacity(_))),
        other => panic!("builder exceeded its ceiling: {other:?}"),
    }
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn exact_profiles_aggregate_once_and_keep_distinct_dependencies_and_live_zero_claims() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut first = handle
        .request(
            &scope,
            vec![
                JointClaimChange::New {
                    dependencies: vec![a.clone()],
                    lane: JointLane::Foreground,
                    amounts: bytes(1),
                },
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(1),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let mut originals = granted(&mut first);
    let profiles = || {
        let state = control.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        let mut values = ledger
            .profiles
            .values()
            .map(|profile| {
                (
                    profile.claims,
                    profile.amounts.get(JointResource::PayloadBytes),
                )
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        values
    };
    assert_eq!(profiles(), vec![(2, 2)]);
    let mut second = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    drive(&mut driver);
    let mut second = granted(&mut second);
    assert_eq!(profiles(), vec![(1, 1), (2, 2)]);
    drop(originals.pop());
    assert_eq!(profiles(), vec![(1, 1), (1, 1)]);
    second[0].release_part(bytes(1)).unwrap();
    assert_eq!(profiles(), vec![(1, 0), (1, 1)]);
    let mut replacement = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                originals[0].view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut replacement)
        .install(&[&originals[0]])
        .unwrap();
    assert_eq!(profiles(), vec![(1, 0), (1, 2)]);
    drop(second);
    assert_eq!(profiles(), vec![(1, 2)]);
    drop(originals);
    assert!(profiles().is_empty());
}

#[test]
fn stable_growth_at_u64_boundary_uses_actual_release_and_preserves_both_holders() {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let (mut config, mut required) = profile();
    config.global = limits(u64::MAX, u64::MAX, 0);
    config.classes = [JointResourceLimits::default(); CLASS_COUNT];
    config.classes[4] = config.global;
    config.lanes = [JointResourceLimits::default(); LANE_COUNT];
    config.lanes[1] = config.global;
    config.class_lanes = [[JointResourceLimits::default(); LANE_COUNT]; CLASS_COUNT];
    config.class_lanes[4][1] = config.global;
    required.global = JointResources::ZERO;
    let (handle, mut driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = handle
        .request(
            &scope,
            vec![
                JointClaimChange::New {
                    dependencies: vec![a.clone()],
                    lane: JointLane::Foreground,
                    amounts: bytes(0),
                },
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(u64::MAX),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let mut original = granted(&mut initial).into_iter();
    let zero = original.next().unwrap();
    let mut full = original.next().unwrap();
    // A zero-delta batch still removes all certified old profile amounts
    // before re-adding its post-state. Adding before subtraction overflows.
    let mut reaffirm = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(zero.view().unwrap(), bytes(0), vec![]),
                JointClaimChange::grow_by(full.view().unwrap(), bytes(0), vec![]),
            ],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut reaffirm).install(&[&zero, &full]).unwrap();
    assert_eq!(held(&control), (2, u64::MAX));
    full.release_part(bytes(u64::MAX)).unwrap();
    let mut growth = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(zero.view().unwrap(), bytes(u64::MAX), vec![]),
                JointClaimChange::grow_by(full.view().unwrap(), bytes(0), vec![]),
            ],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut growth).install(&[&zero, &full]).unwrap();
    assert_eq!(zero.amounts().unwrap(), bytes(u64::MAX));
    assert_eq!(full.amounts().unwrap(), bytes(0));
    assert_eq!(held(&control), (2, u64::MAX));
    drop((zero, full));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn same_profile_other_owner_release_is_not_restored_by_affected_profile_commit() {
    let (control, handle, mut driver) = control();
    let first = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let second = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let a = dependency(&handle, "a");
    let mut initial = request(
        &handle,
        &first.owner.scope(),
        &[&a],
        JointLane::Foreground,
        1,
    );
    drive(&mut driver);
    let initial = granted(&mut initial);
    let mut next = request(
        &handle,
        &second.owner.scope(),
        &[&a],
        JointLane::Foreground,
        1,
    );
    driver.step();
    driver.step();
    driver.step(); // Same-key old amount copied.
    drop(initial);
    first.owner.complete();
    first.business.release();
    drive(&mut driver);
    let next = granted(&mut next);
    assert_eq!(held(&control), (1, 1));
    // A stale aggregate would still contain 2 and falsely block this request.
    let third = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let mut last = request(
        &handle,
        &third.owner.scope(),
        &[&a],
        JointLane::Foreground,
        1,
    );
    drive(&mut driver);
    let last = granted(&mut last);
    assert_eq!(held(&control), (2, 2));
    {
        let state = control.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        assert_eq!(ledger.profiles.len(), 1);
        assert_eq!(ledger.profiles.values().next().unwrap().amounts, bytes(2));
    }
    drop((next, last));
    assert_eq!(held(&control), (0, 0));
}

fn batch(request: &mut JointAdmission) -> JointGrantedBatch {
    match poll(request) {
        Poll::Ready(Ok(batch)) => batch,
        other => panic!("expected granted batch: {other:?}"),
    }
}
fn adjustment(request: &mut JointAdmission) -> JointAdjustmentReceipt {
    let batch = batch(request);
    assert!(batch.new_claims.is_empty());
    batch.adjustment.expect("adjustment receipt")
}
fn roomy_control() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let (mut config, _) = profile();
    config.global = limits(100, 100, 0);
    config.classes = [JointResourceLimits::default(); CLASS_COUNT];
    config.classes[4] = config.global;
    config.lanes = [JointResourceLimits::default(); LANE_COUNT];
    config.lanes[1] = config.global;
    config.class_lanes = [[JointResourceLimits::default(); LANE_COUNT]; CLASS_COUNT];
    config.class_lanes[4][1] = config.global;
    let required = RequiredJointProgress {
        global: JointResources::ZERO,
        classes: [JointResources::ZERO; CLASS_COUNT],
        lanes: [JointResources::ZERO; LANE_COUNT],
        class_lanes: [[JointResources::ZERO; LANE_COUNT]; CLASS_COUNT],
    };
    let (handle, driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}

#[test]
fn waiting_future_drop_and_unwind_preserve_the_stable_holder() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _pending = handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(1),
                    vec![],
                )],
            )
            .unwrap();
        drive(&mut driver);
        panic!("caller unwinds while a complete growth vector is blocked");
    }));
    assert!(unwound.is_err());
    assert_eq!(original.amounts().unwrap(), bytes(2));
    assert_eq!(held(&control), (1, 2));
    let next = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![],
            )],
        )
        .unwrap();
    drop(next);
    drop(original);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn receipt_drop_uses_live_active_after_partial_release() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let mut original = granted(&mut initial).pop().unwrap();
    let old_view = original.view().unwrap();
    let mut growth = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(4),
                vec![b],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut growth);
    assert_eq!(original.amounts().unwrap(), bytes(10));
    assert_eq!(held(&control), (1, 14));
    original.release_part(bytes(3)).unwrap();
    assert_eq!(held(&control), (1, 11));
    drop(receipt);
    assert_eq!(original.amounts().unwrap(), bytes(7));
    assert_eq!(held(&control), (1, 7));
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(old_view, bytes(1), vec![])]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    let mut next = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut next).install(&[&original]).unwrap();
    assert_eq!(original.amounts().unwrap(), bytes(8));
}

#[test]
fn zero_delta_dependency_extension_proves_the_full_original_amount() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut extend = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![b.clone()],
            )],
        )
        .unwrap();
    drive(&mut driver);
    // Two domains require protected headroom for their intersection; charging
    // only the zero delta would falsely grant this extension.
    assert!(poll(&mut extend).is_pending());
    assert_eq!(held(&control), (1, 2));
    assert_eq!(original.amounts().unwrap(), bytes(2));
    let mut healthy = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    drive(&mut driver);
    let healthy = granted(&mut healthy);
    assert_eq!(held(&control), (2, 3));
    drop((extend, healthy, original));
}

#[test]
fn single_pending_and_queued_partial_release_have_exact_conflict() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let mut original = granted(&mut initial).pop().unwrap();
    let mut grow = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![]
            )]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    driver.step();
    original.release_part(bytes(1)).unwrap();
    drive(&mut driver);
    assert!(matches!(
        poll(&mut grow),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Conflict
        }))
    ));
    assert_eq!(original.amounts().unwrap(), bytes(0));
    assert_eq!(held(&control), (1, 0));
}

#[test]
fn delivered_new_survives_source_drop_and_late_receipt_withdrawal() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut mixed = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(original.view().unwrap(), bytes(4), vec![]),
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(5),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let delivered = batch(&mut mixed);
    assert_eq!(held(&control), (2, 19));
    drop(original);
    assert_eq!(held(&control), (1, 5));
    let receipt = delivered.adjustment.unwrap();
    assert_eq!(receipt.install(&[]), Err(WorkError::Released));
    assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
    drop(delivered.new_claims);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn batch_install_is_atomic_and_does_not_return_replacement_guards() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut second = request(&handle, &scope, &[&a], JointLane::Foreground, 20);
    drive(&mut driver);
    let first = granted(&mut first).pop().unwrap();
    let second = granted(&mut second).pop().unwrap();
    let mut growth = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(first.view().unwrap(), bytes(4), vec![]),
                JointClaimChange::grow_by(second.view().unwrap(), bytes(5), vec![]),
            ],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(
        adjustment(&mut growth).install(&[&first]),
        Err(WorkError::Conflict)
    );
    assert_eq!(held(&control), (2, 30));
    assert_eq!(first.amounts().unwrap(), bytes(10));
    assert_eq!(second.amounts().unwrap(), bytes(20));
}

#[test]
fn cancel_before_install_rolls_back_delta_but_install_before_cancel_preserves_actual_custody() {
    for cancel_first in [true, false] {
        let (control, handle, mut driver) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let mut growth = handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(1),
                    vec![],
                )],
            )
            .unwrap();
        drive(&mut driver);
        let receipt = adjustment(&mut growth);
        if cancel_first {
            root.owner.cancel(crate::CancellationReason::Requested);
            assert!(receipt.install(&[&original]).is_err());
            assert_eq!(original.amounts().unwrap(), bytes(1));
        } else {
            receipt.install(&[&original]).unwrap();
            root.owner.cancel(crate::CancellationReason::Requested);
            assert_eq!(original.amounts().unwrap(), bytes(2));
        }
        assert_eq!(held(&control).0, 1);
        drop(original);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn rollback_rekey_behind_snapshot_cursor_forces_fair_reobservation() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    // Keep the lower key {A} present after the live 60-byte charge moves to
    // {A,B}. Snapshot copies this zero group before the rollback rekeys 60.
    let mut zero = request(&handle, &scope, &[&a], JointLane::Foreground, 0);
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 60);
    drive(&mut driver);
    let zero = granted(&mut zero);
    let first = granted(&mut first).pop().unwrap();
    let mut extend = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                first.view().unwrap(),
                bytes(0),
                vec![b.clone()],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut extend);
    let mut blocked = request(&handle, &scope, &[&b], JointLane::Foreground, 50);
    // Selection, bounded arena allocation, then the first exact profile copy.
    driver.step();
    driver.step();
    driver.step();
    drop(receipt);
    drive(&mut driver);
    assert!(poll(&mut blocked).is_pending());
    assert_eq!(first.amounts().unwrap(), bytes(60));
    // A later feasible request still progresses; neither a stale proof nor a
    // stale negative cache may bypass the global 100-byte bound or stop scans.
    let mut healthy = request(&handle, &scope, &[&b], JointLane::Foreground, 40);
    drive(&mut driver);
    let healthy = granted(&mut healthy);
    assert_eq!(held(&control), (3, 100));
    drop((zero, first, healthy));
    drive(&mut driver);
    let blocked = granted(&mut blocked);
    assert_eq!(held(&control), (1, 50));
    drop(blocked);
}

#[test]
fn granted_partial_release_then_install_adopts_only_live_base_and_delta() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let mut original = granted(&mut initial).pop().unwrap();
    let mut grow = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(4),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut grow);
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![]
            )]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    original.release_part(bytes(3)).unwrap();
    receipt.install(&[&original]).unwrap();
    assert_eq!(original.amounts().unwrap(), bytes(11));
    assert_eq!(held(&control), (1, 11));
    drop(original);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn source_drop_before_reply_refunds_only_unstarted_batch_and_reports_released() {
    for grant_first in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let mut mixed = handle
            .request(
                &scope,
                vec![
                    JointClaimChange::grow_by(original.view().unwrap(), bytes(4), vec![]),
                    JointClaimChange::New {
                        dependencies: vec![a],
                        lane: JointLane::Foreground,
                        amounts: bytes(5),
                    },
                ],
            )
            .unwrap();
        if grant_first {
            drive(&mut driver);
        } else {
            driver.step();
        }
        drop(original);
        assert!(matches!(
            poll(&mut mixed),
            Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Released
            }))
        ));
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn driver_exit_after_delivery_refunds_delta_and_preserves_delivered_new() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut mixed = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(original.view().unwrap(), bytes(4), vec![]),
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(5),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let delivered = batch(&mut mixed);
    drop(driver);
    assert_eq!(held(&control), (2, 15));
    assert_eq!(
        delivered.adjustment.unwrap().install(&[&original]),
        Err(WorkError::Closed)
    );
    assert_eq!(original.amounts().unwrap(), bytes(10));
    assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
    drop((delivered.new_claims, original));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn epoch_overflow_without_active_turn_settles_all_tickets_without_refunding_delivered_new() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut second = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut third = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let first = granted(&mut first).pop().unwrap();
    let second = granted(&mut second).pop().unwrap();
    let mut third = granted(&mut third).pop().unwrap();
    let mixed = |source: &JointClaim, amount| {
        handle
            .request(
                &scope,
                vec![
                    JointClaimChange::grow_by(source.view().unwrap(), bytes(4), vec![]),
                    JointClaimChange::New {
                        dependencies: vec![a.clone()],
                        lane: JointLane::Foreground,
                        amounts: bytes(amount),
                    },
                ],
            )
            .unwrap()
    };
    let mut delivered = mixed(&first, 5);
    let mut unreceived = mixed(&second, 6);
    drive(&mut driver);
    let delivered = batch(&mut delivered);
    let mut waiting = mixed(&third, 7);
    control.inner.update(|state| {
        let ledger = state.joint.as_mut().unwrap();
        assert!(ledger.active.is_none());
        ledger.release_epoch = u64::MAX;
    });
    third.release_part(bytes(1)).unwrap();
    drive(&mut driver);
    assert_eq!(held(&control), (4, 34));
    assert!(matches!(
        poll(&mut unreceived),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Closed
        }))
    ));
    assert!(matches!(
        poll(&mut waiting),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Closed
        }))
    ));
    assert_eq!(
        delivered.adjustment.unwrap().install(&[&first]),
        Err(WorkError::Closed)
    );
    assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
    assert_eq!(third.amounts().unwrap(), bytes(9));
    drop((first, second, third, delivered.new_claims));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn closed_before_reply_refuses_new_permission_without_a_driver_turn() {
    for receive_first in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        let mut trigger = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
        drive(&mut driver);
        let first = granted(&mut first).pop().unwrap();
        let mut trigger = granted(&mut trigger).pop().unwrap();
        let mut mixed = handle
            .request(
                &scope,
                vec![
                    JointClaimChange::grow_by(first.view().unwrap(), bytes(4), vec![]),
                    JointClaimChange::New {
                        dependencies: vec![a],
                        lane: JointLane::Foreground,
                        amounts: bytes(5),
                    },
                ],
            )
            .unwrap();
        drive(&mut driver);
        let delivered = receive_first.then(|| batch(&mut mixed));
        control.inner.update(|state| {
            state.joint.as_mut().unwrap().release_epoch = u64::MAX;
        });
        trigger.release_part(bytes(1)).unwrap();
        // Poll races the close before the driver observes it. A previously
        // delivered New is already owned; an unreceived grant is not permission.
        if let Some(delivered) = delivered {
            assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
            assert_eq!(
                delivered.adjustment.unwrap().install(&[&first]),
                Err(WorkError::Closed)
            );
            assert_eq!(held(&control), (3, 15));
            drop(delivered.new_claims);
        } else {
            assert!(matches!(
                poll(&mut mixed),
                Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Closed
                }))
            ));
            assert_eq!(held(&control), (2, 10));
        }
        assert_eq!(first.amounts().unwrap(), bytes(10));
        drop((first, trigger));
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn request_publication_rechecks_sources_after_outside_lock_construction() {
    for release_source in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut original = granted(&mut initial).pop().unwrap();
        let changes = vec![JointClaimChange::grow_by(
            original.view().unwrap(),
            bytes(4),
            vec![],
        )];
        let prepared = handle.prepare_request(&scope, &changes).unwrap();
        if release_source {
            original.release_part(bytes(3)).unwrap();
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Conflict)
            ));
            assert_eq!(held(&control), (1, 7));
        } else {
            let pending = handle.request(&scope, changes).unwrap();
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Conflict)
            ));
            assert_eq!(held(&control), (1, 10));
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
                1
            );
            drop(pending);
        }
        drop(original);
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn request_publication_preserves_original_cancel_and_drop_errors() {
    for drop_source in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let changes = [JointClaimChange::grow_by(
            original.view().unwrap(),
            bytes(4),
            vec![],
        )];
        let prepared = handle.prepare_request(&scope, &changes).unwrap();
        if drop_source {
            drop(original);
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Released)
            ));
        } else {
            root.owner.cancel(crate::CancellationReason::Requested);
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Cancelled(crate::CancellationReason::Requested))
            ));
            assert_eq!(original.amounts().unwrap(), bytes(10));
            drop(original);
        }
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .requests
                .is_empty()
        );
    }
}

#[test]
fn a_late_view_cannot_address_a_later_claim_in_the_same_authority() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut first).pop().unwrap();
    let stale = original.view().unwrap();
    let original_id = *control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims
        .first_key_value()
        .unwrap()
        .0;
    drop(original);
    let mut later = request(&handle, &scope, &[&a], JointLane::Foreground, 20);
    drive(&mut driver);
    let later = granted(&mut later).pop().unwrap();
    let later_id = *control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims
        .first_key_value()
        .unwrap()
        .0;
    assert!(later_id > original_id);
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(stale, bytes(1), vec![])]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Released
        })
    ));
    assert_eq!(later.amounts().unwrap(), bytes(20));
    assert_eq!(held(&control), (1, 20));
}

#[test]
fn matching_numeric_claim_ids_do_not_cross_original_authorities() {
    let (control, handle, mut driver) = roomy_control();
    let (foreign, foreign_handle, mut foreign_driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let other = foreign
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let foreign_a = dependency(&foreign_handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut second = request(
        &foreign_handle,
        &other.owner.scope(),
        &[&foreign_a],
        JointLane::Foreground,
        20,
    );
    drive(&mut driver);
    drive(&mut foreign_driver);
    let original = granted(&mut first).pop().unwrap();
    let other = granted(&mut second).pop().unwrap();
    let ids = |control: &WorkloadControl| {
        control
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
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&control), ids(&foreign));
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                other.view().unwrap(),
                bytes(1),
                vec![]
            )]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::ForeignAuthority
        })
    ));
    let mut growth = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(4),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(
        adjustment(&mut growth).install(&[&other]),
        Err(WorkError::ForeignAuthority)
    );
    assert_eq!(original.amounts().unwrap(), bytes(10));
    assert_eq!(other.amounts().unwrap(), bytes(20));
    assert_eq!(held(&control), (1, 10));
    assert_eq!(held(&foreign), (1, 20));
}

#[test]
fn finite_growth_refuses_self_held_shortage_without_parking_or_releasing_base() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut growth = handle
        .request_finite(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![],
            )],
            std::num::NonZeroU64::new(10_000_000).unwrap(),
        )
        .unwrap();
    drive(&mut driver);
    assert!(matches!(
        poll(&mut growth),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Capacity("joint protected intersections")
        }))
    ));
    assert_eq!(original.amounts().unwrap(), bytes(2));
    assert_eq!(held(&control), (1, 2));
    let mut healthy = handle
        .request_finite(
            &scope,
            vec![JointClaimChange::New {
                dependencies: vec![b],
                lane: JointLane::Foreground,
                amounts: bytes(1),
            }],
            std::num::NonZeroU64::new(10_000_000).unwrap(),
        )
        .unwrap();
    drive(&mut driver);
    let healthy = granted(&mut healthy);
    assert_eq!(held(&control), (2, 3));
    drop((healthy, original));
}

#[test]
fn finite_proof_budget_survives_actual_layout_reobservations_before_builder_work() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut originals = Vec::new();
    let mut receipts = Vec::new();
    for _ in 0..3 {
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let mut extension = handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(0),
                    vec![b.clone()],
                )],
            )
            .unwrap();
        drive(&mut driver);
        receipts.push(adjustment(&mut extension));
        originals.push(original);
    }
    let mut finite = handle
        .request_finite(
            &scope,
            vec![JointClaimChange::New {
                dependencies: vec![b],
                lane: JointLane::Foreground,
                amounts: bytes(1),
            }],
            std::num::NonZeroU64::new(3).unwrap(),
        )
        .unwrap();
    let id = {
        let state = control.inner.state.lock().unwrap();
        *state
            .joint
            .as_ref()
            .unwrap()
            .requests
            .iter()
            .find(|(_, request)| matches!(request.phase, ledger::RequestPhase::Waiting))
            .unwrap()
            .0
    };
    for receipt in receipts {
        for _ in 0..100 {
            driver.step();
            if control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .active
                == Some(id)
            {
                break;
            }
        }
        assert_eq!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .active,
            Some(id)
        );
        // A real granted extension returns to its old DI before the first
        // builder step. Each reobservation must still debit the same budget.
        drop(receipt);
        driver.step();
    }
    drive(&mut driver);
    assert!(matches!(
        poll(&mut finite),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Capacity("joint finite proof work")
        }))
    ));
    assert_eq!(held(&control), (3, 30));
    for original in &originals {
        assert_eq!(original.amounts().unwrap(), bytes(10));
    }
    drop(originals);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn continuous_new_requests_cannot_overtake_a_finite_reobservation_forever() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut extension = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![b.clone()],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut extension);
    let mut finite = handle
        .request_finite(
            &scope,
            vec![JointClaimChange::New {
                dependencies: vec![b.clone()],
                lane: JointLane::Foreground,
                amounts: bytes(1),
            }],
            std::num::NonZeroU64::new(10_000_000).unwrap(),
        )
        .unwrap();
    for _ in 0..100 {
        driver.step();
        if control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .active
            .is_some()
        {
            break;
        }
    }
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .active
            .is_some()
    );
    drop(receipt);
    driver.step();
    let mut newcomer = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    let mut completed_newcomers = 0;
    let mut delivered = None;
    // Replace each completed newcomer before the next selector runs. The queue
    // stays bounded at two requests, but its largest identity keeps increasing.
    for _ in 0..200_000 {
        driver.step();
        if let Poll::Ready(result) = poll(&mut finite) {
            delivered = Some(result.unwrap().new_claims);
            break;
        }
        if let Poll::Ready(result) = poll(&mut newcomer) {
            drop(result.unwrap().new_claims);
            completed_newcomers += 1;
            if completed_newcomers == 8 {
                break;
            }
            newcomer = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
        }
    }
    assert!(
        delivered.is_some(),
        "new arrivals starved the original finite request"
    );
    assert_eq!(original.amounts().unwrap(), bytes(10));
    drop(newcomer);
    assert_eq!(held(&control), (2, 11));
    drop(delivered);
}

#[test]
fn reobservation_rechecks_actual_stop_and_source_facts_before_budget_failure() {
    for cancel in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut original = granted(&mut initial).pop().unwrap();
        let mut finite = handle
            .request_finite(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(1),
                    vec![],
                )],
                std::num::NonZeroU64::new(1).unwrap(),
            )
            .unwrap();
        for _ in 0..100 {
            driver.step();
            if control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .active
                .is_some()
            {
                break;
            }
        }
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .active
                .is_some()
        );
        // This is the real gap after the unlocked validity observation and
        // before reobserve acquires the final authority transaction.
        if cancel {
            root.owner
                .cancellation_requester()
                .request(crate::CancellationReason::Requested)
                .unwrap();
        } else {
            original.release_part(bytes(1)).unwrap();
        }
        driver.reobserve_active_for_test();
        let Poll::Ready(Err(failure)) = poll(&mut finite) else {
            panic!("the original stopped request must finish");
        };
        assert_eq!(
            failure.error,
            if cancel {
                WorkError::Cancelled(crate::CancellationReason::Requested)
            } else {
                WorkError::Conflict
            }
        );
        assert_eq!(
            original.amounts().unwrap(),
            bytes(if cancel { 10 } else { 9 })
        );
    }
}

#[test]
fn finite_pass_cannot_swallow_new_ticket_or_cancellation_before_its_last_skip() {
    for cancel_earlier in [false, true] {
        let (control, handle, mut driver) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut occupying = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
        drive(&mut driver);
        let original = granted(&mut occupying);
        // Both replies remain resident: an older blocked ticket and a later
        // granted Settlement ticket that is independent of cancellation.
        let mut blocked = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
        let mut delivered_later = request(&handle, &scope, &[], JointLane::Settlement, 0);
        drive(&mut driver);
        assert!(poll(&mut blocked).is_pending());
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .joint
                .as_ref()
                .unwrap()
                .requests
                .values()
                .any(|r| matches!(r.phase, ledger::RequestPhase::Granted { .. }))
        );
        // Start a new read-only pass. Its first ticket is blocked at the
        // current release epoch; leave the granted final ticket to be skipped.
        assert_eq!(driver.step(), JointProgress::More);
        let mut newcomer = if cancel_earlier {
            root.owner
                .cancellation_requester()
                .request(crate::CancellationReason::Requested)
                .unwrap();
            None
        } else {
            Some(request(&handle, &scope, &[], JointLane::Settlement, 0))
        };
        assert_eq!(
            driver.step(),
            JointProgress::More,
            "the last skip must not swallow a change to an earlier decision"
        );
        drive(&mut driver);
        if cancel_earlier {
            assert!(matches!(
                poll(&mut blocked),
                Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Cancelled(crate::CancellationReason::Requested)
                }))
            ));
        } else {
            drop(granted(newcomer.as_mut().unwrap()));
        }
        drop(granted(&mut delivered_later));
        drop(original);
    }
}
