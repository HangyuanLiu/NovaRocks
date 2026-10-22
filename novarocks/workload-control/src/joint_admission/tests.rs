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
            NetworkServiceOrigin::from_selected_url(&format!("https://{name}.example")).unwrap(),
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
fn poll(request: &mut JointAdmission) -> Poll<Result<Vec<JointClaim>, JointAdmissionFailure>> {
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
        Poll::Ready(Ok(claims)) => claims,
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
    let mut original = granted(&mut first);
    let replace = handle
        .request(
            &scope,
            vec![JointClaimChange::Replace {
                original: original.pop().unwrap(),
                amounts: bytes(2),
            }],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(held(&control), (1, 2));
    drop(replace);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn foreign_authority_and_owner_rejection_return_original_capability() {
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
    let mut claims = granted(&mut first);
    let failed = handle.request(
        &other.owner.scope(),
        vec![JointClaimChange::Replace {
            original: claims.pop().unwrap(),
            amounts: bytes(2),
        }],
    );
    let Err(failed) = failed else {
        panic!("foreign scope accepted");
    };
    assert_eq!(failed.error, WorkError::ForeignAuthority);
    assert_eq!(failed.originals[0].amounts().unwrap(), bytes(1));
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
fn driver_exit_returns_originals_and_never_mints_a_grant() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let mut claims = granted(&mut first);
    let mut pending = handle
        .request(
            &scope,
            vec![JointClaimChange::Replace {
                original: claims.pop().unwrap(),
                amounts: bytes(2),
            }],
        )
        .unwrap();
    driver.step();
    drop(driver);
    match poll(&mut pending) {
        Poll::Ready(Err(failed)) => {
            assert_eq!(failed.error, WorkError::Closed);
            assert_eq!(failed.originals[0].amounts().unwrap(), bytes(1));
        }
        _ => panic!("driver exit did not reject"),
    }
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
