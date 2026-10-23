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

//! Paid construction through the actual original queue/proof/receipt authors.

use super::*;
use novarocks_resource_contract::NetworkServiceOrigin;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn network(name: usize) -> SharedDependency {
    SharedDependency::Network(
        NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://paid-{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("bounded original driver must reach idle");
}
fn receive(request: &mut JointAdmission) -> JointGrantedBatch {
    let Poll::Ready(Ok(batch)) = Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("actual original grant reply")
    };
    batch
}
fn published(handle: &JointAdmissionHandle, fact: &SharedDependency) -> Option<bool> {
    handle
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .dependencies
        .as_ref()
        .unwrap()
        .published(fact)
}
fn root(control: &WorkloadControl) -> crate::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn finish(root: crate::RootWork) {
    root.business.release();
    root.owner.complete();
}

#[test]
fn original_q_is_reserved_before_backing_and_requires_actual_reap_before_reuse() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 2);
    let work = root(&control);
    let a = handle.begin_paid_proposal(&work.owner).unwrap();
    let b = handle.begin_paid_proposal(&work.owner).unwrap();
    assert!(matches!(
        handle.begin_paid_proposal(&work.owner),
        Err(WorkError::Capacity(_))
    ));
    assert_eq!(a.records.capacity(), 2);
    drop(a);
    assert!(matches!(
        handle.begin_paid_proposal(&work.owner),
        Err(WorkError::Capacity(_))
    ));
    drive(&mut driver);
    let replacement = handle.begin_paid_proposal(&work.owner).unwrap();
    drop(replacement);
    drop(b);
    finish(work);
    drive(&mut driver);
}

#[test]
fn distinct_union_overflow_is_rejected_before_mint_and_cannot_submit_partial_batch() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
    let work = root(&control);
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_dependency(&network(0)).unwrap();
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(
        work.owner.control_claim_view().unwrap(),
        JointResources::ZERO,
    )
    .unwrap();
    let next = handle.inner.state.lock().unwrap().next_id;
    assert!(matches!(
        grow.push_dependency(&network(1)),
        Err(WorkError::Capacity(_))
    ));
    assert_eq!(handle.inner.state.lock().unwrap().next_id, next);
    assert_eq!(published(&handle, &network(1)), None);
    assert!(grow.end_change().is_err());
    assert!(grow.submit().is_err());
    finish(work);
    drive(&mut driver);
}

#[test]
fn shared_provisional_identity_survives_one_withdrawal_and_publishes_only_on_grant() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let a = root(&control);
    let b = root(&control);
    let fact = network(0);
    let mut first = handle.begin_paid_proposal(&a.owner).unwrap();
    first
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    first.push_dependency(&fact).unwrap();
    let identity = first.current.as_ref().unwrap().selected[0].ordinal;
    first.end_change().unwrap();
    let mut second = handle.begin_paid_proposal(&b.owner).unwrap();
    second
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    second.push_dependency(&fact).unwrap();
    assert_eq!(
        second.current.as_ref().unwrap().selected[0].ordinal,
        identity
    );
    second.end_change().unwrap();
    assert_eq!(published(&handle, &fact), Some(false));
    let first = first.submit().unwrap();
    let mut second = second.submit().unwrap();
    drop(first);
    assert_eq!(published(&handle, &fact), Some(false));
    drive(&mut driver);
    assert_eq!(published(&handle, &fact), Some(true));
    let reply = receive(&mut second);
    drop(reply);
    finish(a);
    finish(b);
    drive(&mut driver);
    assert_eq!(published(&handle, &fact), None);
}

#[test]
fn first_bind_deduplicates_but_new_duplicate_poison_and_c_limits_are_finite() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 2);
    let work = root(&control);
    let fact = network(0);
    let mut duplicate = handle.begin_paid_proposal(&work.owner).unwrap();
    duplicate
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    duplicate.push_dependency(&fact).unwrap();
    assert_eq!(duplicate.push_dependency(&fact), Err(WorkError::Conflict));
    assert!(duplicate.submit().is_err());
    drive(&mut driver);
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_dependency(&fact).unwrap();
    bind.push_dependency(&fact).unwrap();
    assert_eq!(bind.current.as_ref().unwrap().selected.len(), 1);
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut limit = handle.begin_paid_proposal(&work.owner).unwrap();
    for _ in 0..2 {
        limit
            .begin_new(JointLane::Custody, JointResources::ZERO)
            .unwrap();
        limit.end_change().unwrap();
    }
    assert!(matches!(
        limit.begin_new(JointLane::Custody, JointResources::ZERO),
        Err(WorkError::Capacity(_))
    ));
    assert!(limit.submit().is_err());
    finish(work);
    drive(&mut driver);
}

#[test]
fn paid_grow_reauthenticates_and_keeps_full_old_and_new_domains() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let a = network(0);
    let b = network(1);
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_dependency(&a).unwrap();
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let old = work.owner.control_claim_view().unwrap();
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(old.clone(), JointResources::ZERO).unwrap();
    grow.push_dependency(&b).unwrap();
    grow.end_change().unwrap();
    let mut grow = grow.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut grow);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    {
        let state = handle.inner.state.lock().unwrap();
        let node = &state.nodes[&work.owner.scope().id()];
        let claim = state.cancellations.slots[node.cancellation.0]
            .claim
            .unwrap();
        assert!(
            state
                .joint
                .as_ref()
                .unwrap()
                .control_has_domains_for_test(claim, &[a, b])
        );
    }
    let mut stale = handle.begin_paid_proposal(&work.owner).unwrap();
    assert_eq!(
        stale.begin_grow(old, JointResources::ZERO),
        Err(WorkError::Conflict)
    );
    drop(stale);
    finish(work);
    drive(&mut driver);
}

#[test]
fn provisional_identity_does_not_mutate_an_active_proofs_numeric_input() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let a = root(&control);
    let b = root(&control);
    let mut first = handle.begin_paid_proposal(&a.owner).unwrap();
    first
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    first.push_dependency(&network(0)).unwrap();
    first.end_change().unwrap();
    let mut first = first.submit().unwrap();
    assert!(matches!(driver.step(), JointProgress::More));
    let before = {
        let state = handle.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        assert!(ledger.active.is_some());
        (
            ledger.active,
            ledger.profile_layout_revision,
            ledger.profiles.len(),
        )
    };
    let mut second = handle.begin_paid_proposal(&b.owner).unwrap();
    second
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    second.push_dependency(&network(1)).unwrap();
    {
        let state = handle.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        assert_eq!(
            (
                ledger.active,
                ledger.profile_layout_revision,
                ledger.profiles.len()
            ),
            before
        );
    }
    assert_eq!(published(&handle, &network(1)), Some(false));
    drop(second);
    drive(&mut driver);
    let batch = receive(&mut first);
    drop(batch);
    finish(a);
    finish(b);
    drive(&mut driver);
    assert_eq!(published(&handle, &network(1)), None);
}

struct PanicOnceWake {
    panicked: std::sync::atomic::AtomicBool,
    inner: Weak<Inner>,
    before_panic: std::sync::Mutex<Option<crate::WorkloadProgressRevision>>,
}
impl std::task::Wake for PanicOnceWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let inner = self.inner.upgrade().unwrap();
        assert!(
            inner.state.try_lock().is_ok(),
            "wake must run after State unlock"
        );
        if !self
            .panicked
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let marker = {
                let state = inner.state.lock().unwrap();
                inner.capture_progress(&state)
            };
            *self.before_panic.lock().unwrap() = Some(marker);
            panic!("one actual post-State wake panic");
        }
    }
}

#[test]
fn caught_push_wake_panic_cannot_submit_and_temporary_last_exit_is_observed() {
    assert_caught_push_exit(false);
}
#[test]
fn selected_route_output_exits_before_wake_even_when_the_builder_survives_panic() {
    assert_caught_push_exit(true);
}
fn assert_caught_push_exit(selected_route: bool) {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let fact = network(0);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    let marker = control.progress_revision();
    let mut waiter = Box::pin(control.wait_progress(marker));
    let wake = Arc::new(PanicOnceWake {
        panicked: std::sync::atomic::AtomicBool::new(false),
        inner: Arc::downgrade(&handle.inner),
        before_panic: std::sync::Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let selected = url::Url::parse("https://paid-0.example/selected/path?private=secret").unwrap();
    let view = SelectedNetworkOrigin::from_selected_url(&selected).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if selected_route {
            proposal.push_selected_network(view)
        } else {
            proposal.push_dependency(&fact)
        }
    }));
    assert!(outcome.is_err());
    assert!(wake.panicked.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(proposal.current.as_ref().unwrap().selected.len(), 0);
    assert_eq!(proposal.failed, Some(WorkError::Conflict));
    let before_exit = wake
        .before_panic
        .lock()
        .unwrap()
        .expect("actual callback marker");
    assert_ne!(
        control.progress_revision(),
        before_exit,
        "temporary data reference exits before the per-call reclaim generation"
    );
    drive(&mut driver);
    assert_eq!(
        published(&handle, &fact),
        None,
        "raw callback result exited before per-call wake"
    );
    let before = control.progress_revision();
    assert_eq!(proposal.end_change(), Err(WorkError::Conflict));
    assert_eq!(
        before,
        control.progress_revision(),
        "reading a sealed error must not publish false progress"
    );
    assert!(proposal.submit().is_err());
    drop(waiter);
    finish(work);
    drive(&mut driver);
}

#[test]
fn production_builder_has_no_unfunded_legacy_fallback() {
    let (_fixed, fixed_handle, _driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let geometry = fixed_handle
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .config
        .config
        .clone();
    let required = RequiredJointProgress {
        global: JointResources::ZERO,
        classes: [JointResources::ZERO; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    let control = WorkloadControl::try_new_counted(crate::WorkloadConfig::default())
        .unwrap()
        .owner;
    let (handle, _driver) = control
        .configure_joint_capacity(geometry.validate(&required).unwrap())
        .unwrap();
    control.mark_ready().unwrap();
    let work = root(&control);
    assert!(matches!(
        handle.begin_paid_proposal(&work.owner),
        Err(WorkError::NotReady)
    ));
    finish(work);
}

#[test]
fn paid_body_store_wake_panic_keeps_original_custody_until_actual_reap() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let fact = network(0);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    proposal.push_dependency(&fact).unwrap();
    proposal.end_change().unwrap();
    let wake = Arc::new(PanicOnceWake {
        panicked: std::sync::atomic::AtomicBool::new(false),
        inner: Arc::downgrade(&handle.inner),
        before_panic: std::sync::Mutex::new(None),
    });
    let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| proposal.submit()));
    assert!(outcome.is_err());
    {
        let state = handle.inner.state.lock().unwrap();
        assert!(state.joint.as_ref().unwrap().requests.is_empty());
        assert_eq!(state.nodes[&work.owner.scope().id()].resource_waiters, 0);
    }
    assert_eq!(
        published(&handle, &fact),
        Some(false),
        "original anchor retains the unpaid-to-publish proposal graph"
    );
    drive(&mut driver);
    assert_eq!(published(&handle, &fact), None);
    drop(waiter);
    finish(work);
    drive(&mut driver);
}

#[test]
fn explicit_proof_budget_cannot_raise_the_composed_total_work_ceiling() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    proposal.end_change().unwrap();
    let request = proposal
        .submit_finite(std::num::NonZeroU64::new(u64::MAX).unwrap())
        .unwrap();
    {
        let state = handle.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        let RequestBudget::Finite { remaining } = ledger.requests.values().next().unwrap().budget
        else {
            panic!("a paid proposal cannot anonymously wait")
        };
        assert_eq!(remaining, ledger.config.config.max_total_proof_work);
    }
    drop(request);
    finish(work);
    drive(&mut driver);
}

#[test]
fn paid_selected_origin_deduplicates_before_owned_output_and_preserves_full_union() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let selected =
        url::Url::parse("https://user:secret@PAID-0.example:443/current?secret=private").unwrap();
    let alias = url::Url::parse("https://paid-0.example/other").unwrap();
    let first = SelectedNetworkOrigin::from_selected_url(&selected).unwrap();
    let same = SelectedNetworkOrigin::from_selected_url(&alias).unwrap();
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_selected_network(first).unwrap();
    let before = handle.inner.state.lock().unwrap().next_id;
    bind.push_selected_network(same).unwrap();
    assert_eq!(handle.inner.state.lock().unwrap().next_id, before);
    assert_eq!(bind.current.as_ref().unwrap().selected.len(), 1);
    assert_eq!(bind.current.as_ref().unwrap().selected[0].fact, network(0));
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(
        work.owner.control_claim_view().unwrap(),
        JointResources::ZERO,
    )
    .unwrap();
    let b = url::Url::parse("https://paid-1.example/object-store").unwrap();
    grow.push_selected_network(SelectedNetworkOrigin::from_selected_url(&b).unwrap())
        .unwrap();
    grow.end_change().unwrap();
    let mut grow = grow.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut grow);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut overflow = handle.begin_paid_proposal(&work.owner).unwrap();
    overflow
        .begin_grow(
            work.owner.control_claim_view().unwrap(),
            JointResources::ZERO,
        )
        .unwrap();
    let next = handle.inner.state.lock().unwrap().next_id;
    let c = url::Url::parse("https://new.example/not-yet-owned").unwrap();
    assert!(matches!(
        overflow.push_selected_network(SelectedNetworkOrigin::from_selected_url(&c).unwrap()),
        Err(WorkError::Capacity(_))
    ));
    assert_eq!(handle.inner.state.lock().unwrap().next_id, next);
    assert!(overflow.submit().is_err());
    finish(work);
    drive(&mut driver);
}
