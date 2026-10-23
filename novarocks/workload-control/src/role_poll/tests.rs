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

//! Actual fixed frame reuse, output custody, and silent stale-waker retirement.

use super::*;
use crate::{
    WorkClass, WorkRequest, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver},
    scope::fixed_control_storage_tests::prototype_profile_with_roles,
};
use std::sync::Mutex;

fn budget() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn drain(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original fixed reaper did not reach idle");
}
struct Input {
    ready: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<Waker>>>,
}
fn new_input(ready: bool) -> Input {
    Input {
        ready: Arc::new(AtomicBool::new(ready)),
        wake: Arc::new(Mutex::new(None)),
    }
}
async fn run(owner: WorkOwner, input: Input) -> u8 {
    std::future::poll_fn(|cx| {
        if input.ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            *input.wake.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    })
    .await;
    owner.complete();
    7
}
fn new_root(control: &WorkloadControl) -> crate::scope::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}

#[tokio::test]
async fn original_frame_and_anchor_reuse_wait_for_output_and_silent_stale_waker_exit() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let observer = root.owner.retirement_observer().unwrap();
    let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
    let frame = std::ptr::from_ref(bank.positions[slot.0].frame.as_ref().get_ref()) as usize;
    let anchor =
        Arc::as_ptr(&control.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].wake)
            as usize;
    let input = new_input(false);
    let ready = Arc::clone(&input.ready);
    let saved = Arc::clone(&input.wake);
    root.business.release();
    bank.mount_owned(root.owner, input)
        .unwrap_or_else(|_| panic!("original mount must fit"));
    assert!(bank.progress_turn(budget()).is_none());
    let stale = saved.lock().unwrap().take().unwrap();
    ready.store(true, Ordering::Release);
    stale.wake_by_ref();
    let completion = bank.progress_turn(NonZeroUsize::new(2).unwrap()).unwrap();
    let PollCompletion::Ready(output) = completion else {
        panic!("actual future must complete");
    };
    assert_eq!(*output.value(), 7);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .values()
            .any(|node| node.completed)
    );
    drop(output);
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
    drain(&mut driver);
    let marker = control.progress_revision();
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(
        control.progress_revision(),
        marker,
        "negative uniqueness checks cannot fake progress"
    );
    drop(stale);
    assert_eq!(
        control.progress_revision(),
        marker,
        "std Arc Waker drop is silent"
    );
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert_eq!(
        std::ptr::from_ref(bank.positions[slot.0].frame.as_ref().get_ref()) as usize,
        frame
    );
    assert_eq!(
        Arc::as_ptr(&control.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].wake)
            as usize,
        anchor
    );
    let next = new_root(&control);
    let next_slot =
        control.inner.state.lock().unwrap().nodes[&next.owner.scope().id()].cancellation;
    assert_eq!(next_slot, slot);
    next.business.release();
    bank.mount_owned(next.owner, new_input(true))
        .unwrap_or_else(|_| panic!("same original frame must be reusable"));
    drop(bank.progress_turn(NonZeroUsize::new(2).unwrap()).unwrap());
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn bounded_round_robin_polls_a_ready_peer_while_the_first_future_is_pending() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let first = new_root(&control);
    let second = new_root(&control);
    let blocked = new_input(false);
    let ready = Arc::clone(&blocked.ready);
    let wake = Arc::clone(&blocked.wake);
    first.business.release();
    second.business.release();
    bank.mount_owned(first.owner, blocked)
        .unwrap_or_else(|_| panic!("first mount"));
    bank.mount_owned(second.owner, new_input(true))
        .unwrap_or_else(|_| panic!("second mount"));
    assert!(bank.progress_turn(budget()).is_none());
    let PollCompletion::Ready(peer) = bank.progress_turn(budget()).unwrap() else {
        panic!("ready peer must not wait for first");
    };
    assert_eq!(*peer.value(), 7);
    drop(peer);
    ready.store(true, Ordering::Release);
    wake.lock().unwrap().take().unwrap().wake();
    drop(bank.progress_turn(budget()).unwrap());
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

struct PanicOnce(AtomicBool);
impl Wake for PanicOnce {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional post-mount progress wake panic");
        }
    }
}
#[tokio::test]
async fn post_state_wake_unwind_keeps_the_actual_owner_and_input_in_the_bank() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    root.business.release();
    let work = root.owner.scope().id();
    let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(
            || bank.mount_owned(root.owner, new_input(true))
        ))
        .is_err()
    );
    drop(progress);
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Active
    );
    let PollCompletion::Ready(output) = bank.progress_turn(NonZeroUsize::new(2).unwrap()).unwrap()
    else {
        panic!("same retained input must run");
    };
    assert_eq!(*output.value(), 7);
    drop(output);
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn foreign_owner_rejection_returns_the_same_unexecuted_owner_and_input() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let foreign = WorkloadControl::try_new_counted(crate::WorkloadConfig::default())
        .unwrap()
        .owner;
    foreign.mark_ready().unwrap();
    let root = new_root(&foreign);
    let id = root.owner.scope().id();
    let original = new_input(false);
    let actual = Arc::clone(&original.ready);
    let rejected = match bank.mount_owned(root.owner, original) {
        Err(rejected) => rejected,
        Ok(()) => panic!("foreign owner must reject"),
    };
    assert_eq!(rejected.error, WorkError::Conflict);
    assert_eq!(rejected.owner.scope().id(), id);
    assert!(Arc::ptr_eq(&rejected.input.ready, &actual));
    root.business.release();
    rejected.owner.complete();
    drop(rejected.input);
    assert!(bank.progress_turn(budget()).is_none());
    assert!(foreign.inner.state.lock().unwrap().nodes.is_empty());
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

struct ExitCheck {
    inner: Arc<Inner>,
    actual_drop: Arc<AtomicBool>,
}
impl Drop for ExitCheck {
    fn drop(&mut self) {
        self.actual_drop
            .store(self.inner.state.try_lock().is_ok(), Ordering::Release);
    }
}
struct FailureInput {
    panic: bool,
    exit: ExitCheck,
}
async fn run_failure(owner: WorkOwner, input: FailureInput) -> u8 {
    let _exit = input.exit;
    owner.complete();
    if input.panic {
        panic!("intentional isolated role poll panic");
    }
    9
}
#[tokio::test]
async fn poll_panic_clears_the_real_frame_outside_state_and_preserves_peer_progress() {
    let roles = [PollRoleGeometry::for_factory(&run_failure).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run_failure).unwrap();
    control.mark_ready().unwrap();
    let first = new_root(&control);
    let peer = new_root(&control);
    let work = first.owner.scope().id();
    let first_exit = Arc::new(AtomicBool::new(false));
    let peer_exit = Arc::new(AtomicBool::new(false));
    first.business.release();
    peer.business.release();
    bank.mount_owned(
        first.owner,
        FailureInput {
            panic: true,
            exit: ExitCheck {
                inner: Arc::clone(&control.inner),
                actual_drop: Arc::clone(&first_exit),
            },
        },
    )
    .unwrap_or_else(|_| panic!("first mount"));
    bank.mount_owned(
        peer.owner,
        FailureInput {
            panic: false,
            exit: ExitCheck {
                inner: Arc::clone(&control.inner),
                actual_drop: Arc::clone(&peer_exit),
            },
        },
    )
    .unwrap_or_else(|_| panic!("peer mount"));
    let PollCompletion::Panicked(failed) = bank.progress_turn(budget()).unwrap() else {
        panic!("panic must be an isolated completion");
    };
    assert!(
        first_exit.load(Ordering::Acquire),
        "actual frame fields exit outside State"
    );
    assert!(control.inner.state.lock().unwrap().nodes[&work].completed);
    let PollCompletion::Ready(healthy) = bank.progress_turn(budget()).unwrap() else {
        panic!("healthy peer still progresses");
    };
    assert_eq!(*healthy.value(), 9);
    assert!(peer_exit.load(Ordering::Acquire));
    drop((failed, healthy));
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn factory_panic_does_not_destroy_an_owner_already_received_by_its_stable_holder() {
    let held = Arc::new(Mutex::new(None::<WorkOwner>));
    let receiver = Arc::clone(&held);
    let make = move |owner: WorkOwner, (): ()| -> std::future::Ready<()> {
        *receiver.lock().unwrap() = Some(owner);
        panic!("intentional role factory panic after original owner handoff");
    };
    let roles = [PollRoleGeometry::for_factory(&make).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, make).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    root.business.release();
    bank.mount_owned(root.owner, ())
        .unwrap_or_else(|_| panic!("original mount"));
    let PollCompletion::Panicked(failed) = bank.progress_turn(budget()).unwrap() else {
        panic!("factory panic must be isolated");
    };
    assert_eq!(held.lock().unwrap().as_ref().unwrap().scope().id(), work);
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Active
    );
    let owner = held.lock().unwrap().take().unwrap();
    owner.complete();
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&work),
        "original footer is still actual custody"
    );
    drop(failed);
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn original_driver_reclaims_output_after_bank_exit_including_silent_stale_waker() {
    for retain_stale in [false, true] {
        let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
        let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
        control.mark_ready().unwrap();
        let root = new_root(&control);
        let observer = root.owner.retirement_observer().unwrap();
        let input = new_input(false);
        let ready = Arc::clone(&input.ready);
        let saved = Arc::clone(&input.wake);
        root.business.release();
        bank.mount_owned(root.owner, input)
            .unwrap_or_else(|_| panic!("original mount"));
        assert!(bank.progress_turn(NonZeroUsize::new(2).unwrap()).is_none());
        let stale = saved.lock().unwrap().take().unwrap();
        ready.store(true, Ordering::Release);
        stale.wake_by_ref();
        let PollCompletion::Ready(output) =
            bank.progress_turn(NonZeroUsize::new(2).unwrap()).unwrap()
        else {
            panic!("original completion");
        };
        let stale = retain_stale.then_some(stale);
        drop(bank);
        assert_eq!(control.mark_ready(), Err(WorkError::Closed));
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::Closed)
        ));
        drain(&mut driver);
        assert_eq!(
            observer.is_reclaimed(),
            Ok(false),
            "the real output still holds its footer"
        );
        assert_eq!(*output.value(), 7);
        drop(output);
        drain(&mut driver);
        if retain_stale {
            assert_eq!(observer.is_reclaimed(), Ok(false));
            let marker = control.progress_revision();
            driver.recheck_retired_backing();
            drain(&mut driver);
            assert_eq!(
                control.progress_revision(),
                marker,
                "negative timer pass is silent"
            );
            drop(stale);
            assert_eq!(
                control.progress_revision(),
                marker,
                "actual last Waker drop is silent"
            );
            driver.recheck_retired_backing();
            drain(&mut driver);
        }
        assert_eq!(observer.is_reclaimed(), Ok(true));
        assert!(control.inner.state.lock().unwrap().is_drained());
    }
}

#[tokio::test]
async fn startup_bank_exit_revokes_registration_and_allows_only_serial_reinstallation() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    assert!(matches!(
        RolePollBank::install(&mut control, 0, run),
        Err(WorkError::Conflict)
    ));
    let old_notify = Arc::as_ptr(&bank.changed);
    drop(bank);
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .cancellations
            .slots
            .iter()
            .all(|slot| slot.polls[0].wake.changed.as_ptr() != old_notify),
        "all original weak anchors leave before replacement allocation"
    );
    assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    drop(bank);
    assert_eq!(control.mark_ready(), Err(WorkError::Closed));
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn dropping_a_mounted_bank_preserves_orphaned_work_until_the_original_owner_recovers_it() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let observer = root.owner.retirement_observer().unwrap();
    root.business.release();
    bank.mount_owned(root.owner, new_input(true))
        .unwrap_or_else(|_| panic!("original mount"));
    // Leave the actual owner in the unpolled input cell, then destroy the bank.
    drop(bank);
    drain(&mut driver);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.nodes[&work].owner, crate::OwnerState::Orphaned);
        assert!(!state.nodes[&work].completed);
        assert_eq!(state.cancellations.retired_polls, 0);
    }
    assert_eq!(observer.is_reclaimed(), Ok(false));
    control.adopt(work).unwrap().complete();
    drain(&mut driver);
    assert_eq!(
        observer.is_reclaimed(),
        Ok(false),
        "ordinary completion retains the original Cancel intent"
    );
    let cancel = control.next_control().unwrap();
    assert_eq!(cancel.scope().id(), work);
    assert!(cancel.intents().contains(crate::ControlIntent::Cancel));
    cancel.acknowledge();
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn startup_teardown_refuses_readiness_while_actual_factory_destructor_is_paused() {
    struct PausedDrop {
        entered: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
    }
    impl Drop for PausedDrop {
        fn drop(&mut self) {
            self.entered.wait();
            self.resume.wait();
        }
    }
    let entered = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let held = PausedDrop {
        entered: Arc::clone(&entered),
        resume: Arc::clone(&resume),
    };
    let make = move |owner: WorkOwner, input: Input| {
        let _actual_factory_custody = &held;
        run(owner, input)
    };
    let roles = [PollRoleGeometry::for_factory(&make).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, make).unwrap();
    std::thread::scope(|threads| {
        let dropping = threads.spawn(move || drop(bank));
        entered.wait();
        assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
        {
            let state = control.inner.state.lock().unwrap();
            assert!(!state.ready);
            assert!(
                !state.closed,
                "startup teardown does not fake a ready-service loss"
            );
            assert!(
                state
                    .cancellations
                    .slots
                    .iter()
                    .all(|slot| slot.polls[0].registration == RegistrationPhase::Stopping)
            );
        }
        resume.wait();
        dropping.join().unwrap();
    });
    assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .cancellations
            .slots
            .iter()
            .all(|slot| slot.polls[0].registration == RegistrationPhase::Absent)
    );
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}
