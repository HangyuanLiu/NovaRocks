// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Lowest-owner regressions: actual Product F/opaque cause and original Work.

use super::*;
use futures::task::{AtomicWaker, noop_waker};
use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig, WorkloadControl};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Cause(Arc<AtomicUsize>);
impl Drop for Cause {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct Gate {
    open: AtomicBool,
    wake: AtomicWaker,
}
impl Gate {
    fn open(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.wake.wake();
    }
}
struct Product {
    original: std::sync::Weak<Mutex<AutomaticPassSlot>>,
    gate: Arc<Gate>,
    drops: Arc<AtomicUsize>,
    cause_drops: Arc<AtomicUsize>,
    panic_poll: bool,
    panic_drop: bool,
}
impl Future for Product {
    type Output = AutomaticProductResult;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let original = self.original.upgrade().unwrap();
        assert!(
            original.try_lock().is_ok(),
            "foreign poll outside the original metadata mutex"
        );
        if self.panic_poll {
            panic_any(Cause(Arc::clone(&self.cause_drops)));
        }
        self.gate.wake.register(cx.waker());
        if !self.gate.open.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        Poll::Ready(AutomaticProductResult::Managed(Err(
            TerminalError::known_uncommitted("original domain failure"),
        )))
    }
}
impl Drop for Product {
    fn drop(&mut self) {
        if let Some(original) = self.original.upgrade() {
            assert!(
                original.try_lock().is_ok(),
                "foreign F Drop outside the original metadata mutex"
            );
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_drop {
            panic_any(Cause(Arc::clone(&self.cause_drops)));
        }
    }
}
struct Fixture {
    control: WorkloadControl,
    _runtime: tokio::runtime::Runtime,
    original: Arc<Mutex<AutomaticPassSlot>>,
    gate: Arc<Gate>,
    drops: Arc<AtomicUsize>,
    cause_drops: Arc<AtomicUsize>,
}
impl Fixture {
    fn new(panic_poll: bool, panic_drop: bool) -> Self {
        // This unit fixture tests the original Work/window and stable record;
        // it neither configures nor claims the production Native profile.
        let parts = WorkloadControl::try_new_counted(WorkloadConfig::default()).unwrap();
        let mut control = parts.owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let work = admit_automatic_query(&control.root_admission(), runtime.handle()).unwrap();
        let original = Arc::new(Mutex::new(AutomaticPassSlot::default()));
        let gate = Arc::new(Gate::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let future = Box::pin(Product {
            original: Arc::downgrade(&original),
            gate: Arc::clone(&gate),
            drops: Arc::clone(&drops),
            cause_drops: Arc::clone(&cause_drops),
            panic_poll,
            panic_drop,
        });
        {
            let mut loan = AutomaticPassLoan::take(&original);
            loan.body_mut().work = Some(work);
            // Only this closed lower-level fixture bypasses management setup.
            // The separate real ManagementEntrance test checks terminal release.
            loan.body_mut().action = Some(AutomaticAction {
                prepared: None,
                future: Some(future),
                result: None,
                dispatch_attempted: true,
                terminal_attempted: false,
                disposition: None,
                terminal: None,
                phase: ActionPhase::Running,
                cause: None,
            });
        }
        Self {
            control,
            _runtime: runtime,
            original,
            gate,
            drops,
            cause_drops,
        }
    }
    fn assert_work_held(&self) {
        let snapshot = self.control.observation().snapshot();
        assert_eq!(snapshot.admitted_queries, 1);
        assert_eq!(snapshot.root_responsibilities, 1);
        assert!(!snapshot.scopes[0].own_completed);
        assert!(
            AutomaticPassLoan::take(&self.original)
                .body()
                .work
                .is_some()
        );
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut loan = AutomaticPassLoan::take(&self.original);
        let action = loan.body_mut().action.as_mut().unwrap();
        match poll_product(action, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(cause)) => {
                poison(action, cause);
                publish_health_after_restore(loan)
            }
        }
    }
}

#[test]
fn dropping_a_borrowed_wait_keeps_the_same_product_and_original_work() {
    let fixture = Fixture::new(false, false);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| fixture.poll(cx)));
    assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    drop(borrowed);
    fixture.assert_work_held();
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    fixture.gate.open();
    assert!(fixture.poll(&mut cx).is_ready());
    {
        let mut loan = AutomaticPassLoan::take(&fixture.original);
        let action = loan.body_mut().action.as_mut().unwrap();
        assert!(
            matches!(&action.result, Some(AutomaticProductResult::Managed(Err(error))) if error.message == "original domain failure")
        );
        assert!(drop_product(action).is_ok());
        assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
        // Actual F exited. The test now explicitly disposes its saved business
        // result; production does this only after the real management terminal.
        drop(loan.body_mut().action.take());
    }
    finish_automatic_pass(&fixture.original);
    assert_eq!(fixture.control.observation().snapshot().admitted_queries, 0);
    assert_eq!(
        fixture
            .control
            .observation()
            .snapshot()
            .root_responsibilities,
        0
    );
}

#[test]
fn product_poll_panic_restores_the_row_before_notifying_the_original_join() {
    let fixture = Fixture::new(true, false);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let marker = catch_unwind(AssertUnwindSafe(|| fixture.poll(&mut cx))).unwrap_err();
    assert!(marker.is::<OriginalAutomaticActionFailed>());
    fixture.assert_work_held();
    let mut loan = AutomaticPassLoan::take(&fixture.original);
    let action = loan.body_mut().action.as_mut().unwrap();
    assert_eq!(action.phase, ActionPhase::Poisoned);
    assert!(action.cause.as_ref().unwrap().is::<Cause>());
    assert!(action.future.is_some());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.cause_drops.load(Ordering::SeqCst), 0);
    // Explicit test-owner disposition, never a source-exit receipt.
    let retired = loan.body_mut().action.take();
    drop(loan);
    drop(retired);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.cause_drops.load(Ordering::SeqCst), 1);
    finish_automatic_pass(&fixture.original);
}

#[test]
fn completed_product_drop_panic_keeps_original_output_cause_and_work() {
    let fixture = Fixture::new(false, true);
    fixture.gate.open();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(fixture.poll(&mut cx).is_ready());
    let marker = catch_unwind(AssertUnwindSafe(|| {
        let mut loan = AutomaticPassLoan::take(&fixture.original);
        let action = loan.body_mut().action.as_mut().unwrap();
        assert!(action.result.is_some());
        if let Err(cause) = drop_product(action) {
            poison(action, cause);
            publish_health_after_restore(loan);
        }
    }))
    .unwrap_err();
    assert!(marker.is::<OriginalAutomaticActionFailed>());
    fixture.assert_work_held();
    let mut loan = AutomaticPassLoan::take(&fixture.original);
    let action = loan.body_mut().action.as_mut().unwrap();
    assert_eq!(action.phase, ActionPhase::Poisoned);
    assert!(action.future.is_none());
    assert!(action.result.is_some());
    assert!(action.cause.as_ref().unwrap().is::<Cause>());
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.cause_drops.load(Ordering::SeqCst), 0);
    let retired = loan.body_mut().action.take();
    drop(loan);
    drop(retired);
    assert_eq!(fixture.cause_drops.load(Ordering::SeqCst), 1);
    finish_automatic_pass(&fixture.original);
}
