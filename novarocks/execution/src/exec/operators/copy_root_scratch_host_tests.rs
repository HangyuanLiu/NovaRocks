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

//! Root scratch probes borrow the ACTUAL production opaque/tracker adapter.
//! These assertions cover root plan vectors only, not all recursive scratch,
//! Arrow output, RSS, or formal query memory settlement.
use super::{expression_allocation_host, MemTracker};
use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
};
use novarocks_functions::selected_copy::{
    CopyError, TakeRootScratchFacts, preflight_take, preflight_take_root_scratch_in,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct Control {
    tracker: Arc<MemTracker>,
    trace: Mutex<Vec<(u32, i64)>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl Control {
    fn new(tracker: Arc<MemTracker>, refusal: Option<(usize, KernelFailure)>) -> Self {
        Self {
            tracker,
            trace: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn trace(&self) -> Vec<(u32, i64)> {
        self.trace.lock().unwrap().clone()
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(ordinal <= *stop, "callback after first refusal");
        }
        trace.push((units, self.tracker.current()));
        match &self.refusal {
            Some((stop, cause)) if ordinal == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("take preflight does not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("root scratch invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("root scratch internal")),
        KernelFailure::Operational(KernelDiagnostic::new("root scratch operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn source() -> ArrayRef {
    Arc::new(Int32Array::from(vec![Some(7), None, Some(9)]))
}
fn indices() -> Vec<Option<u64>> {
    (0..600)
        .map(|row| {
            if row % 5 == 0 {
                None
            } else {
                Some((row % 3) as u64)
            }
        })
        .collect()
}
fn new_tracker() -> Arc<MemTracker> {
    let parent = MemTracker::new_root("by-copy-root-scratch-actual-parent");
    MemTracker::new_child("by-copy-root-scratch-actual-child", &parent)
}

#[test]
fn by_copy_root_scratch_host_actual_grant_layout_live_peak_and_release() {
    let array = source();
    let indices = indices();
    let facts = TakeRootScratchFacts::try_new(indices.len()).unwrap();
    assert_eq!(
        facts.ranges(),
        std::alloc::Layout::array::<std::ops::Range<usize>>(indices.len()).unwrap()
    );
    assert_eq!(facts.bytes(), facts.ranges().size() + facts.blocks().size());
    let tracker = new_tracker();
    let host = expression_allocation_host(Arc::clone(&tracker));
    let control = Control::new(Arc::clone(&tracker), None);
    assert!(preflight_take_root_scratch_in(array.as_ref(), &indices, host, &control).is_ok());
    let trace = control.trace();
    assert_eq!(trace[0], (0, 0));
    assert_eq!(trace[1], (0, 0));
    assert_eq!(
        trace[2],
        (0, i64::try_from(facts.bytes()).unwrap()),
        "actual grant precedes root vectors"
    );
    assert!(
        trace
            .iter()
            .any(|(units, bytes)| *units == MAX_UNOBSERVED_KERNEL_WORK
                && *bytes == i64::try_from(facts.bytes()).unwrap())
    );
    assert_eq!(
        trace.last().unwrap().1,
        0,
        "success tail follows original plan and scope Drop"
    );
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.peak(), i64::try_from(facts.bytes()).unwrap());
    // Legacy remains extent-only, with exactly the same one-pass owned work.
    let mut legacy = Vec::new();
    assert!(
        preflight_take(array.as_ref(), &indices, |boundary| {
            legacy.push(boundary);
            Ok(())
        })
        .is_ok()
    );
    assert_eq!(
        trace
            .iter()
            .map(|(units, _)| u64::from(*units))
            .sum::<u64>(),
        legacy.iter().filter(|boundary| !**boundary).count() as u64
    );
}

#[test]
fn by_copy_root_scratch_host_seven_causes_every_real_checkpoint_rollback_no_footer() {
    let array = source();
    let indices = indices();
    let tracker = new_tracker();
    let control = Control::new(Arc::clone(&tracker), None);
    assert!(
        preflight_take_root_scratch_in(
            array.as_ref(),
            &indices,
            expression_allocation_host(Arc::clone(&tracker)),
            &control
        )
        .is_ok()
    );
    let trace = control.trace();
    assert!(trace.len() > 3);
    for cause in causes() {
        for stop in 0..trace.len() {
            let tracker = new_tracker();
            let control = Control::new(Arc::clone(&tracker), Some((stop, cause.clone())));
            match preflight_take_root_scratch_in(
                array.as_ref(),
                &indices,
                expression_allocation_host(Arc::clone(&tracker)),
                &control,
            ) {
                Err(CopyError::Control(actual)) => assert_eq!(actual, cause),
                other => panic!("expected exact nominal cause {cause:?}, got {other:?}"),
            }
            assert_eq!(control.trace(), trace[..=stop]);
            assert_eq!(
                tracker.current(),
                0,
                "failed original root vectors and scope dropped"
            );
        }
    }
}

#[test]
fn by_copy_root_scratch_host_actual_capacity_refusal_is_not_layout_permission() {
    let array = source();
    let indices = indices();
    let facts = TakeRootScratchFacts::try_new(indices.len()).unwrap();
    let tracker = new_tracker();
    tracker
        .install_limit_once(i64::try_from(facts.bytes() - 1).unwrap())
        .unwrap();
    let control = Control::new(Arc::clone(&tracker), None);
    assert!(matches!(
        preflight_take_root_scratch_in(
            array.as_ref(),
            &indices,
            expression_allocation_host(Arc::clone(&tracker)),
            &control
        ),
        Err(CopyError::Control(KernelFailure::ResourceExhausted))
    ));
    assert_eq!(
        control.trace(),
        vec![(0, 0), (0, 0)],
        "host denial precedes original plan construction"
    );
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
    assert_eq!(
        tracker.peak(),
        i64::try_from(facts.bytes()).unwrap(),
        "actual tracker author attempted and rolled back the exact request"
    );
}

#[test]
fn by_copy_root_scratch_host_original_extent_data_text_and_no_failure_footer() {
    let array = source();
    let indices = [Some(999)];
    let expected = preflight_take(array.as_ref(), &indices, |_| Ok(()))
        .unwrap_err()
        .to_string();
    assert_eq!(
        expected,
        "constant broadcast selected range is outside its source"
    );
    let tracker = new_tracker();
    let control = Control::new(Arc::clone(&tracker), None);
    let error = preflight_take_root_scratch_in(
        array.as_ref(),
        &indices,
        expression_allocation_host(Arc::clone(&tracker)),
        &control,
    )
    .unwrap_err();
    assert!(matches!(&error, CopyError::Invalid(_)));
    assert_eq!(error.to_string(), expected);
    let trace = control.trace();
    assert_eq!(trace.len(), 3);
    assert_eq!(tracker.current(), 0);
    let tracker = new_tracker();
    let control = Control::new(
        Arc::clone(&tracker),
        Some((trace.len(), KernelFailure::Cancelled)),
    );
    let error = preflight_take_root_scratch_in(
        array.as_ref(),
        &indices,
        expression_allocation_host(Arc::clone(&tracker)),
        &control,
    )
    .unwrap_err();
    assert_eq!(error.to_string(), expected);
    assert_eq!(control.trace(), trace);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn by_copy_root_scratch_host_actual_empty_null_and_utf8_offsets_without_stock_estimate() {
    let arrays: [ArrayRef; 3] = [
        Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
        Arc::new(Int32Array::from(vec![None, None])),
        Arc::new(StringArray::from(vec![Some("prefix"), Some("é\0long"), None]).slice(1, 2)),
    ];
    let plans: [Vec<Option<u64>>; 3] = [
        vec![],
        vec![None, None],
        vec![Some(0), Some(1), None, Some(0)],
    ];
    for (array, indices) in arrays.iter().zip(plans.iter()) {
        let facts = TakeRootScratchFacts::try_new(indices.len()).unwrap();
        let tracker = new_tracker();
        let control = Control::new(Arc::clone(&tracker), None);
        assert!(
            preflight_take_root_scratch_in(
                array.as_ref(),
                indices,
                expression_allocation_host(Arc::clone(&tracker)),
                &control
            )
            .is_ok()
        );
        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.peak(), i64::try_from(facts.bytes()).unwrap());
        assert!(preflight_take(array.as_ref(), indices, |_| Ok(())).is_ok());
    }
}

#[test]
fn by_copy_root_scratch_host_actual_unwind_releases_without_retyping() {
    struct PanicControl {
        inner: Control,
        bytes: i64,
    }
    impl KernelEvaluationControl for PanicControl {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            if units != 0 {
                assert_eq!(self.inner.tracker.current(), self.bytes);
                panic!("original host checkpoint panic");
            }
            self.inner.checkpoint(units)
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("take preflight does not wait")
        }
    }
    let array = source();
    let indices = indices();
    let facts = TakeRootScratchFacts::try_new(indices.len()).unwrap();
    let tracker = new_tracker();
    let host = expression_allocation_host(Arc::clone(&tracker));
    let control = PanicControl {
        inner: Control::new(Arc::clone(&tracker), None),
        bytes: i64::try_from(facts.bytes()).unwrap(),
    };
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        preflight_take_root_scratch_in(array.as_ref(), &indices, host, &control)
    }))
    .err()
    .expect("original panic propagates through the granted root scope");
    let text = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str));
    assert_eq!(text, Some("original host checkpoint panic"));
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
    assert_eq!(control.inner.trace().len(), 3);
}
