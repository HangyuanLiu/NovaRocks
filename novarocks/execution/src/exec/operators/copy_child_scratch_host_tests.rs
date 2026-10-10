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

//! Child-reference probes forward every actual block and opaque reservation
//! to the production expression host. Ledger storage is test diagnostics only.
use super::{expression_allocation_host, MemTracker};
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    RunArray, StructArray, UnionArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, UnionFields, Int16Type};
use novarocks_functions::{
    AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    MAX_UNOBSERVED_KERNEL_WORK,
};
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_functions::selected_copy::{
    CopyError, TakeRootScratchFacts, preflight_take, preflight_take_child_tables_in,
};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Attempt(Layout),
    Granted(Layout, i64),
    Released(Layout, i64),
    Opaque(usize, i64),
    OpaqueReleased(usize, i64),
}
struct Host {
    inner: Arc<dyn AggregateStateAllocator>,
    tracker: Arc<MemTracker>,
    events: Mutex<Vec<Event>>,
    allocations: Mutex<usize>,
    refusal: Option<(usize, KernelFailure)>,
}
impl Host {
    fn new(tracker: Arc<MemTracker>, refusal: Option<(usize, KernelFailure)>) -> Arc<Self> {
        Arc::new(Self {
            inner: expression_allocation_host(Arc::clone(&tracker)),
            tracker,
            events: Mutex::new(Vec::new()),
            allocations: Mutex::new(0),
            refusal,
        })
    }
    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        let at = {
            let mut at = self.allocations.lock().unwrap();
            let old = *at;
            *at += 1;
            old
        };
        self.events.lock().unwrap().push(Event::Attempt(layout));
        if let Some((stop, cause)) = &self.refusal {
            assert!(at <= *stop, "allocation after first refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        let block = self.inner.allocate(layout)?;
        self.events
            .lock()
            .unwrap()
            .push(Event::Granted(layout, self.tracker.current()));
        Ok(block)
    }
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the exact block/Layout returned by the unchanged real host.
        unsafe { self.inner.release(block, layout) };
        self.events
            .lock()
            .unwrap()
            .push(Event::Released(layout, self.tracker.current()));
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.inner
            .opaque_allocation_host()
            .expect("real host capability")
            .reserve_opaque(bytes)?;
        self.events
            .lock()
            .unwrap()
            .push(Event::Opaque(bytes, self.tracker.current()));
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        self.inner
            .opaque_allocation_host()
            .expect("real host capability")
            .release_opaque(bytes);
        self.events
            .lock()
            .unwrap()
            .push(Event::OpaqueReleased(bytes, self.tracker.current()));
    }
}
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
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push((units, self.tracker.current()));
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("copy preflight does not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("child table invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("child table internal")),
        KernelFailure::Operational(KernelDiagnostic::new("child table operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn tracker() -> Arc<MemTracker> {
    let root = MemTracker::new_root("child-table-test-parent");
    MemTracker::new_child("child-table-test-child", &root)
}
fn nested() -> ArrayRef {
    let mut array: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(9)]));
    for depth in 0..5 {
        let side: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        array = Arc::new(
            StructArray::try_new(
                vec![
                    Arc::new(Field::new(
                        format!("nested-{depth}"),
                        array.data_type().clone(),
                        true,
                    )),
                    Arc::new(Field::new("side", DataType::Int32, false)),
                ]
                .into(),
                vec![array, side],
                None,
            )
            .unwrap(),
        );
    }
    array
}
fn indices() -> Vec<Option<u64>> {
    (0..600)
        .map(|i| {
            if i % 5 == 0 {
                None
            } else {
                Some((i % 3) as u64)
            }
        })
        .collect()
}
fn evaluate(
    array: &dyn Array,
    indices: &[Option<u64>],
    host: &Arc<Host>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), CopyError> {
    preflight_take_child_tables_in(array, indices, host.clone(), control)
}
fn assert_released(host: &Host) {
    assert_eq!(host.tracker.current(), 0);
    assert_eq!(host.tracker.allocated(), host.tracker.deallocated());
    let events = host.events();
    let granted = events
        .iter()
        .filter(|e| matches!(e, Event::Granted(..)))
        .count();
    let released = events
        .iter()
        .filter(|e| matches!(e, Event::Released(..)))
        .count();
    assert_eq!(
        granted, released,
        "every actually granted physical block released once"
    );
}
#[test]
fn by_copy_child_scratch_host_recursive_actual_layouts_overlap_and_release() {
    let array = nested();
    let indices = indices();
    let tracker = tracker();
    let host = Host::new(tracker.clone(), None);
    let control = Control::new(tracker.clone(), None);
    evaluate(array.as_ref(), &indices, &host, &control).unwrap();
    assert_released(&host);
    let events = host.events();
    let layouts = events
        .iter()
        .filter_map(|e| {
            if let Event::Granted(layout, _) = e {
                Some(*layout)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert!(
        layouts.len() > 5,
        "actual recursive child tables, not source-stock estimate"
    );
    let metadata = layouts[0].size();
    let root = TakeRootScratchFacts::try_new(indices.len())
        .unwrap()
        .bytes();
    assert!(
        tracker.peak() > i64::try_from(root + metadata + layouts[1].size()).unwrap(),
        "ancestor and descendant tables coexist"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e,Event::Opaque(bytes,_) if *bytes==root))
    );
    assert_eq!(
        control.trace().last().unwrap().1,
        0,
        "success footer after all scratch and metadata drop"
    );
    let mut legacy_steps = 0;
    preflight_take(array.as_ref(), &indices, |boundary| {
        legacy_steps += usize::from(!boundary);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        control
            .trace()
            .iter()
            .map(|(units, _)| *units as usize)
            .sum::<usize>(),
        legacy_steps,
        "ONE original work traversal unchanged"
    );
}
#[test]
fn by_copy_child_scratch_host_real_allocator_seven_causes_at_every_discovered_allocation() {
    let array = nested();
    let indices = indices();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    evaluate(array.as_ref(), &indices, &host, &control).unwrap();
    let events = host.events();
    let attempts = events
        .iter()
        .filter(|e| matches!(e, Event::Attempt(_)))
        .count();
    assert!(attempts > 5);
    for cause in causes() {
        for stop in 0..attempts {
            let tr = tracker();
            let host = Host::new(tr.clone(), Some((stop, cause.clone())));
            let control = Control::new(tr, None);
            assert!(
                matches!(evaluate(array.as_ref(),&indices,&host,&control),Err(CopyError::Control(actual)) if actual==cause)
            );
            assert_eq!(*host.allocations.lock().unwrap(), stop + 1);
            assert_released(&host);
            // No successful finish after refusal: the original scratch was still live
            // at any last callback after metadata construction.
            if stop > 0 {
                assert!(control.trace().last().unwrap().1 > 0);
            }
        }
    }
}
#[test]
fn by_copy_child_scratch_host_seven_callback_causes_exact_prefix_no_footer() {
    let array = nested();
    let indices = indices();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    evaluate(array.as_ref(), &indices, &host, &control).unwrap();
    let trace = control.trace();
    assert!(trace.len() > 5);
    for cause in causes() {
        for stop in 0..trace.len() {
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, Some((stop, cause.clone())));
            assert!(
                matches!(evaluate(array.as_ref(),&indices,&host,&control),Err(CopyError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=stop]);
            assert_released(&host);
        }
    }
}
#[test]
fn by_copy_child_scratch_host_actual_tracker_capacity_refusal_rolls_back() {
    let array = nested();
    let indices = indices();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    evaluate(array.as_ref(), &indices, &host, &control).unwrap();
    let required_peak = tr.peak();
    assert!(required_peak > 1);
    let tr = tracker();
    tr.install_limit_once(required_peak - 1).unwrap();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    assert!(matches!(
        evaluate(array.as_ref(), &indices, &host, &control),
        Err(CopyError::Control(KernelFailure::ResourceExhausted))
    ));
    assert_released(&host);
}
#[test]
fn by_copy_child_scratch_host_original_nested_carriers_offsets_and_errors() {
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 2, 3])),
            Arc::new(Int32Array::from(vec![Some(3), None, Some(9)])),
            None,
        )
        .unwrap(),
    );
    let fixed: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            1,
            Arc::new(Int32Array::from(vec![Some(3), None, Some(9)])),
            None,
        )
        .unwrap(),
    );
    let union: ArrayRef = Arc::new(
        UnionArray::try_new(
            UnionFields::try_new(
                [0, 127],
                [
                    Field::new("first", DataType::Int64, false),
                    Field::new("last", DataType::Int64, false),
                ],
            )
            .unwrap(),
            vec![0_i8, 127, 0].into(),
            None,
            vec![
                Arc::new(Int64Array::from(vec![3, 4, 5])),
                Arc::new(Int64Array::from(vec![6, 7, 8])),
            ],
        )
        .unwrap(),
    );
    let runs: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![2, 5]),
            &Int64Array::from(vec![10, 20]),
        )
        .unwrap()
        .slice(1, 3),
    );
    let entries = StructArray::try_new(
        vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![Some(8), None, Some(9)])),
        ],
        None,
    )
    .unwrap();
    let map: ArrayRef = Arc::new(
        MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2, 3])),
            entries,
            None,
            false,
        )
        .unwrap(),
    );
    for array in [nested(), list, fixed, union, runs, map] {
        for indices in [vec![], vec![Some(2), Some(0), Some(2)], vec![Some(999)]] {
            let original = preflight_take(array.as_ref(), &indices, |_| Ok(()));
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, None);
            let result = evaluate(array.as_ref(), &indices, &host, &control);
            match (original, result) {
                (Ok(()), Ok(())) => {}
                (Err(original), Err(actual)) => {
                    assert_eq!(actual.to_string(), original.to_string());
                    assert!(!matches!(actual, CopyError::Control(_)));
                }
                pair => panic!("original exact carrier result differs: {pair:?}"),
            }
            assert_released(&host);
        }
    }
}
#[test]
fn by_copy_child_scratch_host_original_panic_unwinds_actual_recursive_blocks() {
    struct PanicControl<'a>(&'a Control, i64);
    impl KernelEvaluationControl for PanicControl<'_> {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            if units != 0 && self.0.tracker.current() > self.1 {
                panic!("recursive scratch original checkpoint panic")
            }
            self.0.checkpoint(units)
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("copy preflight does not wait")
        }
    }
    let array = nested();
    let plan = indices();
    let tr = tracker();
    let reference = Host::new(tr.clone(), None);
    let reference_control = Control::new(tr, None);
    evaluate(array.as_ref(), &plan, &reference, &reference_control).unwrap();
    let metadata = reference
        .events()
        .into_iter()
        .find_map(|e| {
            if let Event::Granted(layout, _) = e {
                Some(layout.size())
            } else {
                None
            }
        })
        .unwrap();
    let floor =
        i64::try_from(metadata + TakeRootScratchFacts::try_new(plan.len()).unwrap().bytes())
            .unwrap();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        evaluate(array.as_ref(), &plan, &host, &PanicControl(&control, floor))
    }));
    let payload = caught.err().expect("host panic remains a panic");
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("recursive scratch original checkpoint panic")
    );
    assert_released(&host);
}
