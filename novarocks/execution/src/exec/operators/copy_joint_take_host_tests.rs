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

//! Complete take probes borrow the actual production expression host. Test
//! ledgers are diagnostics, never an alternative allocator or capacity wallet.
use super::{expression_allocation_host, MemTracker};
use arrow::array::{
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, LargeStringArray,
    StringViewArray, ListArray, DictionaryArray, UInt32Array, UInt64Array, RunArray, UnionArray,
    StructArray, NullArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int8Type, Int16Type, UnionFields};
use novarocks_functions::{
    AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    MAX_UNOBSERVED_KERNEL_WORK,
};
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_functions::selected_copy::{CopyIndices, CopyOperationError, take_copy_in};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Attempt(Layout),
    OpaqueAttempt(usize),
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
        let at = {
            let mut next = self.allocations.lock().unwrap();
            let at = *next;
            *next += 1;
            at
        };
        self.events
            .lock()
            .unwrap()
            .push(Event::OpaqueAttempt(bytes));
        if let Some((stop, cause)) = &self.refusal {
            assert!(at <= *stop, "host attempt after first refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
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

fn assert_released(host: &Host) {
    assert_eq!(host.tracker.current(), 0);
    assert_eq!(host.tracker.allocated(), host.tracker.deallocated());
    let events = host.events();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Event::Granted(..)))
            .count(),
        events
            .iter()
            .filter(|e| matches!(e, Event::Released(..)))
            .count()
    );
}
struct SourceOwner(Arc<AtomicUsize>);
impl Drop for SourceOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn input() -> ArrayRef {
    let bytes: ArrayRef = Arc::new(StringArray::from(vec![
        Some("first"),
        None,
        Some("x".repeat(12000).as_str()),
    ]));
    let list: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 1, 3])),
            bytes,
            None,
        )
        .unwrap(),
    );
    Arc::new(
        StructArray::try_new(
            vec![Arc::new(Field::new(
                "nested",
                list.data_type().clone(),
                true,
            ))]
            .into(),
            vec![list],
            None,
        )
        .unwrap(),
    )
}
fn selected() -> CopyIndices {
    CopyIndices::UInt32(Arc::new(UInt32Array::from(vec![
        Some(2),
        None,
        Some(0),
        Some(2),
    ])))
}
#[test]
fn by_copy_joint_take_actual_peak_new_backing_and_last_buffer_drop() {
    let source = input();
    let indices = selected();
    let expected = arrow::compute::take(
        source.as_ref(),
        match &indices {
            CopyIndices::UInt32(a) => a.as_ref(),
            CopyIndices::UInt64(a) => a.as_ref(),
        },
        None,
    )
    .unwrap();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    let drops = Arc::new(AtomicUsize::new(0));
    let result = take_copy_in(
        source,
        indices,
        SourceOwner(drops.clone()),
        host.clone(),
        &control,
    )
    .unwrap();
    assert_eq!(result.values().to_data(), expected.to_data());
    assert!(result.facts().operation_peak_bytes() >= result.facts().retained_new_backing_upper());
    assert!(host.events().iter().any(
        |e| matches!(e,Event::Opaque(bytes,_) if *bytes==result.facts().operation_peak_bytes())
    ));
    assert!(tr.current() > 0);
    let output = result.into_values();
    let slice = output.slice(0, 1);
    drop(output);
    let data = slice.to_data();
    let buffer = data.child_data()[0].buffers()[0].clone();
    drop(data);
    drop(slice);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "derived Buffer still owns original source and new backing"
    );
    assert!(tr.current() > 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_released(&host);
}
#[test]
fn by_copy_joint_take_every_actual_host_attempt_seven_causes_rollback_no_retry() {
    let source = input();
    let indices = selected();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    drop(take_copy_in(source.clone(), indices.clone(), (), host.clone(), &control).unwrap());
    let attempts = *host.allocations.lock().unwrap();
    assert!(attempts > 5);
    assert_released(&host);
    for stop in 0..attempts {
        for cause in causes() {
            let tr = tracker();
            let host = Host::new(tr.clone(), Some((stop, cause.clone())));
            let control = Control::new(tr, None);
            let result = take_copy_in(source.clone(), indices.clone(), (), host.clone(), &control);
            assert!(
                matches!(result,Err(CopyOperationError::Control(actual)) if actual==cause),
                "stop={stop}"
            );
            assert_eq!(*host.allocations.lock().unwrap(), stop + 1);
            assert_released(&host);
        }
    }
}
#[test]
fn by_copy_joint_take_every_callback_seven_causes_exact_prefix_no_footer() {
    let source = input();
    let indices = selected();
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    drop(take_copy_in(source.clone(), indices.clone(), (), host.clone(), &control).unwrap());
    let trace = control.trace();
    assert!(trace.len() > 5);
    assert_released(&host);
    for stop in 0..trace.len() {
        for cause in causes() {
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, Some((stop, cause.clone())));
            assert!(
                matches!(take_copy_in(source.clone(),indices.clone(),(),host.clone(),&control),Err(CopyOperationError::Control(actual)) if actual==cause),
                "stop={stop}"
            );
            assert_eq!(control.trace(), trace[..=stop]);
            assert_released(&host);
        }
    }
}
#[test]
fn by_copy_joint_take_real_host_capacity_denial_rolls_back_full_copy_scope() {
    let tr = tracker();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr.clone(), None);
    drop(take_copy_in(input(), selected(), (), host.clone(), &control).unwrap());
    let peak = tr.peak();
    assert!(peak > 1);
    assert_released(&host);
    let tr = tracker();
    tr.install_limit_once(peak - 1).unwrap();
    let host = Host::new(tr.clone(), None);
    let control = Control::new(tr, None);
    assert!(matches!(
        take_copy_in(input(), selected(), (), host.clone(), &control),
        Err(CopyOperationError::Control(
            KernelFailure::ResourceExhausted
        ))
    ));
    assert_released(&host);
}
#[test]
fn by_copy_joint_take_dictionary_and_view_source_aliases_are_not_new_payload_stock() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1, 0]),
            Arc::new(StringArray::from(vec![
                "large-shared-source".repeat(12000),
                "second".into(),
            ])),
        )
        .unwrap(),
    );
    let views: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("long shared view backing"),
        None,
        Some("a second long shared view"),
    ]));
    for source in [dictionary, views] {
        let source_stock = source.get_array_memory_size();
        let indices = CopyIndices::UInt64(Arc::new(UInt64Array::from(vec![
            Some(2_u64),
            None,
            Some(0),
        ])));
        let expected = arrow::compute::take(
            source.as_ref(),
            match &indices {
                CopyIndices::UInt32(a) => a.as_ref(),
                CopyIndices::UInt64(a) => a.as_ref(),
            },
            None,
        )
        .unwrap();
        let tr = tracker();
        let host = Host::new(tr.clone(), None);
        let control = Control::new(tr, None);
        let drops = Arc::new(AtomicUsize::new(0));
        let result = take_copy_in(
            source,
            indices,
            SourceOwner(drops.clone()),
            host.clone(),
            &control,
        )
        .unwrap();
        assert_eq!(result.values().to_data(), expected.to_data());
        if matches!(result.values().data_type(), DataType::Dictionary(..)) {
            assert!(
                result.facts().retained_new_backing_upper() < source_stock,
                "dictionary domain is source-owned, not newly granted output"
            );
        }
        let alias = result.values().slice(0, 1);
        drop(result);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(alias);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_released(&host);
    }
}
#[test]
fn by_copy_joint_take_original_nested_union_run_and_empty_paths() {
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
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef,
            ],
        )
        .unwrap(),
    );
    let dense: ArrayRef = Arc::new(
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
            Some(vec![0_i32, 0, 1].into()),
            vec![
                Arc::new(Int64Array::from(vec![1, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![4])) as ArrayRef,
            ],
        )
        .unwrap(),
    );
    let run: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(
            &Int16Array::from(vec![1, 3]),
            &Int32Array::from(vec![10, 20]),
        )
        .unwrap(),
    );
    let ordinary = [
        input(),
        union,
        dense,
        run,
        Arc::new(NullArray::new(3)) as ArrayRef,
        Arc::new(LargeStringArray::from(vec!["a", "b", "c"])) as ArrayRef,
    ];
    for source in ordinary {
        for raw in [vec![2_u32, 0, 2], Vec::new()] {
            let indices = Arc::new(UInt32Array::from(raw));
            let expected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                arrow::compute::take(source.as_ref(), indices.as_ref(), None)
            }));
            let tr = tracker();
            let host = Host::new(tr.clone(), None);
            let control = Control::new(tr, None);
            let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                take_copy_in(
                    source.clone(),
                    CopyIndices::UInt32(indices.clone()),
                    (),
                    host.clone(),
                    &control,
                )
            }));
            match (expected, actual) {
                (Ok(Ok(expected)), Ok(Ok(actual))) => {
                    assert_eq!(actual.values().to_data(), expected.to_data());
                    drop(actual);
                }
                (Ok(Err(expected)), Ok(Err(CopyOperationError::OriginalData(actual)))) => {
                    assert_eq!(actual, expected.to_string())
                }
                (Err(_), Err(_)) => {}
                (expected, actual) => panic!(
                    "original/hosted discrepancy: original={expected:?}, hosted={}",
                    match actual {
                        Ok(Err(e)) => format!("{e:?}"),
                        Ok(Ok(_)) => "success".into(),
                        Err(_) => "panic".into(),
                    }
                ),
            }
            assert_released(&host);
        }
    }
}

#[test]
fn by_copy_joint_take_source_owner_drops_after_inputs_on_refusal_and_final_custody() {
    struct OrderOwner {
        source: std::sync::Weak<dyn Array>,
        indices: std::sync::Weak<UInt32Array>,
        observed: Arc<AtomicUsize>,
    }
    impl Drop for OrderOwner {
        fn drop(&mut self) {
            assert!(
                self.source.upgrade().is_none(),
                "source ArrayRef destroyed before its original lease"
            );
            assert!(
                self.indices.upgrade().is_none(),
                "index ArrayRef destroyed before its original lease"
            );
            self.observed.fetch_add(1, Ordering::SeqCst);
        }
    }
    for refusal in [None, Some((0, KernelFailure::Cancelled))] {
        let source: ArrayRef = Arc::new(StringArray::from(vec!["first", "second", "third"]));
        let indices = Arc::new(UInt32Array::from(vec![2_u32, 0]));
        let observed = Arc::new(AtomicUsize::new(0));
        let owner = OrderOwner {
            source: Arc::downgrade(&source),
            indices: Arc::downgrade(&indices),
            observed: observed.clone(),
        };
        let tr = tracker();
        let host = Host::new(tr.clone(), refusal.clone());
        let control = Control::new(tr, None);
        match take_copy_in(
            source,
            CopyIndices::UInt32(indices),
            owner,
            host.clone(),
            &control,
        ) {
            Ok(result) => {
                assert!(refusal.is_none());
                assert_eq!(observed.load(Ordering::SeqCst), 0);
                drop(result);
            }
            Err(CopyOperationError::Control(KernelFailure::Cancelled)) => {
                assert!(refusal.is_some())
            }
            Err(error) => panic!("unexpected exact source exit {error:?}"),
        }
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_released(&host);
    }
}

#[cfg(test)]
#[path = "copy_take_null_original_tests.rs"]
mod copy_take_null_original_tests;

#[cfg(test)]
#[path = "copy_original_data_custody_tests.rs"]
mod copy_original_data_custody_tests;

#[path = "copy_original_fixed_list_index_tests.rs"]
mod original_fixed_list_index_tests;

#[path = "copy_original_slice_host_tests.rs"]
mod original_slice_host_tests;

#[path = "copy_original_concat_host_tests.rs"]
mod original_concat_host_tests;
