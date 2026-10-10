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

use super::*;
use crate::kernel_control::{internal, invalid};
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("NDV never waits")
    }
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            if *stop == at {
                return Err(cause.clone());
            }
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn assert_metadata_only(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    let metadata = ledger
        .metadata
        .expect("successful constructor allocated metadata");
    assert_eq!(ledger.live.as_slice(), &[metadata]);
    assert_eq!(ledger.bytes, metadata.1.size());
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}

fn released(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
}
fn prepare(
    host: &HostAggregateAllocator,
    control: &Control,
) -> Result<HostDiagnostic, KernelFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    let result = HostDiagnostic::prepare(host, &mut work, |writer| {
        write!(writer, "{}:{}", "original diagnostic ".repeat(97), "µ𝄞")
    });
    if result.is_err() {
        return result;
    }
    work.finish()?;
    result
}
#[test]
fn real_host_message_backing_is_unbounded_utf8_clone_is_no_allocation_and_last_drop_releases() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let message = prepare(&allocator, &Control::default()).unwrap();
    let expected = format!("{}:{}", "original diagnostic ".repeat(97), "µ𝄞");
    assert!(expected.len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    assert_eq!(message.message(), expected);
    assert_eq!(
        message.retained_bytes() + allocator.metadata_bytes(),
        host.ledger.lock().unwrap().bytes
    );
    let attempts = host.ledger.lock().unwrap().attempts;
    let loan = message.clone();
    assert!(loan.ptr_eq(&message));
    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
    drop(allocator);
    drop(message);
    assert_eq!(loan.message(), expected);
    assert!(!host.ledger.lock().unwrap().live.is_empty());
    drop(loan);
    released(&host);
}
#[test]
fn every_real_payload_and_header_allocation_refusal_keeps_typed_cause_and_rolls_back() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let start = host.ledger.lock().unwrap().attempts;
    let message = prepare(&allocator, &Control::default()).unwrap();
    let allocations = host.ledger.lock().unwrap().attempts - start;
    assert!(allocations >= 2);
    drop(message);
    assert_metadata_only(&host);
    drop(allocator);
    released(&host);
    for cause in causes() {
        for stop in 0..allocations {
            let host = Arc::new(Host::default());
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            arm_refusal(&host, stop, cause.clone());
            assert!(matches!(prepare(&allocator,&Control::default()),Err(error) if error==cause));
            assert_metadata_only(&host);
            drop(allocator);
            released(&host);
        }
    }
}
#[test]
fn every_observed_byte_copy_and_header_checkpoint_is_first_cause_without_tail_or_leak() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let control = Control::default();
    let message = prepare(&allocator, &control).unwrap();
    let steps = control.trace.lock().unwrap().len();
    assert!(control.trace.lock().unwrap().contains(&256));
    drop(message);
    drop(allocator);
    released(&host);
    for cause in causes() {
        for stop in 0..steps {
            let host = Arc::new(Host::default());
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(prepare(&allocator,&control),Err(error) if error==cause));
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            assert_metadata_only(&host);
            drop(allocator);
            released(&host);
        }
    }
}

#[test]
fn host_shared_last_drop_releases_actual_header_when_payload_drop_unwinds() {
    struct Payload {
        bytes: HostVec<u8, HostAggregateAllocator>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            assert_eq!(self.bytes.as_slice(), b"real owned payload");
            panic!("original payload destructor panic");
        }
    }
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut bytes = HostVec::new_in(allocator.clone());
    bytes.try_reserve_exact(18).unwrap();
    bytes.extend_from_slice(b"real owned payload");
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let payload = HostShared::try_new(Payload { bytes }, allocator.clone(), &mut work).unwrap();
    drop(allocator);
    let error =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))).unwrap_err();
    assert_eq!(
        error.downcast_ref::<&str>().copied(),
        Some("original payload destructor panic")
    );
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    assert!(host.ledger.lock().unwrap().live.is_empty());
}
