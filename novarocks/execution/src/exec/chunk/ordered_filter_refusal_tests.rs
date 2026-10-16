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

//! Late refusal of the creator's original public host requests. Every success
//! delegates to the same real expression allocator. The constructor owner has
//! its separate original port: this is not a sweep of private Arrow requests,
//! typed-construction callbacks, GLOBAL, or a physical peak bound. Existing
//! positive MemTracker-limit tests remain the actual capacity-refusal oracle.
use super::*;
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use std::{alloc::Layout, ptr::NonNull, sync::Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Request {
    Typed(Layout),
    Opaque(usize),
}
#[derive(Default)]
struct Ledger {
    requests: Vec<Request>,
    blocks: Vec<(usize, Layout)>,
    opaque_bytes: usize,
}
struct RefusingPort {
    original: Arc<dyn AggregateStateAllocator>,
    refuse_at: Option<usize>,
    ledger: Mutex<Ledger>,
}
impl RefusingPort {
    fn new(original: Arc<dyn AggregateStateAllocator>, refuse_at: Option<usize>) -> Arc<Self> {
        assert!(
            original.opaque_allocation_host().is_some(),
            "the actual host has its original opaque capability"
        );
        Arc::new(Self {
            original,
            refuse_at,
            ledger: Mutex::new(Ledger::default()),
        })
    }
    fn request(&self, request: Request) -> Result<(), KernelFailure> {
        let mut ledger = self.ledger.lock().unwrap();
        let ordinal = ledger.requests.len();
        ledger.requests.push(request);
        if self.refuse_at == Some(ordinal) {
            return Err(KernelFailure::ResourceExhausted);
        }
        Ok(())
    }
    fn requests(&self) -> Vec<Request> {
        self.ledger.lock().unwrap().requests.clone()
    }
    fn assert_returned(&self) {
        let ledger = self.ledger.lock().unwrap();
        assert!(
            ledger.blocks.is_empty(),
            "every successful public typed allocation was returned"
        );
        assert_eq!(
            ledger.opaque_bytes, 0,
            "every successful public opaque reserve was released"
        );
    }
}
impl AggregateStateAllocator for RefusingPort {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        self.request(Request::Typed(layout))?;
        let block = self.original.allocate(layout)?;
        self.ledger
            .lock()
            .unwrap()
            .blocks
            .push((block.as_ptr().addr(), layout));
        Ok(block)
    }
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        {
            let mut ledger = self.ledger.lock().unwrap();
            let index = ledger
                .blocks
                .iter()
                .position(|&(address, actual)| address == block.as_ptr().addr() && actual == layout)
                .expect("the exact successful public allocation is released once");
            ledger.blocks.swap_remove(index);
        }
        // SAFETY: this port forwards the same pointer/layout, exactly once,
        // to the original allocator that actually created the block.
        unsafe { self.original.release(block, layout) };
    }
}
impl OpaqueAllocationHost for RefusingPort {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.request(Request::Opaque(bytes))?;
        self.original
            .opaque_allocation_host()
            .unwrap()
            .reserve_opaque(bytes)?;
        let mut ledger = self.ledger.lock().unwrap();
        ledger.opaque_bytes = ledger.opaque_bytes.checked_add(bytes).unwrap();
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        self.original
            .opaque_allocation_host()
            .unwrap()
            .release_opaque(bytes);
        let mut ledger = self.ledger.lock().unwrap();
        ledger.opaque_bytes = ledger
            .opaque_bytes
            .checked_sub(bytes)
            .expect("release only the successfully reserved public stock");
    }
}
#[derive(Clone, Copy)]
enum Path {
    MixedFilter,
    Slice,
}
impl Path {
    fn apply(self, fixture: &mut Fixture) -> ExecutionResult<Option<Chunk>> {
        match self {
            Self::MixedFilter => fixture.apply(&mixed_mask(), &Control::running()),
            Self::Slice => fixture
                .input
                .take()
                .unwrap()
                .slice_ordered_scan_controlled(
                    1,
                    3,
                    fixture.host.clone(),
                    &Control::running(),
                    fixture.construction.as_ref(),
                )
                .map(Some),
        }
    }
    fn assert_rows(self, output: &Chunk) {
        match self {
            Self::MixedFilter => assert_mixed_rows(output),
            Self::Slice => {
                assert_eq!(output.len(), 3);
                assert_eq!(
                    output.columns()[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some("drop"), None, Some("海")]
                );
                assert_eq!(
                    output.columns()[1]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(22), None, Some(44)]
                );
            }
        }
    }
}
fn successful_requests(path: Path) -> Vec<Request> {
    let mut fixture = Fixture::new(true, Some(64 << 20));
    let tracker = fixture.tracker.clone();
    let original = Arc::downgrade(&fixture.input.as_ref().unwrap().columns()[0]);
    let authority = fixture
        .input
        .as_ref()
        .unwrap()
        .source_authority()
        .owner_identity();
    let port = RefusingPort::new(fixture.host.clone(), None);
    fixture.host = port.clone();
    let output = path.apply(&mut fixture).unwrap().unwrap();
    path.assert_rows(&output);
    assert_eq!(output.source_authority().owner_identity(), authority);
    let requests = port.requests();
    assert!(requests.iter().any(|r| matches!(r, Request::Typed(_))));
    assert!(
        requests
            .iter()
            .any(|r| matches!(r, Request::Opaque(bytes) if *bytes > 0))
    );
    drop(output);
    drop(fixture);
    assert!(original.upgrade().is_none());
    assert_eq!(tracker.current(), 0);
    port.assert_returned();
    requests
}
fn refuse_each_request(path: Path) {
    let successful = successful_requests(path);
    for ordinal in 0..successful.len() {
        let mut fixture = Fixture::new(true, Some(64 << 20));
        let tracker = fixture.tracker.clone();
        let original = Arc::downgrade(&fixture.input.as_ref().unwrap().columns()[0]);
        let port = RefusingPort::new(fixture.host.clone(), Some(ordinal));
        fixture.host = port.clone();
        let error = path.apply(&mut fixture).unwrap_err();
        assert_eq!(
            error.cause(),
            &ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted),
            "public request {ordinal}: {:?}",
            successful[ordinal]
        );
        assert_eq!(
            port.requests(),
            successful[..=ordinal],
            "refusal stops at the original actual public request; no later request or Chunk is published"
        );
        drop(fixture);
        assert!(
            original.upgrade().is_none(),
            "request {ordinal} retained the original source Array"
        );
        assert_eq!(
            tracker.current(),
            0,
            "request {ordinal} retained native or new stock"
        );
        port.assert_returned();
    }
}

#[test]
fn ordered_output_resource_mixed_filter_rolls_back_every_actual_public_request() {
    refuse_each_request(Path::MixedFilter);
}

#[test]
fn ordered_output_resource_controlled_slice_rolls_back_every_actual_public_request() {
    // The slice's real zero additional-buffer delta still calls the original
    // opaque port. Its injected refusal is a public semantic-host verdict,
    // not evidence of physical allocation or a positive-capacity shortage.
    refuse_each_request(Path::Slice);
}
