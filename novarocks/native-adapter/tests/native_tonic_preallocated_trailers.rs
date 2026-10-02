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

//! Actual EncodeBody owns one preclaimed trailer map through EOF/error/drop.
//! Only map/field storage and their original carriers are funded here. Codec
//! message buffers, source/status formatting and body/test scaffolds are separate.

use bytes::{BufMut, Bytes};
use hyper::body::{Body, Frame};
use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderValue};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use tokio_stream::Stream;
use tonic::codec::{BufferSettings, EncodeBody, EncodeBuf, Encoder};
use tonic::{Code, Status};

#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    family: u8,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    family: 0,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static FAMILY: Cell<u8> = const { Cell::new(0) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static REQUESTED: Cell<usize> = const { Cell::new(0) };
    static RECORDS: RefCell<[Allocation; 128]> = const { RefCell::new([EMPTY; 128]) };
}
fn allocated(pointer: *mut u8, size: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        CALLS.with(|v| v.set(v.get() + 1));
        REQUESTED.with(|v| v.set(v.get() + size));
        let family = FAMILY.with(Cell::get);
        if family != 0 {
            RECORDS.with(|records| {
                let mut records = records.borrow_mut();
                let slot = records.iter_mut().find(|r| r.pointer == 0).unwrap();
                *slot = Allocation {
                    pointer: pointer as usize,
                    family,
                };
            });
        }
    }
}
fn freed(pointer: *mut u8) {
    let _ = RECORDS.try_with(|records| {
        if let Some(record) = records
            .borrow_mut()
            .iter_mut()
            .find(|r| r.pointer == pointer as usize)
        {
            *record = EMPTY;
        }
    });
}
struct Probe;
// SAFETY: Allocation operations delegate unchanged to System. Fixed TLS records
// neither allocate nor dereference the recorded pointers.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer, layout.size());
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        freed(pointer);
        allocated(next, size);
        next
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        freed(pointer);
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Tracking;
impl Drop for Tracking {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
        FAMILY.with(|v| v.set(0));
    }
}
fn measure<T>(family: u8, operation: impl FnOnce() -> T) -> (T, usize, usize) {
    assert!(!TRACK.with(|v| v.replace(true)));
    FAMILY.with(|v| v.set(family));
    CALLS.with(|v| v.set(0));
    REQUESTED.with(|v| v.set(0));
    let tracking = Tracking;
    let result = operation();
    drop(tracking);
    (result, CALLS.with(Cell::get), REQUESTED.with(Cell::get))
}
fn no_allocation<T>(operation: impl FnOnce() -> T) -> T {
    let (result, calls, bytes) = measure(0, operation);
    assert_eq!((calls, bytes), (0, 0));
    result
}
struct PhysicalExit {
    family: u8,
    _credit: ResultWriteCredit,
}
impl Drop for PhysicalExit {
    fn drop(&mut self) {
        if self.family != 0 {
            assert!(
                RECORDS.with(|records| records
                    .borrow()
                    .iter()
                    .all(|r| r.pointer == 0 || r.family != self.family)),
                "original fixed pool/Core/carrier backing must be physically freed before credit"
            );
        }
    }
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("complete original family pregrant");
    };
    credit
}
fn owner(budget: &Arc<ResultRetainedBudget>, bytes: usize, family: u8) -> Bytes {
    let credit = grant(budget, bytes);
    // The carrier has its own complete pregrant, in addition to the fixed pool.
    let (owner, _, requested) = measure(family, || {
        Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            PhysicalExit {
                family,
                _credit: credit,
            },
        )
    });
    assert!(requested <= carrier());
    owner
}
fn carrier() -> usize {
    Bytes::owner_with_exit_guard_metadata_size::<Bytes, PhysicalExit>()
}
struct Funded {
    maps: HeaderMapAllocationPool,
    fields: HeaderFieldAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    total: usize,
}
impl Funded {
    fn new(capacity: usize, positions: usize) -> Self {
        Self::with_maximum(capacity, positions, capacity)
    }
    fn with_maximum(capacity: usize, positions: usize, maximum: usize) -> Self {
        let map_bound = HeaderMapAllocationPool::allocation_capacity_bound(1, 4, 2).unwrap();
        let field_bound =
            HeaderFieldAllocationPool::allocation_capacity_bound(capacity, positions, maximum)
                .unwrap();
        let total = map_bound + field_bound + 2 * carrier();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let field_owner = owner(&budget, field_bound + carrier(), 1);
        let (fields, calls, requested) = measure(1, || {
            HeaderFieldAllocationPool::new(capacity, positions, maximum, field_owner).unwrap()
        });
        assert_eq!(calls, 3);
        assert!(requested <= field_bound);
        let map_owner = owner(&budget, map_bound + carrier(), 2);
        let (maps, calls, requested) = measure(2, || {
            HeaderMapAllocationPool::new(1, 4, 2, map_owner).unwrap()
        });
        assert_eq!(calls, 1);
        assert!(requested <= map_bound);
        fields.try_bind_once().unwrap();
        maps.try_bind_connection_with_fields(&fields).unwrap();
        Self {
            maps,
            fields,
            budget,
            total,
        }
    }
    fn map(&self) -> HeaderMap {
        let (map, calls, _) = measure(2, || {
            HeaderMap::try_from_allocation_pool(&self.maps).unwrap()
        });
        assert_eq!(calls, 3);
        assert_eq!(
            self.maps.available_maps(),
            0,
            "no replacement map position exists"
        );
        map
    }
    fn detach(self) -> (Arc<ResultRetainedBudget>, usize) {
        let Self {
            maps,
            fields,
            budget,
            total,
        } = self;
        drop((maps, fields));
        (budget, total)
    }
}
fn held(budget: &Arc<ResultRetainedBudget>, total: usize) {
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
struct Source {
    items: VecDeque<Result<Bytes, Status>>,
    pending: bool,
    polls: Arc<AtomicUsize>,
    exits: Arc<AtomicUsize>,
}
impl Stream for Source {
    type Item = Result<Bytes, Status>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(self.items.pop_front())
        }
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
fn source(
    items: impl IntoIterator<Item = Result<Bytes, Status>>,
    pending: bool,
) -> (Source, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    (
        Source {
            items: items.into_iter().collect(),
            pending,
            polls: polls.clone(),
            exits: exits.clone(),
        },
        polls,
        exits,
    )
}
struct ActualEncoder {
    fail: bool,
}
impl Encoder for ActualEncoder {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&item);
        if self.fail {
            Err(Status::from_static(Code::InvalidArgument, "bad %"))
        } else {
            Ok(())
        }
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(64, 64)
    }
}
type ServerBody = EncodeBody<ActualEncoder, Source>;
fn body(map: HeaderMap, source: Source, fail: bool) -> ServerBody {
    EncodeBody::new_server_with_trailers(
        ActualEncoder { fail },
        source,
        None,
        Default::default(),
        None,
        map,
    )
}
fn poll(body: &mut ServerBody) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
    Pin::new(body).poll_frame(&mut Context::from_waker(Waker::noop()))
}
fn frame(body: &mut ServerBody) -> Frame<Bytes> {
    match poll(body) {
        Poll::Ready(Some(Ok(frame))) => frame,
        other => panic!("expected frame, got {other:?}"),
    }
}
fn terminal(body: &mut ServerBody, polls: &Arc<AtomicUsize>) {
    let previous = polls.load(Ordering::SeqCst);
    assert!(body.is_end_stream());
    for _ in 0..3 {
        assert!(matches!(no_allocation(|| poll(body)), Poll::Ready(None)));
    }
    assert_eq!(
        polls.load(Ordering::SeqCst),
        previous,
        "terminal body must never repoll source"
    );
}

#[test]
fn eof_emits_the_only_original_map_without_new_scaffolds_and_retains_actual_exit() {
    let funded = Funded::new(256, 4);
    let mut map = funded.map();
    map.try_insert(
        hyper::http::header::HeaderName::from_static("x-original"),
        HeaderValue::from_static("kept"),
    )
    .unwrap();
    let (source, polls, exits) = source([], false);
    let mut body = body(map, source, false);
    let (budget, total) = funded.detach();
    held(&budget, total);
    let frame = no_allocation(|| frame(&mut body));
    let trailers = frame.into_trailers().unwrap();
    assert_eq!(trailers[Status::GRPC_STATUS], "0");
    assert_eq!(trailers["x-original"], "kept");
    assert!(trailers.field_allocation_pool().is_some());
    terminal(&mut body, &polls);
    drop(body);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    held(&budget, total);
    drop(trailers);
    drop(grant(&budget, total));
}

#[test]
fn source_error_consumes_same_map_and_only_escaped_transformed_value_keeps_field_credit() {
    let funded = Funded::new(256, 4);
    let (source, polls, exits) = source(
        [
            Ok(Bytes::from_static(b"ok")),
            Err(Status::from_static(Code::InvalidArgument, "bad %")),
            Ok(Bytes::from_static(b"must not escape")),
        ],
        false,
    );
    let mut body = body(funded.map(), source, false);
    let (budget, total) = funded.detach();
    assert_eq!(
        frame(&mut body).into_data().unwrap().as_ref(),
        b"\0\0\0\0\x02ok",
        "completed messages precede the deferred source status"
    );
    let trailers = frame(&mut body).into_trailers().unwrap();
    assert_eq!(trailers[Status::GRPC_STATUS], "3");
    assert_eq!(trailers[Status::GRPC_MESSAGE].as_bytes(), b"bad%20%");
    let alias = no_allocation(|| trailers[Status::GRPC_MESSAGE].clone());
    assert_eq!(
        alias.as_bytes().as_ptr(),
        trailers[Status::GRPC_MESSAGE].as_bytes().as_ptr()
    );
    terminal(&mut body, &polls);
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    drop((trailers, body));
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    held(&budget, total);
    drop(alias);
    drop(grant(&budget, total));
}

#[test]
fn encoder_error_discards_partial_data_and_terminates_before_following_source_item() {
    // Raw formatted diagnostic and percent-transformed output coexist during
    // serialization. Their 128B + 192B rounded extents require this real arena.
    let funded = Funded::new(512, 4);
    let (source, polls, exits) = source(
        [
            Ok(Bytes::from_static(b"partial")),
            Ok(Bytes::from_static(b"later")),
        ],
        false,
    );
    let mut body = body(funded.map(), source, true);
    // Constructor/source/codec staging are prepared before measurement. The
    // only two requested allocations are original-field owner wrappers: the
    // raw formatted Status Display diagnostic and its transformed wire value.
    let wrapper_bytes = HeaderFieldAllocationPool::allocation_capacity_bound(512, 4, 512).unwrap()
        - HeaderFieldAllocationPool::allocation_capacity_bound(512, 3, 512).unwrap();
    let (result, calls, bytes) = measure(1, || poll(&mut body));
    assert_eq!((calls, bytes), (2, 2 * wrapper_bytes));
    let trailers = match result {
        Poll::Ready(Some(Ok(frame))) => frame.into_trailers().unwrap(),
        other => panic!("expected bounded encoder diagnostic: {other:?}"),
    };
    assert_eq!(trailers[Status::GRPC_STATUS], "13");
    assert_eq!(
        trailers[Status::GRPC_MESSAGE].as_bytes(),
        b"Error%20encoding:%20status:%20InvalidArgument,%20message:%20%22bad%20%%22,%20details:%20[],%20metadata:%20MetadataMap%20%7B%20headers:%20%7B%7D%20%7D"
    );
    assert_eq!(
        funded.fields.available_positions(),
        3,
        "raw formatted diagnostic has actually exited"
    );
    let alias = no_allocation(|| trailers[Status::GRPC_MESSAGE].clone());
    terminal(&mut body, &polls);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    drop((body, trailers));
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    let (budget, total) = funded.detach();
    held(&budget, total);
    drop(alias);
    drop(grant(&budget, total));
}

#[test]
fn formatted_encoder_diagnostic_maximum_or_positions_refuse_without_heap_fallback() {
    for exhausted_position in [false, true] {
        let funded = if exhausted_position {
            Funded::new(512, 1)
        } else {
            // Neither the raw diagnostic nor the static refusal's wire message
            // fits. The terminal error retains the cleared original map rather
            // than attempting heap-backed recursive status serialization.
            Funded::with_maximum(512, 4, 32)
        };
        let blocker = if exhausted_position {
            Some(
                funded
                    .fields
                    .try_fill::<std::convert::Infallible>(1, |bytes| {
                        bytes[0] = b'x';
                        Ok(())
                    })
                    .unwrap(),
            )
        } else {
            None
        };
        let (source, polls, exits) = source(
            [
                Ok(Bytes::from_static(b"partial")),
                Ok(Bytes::from_static(b"must not escape")),
            ],
            false,
        );
        let mut body = body(funded.map(), source, true);
        let (budget, total) = funded.detach();
        let error = match no_allocation(|| poll(&mut body)) {
            Poll::Ready(Some(Err(error))) => error,
            other => panic!("expected static formatted-input refusal: {other:?}"),
        };
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(error.message(), "HTTP status field capacity exhausted");
        assert!(
            error.metadata().is_empty(),
            "no partial status or DATA escaped"
        );
        terminal(&mut body, &polls);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        drop((body, blocker));
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        held(&budget, total);
        drop(error);
        drop(grant(&budget, total));
    }
}

#[test]
fn unpolled_and_pending_body_keep_original_map_until_actual_drop() {
    for pending in [false, true] {
        let funded = Funded::new(256, 4);
        let (source, polls, exits) = source([], pending);
        let mut body = body(funded.map(), source, false);
        let (budget, total) = funded.detach();
        if pending {
            assert!(no_allocation(|| poll(&mut body)).is_pending());
        }
        held(&budget, total);
        assert_eq!(polls.load(Ordering::SeqCst), usize::from(pending));
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(body);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        drop(grant(&budget, total));
    }
}

#[test]
fn exhausted_field_positions_or_extents_return_one_error_with_no_partial_trailer_escape() {
    for (capacity, positions, blocked_len) in [(256, 1, 1), (256, 4, 256)] {
        let funded = Funded::new(capacity, positions);
        let blocker = funded
            .fields
            .try_fill::<std::convert::Infallible>(blocked_len, |bytes| {
                bytes.fill(b'x');
                Ok(())
            })
            .unwrap();
        let (source, polls, _) = source(
            [
                Err(Status::from_static(Code::InvalidArgument, "bad %")),
                Ok(Bytes::from_static(b"later")),
            ],
            false,
        );
        let mut body = body(funded.map(), source, false);
        let (budget, total) = funded.detach();
        let error = match no_allocation(|| poll(&mut body)) {
            Poll::Ready(Some(Err(error))) => error,
            other => panic!("expected terminal serialization refusal: {other:?}"),
        };
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert!(
            error.metadata().is_empty(),
            "partial grpc-status was cleared"
        );
        terminal(&mut body, &polls);
        drop((body, blocker));
        held(&budget, total);
        drop(error);
        drop(grant(&budget, total));
    }
}

#[test]
fn full_trailer_keys_refuse_before_allocation_and_error_retains_cleared_original_map() {
    let funded = Funded::new(256, 4);
    let mut map = funded.map();
    // Four requested keys round to a usable capacity of six. Prebuild static
    // names so payload copying is outside the scaffold refusal oracle.
    for name in ["a", "b", "c", "d", "e", "f"] {
        map.try_insert(
            hyper::http::header::HeaderName::from_static(name),
            HeaderValue::from_static("v"),
        )
        .unwrap();
    }
    let (source, polls, _) = source([], false);
    let mut body = body(map, source, false);
    let (budget, total) = funded.detach();
    let error = match no_allocation(|| poll(&mut body)) {
        Poll::Ready(Some(Err(error))) => error,
        other => panic!("expected key refusal: {other:?}"),
    };
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.metadata().is_empty());
    terminal(&mut body, &polls);
    drop(body);
    held(&budget, total);
    drop(error);
    drop(grant(&budget, total));
}

#[test]
fn ordinary_server_and_client_constructors_preserve_default_eof_roles() {
    let (src, _, _) = source([], false);
    let mut server = EncodeBody::new_server(
        ActualEncoder { fail: false },
        src,
        None,
        Default::default(),
        None,
    );
    assert_eq!(
        frame(&mut server).into_trailers().unwrap()[Status::GRPC_STATUS],
        "0"
    );
    assert!(matches!(poll(&mut server), Poll::Ready(None)));
    let (src, _, _) = source([], false);
    let mut client = EncodeBody::new_client(ActualEncoder { fail: false }, src, None, None);
    assert!(matches!(poll(&mut client), Poll::Ready(None)));
}

#[test]
fn size_limit_diagnostic_uses_two_original_wrappers_and_matches_legacy_wire_bytes() {
    const EXPECTED: &[u8] = b"Error,%20encoded%20message%20length%20too%20large:%20found%207%20bytes,%20the%20limit%20is:%202%20bytes";
    let funded = Funded::new(512, 4);
    let (src, polls, exits) = source(
        [
            Ok(Bytes::from_static(b"partial")),
            Ok(Bytes::from_static(b"must not escape")),
        ],
        false,
    );
    let mut bounded = EncodeBody::new_server_with_trailers(
        ActualEncoder { fail: false },
        src,
        None,
        Default::default(),
        Some(2),
        funded.map(),
    );
    let wrapper_bytes = HeaderFieldAllocationPool::allocation_capacity_bound(512, 4, 512).unwrap()
        - HeaderFieldAllocationPool::allocation_capacity_bound(512, 3, 512).unwrap();
    // All source/body/staging construction is outside this operation. Real
    // finish_encoding rejects after seven bytes are encoded, before DATA can
    // escape; only the raw formatted and percent-output wrappers may allocate.
    let (result, calls, bytes) = measure(1, || poll(&mut bounded));
    assert_eq!((calls, bytes), (2, 2 * wrapper_bytes));
    let trailers = match result {
        Poll::Ready(Some(Ok(frame))) => frame.into_trailers().unwrap(),
        other => panic!("expected bounded size-limit diagnostic: {other:?}"),
    };
    assert_eq!(trailers[Status::GRPC_STATUS], "11");
    assert_eq!(trailers[Status::GRPC_MESSAGE].as_bytes(), EXPECTED);
    assert_eq!(funded.fields.available_positions(), 3);
    let alias = no_allocation(|| trailers[Status::GRPC_MESSAGE].clone());
    assert_eq!(
        alias.as_bytes().as_ptr(),
        trailers[Status::GRPC_MESSAGE].as_bytes().as_ptr()
    );
    terminal(&mut bounded, &polls);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    drop((bounded, trailers));
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    let (budget, total) = funded.detach();
    held(&budget, total);
    assert_eq!(alias.as_bytes(), EXPECTED);
    drop(alias);
    drop(grant(&budget, total));

    // Preserve ordinary finish_encoding's code/text policy. Its independent
    // String/payload allocations are outside the bounded operation's ledger.
    let (src, polls, exits) = source(
        [
            Ok(Bytes::from_static(b"partial")),
            Ok(Bytes::from_static(b"must not escape")),
        ],
        false,
    );
    let mut legacy = EncodeBody::new_server(
        ActualEncoder { fail: false },
        src,
        None,
        Default::default(),
        Some(2),
    );
    let trailers = frame(&mut legacy).into_trailers().unwrap();
    assert_eq!(trailers[Status::GRPC_STATUS], "11");
    assert_eq!(trailers[Status::GRPC_MESSAGE].as_bytes(), EXPECTED);
    terminal(&mut legacy, &polls);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    drop((legacy, trailers));
    assert_eq!(exits.load(Ordering::SeqCst), 1);
}
