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

//! Actual opt-in Grpc unary receives its original response pair before decode.
//! Map/field backing and original carriers are measured and funded. Listener,
//! predecode lane, message/body/status/future allocations and full Native limits
//! are deliberately outside this target's evidence.

use bytes::{Buf, BufMut, Bytes};
use hyper::body::{Body, Frame};
use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderValue};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::future::{Future, Ready, ready};
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::MetadataMap;
use tonic::server::{Grpc, ResponseHeaderMaps, UnaryService};
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
    map_family: u8,
}
impl Funded {
    fn new(family: u8) -> Self {
        let map_bound = HeaderMapAllocationPool::allocation_capacity_bound(3, 4, 2).unwrap();
        let field_bound =
            HeaderFieldAllocationPool::allocation_capacity_bound(1024, 8, 512).unwrap();
        let total = map_bound + field_bound + 2 * carrier();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let field_owner = owner(&budget, field_bound + carrier(), family);
        let (fields, calls, requested) = measure(family, || {
            HeaderFieldAllocationPool::new(1024, 8, 512, field_owner).unwrap()
        });
        assert_eq!(calls, 3 + usize::from(cfg!(target_os = "macos")));
        assert!(requested <= field_bound);
        let map_owner = owner(&budget, map_bound + carrier(), family + 1);
        let (maps, calls, requested) = measure(family + 1, || {
            HeaderMapAllocationPool::new(3, 4, 2, map_owner).unwrap()
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
            map_family: family + 1,
        }
    }
    fn map(&self) -> HeaderMap {
        let (map, calls, _) = measure(self.map_family, || {
            HeaderMap::try_from_allocation_pool(&self.maps).unwrap()
        });
        assert_eq!(calls, 3);
        map
    }
    fn pair(&self) -> ResponseHeaderMaps {
        let initial = self.map();
        let trailers = self.map();
        no_allocation(|| ResponseHeaderMaps::new(initial, trailers)).unwrap()
    }
    fn detach(self) -> (Arc<ResultRetainedBudget>, usize) {
        let Self {
            maps,
            fields,
            budget,
            total,
            ..
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
struct RequestBody {
    data: Option<Bytes>,
    pending: bool,
    polls: Arc<AtomicUsize>,
    exits: Arc<AtomicUsize>,
}
impl Body for RequestBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
        }
    }
}
impl Drop for RequestBody {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
fn request_body(pending: bool) -> (RequestBody, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    (
        RequestBody {
            data: Some(Bytes::from_static(b"\0\0\0\0\x01q")),
            pending,
            polls: polls.clone(),
            exits: exits.clone(),
        },
        polls,
        exits,
    )
}
struct ActualCodec {
    fail_decode: bool,
    decoded: Arc<AtomicUsize>,
}
struct ActualDecoder {
    fail: bool,
    calls: Arc<AtomicUsize>,
}
struct ActualEncoder;
impl Codec for ActualCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = ActualEncoder;
    type Decoder = ActualDecoder;
    fn encoder(&mut self) -> ActualEncoder {
        ActualEncoder
    }
    fn decoder(&mut self) -> ActualDecoder {
        ActualDecoder {
            fail: self.fail_decode,
            calls: self.decoded.clone(),
        }
    }
}
impl Decoder for ActualDecoder {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            Err(Status::from_static(Code::DataLoss, "decode denied"))
        } else {
            Ok(Some(source.copy_to_bytes(source.remaining())))
        }
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(64, 64)
    }
}
impl Encoder for ActualEncoder {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, target: &mut EncodeBuf<'_>) -> Result<(), Status> {
        target.put_slice(&item);
        Ok(())
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(64, 64)
    }
}
struct Service {
    error: bool,
    move_request: bool,
    metadata: Option<MetadataMap>,
    calls: Arc<AtomicUsize>,
}
impl UnaryService<Bytes> for Service {
    type Response = Bytes;
    type Future = Ready<Result<tonic::Response<Bytes>, Status>>;
    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (incoming, _, message) = request.into_parts();
        if self.error {
            return ready(Err(Status::from_static(
                Code::InvalidArgument,
                "service denied",
            )));
        }
        let mut response = tonic::Response::new(message);
        *response.metadata_mut() = if self.move_request {
            incoming
        } else {
            self.metadata.take().unwrap_or_default()
        };
        ready(Ok(response))
    }
}
fn service(
    error: bool,
    move_request: bool,
    metadata: Option<MetadataMap>,
) -> (Service, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Service {
            error,
            move_request,
            metadata,
            calls: calls.clone(),
        },
        calls,
    )
}
fn grpc(fail_decode: bool) -> (Grpc<ActualCodec>, Arc<AtomicUsize>) {
    let decoded = Arc::new(AtomicUsize::new(0));
    (
        Grpc::new(ActualCodec {
            fail_decode,
            decoded: decoded.clone(),
        }),
        decoded,
    )
}
fn complete<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("deterministic complete body/service must not suspend"),
    }
}
fn next(body: &mut tonic::body::BoxBody) -> Option<Result<Frame<Bytes>, Status>> {
    match Pin::new(body).poll_frame(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(frame) => frame,
        Poll::Pending => panic!("deterministic response must not suspend"),
    }
}

#[test]
fn successful_unary_moves_source_headers_and_escapes_only_original_field_alias() {
    let funded = Funded::new(1);
    let pair = funded.pair();
    let mut input = funded.map();
    let field = funded
        .fields
        .try_fill::<std::convert::Infallible>(7, |bytes| {
            bytes.copy_from_slice(b"payload");
            Ok(())
        })
        .unwrap();
    let pointer = field.as_ptr();
    input
        .try_insert(
            hyper::http::header::HeaderName::from_static("x-source"),
            HeaderValue::from_maybe_shared(field).unwrap(),
        )
        .unwrap();
    assert_eq!(funded.maps.available_maps(), 0);
    let (body, _, exits) = request_body(false);
    let mut request = hyper::http::Request::new(body);
    *request.headers_mut() = input;
    let (service, calls) = service(false, true, None);
    let (mut grpc, decoded) = grpc(false);
    let response = complete(grpc.unary_with_response_headers(service, request, pair));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(decoded.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(
        funded.maps.available_maps(),
        1,
        "source map exited; only preclaimed pair remains"
    );
    assert!(
        response
            .headers()
            .field_allocation_pool()
            .unwrap()
            .same_pool(&funded.fields)
    );
    assert_eq!(response.headers()["content-type"], "application/grpc");
    assert_eq!(response.headers()["x-source"].as_bytes().as_ptr(), pointer);
    let alias = no_allocation(|| response.headers()["x-source"].clone());
    let (parts, mut body) = response.into_parts();
    assert_eq!(
        next(&mut body)
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .as_ref(),
        b"\0\0\0\0\x01q"
    );
    let trailers = next(&mut body).unwrap().unwrap().into_trailers().unwrap();
    assert_eq!(trailers[Status::GRPC_STATUS], "0");
    assert!(
        trailers
            .field_allocation_pool()
            .unwrap()
            .same_pool(&funded.fields)
    );
    assert!(next(&mut body).is_none());
    drop((parts, body, trailers, grpc));
    let (budget, total) = funded.detach();
    held(&budget, total);
    assert_eq!(alias.as_bytes(), b"payload");
    drop(alias);
    drop(grant(&budget, total));
}

#[test]
fn decode_and_service_errors_use_preclaimed_initial_map_and_original_transformed_fields() {
    for fail_decode in [false, true] {
        let funded = Funded::new(1);
        let pair = funded.pair();
        let (body, _, exits) = request_body(false);
        let (service, calls) = service(true, false, None);
        let (mut grpc, decoded) = grpc(fail_decode);
        let response = complete(grpc.unary_with_response_headers(
            service,
            hyper::http::Request::new(body),
            pair,
        ));
        assert_eq!(decoded.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!fail_decode));
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert_eq!(
            funded.maps.available_maps(),
            2,
            "unused trailer position is physically returned"
        );
        assert_eq!(
            response.headers()[Status::GRPC_STATUS],
            if fail_decode { "15" } else { "3" }
        );
        assert_eq!(
            response.headers()[Status::GRPC_MESSAGE].as_bytes(),
            if fail_decode {
                b"decode%20denied".as_slice()
            } else {
                b"service%20denied".as_slice()
            }
        );
        assert!(
            response
                .headers()
                .field_allocation_pool()
                .unwrap()
                .same_pool(&funded.fields)
        );
        assert_eq!(
            funded.fields.available_positions(),
            7,
            "successful percent transform checked out original extent"
        );
        let alias = response.headers()[Status::GRPC_MESSAGE].clone();
        drop((response, grpc));
        let (budget, total) = funded.detach();
        held(&budget, total);
        drop(alias);
        drop(grant(&budget, total));
    }
}

#[test]
fn invalid_response_pairs_decline_without_allocating_or_holding_wrong_original_families() {
    let first = Funded::new(1);
    let second = Funded::new(3);
    let left = first.map();
    let right = second.map();
    let error = no_allocation(|| ResponseHeaderMaps::new(left, right)).unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        error.message(),
        "response maps require the same original map and field families"
    );
    assert_eq!(first.maps.available_maps(), 3);
    assert_eq!(second.maps.available_maps(), 3);
    let left = first.map();
    let error = no_allocation(|| ResponseHeaderMaps::new(HeaderMap::new(), left)).unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    // A distinct originally granted map family can retain the SAME field
    // arena; comparing fields alone would incorrectly accept this pair.
    let map_bound = HeaderMapAllocationPool::allocation_capacity_bound(1, 4, 2).unwrap();
    let map_total = map_bound + carrier();
    let other_budget = ResultRetainedBudget::new(NonZeroUsize::new(map_total).unwrap());
    let other_owner = owner(&other_budget, map_total, 5);
    let (other_maps, calls, requested) = measure(5, || {
        HeaderMapAllocationPool::new(1, 4, 2, other_owner).unwrap()
    });
    assert_eq!(calls, 1);
    assert!(requested <= map_bound);
    other_maps
        .try_bind_connection_with_fields(&first.fields)
        .unwrap();
    let (right, calls, _) = measure(5, || {
        HeaderMap::try_from_allocation_pool(&other_maps).unwrap()
    });
    assert_eq!(calls, 3);
    assert!(
        right
            .field_allocation_pool()
            .unwrap()
            .same_pool(&first.fields)
    );
    let left = first.map();
    let error = no_allocation(|| ResponseHeaderMaps::new(left, right)).unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(first.maps.available_maps(), 3);
    assert_eq!(other_maps.available_maps(), 1);
    drop(other_maps);
    drop(grant(&other_budget, map_total));
    let (budget, total) = first.detach();
    drop(grant(&budget, total));
    let (budget, total) = second.detach();
    drop(grant(&budget, total));
}

#[test]
fn initial_merge_refusal_returns_one_http_error_and_retains_cleared_original_map() {
    let funded = Funded::new(1);
    let mut initial = funded.map();
    for name in ["a", "b", "c", "d", "e", "f"] {
        initial
            .try_insert(
                hyper::http::header::HeaderName::from_static(name),
                HeaderValue::from_static("old"),
            )
            .unwrap();
    }
    let pair = ResponseHeaderMaps::new(initial, funded.map()).unwrap();
    let mut response_metadata = MetadataMap::new();
    response_metadata.insert("x-overflow", "new".parse().unwrap());
    let (service, calls) = service(false, false, Some(response_metadata));
    let (body, _, exits) = request_body(false);
    let (mut grpc, _) = grpc(false);
    let response =
        complete(grpc.unary_with_response_headers(service, hyper::http::Request::new(body), pair));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(
        response.status(),
        hyper::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(
        response.headers().is_empty(),
        "no partially merged metadata is published"
    );
    assert!(
        response
            .headers()
            .field_allocation_pool()
            .unwrap()
            .same_pool(&funded.fields)
    );
    let (parts, mut body) = response.into_parts();
    let error = next(&mut body).unwrap().unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(next(&mut body).is_none());
    drop((error, body, grpc));
    let (budget, total) = funded.detach();
    held(&budget, total);
    drop(parts);
    drop(grant(&budget, total));
}

#[test]
fn pending_decode_cancellation_drops_both_preclaimed_maps_only_on_actual_future_exit() {
    let funded = Funded::new(1);
    let pair = funded.pair();
    let (body, polls, exits) = request_body(true);
    let (service, calls) = service(false, false, None);
    let (mut grpc, decoded) = grpc(false);
    let (budget, total) = funded.detach();
    let mut future =
        Box::pin(grpc.unary_with_response_headers(service, hyper::http::Request::new(body), pair));
    held(&budget, total);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(decoded.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    held(&budget, total);
    drop(future);
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    drop(grant(&budget, total));
}
