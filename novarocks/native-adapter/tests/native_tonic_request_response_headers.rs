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

//! Actual default Grpc entrypoints preclaim responses from installed request capability.
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
use tonic::server::{
    ClientStreamingService, Grpc, ServerStreamingService, StreamingService, UnaryService,
};
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
    fn new(family: u8, map_positions: usize) -> Self {
        let map_bound =
            HeaderMapAllocationPool::allocation_capacity_bound(map_positions, 4, 2).unwrap();
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
            HeaderMapAllocationPool::new(map_positions, 4, 2, map_owner).unwrap()
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
    maps: Option<HeaderMapAllocationPool>,
}
impl Body for RequestBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if let Some(maps) = &self.maps {
            assert_eq!(
                maps.available_maps(),
                0,
                "both response positions are claimed before body poll"
            );
        }
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
            maps: None,
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
    constructed: Arc<AtomicUsize>,
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
        self.constructed.fetch_add(1, Ordering::SeqCst);
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
type Results = tokio_stream::Once<Result<Bytes, Status>>;
type ServiceFuture<T> = Pin<Box<dyn Future<Output = Result<tonic::Response<T>, Status>> + Send>>;
struct Service {
    error: bool,
    calls: Arc<AtomicUsize>,
    maps: Option<HeaderMapAllocationPool>,
}
impl Service {
    fn called(&self) {
        if let Some(maps) = &self.maps {
            assert_eq!(
                maps.available_maps(),
                0,
                "preclaim precedes service invocation"
            );
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
#[expect(
    clippy::result_large_err,
    reason = "The fixture returns the exact Tonic service Status without allocating another Box."
)]
fn respond(
    metadata: MetadataMap,
    message: Bytes,
    error: bool,
) -> Result<tonic::Response<Bytes>, Status> {
    if error {
        return Err(Status::from_static(Code::InvalidArgument, "service denied"));
    }
    let mut response = tonic::Response::new(message);
    *response.metadata_mut() = metadata;
    Ok(response)
}
impl UnaryService<Bytes> for Service {
    type Response = Bytes;
    type Future = Ready<Result<tonic::Response<Bytes>, Status>>;
    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        self.called();
        let (metadata, _, message) = request.into_parts();
        ready(respond(metadata, message, self.error))
    }
}
impl ServerStreamingService<Bytes> for Service {
    type Response = Bytes;
    type ResponseStream = Results;
    type Future = Ready<Result<tonic::Response<Results>, Status>>;
    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        self.called();
        let (metadata, _, message) = request.into_parts();
        ready(
            respond(metadata, message, self.error)
                .map(|r| r.map(|item| tokio_stream::once(Ok(item)))),
        )
    }
}
async fn receive_stream(
    request: tonic::Request<tonic::Streaming<Bytes>>,
    error: bool,
) -> Result<tonic::Response<Bytes>, Status> {
    let (metadata, _, mut stream) = request.into_parts();
    let message = stream.message().await?.unwrap();
    while stream.message().await?.is_some() {}
    drop(stream);
    respond(metadata, message, error)
}
impl ClientStreamingService<Bytes> for Service {
    type Response = Bytes;
    type Future = ServiceFuture<Bytes>;
    fn call(&mut self, request: tonic::Request<tonic::Streaming<Bytes>>) -> Self::Future {
        self.called();
        Box::pin(receive_stream(request, self.error))
    }
}
impl StreamingService<Bytes> for Service {
    type Response = Bytes;
    type ResponseStream = Results;
    type Future = ServiceFuture<Results>;
    fn call(&mut self, request: tonic::Request<tonic::Streaming<Bytes>>) -> Self::Future {
        self.called();
        let error = self.error;
        Box::pin(async move {
            receive_stream(request, error)
                .await
                .map(|r| r.map(|item| tokio_stream::once(Ok(item))))
        })
    }
}
fn service(error: bool, maps: Option<HeaderMapAllocationPool>) -> (Service, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        Service {
            error,
            maps,
            calls: calls.clone(),
        },
        calls,
    )
}
fn grpc(fail_decode: bool) -> (Grpc<ActualCodec>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let decoded = Arc::new(AtomicUsize::new(0));
    let constructed = Arc::new(AtomicUsize::new(0));
    (
        Grpc::new(ActualCodec {
            fail_decode,
            decoded: decoded.clone(),
            constructed: constructed.clone(),
        }),
        decoded,
        constructed,
    )
}
#[derive(Copy, Clone)]
enum Shape {
    Unary,
    ServerStreaming,
    ClientStreaming,
    Streaming,
}
const SHAPES: [Shape; 4] = [
    Shape::Unary,
    Shape::ServerStreaming,
    Shape::ClientStreaming,
    Shape::Streaming,
];
async fn invoke(
    shape: Shape,
    grpc: &mut Grpc<ActualCodec>,
    service: Service,
    request: hyper::http::Request<RequestBody>,
) -> hyper::http::Response<tonic::body::BoxBody> {
    match shape {
        Shape::Unary => grpc.unary(service, request).await,
        Shape::ServerStreaming => grpc.server_streaming(service, request).await,
        Shape::ClientStreaming => grpc.client_streaming(service, request).await,
        Shape::Streaming => grpc.streaming(service, request).await,
    }
}
fn complete<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("deterministic complete input/service must not suspend"),
    }
}
fn next(body: &mut tonic::body::BoxBody) -> Option<Result<Frame<Bytes>, Status>> {
    match Pin::new(body).poll_frame(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(frame) => frame,
        Poll::Pending => panic!("deterministic response must not suspend"),
    }
}
fn input(
    funded: &Funded,
    pending: bool,
) -> (
    hyper::http::Request<RequestBody>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let (mut body, polls, exits) = request_body(pending);
    body.maps = Some(funded.maps.clone());
    let mut request = hyper::http::Request::new(body);
    *request.headers_mut() = funded.map();
    (request, polls, exits)
}

#[test]
fn all_four_service_shapes_preclaim_three_positions_and_retain_only_escaped_source_value() {
    for shape in SHAPES {
        let funded = Funded::new(1, 3);
        let (mut request, _, exits) = input(&funded, false);
        let field = funded
            .fields
            .try_fill::<std::convert::Infallible>(7, |bytes| {
                bytes.copy_from_slice(b"payload");
                Ok(())
            })
            .unwrap();
        let pointer = field.as_ptr();
        request
            .headers_mut()
            .try_insert(
                hyper::http::header::HeaderName::from_static("x-source"),
                HeaderValue::from_maybe_shared(field).unwrap(),
            )
            .unwrap();
        let (service, calls) = service(false, Some(funded.maps.clone()));
        let (mut grpc, decoded, constructed) = grpc(false);
        let response = complete(invoke(shape, &mut grpc, service, request));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decoded.load(Ordering::SeqCst), 1);
        assert_eq!(constructed.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert_eq!(
            funded.maps.available_maps(),
            1,
            "only response initial/trailer remain"
        );
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
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
        assert!(trailers.allocation_pool().unwrap().same_pool(&funded.maps));
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
        drop(alias);
        drop(grant(&budget, total));
    }
}

#[test]
fn decode_and_service_error_branches_preserve_original_initial_maps_in_all_shapes() {
    for shape in SHAPES {
        for fail_decode in [false, true] {
            let funded = Funded::new(1, 3);
            let (request, _, exits) = input(&funded, false);
            let (service, calls) = service(!fail_decode, Some(funded.maps.clone()));
            let (mut grpc, decoded, constructed) = grpc(fail_decode);
            let response = complete(invoke(shape, &mut grpc, service, request));
            assert_eq!(decoded.load(Ordering::SeqCst), 1);
            assert_eq!(constructed.load(Ordering::SeqCst), 1);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                usize::from(
                    !fail_decode || matches!(shape, Shape::ClientStreaming | Shape::Streaming)
                )
            );
            assert_eq!(exits.load(Ordering::SeqCst), 1);
            assert_eq!(
                response.headers()[Status::GRPC_STATUS],
                if fail_decode { "15" } else { "3" }
            );
            assert!(
                response
                    .headers()
                    .allocation_pool()
                    .unwrap()
                    .same_pool(&funded.maps)
            );
            assert!(
                response
                    .headers()
                    .field_allocation_pool()
                    .unwrap()
                    .same_pool(&funded.fields)
            );
            assert_eq!(
                funded.maps.available_maps(),
                2,
                "unused terminal map physically retired"
            );
            let alias = response.headers()[Status::GRPC_MESSAGE].clone();
            drop((response, grpc));
            let (budget, total) = funded.detach();
            held(&budget, total);
            drop(alias);
            drop(grant(&budget, total));
        }
    }
}

#[test]
fn immediate_unsupported_encoding_keeps_initial_family_without_any_body_or_service_poll() {
    for shape in SHAPES {
        let funded = Funded::new(1, 3);
        let (mut request, polls, exits) = input(&funded, false);
        request
            .headers_mut()
            .try_insert(
                hyper::http::header::HeaderName::from_static("grpc-encoding"),
                HeaderValue::from_static("unrecognized"),
            )
            .unwrap();
        let (service, calls) = service(false, Some(funded.maps.clone()));
        let (mut grpc, decoded, constructed) = grpc(false);
        let response = complete(invoke(shape, &mut grpc, service, request));
        assert_eq!(response.headers()[Status::GRPC_STATUS], "12");
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(decoded.load(Ordering::SeqCst), 0);
        assert_eq!(constructed.load(Ordering::SeqCst), 0);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        drop((response, grpc));
        let (budget, total) = funded.detach();
        drop(grant(&budget, total));
    }
}

#[test]
fn insufficient_response_positions_refuse_before_decode_service_or_body_poll() {
    for shape in SHAPES {
        for positions in [1, 2] {
            let funded = Funded::new(1, positions);
            let (mut request, polls, exits) = input(&funded, false);
            request
                .headers_mut()
                .try_insert(
                    hyper::http::header::HeaderName::from_static("x-discard"),
                    HeaderValue::from_static("clear me"),
                )
                .unwrap();
            let (service, calls) = service(false, Some(funded.maps.clone()));
            let (mut grpc, decoded, constructed) = grpc(false);
            let response = complete(invoke(shape, &mut grpc, service, request));
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(decoded.load(Ordering::SeqCst), 0);
            assert_eq!(constructed.load(Ordering::SeqCst), 0);
            assert_eq!(exits.load(Ordering::SeqCst), 1);
            assert!(
                response
                    .headers()
                    .allocation_pool()
                    .unwrap()
                    .same_pool(&funded.maps)
            );
            assert!(!response.headers().contains_key("x-discard"));
            assert_eq!(response.status(), hyper::http::StatusCode::OK);
            assert_eq!(response.headers()[Status::GRPC_STATUS], "8");
            assert_eq!(
                response.headers()[Status::GRPC_MESSAGE].as_bytes(),
                b"HTTP%20response%20header%20capacity%20exhausted"
            );
            assert_eq!(
                funded.maps.available_maps(),
                positions - 1,
                "partial pair claims were rolled back"
            );
            let (budget, total) = funded.detach();
            held(&budget, total);
            drop((response, grpc));
            drop(grant(&budget, total));
        }
    }
}

#[test]
fn pending_decode_cancel_retains_the_preclaimed_pair_until_actual_future_exit() {
    for shape in SHAPES {
        let funded = Funded::new(1, 3);
        let (request, polls, exits) = input(&funded, true);
        let (service, calls) = service(false, Some(funded.maps.clone()));
        let (mut grpc, decoded, constructed) = grpc(false);
        let mut future = Box::pin(invoke(shape, &mut grpc, service, request));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(funded.maps.available_maps(), 0);
        assert_eq!(decoded.load(Ordering::SeqCst), 0);
        assert_eq!(constructed.load(Ordering::SeqCst), 1);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(matches!(shape, Shape::ClientStreaming | Shape::Streaming))
        );
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        let (budget, total) = funded.detach();
        held(&budget, total);
        drop(future);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        drop(grpc);
        drop(grant(&budget, total));
    }
}

#[test]
fn ordinary_and_map_only_requests_keep_legacy_headers_and_terminal_maps() {
    for map_only in [false, true] {
        let bound = HeaderMapAllocationPool::allocation_capacity_bound(1, 4, 2).unwrap();
        let total = bound + carrier();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let original = owner(&budget, total, 2);
        let (maps, _, _) = measure(2, || {
            HeaderMapAllocationPool::new(1, 4, 2, original).unwrap()
        });
        maps.try_bind_connection().unwrap();
        let (body, _, exits) = request_body(false);
        let mut request = hyper::http::Request::new(body);
        if map_only {
            let (headers, _, _) =
                measure(2, || HeaderMap::try_from_allocation_pool(&maps).unwrap());
            *request.headers_mut() = headers;
        }
        assert!(request.headers().field_allocation_pool().is_none());
        let (service, calls) = service(false, None);
        let (mut grpc, _, _) = grpc(false);
        let response = complete(grpc.unary(service, request));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        assert!(response.headers().field_allocation_pool().is_none());
        let (parts, mut body) = response.into_parts();
        assert!(next(&mut body).unwrap().unwrap().is_data());
        let trailers = next(&mut body).unwrap().unwrap().into_trailers().unwrap();
        assert_eq!(trailers[Status::GRPC_STATUS], "0");
        assert!(trailers.allocation_pool().is_none());
        assert!(trailers.field_allocation_pool().is_none());
        assert!(next(&mut body).is_none());
        drop((parts, body, trailers, grpc, maps));
        drop(grant(&budget, total));
    }
}
