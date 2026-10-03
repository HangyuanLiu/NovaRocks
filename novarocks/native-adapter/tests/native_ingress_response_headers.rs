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

//! Actual public Native ingress preserves originally funded error response maps.
//! Exit guards prove carrier/credit lifetime; no global allocator or whole Native
//! connection claim is made. Existing pool probes verify physical backing order.

use axum::body::Body;
use bytes::{Buf, BufMut, Bytes};
use hyper::body::{Body as HttpBody, Frame};
use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderValue, Request, Response};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_native_adapter::native_ingress::NativeIngressService;
use novarocks_native_adapter::native_server::NativeIngressConfig;
use novarocks_proto_codec::native_rpc::{NativeEndpointDomain, NativeRpcMethod};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::future::{Future, Ready, ready};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tonic::Status;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::{Grpc, UnaryService};
use tower::Service;

struct OriginalExit {
    exits: Arc<[AtomicUsize; 2]>,
    family: usize,
    _credit: ResultWriteCredit,
}
impl Drop for OriginalExit {
    fn drop(&mut self) {
        self.exits[self.family].fetch_add(1, Ordering::SeqCst);
    }
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    match budget.try_reserve_process(bytes).unwrap() {
        ResultWriteAdmission::Granted(credit) => credit,
        ResultWriteAdmission::Blocked => panic!("complete original pregrant required"),
    }
}
fn held(budget: &Arc<ResultRetainedBudget>, total: usize) {
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
struct Funded {
    maps: HeaderMapAllocationPool,
    fields: HeaderFieldAllocationPool,
    budget: Arc<ResultRetainedBudget>,
    exits: Arc<[AtomicUsize; 2]>,
    total: usize,
}
impl Funded {
    fn new(positions: usize, attach_fields: bool) -> Self {
        let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, OriginalExit>();
        let field_bytes =
            HeaderFieldAllocationPool::allocation_capacity_bound(1024, 8, 256).unwrap() + carrier;
        let map_bytes =
            HeaderMapAllocationPool::allocation_capacity_bound(positions, 4, 2).unwrap() + carrier;
        let total = field_bytes + map_bytes;
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
        let exits = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let owner = |bytes, family| {
            Bytes::from_owner_with_exit_guard(
                Bytes::new(),
                OriginalExit {
                    exits: exits.clone(),
                    family,
                    _credit: grant(&budget, bytes),
                },
            )
        };
        let fields = HeaderFieldAllocationPool::new(1024, 8, 256, owner(field_bytes, 0)).unwrap();
        let maps = HeaderMapAllocationPool::new(positions, 4, 2, owner(map_bytes, 1)).unwrap();
        fields.try_bind_once().unwrap();
        if attach_fields {
            maps.try_bind_connection_with_fields(&fields).unwrap();
        } else {
            maps.try_bind_connection().unwrap();
        }
        Self {
            maps,
            fields,
            budget,
            exits,
            total,
        }
    }
    fn request(&self, body: Body) -> Request<Body> {
        let mut request = Request::builder()
            .uri(NativeRpcMethod::FetchTaskResult.contract().path)
            .body(body)
            .unwrap();
        *request.headers_mut() = HeaderMap::try_from_allocation_pool(&self.maps).unwrap();
        request
    }
    fn value(&self, value: &[u8]) -> HeaderValue {
        HeaderValue::from_maybe_shared(
            self.fields
                .try_fill::<Infallible>(value.len(), |out| {
                    out.copy_from_slice(value);
                    Ok(())
                })
                .unwrap(),
        )
        .unwrap()
    }
    fn detach(self) -> (Arc<ResultRetainedBudget>, Arc<[AtomicUsize; 2]>, usize) {
        let Self {
            maps,
            fields,
            budget,
            exits,
            total,
        } = self;
        drop((maps, fields));
        (budget, exits, total)
    }
}
struct ObservedBody {
    data: Option<Bytes>,
    polls: Arc<AtomicUsize>,
    exits: Arc<AtomicUsize>,
    all_claimed: Option<HeaderMapAllocationPool>,
}
impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(maps) = &self.all_claimed {
            assert_eq!(
                maps.available_maps(),
                0,
                "request + ingress fallback + Tonic pair claimed before decode"
            );
        }
        Poll::Ready(self.data.take().map(|bytes| Ok(Frame::data(bytes))))
    }
}
impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
fn body(
    all_claimed: Option<HeaderMapAllocationPool>,
) -> (Body, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    (
        Body::new(ObservedBody {
            data: Some(Bytes::from_static(b"\0\0\0\0\x01q")),
            polls: polls.clone(),
            exits: exits.clone(),
            all_claimed,
        }),
        polls,
        exits,
    )
}
#[derive(Clone, Copy)]
enum Mode {
    Pending,
    Ready,
    Tonic,
}
#[derive(Clone)]
struct Inner {
    mode: Mode,
    calls: Arc<AtomicUsize>,
    future_exits: Arc<AtomicUsize>,
}
struct CapturedPending {
    request: Option<Request<Body>>,
    exits: Arc<AtomicUsize>,
}
impl Future for CapturedPending {
    type Output = Result<Response<tonic::body::BoxBody>, Infallible>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for CapturedPending {
    fn drop(&mut self) {
        drop(self.request.take());
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
impl Service<Request<Body>> for Inner {
    type Response = Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            Mode::Pending => Box::pin(CapturedPending {
                request: Some(request),
                exits: self.future_exits.clone(),
            }),
            Mode::Ready => Box::pin(async move {
                let (parts, body) = request.into_parts();
                drop(body);
                let mut response = Response::new(tonic::body::empty_body());
                *response.headers_mut() = parts.headers;
                Ok(response)
            }),
            Mode::Tonic => {
                Box::pin(async move { Ok(Grpc::new(ActualCodec).unary(Echo, request).await) })
            }
        }
    }
}
fn inner(mode: Mode) -> (Inner, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    (
        Inner {
            mode,
            calls: calls.clone(),
            future_exits: exits.clone(),
        },
        calls,
        exits,
    )
}
fn config(running: usize, waiting: usize) -> NativeIngressConfig {
    NativeIngressConfig {
        ordinary_running: running,
        ordinary_waiting: waiting,
        ordinary_request_max_bytes: 8,
        ..NativeIngressConfig::default()
    }
}
fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn next(response: &mut Response<tonic::body::BoxBody>) -> Option<Result<Frame<Bytes>, Status>> {
    match Pin::new(response.body_mut()).poll_frame(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(frame) => frame,
        Poll::Pending => panic!("deterministic output must not suspend"),
    }
}
struct ActualCodec;
struct ActualEncoder;
struct ActualDecoder;
impl Codec for ActualCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = ActualEncoder;
    type Decoder = ActualDecoder;
    fn encoder(&mut self) -> ActualEncoder {
        ActualEncoder
    }
    fn decoder(&mut self) -> ActualDecoder {
        ActualDecoder
    }
}
impl Encoder for ActualEncoder {
    type Item = Bytes;
    type Error = Status;
    fn encode(&mut self, item: Bytes, out: &mut EncodeBuf<'_>) -> Result<(), Status> {
        out.put_slice(&item);
        Ok(())
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(64, 64)
    }
}
impl Decoder for ActualDecoder {
    type Item = Bytes;
    type Error = Status;
    fn decode(&mut self, input: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(input.copy_to_bytes(input.remaining())))
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(64, 64)
    }
}
struct Echo;
impl UnaryService<Bytes> for Echo {
    type Response = Bytes;
    type Future = Ready<Result<tonic::Response<Bytes>, Status>>;
    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let (metadata, _, message) = request.into_parts();
        let mut response = tonic::Response::new(message);
        *response.metadata_mut() = metadata;
        ready(Ok(response))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn spare_error_map_is_claimed_before_gate_wait_and_cancellation_physically_drops_request() {
    let funded = Funded::new(2, true);
    let (body, polls, drops) = body(None);
    let request = funded.request(body);
    let (inner, calls, _) = inner(Mode::Ready);
    let mut ingress = NativeIngressService::new(
        inner,
        config(0, 1),
        "Ignored",
        true,
        NativeEndpointDomain::BackendData,
    );
    let mut future = ingress.call(request);
    assert_eq!(
        funded.maps.available_maps(),
        0,
        "spare claimed synchronously before future poll"
    );
    assert!(poll(future.as_mut()).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let (budget, exits, total) = funded.detach();
    held(&budget, total);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(future);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(exits[1].load(Ordering::SeqCst), 1);
    drop(grant(&budget, total));
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_deadline_and_content_length_keep_original_response_family_without_body_poll() {
    for invalid_deadline in [true, false] {
        let funded = Funded::new(2, true);
        let (body, polls, drops) = body(None);
        let mut request = funded.request(body);
        if invalid_deadline {
            request
                .headers_mut()
                .try_insert(
                    hyper::http::header::HeaderName::from_static("grpc-timeout"),
                    HeaderValue::from_static("bad"),
                )
                .unwrap();
        } else {
            request
                .headers_mut()
                .try_insert(
                    hyper::http::header::CONTENT_LENGTH,
                    HeaderValue::from_static("14"),
                )
                .unwrap();
        }
        let (inner, calls, _) = inner(Mode::Ready);
        let mut ingress = NativeIngressService::new(
            inner,
            config(1, 0),
            "Ignored",
            true,
            NativeEndpointDomain::BackendData,
        );
        let response = ingress.call(request).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        assert_eq!(
            response.headers()[Status::GRPC_STATUS],
            if invalid_deadline { "3" } else { "8" }
        );
        if !invalid_deadline {
            assert_eq!(
                response.headers()["x-novarocks-ingress-rejection"],
                "body_limit"
            );
        }
        let alias = response.headers()[Status::GRPC_MESSAGE].clone();
        let (budget, exits, total) = funded.detach();
        drop(response);
        assert_eq!(exits[1].load(Ordering::SeqCst), 1);
        assert_eq!(exits[0].load(Ordering::SeqCst), 0);
        held(&budget, total);
        drop(alias);
        drop(grant(&budget, total));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_after_inner_owns_request_uses_preclaimed_map_and_original_transformed_field() {
    let funded = Funded::new(2, true);
    let (body, polls, drops) = body(None);
    let mut request = funded.request(body);
    request
        .headers_mut()
        .try_insert(
            hyper::http::header::HeaderName::from_static("grpc-timeout"),
            HeaderValue::from_static("200m"),
        )
        .unwrap();
    let (inner, calls, future_exits) = inner(Mode::Pending);
    let mut ingress = NativeIngressService::new(
        inner,
        config(1, 0),
        "Ignored",
        true,
        NativeEndpointDomain::BackendData,
    );
    let mut future = ingress.call(request);
    assert!(poll(future.as_mut()).is_pending());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "request has moved into actual pending inner future"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(funded.maps.available_maps(), 0);
    let response = tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(future_exits.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(response.headers()[Status::GRPC_STATUS], "4");
    assert_eq!(
        response.headers()[Status::GRPC_MESSAGE],
        "native%20ingress%20deadline%20elapsed"
    );
    assert!(
        response
            .headers()
            .allocation_pool()
            .unwrap()
            .same_pool(&funded.maps)
    );
    let alias = response.headers()[Status::GRPC_MESSAGE].clone();
    let (budget, exits, total) = funded.detach();
    drop(response);
    assert_eq!(exits[1].load(Ordering::SeqCst), 1);
    assert_eq!(exits[0].load(Ordering::SeqCst), 0);
    held(&budget, total);
    drop(alias);
    drop(grant(&budget, total));
}

#[tokio::test(flavor = "current_thread")]
async fn preparation_shortage_reuses_cleared_input_without_service_or_body_poll() {
    let funded = Funded::new(1, true);
    let (body, polls, drops) = body(None);
    let mut request = funded.request(body);
    request
        .headers_mut()
        .try_insert(
            hyper::http::header::HeaderName::from_static("x-discard"),
            funded.value(b"private input"),
        )
        .unwrap();
    let (inner, calls, _) = inner(Mode::Ready);
    let mut ingress = NativeIngressService::new(
        inner,
        config(1, 0),
        "Ignored",
        true,
        NativeEndpointDomain::BackendData,
    );
    let future = ingress.call(request);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "synchronous refusal already dropped input body"
    );
    let response = future.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(response.headers()[Status::GRPC_STATUS], "8");
    assert!(!response.headers().contains_key("x-discard"));
    assert!(
        response
            .headers()
            .allocation_pool()
            .unwrap()
            .same_pool(&funded.maps)
    );
    let (budget, _, total) = funded.detach();
    held(&budget, total);
    drop(response);
    drop(grant(&budget, total));
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_after_request_moves_into_inner_retires_both_maps_on_future_drop() {
    let funded = Funded::new(2, true);
    let (body, polls, drops) = body(None);
    let request = funded.request(body);
    let (inner, calls, future_exits) = inner(Mode::Pending);
    let mut ingress = NativeIngressService::new(
        inner,
        config(1, 0),
        "Ignored",
        true,
        NativeEndpointDomain::BackendData,
    );
    let mut future = ingress.call(request);
    assert!(poll(future.as_mut()).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (budget, exits, total) = funded.detach();
    held(&budget, total);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(future);
    assert_eq!(future_exits.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(exits[1].load(Ordering::SeqCst), 1);
    drop(grant(&budget, total));
}

#[tokio::test(flavor = "current_thread")]
async fn fields_none_and_frontend_bypass_keep_their_existing_response_paths() {
    let funded = Funded::new(1, false);
    let (payload, _, drops) = body(None);
    let mut request = funded.request(payload);
    request
        .headers_mut()
        .try_insert(
            hyper::http::header::HeaderName::from_static("grpc-timeout"),
            HeaderValue::from_static("bad"),
        )
        .unwrap();
    let (delegate, calls, _) = inner(Mode::Ready);
    let mut ingress = NativeIngressService::new(
        delegate,
        config(1, 0),
        "Ignored",
        true,
        NativeEndpointDomain::BackendData,
    );
    let response = ingress.call(request).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(response.headers()[Status::GRPC_STATUS], "3");
    assert!(response.headers().allocation_pool().is_none());
    drop(response);
    let (budget, _, total) = funded.detach();
    drop(grant(&budget, total));

    let funded = Funded::new(1, true);
    let (payload, polls, drops) = body(None);
    let mut request = funded.request(payload);
    request
        .headers_mut()
        .try_insert(
            hyper::http::header::HeaderName::from_static("grpc-timeout"),
            HeaderValue::from_static("bad"),
        )
        .unwrap();
    let (delegate, calls, _) = inner(Mode::Ready);
    *request.uri_mut() = NativeRpcMethod::AnnounceBackend
        .contract()
        .path
        .parse()
        .unwrap();
    let mut ingress = NativeIngressService::new(
        delegate,
        config(0, 0),
        "Ignored",
        false,
        NativeEndpointDomain::FrontendMembership,
    );
    let response = ingress.call(request).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "FE bypass ignores BE gates/deadline/error-map claim"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(response.headers()["grpc-timeout"], "bad");
    let (budget, _, total) = funded.detach();
    held(&budget, total);
    drop(response);
    drop(grant(&budget, total));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ingress_and_tonic_chain_needs_four_live_maps_before_decode() {
    let funded = Funded::new(4, true);
    let (body, _, drops) = body(Some(funded.maps.clone()));
    let mut request = funded.request(body);
    let value = funded.value(b"source field");
    let pointer = value.as_bytes().as_ptr();
    request
        .headers_mut()
        .try_insert(
            hyper::http::header::HeaderName::from_static("x-source"),
            value,
        )
        .unwrap();
    let (inner, calls, _) = inner(Mode::Tonic);
    let mut ingress = NativeIngressService::new(
        inner,
        config(1, 0),
        "Ignored",
        true,
        NativeEndpointDomain::BackendData,
    );
    let mut response = ingress.call(request).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(response.headers()["x-source"].as_bytes().as_ptr(), pointer);
    let alias = response.headers()["x-source"].clone();
    assert_eq!(
        next(&mut response)
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .as_ref(),
        b"\0\0\0\0\x01q"
    );
    let trailers = next(&mut response)
        .unwrap()
        .unwrap()
        .into_trailers()
        .unwrap();
    assert_eq!(trailers[Status::GRPC_STATUS], "0");
    assert!(trailers.allocation_pool().unwrap().same_pool(&funded.maps));
    assert!(next(&mut response).is_none());
    drop((response, trailers));
    let (budget, exits, total) = funded.detach();
    assert_eq!(exits[1].load(Ordering::SeqCst), 1);
    assert_eq!(exits[0].load(Ordering::SeqCst), 0);
    held(&budget, total);
    drop(alias);
    drop(grant(&budget, total));
}
