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

//! Public Tonic consumers of originally funded HTTP map scaffolds. The codec,
//! message/details payloads, mock service and body fixtures are separate owners;
//! this target makes no whole-connection or whole-Native allocation claim.

use std::collections::VecDeque;
use std::future::{Ready, ready};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes};
use hyper::body::{Body, Frame};
use hyper::http::header::HeaderMapAllocationPool;
use hyper::http::{HeaderMap, HeaderValue, Response, StatusCode, uri::PathAndQuery};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Status, Streaming};

struct Exit {
    exits: Arc<AtomicUsize>,
    _credit: ResultWriteCredit,
}
impl Drop for Exit {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::AcqRel);
    }
}

struct Funded {
    pool: Option<HeaderMapAllocationPool>,
    budget: Arc<ResultRetainedBudget>,
    grant: usize,
    exits: Arc<AtomicUsize>,
}
impl Funded {
    fn new(maps: usize, keys: usize, extra: usize) -> Self {
        let bound = HeaderMapAllocationPool::allocation_capacity_bound(maps, keys, extra).unwrap();
        let grant = bound + Bytes::owner_with_exit_guard_metadata_size::<Bytes, Exit>();
        let budget = ResultRetainedBudget::new(NonZeroUsize::new(grant).unwrap());
        let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(grant).unwrap()
        else {
            panic!("original complete metadata family grant");
        };
        let exits = Arc::new(AtomicUsize::new(0));
        let owner = Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            Exit {
                exits: Arc::clone(&exits),
                _credit: credit,
            },
        );
        Self {
            pool: Some(HeaderMapAllocationPool::new(maps, keys, extra, owner).unwrap()),
            budget,
            grant,
            exits,
        }
    }
    fn pool(&self) -> &HeaderMapAllocationPool {
        self.pool.as_ref().unwrap()
    }
    fn map(&self) -> HeaderMap {
        HeaderMap::try_from_allocation_pool(self.pool()).unwrap()
    }
    fn release_handle(&mut self) {
        drop(self.pool.take());
    }
    fn held(&self) {
        assert_eq!(self.exits.load(Ordering::Acquire), 0);
        assert!(matches!(
            self.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    fn finish(mut self) {
        self.release_handle();
        assert_eq!(self.exits.load(Ordering::Acquire), 1);
        let ResultWriteAdmission::Granted(credit) =
            self.budget.try_reserve_process(self.grant).unwrap()
        else {
            panic!("all original metadata owners must physically exit");
        };
        drop(credit);
    }
}

fn insert(map: &mut HeaderMap, key: &'static str, value: &'static str) {
    map.try_insert(key, HeaderValue::from_static(value))
        .unwrap();
}
fn append(map: &mut HeaderMap, key: &'static str, value: &'static str) {
    map.try_append(key, HeaderValue::from_static(value))
        .unwrap();
}
fn values(map: &HeaderMap, key: &str) -> Vec<String> {
    map.get_all(key)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect()
}
fn metadata_values(map: &MetadataMap, key: &str) -> Vec<String> {
    map.get_all(key)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect()
}

#[test]
fn public_status_copy_claims_and_last_alias_holds_original_grant() {
    let mut funded = Funded::new(2, 2, 2);
    let mut headers = funded.map();
    let mut sensitive = HeaderValue::from_static("secret");
    sensitive.set_sensitive(true);
    headers.try_insert("authorization", sensitive).unwrap();
    append(&mut headers, "authorization", "second");
    let status = Status::with_metadata(Code::Aborted, "", MetadataMap::from_headers(headers));
    let copy = status.try_clone().unwrap();
    assert_eq!(copy.code(), Code::Aborted);
    assert!(copy.metadata().get("authorization").unwrap().is_sensitive());
    assert_eq!(
        metadata_values(copy.metadata(), "authorization"),
        ["secret", "second"]
    );
    assert_eq!(funded.pool().available_maps(), 0);
    assert_eq!(
        status.try_clone().unwrap_err().code(),
        Code::ResourceExhausted
    );
    funded.release_handle();
    drop(status);
    funded.held();
    drop(copy);
    funded.finish();
}

#[test]
fn public_status_parse_exhaustion_refuses_success_without_copying_original() {
    let funded = Funded::new(1, 2, 0);
    let mut headers = funded.map();
    insert(&mut headers, "grpc-status", "0");
    insert(&mut headers, "x-user", "original");
    let status = Status::from_header_map(&headers).unwrap();
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(headers["grpc-status"], "0");
    assert_eq!(headers["x-user"], "original");
    assert_eq!(funded.pool().available_maps(), 0);
    drop(status);
    drop(headers);
    funded.finish();
}

#[test]
fn invalid_status_details_returns_internal_and_retires_the_acquired_copy_position() {
    let funded = Funded::new(2, 2, 0);
    let mut headers = funded.map();
    insert(&mut headers, "grpc-status", "0");
    insert(&mut headers, "grpc-status-details-bin", "%%%invalid%%%");
    let status = Status::from_header_map(&headers).unwrap();
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(status.message(), "Invalid grpc-status-details-bin header");
    assert!(status.metadata().is_empty());
    assert_eq!(funded.pool().available_maps(), 1);
    assert_eq!(headers["grpc-status-details-bin"], "%%%invalid%%%");
    funded.held();
    drop(status);
    drop(headers);
    funded.finish();
}

#[test]
fn borrowed_status_add_replaces_duplicate_groups_without_extra_map_position() {
    let funded = Funded::new(2, 6, 4);
    let mut source = funded.map();
    insert(&mut source, "x-dup", "new-first");
    append(&mut source, "x-dup", "new-second");
    insert(&mut source, "grpc-message", "reserved-ignored");
    let status = Status::with_metadata(Code::Aborted, "", MetadataMap::from_headers(source));
    let mut target = funded.map();
    insert(&mut target, "x-dup", "old");
    append(&mut target, "x-dup", "old-extra");
    insert(&mut target, "x-stays", "kept");
    assert_eq!(funded.pool().available_maps(), 0);
    status.add_header(&mut target).unwrap();
    assert_eq!(values(&target, "x-dup"), ["new-first", "new-second"]);
    assert_eq!(target["x-stays"], "kept");
    assert_eq!(target["grpc-status"], "10");
    assert!(!target.contains_key("grpc-message"));
    assert_eq!(funded.pool().available_maps(), 0);
    drop(target);
    drop(status);
    funded.finish();
}

#[test]
fn status_add_into_ordinary_map_requires_original_copy_position_and_preserves_family() {
    let mut funded = Funded::new(2, 6, 2);
    let mut source = funded.map();
    insert(&mut source, "x-dup", "source");
    append(&mut source, "x-dup", "second");
    let status = Status::with_metadata(Code::Aborted, "", MetadataMap::from_headers(source));
    let mut target = HeaderMap::new();
    insert(&mut target, "x-dup", "discarded");
    insert(&mut target, "x-old", "kept");
    status.add_header(&mut target).unwrap();
    assert!(target.allocation_pool().is_some());
    assert_eq!(values(&target, "x-dup"), ["source", "second"]);
    assert_eq!(target["x-old"], "kept");
    assert_eq!(funded.pool().available_maps(), 0);
    funded.release_handle();
    drop(status);
    funded.held();
    drop(target);
    funded.finish();

    let funded = Funded::new(1, 2, 0);
    let mut source = funded.map();
    insert(&mut source, "x-user", "held");
    let status = Status::with_metadata(Code::Aborted, "", MetadataMap::from_headers(source));
    let mut target = HeaderMap::new();
    assert_eq!(
        status.add_header(&mut target).unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert!(target.is_empty());
    assert!(target.allocation_pool().is_none());
    drop(status);
    drop(target);
    funded.finish();
}

struct ByteCodec;
struct ByteEncoder;
struct ByteDecoder;
impl Codec for ByteCodec {
    type Encode = u8;
    type Decode = u8;
    type Encoder = ByteEncoder;
    type Decoder = ByteDecoder;
    fn encoder(&mut self) -> Self::Encoder {
        ByteEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        ByteDecoder
    }
}
impl Encoder for ByteEncoder {
    type Item = u8;
    type Error = Status;
    fn encode(&mut self, item: u8, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_u8(item);
        Ok(())
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(16, 16)
    }
}
impl Decoder for ByteDecoder {
    type Item = u8;
    type Error = Status;
    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<u8>, Status> {
        if src.remaining() != 1 {
            return Err(Status::internal("expected one byte fixture message"));
        }
        Ok(Some(src.get_u8()))
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(16, 16)
    }
}

struct Frames {
    frames: VecDeque<Result<Frame<Bytes>, Status>>,
}
impl Frames {
    fn new(frames: impl IntoIterator<Item = Result<Frame<Bytes>, Status>>) -> Self {
        Self {
            frames: frames.into_iter().collect(),
        }
    }
}
impl Body for Frames {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        Poll::Ready(self.frames.pop_front())
    }
}
struct Service {
    response: Option<Response<Frames>>,
    calls: Arc<AtomicUsize>,
}
impl tonic::client::GrpcService<tonic::body::BoxBody> for Service {
    type ResponseBody = Frames;
    type Error = Status;
    type Future = Ready<Result<Response<Frames>, Status>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: hyper::http::Request<tonic::body::BoxBody>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::AcqRel);
        ready(Ok(self
            .response
            .take()
            .expect("one actual Grpc service call")))
    }
}
fn client(response: Option<Response<Frames>>) -> (tonic::client::Grpc<Service>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (
        tonic::client::Grpc::new(Service {
            response,
            calls: Arc::clone(&calls),
        }),
        calls,
    )
}
fn response(headers: HeaderMap, frames: Frames) -> Response<Frames> {
    let mut response = Response::new(frames);
    *response.headers_mut() = headers;
    response
}
fn data() -> Frame<Bytes> {
    Frame::data(Bytes::from_static(&[0, 0, 0, 0, 1, 7]))
}
fn path() -> PathAndQuery {
    PathAndQuery::from_static("/fixture.Capacity/Unary")
}

#[tokio::test]
async fn grpc_protocol_header_shortage_is_returned_before_actual_service_call() {
    let funded = Funded::new(1, 1, 0);
    let mut headers = funded.map();
    insert(&mut headers, "x-user", "full");
    let mut request = Request::new(1_u8);
    *request.metadata_mut() = MetadataMap::from_headers(headers);
    let (mut grpc, calls) = client(None);
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        grpc.unary(request, path(), ByteCodec),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(funded.pool().available_maps(), 1);
    drop(error);
    funded.finish();
}

#[tokio::test]
async fn actual_grpc_unary_trailers_replace_groups_and_preserve_original_owner() {
    let mut funded = Funded::new(3, 6, 3);
    let mut initial = funded.map();
    insert(&mut initial, "x-dup", "old-first");
    append(&mut initial, "x-dup", "old-second");
    insert(&mut initial, "x-initial", "retained");
    let mut trailers = funded.map();
    insert(&mut trailers, "grpc-status", "0");
    insert(&mut trailers, "x-dup", "new-first");
    append(&mut trailers, "x-dup", "new-second");
    insert(&mut trailers, "x-trailer", "last");
    let (mut grpc, calls) = client(Some(response(
        initial,
        Frames::new([Ok(data()), Ok(Frame::trailers(trailers))]),
    )));
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        grpc.unary(Request::new(1_u8), path(), ByteCodec),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(*response.get_ref(), 7);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(
        metadata_values(response.metadata(), "x-dup"),
        ["new-first", "new-second"]
    );
    assert_eq!(response.metadata().get("x-initial").unwrap(), "retained");
    assert_eq!(response.metadata().get("x-trailer").unwrap(), "last");
    assert_eq!(funded.pool().available_maps(), 2);
    drop(grpc);
    funded.release_handle();
    funded.held();
    drop(response);
    funded.finish();
}

#[tokio::test]
async fn actual_grpc_unary_merge_shortage_returns_error_without_partial_response() {
    let funded = Funded::new(3, 2, 0);
    let mut initial = funded.map();
    insert(&mut initial, "x-a", "one");
    insert(&mut initial, "x-b", "two");
    let mut trailers = funded.map();
    insert(&mut trailers, "grpc-status", "0");
    insert(&mut trailers, "x-c", "three");
    let (mut grpc, calls) = client(Some(response(
        initial,
        Frames::new([Ok(data()), Ok(Frame::trailers(trailers))]),
    )));
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        grpc.unary(Request::new(1_u8), path(), ByteCodec),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert!(error.metadata().is_empty());
    assert_eq!(funded.pool().available_maps(), 3);
    drop(error);
    funded.finish();
}

#[tokio::test]
async fn actual_grpc_body_error_adopts_initial_funded_metadata_without_new_map() {
    let mut funded = Funded::new(1, 2, 1);
    let mut initial = funded.map();
    insert(&mut initial, "x-initial", "first");
    append(&mut initial, "x-initial", "second");
    let (mut grpc, _) = client(Some(response(
        initial,
        Frames::new([Err(Status::aborted("fixture body error"))]),
    )));
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        grpc.unary(Request::new(1_u8), path(), ByteCodec),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Code::Aborted);
    assert_eq!(
        metadata_values(error.metadata(), "x-initial"),
        ["first", "second"]
    );
    assert_eq!(funded.pool().available_maps(), 0);
    drop(grpc);
    funded.release_handle();
    funded.held();
    drop(error);
    funded.finish();
}

#[tokio::test]
async fn streaming_failed_partial_trailer_merge_is_terminal_and_cannot_escape_metadata() {
    // Multiple trailer frames exercise Tonic's public generic Body contract;
    // this fixture does not claim multiple trailers are valid on an H2 stream.
    let funded = Funded::new(2, 2, 0);
    let mut first = funded.map();
    insert(&mut first, "x-a", "first");
    let mut second = funded.map();
    insert(&mut second, "x-prefix", "fits-before-failure");
    insert(&mut second, "x-overflow", "must-not-publish");
    let mut stream = Streaming::new_response(
        ByteDecoder,
        Frames::new([Ok(Frame::trailers(first)), Ok(Frame::trailers(second))]),
        StatusCode::OK,
        None,
        Some(16),
    );
    assert_eq!(stream.message().await.unwrap(), None);
    assert_eq!(
        stream.message().await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(stream.message().await.unwrap(), None);
    assert!(stream.trailers().await.unwrap().is_none());
    assert_eq!(funded.pool().available_maps(), 2);
    drop(stream);
    funded.finish();
}

#[tokio::test]
async fn streaming_status_copy_shortage_emits_one_error_and_retires_original_metadata() {
    let funded = Funded::new(1, 2, 0);
    let mut metadata = funded.map();
    insert(&mut metadata, "x-original", "held");
    let status = Status::with_metadata(Code::Aborted, "", MetadataMap::from_headers(metadata));
    let mut stream = Streaming::new_response(
        ByteDecoder,
        Frames::new([Err(status)]),
        StatusCode::OK,
        None,
        Some(16),
    );
    assert_eq!(
        stream.message().await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(stream.message().await.unwrap(), None);
    assert!(stream.trailers().await.unwrap().is_none());
    assert_eq!(funded.pool().available_maps(), 1);
    drop(stream);
    funded.finish();
}

#[tokio::test]
async fn public_status_into_http_moves_original_metadata_without_a_copy_position() {
    let mut funded = Funded::new(1, 6, 2);
    let mut metadata = funded.map();
    insert(&mut metadata, "x-user", "first");
    append(&mut metadata, "x-user", "second");
    let status = Status::with_metadata(Code::Ok, "", MetadataMap::from_headers(metadata));
    let mut response = status.into_http();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["grpc-status"], "0");
    assert_eq!(response.headers()["content-type"], "application/grpc");
    assert_eq!(values(response.headers(), "x-user"), ["first", "second"]);
    assert!(response.headers().allocation_pool().is_some());
    assert_eq!(funded.pool().available_maps(), 0);
    assert!(
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
            .await
            .is_none()
    );
    funded.release_handle();
    funded.held();
    drop(response);
    funded.finish();
}

#[tokio::test]
async fn public_status_into_http_shortage_retains_empty_original_map_and_errors_once() {
    let mut funded = Funded::new(1, 1, 0);
    let mut metadata = funded.map();
    insert(&mut metadata, "x-user", "full");
    let status = Status::with_metadata(Code::Ok, "", MetadataMap::from_headers(metadata));
    let mut response = status.into_http();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().is_empty());
    assert!(response.headers().allocation_pool().is_some());
    assert_eq!(funded.pool().available_maps(), 0);
    funded.release_handle();
    funded.held();
    let error = std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
            .await
            .is_none()
    );
    drop(error);
    funded.held();
    drop(response);
    funded.finish();
}

struct UnaryService {
    response: Option<tonic::Response<u8>>,
    calls: Arc<AtomicUsize>,
}
impl tonic::server::UnaryService<u8> for UnaryService {
    type Response = u8;
    type Future = Ready<Result<tonic::Response<u8>, Status>>;
    fn call(&mut self, request: Request<u8>) -> Self::Future {
        assert_eq!(*request.get_ref(), 7);
        self.calls.fetch_add(1, Ordering::AcqRel);
        ready(Ok(self.response.take().unwrap()))
    }
}

#[tokio::test]
async fn actual_server_unary_protocol_shortage_keeps_original_response_owner() {
    let mut funded = Funded::new(1, 1, 0);
    let mut metadata = funded.map();
    insert(&mut metadata, "x-user", "full");
    let mut service_response = tonic::Response::new(9_u8);
    *service_response.metadata_mut() = MetadataMap::from_headers(metadata);
    let calls = Arc::new(AtomicUsize::new(0));
    let service = UnaryService {
        response: Some(service_response),
        calls: Arc::clone(&calls),
    };
    let request = hyper::http::Request::new(Frames::new([Ok(data())]));
    let mut grpc = tonic::server::Grpc::new(ByteCodec);
    let mut response = tokio::time::timeout(Duration::from_secs(5), grpc.unary(service, request))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().is_empty());
    assert!(response.headers().allocation_pool().is_some());
    assert_eq!(funded.pool().available_maps(), 0);
    funded.release_handle();
    funded.held();
    let error = std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx))
            .await
            .is_none()
    );
    drop(error);
    funded.held();
    drop(response);
    funded.finish();
}
