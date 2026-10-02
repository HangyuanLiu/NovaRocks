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

//! Original response header storage for Native middleware refusals.
//! Body/future backing and pre-existing Status payloads remain caller-owned.

use hyper::body::{Body, Frame};
use hyper::http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::{Code, Status, body::BoxBody};

/// One fallback map claimed before the middleware starts asynchronous work.
/// Its existing map family also retains the original field capability.
#[derive(Debug)]
pub(crate) struct NativeResponseHeaders(Option<HeaderMap>);

impl NativeResponseHeaders {
    #[expect(
        clippy::result_large_err,
        reason = "Capacity refusal returns the original static Status without allocating another Box."
    )]
    pub(crate) fn prepare<B>(request: &Request<B>) -> Result<Self, Status> {
        if request.headers().field_allocation_pool().is_none() {
            return Ok(Self(None));
        }
        let pool = request.headers().allocation_pool().ok_or_else(|| {
            Status::from_static(Code::Internal, "missing original response map family")
        })?;
        HeaderMap::try_from_allocation_pool(pool)
            .map(|headers| Self(Some(headers)))
            .map_err(|_| {
                Status::from_static(
                    Code::ResourceExhausted,
                    "HTTP response header capacity exhausted",
                )
            })
    }

    pub(crate) fn respond(self, status: Status) -> Response<BoxBody> {
        match self.0 {
            Some(headers) => status.into_http_with_headers(headers),
            None => status.into_http(),
        }
    }

    /// Write Native's static rejection reason directly into original storage.
    /// Ordinary requests preserve their legacy Status metadata construction.
    pub(crate) fn respond_with_static_reason(
        self,
        mut status: Status,
        reason: &'static str,
    ) -> Response<BoxBody> {
        match self.0 {
            Some(mut headers) => {
                if headers
                    .try_insert(
                        "x-novarocks-ingress-rejection",
                        HeaderValue::from_static(reason),
                    )
                    .is_err()
                {
                    // Match Tonic's metadata-capacity failure: do not publish
                    // partial headers or recursively serialize another status.
                    headers.clear();
                    let body = BoxBody::new(HeaderCapacityFailureBody(Some(Status::from_static(
                        Code::ResourceExhausted,
                        "HTTP metadata capacity exhausted",
                    ))));
                    let mut response = Response::new(body);
                    *response.headers_mut() = headers;
                    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    return response;
                }
                status.into_http_with_headers(headers)
            }
            None => {
                status.metadata_mut().insert(
                    "x-novarocks-ingress-rejection",
                    tonic::metadata::MetadataValue::from_static(reason),
                );
                status.into_http()
            }
        }
    }
}

/// The single terminal body error has its normal caller-owned Box layout.
/// BoxBody::new avoids an additional boxed error-conversion layer on polling.
struct HeaderCapacityFailureBody(Option<Status>);

impl Body for HeaderCapacityFailureBody {
    type Data = bytes::Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.get_mut().0.take().map(Err))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
}

/// Refuse without polling the body or claiming another original map position.
/// Original request fields are cleared before using its storage for the status.
pub(crate) fn respond_from_request<B>(request: Request<B>, status: Status) -> Response<BoxBody> {
    let (mut parts, body) = request.into_parts();
    drop(body);
    if parts.headers.field_allocation_pool().is_some() {
        parts.headers.clear();
        status.into_http_with_headers(parts.headers)
    } else {
        status.into_http()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use hyper::body::{Body, Frame};
    use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
    use hyper::http::{HeaderValue, StatusCode};
    use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
    use novarocks_worker::result_buffer::ResultRetainedBudget;
    use std::convert::Infallible;
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    // These tests prove the actual owner graph and original credit lifetime.
    // Pool/System tests separately prove deallocation before the carrier exits;
    // this module installs neither a global allocator nor a new production wallet.
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

    fn held(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
        assert!(matches!(
            budget.try_reserve_process(bytes).unwrap(),
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
        fn new(positions: usize, keys: usize, attach_fields: bool) -> Self {
            let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, OriginalExit>();
            let field_bytes = HeaderFieldAllocationPool::allocation_capacity_bound(512, 4, 128)
                .unwrap()
                + carrier;
            let map_bytes = HeaderMapAllocationPool::allocation_capacity_bound(positions, keys, 2)
                .unwrap()
                + carrier;
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
            let fields =
                HeaderFieldAllocationPool::new(512, 4, 128, owner(field_bytes, 0)).unwrap();
            let maps =
                HeaderMapAllocationPool::new(positions, keys, 2, owner(map_bytes, 1)).unwrap();
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

        fn request<B>(&self, body: B) -> Request<B> {
            let mut request = Request::new(body);
            *request.headers_mut() = HeaderMap::try_from_allocation_pool(&self.maps).unwrap();
            request
        }

        fn value(&self, value: &[u8]) -> HeaderValue {
            let bytes = self
                .fields
                .try_fill(value.len(), |out| {
                    out.copy_from_slice(value);
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            HeaderValue::from_maybe_shared(bytes).unwrap()
        }
    }

    struct UnpolledBody {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Body for UnpolledBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            panic!("refused request body must not be polled")
        }
    }
    impl Drop for UnpolledBody {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn body() -> (UnpolledBody, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        (
            UnpolledBody {
                polls: polls.clone(),
                drops: drops.clone(),
            },
            polls,
            drops,
        )
    }

    fn error_once(response: &mut Response<BoxBody>) {
        let mut cx = Context::from_waker(Waker::noop());
        match Pin::new(response.body_mut()).poll_frame(&mut cx) {
            Poll::Ready(Some(Err(error))) => assert_eq!(error.code(), Code::ResourceExhausted),
            _ => panic!("one immediate capacity body error required"),
        }
        assert!(matches!(
            Pin::new(response.body_mut()).poll_frame(&mut cx),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn prepare_claims_one_spare_map_and_response_alias_keeps_original_fields() {
        let funded = Funded::new(2, 4, true);
        let request = funded.request(());
        assert_eq!(funded.maps.available_maps(), 1);
        let headers = NativeResponseHeaders::prepare(&request).unwrap();
        assert_eq!(funded.maps.available_maps(), 0);
        let response = headers.respond(Status::from_static(Code::Unavailable, "retry later"));
        assert_eq!(response.headers()[Status::GRPC_STATUS], "14");
        assert_eq!(response.headers()[Status::GRPC_MESSAGE], "retry%20later");
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        let alias = response.headers()[Status::GRPC_MESSAGE].clone();
        assert_eq!(
            alias.as_bytes().as_ptr(),
            response.headers()[Status::GRPC_MESSAGE].as_bytes().as_ptr()
        );
        let budget = funded.budget.clone();
        let exits = funded.exits.clone();
        let total = funded.total;
        drop(request);
        drop(funded);
        held(&budget, total);
        assert_eq!(exits[1].load(Ordering::SeqCst), 0);
        drop(response);
        assert_eq!(exits[1].load(Ordering::SeqCst), 1);
        assert_eq!(exits[0].load(Ordering::SeqCst), 0);
        held(&budget, total);
        drop(alias);
        assert_eq!(exits[0].load(Ordering::SeqCst), 1);
        drop(grant(&budget, total));
    }

    #[test]
    fn no_spare_map_refusal_reuses_request_and_drops_body_without_polling() {
        let funded = Funded::new(1, 4, true);
        let (body, polls, drops) = body();
        let mut request = funded.request(body);
        request
            .headers_mut()
            .insert("authorization", funded.value(b"private caller"));
        let status = NativeResponseHeaders::prepare(&request).unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), "HTTP response header capacity exhausted");
        let response = respond_from_request(request, status);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!response.headers().contains_key("authorization"));
        assert_eq!(response.headers()[Status::GRPC_STATUS], "8");
        assert_eq!(funded.maps.available_maps(), 0);
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        let budget = funded.budget.clone();
        let total = funded.total;
        drop(funded);
        held(&budget, total);
        drop(response);
        drop(grant(&budget, total));
    }

    #[test]
    fn unpolled_caller_future_retains_and_then_retires_both_original_maps() {
        let funded = Funded::new(2, 4, true);
        let (body, polls, drops) = body();
        let request = funded.request(body);
        let headers = NativeResponseHeaders::prepare(&request).unwrap();
        let caller = async move {
            drop(request);
            headers.respond(Status::from_static(Code::Cancelled, "cancelled"))
        };
        assert_eq!(funded.maps.available_maps(), 0);
        let budget = funded.budget.clone();
        let total = funded.total;
        drop(funded);
        held(&budget, total);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(caller);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(grant(&budget, total));
    }

    #[test]
    fn map_serialization_failure_keeps_cleared_original_response_until_drop() {
        let funded = Funded::new(1, 1, true);
        let mut request = funded.request(());
        request
            .headers_mut()
            .insert("authorization", funded.value(b"private"));
        let status = NativeResponseHeaders::prepare(&request).unwrap_err();
        let mut response = respond_from_request(request, status);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().is_empty());
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        error_once(&mut response);
        let budget = funded.budget.clone();
        let exits = funded.exits.clone();
        let total = funded.total;
        drop(funded);
        assert_eq!(exits[1].load(Ordering::SeqCst), 0);
        held(&budget, total);
        drop(response);
        assert_eq!(exits[1].load(Ordering::SeqCst), 1);
        drop(grant(&budget, total));
    }

    #[test]
    fn field_serialization_failure_preserves_map_and_independent_blocker_aliases() {
        let funded = Funded::new(2, 4, true);
        let request = funded.request(());
        let headers = NativeResponseHeaders::prepare(&request).unwrap();
        let blockers = std::array::from_fn::<_, 4, _>(|_| funded.value(&[b'x'; 128]));
        assert_eq!(funded.fields.available_positions(), 0);
        let mut response = headers.respond(Status::from_static(Code::Unavailable, "retry later"));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().is_empty());
        error_once(&mut response);
        let budget = funded.budget.clone();
        let exits = funded.exits.clone();
        let total = funded.total;
        drop(request);
        drop(funded);
        held(&budget, total);
        drop(response);
        assert_eq!(exits[1].load(Ordering::SeqCst), 1);
        assert_eq!(exits[0].load(Ordering::SeqCst), 0);
        held(&budget, total);
        drop(blockers);
        drop(grant(&budget, total));
    }

    #[test]
    fn ordinary_request_uses_legacy_response_and_drops_unpolled_body() {
        let request = Request::new(());
        let headers = NativeResponseHeaders::prepare(&request).unwrap();
        assert!(headers.0.is_none());
        let response = headers.respond(Status::from_static(Code::InvalidArgument, "legacy"));
        assert!(response.headers().allocation_pool().is_none());
        assert_eq!(response.headers()[Status::GRPC_STATUS], "3");
        let (body, polls, drops) = body();
        let mut request = Request::new(body);
        request
            .headers_mut()
            .insert("authorization", HeaderValue::from_static("private"));
        let response = respond_from_request(request, Status::from_static(Code::NotFound, "absent"));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(!response.headers().contains_key("authorization"));
        assert!(response.headers().allocation_pool().is_none());
        assert_eq!(response.headers()[Status::GRPC_STATUS], "5");
    }

    #[test]
    fn map_without_field_capability_does_not_claim_or_adopt_response_storage() {
        let funded = Funded::new(1, 4, false);
        let request = funded.request(());
        let headers = NativeResponseHeaders::prepare(&request).unwrap();
        assert!(headers.0.is_none());
        assert_eq!(funded.maps.available_maps(), 0);
        let response = respond_from_request(request, Status::from_static(Code::NotFound, "legacy"));
        assert!(response.headers().allocation_pool().is_none());
        assert_eq!(funded.maps.available_maps(), 1);
        drop(response);
        let budget = funded.budget.clone();
        let total = funded.total;
        drop(funded);
        drop(grant(&budget, total));
    }

    #[test]
    fn static_rejection_reason_uses_preclaimed_map_and_preserves_final_field_alias() {
        let funded = Funded::new(2, 4, true);
        let request = funded.request(());
        let headers = NativeResponseHeaders::prepare(&request).unwrap();
        let response = headers.respond_with_static_reason(
            Status::from_static(Code::ResourceExhausted, "body exceeds limit"),
            "body_limit",
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[Status::GRPC_STATUS].as_bytes(), b"8");
        assert_eq!(
            response.headers()["x-novarocks-ingress-rejection"].as_bytes(),
            b"body_limit"
        );
        assert_eq!(
            response.headers()[Status::GRPC_MESSAGE].as_bytes(),
            b"body%20exceeds%20limit"
        );
        assert_eq!(funded.maps.available_maps(), 0);
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        let alias = response.headers()[Status::GRPC_MESSAGE].clone();
        let budget = funded.budget.clone();
        let exits = funded.exits.clone();
        let total = funded.total;
        drop(request);
        drop(funded);
        held(&budget, total);
        drop(response);
        assert_eq!(exits[1].load(Ordering::SeqCst), 1);
        assert_eq!(exits[0].load(Ordering::SeqCst), 0);
        held(&budget, total);
        drop(alias);
        drop(grant(&budget, total));
    }

    #[test]
    fn rejection_reason_insert_failure_keeps_cleared_original_map_and_terminal_error() {
        let funded = Funded::new(2, 1, true);
        let request = funded.request(());
        let mut headers = NativeResponseHeaders::prepare(&request).unwrap();
        headers
            .0
            .as_mut()
            .unwrap()
            .insert("old", HeaderValue::from_static("private"));
        let mut response = headers.respond_with_static_reason(
            Status::from_static(Code::ResourceExhausted, "queue is full"),
            "queue_full",
        );
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().is_empty());
        assert!(
            response
                .headers()
                .allocation_pool()
                .unwrap()
                .same_pool(&funded.maps)
        );
        assert_eq!(funded.maps.available_maps(), 0);
        error_once(&mut response);
        assert!(response.body().is_end_stream());
        let budget = funded.budget.clone();
        let exits = funded.exits.clone();
        let total = funded.total;
        drop(request);
        drop(funded);
        held(&budget, total);
        assert_eq!(exits[1].load(Ordering::SeqCst), 0);
        drop(response);
        assert_eq!(exits[1].load(Ordering::SeqCst), 1);
        drop(grant(&budget, total));
    }

    #[test]
    fn static_rejection_reason_without_field_capability_keeps_legacy_metadata() {
        let request = Request::new(());
        let response = NativeResponseHeaders::prepare(&request)
            .unwrap()
            .respond_with_static_reason(
                Status::from_static(Code::ResourceExhausted, "queue is full"),
                "queue_full",
            );
        assert!(response.headers().allocation_pool().is_none());
        assert_eq!(response.headers()[Status::GRPC_STATUS].as_bytes(), b"8");
        assert_eq!(
            response.headers()["x-novarocks-ingress-rejection"].as_bytes(),
            b"queue_full"
        );
    }
}
