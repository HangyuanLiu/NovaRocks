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

//! Exact manifest admission before body, handler, gate, or spare-map admission.
//! Original pool exit guards cover their own backing/carrier lifetimes only.

use super::*;
use hyper::http::header::{HeaderFieldAllocationPool, HeaderMapAllocationPool};
use hyper::http::{HeaderMap, HeaderValue};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_proto_codec::native_rpc::NATIVE_METHODS;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::future::{Ready, ready};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct Observed {
    clones: AtomicUsize,
    calls: AtomicUsize,
    polls: AtomicUsize,
    exits: AtomicUsize,
}
struct UnreadBody(Arc<Observed>);
impl HttpBody for UnreadBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0.polls.fetch_add(1, Ordering::SeqCst);
        panic!("endpoint refusal and fixture handlers must not poll the body")
    }
}
impl Drop for UnreadBody {
    fn drop(&mut self) {
        self.0.exits.fetch_add(1, Ordering::SeqCst);
    }
}
struct Handler {
    observed: Arc<Observed>,
    forbidden: bool,
    expected_gate: Option<&'static str>,
}
impl Clone for Handler {
    fn clone(&self) -> Self {
        self.observed.clones.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.forbidden,
            "refused request must not clone the service"
        );
        Self {
            observed: self.observed.clone(),
            forbidden: false,
            expected_gate: self.expected_gate,
        }
    }
}
impl Service<Request<Body>> for Handler {
    type Response = Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        assert!(!self.forbidden, "refused request must not poll the handler");
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<Body>) -> Self::Future {
        assert!(
            !self.forbidden,
            "refused request must not dispatch a handler"
        );
        self.observed.calls.fetch_add(1, Ordering::SeqCst);
        let ownership = request.extensions().get::<Arc<NativeIngressOwnership>>();
        assert_eq!(
            ownership.map(|owner| owner._permit.class),
            self.expected_gate
        );
        drop(request);
        ready(Ok(Response::new(tonic::body::empty_body())))
    }
}
fn config(domain: NativeEndpointDomain) -> NativeIngressConfig {
    NativeIngressConfig {
        ordinary_running: usize::from(domain == NativeEndpointDomain::BackendData),
        ordinary_waiting: 0,
        control_running: usize::from(domain == NativeEndpointDomain::BackendControl),
        control_waiting: 0,
        ..NativeIngressConfig::default()
    }
}
fn request(path: &str, observed: &Arc<Observed>) -> Request<Body> {
    Request::builder()
        .uri(path)
        .body(Body::new(UnreadBody(observed.clone())))
        .unwrap()
}
fn forbidden(
    domain: NativeEndpointDomain,
    observed: &Arc<Observed>,
) -> NativeIngressService<Handler> {
    NativeIngressService::new(
        Handler {
            observed: observed.clone(),
            forbidden: true,
            expected_gate: None,
        },
        config(domain),
        "diagnostic-name-is-not-a-routing-prefix",
        false,
        domain,
    )
}

#[tokio::test]
async fn wrong_domain_and_retired_manifest_entries_refuse_before_body_handler_and_gates() {
    for domain in [
        NativeEndpointDomain::BackendData,
        NativeEndpointDomain::BackendControl,
        NativeEndpointDomain::FrontendMembership,
    ] {
        for contract in NATIVE_METHODS {
            if contract.method.is_allowed_at(domain) {
                continue;
            }
            let observed = Arc::new(Observed::default());
            let mut ingress = forbidden(domain, &observed);
            let mut input = request(contract.path, &observed);
            // Exact domain refusal wins over deadline/body-limit validation.
            input
                .headers_mut()
                .insert("grpc-timeout", HeaderValue::from_static("invalid"));
            input.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_static("18446744073709551615"),
            );
            let capacities = (
                ingress.ordinary.running.available_permits(),
                ingress.control.running.available_permits(),
            );
            let response = ingress.call(input);
            assert_eq!(
                observed.exits.load(Ordering::SeqCst),
                1,
                "body exits synchronously at refusal"
            );
            assert_eq!(observed.clones.load(Ordering::SeqCst), 0);
            assert_eq!(
                capacities,
                (
                    ingress.ordinary.running.available_permits(),
                    ingress.control.running.available_permits()
                )
            );
            assert_eq!(response.await.unwrap().headers()["grpc-status"], "12");
            assert_eq!(observed.calls.load(Ordering::SeqCst), 0);
            assert_eq!(observed.polls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn unknown_paths_and_service_name_aliases_refuse_before_frontend_bypass() {
    for domain in [
        NativeEndpointDomain::BackendData,
        NativeEndpointDomain::BackendControl,
        NativeEndpointDomain::FrontendMembership,
    ] {
        for path in [
            "/Other/Heartbeat",
            "/novarocks.NovaRocksGrpc/Unknown",
            "/Other/AnnounceBackend",
            "/",
        ] {
            let observed = Arc::new(Observed::default());
            let mut ingress = forbidden(domain, &observed);
            let response = ingress.call(request(path, &observed)).await.unwrap();
            assert_eq!(response.headers()["grpc-status"], "12");
            assert_eq!(observed.exits.load(Ordering::SeqCst), 1);
            assert_eq!(observed.polls.load(Ordering::SeqCst), 0);
            assert_eq!(observed.clones.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn every_allowed_manifest_method_uses_its_domain_even_without_metrics() {
    for domain in [
        NativeEndpointDomain::BackendData,
        NativeEndpointDomain::BackendControl,
        NativeEndpointDomain::FrontendMembership,
    ] {
        for contract in NATIVE_METHODS {
            if !contract.method.is_allowed_at(domain) {
                continue;
            }
            let observed = Arc::new(Observed::default());
            let mut ingress = NativeIngressService::new(
                Handler {
                    observed: observed.clone(),
                    forbidden: false,
                    expected_gate: match domain {
                        NativeEndpointDomain::BackendData => Some("ordinary"),
                        NativeEndpointDomain::BackendControl => Some("control"),
                        NativeEndpointDomain::FrontendMembership => None,
                    },
                },
                config(domain),
                "Ignored",
                false,
                domain,
            );
            let response = ingress
                .call(request(contract.path, &observed))
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
            assert_eq!(observed.polls.load(Ordering::SeqCst), 0);
            assert_eq!(observed.exits.load(Ordering::SeqCst), 1);
            drop(response);
            assert_eq!(
                ingress.ordinary.running.available_permits(),
                config(domain).ordinary_running
            );
            assert_eq!(
                ingress.control.running.available_permits(),
                config(domain).control_running
            );
        }
    }
}

#[tokio::test]
async fn heartbeat_uses_control_body_limit_and_never_ordinary_gate() {
    let observed = Arc::new(Observed::default());
    let mut limits = config(NativeEndpointDomain::BackendControl);
    limits.control_request_max_bytes = 1;
    limits.ordinary_request_max_bytes = 1024;
    let mut ingress = NativeIngressService::new(
        Handler {
            observed: observed.clone(),
            forbidden: false,
            expected_gate: Some("control"),
        },
        limits,
        "Ignored",
        false,
        NativeEndpointDomain::BackendControl,
    );
    let mut input = request(NativeRpcMethod::Heartbeat.contract().path, &observed);
    input
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("7"));
    let response = ingress.call(input).await.unwrap();
    assert_eq!(response.headers()["grpc-status"], "8");
    assert_eq!(observed.calls.load(Ordering::SeqCst), 0);
    assert_eq!(observed.polls.load(Ordering::SeqCst), 0);
    assert_eq!(ingress.control.running.available_permits(), 1);
}

struct OriginalExit {
    exits: Arc<AtomicUsize>,
    _credit: ResultWriteCredit,
}
impl Drop for OriginalExit {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}
fn grant(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    match budget.try_reserve_process(bytes).unwrap() {
        ResultWriteAdmission::Granted(credit) => credit,
        ResultWriteAdmission::Blocked => panic!("complete original grant required"),
    }
}

#[tokio::test]
async fn refusal_consumes_only_original_request_map_and_retains_credit_until_response_exit() {
    let carrier = Bytes::owner_with_exit_guard_metadata_size::<Bytes, OriginalExit>();
    let map_bytes = HeaderMapAllocationPool::allocation_capacity_bound(1, 4, 2).unwrap() + carrier;
    let field_bytes =
        HeaderFieldAllocationPool::allocation_capacity_bound(1024, 8, 256).unwrap() + carrier;
    let total = map_bytes + field_bytes;
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(total).unwrap());
    let exits = Arc::new(AtomicUsize::new(0));
    let owner = |bytes| {
        Bytes::from_owner_with_exit_guard(
            Bytes::new(),
            OriginalExit {
                exits: exits.clone(),
                _credit: grant(&budget, bytes),
            },
        )
    };
    let fields = HeaderFieldAllocationPool::new(1024, 8, 256, owner(field_bytes)).unwrap();
    fields.try_bind_once().unwrap();
    let maps = HeaderMapAllocationPool::new(1, 4, 2, owner(map_bytes)).unwrap();
    maps.try_bind_connection_with_fields(&fields).unwrap();
    let observed = Arc::new(Observed::default());
    let mut input = request(
        NativeRpcMethod::ApplyTaskOperations.contract().path,
        &observed,
    );
    *input.headers_mut() = HeaderMap::try_from_allocation_pool(&maps).unwrap();
    input
        .headers_mut()
        .try_insert("x-input", HeaderValue::from_static("clear-on-refusal"))
        .unwrap();
    let mut ingress = forbidden(NativeEndpointDomain::BackendControl, &observed);
    let outcome = ingress.call(input);
    assert_eq!(observed.exits.load(Ordering::SeqCst), 1);
    let response = outcome.await.unwrap();
    assert_eq!(response.headers()["grpc-status"], "12");
    assert!(!response.headers().contains_key("x-input"));
    assert!(
        response
            .headers()
            .allocation_pool()
            .unwrap()
            .same_pool(&maps)
    );
    assert_eq!(
        maps.available_maps(),
        0,
        "refusal never claims a spare position"
    );
    drop((fields, maps, ingress));
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert!(matches!(
        budget.try_reserve_process(total).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(response);
    assert_eq!(exits.load(Ordering::SeqCst), 2);
    drop(grant(&budget, total));
}
