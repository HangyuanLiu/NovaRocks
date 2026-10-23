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

//! The original process row's non-Clone low-level connection and sender.
//! Producers, request custody and actual child joins remain in the original root.
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, Uri};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use novarocks_transport_io::OwnedIo;
use opendal::Buffer;

use super::HttpBufferBody;
use super::connector::{ConnectedHttpIo, NegotiatedHttpProtocol, RequestTargetForm};
use super::route::{HttpOrigin, HttpScheme, PreparedHttpRoute};
use crate::FileError;

impl From<RawHttpError> for HttpConnectionError {
    fn from(error: RawHttpError) -> Self {
        match error {
            RawHttpError::Hyper(error) => Self::Hyper(error),
            RawHttpError::H2(error) => Self::H2(error),
        }
    }
}

type WireIo = TokioIo<OwnedIo>;
type RawResponse = Result<Response<RawBody>, RawHttpError>;

#[path = "connection/h2_stream.rs"]
mod h2_stream;
pub(super) use h2_stream::{H2Upload, RawBody, RawHttpError, UploadFailure, UploadStep};
type H1Driver = http1::Connection<WireIo, HttpBufferBody>;
type H2Driver = h2::client::Connection<OwnedIo, Bytes>;

enum Sender {
    H1(http1::SendRequest<HttpBufferBody>),
    H2(h2::client::SendRequest<Bytes>),
}
enum Driver {
    H1(Pin<Box<H1Driver>>),
    H2(Pin<Box<H2Driver>>),
}

#[derive(Debug)]
pub(crate) enum HttpConnectionError {
    Closed,
    NotReady,
    MissingRequest,
    Invalid(FileError),
    Hyper(hyper::Error),
    H2(h2::Error),
}
impl fmt::Display for HttpConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("HTTP sender is closed"),
            Self::NotReady => f.write_str("HTTP sender has no observed ready permission"),
            Self::MissingRequest => f.write_str("HTTP request source is empty"),
            Self::Invalid(error) => fmt::Display::fmt(error, f),
            Self::Hyper(error) => fmt::Display::fmt(error, f),
            Self::H2(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl StdError for HttpConnectionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::Hyper(error) => Some(error),
            Self::H2(error) => Some(error),
            _ => None,
        }
    }
}

struct ConnectionRoute {
    target: HttpOrigin,
    proxy: Option<HttpOrigin>,
    protocol: NegotiatedHttpProtocol,
    form: RequestTargetForm,
}
impl ConnectionRoute {
    fn accepts(&self, route: &PreparedHttpRoute) -> bool {
        self.target == *route.target() && self.proxy.as_ref() == route.proxy()
    }
}

/// Keep in the one original fixed connection row. Sender drops before driver.
/// H2 request streams stay in the original R rows; this driver spawns no children.
#[must_use]
pub(crate) struct HttpConnection {
    sender: Option<Sender>,
    driver: Driver,
    route: ConnectionRoute,
    ready: bool,
    driver_complete: bool,
}
impl HttpConnection {
    /// Store this owned future in the original C row. Its exact result is
    /// installed before callbacks; the same root owns the resulting driver/IO.
    /// Neither H1 nor public h2 submits a child task or needs an executor.
    pub(crate) fn handshake(
        connected: ConnectedHttpIo,
        route: &PreparedHttpRoute,
    ) -> HttpHandshakeFuture {
        let selected = ConnectionRoute {
            target: route.target().clone(),
            proxy: route.proxy().cloned(),
            protocol: connected.negotiated_protocol,
            form: connected.request_target_form,
        };
        HttpHandshakeFuture {
            completed: false,
            inner: Box::pin(async move {
                let expected =
                    if selected.proxy.is_some() && selected.target.scheme() == HttpScheme::Http {
                        RequestTargetForm::Absolute
                    } else {
                        RequestTargetForm::Origin
                    };
                if selected.form != expected
                    || (selected.protocol == NegotiatedHttpProtocol::H2
                        && selected.form == RequestTargetForm::Absolute)
                {
                    return Err(HttpConnectionError::Invalid(FileError::invalid(
                        "connected HTTP wire form disagrees with its selected route",
                    )));
                }
                let (sender, driver) = match selected.protocol {
                    NegotiatedHttpProtocol::H1 => {
                        let (sender, driver) = http1::Builder::new()
                            .handshake(TokioIo::new(connected.io))
                            .await
                            .map_err(HttpConnectionError::Hyper)?;
                        (Sender::H1(sender), Driver::H1(Box::pin(driver)))
                    }
                    NegotiatedHttpProtocol::H2 => {
                        let (sender, driver) = h2_stream::builder()
                            .handshake::<_, Bytes>(connected.io)
                            .await
                            .map_err(HttpConnectionError::H2)?;
                        (Sender::H2(sender), Driver::H2(Box::pin(driver)))
                    }
                };
                Ok(Self {
                    sender: Some(sender),
                    driver,
                    route: selected,
                    ready: false,
                    driver_complete: false,
                })
            }),
        }
    }

    /// Poll only from the original root, outside its State lock. Ready is the
    /// exact driver result, not an IO/row-reclamation receipt.
    pub(crate) fn poll_connection(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), RawHttpError>> {
        assert!(
            !self.driver_complete,
            "HTTP connection polled after its result was observed"
        );
        let result = match &mut self.driver {
            Driver::H1(driver) => driver.as_mut().poll(cx).map_err(RawHttpError::Hyper),
            Driver::H2(driver) => driver.as_mut().poll(cx).map_err(RawHttpError::H2),
        };
        if result.is_ready() {
            self.driver_complete = true;
            self.ready = false;
        }
        // Keep the actual driver object in the stable row after Ready. The root
        // stores the verdict before actually dropping its original IO.
        result
    }
    pub(crate) fn is_h2(&self) -> bool {
        self.route.protocol == NegotiatedHttpProtocol::H2
    }
    pub(crate) fn driver_completed(&self) -> bool {
        self.driver_complete
    }

    pub(crate) fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), HttpConnectionError>> {
        if self.driver_complete {
            self.ready = false;
            return Poll::Ready(Err(HttpConnectionError::Closed));
        }
        let Some(sender) = &mut self.sender else {
            self.ready = false;
            return Poll::Ready(Err(HttpConnectionError::Closed));
        };
        let result = match sender {
            Sender::H1(sender) => sender.poll_ready(cx).map_err(HttpConnectionError::Hyper),
            Sender::H2(sender) => sender.poll_ready(cx).map_err(HttpConnectionError::H2),
        };
        self.ready = matches!(result, Poll::Ready(Ok(())));
        result
    }

    /// The original request row retains its source and real protocol objects.
    /// Normal refusal leaves source unchanged. A publication wake panic can
    /// leave a dispatched request without a returned future; the same C/R root
    /// retains custody and supervises actual protocol/IO destruction.
    /// Source=None is never a response result or an exit receipt.
    pub(crate) fn start_send_in<G: Send + Sync + 'static>(
        &mut self,
        source: &mut Option<Request<Buffer>>,
        route: &PreparedHttpRoute,
        custody: &Arc<G>,
        upload: &mut Option<H2Upload>,
    ) -> Result<HttpResponseFuture, HttpConnectionError> {
        if self.sender.is_none() || self.driver_complete {
            return Err(HttpConnectionError::Closed);
        }
        if !self.ready {
            return Err(HttpConnectionError::NotReady);
        }
        let request = source.as_ref().ok_or(HttpConnectionError::MissingRequest)?;
        let plan = self.plan_wire_request(request, route)?;
        let original_uri = request.uri().clone();
        let is_head = request.method() == Method::HEAD;
        if self.is_h2() && request.method() == Method::CONNECT && !request.body().is_empty() {
            return Err(HttpConnectionError::Invalid(FileError::invalid(
                "HTTP/2 CONNECT request with non-zero body is unsupported",
            )));
        }
        if self.is_h2() {
            h2_stream::validate_headers(request).map_err(HttpConnectionError::Invalid)?;
        }
        assert!(upload.is_none(), "original upload row already occupied");
        // Pre-publication edits borrow the stable source. An unwinding header
        // destructor cannot first move the body out of that source slot.
        plan.apply(source.as_mut().expect("validated HTTP source"));
        self.ready = false;
        let inner: Pin<Box<dyn Future<Output = RawResponse> + Send + 'static>> =
            match self.sender.as_mut().expect("validated HTTP sender") {
                Sender::H1(sender) => {
                    let request = source
                        .take()
                        .expect("validated HTTP source")
                        .map(|body| HttpBufferBody::retained(body, Arc::clone(custody)));
                    let future = sender.send_request(request);
                    Box::pin(async move {
                        future
                            .await
                            .map(|response| response.map(RawBody::from))
                            .map_err(RawHttpError::Hyper)
                    })
                }
                Sender::H2(sender) => {
                    // Install original body/G before the SDK can publish/wake.
                    // A synchronous unwind is NOT retried as a new request.
                    h2_stream::prepare_headers(source.as_mut().unwrap());
                    let (parts, body) = source.take().unwrap().into_parts();
                    let end = body.is_empty() && parts.method != Method::CONNECT;
                    // The original ready sender can itself own a pending-open
                    // stream ref. Keep that sender in this R too, not in C.
                    let replacement = sender.clone();
                    let request_sender = std::mem::replace(sender, replacement);
                    *upload = Some(H2Upload::prepared(
                        body,
                        Arc::clone(custody),
                        end,
                        request_sender,
                    ));
                    let future = upload
                        .as_mut()
                        .unwrap()
                        .publish(Request::from_parts(parts, ()), end)
                        .map_err(HttpConnectionError::H2)?;
                    Box::pin(async move {
                        future
                            .await
                            .map(|response| response.map(RawBody::from))
                            .map_err(RawHttpError::H2)
                    })
                }
            };
        Ok(HttpResponseFuture {
            inner,
            original_uri: Some(original_uri),
            is_head,
            completed: false,
        })
    }

    /// Stop publishing; continue polling the real driver and the original task owner.
    /// No destructor runs under a new lock or pretends that IO has exited.
    pub(crate) fn close_sender(&mut self) {
        self.ready = false;
        drop(self.sender.take());
    }

    fn plan_wire_request(
        &self,
        request: &Request<Buffer>,
        route: &PreparedHttpRoute,
    ) -> Result<WirePlan, HttpConnectionError> {
        let invalid = |message| HttpConnectionError::Invalid(FileError::invalid(message));
        if request.uri() != route.original_uri() {
            return Err(invalid("HTTP request differs from its frozen route URI"));
        }
        if !self.route.accepts(route) {
            return Err(invalid(
                "HTTP request route differs from its original connection",
            ));
        }
        let original = request.uri();
        let authority = original
            .authority()
            .ok_or_else(|| invalid("HTTP request authority is absent"))?;
        // URI authority was already parsed. Remove only userinfo from its wire
        // projection; origin Authorization remains the original request author's job.
        let authority = if authority.as_str().contains('@') {
            authority
                .as_str()
                .rsplit('@')
                .next()
                .expect("authority suffix")
                .parse::<http::uri::Authority>()
                .map_err(|_| invalid("HTTP wire authority is invalid"))?
        } else {
            authority.clone()
        };
        let mut parts = http::uri::Parts::default();
        parts.path_and_query = Some(
            original
                .path_and_query()
                .cloned()
                .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/")),
        );
        if self.route.protocol == NegotiatedHttpProtocol::H2
            || self.route.form == RequestTargetForm::Absolute
        {
            parts.scheme = original.scheme().cloned();
            parts.authority = Some(authority.clone());
        }
        let uri = Uri::from_parts(parts).map_err(|_| invalid("HTTP wire target is invalid"))?;
        let host = if request.headers().contains_key(http::header::HOST) {
            None
        } else {
            Some(
                HeaderValue::from_str(authority.as_str())
                    .map_err(|_| invalid("HTTP Host is invalid"))?,
            )
        };
        let forward_proxy = self.route.form == RequestTargetForm::Absolute;
        let proxy_auth = if forward_proxy
            && !request
                .headers()
                .contains_key(http::header::PROXY_AUTHORIZATION)
        {
            route.forward_proxy_authorization().cloned()
        } else {
            None
        };
        Ok(WirePlan {
            uri,
            host,
            forward_proxy,
            proxy_auth,
        })
    }
}

struct WirePlan {
    uri: Uri,
    host: Option<HeaderValue>,
    forward_proxy: bool,
    proxy_auth: Option<HeaderValue>,
}
impl WirePlan {
    fn apply(self, request: &mut Request<Buffer>) {
        *request.uri_mut() = self.uri;
        if let Some(host) = self.host {
            request.headers_mut().insert(http::header::HOST, host);
        }
        if !self.forward_proxy {
            request
                .headers_mut()
                .remove(http::header::PROXY_AUTHORIZATION);
        } else {
            if let Some(auth) = self.proxy_auth {
                request
                    .headers_mut()
                    .insert(http::header::PROXY_AUTHORIZATION, auth);
            }
            for (name, value) in request.headers_mut().iter_mut() {
                if name == &http::header::PROXY_AUTHORIZATION {
                    value.set_sensitive(true);
                }
            }
        }
    }
}

#[must_use]
pub(crate) struct HttpHandshakeFuture {
    inner:
        Pin<Box<dyn Future<Output = Result<HttpConnection, HttpConnectionError>> + Send + 'static>>,
    completed: bool,
}
impl Future for HttpHandshakeFuture {
    type Output = Result<HttpConnection, HttpConnectionError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !this.completed,
            "HTTP handshake polled after its result was observed"
        );
        let result = this.inner.as_mut().poll(cx);
        if result.is_ready() {
            this.completed = true;
        }
        // Completed future backing stays here until the original row really drops it.
        result
    }
}

/// A real headers/error result; Incoming still belongs to the original request row.
/// HEAD and semantic URI are carried even on a protocol error, without formatting it.
#[must_use]
pub(crate) struct HttpResponseOutcome {
    pub(crate) response: RawResponse,
    pub(crate) original_uri: Uri,
    pub(crate) is_head: bool,
}
#[must_use]
pub(crate) struct HttpResponseFuture {
    inner: Pin<Box<dyn Future<Output = RawResponse> + Send + 'static>>,
    original_uri: Option<Uri>,
    is_head: bool,
    completed: bool,
}
impl Future for HttpResponseFuture {
    type Output = HttpResponseOutcome;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !this.completed,
            "HTTP response polled after its result was observed"
        );
        match this.inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(response) => {
                this.completed = true;
                Poll::Ready(HttpResponseOutcome {
                    response,
                    original_uri: this
                        .original_uri
                        .take()
                        .expect("HTTP response URI retained"),
                    is_head: this.is_head,
                })
            }
        }
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
