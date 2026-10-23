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

//! Public h2 glue stored and polled by the original fixed C/R rows.
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::super::HttpBufferBody;
use bytes::Bytes;
use http::{HeaderValue, Method, Request};
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use opendal::Buffer;

#[derive(Debug)]
pub(crate) enum RawHttpError {
    Hyper(hyper::Error),
    H2(h2::Error),
}
impl fmt::Display for RawHttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hyper(e) => fmt::Display::fmt(e, f),
            Self::H2(e) => fmt::Display::fmt(e, f),
        }
    }
}
impl std::error::Error for RawHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Hyper(e) => Some(e),
            Self::H2(e) => Some(e),
        }
    }
}

// Explicitly retain the original Hyper H2 defaults, not h2's different defaults.
pub(super) fn builder() -> h2::client::Builder {
    let mut builder = h2::client::Builder::new();
    builder
        .initial_window_size(2 * 1024 * 1024)
        .initial_connection_window_size(5 * 1024 * 1024)
        .max_frame_size(16 * 1024)
        .max_header_list_size(16 * 1024)
        .max_send_buffer_size(1024 * 1024)
        .initial_max_send_streams(100)
        .enable_push(false);
    builder
}

pub(super) fn validate_headers(request: &Request<Buffer>) -> Result<(), crate::FileError> {
    if let Some(connection) = request.headers().get(http::header::CONNECTION) {
        let names = connection.to_str().map_err(|_| {
            crate::FileError::invalid("HTTP/2 Connection header is not valid ASCII")
        })?;
        for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            http::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                crate::FileError::invalid("HTTP/2 Connection header names an invalid field")
            })?;
        }
    }
    Ok(())
}
pub(super) fn prepare_headers(request: &mut Request<Buffer>) {
    // Preserve the old Hyper H2 boundary's connection-header projection.
    let headers = request.headers_mut();
    for name in [
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
    if headers
        .get(http::header::TE)
        .is_some_and(|v| v != "trailers")
    {
        headers.remove(http::header::TE);
    }
    if let Some(connection) = headers.remove(http::header::CONNECTION) {
        let names = connection
            .to_str()
            .expect("validated original Connection header");
        for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            headers.remove(name);
        }
    }
    let len = request.body().len();
    if len != 0
        || !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::DELETE | Method::CONNECT
        )
    {
        request
            .headers_mut()
            .entry(http::header::CONTENT_LENGTH)
            .or_insert_with(|| HeaderValue::from(len));
    }
}

pub(crate) enum RawBody {
    H1(Incoming),
    H2 {
        stream: h2::RecvStream,
        data_done: bool,
        complete: bool,
    },
}
impl From<Incoming> for RawBody {
    fn from(body: Incoming) -> Self {
        Self::H1(body)
    }
}
impl From<h2::RecvStream> for RawBody {
    fn from(stream: h2::RecvStream) -> Self {
        Self::H2 {
            stream,
            data_done: false,
            complete: false,
        }
    }
}
impl Body for RawBody {
    type Data = Bytes;
    type Error = RawHttpError;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, RawHttpError>>> {
        match self.get_mut() {
            Self::H1(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|v| v.map(|r| r.map_err(RawHttpError::Hyper))),
            Self::H2 {
                stream,
                data_done,
                complete,
            } => {
                if *complete {
                    return Poll::Ready(None);
                }
                if !*data_done {
                    match stream.poll_data(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(RawHttpError::H2(e))));
                        }
                        Poll::Ready(Some(Ok(data))) => {
                            // Protocol credit is independent of physical payload custody.
                            if let Err(e) = stream.flow_control().release_capacity(data.len()) {
                                return Poll::Ready(Some(Err(RawHttpError::H2(e))));
                            }
                            return Poll::Ready(Some(Ok(Frame::data(data))));
                        }
                        Poll::Ready(None) => *data_done = true,
                    }
                }
                match stream.poll_trailers(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(RawHttpError::H2(e)))),
                    Poll::Ready(Ok(trailers)) => {
                        *complete = true;
                        Poll::Ready(trailers.map(|t| Ok(Frame::trailers(t))))
                    }
                }
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        match self {
            Self::H1(b) => b.is_end_stream(),
            Self::H2 { complete, .. } => *complete,
        }
    }
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::H1(b) => b.size_hint(),
            Self::H2 { .. } => SizeHint::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UploadStep {
    Progress,
    Complete,
}
#[derive(Debug)]
pub(crate) enum UploadFailure {
    Protocol(h2::Error),
    Reset(h2::Reason),
    Closed,
}
impl From<h2::Error> for UploadFailure {
    fn from(error: h2::Error) -> Self {
        Self::Protocol(error)
    }
}

/// Installed in the original R before public send_request can wake.
/// Body/frame/stream drop before custody; emitted Bytes independently retain G.
pub(crate) struct H2Upload {
    body: Option<HttpBufferBody>,
    pending: Option<Bytes>,
    stream: Option<h2::SendStream<Bytes>>,
    sender: Option<h2::client::SendRequest<Bytes>>,
    end: bool,
    complete: bool,
    reset: bool,
    _custody: Arc<dyn Send + Sync>,
}
impl H2Upload {
    pub(super) fn prepared<G: Send + Sync + 'static>(
        body: Buffer,
        custody: Arc<G>,
        end: bool,
        sender: h2::client::SendRequest<Bytes>,
    ) -> Self {
        Self {
            body: Some(HttpBufferBody::retained(body, Arc::clone(&custody))),
            pending: None,
            stream: None,
            sender: Some(sender),
            end,
            complete: end,
            reset: false,
            _custody: custody,
        }
    }
    pub(super) fn publish(
        &mut self,
        request: Request<()>,
        end: bool,
    ) -> Result<h2::client::ResponseFuture, h2::Error> {
        assert!(self.stream.is_none(), "original H2 stream installed twice");
        let (response, stream) = self.sender.as_mut().unwrap().send_request(request, end)?;
        self.stream = Some(stream);
        Ok(response)
    }
    pub(crate) fn complete(&self) -> bool {
        self.complete
    }
    pub(crate) fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<UploadStep, UploadFailure>> {
        if self.reset {
            return Poll::Ready(Err(UploadFailure::Closed));
        }
        if self.complete {
            return Poll::Ready(Ok(UploadStep::Complete));
        }
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Pending;
        };
        match stream.poll_reset(cx) {
            Poll::Ready(Ok(h2::Reason::NO_ERROR)) => {
                // A server may finish a response early and decline the rest
                // of the upload with RST_STREAM(NO_ERROR). Keep response refs.
                self.complete = true;
                drop((self.body.take(), self.pending.take()));
                return Poll::Ready(Ok(UploadStep::Complete));
            }
            Poll::Ready(Ok(reason)) => return Poll::Ready(Err(UploadFailure::Reset(reason))),
            Poll::Ready(Err(error)) => return Poll::Ready(Err(UploadFailure::Protocol(error))),
            Poll::Pending => {}
        }
        if self.pending.is_none() {
            let body = self.body.as_mut().expect("original upload body retained");
            match Pin::new(&mut *body).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(never))) => match never {},
                Poll::Ready(Some(Ok(frame))) => {
                    self.pending = Some(frame.into_data().ok().expect("Buffer body emits DATA"));
                    self.end = body.is_end_stream();
                    // At most one body/capacity/send action per root turn.
                    return Poll::Ready(Ok(UploadStep::Progress));
                }
                Poll::Ready(None) => {
                    self.pending = Some(Bytes::new());
                    self.end = true;
                }
            }
        }
        let data = self.pending.as_mut().unwrap();
        if data.is_empty() {
            let data = self.pending.take().unwrap();
            stream.send_data(data, self.end)?;
            self.complete = self.end;
            return Poll::Ready(Ok(if self.complete {
                UploadStep::Complete
            } else {
                UploadStep::Progress
            }));
        }
        // h2's public reserve_capacity casts usize to WindowSize. The same
        // configured 1MiB send buffer bounds each reservation without truncation.
        stream.reserve_capacity(data.len().min(1024 * 1024));
        let capacity = match stream.capacity() {
            0 => match stream.poll_capacity(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(UploadFailure::Protocol(e))),
                Poll::Ready(None) => return Poll::Ready(Err(UploadFailure::Closed)),
                Poll::Ready(Some(Ok(0))) => return Poll::Ready(Ok(UploadStep::Progress)),
                Poll::Ready(Some(Ok(n))) => n,
            },
            n => n,
        };
        let size = data.len().min(capacity);
        let bytes = data.split_to(size);
        let end = data.is_empty() && self.end;
        if data.is_empty() {
            drop(self.pending.take());
        }
        stream.send_data(bytes, end)?;
        self.complete = end;
        Poll::Ready(Ok(if end {
            UploadStep::Complete
        } else {
            UploadStep::Progress
        }))
    }
    /// No connection shutdown, no draining, no exit claim for queued Bytes.
    /// Caller stores the original failure before calling this operation.
    pub(crate) fn reset(&mut self) {
        self.reset = true;
        if let Some(stream) = self.stream.as_mut() {
            stream.send_reset(h2::Reason::CANCEL);
        }
        drop((
            self.body.take(),
            self.pending.take(),
            self.stream.take(),
            self.sender.take(),
        ));
    }
}
