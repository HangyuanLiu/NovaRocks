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

//! Owned TCP/TLS/proxy connection establishment; no hidden spawned HTTP driver.
use std::future::{Future, poll_fn};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use http::{Method, Request};
use hyper_util::client::legacy::connect::{HttpConnector as TcpConnector, dns::Name};
use novarocks_transport_io::OwnedIo;
use rustls::{ClientConfig, pki_types::ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_rustls::TlsConnector;
use tower_service::Service;

use super::route::{HttpOrigin, HttpScheme, PreparedHttpRoute, ReferenceHost};
use crate::{FileError, FileErrorKind, FileResult};

/// Implemented by the original process resolver owner, not by this connector.
///
/// `resolve` must retain its original position inside the actual blocking closure until
/// that closure exits. Cancelling this future must not publish resolver exit. Returned
/// the immutable address list must retain the reference port; the connector checks this source contract.
/// Address storage and its original retention remain owned by the returned iterator until
/// it actually drops. Public HttpConnector then owns its separately paid collected vectors.
/// No resolver runtime, semaphore, quota, spawn or host aliasing is created here.
pub(crate) trait HttpDnsPort: Send + Sync + 'static {
    type Addresses: AsRef<[SocketAddr]> + Send + 'static;
    fn resolve<'a>(
        &'a self,
        origin: &'a HttpOrigin,
    ) -> impl Future<Output = FileResult<Self::Addresses>> + Send + 'a;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NegotiatedHttpProtocol {
    H1,
    H2,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestTargetForm {
    Origin,
    Absolute,
}

pub(crate) struct ConnectedHttpIo {
    pub(crate) io: OwnedIo,
    pub(crate) negotiated_protocol: NegotiatedHttpProtocol,
    pub(crate) request_target_form: RequestTargetForm,
}

/// Process-owned TLS configuration. The production route preserves the old native
/// system trust/hostname author; the explicit rustls route is separately configured.
pub(crate) struct HttpTlsBackend(TlsBackend);
enum TlsBackend {
    NativeH1(tokio_native_tls::TlsConnector),
    Rustls {
        origin: Arc<ClientConfig>,
        proxy: Arc<ClientConfig>,
    },
}
impl HttpTlsBackend {
    /// The exact current graph has no native-tls `alpn` feature and the old reqwest
    /// default selected native TLS. Keep that actual H1 behavior without inventing
    /// a negotiated protocol getter or substituting WebPKI for system trust.
    pub(crate) fn native_h1(verified_tls: native_tls::TlsConnector) -> Self {
        Self(TlsBackend::NativeH1(tokio_native_tls::TlsConnector::from(
            verified_tls,
        )))
    }
    /// Explicit backend, not a projection or replacement of native system trust.
    pub(crate) fn rustls(verified_tls: Arc<ClientConfig>) -> Self {
        let mut origin = (*verified_tls).clone();
        origin.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let mut proxy = (*verified_tls).clone();
        proxy.alpn_protocols = vec![b"http/1.1".to_vec()];
        Self(TlsBackend::Rustls {
            origin: Arc::new(origin),
            proxy: Arc::new(proxy),
        })
    }
}

pub(crate) struct HttpConnector<D> {
    dns: Arc<D>,
    tls: HttpTlsBackend,
}
impl<D: HttpDnsPort> HttpConnector<D> {
    pub(crate) fn new(dns: D, tls: HttpTlsBackend) -> Self {
        Self {
            dns: Arc::new(dns),
            tls,
        }
    }

    pub(crate) async fn connect<G: Send + Sync + 'static>(
        &self,
        route: &PreparedHttpRoute,
        guard: G,
    ) -> FileResult<ConnectedHttpIo> {
        let dial = route.proxy().unwrap_or_else(|| route.target());
        let guard = Arc::new(guard);
        let resolver = SelectedResolver {
            dns: Arc::clone(&self.dns),
            origin: dial.clone(),
        };
        // Reuse the original public Happy Eyeballs author, with no default DNS fallback.
        let mut tcp = TcpConnector::new_with_resolver(resolver);
        tcp.enforce_http(false);
        tcp.set_nodelay(true);
        tcp.set_keepalive(Some(Duration::from_secs(15)));
        tcp.set_keepalive_interval(Some(Duration::from_secs(15)));
        tcp.set_keepalive_retries(Some(3));
        tcp.set_connect_timeout(None);
        tcp.set_happy_eyeballs_timeout(Some(Duration::from_millis(300)));
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        tcp.set_tcp_user_timeout(Some(Duration::from_secs(30)));
        let dial_uri = dial.connector_uri()?;
        let connecting = async move {
            poll_fn(|cx| tcp.poll_ready(cx)).await?;
            tcp.call(dial_uri).await
        };
        // The whole future owns both racing families and their losers until actual drop.
        let (result, socket_guard) = RetainedFuture::new(connecting, Arc::clone(&guard))
            .finish()
            .await;
        let stream = result
            .map_err(|error| {
                let dns_failure = std::error::Error::source(&error)
                    .and_then(|source| source.downcast_ref::<FileError>());
                let kind = dns_failure.map_or(FileErrorKind::Transient, FileError::kind);
                let authority = dns_failure.and_then(FileError::authority_cause).cloned();
                let failure = FileError::with_source(kind, "connect HTTP socket", error);
                match authority {
                    Some(cause) => failure.with_authority_cause(cause),
                    None => failure,
                }
            })?
            .into_inner();
        let mut io = OwnedIo::with_guard(Box::new(stream), socket_guard);
        let mut protocol = NegotiatedHttpProtocol::H1;
        if route.proxy().is_some() {
            if dial.scheme() == HttpScheme::Https {
                (io, _) = tls(io, dial, &self.tls, true, Arc::clone(&guard)).await?;
            }
            if route.target().scheme() == HttpScheme::Https {
                let (result, tunnel_guard) = RetainedFuture::new(
                    connect_tunnel(io, route, Arc::clone(&guard)),
                    Arc::clone(&guard),
                )
                .finish()
                .await;
                io = result?;
                drop(tunnel_guard);
            }
        }
        if route.target().scheme() == HttpScheme::Https {
            (io, protocol) = tls(io, route.target(), &self.tls, false, Arc::clone(&guard)).await?;
        }
        let request_target_form =
            if route.proxy().is_some() && route.target().scheme() == HttpScheme::Http {
                RequestTargetForm::Absolute
            } else {
                RequestTargetForm::Origin
            };
        Ok(ConnectedHttpIo {
            io,
            negotiated_protocol: protocol,
            request_target_form,
        })
    }
}

// Public HttpConnector requires an owned, cloneable Service<Name>. This adapter
// carries exactly the selected origin and original resolver owner, not a second DNS policy.
struct SelectedResolver<D> {
    dns: Arc<D>,
    origin: HttpOrigin,
}
impl<D> Clone for SelectedResolver<D> {
    fn clone(&self) -> Self {
        Self {
            dns: Arc::clone(&self.dns),
            origin: self.origin.clone(),
        }
    }
}
struct OwnedAddresses<A> {
    backing: A,
    next: usize,
}
impl<A: AsRef<[SocketAddr]>> Iterator for OwnedAddresses<A> {
    type Item = SocketAddr;
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.backing.as_ref().get(self.next).copied();
        if result.is_some() {
            self.next += 1;
        }
        result
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.backing.as_ref().len().saturating_sub(self.next);
        (remaining, Some(remaining))
    }
}
impl<D: HttpDnsPort> Service<Name> for SelectedResolver<D> {
    type Response = OwnedAddresses<D::Addresses>;
    type Error = FileError;
    type Future = Pin<Box<dyn Future<Output = FileResult<Self::Response>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, name: Name) -> Self::Future {
        let dns = Arc::clone(&self.dns);
        let origin = self.origin.clone();
        Box::pin(async move {
            if !matches!(origin.host(), ReferenceHost::Dns(expected) if expected == name.as_str()) {
                return Err(FileError::new(
                    FileErrorKind::Internal,
                    "HTTP resolver changed the selected reference host",
                ));
            }
            let addresses = dns.resolve(&origin).await?;
            if addresses
                .as_ref()
                .iter()
                .any(|address| address.port() != origin.port())
            {
                return Err(FileError::new(
                    FileErrorKind::Internal,
                    "HTTP DNS port changed the reference port",
                ));
            }
            Ok(OwnedAddresses {
                backing: addresses,
                next: 0,
            })
        })
    }
}

const CONNECT_MAX_BUFFER_BYTES: usize = 8192;
// A legal header needs at least one name byte, ':', and CRLF. The status line and
// terminating CRLF further reduce the possible count inside the original byte bound.
const CONNECT_MAX_RESPONSE_HEADERS: usize = CONNECT_MAX_BUFFER_BYTES / 4;

// Parse only the response head with Hyper's locked public grammar author.
// Do not use Hyper's CONNECT upgrade decoder: its 204 branch is not an upgrade.
fn parse_connect_head(bytes: &[u8]) -> FileResult<Option<(usize, http::StatusCode)>> {
    let mut headers = [httparse::EMPTY_HEADER; CONNECT_MAX_RESPONSE_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    match response.parse(bytes) {
        Ok(httparse::Status::Partial) => Ok(None),
        Ok(httparse::Status::Complete(end)) => {
            let code = response.code.ok_or_else(|| {
                FileError::new(
                    FileErrorKind::Corrupt,
                    "HTTP CONNECT response has no status",
                )
            })?;
            let status = http::StatusCode::from_u16(code).map_err(|error| {
                FileError::with_source(FileErrorKind::Corrupt, "invalid HTTP CONNECT status", error)
            })?;
            Ok(Some((end, status)))
        }
        Err(httparse::Error::TooManyHeaders) => Err(FileError::with_source(
            FileErrorKind::ResourceExhausted,
            "HTTP CONNECT response exceeds the header budget",
            httparse::Error::TooManyHeaders,
        )),
        Err(error) => Err(FileError::with_source(
            FileErrorKind::Corrupt,
            "malformed HTTP CONNECT response head",
            error,
        )),
    }
}

fn connect_io_error(error: io::Error) -> FileError {
    FileError::with_source(FileErrorKind::Transient, "HTTP CONNECT I/O failed", error)
}

async fn connect_tunnel<G: Send + Sync + 'static>(
    mut io: OwnedIo,
    route: &PreparedHttpRoute,
    guard: Arc<G>,
) -> FileResult<OwnedIo> {
    let target = route.target().connector_uri()?;
    let authority = target
        .authority()
        .ok_or_else(|| FileError::invalid("CONNECT authority is absent"))?;
    let uri = http::Uri::builder()
        .authority(authority.clone())
        .build()
        .map_err(|error| {
            FileError::with_source(FileErrorKind::Invalid, "construct CONNECT authority", error)
        })?;
    let mut request = Request::builder()
        .method(Method::CONNECT)
        .version(http::Version::HTTP_11)
        .uri(uri)
        .header(http::header::HOST, authority.as_str())
        .body(())
        .map_err(|error| {
            FileError::with_source(FileErrorKind::Invalid, "construct HTTP CONNECT", error)
        })?;
    if let Some(auth) = route.proxy_authorization() {
        request
            .headers_mut()
            .insert(http::header::PROXY_AUTHORIZATION, auth.clone());
    }
    // Serialize only this typed, fixed CONNECT request; no object path/query or
    // origin Authorization is present. HeaderName/HeaderValue validate its fields.
    io.write_all(request.method().as_str().as_bytes())
        .await
        .map_err(connect_io_error)?;
    io.write_all(b" ").await.map_err(connect_io_error)?;
    io.write_all(authority.as_str().as_bytes())
        .await
        .map_err(connect_io_error)?;
    io.write_all(b" HTTP/1.1\r\n")
        .await
        .map_err(connect_io_error)?;
    for (name, value) in request.headers() {
        io.write_all(name.as_str().as_bytes())
            .await
            .map_err(connect_io_error)?;
        io.write_all(b": ").await.map_err(connect_io_error)?;
        io.write_all(value.as_bytes())
            .await
            .map_err(connect_io_error)?;
        io.write_all(b"\r\n").await.map_err(connect_io_error)?;
    }
    io.write_all(b"\r\n").await.map_err(connect_io_error)?;
    io.flush().await.map_err(connect_io_error)?;

    // One finite budget includes every interim and final head. No moving growth
    // or drain is needed; successful read-ahead remains in this original backing.
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(CONNECT_MAX_BUFFER_BYTES)
        .map_err(|error| {
            FileError::with_source(
                FileErrorKind::ResourceExhausted,
                "reserve HTTP CONNECT response buffer",
                error,
            )
        })?;
    buffer.resize(CONNECT_MAX_BUFFER_BYTES, 0);
    let mut filled = 0;
    let mut head_start = 0;
    loop {
        if let Some((head_len, status)) = parse_connect_head(&buffer[head_start..filled])? {
            head_start += head_len;
            if status.is_informational() && status != http::StatusCode::SWITCHING_PROTOCOLS {
                continue;
            }
            if !status.is_success() {
                // Do not read or drain any refusal body, including 407/101.
                return Err(FileError::new(
                    FileErrorKind::Transient,
                    format!("HTTP proxy refused CONNECT with status {}", status.as_u16()),
                ));
            }
            // RFC CONNECT semantics switch at the final 2xx head boundary,
            // including 204. Content-Length/Transfer-Encoding do not frame a
            // successful tunnel. Preserve every byte already read after it.
            buffer.truncate(filled);
            let mut prefix = Bytes::from(buffer);
            prefix.advance(head_start);
            return Ok(OwnedIo::with_guard(
                Box::new(PrefixIo { prefix, io }),
                guard,
            ));
        }
        if filled == CONNECT_MAX_BUFFER_BYTES {
            return Err(FileError::new(
                FileErrorKind::ResourceExhausted,
                "HTTP CONNECT response heads exceed the byte budget",
            ));
        }
        let read = io
            .read(&mut buffer[filled..])
            .await
            .map_err(connect_io_error)?;
        if read == 0 {
            return Err(connect_io_error(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "HTTP CONNECT response ended before a final head",
            )));
        }
        filled += read;
    }
}

struct PrefixIo {
    prefix: Bytes,
    io: OwnedIo,
}
impl AsyncRead for PrefixIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if !this.prefix.is_empty() {
            let count = this.prefix.len().min(buf.remaining());
            buf.put_slice(&this.prefix[..count]);
            this.prefix.advance(count);
            if this.prefix.is_empty() {
                this.prefix = Bytes::new();
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for PrefixIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, bytes)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write_vectored(cx, buffers)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

async fn tls<G: Send + Sync + 'static>(
    io: OwnedIo,
    origin: &HttpOrigin,
    backend: &HttpTlsBackend,
    proxy: bool,
    guard: Arc<G>,
) -> FileResult<(OwnedIo, NegotiatedHttpProtocol)> {
    match &backend.0 {
        TlsBackend::NativeH1(connector) => {
            let reference_host = match origin.host() {
                ReferenceHost::Dns(name) => name.clone(),
                ReferenceHost::Ip(address) => address.to_string(),
            };
            let (result, guard) =
                RetainedFuture::new(connector.connect(&reference_host, io), guard)
                    .finish()
                    .await;
            let stream = result.map_err(|error| {
                FileError::with_source(
                    FileErrorKind::Transient,
                    "HTTP native TLS handshake failed",
                    error,
                )
            })?;
            Ok((
                OwnedIo::with_guard(Box::new(stream), guard),
                NegotiatedHttpProtocol::H1,
            ))
        }
        TlsBackend::Rustls {
            origin: origin_config,
            proxy: proxy_config,
        } => {
            let server_name = match origin.host() {
                ReferenceHost::Dns(name) => ServerName::try_from(name.clone())
                    .map_err(|_| FileError::invalid("invalid HTTP TLS reference host"))?,
                ReferenceHost::Ip(address) => ServerName::IpAddress((*address).into()),
            };
            let config = if proxy {
                Arc::clone(proxy_config)
            } else {
                Arc::clone(origin_config)
            };
            let (result, guard) =
                RetainedFuture::new(TlsConnector::from(config).connect(server_name, io), guard)
                    .finish()
                    .await;
            let stream = result.map_err(|error| {
                FileError::with_source(
                    FileErrorKind::Transient,
                    "HTTP rustls handshake failed",
                    error,
                )
            })?;
            let protocol = match stream.get_ref().1.alpn_protocol() {
                None | Some(b"http/1.1") => NegotiatedHttpProtocol::H1,
                Some(b"h2") => NegotiatedHttpProtocol::H2,
                Some(_) => {
                    return Err(FileError::unsupported(
                        "HTTP TLS selected an unsupported ALPN protocol",
                    ));
                }
            };
            // The inner socket guard alone cannot cover the remaining session fields.
            Ok((OwnedIo::with_guard(Box::new(stream), guard), protocol))
        }
    }
}

// A handshake/CONNECT future can own TLS session state in addition to its inner IO.
// Its real destruction must precede releasing this alias of the original guard.
struct RetainedFuture<F, G> {
    future: Option<Pin<Box<F>>>,
    guard: Option<Arc<G>>,
}
impl<F: Future, G> RetainedFuture<F, G> {
    fn new(future: F, guard: Arc<G>) -> Self {
        Self {
            future: Some(Box::pin(future)),
            guard: Some(guard),
        }
    }
    async fn finish(mut self) -> (F::Output, Arc<G>) {
        let result = self
            .future
            .as_mut()
            .expect("retained future installed")
            .await;
        drop(self.future.take());
        (result, self.guard.take().expect("retained guard installed"))
    }
}
impl<F, G> Drop for RetainedFuture<F, G> {
    fn drop(&mut self) {
        let guard = self.guard.take();
        drop(self.future.take());
        drop(guard);
    }
}

#[cfg(test)]
#[path = "connector_tests.rs"]
mod tests;
