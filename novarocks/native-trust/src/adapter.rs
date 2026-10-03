// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::{
    alloc::Layout,
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use hyper_util::rt::TokioIo;
use novarocks_types::NativeEndpoint;
use rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tower::Service;

use crate::{AutomaticTlsMaterial, NativeTlsMaterial, NativeTransportMode, NativeTrustFailureKind};

/// Object-safe Native IO for Tonic's `connect_with_connector` bridge. The
/// connector preserves the `NativeEndpoint` reference host for TLS name
/// verification; DNS resolution never becomes its identity key.
pub trait NativeIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> NativeIo for T {}
pub type BoxedNativeIo = Box<dyn NativeIo>;

/// The concrete Native transport Box produced by the connector or acceptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeIoDirection {
    Client,
    Server,
}

/// Requested layout of the actual concrete Native transport Box.
///
/// Obtain original capacity before calling the connector or acceptor that
/// constructs this Box. Automatic and PEM transports have the same Rust
/// layout. This covers only the outer concrete Box: socket/runtime state,
/// TLS configuration, cryptographic buffers and enclosing futures are separate.
pub fn native_io_box_layout(mode: NativeTransportMode, direction: NativeIoDirection) -> Layout {
    match (mode, direction) {
        (NativeTransportMode::Disabled, _) => Layout::new::<TcpStream>(),
        (NativeTransportMode::Automatic | NativeTransportMode::Pem, NativeIoDirection::Client) => {
            Layout::new::<tokio_rustls::client::TlsStream<TcpStream>>()
        }
        (NativeTransportMode::Automatic | NativeTransportMode::Pem, NativeIoDirection::Server) => {
            Layout::new::<tokio_rustls::server::TlsStream<TcpStream>>()
        }
    }
}

/// Native IO and the caller's original capacity for its actual concrete Box.
///
/// The wrapper allocates nothing. Its optional owner is retained until the
/// boxed transport has been destroyed and its Box allocation has exited,
/// including when the transport destructor unwinds. The original owner must
/// already cover that concrete layout and its own carrier before allocation;
/// retaining an unrelated Bytes value does not establish this funding proof.
/// There is deliberately no IO or owner extraction API.
pub struct OwnedNativeIo {
    io: Option<BoxedNativeIo>,
    owner: Option<Bytes>,
}

impl OwnedNativeIo {
    pub fn new(io: BoxedNativeIo, owner: Option<Bytes>) -> Self {
        Self {
            io: Some(io),
            owner,
        }
    }

    fn io_mut(&mut self) -> &mut (dyn NativeIo + 'static) {
        self.io.as_deref_mut().expect("owned Native IO is live")
    }
}

impl fmt::Debug for OwnedNativeIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedNativeIo")
            .field("has_original_owner", &self.owner.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedNativeIo {
    fn drop(&mut self) {
        // A local owner unwinds after drop(io), even if the boxed destructor
        // panics. Box drop glue deallocates its backing during that unwind.
        let owner = self.owner.take();
        drop(self.io.take());
        drop(owner);
    }
}

impl AsyncRead for OwnedNativeIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedNativeIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().io_mut()).poll_write(cx, bytes)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().io_mut()).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.io
            .as_deref()
            .expect("owned Native IO is live")
            .is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io_mut()).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io_mut()).poll_shutdown(cx)
    }
}

#[derive(Clone)]
pub struct NativeEndpointConnector {
    endpoint: NativeEndpoint,
    client_tls: Option<Arc<ClientConfig>>,
}

impl NativeEndpointConnector {
    pub fn plaintext(endpoint: NativeEndpoint) -> Self {
        Self {
            endpoint,
            client_tls: None,
        }
    }

    pub fn pem(endpoint: NativeEndpoint, material: &NativeTlsMaterial) -> Self {
        Self {
            endpoint,
            client_tls: Some(material.client_config()),
        }
    }

    pub fn automatic(
        endpoint: NativeEndpoint,
        material: &AutomaticTlsMaterial,
    ) -> Result<Self, NativeTrustFailureKind> {
        Ok(Self {
            client_tls: Some(material.client_config_for(&endpoint)?),
            endpoint,
        })
    }

    pub fn endpoint(&self) -> &NativeEndpoint {
        &self.endpoint
    }

    pub async fn connect(&self) -> Result<BoxedNativeIo, NativeConnectFailure> {
        // A refused or timed-out connect is the peer being gone, not this
        // deployment being misconfigured.
        let stream = TcpStream::connect(self.endpoint.as_host_port())
            .await
            .map_err(NativeConnectFailure::Unreachable)?;
        match &self.client_tls {
            None => Ok(Box::new(stream)),
            Some(config) => {
                let server_name = ServerName::try_from(self.endpoint.host().to_owned())
                    .map_err(|_| NativeConnectFailure::UnverifiableServerName)?;
                let stream = TlsConnector::from(config.clone())
                    .connect(server_name, stream)
                    .await
                    .map_err(NativeConnectFailure::Handshake)?;
                Ok(Box::new(stream))
            }
        }
    }
}

/// Why one attempt to reach a native endpoint produced no stream.
///
/// The classification alone is not enough to act on: "native peer is
/// unreachable" is true for a refused connection and for an exhausted
/// descriptor table, and those send an operator to opposite places. The
/// operating system already said which one it was, so this keeps that answer
/// instead of discarding it at the point that produced it.
///
/// The kept detail is an `io::Error` from `connect` or from the TLS record
/// layer. Neither carries key material, a token or a payload, so this does not
/// widen what a failure discloses.
#[derive(Debug)]
pub enum NativeConnectFailure {
    /// The TCP connection could not be established.
    Unreachable(io::Error),
    /// The endpoint's reference host is not a name TLS can verify against.
    /// This one really is configuration.
    UnverifiableServerName,
    /// The TCP connection formed but the TLS handshake did not complete.
    Handshake(io::Error),
}

impl NativeConnectFailure {
    /// The redacted trust vocabulary this failure answers to.
    pub fn kind(&self) -> NativeTrustFailureKind {
        match self {
            Self::Unreachable(_) => NativeTrustFailureKind::TransportUnreachable,
            Self::UnverifiableServerName => NativeTrustFailureKind::TransportConfiguration,
            Self::Handshake(_) => NativeTrustFailureKind::TransportHandshake,
        }
    }
}

impl fmt::Display for NativeConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable(error) => write!(formatter, "{}: {error}", self.kind()),
            Self::UnverifiableServerName => write!(formatter, "{}", self.kind()),
            Self::Handshake(error) => write!(formatter, "{}: {error}", self.kind()),
        }
    }
}

impl std::error::Error for NativeConnectFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unreachable(error) | Self::Handshake(error) => Some(error),
            Self::UnverifiableServerName => None,
        }
    }
}

impl Service<http::Uri> for NativeEndpointConnector {
    type Response = TokioIo<BoxedNativeIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let connector = self.clone();
        Box::pin(async move {
            connector
                .connect()
                .await
                .map(TokioIo::new)
                .map_err(|failure| {
                    io::Error::other(format!("native transport connector failed: {failure}"))
                })
        })
    }
}

#[derive(Clone)]
pub struct NativeIncomingAdapter {
    mode: NativeTransportMode,
    server_tls: Option<Arc<ServerConfig>>,
}

impl NativeIncomingAdapter {
    pub fn plaintext() -> Self {
        Self {
            mode: NativeTransportMode::Disabled,
            server_tls: None,
        }
    }
    pub fn pem(material: &NativeTlsMaterial) -> Self {
        Self {
            mode: NativeTransportMode::Pem,
            server_tls: Some(material.server_config()),
        }
    }
    pub fn automatic(material: &AutomaticTlsMaterial) -> Self {
        Self {
            mode: NativeTransportMode::Automatic,
            server_tls: Some(material.server_config()),
        }
    }
    pub fn mode(&self) -> NativeTransportMode {
        self.mode
    }

    pub async fn accept(&self, stream: TcpStream) -> Result<BoxedNativeIo, NativeTrustFailureKind> {
        match &self.server_tls {
            None => Ok(Box::new(stream)),
            Some(config) => TlsAcceptor::from(config.clone())
                .accept(stream)
                .await
                .map(|stream| Box::new(stream) as BoxedNativeIo)
                .map_err(|_| NativeTrustFailureKind::TransportConfiguration),
        }
    }
}
