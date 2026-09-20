// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use hyper_util::rt::TokioIo;
use novarocks_types::NativeEndpoint;
use rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
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
