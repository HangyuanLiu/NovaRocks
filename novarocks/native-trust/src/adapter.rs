// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::{
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

    pub async fn connect(&self) -> Result<BoxedNativeIo, NativeTrustFailureKind> {
        let stream = TcpStream::connect(self.endpoint.as_host_port())
            .await
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        match &self.client_tls {
            None => Ok(Box::new(stream)),
            Some(config) => {
                let server_name = ServerName::try_from(self.endpoint.host().to_owned())
                    .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
                let stream = TlsConnector::from(config.clone())
                    .connect(server_name, stream)
                    .await
                    .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
                Ok(Box::new(stream))
            }
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
