// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  See the LICENSE file for details.

mod brpc;
mod codec;
mod flight;
mod http;

pub use brpc::StarRocksBrpcReader;
pub use codec::{StarRocksRpcOutputBinding, StarRocksRpcSplit};
pub(crate) use codec::{decode_rpc_split, encode_rpc_split};
pub use flight::{StarRocksArrowFlightClient, StarRocksArrowFlightStream, StarRocksFlightReader};
pub use http::{
    StarRocksHttpTransport, StarRocksRemoteControlClient, StarRocksRemoteControlConfig,
    StarRocksRemoteMetadataSource, StarRocksRemoteScanPlanner,
};

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorOpenReaderRequest, ConnectorRequestContext,
};

use crate::domain::{StarRocksRpcTransport, invalid, unavailable};
use crate::execution::StarRocksRpcReaderFactory;

#[derive(Clone, Eq, PartialEq)]
pub struct StarRocksRemoteEndpoint {
    host: Arc<str>,
    port: u16,
}

impl StarRocksRemoteEndpoint {
    pub fn try_new(host: impl Into<Arc<str>>, port: u16) -> Result<Self, ConnectorError> {
        let host = host.into();
        if host.is_empty()
            || port == 0
            || host.contains("://")
            || host.contains('@')
            || host.contains('/')
            || host.contains('?')
            || host.contains('#')
        {
            return Err(invalid("invalid StarRocks remote RPC endpoint"));
        }
        Ok(Self { host, port })
    }

    pub fn host(&self) -> &str {
        &self.host
    }
    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Debug for StarRocksRemoteEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksRemoteEndpoint")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

/// Startup-local port for a connector-native BRPC adapter. The adapter owns
/// networking and retry; connector code owns packet identity and decoding.
pub trait StarRocksBrpcTransport: Send + Sync {
    fn fetch(
        &self,
        endpoint: &StarRocksRemoteEndpoint,
        request: Bytes,
        context: &ConnectorRequestContext,
    ) -> Result<Bytes, ConnectorError>;
}

/// Startup-local RPC reader composition. A selected transport is never retried
/// through another transport or through direct read.
#[derive(Default)]
pub struct StarRocksRemoteRpcReaderFactory {
    brpc: Option<Arc<dyn StarRocksBrpcTransport>>,
    flight: Option<Arc<dyn StarRocksArrowFlightClient>>,
}

impl StarRocksRemoteRpcReaderFactory {
    pub fn new(
        brpc: Option<Arc<dyn StarRocksBrpcTransport>>,
        flight: Option<Arc<dyn StarRocksArrowFlightClient>>,
    ) -> Self {
        Self { brpc, flight }
    }
}

impl StarRocksRpcReaderFactory for StarRocksRemoteRpcReaderFactory {
    fn open_rpc_reader(
        &self,
        transport: StarRocksRpcTransport,
        split: StarRocksRpcSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        if split.transport() != transport {
            return Err(invalid(
                "StarRocks RPC split transport does not match the selected transport",
            ));
        }
        match transport {
            StarRocksRpcTransport::BrpcChunk => self
                .brpc
                .as_ref()
                .cloned()
                .map(|transport| {
                    Box::new(StarRocksBrpcReader::open(transport, split, request))
                        as Box<dyn ConnectorBatchReader>
                })
                .ok_or_else(|| unavailable("StarRocks BRPC transport is unavailable")),
            StarRocksRpcTransport::ArrowFlight => self
                .flight
                .as_ref()
                .cloned()
                .ok_or_else(|| unavailable("StarRocks Arrow Flight client is unavailable"))
                .and_then(|client| {
                    StarRocksFlightReader::open(client, split, request)
                        .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>)
                }),
        }
    }
}
