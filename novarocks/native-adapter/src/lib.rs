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

//! Server-resolved Backend Native transport capability.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use novarocks_native_trust::{
    AutomaticTlsMaterial, NativeEndpointConnector, NativeIncomingAdapter, NativeTlsMaterial,
    NativeTrust,
};
use novarocks_task_codec::domain::ConfidentialTransport;
use novarocks_types::NativeEndpoint;
use tokio::runtime::Handle;
use tonic::transport::Channel;

#[derive(Clone, Debug)]
pub enum BackendNativeTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl BackendNativeTransport {
    pub const fn confidentiality(&self) -> ConfidentialTransport {
        match self {
            Self::Plaintext => ConfidentialTransport::Plaintext,
            Self::Automatic(_) | Self::Pem(_) => ConfidentialTransport::Confidential,
        }
    }
    pub fn connector_for(
        &self,
        endpoint: NativeEndpoint,
    ) -> Result<NativeEndpointConnector, String> {
        match self {
            Self::Plaintext => Ok(NativeEndpointConnector::plaintext(endpoint)),
            Self::Automatic(material) => NativeEndpointConnector::automatic(endpoint, material)
                .map_err(|error| format!("construct automatic native connector: {error}")),
            Self::Pem(material) => Ok(NativeEndpointConnector::pem(endpoint, material)),
        }
    }
    pub fn incoming_adapter(&self) -> NativeIncomingAdapter {
        match self {
            Self::Plaintext => NativeIncomingAdapter::plaintext(),
            Self::Automatic(material) => NativeIncomingAdapter::automatic(material),
            Self::Pem(material) => NativeIncomingAdapter::pem(material),
        }
    }
}

/// Server-materialized transport capability consumed by the Frontend role.
///
/// It contains no source configuration or filesystem path. The Server builds
/// it before role startup and Frontend uses it for every Native dial and the
/// report listener's incoming stream.
#[derive(Clone)]
pub enum FrontendNativeTransport {
    Plaintext,
    Automatic(AutomaticTlsMaterial),
    Pem(NativeTlsMaterial),
}

impl FrontendNativeTransport {
    pub fn plaintext() -> Self {
        Self::Plaintext
    }

    pub fn automatic(material: AutomaticTlsMaterial) -> Self {
        Self::Automatic(material)
    }

    pub fn pem(material: NativeTlsMaterial) -> Self {
        Self::Pem(material)
    }

    /// Whether this concrete role-local Native transport encrypts the wire.
    /// Confidential query-attempt lease material is admitted only through this
    /// capability, never through an untrusted protobuf claim.
    pub const fn permits_confidential_credential_leases(&self) -> bool {
        matches!(self, Self::Automatic(_) | Self::Pem(_))
    }

    pub fn connector_for(
        &self,
        endpoint: NativeEndpoint,
    ) -> Result<NativeEndpointConnector, String> {
        match self {
            Self::Plaintext => Ok(NativeEndpointConnector::plaintext(endpoint)),
            Self::Automatic(material) => NativeEndpointConnector::automatic(endpoint, material)
                .map_err(|error| {
                    format!("construct automatic Native TLS connector failed: {error}")
                }),
            Self::Pem(material) => Ok(NativeEndpointConnector::pem(endpoint, material)),
        }
    }

    pub fn incoming_adapter(&self) -> NativeIncomingAdapter {
        match self {
            Self::Plaintext => NativeIncomingAdapter::plaintext(),
            Self::Automatic(material) => NativeIncomingAdapter::automatic(material),
            Self::Pem(material) => NativeIncomingAdapter::pem(material),
        }
    }
}

#[derive(Clone)]
pub struct BackendDataRuntime {
    handle: Handle,
    native_trust: Arc<NativeTrust>,
    native_transport: BackendNativeTransport,
    channels: Arc<Mutex<HashMap<NativeEndpoint, Channel>>>,
}

impl BackendDataRuntime {
    pub fn new(
        handle: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: BackendNativeTransport,
    ) -> Self {
        Self {
            handle,
            native_trust,
            native_transport,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.handle.block_on(future))
        } else {
            self.handle.block_on(future)
        }
    }
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
    pub fn native_trust(&self) -> &Arc<NativeTrust> {
        &self.native_trust
    }
    pub fn native_transport(&self) -> &BackendNativeTransport {
        &self.native_transport
    }
    pub fn channels(&self) -> &Arc<Mutex<HashMap<NativeEndpoint, Channel>>> {
        &self.channels
    }
}
