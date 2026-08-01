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

use std::sync::Arc;

use super::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorExecutionDeclaration,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorOpenReaderRequest,
    ConnectorProviderId, ConnectorRequestContext, ConnectorSplit, ConnectorWriteExecution,
};

/// Immutable identity shared across FE control and BE execution processes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExecutionBindingKey {
    pub instance_id: ConnectorInstanceId,
    pub incarnation: ConnectorInstanceIncarnation,
}

/// BE-only read capability. A provider implementation cannot perform metadata
/// lookup or split planning through this trait.
pub trait ConnectorReadExecution: Send + Sync {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey;

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError>;
}

/// Startup-composed BE execution binding. The provider ID is retained only to
/// validate installer output and for redacted diagnostics; it never travels in
/// a fragment carrier.
pub struct ConnectorExecutionBinding {
    provider_id: ConnectorProviderId,
    key: ConnectorExecutionBindingKey,
    read: Option<Arc<dyn ConnectorReadExecution>>,
    write: Option<Arc<dyn ConnectorWriteExecution>>,
}

impl ConnectorExecutionBinding {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        key: ConnectorExecutionBindingKey,
        read: Arc<dyn ConnectorReadExecution>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_capabilities(provider_id, key, Some(read), None)
    }

    pub fn try_new_capabilities(
        provider_id: ConnectorProviderId,
        key: ConnectorExecutionBindingKey,
        read: Option<Arc<dyn ConnectorReadExecution>>,
        write: Option<Arc<dyn ConnectorWriteExecution>>,
    ) -> Result<Self, ConnectorError> {
        if read.is_none() && write.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector execution binding requires at least one capability",
            ));
        }
        if read.as_ref().is_some_and(|read| read.binding_key() != &key) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector read execution owner does not match its execution binding",
            ));
        }
        if write
            .as_ref()
            .is_some_and(|write| write.binding_key() != &key)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write execution owner does not match its execution binding",
            ));
        }
        Ok(Self {
            provider_id,
            key,
            read,
            write,
        })
    }

    pub fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub fn key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    pub fn read(&self) -> Option<&Arc<dyn ConnectorReadExecution>> {
        self.read.as_ref()
    }

    pub fn write(&self) -> Option<&Arc<dyn ConnectorWriteExecution>> {
        self.write.as_ref()
    }
}

/// Startup-composed provider factory. Implementations use only local process
/// bindings for credentials and clients; declaration payloads are opaque,
/// bounded facts from the control plane.
pub trait ConnectorExecutionInstaller: Send + Sync {
    fn provider_id(&self) -> &ConnectorProviderId;

    fn install(
        &self,
        declaration: &ConnectorExecutionDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionBinding, ConnectorError>;
}

/// A resolver scoped to one admitted BE query. Generic native decode receives
/// only this interface and therefore cannot install or select providers.
pub trait ConnectorExecutionResolver: Send + Sync {
    fn resolve(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError>;
}
