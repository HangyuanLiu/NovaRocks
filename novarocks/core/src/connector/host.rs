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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorInstance, ConnectorInstanceId, ConnectorProviderId,
};

use crate::connector::file_execution::FileScanRange;
use crate::exec::chunk::ChunkSchemaRef;

/// Rehydrates a provider-owned reader instance from a native transport carrier.
///
/// The host selects this factory by the typed provider ID. The generic native
/// decoder never interprets `scan_payload`; core file sidecars remain a
/// separate core-owned input for file-backed providers.
pub(crate) trait ConnectorTransportFactory: Send + Sync {
    fn provider_id(&self) -> &ConnectorProviderId;

    fn materialize(
        &self,
        instance_id: ConnectorInstanceId,
        scan_payload: Bytes,
        file_ranges: &[FileScanRange],
        output_schema: ChunkSchemaRef,
    ) -> Result<ConnectorInstance, ConnectorError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectorHostErrorKind {
    DuplicateInstance,
    UnknownInstance,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectorHostError {
    kind: ConnectorHostErrorKind,
    message: String,
}

impl ConnectorHostError {
    pub(crate) fn kind(&self) -> ConnectorHostErrorKind {
        self.kind
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: ConnectorHostErrorKind::UnknownInstance,
            message: message.into(),
        }
    }
}

impl fmt::Display for ConnectorHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for ConnectorHostError {}

#[derive(Clone, Default)]
pub(crate) struct ConnectorHost {
    instances: BTreeMap<ConnectorInstanceId, Arc<ConnectorInstance>>,
    transport_factories: BTreeMap<ConnectorProviderId, Arc<dyn ConnectorTransportFactory>>,
}

impl ConnectorHost {
    pub(crate) fn register(
        &mut self,
        instance: ConnectorInstance,
    ) -> Result<(), ConnectorHostError> {
        let instance_id = instance.descriptor().instance_id.clone();
        if self.instances.contains_key(&instance_id) {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::DuplicateInstance,
                message: format!(
                    "connector instance `{}` is already registered",
                    instance_id.as_str()
                ),
            });
        }
        self.instances.insert(instance_id, Arc::new(instance));
        Ok(())
    }

    pub(crate) fn unregister(
        &mut self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.instances
            .remove(instance_id)
            .ok_or_else(|| unknown_instance(instance_id))
    }

    pub(crate) fn resolve(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<ConnectorInstance>, ConnectorHostError> {
        self.instances
            .get(instance_id)
            .cloned()
            .ok_or_else(|| unknown_instance(instance_id))
    }

    pub(crate) fn register_transport_factory(
        &mut self,
        factory: Arc<dyn ConnectorTransportFactory>,
    ) -> Result<(), ConnectorHostError> {
        let provider_id = factory.provider_id().clone();
        if self.transport_factories.contains_key(&provider_id) {
            return Err(ConnectorHostError {
                kind: ConnectorHostErrorKind::DuplicateInstance,
                message: format!(
                    "connector transport factory `{}` is already registered",
                    provider_id.as_str()
                ),
            });
        }
        self.transport_factories.insert(provider_id, factory);
        Ok(())
    }

    pub(crate) fn materialize_transport_instance(
        &self,
        provider_id: &ConnectorProviderId,
        instance_id: ConnectorInstanceId,
        scan_payload: Bytes,
        file_ranges: &[FileScanRange],
        output_schema: ChunkSchemaRef,
    ) -> Result<ConnectorInstance, ConnectorHostError> {
        let factory =
            self.transport_factories
                .get(provider_id)
                .ok_or_else(|| ConnectorHostError {
                    kind: ConnectorHostErrorKind::UnknownInstance,
                    message: format!(
                        "unknown connector transport provider `{}`",
                        provider_id.as_str()
                    ),
                })?;
        factory
            .materialize(instance_id, scan_payload, file_ranges, output_schema)
            .map_err(|error| ConnectorHostError {
                kind: ConnectorHostErrorKind::UnknownInstance,
                message: error.to_string(),
            })
    }
}

fn unknown_instance(instance_id: &ConnectorInstanceId) -> ConnectorHostError {
    ConnectorHostError {
        kind: ConnectorHostErrorKind::UnknownInstance,
        message: format!("unknown connector instance `{}`", instance_id.as_str()),
    }
}

/// Keeps a decoder-created connector instance registered for the lifetime of
/// its physical scan source.  Dropping the last lease unregisters the exact
/// instance, so query-local credentials never accumulate in the shared host.
pub(crate) struct ConnectorInstanceLease {
    host: Arc<std::sync::RwLock<ConnectorHost>>,
    instance_id: ConnectorInstanceId,
}

impl ConnectorInstanceLease {
    pub(crate) fn new(
        host: Arc<std::sync::RwLock<ConnectorHost>>,
        instance_id: ConnectorInstanceId,
    ) -> Self {
        Self { host, instance_id }
    }
}

impl Drop for ConnectorInstanceLease {
    fn drop(&mut self) {
        if let Ok(mut host) = self.host.write() {
            let _ = host.unregister(&self.instance_id);
        }
    }
}
