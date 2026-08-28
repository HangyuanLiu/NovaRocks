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

//! Backend-local mirror of an admitted connector read execution.
//!
//! The Host owns admission and retirement. This registry only retains the
//! exact provider factory and codec selected by that Host admission, so plan
//! decode and TaskUpdate recover SPI handles from the same binding generation.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_proto_codec::connector_read::ConnectorReadCodec;
use novarocks_spi::connector::ConnectorExecutionBindingKey;
use novarocks_spi::connector::read_stack::ConnectorReadProviderFactory;

/// One exact binding's complete worker read unit. The factory and codec must
/// travel together because every recovered handle belongs to the factory that
/// will consume it.
#[derive(Clone)]
pub struct InstalledReadExecution {
    factory: Arc<dyn ConnectorReadProviderFactory>,
    codec: Arc<dyn ConnectorReadCodec>,
}

impl InstalledReadExecution {
    pub fn new(
        factory: Arc<dyn ConnectorReadProviderFactory>,
        codec: Arc<dyn ConnectorReadCodec>,
    ) -> Self {
        Self { factory, codec }
    }

    pub fn factory(&self) -> Arc<dyn ConnectorReadProviderFactory> {
        Arc::clone(&self.factory)
    }

    pub fn codec(&self) -> Arc<dyn ConnectorReadCodec> {
        Arc::clone(&self.codec)
    }
}

impl fmt::Debug for InstalledReadExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledReadExecution")
            .finish_non_exhaustive()
    }
}

/// Passive exact-key BE mirror. It has no provider discovery or generation
/// authority; callers obtain the key from existing Host admission first.
#[derive(Default)]
pub struct InstalledReadExecutionRegistry {
    installed: Mutex<BTreeMap<ConnectorExecutionBindingKey, InstalledReadExecution>>,
}

impl InstalledReadExecutionRegistry {
    /// Keep the first bundle installed for this exact key. Host admission is
    /// idempotent, so a replay returns the same registered pair.
    pub fn install_or_resolve(
        &self,
        key: ConnectorExecutionBindingKey,
        execution: InstalledReadExecution,
    ) -> InstalledReadExecution {
        let mut installed = self
            .installed
            .lock()
            .expect("installed read execution registry lock");
        installed.entry(key).or_insert(execution).clone()
    }

    pub fn resolve(&self, key: &ConnectorExecutionBindingKey) -> Option<InstalledReadExecution> {
        self.installed
            .lock()
            .expect("installed read execution registry lock")
            .get(key)
            .cloned()
    }

    pub fn retire(&self, key: &ConnectorExecutionBindingKey) -> bool {
        self.installed
            .lock()
            .expect("installed read execution registry lock")
            .remove(key)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::InstalledReadExecutionRegistry;
    use novarocks_spi::connector::{
        ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
    };

    fn key() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::try_from_canonical("catalog.analytics")
                .expect("canonical instance id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    #[test]
    fn empty_registry_has_no_exact_execution_and_retire_is_idempotent() {
        let registry = InstalledReadExecutionRegistry::default();
        let key = key();
        assert!(registry.resolve(&key).is_none());
        assert!(!registry.retire(&key));
    }
}
