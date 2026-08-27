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

//! Frontend-local registry of typed connector control entry points.
//!
//! The frontend cannot link a provider crate, so the server composition root
//! installs the provider's control and split-enumeration entry points here at
//! binding time. Resolution is keyed by the exact execution binding generation,
//! so a stale generation can never be reached and there is no name-based
//! lookup, dynamic resolver, or fallback.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_proto::connector_read::{TypedConnectorMetadata, TypedConnectorSplitManager};
use novarocks_spi::connector::ConnectorExecutionBindingKey;

/// The pair of coordinator-side entry points one installed provider offers.
#[derive(Clone)]
pub struct TypedConnectorControl {
    metadata: Arc<dyn TypedConnectorMetadata>,
    splits: Arc<dyn TypedConnectorSplitManager>,
}

impl TypedConnectorControl {
    pub fn new(
        metadata: Arc<dyn TypedConnectorMetadata>,
        splits: Arc<dyn TypedConnectorSplitManager>,
    ) -> Self {
        Self { metadata, splits }
    }

    pub fn metadata(&self) -> Arc<dyn TypedConnectorMetadata> {
        Arc::clone(&self.metadata)
    }

    pub fn splits(&self) -> Arc<dyn TypedConnectorSplitManager> {
        Arc::clone(&self.splits)
    }
}

impl fmt::Debug for TypedConnectorControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedConnectorControl")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedControlRegistryErrorKind {
    /// The instance is already installed at another incarnation. Replacing it
    /// silently would let an in-flight statement straddle two control
    /// generations.
    GenerationConflict,
    /// Nothing is installed for this exact generation.
    NotInstalled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedControlRegistryError {
    kind: TypedControlRegistryErrorKind,
    instance_id: String,
    detail: String,
}

impl TypedControlRegistryError {
    pub(crate) const fn kind(&self) -> TypedControlRegistryErrorKind {
        self.kind
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for TypedControlRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "typed connector control for '{}': {}",
            self.instance_id, self.detail
        )
    }
}

impl std::error::Error for TypedControlRegistryError {}

/// Frontend-owned map from one exact binding generation to its control pair.
#[derive(Default)]
pub struct TypedConnectorControlRegistry {
    installed: Mutex<BTreeMap<ConnectorExecutionBindingKey, TypedConnectorControl>>,
}

impl TypedConnectorControlRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install(
        &self,
        key: ConnectorExecutionBindingKey,
        control: TypedConnectorControl,
    ) -> Result<(), TypedControlRegistryError> {
        let mut installed = self.installed.lock().expect("typed control registry lock");
        if let Some((existing, _)) = installed
            .iter()
            .find(|(candidate, _)| candidate.instance_id == key.instance_id)
            && existing.incarnation != key.incarnation
        {
            return Err(TypedControlRegistryError {
                kind: TypedControlRegistryErrorKind::GenerationConflict,
                instance_id: key.instance_id.as_str().to_owned(),
                detail: "another incarnation of this instance is already installed".to_owned(),
            });
        }
        // Reinstalling the same generation is idempotent: binding install is
        // retried on an ambiguous response, and a retry must not be a conflict.
        installed.insert(key, control);
        Ok(())
    }

    pub fn resolve(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<TypedConnectorControl, TypedControlRegistryError> {
        self.installed
            .lock()
            .expect("typed control registry lock")
            .get(key)
            .cloned()
            .ok_or_else(|| TypedControlRegistryError {
                kind: TypedControlRegistryErrorKind::NotInstalled,
                instance_id: key.instance_id.as_str().to_owned(),
                detail: "no typed connector control is installed for this exact generation"
                    .to_owned(),
            })
    }

    pub fn retire(&self, key: &ConnectorExecutionBindingKey) -> bool {
        self.installed
            .lock()
            .expect("typed control registry lock")
            .remove(key)
            .is_some()
    }

    pub fn len(&self) -> usize {
        self.installed
            .lock()
            .expect("typed control registry lock")
            .len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use novarocks_proto::connector_read::{
        CatalogTableHandle, TypedColumnBinding, TypedFilterApplication, TypedLimitApplication,
        TypedRelationVersion, TypedSystemTablePlan, WireConstraint, WireDynamicFilterSnapshot,
    };
    use novarocks_proto::connector_read::{
        ScanAssignment, TypedConnectorSplitSource, ValidatedColumnHandle,
    };
    use novarocks_spi::connector::read_stack::{ConnectorSession, SchemaTableName};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorInstanceId, ConnectorInstanceIncarnation,
    };

    use super::*;

    struct StubControl;

    impl TypedConnectorMetadata for StubControl {
        fn get_table_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _version: TypedRelationVersion,
            _reference: Option<&str>,
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            Ok(None)
        }

        fn get_column_bindings(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
        ) -> Result<Vec<TypedColumnBinding>, ConnectorError> {
            Ok(Vec::new())
        }

        fn apply_filter(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _constraint: &WireConstraint,
        ) -> Result<Option<TypedFilterApplication>, ConnectorError> {
            Ok(None)
        }

        fn apply_projection(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _assignments: &[ScanAssignment],
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            Ok(None)
        }

        fn apply_limit(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _limit: u64,
        ) -> Result<Option<TypedLimitApplication>, ConnectorError> {
            Ok(None)
        }

        fn get_system_table_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
        ) -> Result<Option<TypedSystemTablePlan>, ConnectorError> {
            Ok(None)
        }

        fn get_change_window_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _window: novarocks_proto::connector_read::TypedChangeWindow,
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            Ok(None)
        }
    }

    impl TypedConnectorSplitManager for StubControl {
        fn get_splits(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _columns: &[ScanAssignment],
            _dynamic_filter_columns: &BTreeSet<ValidatedColumnHandle>,
            _constraint: &WireConstraint,
        ) -> Result<Box<dyn TypedConnectorSplitSource>, ConnectorError> {
            let _ = WireDynamicFilterSnapshot::all_complete();
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "stub control enumerates no splits",
            ))
        }
    }

    fn key(instance: &str, incarnation: u8) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::try_from_canonical(instance).expect("instance id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
        }
    }

    fn control() -> TypedConnectorControl {
        let stub = Arc::new(StubControl);
        TypedConnectorControl::new(stub.clone(), stub)
    }

    #[test]
    fn resolution_requires_the_exact_generation() {
        let registry = TypedConnectorControlRegistry::new();
        registry.install(key("ice", 1), control()).expect("install");
        assert!(registry.resolve(&key("ice", 1)).is_ok());
        assert_eq!(
            registry
                .resolve(&key("ice", 2))
                .expect_err("stale generation")
                .kind(),
            TypedControlRegistryErrorKind::NotInstalled
        );
    }

    #[test]
    fn installing_another_incarnation_of_the_same_instance_is_a_conflict() {
        let registry = TypedConnectorControlRegistry::new();
        registry.install(key("ice", 1), control()).expect("install");
        let error = registry
            .install(key("ice", 2), control())
            .expect_err("generation conflict");
        assert_eq!(
            error.kind(),
            TypedControlRegistryErrorKind::GenerationConflict
        );
        assert_eq!(error.instance_id(), "ice");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn reinstalling_the_same_generation_is_idempotent() {
        let registry = TypedConnectorControlRegistry::new();
        registry.install(key("ice", 1), control()).expect("install");
        registry.install(key("ice", 1), control()).expect("retry");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn retiring_removes_only_the_named_generation() {
        let registry = TypedConnectorControlRegistry::new();
        registry.install(key("ice", 1), control()).expect("install");
        registry
            .install(key("other", 3), control())
            .expect("install");
        assert!(registry.retire(&key("ice", 1)));
        assert!(!registry.retire(&key("ice", 1)));
        assert_eq!(registry.len(), 1);
    }
}
