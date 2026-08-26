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

//! The backend-local registry of installed typed connector providers.
//!
//! Responsibilities:
//! - Holds, per connector execution binding generation, the worker-side
//!   providers a typed scan needs: the page-source provider and the system
//!   table provider.
//! - Makes a stale generation structurally unresolvable: the incarnation is
//!   part of the key, so a plan frozen against a retired instance resolves to
//!   nothing instead of silently reading through a replacement.
//!
//! Key exported interfaces:
//! - Types: `TypedConnectorProviderRegistry`, `TypedConnectorProviders`,
//!   `TypedProviderRegistryError`, `TypedProviderRegistryErrorKind`.
//!
//! Current limitations:
//! - Installation is a composition-root action. Nothing here discovers,
//!   constructs, or names a provider: there is deliberately no dynamic
//!   resolver, no provider-name lookup, and no fallback, because the only crate
//!   that sees both the protocol boundary and a concrete provider is the server
//!   composition root. The backend therefore never links a provider crate.
//!
//! Provider neutrality: this file holds trait objects and an identity key only.
//! It never matches a provider variant, so it compiles with no provider crate
//! in the dependency graph.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_proto::connector_read::TypedConnectorProviderFactory;
use novarocks_proto::connector_read::{
    TypedConnectorPageSourceProvider, TypedConnectorSystemTableProvider,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorExecutionBindingKey, ConnectorInstanceIncarnation,
    ConnectorRequestContext,
};

/// The worker-side provider factory of exactly one connector binding
/// generation.
///
/// A factory rather than an instance: the provider owns a footer cache and a
/// delete manager that must not outlive the query that opened them, and it
/// needs that request's deadline and cancellation. Both worker entry points
/// come from one factory because a system relation and a data relation of one
/// catalog must never resolve to different incarnations of it.
#[derive(Clone)]
pub struct TypedConnectorProviders {
    factory: Arc<dyn TypedConnectorProviderFactory>,
}

impl TypedConnectorProviders {
    pub fn new(factory: Arc<dyn TypedConnectorProviderFactory>) -> Self {
        Self { factory }
    }

    /// Build the data-relation reader factory for one fragment instance.
    pub fn page_source(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorPageSourceProvider>, ConnectorError> {
        self.factory.create_page_source_provider(request)
    }

    /// Build the system-relation reader for one fragment instance.
    pub fn system_table(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorSystemTableProvider>, ConnectorError> {
        self.factory.create_system_table_provider(request)
    }
}

impl fmt::Debug for TypedConnectorProviders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The factory itself is opaque here on purpose: printing anything about
        // it would be the first step toward branching on it.
        formatter.debug_struct("TypedConnectorProviders").finish()
    }
}

/// Why an install or a resolve was refused.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TypedProviderRegistryErrorKind {
    /// The exact generation is already installed. Rotating providers is
    /// `retire` followed by `install`, never an overwrite in place.
    AlreadyInstalled,
    /// A different incarnation of the same instance is installed. Two live
    /// generations of one catalog would make "which one does this plan read"
    /// unanswerable, so the second install is refused.
    IncarnationConflict,
    /// The instance is installed, but at another incarnation. The caller holds
    /// a plan frozen against a generation that no longer exists.
    StaleIncarnation,
    /// Nothing is installed for this instance at all.
    NotInstalled,
}

impl fmt::Display for TypedProviderRegistryErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyInstalled => "already installed",
            Self::IncarnationConflict => "incarnation conflict",
            Self::StaleIncarnation => "stale incarnation",
            Self::NotInstalled => "not installed",
        })
    }
}

/// A typed refusal naming the generation it is about.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedProviderRegistryError {
    kind: TypedProviderRegistryErrorKind,
    instance_id: String,
    incarnation: [u8; 16],
    detail: String,
}

impl TypedProviderRegistryError {
    fn new(
        kind: TypedProviderRegistryErrorKind,
        key: &ConnectorExecutionBindingKey,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            instance_id: key.instance_id().to_owned(),
            incarnation: key.incarnation(),
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> TypedProviderRegistryErrorKind {
        self.kind
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub const fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for TypedProviderRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "typed connector provider {} for instance `{}` incarnation {}: {}",
            self.kind,
            self.instance_id,
            hex::encode(self.incarnation),
            self.detail
        )
    }
}

impl std::error::Error for TypedProviderRegistryError {}

/// Every typed connector generation installed in this backend process role.
///
/// This is a plain owned object, deliberately not a process global: the server
/// composition root creates it, installs into it, and it dies with the role.
#[derive(Default)]
pub struct TypedConnectorProviderRegistry {
    /// Keyed by instance id, because at most one generation of one instance may
    /// be installed at a time; the stored incarnation completes the key.
    installed: Mutex<HashMap<String, InstalledGeneration>>,
}

struct InstalledGeneration {
    incarnation: ConnectorInstanceIncarnation,
    providers: TypedConnectorProviders,
}

impl TypedConnectorProviderRegistry {
    pub fn new() -> Self {
        Self {
            installed: Mutex::new(HashMap::new()),
        }
    }

    /// Install one generation's providers.
    ///
    /// Never a silent replace: an already-installed generation and a competing
    /// incarnation are both typed refusals, so a composition mistake is visible
    /// at startup rather than as a query that reads the wrong snapshot.
    pub fn install(
        &self,
        key: &ConnectorExecutionBindingKey,
        providers: TypedConnectorProviders,
    ) -> Result<(), TypedProviderRegistryError> {
        let mut installed = self
            .installed
            .lock()
            .expect("typed connector provider registry lock");
        match installed.get(key.instance_id()) {
            Some(existing) if existing.incarnation == key.incarnation => {
                Err(TypedProviderRegistryError::new(
                    TypedProviderRegistryErrorKind::AlreadyInstalled,
                    key,
                    "retire this generation before installing it again",
                ))
            }
            Some(existing) => Err(TypedProviderRegistryError::new(
                TypedProviderRegistryErrorKind::IncarnationConflict,
                key,
                format!(
                    "incarnation {} is already installed for this instance",
                    hex::encode(existing.incarnation.to_bytes())
                ),
            )),
            None => {
                installed.insert(
                    key.instance_id().to_owned(),
                    InstalledGeneration {
                        incarnation: key.incarnation,
                        providers,
                    },
                );
                Ok(())
            }
        }
    }

    /// Resolve the providers of exactly the requested generation.
    pub fn resolve(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Result<TypedConnectorProviders, TypedProviderRegistryError> {
        let installed = self
            .installed
            .lock()
            .expect("typed connector provider registry lock");
        match installed.get(key.instance_id()) {
            Some(existing) if existing.incarnation == key.incarnation => {
                Ok(existing.providers.clone())
            }
            Some(existing) => Err(TypedProviderRegistryError::new(
                TypedProviderRegistryErrorKind::StaleIncarnation,
                key,
                format!(
                    "instance is installed at incarnation {}",
                    hex::encode(existing.incarnation.to_bytes())
                ),
            )),
            None => Err(TypedProviderRegistryError::new(
                TypedProviderRegistryErrorKind::NotInstalled,
                key,
                "no typed provider is installed for this connector instance",
            )),
        }
    }

    /// Remove one generation. Idempotent; reports whether it removed anything.
    ///
    /// Retiring another incarnation of the same instance removes nothing: a
    /// late retire of a superseded generation must not unbind the live one.
    pub fn retire(&self, key: &ConnectorExecutionBindingKey) -> bool {
        let mut installed = self
            .installed
            .lock()
            .expect("typed connector provider registry lock");
        match installed.get(key.instance_id()) {
            Some(existing) if existing.incarnation == key.incarnation => {
                installed.remove(key.instance_id());
                true
            }
            Some(_) | None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.installed
            .lock()
            .expect("typed connector provider registry lock")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for TypedConnectorProviderRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedConnectorProviderRegistry")
            .field("installed", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use novarocks_proto::connector_read::{
        CatalogTableHandle, ScanAssignment, ValidatedConnectorSplit, WireDynamicFilter,
    };
    use novarocks_spi::connector::read_stack::{ConnectorPageSource, ConnectorSession};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId};

    use super::*;

    /// A provider that never produces anything. Provider identity is irrelevant
    /// to the registry, so the fake carries none.
    /// A factory that hands back inert providers. The registry only ever moves
    /// this value around, so the providers never have to do anything.
    struct InertProviderFactory;

    impl TypedConnectorProviderFactory for InertProviderFactory {
        fn create_page_source_provider(
            &self,
            _request: &ConnectorRequestContext,
        ) -> Result<Arc<dyn TypedConnectorPageSourceProvider>, ConnectorError> {
            Ok(Arc::new(InertPageSourceProvider))
        }

        fn create_system_table_provider(
            &self,
            _request: &ConnectorRequestContext,
        ) -> Result<Arc<dyn TypedConnectorSystemTableProvider>, ConnectorError> {
            Ok(Arc::new(InertSystemTableProvider))
        }
    }

    struct InertPageSourceProvider;

    impl TypedConnectorPageSourceProvider for InertPageSourceProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _split: &ValidatedConnectorSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[ScanAssignment],
            _dynamic_filter: &Arc<WireDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "inert test provider",
            ))
        }
    }

    struct InertSystemTableProvider;

    impl TypedConnectorSystemTableProvider for InertSystemTableProvider {
        fn create_system_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &CatalogTableHandle,
            _columns: &[ScanAssignment],
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "inert test provider",
            ))
        }
    }

    fn providers() -> TypedConnectorProviders {
        TypedConnectorProviders::new(Arc::new(InertProviderFactory))
    }

    fn key(instance: &str, incarnation: u8) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse(instance).expect("instance id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([incarnation; 16]),
        }
    }

    #[test]
    fn typed_install_then_resolve_returns_the_installed_generation() {
        let registry = TypedConnectorProviderRegistry::new();
        assert!(registry.is_empty());
        registry
            .install(&key("test.typed", 1), providers())
            .expect("first install");
        assert_eq!(registry.len(), 1);
        registry
            .resolve(&key("test.typed", 1))
            .expect("the installed generation resolves");
    }

    #[test]
    fn typed_registry_rejects_a_conflicting_provider_for_the_same_instance() {
        let registry = TypedConnectorProviderRegistry::new();
        registry
            .install(&key("test.typed", 1), providers())
            .expect("first install");

        let error = registry
            .install(&key("test.typed", 2), providers())
            .expect_err("a second incarnation must not silently replace the first");
        assert_eq!(
            error.kind(),
            TypedProviderRegistryErrorKind::IncarnationConflict
        );
        assert_eq!(error.instance_id(), "test.typed");

        // The live generation is untouched by the refused install.
        registry
            .resolve(&key("test.typed", 1))
            .expect("the first generation still resolves");
    }

    #[test]
    fn typed_registry_rejects_reinstalling_the_same_generation() {
        let registry = TypedConnectorProviderRegistry::new();
        registry
            .install(&key("test.typed", 1), providers())
            .expect("first install");
        let error = registry
            .install(&key("test.typed", 1), providers())
            .expect_err("an install is never an overwrite in place");
        assert_eq!(
            error.kind(),
            TypedProviderRegistryErrorKind::AlreadyInstalled
        );
    }

    #[test]
    fn typed_registry_does_not_resolve_a_stale_generation() {
        let registry = TypedConnectorProviderRegistry::new();
        registry
            .install(&key("test.typed", 1), providers())
            .expect("first install");
        let error = registry
            .resolve(&key("test.typed", 9))
            .expect_err("a plan frozen against another incarnation must not resolve");
        assert_eq!(
            error.kind(),
            TypedProviderRegistryErrorKind::StaleIncarnation
        );

        let error = registry
            .resolve(&key("other.typed", 1))
            .expect_err("an uninstalled instance must not resolve");
        assert_eq!(error.kind(), TypedProviderRegistryErrorKind::NotInstalled);
    }

    #[test]
    fn typed_retire_is_idempotent_and_never_unbinds_another_generation() {
        let registry = TypedConnectorProviderRegistry::new();
        registry
            .install(&key("test.typed", 1), providers())
            .expect("first install");

        // A late retire of a superseded generation must leave the live one bound.
        assert!(!registry.retire(&key("test.typed", 4)));
        registry
            .resolve(&key("test.typed", 1))
            .expect("the live generation survives a stale retire");

        assert!(registry.retire(&key("test.typed", 1)));
        assert!(!registry.retire(&key("test.typed", 1)));
        assert!(registry.is_empty());

        // Retiring frees the instance for its replacement generation.
        registry
            .install(&key("test.typed", 2), providers())
            .expect("install after retire");
        registry
            .resolve(&key("test.typed", 2))
            .expect("the replacement generation resolves");
    }
}
