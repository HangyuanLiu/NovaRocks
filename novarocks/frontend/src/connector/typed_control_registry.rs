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
use std::sync::{Arc, Mutex, Weak};

use novarocks_proto_codec::connector_read::{
    ConnectorReadCodec, TypedConnectorMetadata, TypedConnectorSplitManager,
};
use novarocks_spi::connector::ConnectorExecutionBindingKey;
use novarocks_spi::connector::read_stack::{ConnectorReadMetadata, ConnectorReadSplitManager};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

/// Frontend spelling of the SPI marker retained by a connector-owned control
/// generation.  Keeping the marker in SPI lets a connector retain the lease
/// without depending on this role crate.
pub use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease as ReadControlRegistrationLease;

/// One exact binding's complete transport-neutral coordinator read unit.
///
/// The service traits and their matching codec are installed and resolved as
/// one value.  A role never chooses a codec independently of the services
/// that minted its opaque handles.
#[derive(Clone)]
pub struct InstalledReadControl {
    metadata: Arc<dyn ConnectorReadMetadata>,
    splits: Arc<dyn ConnectorReadSplitManager>,
    codec: Arc<dyn ConnectorReadCodec>,
    registration_lease: Option<Weak<dyn ReadControlRegistrationLease>>,
}

impl InstalledReadControl {
    pub fn new(
        metadata: Arc<dyn ConnectorReadMetadata>,
        splits: Arc<dyn ConnectorReadSplitManager>,
        codec: Arc<dyn ConnectorReadCodec>,
    ) -> Self {
        Self {
            metadata,
            splits,
            codec,
            registration_lease: None,
        }
    }

    pub fn with_registration_lease(
        mut self,
        lease: Weak<dyn ReadControlRegistrationLease>,
    ) -> Self {
        self.registration_lease = Some(lease);
        self
    }

    pub fn metadata(&self) -> Arc<dyn ConnectorReadMetadata> {
        Arc::clone(&self.metadata)
    }

    pub fn splits(&self) -> Arc<dyn ConnectorReadSplitManager> {
        Arc::clone(&self.splits)
    }

    pub fn codec(&self) -> Arc<dyn ConnectorReadCodec> {
        Arc::clone(&self.codec)
    }

    pub fn registration_is_live(&self) -> bool {
        self.registration_lease
            .as_ref()
            .is_none_or(|lease| lease.strong_count() > 0)
    }
}

impl fmt::Debug for InstalledReadControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledReadControl")
            .finish_non_exhaustive()
    }
}

/// Passive FE-local registry for complete read units.  It is keyed by the
/// exact authority decision made elsewhere and deliberately permits old and
/// new generations to coexist while their callers still hold leases.
pub struct InstalledReadControlRegistry {
    state: Arc<Mutex<InstalledReadControlRegistryState>>,
}

#[derive(Default)]
struct InstalledReadControlRegistryState {
    installed: BTreeMap<ConnectorExecutionBindingKey, InstalledReadControlEntry>,
    next_ticket: u64,
}

struct InstalledReadControlEntry {
    control: InstalledReadControl,
    lease: Weak<dyn ReadControlRegistrationLease>,
    ticket: u64,
}

/// The strong lease returned to the connector generation.  It has no role
/// authority beyond conditionally removing the exact entry it installed.
struct InstalledReadControlLease {
    state: Weak<Mutex<InstalledReadControlRegistryState>>,
    key: ConnectorExecutionBindingKey,
    ticket: u64,
}

impl ReadControlRegistrationLease for InstalledReadControlLease {}

impl Drop for InstalledReadControlLease {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let Ok(mut state) = state.lock() else {
            // A poisoned frontend-local registry cannot safely make a
            // conditional mutation during drop.  It has no external effect.
            return;
        };
        if state
            .installed
            .get(&self.key)
            .is_some_and(|entry| entry.ticket == self.ticket)
        {
            state.installed.remove(&self.key);
        }
    }
}

impl Default for InstalledReadControlRegistry {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(InstalledReadControlRegistryState::default())),
        }
    }
}

impl InstalledReadControlRegistry {
    /// Atomically retain one complete read unit and return the generation's
    /// shared strong lease.  Existing exact keys preserve their already
    /// installed services and codec; a new candidate is never mixed into that
    /// unit.  If an old owner has gone away before its drop handler runs, a
    /// replacement lease is issued for the same preserved unit.
    pub fn install_or_resolve(
        &self,
        key: ConnectorExecutionBindingKey,
        control: InstalledReadControl,
    ) -> Result<Arc<dyn ReadControlRegistrationLease>, ConnectorError> {
        let mut state = self.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "installed read control registry lock is poisoned",
            )
        })?;

        if let Some(existing) = state.installed.get(&key)
            && let Some(lease) = existing.lease.upgrade()
        {
            return Ok(lease);
        }

        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        let lease: Arc<dyn ReadControlRegistrationLease> = Arc::new(InstalledReadControlLease {
            state: Arc::downgrade(&self.state),
            key: key.clone(),
            ticket,
        });
        let weak_lease = Arc::downgrade(&lease);

        if let Some(existing) = state.installed.get_mut(&key) {
            // Reuse the original complete unit.  Only its weak owner edge is
            // renewed, so services and codec remain a matching exact bundle.
            existing.control = existing
                .control
                .clone()
                .with_registration_lease(weak_lease.clone());
            existing.lease = weak_lease;
            existing.ticket = ticket;
        } else {
            state.installed.insert(
                key,
                InstalledReadControlEntry {
                    control: control.with_registration_lease(weak_lease.clone()),
                    lease: weak_lease,
                    ticket,
                },
            );
        }
        Ok(lease)
    }

    pub fn resolve(&self, key: &ConnectorExecutionBindingKey) -> Option<InstalledReadControl> {
        self.state
            .lock()
            .expect("installed read control registry lock")
            .installed
            .get(key)
            .and_then(|entry| entry.lease.upgrade().map(|_| entry.control.clone()))
    }

    /// Conditional retirement leaves a concurrent replacement untouched.
    pub fn retire(&self, key: &ConnectorExecutionBindingKey) -> bool {
        self.state
            .lock()
            .expect("installed read control registry lock")
            .installed
            .remove(key)
            .is_some()
    }
}

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
    read_controls: InstalledReadControlRegistry,
}

impl TypedConnectorControlRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the complete SPI read unit beside the legacy control entry
    /// while composition is migrated.  Scan planning resolves only this unit;
    /// the old registry remains for unrelated callers until their cutover.
    pub fn install_read_control(
        &self,
        key: ConnectorExecutionBindingKey,
        control: InstalledReadControl,
    ) -> Result<Arc<dyn ReadControlRegistrationLease>, ConnectorError> {
        self.read_controls.install_or_resolve(key, control)
    }

    pub fn resolve_read_control(
        &self,
        key: &ConnectorExecutionBindingKey,
    ) -> Option<InstalledReadControl> {
        self.read_controls.resolve(key)
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

    use novarocks_proto_codec::connector_read::{
        CatalogTableHandle, ConnectorReadCodec, ConnectorReadCodecError, TypedColumnBinding,
        TypedFilterApplication, TypedLimitApplication, TypedRelationVersion, TypedSystemTablePlan,
        WireConstraint, WireDynamicFilterSnapshot,
    };
    use novarocks_proto_codec::connector_read::{
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

        fn get_pinned_file_set_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _pinned: &novarocks_spi::connector::ConnectorPinnedFileSet,
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
            _window: novarocks_proto_codec::connector_read::TypedChangeWindow,
        ) -> Result<Option<CatalogTableHandle>, ConnectorError> {
            Ok(None)
        }

        fn get_table_execute_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _procedure: novarocks_proto_codec::connector_read::TypedTableExecuteProcedure<'_>,
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

    struct StubReadControl;

    impl ConnectorReadMetadata for StubReadControl {
        fn relation(
            &self,
            _kind: novarocks_spi::connector::read_stack::ConnectorReadRelationKind,
            _table: novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
        ) -> Result<novarocks_spi::connector::read_stack::ConnectorReadRelation, ConnectorError>
        {
            unreachable!("registry test never invokes metadata")
        }

        fn get_table_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _version: novarocks_spi::connector::read_stack::ConnectorReadRelationVersion,
            _reference: Option<&str>,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadTableHandle>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn get_pinned_file_set_handle(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _pinned: &novarocks_spi::connector::ConnectorPinnedFileSet,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadTableHandle>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn get_column_bindings(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
        ) -> Result<
            Vec<novarocks_spi::connector::read_stack::ConnectorReadColumnBinding>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn apply_filter(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _constraint: &novarocks_spi::connector::read_stack::ConnectorReadConstraint,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadFilterApplication>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn apply_projection(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _assignments: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadTableHandle>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn apply_limit(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _limit: u64,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadLimitApplication>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn get_system_table_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadSystemTablePlan>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn get_change_window_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _window: novarocks_spi::connector::read_stack::ConnectorReadChangeWindow,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadTableHandle>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }

        fn get_table_execute_plan(
            &self,
            _session: &ConnectorSession,
            _name: &SchemaTableName,
            _procedure: novarocks_spi::connector::read_stack::ConnectorReadTableExecuteProcedure,
        ) -> Result<
            Option<novarocks_spi::connector::read_stack::ConnectorReadTableHandle>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes metadata")
        }
    }

    impl ConnectorReadSplitManager for StubReadControl {
        fn get_splits(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _dynamic_filter_columns: &BTreeSet<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >,
            _constraint: &novarocks_spi::connector::read_stack::ConnectorReadConstraint,
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorReadSplitSource>,
            ConnectorError,
        > {
            unreachable!("registry test never invokes split planning")
        }
    }

    struct StubReadCodec;

    impl ConnectorReadCodec for StubReadCodec {
        fn owner(&self) -> &str {
            "test"
        }

        fn decode_relation(
            &self,
            _relation: &CatalogTableHandle,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadRelation,
            ConnectorReadCodecError,
        > {
            unreachable!("registry test never invokes codec")
        }

        fn encode_relation(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_proto_models::connector_read::CatalogTableHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("registry test never invokes codec")
        }

        fn decode_column(
            &self,
            _column: &ValidatedColumnHandle,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("registry test never invokes codec")
        }

        fn encode_column(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<novarocks_proto_models::connector_read::ColumnHandle, ConnectorReadCodecError>
        {
            unreachable!("registry test never invokes codec")
        }

        fn decode_transaction(
            &self,
            _transaction: &novarocks_proto_codec::connector_read::ValidatedTransactionHandle,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("registry test never invokes codec")
        }

        fn encode_transaction(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_proto_models::connector_read::ConnectorTransactionHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("registry test never invokes codec")
        }

        fn decode_split(
            &self,
            _split: &novarocks_proto_codec::connector_read::ValidatedConnectorSplit,
        ) -> Result<novarocks_spi::connector::read_stack::ConnectorReadSplit, ConnectorReadCodecError>
        {
            unreachable!("registry test never invokes codec")
        }

        fn encode_split(
            &self,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
        ) -> Result<novarocks_proto_models::connector_read::ConnectorSplit, ConnectorReadCodecError>
        {
            unreachable!("registry test never invokes codec")
        }
    }

    fn read_control() -> InstalledReadControl {
        let stub = Arc::new(StubReadControl);
        InstalledReadControl::new(stub.clone(), stub, Arc::new(StubReadCodec))
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

    #[test]
    fn read_registration_lease_removes_only_its_own_exact_slot() {
        let registry = InstalledReadControlRegistry::default();
        let first_key = key("ice", 1);
        let second_key = key("ice", 2);
        let first = registry
            .install_or_resolve(first_key.clone(), read_control())
            .expect("install first");
        let second = registry
            .install_or_resolve(second_key.clone(), read_control())
            .expect("install second");
        assert!(registry.resolve(&first_key).is_some());
        assert!(registry.resolve(&second_key).is_some());

        drop(first);
        assert!(registry.resolve(&first_key).is_none());
        assert!(registry.resolve(&second_key).is_some());
        drop(second);
        assert!(registry.resolve(&second_key).is_none());
    }

    #[test]
    fn same_exact_key_reuses_the_shared_generation_lease() {
        let registry = InstalledReadControlRegistry::default();
        let installed_key = key("ice", 1);
        let first = registry
            .install_or_resolve(installed_key.clone(), read_control())
            .expect("install");
        let retry = registry
            .install_or_resolve(installed_key.clone(), read_control())
            .expect("retry");

        assert!(Arc::ptr_eq(&first, &retry));
        assert!(
            registry
                .resolve(&installed_key)
                .expect("resolved control")
                .registration_is_live()
        );
        drop(first);
        assert!(registry.resolve(&installed_key).is_some());
        drop(retry);
        assert!(registry.resolve(&installed_key).is_none());
    }
}
