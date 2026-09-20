// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use novarocks_spi::connector::read_stack::ConnectorReadBinding;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorErrorKind, ConnectorReadSelector,
    ConnectorSemanticFact, ConnectorTableHandle,
};
use novarocks_sql::binding::{SqlTableBindingAllocator, SqlTableBindingId, SqlTableBindingScopeId};

const MAX_FACT_BYTES: usize = 64 * 1024;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_DIAGNOSTIC_MESSAGE_BYTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObjectPath(Arc<[Arc<str>]>);

impl ObjectPath {
    pub fn try_new(parts: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Option<Self> {
        let parts: Vec<Arc<str>> = parts.into_iter().map(Into::into).collect();
        if parts.is_empty() || parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        Some(Self(parts.into()))
    }

    pub fn parts(&self) -> &[Arc<str>] {
        &self.0
    }

    fn encoded_len(&self) -> Option<usize> {
        self.0
            .iter()
            .try_fold(0_usize, |total, part| total.checked_add(part.len()))
    }
}

/// Query-owned identity of the provider and encoding used for one opaque fact.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProviderFactFormat {
    provider: Arc<str>,
    format: Arc<str>,
    version: u16,
}

impl ProviderFactFormat {
    pub fn try_new(provider: impl Into<Arc<str>>, format: impl Into<Arc<str>>) -> Option<Self> {
        Self::try_new_versioned(provider, format, 1)
    }

    pub fn try_new_versioned(
        provider: impl Into<Arc<str>>,
        format: impl Into<Arc<str>>,
        version: u16,
    ) -> Option<Self> {
        let provider = provider.into();
        let format = format.into();
        if provider.is_empty()
            || provider.len() > MAX_IDENTITY_BYTES
            || format.is_empty()
            || format.len() > MAX_IDENTITY_BYTES
            || version == 0
        {
            return None;
        }
        Some(Self {
            provider,
            format,
            version,
        })
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn format(&self) -> &str {
        &self.format
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    fn encoded_len(&self) -> Option<usize> {
        self.provider
            .len()
            .checked_add(self.format.len())?
            .checked_add(std::mem::size_of::<u16>())
    }
}

macro_rules! encoded_binding_fact {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            format: ProviderFactFormat,
            value: Arc<[u8]>,
        }

        impl $name {
            pub fn try_new(
                format: ProviderFactFormat,
                value: impl Into<Arc<[u8]>>,
            ) -> Option<Self> {
                let value = value.into();
                if value.is_empty() || value.len() > MAX_FACT_BYTES {
                    return None;
                }
                Some(Self { format, value })
            }

            pub const fn format_identity(&self) -> &ProviderFactFormat {
                &self.format
            }

            pub fn encoded_value(&self) -> &[u8] {
                &self.value
            }

            #[allow(
                dead_code,
                reason = "catalog generation uses a process-local format while semantic facts use this provider conversion"
            )]
            fn from_provider_fact(fact: &ConnectorSemanticFact) -> Option<Self> {
                Self::try_new(
                    ProviderFactFormat::try_new_versioned(
                        fact.provider().as_str(),
                        fact.format(),
                        fact.version(),
                    )?,
                    Arc::<[u8]>::from(fact.value().as_ref()),
                )
            }

            fn encoded_len(&self) -> Option<usize> {
                self.format
                    .encoded_len()
                    .and_then(|size| size.checked_add(self.value.len()))
            }
        }
    };
}

encoded_binding_fact!(CatalogGeneration);
encoded_binding_fact!(ObjectIdentity);
encoded_binding_fact!(DataVersion);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactObjectBinding {
    object: ObjectPath,
    catalog_generation: CatalogGeneration,
    object_identity: Option<ObjectIdentity>,
    data_version: Option<DataVersion>,
    read_binding: Option<ConnectorReadBinding>,
}

impl ExactObjectBinding {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        object: ObjectPath,
        catalog_generation: CatalogGeneration,
        object_identity: ObjectIdentity,
        data_version: DataVersion,
    ) -> Self {
        Self {
            object,
            catalog_generation,
            object_identity: Some(object_identity),
            data_version: Some(data_version),
            read_binding: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_publication_test(
        object: ObjectPath,
        catalog_generation: CatalogGeneration,
        relation: &novarocks_sql::compiler::SqlMvRewritePublicationRelation,
    ) -> Self {
        Self {
            object,
            catalog_generation,
            object_identity: ObjectIdentity::from_provider_fact(
                relation.revision().object_identity(),
            ),
            data_version: DataVersion::from_provider_fact(relation.revision().data_version()),
            read_binding: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_without_semantic_revision_for_test(
        object: ObjectPath,
        catalog_generation: CatalogGeneration,
    ) -> Self {
        Self {
            object,
            catalog_generation,
            object_identity: None,
            data_version: None,
            read_binding: None,
        }
    }

    pub const fn object(&self) -> &ObjectPath {
        &self.object
    }

    pub const fn catalog_generation(&self) -> &CatalogGeneration {
        &self.catalog_generation
    }

    pub const fn object_identity(&self) -> Option<&ObjectIdentity> {
        self.object_identity.as_ref()
    }

    pub const fn data_version(&self) -> Option<&DataVersion> {
        self.data_version.as_ref()
    }

    pub fn has_stable_semantic_revision(&self) -> bool {
        self.object_identity.is_some() && self.data_version.is_some()
    }

    pub(crate) fn matches_publication_relation(
        &self,
        relation: &novarocks_sql::compiler::SqlMvRewritePublicationRelation,
    ) -> Result<bool, String> {
        let expected_parts = relation.table_fqn().split('.').collect::<Vec<_>>();
        let actual_parts = self.object.parts();
        if expected_parts.len() != actual_parts.len()
            || expected_parts
                .iter()
                .zip(actual_parts)
                .any(|(expected, actual)| *expected != actual.as_ref())
        {
            return Ok(false);
        }
        let expected_object =
            ObjectIdentity::from_provider_fact(relation.revision().object_identity())
                .ok_or_else(|| "MV publication object identity is invalid".to_string())?;
        let expected_version = DataVersion::from_provider_fact(relation.revision().data_version())
            .ok_or_else(|| "MV publication data version is invalid".to_string())?;
        Ok(self.object_identity.as_ref() == Some(&expected_object)
            && self.data_version.as_ref() == Some(&expected_version))
    }

    pub(crate) fn read_binding(&self) -> Result<&ConnectorReadBinding, String> {
        self.read_binding.as_ref().ok_or_else(|| {
            "exact binding receipt has no production Connector read binding".to_string()
        })
    }

    fn retained_encoded_len(&self) -> Option<usize> {
        self.object
            .encoded_len()?
            .checked_add(self.catalog_generation.encoded_len()?)?
            .checked_add(
                self.object_identity
                    .as_ref()
                    .and_then(ObjectIdentity::encoded_len)
                    .unwrap_or_default(),
            )?
            .checked_add(
                self.data_version
                    .as_ref()
                    .and_then(DataVersion::encoded_len)
                    .unwrap_or_default(),
            )
    }
}

/// Query-owned receipt authority paired with one request-local binding store.
///
/// Production registration accepts only the opaque Connector lease, table
/// handle, selector, and the SQL token minted by that store. It derives all
/// identity facts internally; no external adapter can fill generation,
/// object-identity, or data-version fields.
pub struct ExactBindingReceiptStore {
    scope: SqlTableBindingScopeId,
    state: Mutex<ExactBindingReceiptState>,
}

#[derive(Default)]
struct ExactBindingReceiptState {
    receipts: HashMap<SqlTableBindingId, ExactObjectBinding>,
    sealed: Option<Arc<HashMap<SqlTableBindingId, ExactObjectBinding>>>,
}

/// Immutable read authority produced when the query binding owner closes its
/// semantic admission phase.
#[derive(Clone)]
pub struct SealedExactBindingReceipts {
    scope: SqlTableBindingScopeId,
    receipts: Arc<HashMap<SqlTableBindingId, ExactObjectBinding>>,
}

impl ExactBindingReceiptStore {
    pub fn new(allocator: &SqlTableBindingAllocator) -> Self {
        Self {
            scope: allocator.scope(),
            state: Mutex::new(ExactBindingReceiptState::default()),
        }
    }

    pub fn register_connector_binding(
        &self,
        binding: SqlTableBindingId,
        object: [&str; 3],
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableHandle,
        selector: ConnectorReadSelector,
    ) -> Result<(), String> {
        if !binding.belongs_to(self.scope) {
            return Err("exact binding receipt token belongs to another query".to_string());
        }
        let descriptor = planning_lease.binding().descriptor();
        if table.owner() != &descriptor.instance_id {
            return Err(
                "exact binding receipt table handle belongs to another Connector instance"
                    .to_string(),
            );
        }
        let provider = descriptor.provider_id.as_str();
        let format = |kind: &str| {
            ProviderFactFormat::try_new(provider, kind)
                .ok_or_else(|| "Connector binding fact format is invalid".to_string())
        };
        let object = ObjectPath::try_new(object)
            .ok_or_else(|| "exact binding receipt object path is invalid".to_string())?;
        let catalog_generation = CatalogGeneration::try_new(
            format("connector-control-runtime/v1")?,
            planning_lease.control_runtime_id().to_bytes(),
        )
        .ok_or_else(|| "Connector control generation fact is invalid".to_string())?;

        let semantic_revision = match planning_lease
            .binding()
            .metadata()
            .exact_semantic_revision(table, selector)
        {
            Ok(revision) => Some(revision),
            Err(error) if error.kind() == ConnectorErrorKind::Unsupported => None,
            Err(error) => return Err(error.to_string()),
        };
        if semantic_revision.as_ref().is_some_and(|revision| {
            revision.object_identity().provider() != &descriptor.provider_id
                || revision.data_version().provider() != &descriptor.provider_id
        }) {
            return Err(
                "provider-issued Connector semantic revision names another provider".to_string(),
            );
        }
        let object_identity = semantic_revision
            .as_ref()
            .and_then(|revision| ObjectIdentity::from_provider_fact(revision.object_identity()));
        let data_version = semantic_revision
            .as_ref()
            .and_then(|revision| DataVersion::from_provider_fact(revision.data_version()));
        if semantic_revision.is_some() && (object_identity.is_none() || data_version.is_none()) {
            return Err("provider-issued Connector semantic revision is invalid".to_string());
        }
        let receipt = ExactObjectBinding {
            object,
            catalog_generation,
            object_identity,
            data_version,
            read_binding: Some(ConnectorReadBinding::new(
                descriptor.clone(),
                planning_lease
                    .binding()
                    .catalog_properties()
                    .map_err(|error| error.to_string())?
                    .handle()
                    .clone(),
            )),
        };
        self.register_receipt(binding, receipt)
    }

    /// Register a provider-frozen read that has no separately materialized
    /// table selector. The frozen source facts remain opaque in the provider
    /// read request; this receipt only binds the synthetic SQL relation to the
    /// exact control generation that must serve those facts.
    pub fn register_frozen_connector_binding(
        &self,
        binding: SqlTableBindingId,
        object: [&str; 3],
        planning_lease: &ConnectorControlPlanningLease,
    ) -> Result<(), String> {
        if !binding.belongs_to(self.scope) {
            return Err("exact binding receipt token belongs to another query".to_string());
        }
        let descriptor = planning_lease.binding().descriptor();
        let provider = descriptor.provider_id.as_str();
        let format = |kind: &str| {
            ProviderFactFormat::try_new(provider, kind)
                .ok_or_else(|| "Connector binding fact format is invalid".to_string())
        };
        let object = ObjectPath::try_new(object)
            .ok_or_else(|| "exact binding receipt object path is invalid".to_string())?;
        let catalog_generation = CatalogGeneration::try_new(
            format("connector-control-runtime/v1")?,
            planning_lease.control_runtime_id().to_bytes(),
        )
        .ok_or_else(|| "Connector control generation fact is invalid".to_string())?;
        let receipt = ExactObjectBinding {
            object,
            catalog_generation,
            object_identity: None,
            data_version: None,
            read_binding: Some(ConnectorReadBinding::new(
                descriptor.clone(),
                planning_lease
                    .binding()
                    .catalog_properties()
                    .map_err(|error| error.to_string())?
                    .handle()
                    .clone(),
            )),
        };
        self.register_receipt(binding, receipt)
    }

    fn register_receipt(
        &self,
        binding: SqlTableBindingId,
        receipt: ExactObjectBinding,
    ) -> Result<(), String> {
        let mut state = self.state.lock().expect("exact binding receipt lock");
        if state.sealed.is_some() {
            return Err("exact binding receipt store is semantically sealed".to_string());
        }
        match state.receipts.get(&binding) {
            Some(existing) if existing == &receipt => Ok(()),
            Some(_) => {
                Err("exact binding receipt token was registered with conflicting facts".to_string())
            }
            None => {
                state.receipts.insert(binding, receipt);
                Ok(())
            }
        }
    }

    /// Close registration and publish one immutable snapshot. Repeated calls
    /// return the same snapshot and never reopen admission.
    pub fn seal(&self) -> SealedExactBindingReceipts {
        let mut state = self.state.lock().expect("exact binding receipt lock");
        let receipts = match &state.sealed {
            Some(receipts) => Arc::clone(receipts),
            None => {
                let receipts = Arc::new(state.receipts.clone());
                state.sealed = Some(Arc::clone(&receipts));
                receipts
            }
        };
        SealedExactBindingReceipts {
            scope: self.scope,
            receipts,
        }
    }

    pub fn sealed_view(&self) -> Option<SealedExactBindingReceipts> {
        let state = self.state.lock().expect("exact binding receipt lock");
        state
            .sealed
            .as_ref()
            .map(|receipts| SealedExactBindingReceipts {
                scope: self.scope,
                receipts: Arc::clone(receipts),
            })
    }

    #[cfg(test)]
    pub(crate) fn register_for_test(
        &self,
        binding: SqlTableBindingId,
        receipt: ExactObjectBinding,
    ) {
        assert!(binding.belongs_to(self.scope));
        self.register_receipt(binding, receipt)
            .expect("test receipt registration must precede semantic seal");
    }
}

impl SealedExactBindingReceipts {
    pub(crate) fn resolve(&self, binding: SqlTableBindingId) -> Result<ExactObjectBinding, String> {
        if !binding.belongs_to(self.scope) {
            return Err("exact binding receipt token belongs to another query".to_string());
        }
        self.receipts
            .get(&binding)
            .cloned()
            .ok_or_else(|| "exact binding receipt is missing from this query".to_string())
    }
}
