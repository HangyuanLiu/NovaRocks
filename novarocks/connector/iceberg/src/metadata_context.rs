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

//! Runtime-private state for one Iceberg control generation.
//!
//! A control generation owns precisely one catalog client and the physical
//! caches derived from its parsed configuration.  The frontend owns the map
//! of generations; this value deliberately has no catalog-name registry and
//! never falls back to a process-global Tokio runtime.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorErrorKind;
use novarocks_types::naming::normalize_identifier;

use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::loaded_table::IcebergPhysicalTable;
use crate::resources::IcebergMetadataResources;

/// Everything one Iceberg control generation owns.
///
/// The generation holds exactly one catalog, and every operation family reaches
/// it through [`IcebergMetadataContext::novarocks_catalog`]. There is no second
/// handle and no concrete-kind slot: a generation that held two clients would
/// have two pieces of in-memory state that disagree about the same lake.
///
/// Callers that need the vendored client -- the commit machinery, which submits
/// through `Catalog::update_table` -- derive it from the owner rather than from
/// a field of their own.
#[derive(Clone)]
pub struct IcebergMetadataContext {
    control_state: IcebergCatalogControlState,
    resources: IcebergMetadataResources,
    /// The one semantic owner of catalog behavior for this generation.
    novarocks_catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
    /// Proven-committed drops awaiting collection. Generation-local and
    /// bounded; retired with the generation.
    drop_cleanup: Arc<crate::catalog_control::drop_cleanup::DropCleanupQueue>,
    write_activations: Arc<crate::write_activation::IcebergWriteActivationReservations>,
}

#[allow(dead_code)]
impl IcebergMetadataContext {
    /// Construct one fully local provider generation.  REST and HMS client
    /// initialization is polled only through the runtime injected by server
    /// composition, so factory construction remains deterministic in every
    /// frontend role.
    pub fn try_new(
        control_state: IcebergCatalogControlState,
        resources: IcebergMetadataResources,
    ) -> Result<Self, String> {
        let configuration = control_state.configuration().clone();
        let catalog = resources
            .catalog_runtime()
            .block_on(
                async move { crate::catalog_runtime::build_catalog_client(&configuration).await },
            )?
            .map_err(|error| format!("build Iceberg control-generation catalog: {error}"))?;
        // Every handle below is derived from this one client. Building a second
        // one for the owner would give the generation two clients with separate
        // in-memory state, and they would disagree about the same lake -- a
        // table dropped through one still resolving through the other.
        let novarocks_catalog = crate::catalog::factory::NovaRocksCatalogFactory::adopt(
            control_state.configuration(),
            &catalog,
        )?;
        Ok(Self {
            control_state,
            resources,
            novarocks_catalog,
            drop_cleanup: Arc::new(crate::catalog_control::drop_cleanup::DropCleanupQueue::new()),
            write_activations: Arc::new(
                crate::write_activation::IcebergWriteActivationReservations::default(),
            ),
        })
    }

    pub(crate) fn control_state(&self) -> &IcebergCatalogControlState {
        &self.control_state
    }

    /// The generation's single catalog owner.
    pub(crate) fn novarocks_catalog(&self) -> &Arc<dyn crate::catalog::NovaRocksCatalog> {
        &self.novarocks_catalog
    }

    /// Proven-committed drops awaiting their collection pass.
    pub(crate) fn drop_cleanup(
        &self,
    ) -> &Arc<crate::catalog_control::drop_cleanup::DropCleanupQueue> {
        &self.drop_cleanup
    }

    pub(crate) fn resources(&self) -> &IcebergMetadataResources {
        &self.resources
    }

    pub(crate) fn load_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergPhysicalTable, String> {
        self.load_table_classified(namespace, table)
            .map_err(|(_, message)| message)
    }

    /// Load a table while keeping the catalog's own error classification.
    ///
    /// The string-returning `load_table` erases it, but the metadata SPI has to
    /// keep absence distinguishable from a transport failure: callers drive
    /// `CREATE ... IF NOT EXISTS` and MV target creation off
    /// `ConnectorErrorKind::NotFound`, and an absent table reported as
    /// `Unavailable` turns those into hard errors.
    pub(crate) fn load_table_classified(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergPhysicalTable, (ConnectorErrorKind, String)> {
        let namespace = normalize_identifier(namespace).map_err(invalid_request)?;
        let table = normalize_identifier(table).map_err(invalid_request)?;
        if let Some(table) = self
            .control_state
            .physical_table_cache()
            .get(&namespace, &table)
            .map_err(unavailable)?
        {
            return Ok(table);
        }
        let ident = TableIdent::from_strs([namespace.as_str(), table.as_str()])
            .map_err(|error| invalid_request(format!("build Iceberg table identity: {error}")))?;
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let loaded = self
            .resources
            .catalog_runtime()
            .block_on(async move { owner.load_table(target).await })
            .map_err(unavailable)?
            .map_err(|error| {
                (
                    error.kind(),
                    format!("load Iceberg table {namespace}.{table}: {error}"),
                )
            })?;
        let physical =
            IcebergPhysicalTable::new(loaded, self.control_state.object_store_config().cloned());
        self.control_state
            .physical_table_cache()
            .insert(&namespace, &table, physical.clone())
            .map_err(unavailable)?;
        Ok(physical)
    }

    pub(crate) fn list_namespaces(&self) -> Result<Vec<String>, String> {
        let owner = Arc::clone(self.novarocks_catalog());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.list_namespaces().await })?
            .map_err(|error| format!("list Iceberg namespaces: {error}"))
    }

    pub(crate) fn namespace_exists(&self, namespace: &str) -> Result<bool, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.namespace_exists(target).await })?
            .map_err(|error| format!("check Iceberg namespace {namespace_label}: {error}"))
    }

    pub(crate) fn list_tables(&self, namespace: &str) -> Result<Vec<String>, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target = crate::catalog::CatalogNamespaceName::new(namespace.to_url_string());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.list_tables(target).await })?
            .map_err(|error| format!("list Iceberg tables in {namespace_label}: {error}"))
    }

    pub(crate) fn table_exists(&self, namespace: &str, table: &str) -> Result<bool, String> {
        let ident = TableIdent::new(
            NamespaceIdent::new(normalize_identifier(namespace)?),
            normalize_identifier(table)?,
        );
        let ident_label = ident.to_string();
        let owner = Arc::clone(self.novarocks_catalog());
        let target =
            crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        self.resources
            .catalog_runtime()
            .block_on(async move { owner.table_exists(target).await })?
            .map_err(|error| format!("check Iceberg table {ident_label}: {error}"))
    }

    /// Shared reservation scope for every write capability assembled from
    /// this exact control generation.
    pub(crate) fn write_activation_reservations(
        &self,
    ) -> &Arc<crate::write_activation::IcebergWriteActivationReservations> {
        &self.write_activations
    }
}

fn invalid_request(message: String) -> (ConnectorErrorKind, String) {
    (ConnectorErrorKind::InvalidRequest, message)
}

fn unavailable(message: String) -> (ConnectorErrorKind, String) {
    (ConnectorErrorKind::Unavailable, message)
}

impl std::fmt::Debug for IcebergMetadataContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergMetadataContext")
            .field("control_state", &"<provider catalog state>")
            .field("resources", &self.resources)
            .field("catalog", &"<provider catalog client>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

    #[test]
    fn generation_runtime_keeps_one_explicit_catalog_client() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let control = IcebergMetadataResources::new(binding, runtime.handle().clone());
        let generation = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            control,
        )
        .expect("generation runtime");

        assert_eq!(generation.control_state().properties().len(), 2);
        assert!(Arc::strong_count(generation.write_activation_reservations()) >= 1);

        // Every vendored handle the generation hands out is the same
        // allocation. Two clients built from one configuration would have
        // separate in-memory state and disagree about the same lake -- a table
        // dropped through one would still resolve through the other. That held
        // once during development and cost a passing test to find, which is why
        // the context now has no second handle to get wrong.
        let first = generation.novarocks_catalog().vendored_client();
        let second = generation.novarocks_catalog().vendored_client();
        assert!(
            Arc::ptr_eq(&first, &second),
            "a generation must hold exactly one catalog client"
        );
    }
}
