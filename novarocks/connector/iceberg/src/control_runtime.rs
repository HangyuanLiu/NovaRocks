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

use novarocks_catalog::identifier::normalize_identifier;

use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::loaded_table::IcebergPhysicalTable;
use crate::resources::IcebergControlResources;

#[allow(dead_code)] // Assembled into the concrete provider factory during R3C.
#[derive(Clone)]
pub struct IcebergControlRuntime {
    control_state: IcebergCatalogControlState,
    resources: IcebergControlResources,
    catalog: Arc<dyn crate::iceberg::Catalog>,
    rest_catalog: Option<Arc<crate::iceberg_catalog_rest::RestCatalog>>,
    write_activations: Arc<crate::write_activation::IcebergWriteActivationReservations>,
}

#[allow(dead_code)]
impl IcebergControlRuntime {
    /// Construct one fully local provider generation.  REST and HMS client
    /// initialization is polled only through the runtime injected by server
    /// composition, so factory construction remains deterministic in every
    /// frontend role.
    pub fn try_new(
        control_state: IcebergCatalogControlState,
        resources: IcebergControlResources,
    ) -> Result<Self, String> {
        let configuration = control_state.configuration().clone();
        let catalog = resources
            .catalog_runtime()
            .block_on(
                async move { crate::catalog_runtime::build_catalog_client(&configuration).await },
            )?
            .map_err(|error| format!("build Iceberg control-generation catalog: {error}"))?;
        Ok(Self {
            control_state,
            resources,
            catalog: Arc::clone(catalog.generic()),
            rest_catalog: catalog.rest().cloned(),
            write_activations: Arc::new(
                crate::write_activation::IcebergWriteActivationReservations::default(),
            ),
        })
    }

    pub(crate) fn control_state(&self) -> &IcebergCatalogControlState {
        &self.control_state
    }

    pub(crate) fn catalog(&self) -> &Arc<dyn crate::iceberg::Catalog> {
        &self.catalog
    }

    pub(crate) fn rest_catalog(&self) -> Option<&Arc<crate::iceberg_catalog_rest::RestCatalog>> {
        self.rest_catalog.as_ref()
    }

    pub(crate) fn resources(&self) -> &IcebergControlResources {
        &self.resources
    }

    pub(crate) fn load_table(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<IcebergPhysicalTable, String> {
        let namespace = normalize_identifier(namespace)?;
        let table = normalize_identifier(table)?;
        if let Some(table) = self
            .control_state
            .physical_table_cache()
            .get(&namespace, &table)?
        {
            return Ok(table);
        }
        let ident = TableIdent::from_strs([namespace.as_str(), table.as_str()])
            .map_err(|error| format!("build Iceberg table identity: {error}"))?;
        let catalog = Arc::clone(&self.catalog);
        let loaded = self
            .resources
            .catalog_runtime()
            .block_on(async move { catalog.load_table(&ident).await })?
            .map_err(|error| format!("load Iceberg table {namespace}.{table}: {error}"))?;
        let physical =
            IcebergPhysicalTable::new(loaded, self.control_state.object_store_config().cloned());
        self.control_state
            .physical_table_cache()
            .insert(&namespace, &table, physical.clone())?;
        Ok(physical)
    }

    pub(crate) fn list_namespaces(&self) -> Result<Vec<String>, String> {
        let catalog = Arc::clone(&self.catalog);
        let mut namespaces = self
            .resources
            .catalog_runtime()
            .block_on(async move { catalog.list_namespaces(None).await })?
            .map_err(|error| format!("list Iceberg namespaces: {error}"))?
            .into_iter()
            .flat_map(|namespace| {
                namespace
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|namespace| !namespace.starts_with('.'))
            .collect::<Vec<_>>();
        namespaces.sort();
        namespaces.dedup();
        Ok(namespaces)
    }

    pub(crate) fn namespace_exists(&self, namespace: &str) -> Result<bool, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let catalog = Arc::clone(&self.catalog);
        self.resources
            .catalog_runtime()
            .block_on(async move { catalog.namespace_exists(&namespace).await })?
            .map_err(|error| format!("check Iceberg namespace {namespace_label}: {error}"))
    }

    pub(crate) fn list_tables(&self, namespace: &str) -> Result<Vec<String>, String> {
        let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
        let namespace_label = namespace.to_string();
        let catalog = Arc::clone(&self.catalog);
        let mut tables = self
            .resources
            .catalog_runtime()
            .block_on(async move { catalog.list_tables(&namespace).await })?
            .map_err(|error| format!("list Iceberg tables in {namespace_label}: {error}"))?
            .into_iter()
            .map(|table| table.name)
            .collect::<Vec<_>>();
        tables.sort();
        tables.dedup();
        Ok(tables)
    }

    pub(crate) fn table_exists(&self, namespace: &str, table: &str) -> Result<bool, String> {
        let ident = TableIdent::new(
            NamespaceIdent::new(normalize_identifier(namespace)?),
            normalize_identifier(table)?,
        );
        let ident_label = ident.to_string();
        let catalog = Arc::clone(&self.catalog);
        self.resources
            .catalog_runtime()
            .block_on(async move { catalog.table_exists(&ident).await })?
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

impl std::fmt::Debug for IcebergControlRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergControlRuntime")
            .field("control_state", &"<provider catalog state>")
            .field("resources", &self.resources)
            .field("catalog", &"<provider catalog client>")
            .field("rest_catalog", &self.rest_catalog.is_some())
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
        let control = IcebergControlResources::new(binding, runtime.handle().clone());
        let generation =
            IcebergControlRuntime::try_new(IcebergCatalogControlState::new(configuration), control)
                .expect("generation runtime");

        assert_eq!(generation.control_state().properties().len(), 2);
        assert!(Arc::strong_count(generation.catalog()) >= 1);
        assert!(Arc::strong_count(generation.write_activation_reservations()) >= 1);
    }
}
