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

use crate::connector::ConnectorRegistry;
use crate::connector::IcebergCatalogRegistry;
use crate::sql::catalog::{
    CatalogRuntimeMetadata, IcebergMetadataTableProvider, PlannerTableProvider,
    ResolvedAnalyzerTable, TableLookupMode,
};
use crate::sql::planner::table::TableDef;
use novarocks_catalog::partition::LegacyRangePartition;
use novarocks_catalog::provider::CatalogProvider;
use novarocks_catalog::service::CatalogService;
use novarocks_catalog::table::CatalogTable;

pub(crate) struct CatalogServiceProvider<'a> {
    current_catalog: Option<&'a str>,
    service: &'a CatalogService<TableDef, CatalogRuntimeMetadata>,
    connectors: &'a ConnectorRegistry,
    iceberg_catalogs: &'a std::sync::Arc<std::sync::RwLock<IcebergCatalogRegistry>>,
    lookup_mode: TableLookupMode,
}

impl<'a> CatalogServiceProvider<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        service: &'a CatalogService<TableDef, CatalogRuntimeMetadata>,
        connectors: &'a ConnectorRegistry,
        iceberg_catalogs: &'a std::sync::Arc<std::sync::RwLock<IcebergCatalogRegistry>>,
        lookup_mode: TableLookupMode,
    ) -> Self {
        Self {
            current_catalog,
            service,
            connectors,
            iceberg_catalogs,
            lookup_mode,
        }
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                let planner = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock")
                    .get(database, table)?;
                Ok(ResolvedAnalyzerTable::from_planner(
                    Some("default_catalog"),
                    database,
                    planner,
                ))
            }
            Some(catalog) => match self.lookup_mode {
                TableLookupMode::SchemaOnly => {
                    let metadata = self
                        .service
                        .registry()
                        .read()
                        .expect("catalog service registry read lock")
                        .resolve(catalog, database, table)?;
                    let planner = metadata.to_table_def();
                    Ok(ResolvedAnalyzerTable {
                        catalog: metadata.table,
                        planner,
                    })
                }
                TableLookupMode::ExplainStats => {
                    let (planner, _) = crate::connector::iceberg::provider::load_schema_table_def(
                        self.connectors,
                        self.iceberg_catalogs,
                        catalog,
                        database,
                        table,
                    )?;
                    Ok(ResolvedAnalyzerTable::from_planner(
                        Some(catalog),
                        database,
                        planner,
                    ))
                }
            },
        }
    }

    fn iceberg_metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self
                .service
                .local()
                .read()
                .expect("catalog service local read lock")
                .get(database, table),
            Some(catalog) => crate::connector::iceberg::provider::load_metadata_table_def(
                self.connectors,
                self.iceberg_catalogs,
                catalog,
                database,
                table,
                metadata_table_type,
            ),
        }
    }
}

impl CatalogProvider for CatalogServiceProvider<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(None, database, table)
            .map(|resolved| resolved.catalog)
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
            .map(|resolved| resolved.catalog)
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        self.service
            .local()
            .read()
            .expect("catalog service local read lock")
            .get_legacy_range_partition(database, table, partition)
    }
}

impl PlannerTableProvider for CatalogServiceProvider<'_> {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl IcebergMetadataTableProvider for CatalogServiceProvider<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String> {
        self.iceberg_metadata_table_def(catalog, database, table, metadata_table_type)
    }
}
