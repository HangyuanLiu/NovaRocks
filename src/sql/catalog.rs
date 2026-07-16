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

use crate::catalog::identifier::TableIdentity;
use crate::catalog::table::CatalogTable;
use crate::sql::planner::table::TableDef;

mod conversion;
mod iceberg;
mod internal;
pub(crate) mod local;
pub(crate) mod metadata;
pub(crate) mod provider;

use metadata::{CatalogRuntimeBinding, CatalogRuntimeMetadata};

#[derive(Clone, Debug)]
pub(crate) struct ResolvedAnalyzerTable {
    pub(crate) catalog: CatalogTable,
    pub(crate) planner: TableDef,
}

impl ResolvedAnalyzerTable {
    pub(crate) fn from_planner(catalog: Option<&str>, database: &str, planner: TableDef) -> Self {
        let identity = TableIdentity::new(
            catalog.unwrap_or("default_catalog"),
            database,
            &planner.name,
        );
        let table = CatalogTable {
            identity,
            columns: planner.columns.clone(),
            hidden_columns: planner.iceberg_row_lineage_metadata_columns.clone(),
        };
        Self {
            catalog: table,
            planner,
        }
    }
}

/// Planner-facing table materialization extension.
///
/// This is the only ordinary analyzer/planner lookup seam: implementations
/// must return the neutral schema and planner binding from one authoritative
/// resolution.
pub(crate) trait PlannerTableProvider {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String>;

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        None
    }
}

pub(crate) trait IcebergMetadataTableProvider {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String>;
}
