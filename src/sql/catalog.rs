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

use crate::sql::planner::table::TableDef;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyRangePartition {
    pub name: String,
    pub column: String,
    pub lower_sql: String,
    pub upper_sql: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableLookupMode {
    SchemaOnly,
    IcebergMetadata {
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    },
    ExplainStats,
}

/// Catalog abstraction for SQL analysis.
///
/// This remains the analyzer adapter surface. Engine entrypoints that resolve
/// standalone external catalogs should construct a `CatalogMgrProvider` instead
/// of reintroducing query-scoped global `InMemoryCatalog` registration.
pub trait CatalogProvider {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String>;

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<TableDef, String> {
        let _ = catalog;
        self.get_table(database, table)
    }

    fn get_table_with_mode(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        mode: TableLookupMode,
    ) -> Result<TableDef, String> {
        let _ = mode;
        self.get_table_in_catalog(catalog, database, table)
    }

    fn get_legacy_range_partition(
        &self,
        _database: &str,
        _table: &str,
        _partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        Ok(None)
    }
}
