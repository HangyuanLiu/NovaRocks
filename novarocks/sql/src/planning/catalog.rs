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

//! Narrow catalog and view-analysis handoffs.
//!
//! SQL syntax is exposed only through [`crate::syntax`]. This module carries
//! catalog materialization and typed view-analysis facts; it does not expose a
//! parser or planner implementation tree.

pub use crate::catalog::{IcebergMetadataTableProvider, PlannerTableProvider, TableLookupMode};
/// SQL metadata-relation vocabulary needed by application catalog admission.
/// This is a value-only DTO; it carries neither a table definition nor a
/// provider handle.
pub use crate::planner::table::SqlMetadataTableKind as MetadataTableKind;
pub use crate::planner::table::TableDef;

/// Build the SQL-owned analyzer relation for an already admitted metadata
/// table. Application code supplies only immutable identity/schema facts and
/// the request-local binding token; the planner graph remains internal.
pub fn resolved_metadata_table(
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_type: MetadataTableKind,
    columns: Vec<novarocks_catalog::schema::ColumnDef>,
    iceberg_row_lineage_metadata_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    binding: crate::binding::SqlTableBindingId,
) -> crate::catalog::ResolvedAnalyzerTable {
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
    };

    let planner = TableDef {
        name: table.to_string(),
        columns,
        iceberg_row_lineage_metadata_columns,
        source: ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
            },
            SqlScanKind::Metadata {
                kind: metadata_table_type,
                version: SqlTableVersionSelector::Current,
            },
        )),
    };
    crate::catalog::ResolvedAnalyzerTable::from_planner(Some(catalog), namespace, planner)
}

/// The local catalog is a neutral in-memory catalog of SQL table definitions.
pub type PlannerMemoryCatalog = novarocks_catalog::memory::MemoryCatalog<TableDef>;

/// One visible output column of a validated external view definition.
#[derive(Clone, Debug, PartialEq)]
pub struct ViewOutputColumn {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

/// Analyze a view query using only the immutable table-provider contract.
/// Catalog application retains the provider and any connector authority.
pub fn analyze_view_query(
    query: &sqlparser::ast::Query,
    provider: &dyn PlannerTableProvider,
    database: &str,
) -> Result<Vec<ViewOutputColumn>, String> {
    let (resolved, _ctes, _factory) = crate::analyzer::analyze(query, provider, database)
        .map_err(|error| format!("analyze view definition failed: {error}"))?;
    Ok(resolved
        .output_columns
        .into_iter()
        .filter(|column| !column.is_internal)
        .map(|column| ViewOutputColumn {
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
        })
        .collect())
}
