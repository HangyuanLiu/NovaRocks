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

use crate::planner::table::TableDef;
use novarocks_catalog::memory::MemoryCatalogEntry;

pub use crate::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};
/// SQL metadata-relation vocabulary needed by application catalog admission.
/// This is a value-only DTO; it carries neither a table definition nor a
/// provider handle.
pub use crate::planner::table::SqlMetadataTableKind as MetadataTableKind;

/// Immutable provider-neutral facts for one connector read admitted by the
/// application boundary.  This contains no connector handle or lease: those
/// remain paired with the binding token in the application request store.
pub struct ConnectorReadTableFacts {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub iceberg_row_lineage_metadata_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub schema: arrow::datatypes::SchemaRef,
    pub binding: crate::binding::SqlTableBindingId,
    pub selector: novarocks_spi::connector::ConnectorReadSelector,
    pub planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts,
}

/// Opaque SQL materialization for one admitted connector read.  The embedded
/// analyzer relation is intentionally inaccessible outside this crate.
pub struct ConnectorReadTableMaterialization {
    resolved: crate::catalog::ResolvedAnalyzerTable,
    frozen_snapshot_id: Option<i64>,
}

impl ConnectorReadTableMaterialization {
    pub fn frozen_snapshot_id(&self) -> Option<i64> {
        self.frozen_snapshot_id
    }

    pub fn into_resolved_table(self) -> crate::catalog::ResolvedAnalyzerTable {
        self.resolved
    }
}

/// Copied identity facts from an opaque SQL materialization.
///
/// This is deliberately not a planner table or scan source. Consumers can
/// compare a candidate identity or form a stable digest, but cannot recreate
/// the SQL graph that carried it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlCatalogIdentityFacts {
    catalog: String,
    namespace: String,
    table: String,
}

impl SqlCatalogIdentityFacts {
    pub fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }

    pub fn matches(&self, catalog: &str, namespace: &str, table: &str) -> bool {
        self.catalog == catalog && self.namespace == namespace && self.table == table
    }
}

/// Materialize immutable connector metadata facts as one SQL analyzer table.
/// The request-local binding is the only route from this table to later scan
/// preparation, so this constructor cannot recreate or replace an admission.
pub fn materialize_connector_read_table(
    facts: ConnectorReadTableFacts,
) -> Result<ConnectorReadTableMaterialization, String> {
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    let (scan_kind, frozen_snapshot_id) = match facts.selector {
        novarocks_spi::connector::ConnectorReadSelector::Current => (
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
            None,
        ),
        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id) => (
            SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::Snapshot(snapshot_id),
            },
            Some(snapshot_id),
        ),
        novarocks_spi::connector::ConnectorReadSelector::TimestampMicros(timestamp_micros) => {
            return Err(format!(
                "connector read selector timestamp {timestamp_micros} must resolve to a snapshot before SQL materialization"
            ));
        }
    };
    let ukfk_facts = crate::planner::table::SqlUkFkTableFacts::from_connector_planning_facts(
        &facts.schema,
        &facts.planning_facts,
    );
    let planner = TableDef {
        name: facts.table.clone(),
        columns: facts.columns,
        iceberg_row_lineage_metadata_columns: facts.iceberg_row_lineage_metadata_columns,
        source: ScanSource::Sql(
            SqlScanSource::new(
                facts.binding,
                SqlTableIdentity {
                    catalog: facts.catalog.clone(),
                    namespace: facts.namespace.clone(),
                    table: facts.table,
                },
                scan_kind,
            )
            .with_ukfk_facts(ukfk_facts),
        ),
    };
    Ok(ConnectorReadTableMaterialization {
        resolved: crate::catalog::ResolvedAnalyzerTable::from_planner(
            Some(&facts.catalog),
            &facts.namespace,
            planner,
        ),
        frozen_snapshot_id,
    })
}

/// Attach one application-reserved token to a local SQL relation.
///
/// Local catalog relations have no connector authority, but their scan source
/// must still carry the exact request-local token selected by the application
/// binding store. The planner table and scan source remain SQL-private.
pub fn attach_binding_to_local_materialization(
    mut materialization: crate::catalog::ResolvedAnalyzerTable,
    binding: crate::binding::SqlTableBindingId,
) -> crate::catalog::ResolvedAnalyzerTable {
    let identity = &materialization.catalog.identity;
    materialization.planner.source =
        crate::planner::table::ScanSource::Sql(crate::planner::table::SqlScanSource::new(
            binding,
            crate::planner::table::SqlTableIdentity {
                catalog: identity.catalog.clone(),
                namespace: identity.namespace.clone(),
                table: identity.table.clone(),
            },
            crate::planner::table::SqlScanKind::ConnectorRead,
        ));
    materialization
}

/// Verify that an opaque materialization carries the application-reserved
/// binding token. Any mismatch is a fail-closed admission error.
pub fn validate_materialization_binding(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
    binding: crate::binding::SqlTableBindingId,
) -> Result<(), String> {
    if table_binding_id(materialization) == binding {
        Ok(())
    } else {
        Err(
            "catalog materialization produced a SQL scan with a different request binding"
                .to_string(),
        )
    }
}

/// Copy only catalog identity facts needed by application-side validation and
/// stable digest construction. The analyzer relation and SQL scan graph stay
/// inaccessible outside SQL.
pub fn materialization_identity_facts(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> SqlCatalogIdentityFacts {
    let identity = &materialization.catalog.identity;
    SqlCatalogIdentityFacts {
        catalog: identity.catalog.clone(),
        namespace: identity.namespace.clone(),
        table: identity.table.clone(),
    }
}

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

/// Resolve one local-catalog entry into SQL's opaque analyzer materialization.
/// The application may hold the catalog service, but only SQL may inspect its
/// table definition or create an analyzer relation from it.
pub fn resolve_local_catalog_table(
    catalog: &PlannerMemoryCatalog,
    database: &str,
    table: &str,
) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
    let planner = catalog.get(database, table)?.0;
    Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
        Some("default_catalog"),
        database,
        planner,
    ))
}

/// Read the neutral catalog schema carried by an opaque analyzer
/// materialization. This never exposes the SQL planner table or scan graph.
pub fn catalog_table(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> novarocks_catalog::table::CatalogTable {
    materialization.catalog.clone()
}

/// Read durable local-catalog schema facts without exposing the SQL planner
/// entry used to store them.
pub fn local_catalog_table(
    catalog: &PlannerMemoryCatalog,
    database: &str,
    table: &str,
) -> Result<novarocks_catalog::table::CatalogTable, String> {
    Ok(catalog
        .get(database, table)?
        .to_catalog_table("default_catalog", database))
}

/// Return the request-local binding token carried by an opaque analyzer
/// materialization.  Application validation may compare this value, but it
/// cannot inspect or mutate the SQL scan source.
pub fn table_binding_id(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> crate::binding::SqlTableBindingId {
    match &materialization.planner.source {
        crate::planner::table::ScanSource::Sql(source) => source.binding,
    }
}

/// Return the frozen snapshot only when this table is an admitted frozen-input
/// scan. All other SQL scan forms intentionally collapse to `None`.
pub fn frozen_input_snapshot_id(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> Option<i64> {
    match &materialization.planner.source {
        crate::planner::table::ScanSource::Sql(source) => match source.kind {
            crate::planner::table::SqlScanKind::FrozenInputSet {
                version: crate::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
            } => Some(snapshot_id),
            _ => None,
        },
    }
}

/// Opaque local-catalog entry.  Only SQL may inspect the enclosed planner
/// definition; application code can hold the catalog and request neutral facts
/// through this module.
#[derive(Clone, Debug)]
pub struct SqlLocalCatalogEntry(TableDef);

impl MemoryCatalogEntry for SqlLocalCatalogEntry {
    fn table_name(&self) -> &str {
        &self.0.name
    }

    fn to_catalog_table(
        &self,
        catalog: &str,
        database: &str,
    ) -> novarocks_catalog::table::CatalogTable {
        self.0.to_catalog_table(catalog, database)
    }
}

/// The local catalog is a neutral in-memory catalog whose planner entries are
/// opaque outside SQL.
pub type PlannerMemoryCatalog = novarocks_catalog::memory::MemoryCatalog<SqlLocalCatalogEntry>;

/// Register one closed connector-read relation for an application behavior
/// test. The fixture keeps the `TableDef` and scan-source construction inside
/// SQL; callers supply only catalog-visible schema facts.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn register_test_connector_read_table(
    catalog: &mut PlannerMemoryCatalog,
    database: &str,
    table: &str,
    columns: Vec<novarocks_catalog::schema::ColumnDef>,
) -> Result<(), String> {
    catalog.register(
        database,
        SqlLocalCatalogEntry(TableDef {
            name: table.to_string(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::planner::table::test_sql_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }),
    )
}

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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use super::*;

    fn test_binding_allocator() -> crate::binding::SqlTableBindingAllocator {
        crate::binding::SqlTableBindingAllocator::try_new(
            NonZeroU64::new(17).expect("test scope is nonzero"),
        )
        .expect("test allocator")
    }

    fn connector_read_materialization(
        binding: crate::binding::SqlTableBindingId,
    ) -> crate::catalog::ResolvedAnalyzerTable {
        materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "orders".to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            binding,
            selector: novarocks_spi::connector::ConnectorReadSelector::Current,
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
        })
        .expect("materialize connector test facts")
        .into_resolved_table()
    }

    #[test]
    fn test_catalog_fixture_registers_only_catalog_visible_schema_facts() {
        let mut catalog = PlannerMemoryCatalog::default();
        catalog
            .create_database("analytics")
            .expect("create database");
        register_test_connector_read_table(
            &mut catalog,
            "analytics",
            "orders",
            vec![novarocks_catalog::schema::ColumnDef {
                name: "order_id".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
        )
        .expect("register sealed connector fixture");
        assert!(catalog.get("analytics", "orders").is_ok());
    }

    #[test]
    fn sqlx4a_catalog_binding_facts_are_opaque_and_fail_closed() {
        let mut allocator = test_binding_allocator();
        let original = allocator.allocate().expect("original binding");
        let replacement = allocator.allocate().expect("replacement binding");
        let materialization = connector_read_materialization(original);

        let identity = materialization_identity_facts(&materialization);
        assert_eq!(identity.fqn(), "ice.analytics.orders");
        assert!(identity.matches("ice", "analytics", "orders"));
        assert!(!identity.matches("ice", "analytics", "customers"));
        validate_materialization_binding(&materialization, original)
            .expect("original binding validates");
        assert!(validate_materialization_binding(&materialization, replacement).is_err());

        let local = attach_binding_to_local_materialization(materialization, replacement);
        validate_materialization_binding(&local, replacement)
            .expect("local materialization carries the replacement token");
        assert!(validate_materialization_binding(&local, original).is_err());
        assert_eq!(materialization_identity_facts(&local), identity);
    }
}
