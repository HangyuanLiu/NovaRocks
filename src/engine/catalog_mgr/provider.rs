//! Analyzer-facing adapter over CatalogMgr plus the local InMemoryCatalog.

use crate::connector::ConnectorRegistry;
use crate::engine::catalog::InMemoryCatalog;
use crate::engine::catalog_mgr::CatalogMgr;
use crate::sql::catalog::{CatalogProvider, TableDef, TableLookupMode};

pub(crate) struct CatalogMgrProvider<'a> {
    current_catalog: Option<&'a str>,
    local: &'a InMemoryCatalog,
    catalog_mgr: &'a CatalogMgr,
    connectors: &'a ConnectorRegistry,
    default_mode: TableLookupMode,
}

impl<'a> CatalogMgrProvider<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        local: &'a InMemoryCatalog,
        catalog_mgr: &'a CatalogMgr,
        connectors: &'a ConnectorRegistry,
        default_mode: TableLookupMode,
    ) -> Self {
        Self {
            current_catalog,
            local,
            catalog_mgr,
            connectors,
            default_mode,
        }
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn iceberg_table_def(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        mode: &TableLookupMode,
    ) -> Result<TableDef, String> {
        match mode {
            TableLookupMode::SchemaOnly => self
                .catalog_mgr
                .resolve(catalog, database, table)
                .map(|metadata| metadata.to_table_def()),
            TableLookupMode::ExplainStats
            | TableLookupMode::IcebergMetadata {
                metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType::Partitions,
            } => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_table_def(&resolved)
            }
            TableLookupMode::IcebergMetadata { .. } => self
                .catalog_mgr
                .resolve(catalog, database, table)
                .map(|metadata| metadata.to_table_def()),
        }
    }
}

impl CatalogProvider for CatalogMgrProvider<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get_table_with_mode(None, database, table, self.default_mode.clone())
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<TableDef, String> {
        self.get_table_with_mode(catalog, database, table, self.default_mode.clone())
    }

    fn get_table_with_mode(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        mode: TableLookupMode,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self.local.get_table(database, table),
            Some(catalog) => self.iceberg_table_def(catalog, database, table, &mode),
        }
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<crate::sql::catalog::LegacyRangePartition>, String> {
        self.local
            .get_legacy_range_partition(database, table, partition)
    }

    fn get_physical_layout(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<crate::sql::catalog::PhysicalTableLayout>, String> {
        self.local.get_physical_layout(database, table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableLookupMode,
    };
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    struct FixedIceCatalog;
    impl Catalog for FixedIceCatalog {
        fn name(&self) -> &str {
            "ice"
        }

        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            Ok(TableMetadata {
                identity: TableIdentity::new("ice", namespace, table),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Iceberg {
                    info: iceberg_info(),
                },
            })
        }
    }

    fn iceberg_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("uuid-1".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 3,
            location: "s3://warehouse/db/orders".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        }
    }

    #[test]
    fn provider_resolves_current_catalog_without_mutating_local_catalog() {
        let local = InMemoryCatalog::default();
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(FixedIceCatalog));
        let connectors = crate::connector::ConnectorRegistry::default();
        let provider = CatalogMgrProvider::new(
            Some("ice"),
            &local,
            &mgr,
            &connectors,
            TableLookupMode::SchemaOnly,
        );

        let table = provider.get_table("db", "orders").expect("resolve");

        assert_eq!(table.name, "orders");
        assert!(matches!(table.source, ScanSource::IcebergDataFiles { .. }));
        assert!(local.get("db", "orders").is_err());
    }
}
