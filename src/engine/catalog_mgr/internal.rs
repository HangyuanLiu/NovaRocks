//! `InternalCatalog`: a `Catalog` over the existing `InMemoryCatalog`, serving
//! local / StarRocks tables (registered at CREATE time, stable schema). Shares
//! the same `InMemoryCatalog` instance as the rest of the engine via `Arc`.

use std::sync::{Arc, RwLock};

use crate::engine::catalog::InMemoryCatalog;
use crate::engine::catalog_mgr::catalog::Catalog;
use crate::engine::catalog_mgr::metadata::{TableIdentity, TableMetadata};

pub(crate) struct InternalCatalog {
    name: String,
    inner: Arc<RwLock<InMemoryCatalog>>,
}

impl InternalCatalog {
    pub(crate) fn new(name: &str, inner: Arc<RwLock<InMemoryCatalog>>) -> Self {
        Self {
            name: name.to_string(),
            inner,
        }
    }
}

impl Catalog for InternalCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(&self, namespace: &str, table: &str) -> Result<TableMetadata, String> {
        let td = self
            .inner
            .read()
            .expect("internal catalog read lock")
            .get(namespace, table)?;
        let identity = TableIdentity::new(&self.name, namespace, table);
        TableMetadata::from_table_def(identity, &td)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::TableBinding;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use std::sync::{Arc, RwLock};

    fn starrocks_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 5,
                table_id: 6,
            },
        }
    }

    #[test]
    fn resolves_registered_internal_table() {
        let mut inner = InMemoryCatalog::default();
        inner.create_database("db").expect("create db");
        inner
            .register("db", starrocks_table_def())
            .expect("register");

        let cat = InternalCatalog::new("default_catalog", Arc::new(RwLock::new(inner)));
        let meta = cat.get_table_metadata("db", "t").expect("resolve");

        assert_eq!(meta.identity.catalog, "default_catalog");
        assert_eq!(meta.columns.len(), 1);
        assert_eq!(
            meta.binding,
            TableBinding::Internal {
                db_id: 5,
                table_id: 6
            }
        );
    }

    #[test]
    fn missing_table_errors() {
        let inner = InMemoryCatalog::default();
        let cat = InternalCatalog::new("default_catalog", Arc::new(RwLock::new(inner)));
        assert!(cat.get_table_metadata("db", "nope").is_err());
    }
}
