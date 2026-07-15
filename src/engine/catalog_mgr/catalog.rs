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

//! The `Catalog` trait: one named catalog's schema-resolution interface.
//! Implemented by `InternalCatalog` (local/StarRocks) and `IcebergCatalog`.

use crate::engine::catalog_mgr::metadata::TableMetadata;

pub(crate) trait Catalog: Send + Sync {
    /// The catalog's registered name (e.g. "default_catalog", "iceberg_cat_x").
    fn name(&self) -> &str;

    /// Resolve schema-level metadata for `namespace.table`. Returns an error
    /// when the table does not exist or cannot be resolved.
    fn get_table_metadata(&self, namespace: &str, table: &str) -> Result<TableMetadata, String>;

    fn invalidate_table(&self, _namespace: &str, _table: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::identifier::TableIdentity;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableMetadata};

    struct FixedCatalog;

    impl Catalog for FixedCatalog {
        fn name(&self) -> &str {
            "fixed"
        }
        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            if table == "missing" {
                return Err(format!("unknown table: {table}"));
            }
            Ok(TableMetadata {
                identity: TableIdentity::new("fixed", namespace, table),
                columns: vec![],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Internal {
                    db_id: 1,
                    table_id: 2,
                },
            })
        }
    }

    #[test]
    fn catalog_trait_object_resolves_table() {
        let cat: Box<dyn Catalog> = Box::new(FixedCatalog);
        assert_eq!(cat.name(), "fixed");
        let meta = cat.get_table_metadata("ns", "t").expect("resolve");
        assert_eq!(meta.identity.table, "t");
        assert!(cat.get_table_metadata("ns", "missing").is_err());
    }
}
