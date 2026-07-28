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
use crate::sql::catalog::CatalogRuntimeMetadata;
use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::registry::Catalog;
use novarocks_catalog::schema_cache::SchemaCache;

pub(super) struct IcebergCatalog {
    name: String,
    connectors: ConnectorRegistry,
    cache: SchemaCache<CatalogRuntimeMetadata>,
}

impl IcebergCatalog {
    pub(super) fn new(name: &str, connectors: ConnectorRegistry) -> Self {
        Self {
            name: name.to_string(),
            connectors,
            cache: SchemaCache::new(),
        }
    }

    fn invalidate(&self, namespace: &str, table: &str) {
        self.cache
            .invalidate(&TableIdentity::new(&self.name, namespace, table));
    }
}

impl Catalog<CatalogRuntimeMetadata> for IcebergCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<CatalogRuntimeMetadata, String> {
        let identity = TableIdentity::new(&self.name, namespace, table);
        let (table_def, current_schema_id) =
            crate::connector::iceberg::provider::load_schema_table_def(
                &self.connectors,
                crate::connector::connector_request_context(
                    None,
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                )?,
                &self.name,
                namespace,
                table,
            )?;
        self.cache
            .get_or_build_validated(&identity, current_schema_id, || {
                CatalogRuntimeMetadata::from_table_def(identity.clone(), &table_def)
            })
    }

    fn invalidate_table(&self, namespace: &str, table: &str) {
        self.invalidate(namespace, table);
    }
}
