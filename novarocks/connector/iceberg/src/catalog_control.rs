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

//! Provider-owned control-generation catalog state.

use std::collections::BTreeMap;
use std::ops::Deref;

use novarocks_catalog::identifier::normalize_identifier;

use crate::catalog_cache::IcebergDataFilesCache;
use crate::catalog_config::IcebergCatalogConfiguration;
use crate::loaded_table::IcebergPhysicalTableCache;
use crate::manifest::DataFileWithStats;

/// State that is private to one Iceberg control generation.
///
/// It carries provider configuration plus physical catalog caches. SQL table
/// projections and application services are intentionally excluded.
#[derive(Clone)]
pub struct IcebergCatalogControlState {
    configuration: IcebergCatalogConfiguration,
    physical_table_cache: IcebergPhysicalTableCache,
    data_files_cache: IcebergDataFilesCache,
}

impl IcebergCatalogControlState {
    pub fn new(configuration: IcebergCatalogConfiguration) -> Self {
        Self {
            configuration,
            physical_table_cache: IcebergPhysicalTableCache::default(),
            data_files_cache: IcebergDataFilesCache::default(),
        }
    }

    pub fn configuration(&self) -> &IcebergCatalogConfiguration {
        &self.configuration
    }

    pub fn physical_table_cache(&self) -> &IcebergPhysicalTableCache {
        &self.physical_table_cache
    }

    pub fn data_files_cache(&self) -> &IcebergDataFilesCache {
        &self.data_files_cache
    }

    pub fn invalidate_table(&self, namespace_name: &str, table_name: &str) {
        self.physical_table_cache
            .invalidate(namespace_name, table_name);
        self.data_files_cache
            .invalidate_table(namespace_name, table_name);
    }

    pub fn properties(&self) -> &[(String, String)] {
        &self.configuration.properties
    }

    pub fn is_s3(&self) -> bool {
        self.configuration.object_store_config.is_some()
    }

    pub fn uses_remote_catalog(&self) -> bool {
        matches!(
            self.configuration.kind,
            crate::catalog_config::IcebergCatalogKind::Rest
                | crate::catalog_config::IcebergCatalogKind::Hive
        )
    }

    pub fn object_store_config(&self) -> Option<&novarocks_fs::ObjectStoreConfig> {
        self.configuration.object_store_config.as_ref()
    }

    pub fn cloud_properties_map(&self) -> BTreeMap<String, String> {
        self.configuration
            .properties
            .iter()
            .filter(|(key, _)| novarocks_fs::AWS_S3_CATALOG_PROPERTY_KEYS.contains(&key.as_str()))
            .cloned()
            .collect()
    }

    pub fn invalidate_table_cache(&self, namespace_name: &str, table_name: &str) {
        if let (Ok(namespace), Ok(table)) = (
            normalize_identifier(namespace_name),
            normalize_identifier(table_name),
        ) {
            self.invalidate_table(&namespace, &table);
        }
    }

    #[doc(hidden)]
    pub fn poison_table_cache_for_test(&self) {
        self.physical_table_cache.poison_for_test();
    }

    pub fn cached_data_files(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
    ) -> Result<Option<Vec<DataFileWithStats>>, String> {
        self.data_files_cache
            .get(namespace_name, table_name, snapshot_id)
    }

    pub fn cache_data_files(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
        data_files: Vec<DataFileWithStats>,
    ) -> Result<(), String> {
        self.data_files_cache
            .insert(namespace_name, table_name, snapshot_id, data_files)
    }
}

impl Deref for IcebergCatalogControlState {
    type Target = IcebergCatalogConfiguration;

    fn deref(&self) -> &Self::Target {
        self.configuration()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn provider_control_state_owns_catalog_properties_and_cache_scope() {
        let state = IcebergCatalogControlState::new(IcebergCatalogConfiguration {
            kind: crate::catalog_config::IcebergCatalogKind::Hadoop,
            warehouse_uri: "file:///tmp/warehouse".to_string(),
            rest_uri: None,
            hms_uris: None,
            properties: vec![
                ("type".to_string(), "iceberg".to_string()),
                (
                    "aws.s3.endpoint".to_string(),
                    "http://minio:9000".to_string(),
                ),
                ("unrelated".to_string(), "ignored".to_string()),
            ],
            object_store_config: None,
            warehouse_path: PathBuf::from("/tmp/warehouse"),
        });

        assert!(!state.is_s3());
        assert!(!state.uses_remote_catalog());
        assert_eq!(state.properties().len(), 3);
        assert_eq!(
            state.cloud_properties_map().get("aws.s3.endpoint"),
            Some(&"http://minio:9000".to_string())
        );
        assert!(!state.cloud_properties_map().contains_key("unrelated"));
    }
}
