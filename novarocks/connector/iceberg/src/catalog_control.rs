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

use std::ops::Deref;

use crate::catalog_cache::IcebergDataFilesCache;
use crate::catalog_config::IcebergCatalogConfiguration;
use crate::loaded_table::IcebergPhysicalTableCache;

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
}

impl Deref for IcebergCatalogControlState {
    type Target = IcebergCatalogConfiguration;

    fn deref(&self) -> &Self::Target {
        self.configuration()
    }
}
