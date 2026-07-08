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

//! Lake-native Iceberg MV package discovery.
//!
//! Discovery enumerates lake tables and uses the descriptor package-id marker
//! before parsing the full descriptor.

use std::sync::Arc;

use crate::connector::iceberg::catalog::IcebergLoadedTable;
use crate::connector::iceberg::catalog::registry::{IcebergCatalogEntry, list_tables, load_table};
use crate::engine::StandaloneState;
use crate::meta::repository::mv_descriptor::{MV_DESCRIPTOR_PACKAGE_ID_PROP, MvDescriptorV1};

#[derive(Clone, Debug)]
pub(crate) struct DiscoveredIcebergMv {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) public_name: String,
    pub(crate) table: String,
    pub(crate) descriptor: MvDescriptorV1,
}

pub(crate) fn discover_iceberg_mvs(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
) -> Result<Vec<DiscoveredIcebergMv>, String> {
    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(catalog)?
    };
    discover_iceberg_mvs_from_entry(&entry, catalog, namespace)
}

pub(crate) fn discover_iceberg_mvs_from_entry(
    entry: &IcebergCatalogEntry,
    catalog: &str,
    namespace: &str,
) -> Result<Vec<DiscoveredIcebergMv>, String> {
    let mut discovered = Vec::new();
    for table in list_tables(entry, namespace)? {
        let loaded = load_table(entry, namespace, &table)?;
        let Some(descriptor) = descriptor_from_loaded_table(&loaded)? else {
            continue;
        };
        let expected_package_id = format!("{namespace}.{table}");
        if descriptor.package_id != expected_package_id {
            return Err(format!(
                "Iceberg MV descriptor package id mismatch for discovered table {catalog}.{namespace}.{table}: expected {expected_package_id}, got {}",
                descriptor.package_id
            ));
        }
        discovered.push(DiscoveredIcebergMv {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            public_name: table.clone(),
            table,
            descriptor,
        });
    }
    discovered.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then(left.public_name.cmp(&right.public_name))
            .then(left.table.cmp(&right.table))
    });
    Ok(discovered)
}

fn descriptor_from_loaded_table(
    loaded: &IcebergLoadedTable,
) -> Result<Option<MvDescriptorV1>, String> {
    let props = loaded.table.metadata().properties();
    if !props.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP) {
        return Ok(None);
    }
    MvDescriptorV1::from_storage_properties(props).map(Some)
}
