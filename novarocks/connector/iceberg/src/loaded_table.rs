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

//! Provider-private physical table state captured by a catalog load.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use novarocks_types::naming::normalize_identifier;

#[derive(Clone, Debug)]
pub struct IcebergPhysicalTable {
    pub table: crate::iceberg::table::Table,
}

impl IcebergPhysicalTable {
    pub fn new(table: crate::iceberg::table::Table) -> Self {
        Self { table }
    }

    pub fn into_table(self) -> crate::iceberg::table::Table {
        self.table
    }
}

/// Per-control-generation cache of provider-private physical table state.
/// SQL table projections deliberately do not enter this cache.
#[derive(Clone, Default)]
pub struct IcebergPhysicalTableCache {
    entries: Arc<RwLock<HashMap<(String, String), IcebergPhysicalTable>>>,
}

impl IcebergPhysicalTableCache {
    pub fn get(
        &self,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<Option<IcebergPhysicalTable>, String> {
        let key = cache_key(namespace_name, table_name)?;
        let entries = match self.entries.read() {
            Ok(entries) => entries,
            Err(poisoned) => {
                let message = format!("table cache lock: {poisoned}");
                drop(poisoned.into_inner());
                self.entries.clear_poison();
                if let Ok(mut entries) = self.entries.write() {
                    entries.clear();
                }
                return Err(message);
            }
        };
        Ok(entries.get(&key).cloned())
    }

    pub fn insert(
        &self,
        namespace_name: &str,
        table_name: &str,
        physical: IcebergPhysicalTable,
    ) -> Result<(), String> {
        let key = cache_key(namespace_name, table_name)?;
        let mut entries = self
            .entries
            .write()
            .map_err(|error| format!("table cache lock: {error}"))?;
        entries.insert(key, physical);
        Ok(())
    }

    pub fn invalidate(&self, namespace_name: &str, table_name: &str) {
        let Ok(key) = cache_key(namespace_name, table_name) else {
            return;
        };
        if let Ok(mut entries) = self.entries.write() {
            entries.remove(&key);
        }
    }

    /// Test-only fault injection used by Core integration coverage. This is
    /// intentionally provider-owned: production composition never invokes it.
    #[doc(hidden)]
    pub fn poison_for_test(&self) {
        let entries = Arc::clone(&self.entries);
        let _ = std::thread::spawn(move || {
            let _guard = entries.write().expect("table cache write lock");
            panic!("injected table cache failure");
        })
        .join();
    }
}

fn cache_key(namespace_name: &str, table_name: &str) -> Result<(String, String), String> {
    Ok((
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
    ))
}
