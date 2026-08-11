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

//! Provider-owned cache of Iceberg manifest facts.
//!
//! The cache is scoped to one provider control generation. Its values are
//! Iceberg manifest facts, never Core SQL projections or application state.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use novarocks_catalog::identifier::normalize_identifier;

use crate::manifest::DataFileWithStats;

type CacheKey = (String, String, Option<i64>);

#[derive(Clone, Default)]
pub struct IcebergDataFilesCache {
    entries: Arc<RwLock<HashMap<CacheKey, Vec<DataFileWithStats>>>>,
}

impl IcebergDataFilesCache {
    pub fn get(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
    ) -> Result<Option<Vec<DataFileWithStats>>, String> {
        let key = cache_key(namespace_name, table_name, snapshot_id)?;
        let entries = self
            .entries
            .read()
            .map_err(|error| format!("iceberg data-file cache lock: {error}"))?;
        Ok(entries.get(&key).cloned())
    }

    pub fn insert(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
        data_files: Vec<DataFileWithStats>,
    ) -> Result<(), String> {
        let key = cache_key(namespace_name, table_name, snapshot_id)?;
        let mut entries = self
            .entries
            .write()
            .map_err(|error| format!("iceberg data-file cache lock: {error}"))?;
        entries.insert(key, data_files);
        Ok(())
    }

    pub fn invalidate_table(&self, namespace_name: &str, table_name: &str) {
        let (Ok(namespace), Ok(table)) = (
            normalize_identifier(namespace_name),
            normalize_identifier(table_name),
        ) else {
            return;
        };
        if let Ok(mut entries) = self.entries.write() {
            entries.retain(|(cached_namespace, cached_table, _), _| {
                cached_namespace != &namespace || cached_table != &table
            });
        }
    }
}

fn cache_key(
    namespace_name: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<CacheKey, String> {
    Ok((
        normalize_identifier(namespace_name)?,
        normalize_identifier(table_name)?,
        snapshot_id,
    ))
}

#[cfg(test)]
mod tests {
    use super::IcebergDataFilesCache;
    use crate::manifest::DataFileWithStats;

    fn data_file(path: &str) -> DataFileWithStats {
        DataFileWithStats {
            path: path.to_string(),
            size: 1,
            record_count: None,
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: vec![],
            first_row_id: None,
            data_sequence_number: None,
            delete_files: vec![],
        }
    }

    #[test]
    fn cache_is_snapshot_scoped_and_table_invalidation_is_exact() {
        let cache = IcebergDataFilesCache::default();
        cache
            .insert("Db", "Orders", Some(1), vec![data_file("file:///one")])
            .expect("insert snapshot one");
        cache
            .insert("db", "orders", Some(2), vec![data_file("file:///two")])
            .expect("insert snapshot two");
        cache
            .insert("db", "other", Some(1), vec![data_file("file:///other")])
            .expect("insert other table");

        assert_eq!(
            cache
                .get("DB", "ORDERS", Some(1))
                .expect("get snapshot one")
                .expect("snapshot one cached")[0]
                .path,
            "file:///one"
        );

        cache.invalidate_table("db", "orders");
        assert!(
            cache
                .get("db", "orders", Some(1))
                .expect("get invalidated snapshot")
                .is_none()
        );
        assert!(
            cache
                .get("db", "orders", Some(2))
                .expect("get second invalidated snapshot")
                .is_none()
        );
        assert!(
            cache
                .get("db", "other", Some(1))
                .expect("get retained snapshot")
                .is_some()
        );
    }
}
