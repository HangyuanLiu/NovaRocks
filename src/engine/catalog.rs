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

//! In-memory database/table catalog and shared catalog utilities.
//!
//! Holds the logical `InMemoryCatalog` (databases -> tables). Everything here
//! is backend-agnostic — the StarRocks table and iceberg subsystems both query
//! this catalog for table metadata.

use std::collections::HashMap;

use crate::catalog::identifier::normalize_identifier;
pub use crate::sql::catalog::CatalogProvider;
use crate::sql::catalog::LegacyRangePartition;
#[cfg(any(test, feature = "compat"))]
use crate::sql::planner::table::ScanSource;
use crate::sql::planner::table::TableDef;

#[derive(Clone, Debug)]
struct DatabaseDef {
    tables: HashMap<String, TableDef>,
}

#[derive(Clone, Debug)]
pub(crate) struct InMemoryCatalog {
    databases: HashMap<String, DatabaseDef>,
    legacy_range_partitions: HashMap<(String, String), Vec<LegacyRangePartition>>,
}

pub(crate) const DEFAULT_DATABASE: &str = "default";

impl Default for InMemoryCatalog {
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_string(),
            DatabaseDef {
                tables: HashMap::new(),
            },
        );
        Self {
            databases,
            legacy_range_partitions: HashMap::new(),
        }
    }
}

impl InMemoryCatalog {
    pub(crate) fn create_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if self.databases.contains_key(&key) {
            return Ok(()); // idempotent — matches IF NOT EXISTS semantics
        }
        self.databases.insert(
            key,
            DatabaseDef {
                tables: HashMap::new(),
            },
        );
        Ok(())
    }

    pub(crate) fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        let key = normalize_identifier(database_name)?;
        Ok(self.databases.contains_key(&key))
    }

    pub(crate) fn database_names(&self) -> impl Iterator<Item = &str> {
        self.databases.keys().map(String::as_str)
    }

    /// Enumerate the (already-normalized) table names registered in the
    /// in-memory catalog under `database_name`. Returns an empty list if
    /// the database does not exist.
    pub(crate) fn table_names_in_database(&self, database_name: &str) -> Vec<String> {
        let Ok(db_key) = normalize_identifier(database_name) else {
            return Vec::new();
        };
        self.databases
            .get(&db_key)
            .map(|db| db.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn register(&mut self, database_name: &str, table: TableDef) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(&table.name)?;
        // Allow re-registration (overwrite) — callers use this to update storage.
        db.tables.insert(table_key, table);
        Ok(())
    }

    pub(crate) fn drop_table(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(table_name)?;
        db.tables
            .remove(&table_key)
            .ok_or_else(|| format!("unknown table: {table_name}"))?;
        self.legacy_range_partitions.remove(&(db_key, table_key));
        Ok(())
    }

    pub(crate) fn drop_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if key == DEFAULT_DATABASE {
            return Err("cannot drop default database".to_string());
        }
        self.databases
            .remove(&key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        Ok(())
    }

    pub(crate) fn get(&self, database_name: &str, table_name: &str) -> Result<TableDef, String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        self.databases
            .get(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("unknown table: {table_name}"))
    }

    pub(crate) fn set_legacy_range_partitions(
        &mut self,
        database_name: &str,
        table_name: &str,
        partitions: Vec<LegacyRangePartition>,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        if partitions.is_empty() {
            self.legacy_range_partitions.remove(&(db_key, table_key));
        } else {
            self.legacy_range_partitions
                .insert((db_key, table_key), partitions);
        }
        Ok(())
    }

    pub(crate) fn add_legacy_range_partition(
        &mut self,
        database_name: &str,
        table_name: &str,
        partition: LegacyRangePartition,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        let partition_key = normalize_identifier(&partition.name)?;
        let entries = self
            .legacy_range_partitions
            .entry((db_key, table_key))
            .or_default();
        entries.retain(|existing| {
            normalize_identifier(&existing.name).ok().as_deref() != Some(&partition_key)
        });
        entries.push(partition);
        Ok(())
    }

    pub(crate) fn rename_column(
        &mut self,
        database_name: &str,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        let old_key = normalize_identifier(old_name)?;
        let new_key = normalize_identifier(new_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table = db
            .tables
            .get_mut(&table_key)
            .ok_or_else(|| format!("unknown table: {table_name}"))?;
        if table
            .columns
            .iter()
            .any(|column| normalize_identifier(&column.name).ok().as_deref() == Some(&new_key))
        {
            return Err(format!("column `{new_name}` already exists"));
        }
        let column = table
            .columns
            .iter_mut()
            .find(|column| normalize_identifier(&column.name).ok().as_deref() == Some(&old_key))
            .ok_or_else(|| format!("unknown column `{old_name}`"))?;
        column.name = new_key.clone();

        if let Some(partitions) = self
            .legacy_range_partitions
            .get_mut(&(db_key.clone(), table_key.clone()))
        {
            for partition in partitions {
                if normalize_identifier(&partition.column).ok().as_deref() == Some(&old_key) {
                    partition.column = new_key.clone();
                }
            }
        }
        Ok(())
    }
}

impl CatalogProvider for InMemoryCatalog {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get(database, table)
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        let db_key = normalize_identifier(database)?;
        let table_key = normalize_identifier(table)?;
        let partition_key = normalize_identifier(partition)?;
        Ok(self
            .legacy_range_partitions
            .get(&(db_key, table_key))
            .and_then(|partitions| {
                partitions
                    .iter()
                    .find(|p| normalize_identifier(&p.name).ok().as_deref() == Some(&partition_key))
                    .cloned()
            }))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::catalog::schema::ColumnDef;

    fn test_table(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 10,
                table_id: 20,
            },
        }
    }

    #[test]
    fn register_overwrites_starrocks_logical_table() {
        let mut catalog = InMemoryCatalog::default();
        catalog
            .register(DEFAULT_DATABASE, test_table("starrocks_tbl"))
            .expect("register StarRocks table");
        let mut replacement = test_table("starrocks_tbl");
        replacement.columns[0].nullable = true;

        catalog
            .register(DEFAULT_DATABASE, replacement)
            .expect("overwrite with logical table");
        assert!(
            catalog
                .get(DEFAULT_DATABASE, "starrocks_tbl")
                .expect("overwritten logical table")
                .columns[0]
                .nullable
        );
    }
}
