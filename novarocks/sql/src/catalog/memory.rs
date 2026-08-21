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

use std::collections::HashMap;

use novarocks_types::naming::{DEFAULT_DATABASE, normalize_identifier};

use crate::planner::table::TableDef;

const DEFAULT_CATALOG: &str = "default_catalog";

#[derive(Clone, Debug)]
struct DatabaseDef {
    tables: HashMap<String, TableDef>,
}

/// SQL-owned local catalog. Its planner entries remain private to this crate;
/// application code receives only catalog-visible materializations through
/// `planning::catalog`.
#[derive(Clone, Debug)]
pub struct PlannerMemoryCatalog {
    databases: HashMap<String, DatabaseDef>,
}

impl Default for PlannerMemoryCatalog {
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_string(),
            DatabaseDef {
                tables: HashMap::new(),
            },
        );
        Self { databases }
    }
}

impl PlannerMemoryCatalog {
    pub fn create_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if self.databases.contains_key(&key) {
            return Ok(());
        }
        self.databases.insert(
            key,
            DatabaseDef {
                tables: HashMap::new(),
            },
        );
        Ok(())
    }

    pub fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        let key = normalize_identifier(database_name)?;
        Ok(self.databases.contains_key(&key))
    }

    pub fn database_names(&self) -> impl Iterator<Item = &str> {
        self.databases.keys().map(String::as_str)
    }

    pub fn table_names_in_database(&self, database_name: &str) -> Vec<String> {
        let Ok(db_key) = normalize_identifier(database_name) else {
            return Vec::new();
        };
        self.databases
            .get(&db_key)
            .map(|database| database.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn register(&mut self, database_name: &str, table: TableDef) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let database = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(&table.name)?;
        database.tables.insert(table_key, table);
        Ok(())
    }

    pub fn drop_table(&mut self, database_name: &str, table_name: &str) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let database = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(table_name)?;
        database
            .tables
            .remove(&table_key)
            .ok_or_else(|| format!("unknown table: {table_name}"))?;
        Ok(())
    }

    pub fn drop_database(&mut self, database_name: &str) -> Result<(), String> {
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
}

impl crate::catalog::PlannerTableProvider for PlannerMemoryCatalog {
    fn resolve_table_for_analysis(
        &self,
        _catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
            Some(DEFAULT_CATALOG),
            database,
            self.get(database, table)?,
        ))
    }

    fn iceberg_metadata_provider(
        &self,
    ) -> Option<&dyn crate::catalog::IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl crate::catalog::IcebergMetadataTableProvider for PlannerMemoryCatalog {
    fn get_iceberg_metadata_table(
        &self,
        _catalog: Option<&str>,
        database: &str,
        table: &str,
        _metadata_table_type: crate::planner::table::SqlMetadataTableKind,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
            Some(DEFAULT_CATALOG),
            database,
            self.get(database, table)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::PlannerMemoryCatalog;
    use novarocks_types::naming::DEFAULT_DATABASE;

    fn test_table(name: &str) -> crate::planner::table::TableDef {
        crate::planner::table::TableDef {
            name: name.to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::planner::table::test_sql_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }
    }

    #[test]
    fn creates_lists_and_drops_databases_with_normalized_names() {
        let mut catalog = PlannerMemoryCatalog::default();

        assert!(
            catalog
                .database_exists("DEFAULT")
                .expect("default database")
        );
        catalog
            .create_database("  `Sales_2026`  ")
            .expect("create normalized database");
        catalog
            .create_database("sales_2026")
            .expect("idempotent create");

        let mut names = catalog.database_names().collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, vec![DEFAULT_DATABASE, "sales_2026"]);

        catalog
            .drop_database("SALES_2026")
            .expect("drop normalized database");
        assert!(
            !catalog
                .database_exists("sales_2026")
                .expect("database absent")
        );
        assert_eq!(
            catalog.drop_database(DEFAULT_DATABASE),
            Err("cannot drop default database".to_string())
        );
        assert_eq!(
            catalog.drop_database("Missing"),
            Err("unknown database: Missing".to_string())
        );
    }

    #[test]
    fn registers_overwrites_lists_gets_and_drops_tables() {
        let mut catalog = PlannerMemoryCatalog::default();
        catalog.create_database("Sales").expect("create database");

        catalog
            .register("SALES", test_table("  `Orders_2026`  "))
            .expect("register normalized table");
        assert_eq!(
            catalog
                .get("sales", "orders_2026")
                .expect("registered table")
                .name,
            "  `Orders_2026`  "
        );
        assert_eq!(
            catalog.table_names_in_database("`Sales`"),
            vec!["orders_2026".to_string()]
        );

        catalog
            .register("sales", test_table("ORDERS_2026"))
            .expect("overwrite table");
        assert_eq!(
            catalog
                .get("SALES", "Orders_2026")
                .expect("replacement")
                .name,
            "ORDERS_2026"
        );

        catalog
            .drop_table("sales", "ORDERS_2026")
            .expect("drop table");
        assert!(catalog.table_names_in_database("sales").is_empty());
    }

    #[test]
    fn preserves_exact_unknown_database_and_table_errors() {
        let mut catalog = PlannerMemoryCatalog::default();

        assert_eq!(
            catalog.register("MissingDb", test_table("t")),
            Err("unknown database: MissingDb".to_string())
        );
        assert!(matches!(
            catalog.get("MissingDb", "t"),
            Err(error) if error == "unknown database: MissingDb"
        ));
        assert_eq!(
            catalog.drop_table("MissingDb", "t"),
            Err("unknown database: MissingDb".to_string())
        );

        catalog.create_database("db").expect("create database");
        assert!(matches!(
            catalog.get("db", "MissingTable"),
            Err(error) if error == "unknown table: MissingTable"
        ));
        assert_eq!(
            catalog.drop_table("db", "MissingTable"),
            Err("unknown table: MissingTable".to_string())
        );
        assert!(catalog.table_names_in_database("missing").is_empty());
        assert!(catalog.table_names_in_database("bad-name").is_empty());
    }
}
