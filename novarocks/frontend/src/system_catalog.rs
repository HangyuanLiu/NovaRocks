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

//! Frontend-owned system catalog: the information_schema virtual-table registry
//! and providers. Implements core's `SystemCatalog` port; core resolves through
//! the port and never names these types (FEH-3).

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog_application::system_catalog::{
    SystemCatalog, SystemCatalogInputs, SystemTableData,
};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::schema::ColumnDef;

const INFORMATION_SCHEMA_DB: &str = "information_schema";

/// Contract for a single information_schema virtual table. Unlike the former
/// core trait, `scan` receives only the narrow inputs it needs — never engine
/// state.
trait VirtualTableProvider: Send + Sync {
    fn database(&self) -> &str;
    fn table(&self) -> &str;
    fn columns(&self) -> Vec<ColumnDef>;
    fn scan(&self, inputs: &SystemCatalogInputs<'_>) -> Result<Vec<RecordBatch>, String>;
}

struct VirtualTableRegistry {
    providers: HashMap<(String, String), Arc<dyn VirtualTableProvider>>,
}

impl VirtualTableRegistry {
    fn with_defaults() -> Self {
        let mut registry = Self {
            providers: HashMap::new(),
        };
        registry.register(Arc::new(SchemataProvider));
        registry.register(Arc::new(TablesProvider));
        registry
    }

    fn register(&mut self, provider: Arc<dyn VirtualTableProvider>) {
        let key = (
            provider.database().to_ascii_lowercase(),
            provider.table().to_ascii_lowercase(),
        );
        self.providers.insert(key, provider);
    }

    fn lookup(&self, database: &str, table: &str) -> Option<Arc<dyn VirtualTableProvider>> {
        self.providers
            .get(&(database.to_ascii_lowercase(), table.to_ascii_lowercase()))
            .cloned()
    }
}

const SCHEMATA_COLUMNS: &[(&str, bool)] = &[
    ("catalog_name", false),
    ("schema_name", false),
    ("default_character_set_name", false),
    ("default_collation_name", false),
    ("sql_path", true),
];

fn schemata_columns() -> Vec<ColumnDef> {
    SCHEMATA_COLUMNS
        .iter()
        .map(|(name, nullable)| ColumnDef {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: *nullable,
            write_default: None,
            logical_type: None,
        })
        .collect()
}

/// Build the single schemata `RecordBatch`: one row per schema, `catalog_name`
/// fixed to `catalog`. Byte-identical to the former core `build_schemata_batch`
/// (`information_schema.rs`); schema exactly matches `schemata_columns()`.
fn build_schemata_batch(catalog: &str, databases: &[String]) -> Result<Vec<RecordBatch>, String> {
    let row_count = databases.len();
    let catalog_name = StringArray::from(vec![catalog; row_count]);
    let schema_name = StringArray::from_iter_values(databases.iter().map(String::as_str));
    let default_charset = StringArray::from(vec!["utf8"; row_count]);
    let default_collation = StringArray::from(vec!["utf8_general_ci"; row_count]);
    let sql_path: StringArray = std::iter::repeat_n(None::<&str>, row_count).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("default_character_set_name", DataType::Utf8, false),
        Field::new("default_collation_name", DataType::Utf8, false),
        Field::new("sql_path", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(catalog_name) as ArrayRef,
            Arc::new(schema_name) as ArrayRef,
            Arc::new(default_charset) as ArrayRef,
            Arc::new(default_collation) as ArrayRef,
            Arc::new(sql_path) as ArrayRef,
        ],
    )
    .map_err(|e| format!("build information_schema.schemata batch failed: {e}"))?;
    Ok(vec![batch])
}

const TABLES_COLUMNS: &[(&str, bool)] = &[
    ("table_catalog", false),
    ("table_schema", false),
    ("table_name", false),
    ("table_type", false),
];

fn tables_columns() -> Vec<ColumnDef> {
    TABLES_COLUMNS
        .iter()
        .map(|(name, nullable)| ColumnDef {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: *nullable,
            write_default: None,
            logical_type: None,
        })
        .collect()
}

/// One row per table, so a namespace's contents are knowable to SQL.
///
/// Without this a caller can only drop children it can already name, which is
/// no help to anyone who did not create them -- and on a catalog that cannot
/// enumerate views, `DROP DATABASE ... FORCE` is refused, so naming them is the
/// only way through.
fn build_tables_batch(
    catalog: &str,
    tables: &[(String, String)],
) -> Result<Vec<RecordBatch>, String> {
    let row_count = tables.len();
    let table_catalog = StringArray::from(vec![catalog; row_count]);
    let table_schema =
        StringArray::from_iter_values(tables.iter().map(|(schema, _)| schema.as_str()));
    let table_name = StringArray::from_iter_values(tables.iter().map(|(_, name)| name.as_str()));
    // Every row is a base table: this listing comes from the catalog's table
    // enumeration, and a catalog that also holds views reports those through
    // the view metadata surface instead.
    let table_type = StringArray::from(vec!["BASE TABLE"; row_count]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_catalog", DataType::Utf8, false),
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(table_catalog) as ArrayRef,
            Arc::new(table_schema) as ArrayRef,
            Arc::new(table_name) as ArrayRef,
            Arc::new(table_type) as ArrayRef,
        ],
    )
    .map_err(|e| format!("build information_schema.tables batch failed: {e}"))?;
    Ok(vec![batch])
}

struct TablesProvider;

impl VirtualTableProvider for TablesProvider {
    fn database(&self) -> &str {
        INFORMATION_SCHEMA_DB
    }

    fn table(&self) -> &str {
        "tables"
    }

    fn columns(&self) -> Vec<ColumnDef> {
        tables_columns()
    }

    fn scan(&self, inputs: &SystemCatalogInputs<'_>) -> Result<Vec<RecordBatch>, String> {
        build_tables_batch(inputs.catalog_name, inputs.table_names)
    }
}

struct SchemataProvider;

impl VirtualTableProvider for SchemataProvider {
    fn database(&self) -> &str {
        INFORMATION_SCHEMA_DB
    }

    fn table(&self) -> &str {
        "schemata"
    }

    fn columns(&self) -> Vec<ColumnDef> {
        schemata_columns()
    }

    fn scan(&self, inputs: &SystemCatalogInputs<'_>) -> Result<Vec<RecordBatch>, String> {
        build_schemata_batch(inputs.catalog_name, inputs.schema_names)
    }
}

/// The frontend system-catalog domain service.
pub struct SystemCatalogService {
    registry: VirtualTableRegistry,
}

impl SystemCatalogService {
    pub fn with_defaults() -> Self {
        Self {
            registry: VirtualTableRegistry::with_defaults(),
        }
    }
}

impl SystemCatalog for SystemCatalogService {
    fn resolve(
        &self,
        db: &str,
        tbl: &str,
        inputs: &SystemCatalogInputs<'_>,
    ) -> Result<Option<SystemTableData>, String> {
        match self.registry.lookup(db, tbl) {
            Some(provider) => Ok(Some(SystemTableData {
                columns: provider.columns(),
                batches: provider.scan(inputs)?,
            })),
            None => Ok(None),
        }
    }
}
