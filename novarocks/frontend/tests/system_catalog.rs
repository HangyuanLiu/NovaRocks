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

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_frontend::SystemCatalogService;
use novarocks_frontend::catalog_application::system_catalog::{SystemCatalog, SystemCatalogInputs};

fn inputs<'a>(catalog_name: &'a str, schema_names: &'a [String]) -> SystemCatalogInputs<'a> {
    SystemCatalogInputs {
        catalog_name,
        schema_names,
        table_names: &[],
    }
}

fn table_inputs<'a>(
    catalog_name: &'a str,
    table_names: &'a [(String, String)],
) -> SystemCatalogInputs<'a> {
    SystemCatalogInputs {
        catalog_name,
        schema_names: &[],
        table_names,
    }
}

#[test]
fn resolve_schemata_returns_exact_columns() {
    let schema_names = vec!["db_a".to_string(), "db_b".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &inputs("default_catalog", &schema_names),
        )
        .expect("schemata resolution must succeed")
        .expect("schemata must be registered");

    let actual: Vec<_> = resolved
        .columns
        .iter()
        .map(|column| (column.name.as_str(), &column.data_type, column.nullable))
        .collect();
    assert_eq!(
        actual,
        vec![
            ("catalog_name", &DataType::Utf8, false),
            ("schema_name", &DataType::Utf8, false),
            ("default_character_set_name", &DataType::Utf8, false),
            ("default_collation_name", &DataType::Utf8, false),
            ("sql_path", &DataType::Utf8, true),
        ]
    );
}

#[test]
fn resolve_schemata_rows_match_inputs() {
    let schema_names = vec!["db_a".to_string(), "db_b".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &inputs("default_catalog", &schema_names),
        )
        .expect("schemata resolution must succeed")
        .expect("schemata must be registered");

    assert_eq!(resolved.batches.len(), 1);
    let batch = &resolved.batches[0];
    assert_eq!(batch.num_rows(), 2);

    let catalog_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("catalog_name must be Utf8");
    assert_eq!(catalog_names.value(0), "default_catalog");
    assert_eq!(catalog_names.value(1), "default_catalog");

    let actual_schema_names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("schema_name must be Utf8");
    assert_eq!(actual_schema_names.value(0), "db_a");
    assert_eq!(actual_schema_names.value(1), "db_b");
}

#[test]
fn resolve_schemata_uses_input_catalog_name() {
    let schema_names = vec!["analytics".to_string(), "staging".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "schemata",
            &inputs("myice", &schema_names),
        )
        .expect("schemata resolution must succeed")
        .expect("schemata must be registered");

    let catalog_names = resolved.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("catalog_name must be Utf8");
    assert_eq!(catalog_names.value(0), "myice");
    assert_eq!(catalog_names.value(1), "myice");
}

#[test]
fn resolve_unknown_table_returns_none() {
    let schema_names = vec!["db_a".to_string()];
    // This once used `tables` as its example of an unregistered name. That name
    // is a real provider now, so the case needs one that genuinely is not.
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "columns",
            &inputs("default_catalog", &schema_names),
        )
        .expect("unknown table resolution must succeed");

    assert!(resolved.is_none());
}

#[test]
fn resolve_is_case_insensitive() {
    let schema_names = vec!["db_a".to_string()];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "INFORMATION_SCHEMA",
            "SCHEMATA",
            &inputs("default_catalog", &schema_names),
        )
        .expect("schemata resolution must succeed");

    assert!(resolved.is_some());
}

/// The listing that makes a namespace's contents knowable to SQL.
///
/// Without it a caller can only drop children it can already name, and on a
/// catalog that cannot enumerate views `DROP DATABASE ... FORCE` is refused --
/// so naming them is the only way through.
#[test]
fn resolve_tables_reports_one_row_per_table() {
    let tables = vec![
        ("db_a".to_string(), "t1".to_string()),
        ("db_a".to_string(), "t2".to_string()),
        ("db_b".to_string(), "t3".to_string()),
    ];
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "tables",
            &table_inputs("ice_cat", &tables),
        )
        .expect("tables resolution must succeed")
        .expect("tables must be registered");

    let names: Vec<&str> = resolved
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["table_catalog", "table_schema", "table_name", "table_type"]
    );
    assert!(
        resolved
            .columns
            .iter()
            .all(|column| column.data_type == DataType::Utf8)
    );

    let batch = resolved.batches.first().expect("one batch");
    assert_eq!(batch.num_rows(), 3);
    let schema = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("schema column");
    let table = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("table column");
    assert_eq!(schema.value(0), "db_a");
    assert_eq!(table.value(0), "t1");
    assert_eq!(schema.value(2), "db_b");
    assert_eq!(table.value(2), "t3");
}

#[test]
fn resolve_tables_on_an_empty_namespace_is_an_empty_listing_not_a_failure() {
    let resolved = SystemCatalogService::with_defaults()
        .resolve(
            "information_schema",
            "tables",
            &table_inputs("ice_cat", &[]),
        )
        .expect("tables resolution must succeed")
        .expect("tables must be registered");
    assert_eq!(
        resolved.batches.first().expect("one batch").num_rows(),
        0,
        "a namespace with no tables lists none, which is a real answer"
    );
}
