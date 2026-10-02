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

//! Exact analyzed Arrow fields to Connector CREATE facts for CTAS.

use novarocks_spi::connector::ConnectorColumnDefinition;

pub(crate) fn arrow_schema_to_connector_columns(
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<ConnectorColumnDefinition>, String> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Ok(ConnectorColumnDefinition {
                name: std::sync::Arc::from(field.name().as_str()),
                data_type: novarocks_types::logical_type::logical_value_from_engine_arrow(field)?
                    .data_type,
                nullable: field.is_nullable(),
                aggregation: None,
                default: None,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::arrow_schema_to_connector_columns;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use novarocks_types::logical_type::{LogicalField, LogicalType, LogicalValue};
    use std::sync::Arc;

    #[test]
    fn ctas_preserves_recursive_required_children_and_order() {
        let schema = Schema::new(vec![
            Field::new(
                "record",
                DataType::Struct(
                    vec![
                        Field::new(
                            "z",
                            DataType::List(Arc::new(Field::new("element", DataType::Int64, false))),
                            false,
                        ),
                        Field::new("a", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new(
                "map",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Int32, false),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]);
        let fields = arrow_schema_to_connector_columns(&schema).unwrap();
        assert!(!fields[0].nullable);
        assert_eq!(
            fields[0].data_type,
            LogicalType::Struct(vec![
                LogicalField {
                    name: "z".into(),
                    data_type: LogicalType::Array {
                        element: Box::new(LogicalValue {
                            data_type: LogicalType::Int64,
                            nullable: false
                        }),
                        fixed_length: None
                    },
                    nullable: false
                },
                LogicalField {
                    name: "a".into(),
                    data_type: LogicalType::Utf8,
                    nullable: true
                },
            ])
        );
        assert_eq!(
            fields[1].data_type,
            LogicalType::Map {
                key: Box::new(LogicalValue {
                    data_type: LogicalType::Utf8,
                    nullable: false
                }),
                value: Box::new(LogicalValue {
                    data_type: LogicalType::Int32,
                    nullable: false
                })
            }
        );
    }

    #[test]
    fn ctas_retains_proved_root_markers_and_time_parameters() {
        let json = Field::new("json", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([("nr_logical_type".into(), "json".into())]),
        );
        let schema = Schema::new(vec![
            json,
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("amount", DataType::Decimal256(45, 4), true),
        ]);
        let fields = arrow_schema_to_connector_columns(&schema).unwrap();
        assert_eq!(fields[0].data_type, LogicalType::Json);
        assert_eq!(
            fields[1].data_type,
            LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: Some("UTC".into())
            }
        );
        assert_eq!(
            fields[2].data_type,
            LogicalType::Decimal {
                bits: 256,
                precision: 45,
                scale: 4
            }
        );
        assert!(
            fields
                .iter()
                .all(|field| field.default.is_none() && field.aggregation.is_none())
        );
    }

    #[test]
    fn ctas_offset_dictionary_carriers_do_not_change_semantic_type() {
        let schema = Schema::new(vec![
            Field::new(
                "value",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                true,
            ),
            Field::new(
                "list",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
        ]);
        let fields = arrow_schema_to_connector_columns(&schema).unwrap();
        assert_eq!(fields[0].data_type, LogicalType::Utf8);
        assert_eq!(
            fields[1].data_type,
            LogicalType::Array {
                element: Box::new(LogicalValue {
                    data_type: LogicalType::Utf8,
                    nullable: false
                }),
                fixed_length: None
            }
        );
    }

    #[test]
    fn ctas_rejects_unknown_semantics_before_create() {
        let field = Field::new("bad", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([("nr_logical_type".into(), "unknown".into())]),
        );
        assert!(arrow_schema_to_connector_columns(&Schema::new(vec![field])).is_err());
        assert!(
            arrow_schema_to_connector_columns(&Schema::new(vec![Field::new(
                "interval",
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                true
            )]))
            .is_err()
        );
    }
    // ---------- IF NOT EXISTS parser test ----------

    #[test]
    fn parse_create_table_if_not_exists_sets_flag() {
        use novarocks_parser::ast::{DmlStatement, Statement, TableStatement};

        let parsed = novarocks_parser::parse("CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x")
            .expect("parse");
        let [Statement::Dml(DmlStatement::CreateTableAsSelect(stmt))] = parsed.as_slice() else {
            panic!("expected CTAS");
        };
        let TableStatement::Create(table) = &stmt.table;
        assert!(
            table.if_not_exists,
            "IF NOT EXISTS must set the if_not_exists field to true"
        );
        assert_eq!(
            novarocks_parser::printer::print_query(&stmt.query),
            "SELECT 1 AS x"
        );
    }

    #[test]
    fn parse_create_table_without_if_not_exists_flag_is_false() {
        use novarocks_parser::ast::{DmlStatement, Statement, TableStatement};

        let parsed = novarocks_parser::parse("CREATE TABLE t AS SELECT 1 AS x").expect("parse");
        let [Statement::Dml(DmlStatement::CreateTableAsSelect(stmt))] = parsed.as_slice() else {
            panic!("expected CTAS");
        };
        let TableStatement::Create(table) = &stmt.table;
        assert!(
            !table.if_not_exists,
            "without IF NOT EXISTS the flag should be false"
        );
    }
}
