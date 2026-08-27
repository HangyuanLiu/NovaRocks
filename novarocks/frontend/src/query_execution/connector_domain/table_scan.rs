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

//! The engine-side typed scan node.
//!
//! Ordered assignments are the sole output-order authority: a connector's
//! projected-column set is a pushdown fact and never reorders anything. The
//! node carries no split list; splits arrive at runtime.

use std::collections::BTreeSet;
use std::fmt;

use novarocks_proto::connector_read::{ConnectorTableScanSource, ValidatedColumnHandle};
use novarocks_proto_models::connector_read as dto;

use super::handle::TableHandle;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TableScanNodeError {
    /// A dynamic filter names a variable this scan does not produce.
    UnboundDynamicFilter { filter_id: u32 },
    /// Two dynamic filters share an id within one scan.
    DuplicateDynamicFilter { filter_id: u32 },
}

impl fmt::Display for TableScanNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnboundDynamicFilter { filter_id } => write!(
                formatter,
                "dynamic filter {filter_id} names a variable this scan does not assign"
            ),
            Self::DuplicateDynamicFilter { filter_id } => {
                write!(formatter, "dynamic filter {filter_id} is declared twice")
            }
        }
    }
}

impl std::error::Error for TableScanNodeError {}

/// One typed connector scan in a fragment plan.
#[derive(Clone, Debug)]
pub(crate) struct TableScanNode {
    plan_node_id: i32,
    table: TableHandle,
    source: ConnectorTableScanSource,
}

impl TableScanNode {
    pub(crate) fn new(
        plan_node_id: i32,
        table: TableHandle,
        source: ConnectorTableScanSource,
    ) -> Result<Self, TableScanNodeError> {
        // The protocol layer already proved every dynamic filter names an
        // assigned variable and that ids are unique; re-check here only what
        // the engine itself depends on, so a future engine-built node cannot
        // bypass the rule.
        let mut seen = BTreeSet::new();
        for binding in source.dynamic_filters() {
            if !seen.insert(binding.filter_id()) {
                return Err(TableScanNodeError::DuplicateDynamicFilter {
                    filter_id: binding.filter_id(),
                });
            }
            if !source
                .assignments()
                .iter()
                .any(|assignment| assignment.variable() == binding.variable())
            {
                return Err(TableScanNodeError::UnboundDynamicFilter {
                    filter_id: binding.filter_id(),
                });
            }
        }
        Ok(Self {
            plan_node_id,
            table,
            source,
        })
    }

    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) const fn table(&self) -> &TableHandle {
        &self.table
    }

    pub(crate) const fn source(&self) -> &ConnectorTableScanSource {
        &self.source
    }

    /// The columns a dynamic filter may constrain on this scan.
    pub(crate) fn dynamic_filter_columns(&self) -> BTreeSet<ValidatedColumnHandle> {
        let named = self
            .source
            .dynamic_filters()
            .iter()
            .map(|binding| binding.variable().to_owned())
            .collect::<BTreeSet<_>>();
        self.source
            .assignments()
            .iter()
            .filter(|assignment| named.contains(assignment.variable()))
            .map(|assignment| assignment.column().clone())
            .collect()
    }

    pub(crate) fn to_proto(&self) -> dto::ConnectorTableScanSource {
        self.source.as_proto().clone()
    }
}

#[cfg(test)]
mod tests {
    use novarocks_proto::FieldPath;

    use super::super::handle::CatalogHandle;
    use super::*;

    fn column(field_id: i32) -> dto::ColumnHandle {
        dto::ColumnHandle {
            handle: Some(dto::column_handle::Handle::Iceberg(
                dto::IcebergColumnHandle {
                    base_column_identity: Some(dto::ColumnIdentity {
                        field_id,
                        name: format!("c{field_id}"),
                        category: dto::ColumnIdentityCategory::Primitive as i32,
                        children: Vec::new(),
                    }),
                    base_type_json: "\"long\"".to_owned(),
                    field_id_path: Vec::new(),
                    type_json: "\"long\"".to_owned(),
                    nullable: true,
                    comment: None,
                },
            )),
        }
    }

    fn value_type() -> dto::ValueType {
        dto::ValueType {
            kind: dto::ValueTypeKind::BigInt as i32,
            decimal_precision: None,
            decimal_scale: None,
            fixed_length: None,
        }
    }

    fn catalog_table_handle() -> dto::CatalogTableHandle {
        dto::CatalogTableHandle {
            catalog_name: "ice".to_owned(),
            instance_incarnation: vec![1_u8; 16],
            transaction: Some(dto::ConnectorTransactionHandle {
                handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                    dto::HiveTransactionHandle {
                        auto_commit: true,
                        uuid: vec![2_u8; 16],
                    },
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::Table(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(
                        dto::IcebergTableHandle {
                            schema_table_name: Some(dto::SchemaTableName {
                                schema_name: "db".to_owned(),
                                table_name: "t".to_owned(),
                            }),
                            snapshot_id: Some(7),
                            table_schema_json: "{}".to_owned(),
                            spec_id: None,
                            partition_spec_jsons: Default::default(),
                            format_version: 2,
                            unenforced_predicate: Some(all_domain()),
                            enforced_predicate: Some(all_domain()),
                            limit: None,
                            pinned_data_files: None,
                            projected_columns: Vec::new(),
                            name_mapping_json: None,
                            table_location: "s3://bucket/table".to_owned(),
                            storage_properties: Default::default(),
                        },
                    )),
                },
            )),
        }
    }

    fn all_domain() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    fn scan_source(dynamic_filters: Vec<dto::DynamicFilterBinding>) -> ConnectorTableScanSource {
        ConnectorTableScanSource::parse(
            dto::ConnectorTableScanSource {
                table: Some(catalog_table_handle()),
                assignments: vec![dto::ScanAssignment {
                    variable: "v0".to_owned(),
                    column: Some(column(1)),
                    value_type: Some(value_type()),
                }],
                enforced_predicate: Some(all_domain()),
                unenforced_predicate: Some(all_domain()),
                remaining_expression: None,
                dynamic_filters,
                max_batch_rows: 4096,
                max_batch_bytes: 1 << 20,
                work_source: dto::ScanWorkSource::RuntimeSplits as i32,
            },
            FieldPath::root("connector_table_scan_source"),
        )
        .expect("valid scan source")
    }

    fn table_handle() -> TableHandle {
        let handle = novarocks_proto::connector_read::CatalogTableHandle::parse(
            catalog_table_handle(),
            FieldPath::root("catalog_table_handle"),
        )
        .expect("valid handle");
        TableHandle::new(CatalogHandle::new("ice", [1; 16]), handle)
    }

    #[test]
    fn a_scan_node_exposes_only_the_columns_a_dynamic_filter_names() {
        let source = scan_source(vec![dto::DynamicFilterBinding {
            filter_id: 3,
            variable: "v0".to_owned(),
        }]);
        let node = TableScanNode::new(11, table_handle(), source).expect("valid node");
        assert_eq!(node.plan_node_id(), 11);
        assert_eq!(node.dynamic_filter_columns().len(), 1);
    }

    #[test]
    fn a_scan_with_no_dynamic_filter_covers_no_column() {
        let node =
            TableScanNode::new(11, table_handle(), scan_source(Vec::new())).expect("valid node");
        assert!(node.dynamic_filter_columns().is_empty());
    }
}
