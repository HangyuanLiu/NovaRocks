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

//! SQL-owned facts for a distributed table-write sink.
//!
//! A compiler may reason about a write target's identity, schema, partition
//! layout, and row-lineage shape.  It must not retain a provider table object,
//! serialized metadata, object-store properties, prepared writer, or connector
//! handle.  The application binding store owns those authorities and converts
//! this contract into a connector-specific writer only after placement is
//! frozen.

use std::collections::BTreeSet;

use arrow::datatypes::DataType;
use novarocks_catalog::schema::ColumnDef;

use crate::sql::analysis::TypedExpr;
use crate::sql::binding::SqlTableBindingId;
use crate::sql::planner::table::SqlTableIdentity;

/// Generic Arrow input selection for a connector batch writer. This is a SQL
/// physical-planning fact: it identifies output columns, never provider
/// metadata or a connector handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConnectorWriteInputBinding {
    RootOutputByOrdinal,
    OutputOrdinals(Vec<usize>),
}

/// SQL-owned terminal writer input. Application code converts the completed
/// contract into a connector-specific writer after it looks up the exact
/// request-local binding token.
#[derive(Clone, Debug)]
pub(crate) struct SqlWritePlanInput {
    pub(crate) contract: SqlWriteSinkContract,
    pub(crate) input: ConnectorWriteInputBinding,
    /// A root-only projection supplied by SQL when hidden or state columns
    /// must be materialized immediately before the terminal sink.
    pub(crate) root_output_exprs: Option<Vec<TypedExpr>>,
}

/// Logical operation performed by the SQL terminal write sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

/// A SQL-level target field. `field_id` is the target schema identity, not a
/// fragment output ordinal and not a provider object reference.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlWriteTargetField {
    pub(crate) field_id: i32,
    pub(crate) column: ColumnDef,
    pub(crate) is_hidden: bool,
}

/// A partition transform exposed to SQL planning. Provider encoders translate
/// this closed vocabulary explicitly; they do not recover it from metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlWritePartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { buckets: u32 },
    Truncate { width: u32 },
    Void,
}

impl SqlWritePartitionTransform {
    /// Stable SQL spelling used by typed position-delete and write contracts.
    pub(crate) fn sql_name(&self) -> String {
        match self {
            Self::Identity => "identity".to_string(),
            Self::Year => "year".to_string(),
            Self::Month => "month".to_string(),
            Self::Day => "day".to_string(),
            Self::Hour => "hour".to_string(),
            Self::Bucket { buckets } => format!("bucket[{buckets}]"),
            Self::Truncate { width } => format!("truncate[{width}]"),
            Self::Void => "void".to_string(),
        }
    }
}

/// One SQL-visible partition field of the write target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlWritePartitionField {
    pub(crate) name: String,
    pub(crate) source_field_id: i32,
    pub(crate) transform: SqlWritePartitionTransform,
}

/// Immutable partition layout selected by the frozen catalog binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlWritePartitionContract {
    pub(crate) spec_id: i32,
    pub(crate) fields: Vec<SqlWritePartitionField>,
}

/// SQL facts for the two physical position-delete columns and any partition
/// source columns emitted by the terminal plan.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlPositionDeleteOutputDescriptor {
    pub(crate) file_path: SqlPositionDeleteOutputField,
    pub(crate) pos: SqlPositionDeleteOutputField,
    pub(crate) partition_source_fields: Vec<SqlPositionDeletePartitionSourceField>,
    pub(crate) target_partition_spec_id: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlPositionDeleteOutputField {
    pub(crate) output_expr_index: usize,
    pub(crate) name: String,
    pub(crate) data_type: DataType,
    pub(crate) field_id: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlPositionDeletePartitionSourceField {
    pub(crate) output_expr_index: usize,
    pub(crate) source_column_name: String,
    pub(crate) partition_field_name: String,
    pub(crate) transform: SqlWritePartitionTransform,
    pub(crate) source_field_id: i32,
    pub(crate) data_type: DataType,
}

/// The compiler-visible write target. The binding token is the sole route back
/// to application-owned provider authority; it is intentionally not
/// serializable and cannot be reused by another request/store.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlWriteSinkTargetContract {
    pub(crate) binding: SqlTableBindingId,
    pub(crate) table: SqlTableIdentity,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) fields: Vec<SqlWriteTargetField>,
    pub(crate) partition: SqlWritePartitionContract,
}

impl SqlWriteSinkTargetContract {
    pub(crate) fn try_new(
        binding: SqlTableBindingId,
        table: SqlTableIdentity,
        target_snapshot_id: Option<i64>,
        fields: Vec<SqlWriteTargetField>,
        partition: SqlWritePartitionContract,
    ) -> Result<Self, String> {
        if table.catalog.is_empty() || table.namespace.is_empty() || table.table.is_empty() {
            return Err("SQL write target requires a canonical table identity".to_string());
        }
        if fields.is_empty() {
            return Err("SQL write target requires at least one target field".to_string());
        }

        let mut field_ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for field in &fields {
            if !field_ids.insert(field.field_id) {
                return Err(format!(
                    "SQL write target contains duplicate field id {}",
                    field.field_id
                ));
            }
            if !names.insert(field.column.name.clone()) {
                return Err(format!(
                    "SQL write target contains duplicate field name {}",
                    field.column.name
                ));
            }
        }

        let target_field_ids = fields
            .iter()
            .map(|field| field.field_id)
            .collect::<BTreeSet<_>>();
        let mut partition_names = BTreeSet::new();
        for field in &partition.fields {
            if !target_field_ids.contains(&field.source_field_id) {
                return Err(format!(
                    "SQL write partition field {} references unknown target field id {}",
                    field.name, field.source_field_id
                ));
            }
            if !partition_names.insert(field.name.clone()) {
                return Err(format!(
                    "SQL write target contains duplicate partition field {}",
                    field.name
                ));
            }
        }

        Ok(Self {
            binding,
            table,
            target_snapshot_id,
            fields,
            partition,
        })
    }
}

/// Complete compiler-owned terminal write contract. It intentionally has no
/// storage location, cloud property, serialized provider metadata, writer
/// handle, or prepared operation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SqlWriteSinkContract {
    pub(crate) mode: SqlWriteSinkMode,
    pub(crate) target: SqlWriteSinkTargetContract,
    pub(crate) input_columns: Vec<ColumnDef>,
    pub(crate) position_delete_output: Option<SqlPositionDeleteOutputDescriptor>,
}

impl SqlWriteSinkContract {
    pub(crate) fn try_new(
        mode: SqlWriteSinkMode,
        target: SqlWriteSinkTargetContract,
        input_columns: Vec<ColumnDef>,
        position_delete_output: Option<SqlPositionDeleteOutputDescriptor>,
    ) -> Result<Self, String> {
        if input_columns.is_empty() {
            return Err("SQL write sink requires at least one input column".to_string());
        }
        let position_delete_mode = matches!(
            mode,
            SqlWriteSinkMode::PositionDeletes | SqlWriteSinkMode::DeletionVectors
        );
        if position_delete_mode != position_delete_output.is_some() {
            return Err(
                "SQL write sink position-delete descriptor does not match write mode".to_string(),
            );
        }
        if let Some(descriptor) = &position_delete_output {
            validate_position_delete_descriptor(descriptor, &target.partition)?;
        }
        Ok(Self {
            mode,
            target,
            input_columns,
            position_delete_output,
        })
    }
}

fn validate_position_delete_descriptor(
    descriptor: &SqlPositionDeleteOutputDescriptor,
    partition: &SqlWritePartitionContract,
) -> Result<(), String> {
    if descriptor.target_partition_spec_id != partition.spec_id {
        return Err(format!(
            "SQL position-delete descriptor partition spec id {} does not match target spec id {}",
            descriptor.target_partition_spec_id, partition.spec_id
        ));
    }
    if descriptor.file_path.output_expr_index != 0
        || descriptor.file_path.name != "file_path"
        || descriptor.file_path.data_type != DataType::Utf8
    {
        return Err("SQL position-delete descriptor has invalid file_path output".to_string());
    }
    if descriptor.pos.output_expr_index != 1
        || descriptor.pos.name != "pos"
        || descriptor.pos.data_type != DataType::Int64
    {
        return Err("SQL position-delete descriptor has invalid pos output".to_string());
    }
    if descriptor.partition_source_fields.len() != partition.fields.len() {
        return Err(format!(
            "SQL position-delete descriptor partition source count {} does not match target partition field count {}",
            descriptor.partition_source_fields.len(),
            partition.fields.len()
        ));
    }
    for (index, (source, target)) in descriptor
        .partition_source_fields
        .iter()
        .zip(&partition.fields)
        .enumerate()
    {
        if source.output_expr_index != index + 2
            || source.partition_field_name != target.name
            || source.source_field_id != target.source_field_id
            || source.transform != target.transform
        {
            return Err(format!(
                "SQL position-delete descriptor partition source {} does not match target partition contract",
                target.name
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::binding::SqlTableBindingScopeId;

    pub(crate) fn simple_sql_write_plan_input(
        input: ConnectorWriteInputBinding,
    ) -> SqlWritePlanInput {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(92).expect("non-zero scope")),
            NonZeroU32::new(1).expect("non-zero ordinal"),
        );
        let column = ColumnDef {
            name: "order_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        let target = SqlWriteSinkTargetContract::try_new(
            binding,
            SqlTableIdentity {
                catalog: "iceberg".to_string(),
                namespace: "analytics".to_string(),
                table: "orders".to_string(),
            },
            Some(42),
            vec![SqlWriteTargetField {
                field_id: 1,
                column: column.clone(),
                is_hidden: false,
            }],
            SqlWritePartitionContract {
                spec_id: 7,
                fields: Vec::new(),
            },
        )
        .expect("valid SQL target");
        SqlWritePlanInput {
            contract: SqlWriteSinkContract::try_new(
                SqlWriteSinkMode::Data,
                target,
                vec![column],
                None,
            )
            .expect("valid SQL write contract"),
            input,
            root_output_exprs: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::binding::SqlTableBindingScopeId;

    fn binding() -> SqlTableBindingId {
        SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(91).expect("non-zero scope")),
            NonZeroU32::new(1).expect("non-zero ordinal"),
        )
    }

    fn column(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn target() -> SqlWriteSinkTargetContract {
        SqlWriteSinkTargetContract::try_new(
            binding(),
            SqlTableIdentity {
                catalog: "iceberg".to_string(),
                namespace: "analytics".to_string(),
                table: "orders".to_string(),
            },
            Some(42),
            vec![SqlWriteTargetField {
                field_id: 1,
                column: column("order_id", DataType::Int64),
                is_hidden: false,
            }],
            SqlWritePartitionContract {
                spec_id: 7,
                fields: vec![SqlWritePartitionField {
                    name: "order_id_bucket".to_string(),
                    source_field_id: 1,
                    transform: SqlWritePartitionTransform::Bucket { buckets: 16 },
                }],
            },
        )
        .expect("valid SQL target")
    }

    #[test]
    fn sqlx2_write_sink_contract_keeps_only_binding_and_sql_facts() {
        let target = target();
        let contract = SqlWriteSinkContract::try_new(
            SqlWriteSinkMode::Data,
            target.clone(),
            vec![column("order_id", DataType::Int64)],
            None,
        )
        .expect("valid write contract");

        assert_eq!(contract.target.binding, binding());
        assert_eq!(contract.target.table.table, "orders");
        assert_eq!(contract.target.target_snapshot_id, Some(42));
        assert_eq!(
            contract.target.partition.fields[0].transform.sql_name(),
            "bucket[16]"
        );
    }

    #[test]
    fn sqlx2_write_sink_contract_rejects_mismatched_position_descriptor() {
        let descriptor = SqlPositionDeleteOutputDescriptor {
            file_path: SqlPositionDeleteOutputField {
                output_expr_index: 0,
                name: "file_path".to_string(),
                data_type: DataType::Utf8,
                field_id: 2_147_483_546,
            },
            pos: SqlPositionDeleteOutputField {
                output_expr_index: 1,
                name: "pos".to_string(),
                data_type: DataType::Int64,
                field_id: 2_147_483_545,
            },
            partition_source_fields: Vec::new(),
            target_partition_spec_id: 8,
        };
        let error = SqlWriteSinkContract::try_new(
            SqlWriteSinkMode::PositionDeletes,
            target(),
            vec![
                column("file_path", DataType::Utf8),
                column("pos", DataType::Int64),
            ],
            Some(descriptor),
        )
        .expect_err("mismatched target partition spec must fail");

        assert!(error.contains("partition spec id 8 does not match target spec id 7"));
    }
}
