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

use super::super::decode_type;
use crate::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteExpectedBinding, bind_position_delete_descriptor,
};
use crate::proto::plan;
use crate::protocol::common::error::ProtocolErrorKind;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

fn position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
) -> Result<PositionDeleteDescriptorInput, NativeFragmentLeafDecodeError> {
    let desc = desc.ok_or_else(|| {
        NativeFragmentLeafDecodeError::invalid(
            "native position delete output descriptor is missing",
        )
    })?;
    let file_path = desc.file_path.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "file_path",
            "native position delete file_path descriptor is missing",
        )
    })?;
    let pos = desc.pos.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "pos",
            "native position delete pos descriptor is missing",
        )
    })?;
    Ok(PositionDeleteDescriptorInput {
        file_path: position_delete_output_field_from_native("file_path", file_path)
            .map_err(|error| error.prepend_field("file_path"))?,
        pos: position_delete_output_field_from_native("pos", pos)
            .map_err(|error| error.prepend_field("pos"))?,
        partition_source_fields: desc
            .partition_source_fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                position_delete_partition_source_field_from_native(field).map_err(|error| {
                    error
                        .prepend_index(index)
                        .prepend_field("partition_source_fields")
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: desc.target_partition_spec_id,
    })
}

pub(crate) fn bind_position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
    expected: PositionDeleteExpectedBinding,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorBinding,
    NativeFragmentLeafDecodeError,
> {
    let desc = position_delete_descriptor_from_native(desc)?;
    Ok(bind_position_delete_descriptor(&desc, &expected)
        .map_err(|err| err.to_bracketed_user_message())?)
}

fn position_delete_output_field_from_native(
    label: &str,
    field: &plan::PositionDeleteOutputField,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
    NativeFragmentLeafDecodeError,
> {
    let output_expr_index = usize::try_from(field.output_expr_index).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "output_expr_index",
            format!("native position delete {label} output_expr_index overflows usize"),
        )
    })?;
    let data_type = field.data_type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "data_type",
            format!("native position delete {label} data_type is missing"),
        )
    })?;
    let data_type = decode_type(data_type).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "data_type", error)
    })?;
    Ok(
        crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField {
            output_expr_index,
            name: field.name.clone(),
            data_type,
            field_id: field.field_id,
        },
    )
}

fn position_delete_partition_source_field_from_native(
    field: &plan::PositionDeletePartitionSourceField,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField,
    NativeFragmentLeafDecodeError,
> {
    let output_expr_index = usize::try_from(field.output_expr_index).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "output_expr_index",
            "native position delete partition source output_expr_index overflows usize",
        )
    })?;
    let data_type = field.data_type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "data_type",
            "native position delete partition source data_type is missing",
        )
    })?;
    let data_type = decode_type(data_type).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "data_type", error)
    })?;
    Ok(
        crate::connector::iceberg::position_delete_descriptor::PositionDeletePartitionSourceField {
            output_expr_index,
            source_column_name: field.source_column_name.clone(),
            partition_field_name: field.partition_field_name.clone(),
            transform_expr: field.transform_expr.clone(),
            source_field_id: field.source_field_id,
            data_type,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::connector::iceberg::sink_plan::IcebergSinkMode;
    use crate::proto::{common, expr, plan};
    use crate::types::native_proto::encode_type;

    fn typed_column_def(name: &str, data_type: DataType, nullable: bool) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(encode_type(&data_type).expect("encode type")),
            nullable,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn output_column(id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id: id,
            name: name.to_string(),
            r#type: Some(encode_type(&data_type).expect("encode type")),
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, name: &str, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(encode_type(&data_type).expect("encode type")),
            nullable: false,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id: id,
                qualifier: None,
                column: Some(name.to_string()),
            })),
        }
    }

    fn native_identity_partition_metadata(
        table_location: &str,
        partition_spec_id: i32,
    ) -> iceberg::spec::TableMetadata {
        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                    42,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ))])
                .build()
                .expect("iceberg schema"),
        );
        let partition_spec = iceberg::spec::UnboundPartitionSpec::builder()
            .with_spec_id(partition_spec_id)
            .add_partition_field(42, "id_part", iceberg::spec::Transform::Identity)
            .expect("identity partition field")
            .build();
        iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            table_location.to_string(),
            iceberg::spec::FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .add_current_schema(iceberg_schema.as_ref().clone())
        .expect("add current schema")
        .add_default_partition_spec(partition_spec)
        .expect("add identity partition spec")
        .build()
        .expect("target metadata")
        .metadata
    }

    fn native_identity_partition_metadata_with_unreadable_snapshot(
        table_location: &str,
        partition_spec_id: i32,
        snapshot_id: i64,
    ) -> iceberg::spec::TableMetadata {
        use iceberg::spec::{Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary};

        let metadata = native_identity_partition_metadata(table_location, partition_spec_id);
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(1_900_000_000_000)
            .with_sequence_number(1)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list(format!(
                "{table_location}/metadata/unreadable-snapshot-{snapshot_id}.avro"
            ))
            .with_schema_id(metadata.current_schema_id())
            .build();
        iceberg::spec::TableMetadataBuilder::new_from_metadata(metadata, None)
            .add_snapshot(snapshot)
            .expect("add unreadable snapshot")
            .set_ref(
                "main",
                SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
            )
            .expect("set main ref")
            .build()
            .expect("metadata with unreadable snapshot")
            .metadata
    }

    fn native_iceberg_table_info(serialized_metadata: String) -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("uuid-t".to_string()),
            current_snapshot_id: None,
            schema_id: 1,
            location: "file:///warehouse/t".to_string(),
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![plan::IcebergSchemaFieldDef {
                    field_id: 42,
                    name: "id".to_string(),
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                }],
            }),
            serialized_metadata: Some(serialized_metadata),
            serialized_metadata_rows: None,
        }
    }

    fn position_delete_descriptor(partition_spec_id: i32) -> plan::PositionDeleteDescriptorInput {
        use crate::connector::iceberg::position_delete_descriptor::{
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        };

        plan::PositionDeleteDescriptorInput {
            file_path: Some(plan::PositionDeleteOutputField {
                output_expr_index: 0,
                name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
                data_type: Some(encode_type(&DataType::Utf8).expect("encode type")),
                field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            }),
            pos: Some(plan::PositionDeleteOutputField {
                output_expr_index: 1,
                name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
                data_type: Some(encode_type(&DataType::Int64).expect("encode type")),
                field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            }),
            partition_source_fields: vec![plan::PositionDeletePartitionSourceField {
                output_expr_index: 2,
                source_column_name: "id".to_string(),
                partition_field_name: "id_part".to_string(),
                transform_expr: "identity".to_string(),
                source_field_id: 42,
                data_type: Some(encode_type(&DataType::Int32).expect("encode type")),
            }],
            target_partition_spec_id: partition_spec_id,
        }
    }

    #[test]
    fn partition_source_type_error_preserves_nested_descriptor_path() {
        let mut descriptor = position_delete_descriptor(7);
        descriptor.partition_source_fields[0].data_type = None;

        let error = super::position_delete_descriptor_from_native(Some(&descriptor))
            .expect_err("missing partition source type must fail")
            .into_native(
                crate::protocol::common::error::FieldPath::root("plan_fragment")
                    .field("sink")
                    .field("iceberg_write")
                    .field("spec")
                    .field("position_delete_output_descriptor"),
            );
        let protocol = error.protocol().expect("typed protocol error");
        assert_eq!(
            protocol.path().to_string(),
            "plan_fragment.sink.iceberg_write.spec.position_delete_output_descriptor.partition_source_fields[0].data_type"
        );
        assert_eq!(
            protocol.kind(),
            crate::protocol::common::error::ProtocolErrorKind::MissingField
        );
    }

    #[test]
    fn deletion_vector_sink_lowers_position_delete_descriptor() {
        let table_location = "file:///warehouse/t";
        let metadata = native_identity_partition_metadata(table_location, 7);
        let partition_spec_id = metadata.default_partition_spec_id();
        let serialized_metadata = serde_json::to_string(&metadata).expect("metadata json");
        let sink = plan::IcebergWriteFragmentSink {
            descriptor_database: "db".to_string(),
            spec: Some(plan::IcebergWriteSinkSpec {
                mode: plan::IcebergWriteSinkMode::DeletionVectors as i32,
                target_table_id: 99,
                target_table: Some(plan::TableDef {
                    name: "t".to_string(),
                    columns: vec![typed_column_def("id", DataType::Int32, false)],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: None,
                }),
                iceberg: Some(native_iceberg_table_info(serialized_metadata)),
                target_columns: vec![
                    typed_column_def(
                        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                        DataType::Utf8,
                        false,
                    ),
                    typed_column_def(
                        crate::exec::row_position::ICEBERG_ROW_POS_COL,
                        DataType::Int64,
                        false,
                    ),
                    typed_column_def("id", DataType::Int32, false),
                ],
                table_location: table_location.to_string(),
                data_location: format!("{table_location}/data"),
                target_partition_spec_id: partition_spec_id,
                cloud_properties: HashMap::new(),
                file_format: "parquet".to_string(),
                compression: plan::IcebergWriteFileCompression::Snappy as i32,
                position_delete_output_descriptor: Some(position_delete_descriptor(
                    partition_spec_id,
                )),
            }),
            input: Some(plan::IcebergWriteInputBinding {
                kind: Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(true)),
            }),
        };
        let output_columns = vec![
            output_column(
                10,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
            ),
            output_column(
                11,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
            ),
            output_column(12, "id", DataType::Int32),
        ];
        let layout =
            crate::protocol::native::decode::layout::layout_from_output_columns(&output_columns)
                .expect("layout");
        let output_exprs = vec![
            column_ref_expr(
                10,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
            ),
            column_ref_expr(
                11,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
            ),
            column_ref_expr(12, "id", DataType::Int32),
        ];

        let (input, mode) = super::super::decode_iceberg_write_sink_factory_input(
            &sink,
            &output_exprs,
            &output_columns,
            &layout,
        )
        .expect("native deletion-vector sink input");

        assert_eq!(mode, IcebergSinkMode::DeletionVectors);
        assert!(input.plan.position_delete_binding.is_some());
        assert_eq!(
            input
                .plan
                .output_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["file_path", "pos"]
        );
        assert_eq!(
            input
                .plan
                .target_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id"]
        );
        assert!(input.plan.position_delete_data_file_partitions.is_empty());
    }

    #[test]
    fn deletion_vector_sink_defers_manifest_index_resolution() {
        let table_location = "file:///definitely-not-readable-during-native-decode/t";
        let snapshot_id = 77;
        let metadata = native_identity_partition_metadata_with_unreadable_snapshot(
            table_location,
            7,
            snapshot_id,
        );
        let partition_spec_id = metadata.default_partition_spec_id();
        let serialized_metadata = serde_json::to_string(&metadata).expect("metadata json");
        let mut table = native_iceberg_table_info(serialized_metadata);
        table.location = table_location.to_string();
        table.current_snapshot_id = Some(snapshot_id);
        let sink = plan::IcebergWriteFragmentSink {
            descriptor_database: "db".to_string(),
            spec: Some(plan::IcebergWriteSinkSpec {
                mode: plan::IcebergWriteSinkMode::DeletionVectors as i32,
                target_table_id: 99,
                target_table: Some(plan::TableDef {
                    name: "t".to_string(),
                    columns: vec![typed_column_def("id", DataType::Int32, false)],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: None,
                }),
                iceberg: Some(table),
                target_columns: vec![
                    typed_column_def(
                        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                        DataType::Utf8,
                        false,
                    ),
                    typed_column_def(
                        crate::exec::row_position::ICEBERG_ROW_POS_COL,
                        DataType::Int64,
                        false,
                    ),
                    typed_column_def("id", DataType::Int32, false),
                ],
                table_location: table_location.to_string(),
                data_location: format!("{table_location}/data"),
                target_partition_spec_id: partition_spec_id,
                cloud_properties: HashMap::new(),
                file_format: "parquet".to_string(),
                compression: plan::IcebergWriteFileCompression::Snappy as i32,
                position_delete_output_descriptor: Some(position_delete_descriptor(
                    partition_spec_id,
                )),
            }),
            input: Some(plan::IcebergWriteInputBinding {
                kind: Some(plan::iceberg_write_input_binding::Kind::RootOutputByOrdinal(true)),
            }),
        };
        let output_columns = vec![
            output_column(
                10,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
            ),
            output_column(
                11,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
            ),
            output_column(12, "id", DataType::Int32),
        ];
        let layout =
            crate::protocol::native::decode::layout::layout_from_output_columns(&output_columns)
                .expect("layout");
        let output_exprs = vec![
            column_ref_expr(
                10,
                crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                DataType::Utf8,
            ),
            column_ref_expr(
                11,
                crate::exec::row_position::ICEBERG_ROW_POS_COL,
                DataType::Int64,
            ),
            column_ref_expr(12, "id", DataType::Int32),
        ];

        let (input, mode) = super::super::decode_iceberg_write_sink_factory_input(
            &sink,
            &output_exprs,
            &output_columns,
            &layout,
        )
        .expect("decode must not read the unreadable manifest list");

        assert_eq!(mode, IcebergSinkMode::DeletionVectors);
        assert_eq!(input.plan.target_snapshot_id, Some(snapshot_id));
        assert!(
            input
                .plan
                .position_delete_data_file_partition_index_input
                .is_some()
        );
        assert!(input.plan.position_delete_data_file_partitions.is_empty());

        let materialization_error =
            match crate::connector::iceberg::sink::IcebergTableSinkFactory::try_new(input) {
                Ok(_) => panic!("factory creation must resolve the deferred manifest index"),
                Err(error) => error,
            };
        assert!(materialization_error.contains("position-delete target manifest list"));
    }
}
