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

use iceberg::spec::TableMetadata;

use super::super::decode_type;
use crate::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteExpectedBinding, bind_position_delete_descriptor,
};
use crate::connector::iceberg::sink::build_staged_file_io;
use crate::connector::iceberg::sink_plan::{
    IcebergSinkObjectStoreConfig, PositionDeleteDataFilePartition,
};
use crate::proto::plan;
use crate::runtime::global_async_runtime::data_block_on;

pub(crate) fn build_position_delete_data_file_partition_index(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
    table_location: &str,
    s3_config: Option<&IcebergSinkObjectStoreConfig>,
) -> Result<HashMap<String, PositionDeleteDataFilePartition>, String> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    let Some(snapshot_id) = target_snapshot_id.or_else(|| metadata.current_snapshot_id()) else {
        return Ok(HashMap::new());
    };
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        format!(
            "native Iceberg delete sink target snapshot id {snapshot_id} not found in table metadata"
        )
    })?;
    let file_io = build_staged_file_io(table_location, s3_config)?;
    data_block_on(async {
        let manifest_list = snapshot
            .load_manifest_list(&file_io, metadata)
            .await
            .map_err(|e| {
                format!("load native Iceberg position-delete target manifest list: {e}")
            })?;
        let mut index = HashMap::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(&file_io).await.map_err(|e| {
                format!(
                    "load native Iceberg position-delete data manifest {} failed: {e}",
                    manifest_file.manifest_path
                )
            })?;
            for entry in manifest.entries() {
                if entry.status == ManifestStatus::Deleted {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    continue;
                }
                let partition = PositionDeleteDataFilePartition {
                    partition_spec_id: manifest_file.partition_spec_id,
                    partition_values: data_file.partition().clone(),
                };
                insert_position_delete_data_file_partition(
                    &mut index,
                    data_file.file_path().to_string(),
                    partition,
                )?;
            }
        }
        Ok(index)
    })?
}

fn insert_position_delete_data_file_partition(
    index: &mut HashMap<String, PositionDeleteDataFilePartition>,
    path: String,
    partition: PositionDeleteDataFilePartition,
) -> Result<(), String> {
    match index.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
            Ok(())
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            Err(format!(
                "native Iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ))
        }
    }
}

fn position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
) -> Result<PositionDeleteDescriptorInput, String> {
    let desc =
        desc.ok_or_else(|| "native position delete output descriptor is missing".to_string())?;
    let file_path = desc
        .file_path
        .as_ref()
        .ok_or_else(|| "native position delete file_path descriptor is missing".to_string())?;
    let pos = desc
        .pos
        .as_ref()
        .ok_or_else(|| "native position delete pos descriptor is missing".to_string())?;
    Ok(PositionDeleteDescriptorInput {
        file_path: position_delete_output_field_from_native("file_path", file_path)?,
        pos: position_delete_output_field_from_native("pos", pos)?,
        partition_source_fields: desc
            .partition_source_fields
            .iter()
            .map(position_delete_partition_source_field_from_native)
            .collect::<Result<Vec<_>, _>>()?,
        target_partition_spec_id: desc.target_partition_spec_id,
    })
}

pub(crate) fn bind_position_delete_descriptor_from_native(
    desc: Option<&plan::PositionDeleteDescriptorInput>,
    expected: PositionDeleteExpectedBinding,
) -> Result<
    crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorBinding,
    String,
> {
    let desc = position_delete_descriptor_from_native(desc)?;
    bind_position_delete_descriptor(&desc, &expected).map_err(|err| err.to_bracketed_user_message())
}

fn position_delete_output_field_from_native(
    label: &str,
    field: &plan::PositionDeleteOutputField,
) -> Result<crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField, String>
{
    let output_expr_index = usize::try_from(field.output_expr_index)
        .map_err(|_| format!("native position delete {label} output_expr_index overflows usize"))?;
    let data_type = field
        .data_type
        .as_ref()
        .ok_or_else(|| format!("native position delete {label} data_type is missing"))
        .and_then(decode_type)?;
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
    String,
> {
    let output_expr_index = usize::try_from(field.output_expr_index).map_err(|_| {
        "native position delete partition source output_expr_index overflows usize".to_string()
    })?;
    let data_type = field
        .data_type
        .as_ref()
        .ok_or_else(|| "native position delete partition source data_type is missing".to_string())
        .and_then(decode_type)?;
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
    use crate::sql::codegen::proto_encode::types::encode_type;

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
        let layout = crate::lower::novarocks::layout::layout_from_output_columns(&output_columns)
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

        let (input, mode) = super::super::lower_iceberg_write_sink_factory_input(
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
}
