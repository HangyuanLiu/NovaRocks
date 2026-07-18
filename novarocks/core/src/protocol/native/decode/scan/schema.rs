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

use arrow::datatypes::Schema;

use super::common::DecodedScanOutputColumns;
use super::variant_path::NativeVariantPathPlan;
use super::virtual_columns::iceberg_virtual_projected_field;
use crate::common::ids::SlotId;
use crate::connector::iceberg::{
    IcebergArrowColumn, IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor,
    IcebergTableDescriptor, build_projected_output_schema,
};
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::proto::{common, plan};
use crate::protocol::common::error::ProtocolErrorKind;
use crate::protocol::native::decode::NativeFragmentDecodeError;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(super) fn validate_decoded_iceberg_output_schema(
    table: &plan::IcebergTableInfo,
    source_path: crate::protocol::common::error::FieldPath,
    output_columns: &DecodedScanOutputColumns,
    variant_path_plan: &NativeVariantPathPlan,
) -> Result<(), NativeFragmentDecodeError> {
    let descriptor =
        iceberg_table_descriptor(table).map_err(|error| error.into_native(source_path))?;
    let variant_output_fields = variant_path_plan
        .specs
        .iter()
        .map(|spec| (spec.output_slot_id, spec.output_field.clone()))
        .collect::<HashMap<_, _>>();
    for (index, column) in output_columns.columns().iter().enumerate() {
        iceberg_projected_field(table, &descriptor, column, &variant_output_fields)
            .map_err(|error| error.into_native(output_columns.source_path(index)))?;
    }
    Ok(())
}

pub(super) fn iceberg_chunk_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ChunkSchemaRef, NativeFragmentLeafDecodeError> {
    iceberg_chunk_schema_from_output_columns_with_variants(
        table,
        output_columns,
        &NativeVariantPathPlan::default(),
    )
}

pub(super) fn iceberg_chunk_schema_from_output_columns_with_variants(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
    variant_path_plan: &NativeVariantPathPlan,
) -> Result<ChunkSchemaRef, NativeFragmentLeafDecodeError> {
    let slot_ids = output_columns
        .iter()
        .map(|col| SlotId::new(col.column_id))
        .collect::<Vec<_>>();
    let arrow_schema = iceberg_arrow_schema_from_output_columns_with_variants(
        table,
        output_columns,
        variant_path_plan,
    )?;
    ChunkSchema::try_ref_from_schema_and_slot_ids(arrow_schema.as_ref(), &slot_ids).map_err(
        |error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "columns",
                error,
            )
        },
    )
}

pub(super) fn iceberg_arrow_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<std::sync::Arc<Schema>, NativeFragmentLeafDecodeError> {
    iceberg_arrow_schema_from_output_columns_with_variants(
        table,
        output_columns,
        &NativeVariantPathPlan::default(),
    )
}

fn iceberg_arrow_schema_from_output_columns_with_variants(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
    variant_path_plan: &NativeVariantPathPlan,
) -> Result<std::sync::Arc<Schema>, NativeFragmentLeafDecodeError> {
    let descriptor = iceberg_table_descriptor(table)?;
    let variant_output_fields = variant_path_plan
        .specs
        .iter()
        .map(|spec| (spec.output_slot_id, spec.output_field.clone()))
        .collect::<HashMap<_, _>>();
    let mut fields = Vec::with_capacity(output_columns.len());
    for (index, col) in output_columns.iter().enumerate() {
        fields.push(
            iceberg_projected_field(table, &descriptor, col, &variant_output_fields)
                .map_err(|error| error.prepend_index(index).prepend_field("columns"))?,
        );
    }
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

fn iceberg_projected_field(
    table: &plan::IcebergTableInfo,
    descriptor: &IcebergTableDescriptor,
    column: &common::OutputColumn,
    variant_output_fields: &HashMap<SlotId, arrow::datatypes::Field>,
) -> Result<arrow::datatypes::Field, NativeFragmentLeafDecodeError> {
    if let Some(field) = variant_output_fields.get(&SlotId::new(column.column_id)) {
        return Ok(field.clone());
    }
    if let Some(field) = iceberg_virtual_projected_field(table, column)? {
        return Ok(field);
    }
    let desc = column.r#type.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "type",
            format!("output column {} type missing", column.name),
        )
    })?;
    let projected = build_projected_output_schema(
        descriptor,
        &[IcebergArrowColumn {
            name: column.name.clone(),
            data_type: super::super::decode_type(desc).map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "type",
                    error,
                )
            })?,
            nullable: column.nullable,
        }],
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "name", error)
    })?
    .ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "table",
            "IcebergDataFiles table schema missing",
        )
    })?;
    Ok(projected.field(0).clone())
}

fn iceberg_table_descriptor(
    table: &plan::IcebergTableInfo,
) -> Result<IcebergTableDescriptor, NativeFragmentLeafDecodeError> {
    let schema = table.schema.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "table",
            "IcebergDataFiles table schema missing",
        )
    })?;
    Ok(IcebergTableDescriptor {
        columns: Vec::new(),
        iceberg_schema: Some(IcebergSchemaDescriptor {
            fields: schema
                .fields
                .iter()
                .map(iceberg_schema_field_descriptor)
                .collect(),
        }),
        equality_delete_schema: None,
        partition_info: Vec::new(),
        current_snapshot_id: table.current_snapshot_id,
        serialized_metadata: table.serialized_metadata.clone(),
    })
}

fn iceberg_schema_field_descriptor(
    field: &plan::IcebergSchemaFieldDef,
) -> IcebergSchemaFieldDescriptor {
    IcebergSchemaFieldDescriptor {
        name: field.name.clone(),
        field_id: Some(field.field_id),
        children: field
            .children
            .iter()
            .map(iceberg_schema_field_descriptor)
            .collect(),
        initial_default_json: field.initial_default_json.clone(),
    }
}
