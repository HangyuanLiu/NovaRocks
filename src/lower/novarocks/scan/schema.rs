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

use super::variant_path::NativeVariantPathPlan;
use super::virtual_columns::iceberg_virtual_projected_field;
use crate::common::ids::SlotId;
use crate::connector::iceberg::{
    IcebergArrowColumn, IcebergSchemaDescriptor, IcebergSchemaFieldDescriptor,
    IcebergTableDescriptor, build_projected_output_schema,
};
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::proto::{common, plan};

pub(super) fn iceberg_chunk_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<ChunkSchemaRef, String> {
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
) -> Result<ChunkSchemaRef, String> {
    let slot_ids = output_columns
        .iter()
        .map(|col| SlotId::new(col.column_id))
        .collect::<Vec<_>>();
    let arrow_schema = iceberg_arrow_schema_from_output_columns_with_variants(
        table,
        output_columns,
        variant_path_plan,
    )?;
    ChunkSchema::try_ref_from_schema_and_slot_ids(arrow_schema.as_ref(), &slot_ids)
}

pub(super) fn iceberg_arrow_schema_from_output_columns(
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<std::sync::Arc<Schema>, String> {
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
) -> Result<std::sync::Arc<Schema>, String> {
    let descriptor = iceberg_table_descriptor(table)?;
    let variant_output_fields = variant_path_plan
        .specs
        .iter()
        .map(|spec| (spec.output_slot_id, spec.output_field.clone()))
        .collect::<HashMap<_, _>>();
    let mut fields = Vec::with_capacity(output_columns.len());
    for col in output_columns {
        if let Some(field) = variant_output_fields.get(&SlotId::new(col.column_id)) {
            fields.push(field.clone());
            continue;
        }
        if let Some(field) = iceberg_virtual_projected_field(table, col)? {
            fields.push(field);
            continue;
        }
        let desc = col
            .r#type
            .as_ref()
            .ok_or_else(|| format!("ScanNode output column {} type missing", col.name))?;
        let projected = build_projected_output_schema(
            &descriptor,
            &[IcebergArrowColumn {
                name: col.name.clone(),
                data_type: super::super::decode_type(desc)?,
                nullable: col.nullable,
            }],
        )?
        .ok_or_else(|| "IcebergDataFiles table schema missing".to_string())?;
        fields.push(projected.field(0).clone());
    }
    Ok(std::sync::Arc::new(Schema::new(fields)))
}

fn iceberg_table_descriptor(
    table: &plan::IcebergTableInfo,
) -> Result<IcebergTableDescriptor, String> {
    let schema = table
        .schema
        .as_ref()
        .ok_or_else(|| "IcebergDataFiles table schema missing".to_string())?;
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
