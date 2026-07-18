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

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use iceberg::spec::TableMetadata;

use super::metadata::arrow_field_id;
use crate::connector::iceberg::commit::EqualityDeleteColumn;
use crate::connector::iceberg::schema::{IcebergTableDescriptor, apply_field_id_recursive};
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(crate) fn validate_equality_delete_unpartitioned_target_metadata(
    iceberg: &IcebergTableDescriptor,
    target_partition_spec_id: i32,
) -> Result<(), NativeFragmentLeafDecodeError> {
    let Some(serialized) = iceberg.serialized_metadata.as_ref() else {
        return Ok(());
    };
    let metadata = serde_json::from_str::<TableMetadata>(serialized)
        .map_err(|e| format!("parse native Iceberg equality-delete target metadata failed: {e}"))?;
    let spec = metadata
        .partition_spec_by_id(target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "native Iceberg equality-delete sink target partition spec id {target_partition_spec_id} not found"
            )
        })?;
    if !spec.fields().is_empty() {
        return Err(format!(
            "native Iceberg equality-delete sink currently supports only unpartitioned tables; \
            target partition spec id {target_partition_spec_id} has {} fields",
            spec.fields().len()
        )
        .into());
    }
    Ok(())
}

pub(crate) fn build_equality_delete_output_schema(
    iceberg: &IcebergTableDescriptor,
) -> Result<(SchemaRef, Vec<EqualityDeleteColumn>), NativeFragmentLeafDecodeError> {
    let columns = &iceberg.columns;
    if columns.is_empty() {
        return Err("native Iceberg equality-delete sink requires equality columns".into());
    }
    let key_fields = iceberg
        .equality_delete_schema
        .as_ref()
        .ok_or_else(|| {
            "native Iceberg equality-delete sink requires projected key fields".to_string()
        })?
        .fields
        .as_slice();
    if key_fields.is_empty() {
        return Err("native Iceberg equality-delete sink requires equality columns".into());
    }
    let mut fields = Vec::with_capacity(key_fields.len());
    let mut equality_columns = Vec::with_capacity(key_fields.len());
    for schema_field in key_fields {
        let column = columns
            .iter()
            .find(|column| column.name == schema_field.name)
            .ok_or_else(|| {
                format!(
                    "native Iceberg equality-delete column {} missing descriptor",
                    schema_field.name
                )
            })?;
        let field = Field::new(
            column.name.clone(),
            column.data_type.clone(),
            column.nullable,
        );
        let field = apply_field_id_recursive(field, schema_field)?;
        let field_id = arrow_field_id(&field)?;
        equality_columns.push(EqualityDeleteColumn {
            name: field.name().to_string(),
            field_id,
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        });
        fields.push(field);
    }
    Ok((Arc::new(Schema::new(fields)), equality_columns))
}
