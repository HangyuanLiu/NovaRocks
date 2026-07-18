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

use arrow::datatypes::{DataType, Field};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::common::ids::SlotId;
use crate::exec::row_position::IcebergVirtualSpec;
use crate::proto::{common, plan};
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(super) fn iceberg_virtual_count_column(column_id: u32) -> common::OutputColumn {
    common::OutputColumn {
        column_id,
        name: "___count___".to_string(),
        r#type: Some(common::TypeDesc {
            kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
                r#type: common::PrimitiveType::Boolean as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            })),
        }),
        nullable: false,
        is_internal: true,
    }
}

pub(super) fn record_iceberg_virtual_column(
    table: &plan::IcebergTableInfo,
    col: &common::OutputColumn,
    spec: &mut IcebergVirtualSpec,
) -> Result<bool, NativeFragmentLeafDecodeError> {
    let Some(field) = iceberg_virtual_projected_field(table, col)? else {
        return Ok(false);
    };
    let slot_id = SlotId::new(col.column_id);
    if crate::exec::row_position::is_iceberg_file_path(&col.name) {
        if spec.file_path_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg _file virtual column".into());
        }
        spec.file_path_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_iceberg_row_pos(&col.name) {
        if spec.row_pos_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg _pos virtual column".into());
        }
        spec.row_pos_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_iceberg_row_id(&col.name) {
        if spec.row_id_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg _row_id virtual column".into());
        }
        spec.row_id_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_iceberg_last_updated_sequence_number(&col.name) {
        if spec.last_updated_seq_slot.replace(slot_id).is_some() {
            return Err(
                "ScanNode duplicate Iceberg _last_updated_sequence_number virtual column".into(),
            );
        }
        spec.last_updated_seq_field = Some(field);
        return Ok(true);
    }
    if crate::exec::row_position::is_change_op(&col.name) {
        if spec.change_op_slot.replace(slot_id).is_some() {
            return Err("ScanNode duplicate Iceberg __change_op virtual column".into());
        }
        spec.change_op_field = Some(field);
        return Ok(true);
    }
    Ok(false)
}

pub(super) fn iceberg_virtual_projected_field(
    table: &plan::IcebergTableInfo,
    col: &common::OutputColumn,
) -> Result<Option<Field>, NativeFragmentLeafDecodeError> {
    if iceberg_schema_has_field(table, &col.name) {
        return Ok(None);
    }
    let desc = col
        .r#type
        .as_ref()
        .ok_or_else(|| format!("ScanNode output column {} type missing", col.name))?;
    let data_type = super::super::decode_type(desc)?;
    if crate::exec::row_position::is_iceberg_file_path(&col.name) {
        if !matches!(data_type, DataType::Utf8) {
            return Err(NativeFragmentLeafDecodeError::new(format!(
                "ScanNode Iceberg _file virtual column expects Utf8, got {:?}",
                data_type
            )));
        }
        return Ok(Some(Field::new(col.name.clone(), data_type, col.nullable)));
    }
    if crate::exec::row_position::is_iceberg_row_pos(&col.name) {
        if !matches!(data_type, DataType::Int64) {
            return Err(NativeFragmentLeafDecodeError::new(format!(
                "ScanNode Iceberg _pos virtual column expects Int64, got {:?}",
                data_type
            )));
        }
        return Ok(Some(Field::new(col.name.clone(), data_type, col.nullable)));
    }
    if crate::exec::row_position::is_iceberg_row_id(&col.name) {
        if !matches!(data_type, DataType::Int64) {
            return Err(NativeFragmentLeafDecodeError::new(format!(
                "ScanNode Iceberg _row_id virtual column expects Int64, got {:?}",
                data_type
            )));
        }
        return Ok(Some(iceberg_virtual_field_with_field_id(
            col,
            data_type,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
        )));
    }
    if crate::exec::row_position::is_iceberg_last_updated_sequence_number(&col.name) {
        if !matches!(data_type, DataType::Int64) {
            return Err(NativeFragmentLeafDecodeError::new(format!(
                "ScanNode Iceberg _last_updated_sequence_number virtual column expects Int64, got {:?}",
                data_type
            )));
        }
        return Ok(Some(iceberg_virtual_field_with_field_id(
            col,
            data_type,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        )));
    }
    if crate::exec::row_position::is_change_op(&col.name) {
        if !matches!(data_type, DataType::Int8) {
            return Err(NativeFragmentLeafDecodeError::new(format!(
                "ScanNode Iceberg __change_op virtual column expects Int8, got {:?}",
                data_type
            )));
        }
        return Ok(Some(Field::new(col.name.clone(), data_type, col.nullable)));
    }
    Ok(None)
}

fn iceberg_virtual_field_with_field_id(
    col: &common::OutputColumn,
    data_type: DataType,
    field_id: i32,
) -> Field {
    Field::new(col.name.clone(), data_type, col.nullable).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        field_id.to_string(),
    )]))
}

fn iceberg_schema_has_field(table: &plan::IcebergTableInfo, name: &str) -> bool {
    table
        .schema
        .as_ref()
        .is_some_and(|schema| schema.fields.iter().any(|field| field.name == name))
}
