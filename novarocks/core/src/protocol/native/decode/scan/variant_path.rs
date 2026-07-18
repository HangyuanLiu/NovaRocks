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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::{DataType, Field};

use super::common::{column_def_data_type, output_column_data_type};
use crate::common::ids::SlotId;
use crate::formats::parquet::VariantPathSpec;
use crate::proto::{common, plan};
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

#[derive(Clone, Debug, Default)]
pub(super) struct NativeVariantPathPlan {
    pub(super) specs: Vec<VariantPathSpec>,
    pub(super) output_slot_ids: HashSet<SlotId>,
}

pub(super) fn parse_native_scan_variant_path_columns(
    scan: &plan::ScanNode,
    table: &plan::IcebergTableInfo,
    output_columns: &[common::OutputColumn],
) -> Result<NativeVariantPathPlan, NativeFragmentLeafDecodeError> {
    if scan.variant_columns.is_empty() {
        return Ok(NativeVariantPathPlan::default());
    }
    let table_def = scan
        .table
        .as_ref()
        .ok_or_else(|| "ScanNode table missing".to_string())?;
    let output_by_slot = output_columns
        .iter()
        .map(|col| (SlotId::new(col.column_id), col))
        .collect::<HashMap<_, _>>();
    let scan_by_slot = scan
        .columns
        .iter()
        .map(|col| (SlotId::new(col.column_id), col))
        .collect::<HashMap<_, _>>();
    let mut plan = NativeVariantPathPlan::default();

    for (idx, column) in scan.variant_columns.iter().enumerate() {
        let source_slot_id = SlotId::new(column.source_column_id);
        let output_slot_id = SlotId::new(column.synthetic_column_id);
        if source_slot_id == output_slot_id {
            return Err(format!(
                "ScanNode variant_columns[{idx}] source_column_id must differ from synthetic_column_id"
            ).into());
        }

        let source_name =
            required_native_variant_path_string(idx, "source_column", &column.source_column)?;
        let output_name =
            required_native_variant_path_string(idx, "synthetic_column", &column.synthetic_column)?;
        let canonical_path =
            required_native_variant_path_string(idx, "canonical_path", &column.canonical_path)?;
        validate_native_variant_path_column_path(idx, &canonical_path)?;

        let source_scan_column = scan_by_slot.get(&source_slot_id).ok_or_else(|| {
            format!(
                "ScanNode variant_columns[{idx}] source_column_id={source_slot_id} is not a scan column"
            )
        })?;
        if source_scan_column.name != source_name {
            return Err(format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} does not match source_column_id={source_slot_id} name {:?}",
                source_scan_column.name
            ).into());
        }
        let source_table_column = table_def
            .columns
            .iter()
            .find(|col| col.name == source_name)
            .ok_or_else(|| {
                format!(
                    "ScanNode variant_columns[{idx}] source_column={source_name:?} is not in table column definitions"
                )
            })?;
        let source_type = column_def_data_type(source_table_column).map_err(|err| {
            format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} type error: {err}"
            )
        })?;
        if !matches!(source_type, DataType::LargeBinary) {
            return Err(format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} expects VARIANT/LargeBinary, got {:?}",
                source_type
            ).into());
        }
        let source_field_id = iceberg_schema_field_id(table, &source_name).ok_or_else(|| {
            format!(
                "ScanNode variant_columns[{idx}] source_column={source_name:?} is missing from Iceberg schema"
            )
        })?;

        let output_column = output_by_slot.get(&output_slot_id).ok_or_else(|| {
            format!(
                "ScanNode variant_columns[{idx}] synthetic_column_id={output_slot_id} is not an output column"
            )
        })?;
        if output_column.name != output_name {
            return Err(format!(
                "ScanNode variant_columns[{idx}] synthetic_column={output_name:?} does not match synthetic_column_id={output_slot_id} name {:?}",
                output_column.name
            ).into());
        }
        let output_type = output_column_data_type(output_column).map_err(|err| {
            format!(
                "ScanNode variant_columns[{idx}] synthetic_column={output_name:?} type error: {err}"
            )
        })?;
        let requested_type_desc = column
            .requested_type
            .as_ref()
            .ok_or_else(|| format!("ScanNode variant_columns[{idx}] missing requested_type"))?;
        let requested_type = super::super::decode_type(requested_type_desc).map_err(|err| {
            format!("ScanNode variant_columns[{idx}] requested_type decode failed: {err}")
        })?;
        if !is_supported_native_variant_path_requested_type(&requested_type) {
            return Err(format!(
                "ScanNode variant_columns[{idx}] unsupported requested_type {:?} for synthetic_column_id={output_slot_id}",
                requested_type
            ).into());
        }
        if requested_type != output_type {
            return Err(format!(
                "ScanNode variant_columns[{idx}] requested_type {:?} does not match synthetic_column_id={output_slot_id} type {:?}",
                requested_type, output_type
            ).into());
        }
        if !plan.output_slot_ids.insert(output_slot_id) {
            return Err(format!(
                "ScanNode duplicate variant_columns synthetic_column_id={output_slot_id}"
            )
            .into());
        }

        plan.specs.push(VariantPathSpec {
            source_slot_id,
            source_read_slot_id: source_slot_id,
            output_slot_id,
            source_field_id: Some(source_field_id),
            source_name: source_name.clone(),
            output_name: output_name.clone(),
            source_field: Field::new(source_name, source_type, source_table_column.nullable),
            output_field: Field::new(output_name, output_type, output_column.nullable),
            canonical_path,
            requested_type,
            strict: column.strict,
        });
    }

    Ok(plan)
}

fn required_native_variant_path_string(
    idx: usize,
    field_name: &str,
    value: &str,
) -> Result<String, NativeFragmentLeafDecodeError> {
    value
        .trim()
        .is_empty()
        .then(|| format!("ScanNode variant_columns[{idx}] missing {field_name}"))
        .map_or_else(|| Ok(value.trim().to_string()), |error| Err(error.into()))
}

fn validate_native_variant_path_column_path(
    idx: usize,
    canonical_path: &str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    let parsed = crate::exec::variant::parse_variant_path(canonical_path).map_err(|err| {
        format!("ScanNode variant_columns[{idx}] invalid canonical_path={canonical_path:?}: {err}")
    })?;
    if parsed.segments.is_empty() {
        return Err(format!(
            "ScanNode variant_columns[{idx}] canonical_path={canonical_path:?} must reference at least one object key"
        ).into());
    }
    if parsed.segments.iter().any(|segment| {
        !matches!(
            segment,
            crate::exec::variant::VariantPathSegment::ObjectKey(_)
        )
    }) {
        return Err(format!(
            "ScanNode variant_columns[{idx}] canonical_path={canonical_path:?} only supports object-key path segments"
        ).into());
    }
    Ok(())
}

fn is_supported_native_variant_path_requested_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean | DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Date32
    )
}

fn iceberg_schema_field_id(table: &plan::IcebergTableInfo, name: &str) -> Option<i32> {
    table
        .schema
        .as_ref()
        .and_then(|schema| schema.fields.iter().find(|field| field.name == name))
        .map(|field| field.field_id)
}
