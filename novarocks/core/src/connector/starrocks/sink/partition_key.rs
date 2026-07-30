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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::common::ids::SlotId;
use crate::connector::starrocks::sink::plan::{
    SinkPartitionDescriptor, SinkSchemaDescriptor, SinkSlotDescriptor,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

#[derive(Clone)]
pub enum PartitionKeySource {
    None,
    SlotRefs(Vec<PartitionSlotRef>),
    Expr(Arc<PartitionExprPlan>),
}

#[derive(Clone)]
pub struct PartitionSlotRef {
    pub slot_id: SlotId,
    pub column_name: String,
}

#[derive(Clone)]
pub struct PartitionExprPlan {
    pub arena: ExprArena,
    pub expr_ids: Vec<ExprId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionMode {
    Unpartitioned,
    Range,
    List,
}

#[derive(Clone, Debug)]
pub struct PartitionRoutingEntry {
    pub partition_id: i64,
    pub tablet_ids: Vec<i64>,
    pub start_key: Option<Vec<PartitionKeyValue>>,
    pub end_key: Option<Vec<PartitionKeyValue>>,
    pub in_keys: Vec<Vec<PartitionKeyValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PartitionKeyValue {
    Null,
    Bool(bool),
    Int(i128),
    Date32(i32),
    TimestampMicros(i64),
    Decimal { value: i128, scale: i8 },
    Utf8(String),
    Binary(Vec<u8>),
}

pub fn partition_key_source_len(source: &PartitionKeySource) -> usize {
    match source {
        PartitionKeySource::None => 0,
        PartitionKeySource::SlotRefs(slot_refs) => slot_refs.len(),
        PartitionKeySource::Expr(expr_plan) => expr_plan.expr_ids.len(),
    }
}

pub fn build_slot_name_map(
    slot_descs: &[SinkSlotDescriptor],
) -> Result<HashMap<String, SlotId>, String> {
    let mut slot_by_name = HashMap::new();
    for (idx, slot) in slot_descs.iter().enumerate() {
        let Some(slot_id) = slot.id else {
            return Err(format!(
                "OLAP_TABLE_SINK schema.slot_descs[{}] missing id while resolving slot names",
                idx
            ));
        };
        if let Some(col_name) = slot
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(col_name.to_ascii_lowercase(), slot_id);
        }
        if let Some(physical_name) = slot
            .col_physical_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(physical_name.to_ascii_lowercase(), slot_id);
        }
    }
    Ok(slot_by_name)
}

pub fn resolve_slot_ids_by_names(
    slot_descs: &[SinkSlotDescriptor],
    names: &[String],
    label: &str,
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<Vec<SlotId>, String> {
    let slot_by_name = build_slot_name_map(slot_descs)?;
    let mut out = Vec::with_capacity(names.len());
    for col in names {
        let slot_id = slot_name_overrides
            .and_then(|map| map.get(col).copied())
            .or_else(|| slot_by_name.get(col).copied())
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK cannot find {} '{}' in schema.slot_descs",
                    label, col
                )
            })?;
        out.push(slot_id);
    }
    Ok(out)
}

pub fn build_partition_key_source(
    partition: &SinkPartitionDescriptor,
    schema: &SinkSchemaDescriptor,
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<PartitionKeySource, String> {
    if let Some(exprs) = partition.partition_exprs.as_ref() {
        return Ok(PartitionKeySource::Expr(Arc::clone(exprs)));
    }

    let partition_columns = &partition.partition_columns;
    if partition_columns.is_empty() {
        return Ok(PartitionKeySource::None);
    }

    let slot_ids = resolve_slot_ids_by_names(
        &schema.slot_descs,
        partition_columns,
        "partition column",
        slot_name_overrides,
    )?;
    let slot_refs = partition_columns
        .iter()
        .zip(slot_ids.iter())
        .map(|(name, slot_id)| PartitionSlotRef {
            slot_id: *slot_id,
            column_name: name.clone(),
        })
        .collect::<Vec<_>>();
    Ok(PartitionKeySource::SlotRefs(slot_refs))
}

pub fn validate_partition_key_length(
    partition_id: i64,
    expected_len: usize,
    start_key: Option<&[PartitionKeyValue]>,
    end_key: Option<&[PartitionKeyValue]>,
    in_keys: &[Vec<PartitionKeyValue>],
) -> Result<(), String> {
    if expected_len == 0 {
        if start_key.is_some() || end_key.is_some() || !in_keys.is_empty() {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} has partition key metadata but key source is empty",
                partition_id
            ));
        }
        return Ok(());
    }

    if let Some(start_key) = start_key
        && start_key.len() != expected_len
    {
        return Err(format!(
            "OLAP_TABLE_SINK partition {} start key length mismatch: expected={} actual={}",
            partition_id,
            expected_len,
            start_key.len()
        ));
    }
    if let Some(end_key) = end_key
        && end_key.len() != expected_len
    {
        return Err(format!(
            "OLAP_TABLE_SINK partition {} end key length mismatch: expected={} actual={}",
            partition_id,
            expected_len,
            end_key.len()
        ));
    }
    for (idx, key) in in_keys.iter().enumerate() {
        if key.len() != expected_len {
            return Err(format!(
                "OLAP_TABLE_SINK partition {} in_keys[{}] length mismatch: expected={} actual={}",
                partition_id,
                idx,
                expected_len,
                key.len()
            ));
        }
    }
    Ok(())
}

pub fn build_partition_key_arrays(
    partition_key_source: &PartitionKeySource,
    chunk: &Chunk,
) -> Result<Vec<ArrayRef>, String> {
    match partition_key_source {
        PartitionKeySource::None => Ok(Vec::new()),
        PartitionKeySource::SlotRefs(slot_refs) => {
            let mut arrays = Vec::with_capacity(slot_refs.len());
            for slot_ref in slot_refs {
                let arr = match chunk.column_by_slot_id(slot_ref.slot_id) {
                    Ok(arr) => arr,
                    Err(slot_err) => {
                        find_chunk_column_by_name(chunk, &slot_ref.column_name).ok_or_else(|| {
                            format!(
                                "OLAP_TABLE_SINK partition slot {} ('{}') is not available in chunk: {}",
                                slot_ref.slot_id,
                                slot_ref.column_name,
                                slot_err
                            )
                        })?
                    }
                };
                arrays.push(arr);
            }
            Ok(arrays)
        }
        PartitionKeySource::Expr(plan) => {
            let mut arrays = Vec::with_capacity(plan.expr_ids.len());
            for expr_id in &plan.expr_ids {
                arrays.push(plan.arena.eval(*expr_id, chunk)?);
            }
            Ok(arrays)
        }
    }
}

fn find_chunk_column_by_name(chunk: &Chunk, column_name: &str) -> Option<ArrayRef> {
    let target = column_name.trim();
    if target.is_empty() {
        return None;
    }
    let idx = chunk
        .batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(target))?;
    chunk.batch.columns().get(idx).cloned()
}

pub fn build_row_partition_key(
    partition_key_arrays: &[ArrayRef],
    row: usize,
) -> Result<Vec<PartitionKeyValue>, String> {
    let mut out = Vec::with_capacity(partition_key_arrays.len());
    for array in partition_key_arrays {
        out.push(read_partition_key_value(array.as_ref(), row)?);
    }
    Ok(out)
}

fn read_partition_key_value(array: &dyn Array, row: usize) -> Result<PartitionKeyValue, String> {
    if array.is_null(row) {
        return Ok(PartitionKeyValue::Null);
    }
    match array.data_type() {
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            Ok(PartitionKeyValue::Bool(typed.value(row)))
        }
        DataType::Int8 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast Int8Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Int16 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast Int16Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt8 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "downcast UInt8Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt16 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "downcast UInt16Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "downcast UInt32Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "downcast UInt64Array failed".to_string())?;
            Ok(PartitionKeyValue::Int(typed.value(row) as i128))
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast Date32Array failed".to_string())?;
            Ok(PartitionKeyValue::Date32(typed.value(row)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "downcast TimestampSecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(
                typed.value(row).saturating_mul(1_000_000),
            ))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "downcast TimestampMillisecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(
                typed.value(row).saturating_mul(1_000),
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TimestampMicrosecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(typed.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "downcast TimestampNanosecondArray failed".to_string())?;
            Ok(PartitionKeyValue::TimestampMicros(typed.value(row) / 1_000))
        }
        DataType::Decimal128(_, scale) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast Decimal128Array failed".to_string())?;
            Ok(PartitionKeyValue::Decimal {
                value: typed.value(row),
                scale: *scale,
            })
        }
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast StringArray failed".to_string())?;
            Ok(PartitionKeyValue::Utf8(typed.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
            Ok(PartitionKeyValue::Utf8(typed.value(row).to_string()))
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "downcast BinaryArray failed".to_string())?;
            Ok(PartitionKeyValue::Binary(typed.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "downcast LargeBinaryArray failed".to_string())?;
            Ok(PartitionKeyValue::Binary(typed.value(row).to_vec()))
        }
        other => Err(format!(
            "unsupported partition key data type in routed chunk: {:?}",
            other
        )),
    }
}

pub fn compare_partition_key_vectors(
    left: &[PartitionKeyValue],
    right: &[PartitionKeyValue],
) -> Result<Ordering, String> {
    if left.len() != right.len() {
        return Err(format!(
            "partition key length mismatch in comparison: left={} right={}",
            left.len(),
            right.len()
        ));
    }
    for (idx, (lhs, rhs)) in left.iter().zip(right.iter()).enumerate() {
        let ord = compare_partition_key_value(lhs, rhs)
            .map_err(|e| format!("partition key compare failed at column {}: {}", idx, e))?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_partition_key_value(
    left: &PartitionKeyValue,
    right: &PartitionKeyValue,
) -> Result<Ordering, String> {
    match (left, right) {
        (PartitionKeyValue::Null, PartitionKeyValue::Null) => Ok(Ordering::Equal),
        (PartitionKeyValue::Null, _) => Ok(Ordering::Less),
        (_, PartitionKeyValue::Null) => Ok(Ordering::Greater),
        (PartitionKeyValue::Bool(lhs), PartitionKeyValue::Bool(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::Int(lhs), PartitionKeyValue::Int(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::Date32(lhs), PartitionKeyValue::Date32(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::TimestampMicros(lhs), PartitionKeyValue::TimestampMicros(rhs)) => {
            Ok(lhs.cmp(rhs))
        }
        (
            PartitionKeyValue::Decimal {
                value: lhs_value,
                scale: lhs_scale,
            },
            PartitionKeyValue::Decimal {
                value: rhs_value,
                scale: rhs_scale,
            },
        ) => {
            if lhs_scale == rhs_scale {
                return Ok(lhs_value.cmp(rhs_value));
            }
            let target_scale = (*lhs_scale).max(*rhs_scale);
            let lhs = scale_decimal_to(*lhs_value, *lhs_scale, target_scale)
                .ok_or_else(|| "decimal scale conversion overflow".to_string())?;
            let rhs = scale_decimal_to(*rhs_value, *rhs_scale, target_scale)
                .ok_or_else(|| "decimal scale conversion overflow".to_string())?;
            Ok(lhs.cmp(&rhs))
        }
        (PartitionKeyValue::Utf8(lhs), PartitionKeyValue::Utf8(rhs)) => Ok(lhs.cmp(rhs)),
        (PartitionKeyValue::Binary(lhs), PartitionKeyValue::Binary(rhs)) => Ok(lhs.cmp(rhs)),
        (lhs, rhs) => Err(format!(
            "incompatible partition key types: left={:?} right={:?}",
            lhs, rhs
        )),
    }
}

fn scale_decimal_to(value: i128, from_scale: i8, to_scale: i8) -> Option<i128> {
    if to_scale == from_scale {
        return Some(value);
    }
    if to_scale < from_scale {
        let divisor = pow10_i128((from_scale - to_scale) as u32)?;
        return Some(value / divisor);
    }
    let multiplier = pow10_i128((to_scale - from_scale) as u32)?;
    value.checked_mul(multiplier)
}

fn pow10_i128(exp: u32) -> Option<i128> {
    let mut out = 1_i128;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}
