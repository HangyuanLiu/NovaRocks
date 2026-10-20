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
use crate::exec::expr::expression_contract;
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date32Builder, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use std::cmp::Ordering;
use std::sync::Arc;

use crate::exec::expr::function::date::common::{
    naive_to_date32, naive_to_timestamp_micros, parse_date, parse_datetime,
};
use novarocks_types::decimal::LEGACY_DECIMALV2_SCALE;
use novarocks_types::largeint;

/// SQL element nullness for the supported execution array types.
/// NullArray has no physical validity bitmap although all its values are NULL.
/// Keep this hot-loop check allocation-free; logical_nulls() computes a bitmap
/// for NullArray. Encoded dictionary/run/union values are not array-kernel inputs.
pub(super) fn is_logically_null(array: &dyn Array, index: usize) -> bool {
    matches!(array.data_type(), DataType::Null) || array.is_null(index)
}

pub(super) fn row_index(row: usize, len: usize) -> usize {
    novarocks_functions::builtin::map_lookup_core::row_index(row, len)
}
pub(super) fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    novarocks_functions::builtin::collection_arrow::cast_output(out, output_type, fn_name)
        .map_err(crate::runtime::fragment::ExecutionFailure::from)
}

pub(super) fn adjust_legacy_decimalv2_target_type(
    source_type: &DataType,
    requested_type: &DataType,
) -> DataType {
    match (source_type, requested_type) {
        (
            DataType::Decimal128(source_precision, source_scale),
            DataType::Decimal128(target_precision, target_scale),
        ) if *target_scale == LEGACY_DECIMALV2_SCALE && source_scale != target_scale => {
            DataType::Decimal128((*target_precision).max(*source_precision), *source_scale)
        }
        _ => requested_type.clone(),
    }
}

fn cast_utf8_to_date32(array: &ArrayRef) -> ExecutionResult<ArrayRef> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("failed to downcast to StringArray"))?;
    let mut builder = Date32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let raw = arr.value(i).trim();
        let days = if let Some(date) = parse_date(raw) {
            naive_to_date32(date)
        } else if let Some(dt) = parse_datetime(raw) {
            naive_to_date32(dt.date())
        } else {
            return Err(literal_failure("date", arr.value(i)));
        };
        builder.append_value(days);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_utf8_to_timestamp(array: &ArrayRef, target_type: &DataType) -> ExecutionResult<ArrayRef> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("failed to downcast to StringArray"))?;
    let mut values = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            values.push(None);
            continue;
        }
        let raw = arr.value(i).trim();
        let micros = if let Some(dt) = parse_datetime(raw) {
            naive_to_timestamp_micros(dt)
        } else if let Some(date) = parse_date(raw) {
            let midnight = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| literal_failure("timestamp", arr.value(i)))?;
            naive_to_timestamp_micros(midnight)
        } else {
            return Err(literal_failure("timestamp", arr.value(i)));
        };
        values.push(Some(micros));
    }
    let micros_array = Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
    if matches!(
        target_type,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ) {
        return Ok(micros_array);
    }
    cast(&micros_array, target_type).map_err(crate::exec::expr::cast::cast_arrow_failure)
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let abs = unscaled.unsigned_abs().to_string();
    if abs.len() <= scale {
        let frac = format!("{:0>width$}", abs, width = scale);
        if unscaled < 0 {
            format!("-0.{}", frac)
        } else {
            format!("0.{}", frac)
        }
    } else {
        let split = abs.len() - scale;
        let integer = &abs[..split];
        let frac = &abs[split..];
        if unscaled < 0 {
            format!("-{}.{}", integer, frac)
        } else {
            format!("{}.{}", integer, frac)
        }
    }
}

fn cast_decimal_to_utf8(array: &ArrayRef) -> ExecutionResult<ArrayRef> {
    let (arr, scale) = match array.data_type() {
        DataType::Decimal128(_, scale) => (
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| expression_contract("failed to downcast Decimal128Array"))?,
            *scale,
        ),
        other => {
            return Err(expression_contract(format_args!(
                "cast_decimal_to_utf8 expects Decimal128Array, got {:?}",
                other
            )));
        }
    };
    let mut out = Vec::with_capacity(arr.len());
    let mut output_offset = 0;
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
        } else {
            let value = format_decimal(arr.value(row), scale);
            output_offset = novarocks_functions::aggregate_scalar::checked_utf8_output_offset(
                output_offset,
                value.len(),
            )?;
            out.push(Some(value));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

/// ARRAY membership already selected its common type. When that type's
/// leaf is VARCHAR, floating-point and timestamp values must use the same
/// representation as SQL CAST, including inside nested arrays.
fn uses_sql_varchar_cast(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (DataType::Float32 | DataType::Float64 | DataType::Timestamp(_, _), DataType::Utf8) => true,
        (DataType::List(source), DataType::List(target)) => {
            uses_sql_varchar_cast(source.data_type(), target.data_type())
        }
        _ => false,
    }
}

pub(super) fn cast_with_special_rules(
    array: &ArrayRef,
    target_type: &DataType,
    _fn_name: &str,
) -> ExecutionResult<ArrayRef> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    match (array.data_type(), target_type) {
        _ if uses_sql_varchar_cast(array.data_type(), target_type) => {
            crate::exec::expr::cast::cast_with_special_rules(array, target_type)
        }
        (DataType::Utf8, DataType::Date32) => cast_utf8_to_date32(array),
        (DataType::Utf8, DataType::Timestamp(_, None)) => {
            cast_utf8_to_timestamp(array, target_type)
        }
        (DataType::Decimal128(_, _), DataType::Utf8) => cast_decimal_to_utf8(array),
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
            crate::exec::expr::cast::cast_with_special_rules(array, target_type)
        }
        _ => cast(array, target_type).map_err(crate::exec::expr::cast::cast_arrow_failure),
    }
}

pub(super) fn compare_value_to_target(
    values: &ArrayRef,
    value_idx: usize,
    targets: &ArrayRef,
    target_row: usize,
) -> ExecutionResult<bool> {
    let target_idx = row_index(target_row, targets.len());
    compare_values_with_null(values, value_idx, targets, target_idx, false)
}

pub(super) fn compare_values_at(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> ExecutionResult<bool> {
    compare_values_with_null(left, left_idx, right, right_idx, false)
}

pub fn compare_values_with_null(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
    null_equals_null: bool,
) -> ExecutionResult<bool> {
    if left_idx >= left.len() || right_idx >= right.len() {
        return Err(expression_contract(
            "array comparison index is outside its physical carriers",
        ));
    }
    let left_null = is_logically_null(left.as_ref(), left_idx);
    let right_null = is_logically_null(right.as_ref(), right_idx);
    if left_null || right_null {
        return Ok(null_equals_null && left_null && right_null);
    }
    if left.data_type() != right.data_type() {
        return Err(expression_contract(format_args!(
            "array compare type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        )));
    }

    match left.data_type() {
        DataType::Null => Ok(null_equals_null),
        DataType::Int8 => {
            let l = left.as_any().downcast_ref::<Int8Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Int16 => {
            let l = left.as_any().downcast_ref::<Int16Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Int32 => {
            let l = left.as_any().downcast_ref::<Int32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Int64 => {
            let l = left.as_any().downcast_ref::<Int64Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Float32 => {
            let l = left.as_any().downcast_ref::<Float32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Float64 => {
            let l = left.as_any().downcast_ref::<Float64Array>().unwrap();
            let r = right.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Boolean => {
            let l = left.as_any().downcast_ref::<BooleanArray>().unwrap();
            let r = right.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Utf8 => {
            let l = left.as_any().downcast_ref::<StringArray>().unwrap();
            let r = right.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Date32 => {
            let l = left.as_any().downcast_ref::<Date32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Decimal128(_, _) => {
            let l = left.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let r = right.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let l = left
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    expression_contract("failed to downcast left to FixedSizeBinaryArray")
                })?;
            let r = right
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    expression_contract("failed to downcast right to FixedSizeBinaryArray")
                })?;
            let lv =
                largeint::i128_from_be_bytes(l.value(left_idx)).map_err(ExecutionFailure::from)?;
            let rv =
                largeint::i128_from_be_bytes(r.value(right_idx)).map_err(ExecutionFailure::from)?;
            Ok(lv == rv)
        }
        DataType::List(_) => compare_list_values(left, left_idx, right, right_idx),
        DataType::Struct(_) => compare_struct_values(left, left_idx, right, right_idx),
        DataType::Map(_, _) => compare_map_values(left, left_idx, right, right_idx),
        other => Err(unsupported_comparison(other, false)),
    }
}

fn compare_list_values(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> ExecutionResult<bool> {
    let l = left
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| expression_contract("failed to downcast left to ListArray"))?;
    let r = right
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| expression_contract("failed to downcast right to ListArray"))?;

    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_start = l_offsets[left_idx] as usize;
    let l_end = l_offsets[left_idx + 1] as usize;
    let r_start = r_offsets[right_idx] as usize;
    let r_end = r_offsets[right_idx + 1] as usize;
    let l_len = l_end.saturating_sub(l_start);
    let r_len = r_end.saturating_sub(r_start);
    if l_len != r_len {
        return Ok(false);
    }

    let l_values = l.values();
    let r_values = r.values();
    for offset in 0..l_len {
        if !compare_values_with_null(l_values, l_start + offset, r_values, r_start + offset, true)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_struct_values(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> ExecutionResult<bool> {
    let l = left
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| expression_contract("failed to downcast left to StructArray"))?;
    let r = right
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| expression_contract("failed to downcast right to StructArray"))?;
    if l.num_columns() != r.num_columns() {
        return Ok(false);
    }
    for col_idx in 0..l.num_columns() {
        if !compare_values_with_null(
            l.column(col_idx),
            left_idx,
            r.column(col_idx),
            right_idx,
            true,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_map_values(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> ExecutionResult<bool> {
    let l = left
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| expression_contract("failed to downcast left to MapArray"))?;
    let r = right
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| expression_contract("failed to downcast right to MapArray"))?;

    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_start = l_offsets[left_idx] as usize;
    let l_end = l_offsets[left_idx + 1] as usize;
    let r_start = r_offsets[right_idx] as usize;
    let r_end = r_offsets[right_idx + 1] as usize;
    let l_len = l_end.saturating_sub(l_start);
    let r_len = r_end.saturating_sub(r_start);
    if l_len != r_len {
        return Ok(false);
    }

    let l_keys = l.keys();
    let r_keys = r.keys();
    let l_values = l.values();
    let r_values = r.values();
    for offset in 0..l_len {
        let li = l_start + offset;
        let ri = r_start + offset;
        if !compare_values_with_null(l_keys, li, r_keys, ri, true)? {
            return Ok(false);
        }
        if !compare_values_with_null(l_values, li, r_values, ri, true)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn compare_values_ordered(
    values: &ArrayRef,
    left_idx: usize,
    right_idx: usize,
) -> ExecutionResult<Ordering> {
    novarocks_functions::builtin::array_sortby_core::compare_values_ordered(
        values, left_idx, right_idx,
    )
    .map_err(ExecutionFailure::from)
}

#[cfg(test)]
mod varchar_coercion_tests {
    use super::*;
    use arrow_buffer::{NullBuffer, OffsetBuffer};

    #[test]
    fn floating_array_values_use_sql_cast_text_and_preserve_nulls_and_slices() {
        for source in [
            Arc::new(Float32Array::from(vec![
                Some(10.0),
                Some(-0.0),
                Some(1.25),
                None,
            ])) as ArrayRef,
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(-0.0),
                Some(1.25),
                None,
            ])) as ArrayRef,
        ] {
            for source in [source.clone(), source.slice(1, 3)] {
                let output =
                    cast_with_special_rules(&source, &DataType::Utf8, "arrays_overlap").unwrap();
                let explicit =
                    crate::exec::expr::cast::cast_with_special_rules(&source, &DataType::Utf8)
                        .unwrap();
                assert_eq!(output.to_data(), explicit.to_data());
                let output = output.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    if source.len() == 4 {
                        vec![Some("10"), Some("0"), Some("1.25"), None]
                    } else {
                        vec![Some("0"), Some("1.25"), None]
                    }
                );
            }
        }
    }

    #[test]
    fn timestamp_array_values_use_sql_datetime_separator_for_every_time_unit() {
        for source in [
            Arc::new(TimestampSecondArray::from(vec![Some(0), None])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![Some(0), None])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![Some(0), None])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![Some(0), None])) as ArrayRef,
        ] {
            let output =
                cast_with_special_rules(&source, &DataType::Utf8, "array_intersect").unwrap();
            let explicit =
                crate::exec::expr::cast::cast_with_special_rules(&source, &DataType::Utf8).unwrap();
            assert_eq!(output.to_data(), explicit.to_data());
            let output = output.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                output.iter().collect::<Vec<_>>(),
                vec![Some("1970-01-01 00:00:00"), None]
            );
        }
    }

    #[test]
    fn nested_varchar_coercion_preserves_parent_nulls_offsets_and_child_nulls() {
        let values = Arc::new(Float64Array::from(vec![Some(10.0), Some(1.25), None])) as ArrayRef;
        let source = Arc::new(ListArray::new(
            Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Float64,
                true,
            )),
            OffsetBuffer::new(vec![0_i32, 1, 3, 3].into()),
            values,
            Some(NullBuffer::from(vec![true, true, false])),
        )) as ArrayRef;
        let target = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        let output = cast_with_special_rules(&source, &target, "arrays_overlap").unwrap();
        let explicit = crate::exec::expr::cast::cast_with_special_rules(&source, &target).unwrap();
        assert_eq!(output.to_data(), explicit.to_data());
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.value_offsets(), &[0, 1, 3, 3]);
        assert!(output.is_null(2));
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("10"), Some("1.25"), None]
        );
    }
}

#[derive(Debug)]
enum ArrayValueFailure {
    Json(serde_json::Error),
    JsonArrayRequired,
    Literal {
        kind: &'static str,
        value: novarocks_failure_contract::BoundedDetail,
    },
    UnsupportedComparison {
        ordered: bool,
        data_type: novarocks_failure_contract::BoundedDetail,
    },
}
impl std::fmt::Display for ArrayValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(source) => write!(f, "array literal JSON parse failed: {source}"),
            Self::JsonArrayRequired => {
                f.write_str("array_length constant cast source must be a JSON array")
            }
            Self::Literal { kind, value } => write!(f, "invalid {kind} literal '{value}'"),
            Self::UnsupportedComparison { ordered, data_type } => write!(
                f,
                "array {}compare unsupported type: {data_type}",
                if *ordered { "ordered " } else { "" }
            ),
        }
    }
}
impl std::error::Error for ArrayValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ArrayValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Data,
            match self {
                Self::Literal { .. } | Self::Json(_) | Self::JsonArrayRequired => {
                    ExecutionReason::ArrayLiteralValue
                }
                Self::UnsupportedComparison { .. } => ExecutionReason::ArrayComparisonDomain,
            }
            .reason(),
        )
    }
}
fn literal_failure(kind: &'static str, value: &str) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayValueFailure::Literal {
        kind,
        value: novarocks_failure_contract::BoundedDetail::truncating(value),
    })
}
pub(super) fn unsupported_comparison(data_type: &DataType, ordered: bool) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayValueFailure::UnsupportedComparison {
        ordered,
        data_type: novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
            "{data_type:?}"
        )),
    })
}
#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn actual_date_literal_failure_is_data_but_invalid_carrier_and_indices_are_not() {
        let input: ArrayRef = Arc::new(StringArray::from(vec!["ResourceExhausted"]));
        let weak = Arc::downgrade(&input);
        let failure = cast_utf8_to_date32(&input).unwrap_err();
        drop(input);
        assert!(weak.upgrade().is_none());
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 526);
        assert!(
            failure
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<ArrayValueFailure>()
        );
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert_eq!(
            cast_utf8_to_date32(&input)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
        assert_eq!(
            compare_values_ordered(&input, 0, 1)
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
    #[test]
    fn ordered_nan_tie_and_original_decimal_magnitude_remain_defined() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, -0.0, 0.0, 7.0]));
        assert_eq!(
            compare_values_ordered(&input, 0, 3).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            compare_values_ordered(&input, 1, 2).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            format_decimal(i128::MIN, 1),
            "-17014118346046923173168730371588410572.8"
        );
        assert_eq!(format_decimal(-1, 3), "-0.001");
    }
}

/// Prepared scalar carriers are either one broadcast row or exactly this batch.
pub(in crate::exec::expr::function) fn validate_rows(
    actual: usize,
    rows: usize,
    label: &'static str,
) -> ExecutionResult<()> {
    if actual != 1 && actual != rows {
        return Err(expression_contract(format_args!(
            "{label} length {actual} differs from batch {rows}"
        )));
    }
    Ok(())
}
#[derive(Debug)]
struct ArrayNumericFailure {
    stage: &'static str,
    source: Option<std::num::TryFromIntError>,
}
impl std::fmt::Display for ArrayNumericFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.stage)
    }
}
impl std::error::Error for ArrayNumericFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|s| s as _)
    }
}
impl novarocks_failure_contract::FailureProjection for ArrayNumericFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &novarocks_functions::DataFailureReason::NumericOutOfRange,
        )
    }
}
pub(super) fn numeric_failure(stage: &'static str) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayNumericFailure {
        stage,
        source: None,
    })
}
pub(super) fn numeric_conversion_failure(
    stage: &'static str,
    source: std::num::TryFromIntError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayNumericFailure {
        stage,
        source: Some(source),
    })
}
#[derive(Debug)]
struct ArrayLengthMismatch {
    function: &'static str,
    row: usize,
    expected: usize,
    actual: usize,
}
impl std::fmt::Display for ArrayLengthMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} input arrays differ in size at row {}: expected {}, got {}",
            self.function, self.row, self.expected, self.actual
        )
    }
}
impl std::error::Error for ArrayLengthMismatch {}
impl novarocks_failure_contract::FailureProjection for ArrayLengthMismatch {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            ExecutionReason::ArrayLengthMismatch.reason(),
        )
    }
}
pub(in crate::exec::expr::function) fn length_mismatch_failure(
    function: &'static str,
    row: usize,
    expected: usize,
    actual: usize,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayLengthMismatch {
        function,
        row,
        expected,
        actual,
    })
}

#[derive(Debug)]
struct LambdaValueFailure(&'static str);
impl std::fmt::Display for LambdaValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for LambdaValueFailure {}
impl novarocks_failure_contract::FailureProjection for LambdaValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            ExecutionReason::LambdaValue.reason(),
        )
    }
}
pub(in crate::exec::expr::function) fn lambda_value_failure(
    message: &'static str,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(LambdaValueFailure(message))
}
#[derive(Debug)]
struct ArrayIndexExtent {
    value: usize,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for ArrayIndexExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "array source row index {} exceeds UInt32 representation",
            self.value
        )
    }
}
impl std::error::Error for ArrayIndexExtent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ArrayIndexExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Representation,
            ExecutionReason::ArraySourceIndexExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: ExecutionReason::ArraySourceIndexExtent.code(),
            },
            required: u64::try_from(self.value).ok(),
            extent_limit: Some(u64::from(u32::MAX)),
            representable_prefix: None,
        })
    }
}
pub(in crate::exec::expr::function) fn index_u32(value: usize) -> ExecutionResult<u32> {
    u32::try_from(value)
        .map_err(|source| ExecutionFailure::from_domain(ArrayIndexExtent { value, source }))
}
#[derive(Debug)]
struct ComparatorPairExtent {
    pairs: u128,
}
impl std::fmt::Display for ComparatorPairExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "array comparator matrix has {} pairs, exceeding addressable UInt32 index storage",
            self.pairs
        )
    }
}
impl std::error::Error for ComparatorPairExtent {}
impl novarocks_failure_contract::FailureProjection for ComparatorPairExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Representation,
            ExecutionReason::ArrayComparatorExtent.reason(),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: domain::EXECUTION,
                code: ExecutionReason::ArrayComparatorExtent.code(),
            },
            required: u64::try_from(self.pairs).ok(),
            extent_limit: u64::try_from(isize::MAX as usize / std::mem::size_of::<u32>()).ok(),
            representable_prefix: None,
        })
    }
}
pub(super) fn comparator_pair_count(len: usize) -> ExecutionResult<usize> {
    let pairs = len as u128 * len as u128;
    if pairs > (isize::MAX as usize / std::mem::size_of::<u32>()) as u128 {
        return Err(ExecutionFailure::from_domain(ComparatorPairExtent {
            pairs,
        }));
    }
    Ok(pairs as usize)
}

/// Retain the original Arrow take author and observe its exact borrowed indices
/// before its output allocation. This check grants no memory or input lifetime.
pub(in crate::exec::expr::function) fn take_values(
    values: &dyn Array,
    indices: &arrow::array::UInt32Array,
    operation: &'static str,
) -> ExecutionResult<ArrayRef> {
    novarocks_functions::selected_copy::preflight_original_take(
        values,
        novarocks_functions::selected_copy::CopyIndexView::UInt32(indices),
        |_| Ok(()),
    )
    .map_err(novarocks_functions::selected_copy::CopyError::into_kernel)?;
    arrow::compute::take(values, indices, None).map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(operation, source)
    })
}
#[cfg(test)]
mod higher_order_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn actual_indices_and_recursive_payload_use_the_original_take_domain() {
        let values = Arc::new(StringArray::from(vec![Some("中文"), None, Some("a")])) as ArrayRef;
        let weak = Arc::downgrade(&values);
        let indices = arrow::array::UInt32Array::from(vec![Some(2), None, Some(0), Some(2)]);
        let out = take_values(values.as_ref(), &indices, "array take property").unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("a"), None, Some("中文"), Some("a")])
        );
        // This internal port consumes operator-computed indices, like Arrow's
        // unchecked take. Preflight observes geometry; it is not index admission.
        // An empty selection is a valid boundary, including nullable payloads.
        let empty = arrow::array::UInt32Array::from(Vec::<u32>::new());
        let empty_out = take_values(values.as_ref(), &empty, "array take property").unwrap();
        assert_eq!(empty_out.len(), 0);
        assert_eq!(empty_out.data_type(), values.data_type());
        drop(empty_out);
        drop(out);
        drop(values);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn index_and_pair_extents_keep_actual_layout_facts_without_a_funding_claim() {
        assert_eq!(comparator_pair_count(3).unwrap(), 9);
        let failure = comparator_pair_count(usize::MAX).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
        assert!(failure.failure_fact().resource().is_none());
        assert_eq!(
            failure
                .failure_fact()
                .representation()
                .unwrap()
                .representable_prefix,
            None
        );
        if usize::BITS > 32 {
            let failure = index_u32(u32::MAX as usize + 1).unwrap_err();
            assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
            assert_eq!(
                failure.failure_fact().representation().unwrap().required,
                Some(u64::from(u32::MAX) + 1)
            );
        }
    }
}

pub(super) fn json_literal_failure(source: serde_json::Error) -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayValueFailure::Json(source))
}
pub(super) fn json_array_required_failure() -> ExecutionFailure {
    ExecutionFailure::from_domain(ArrayValueFailure::JsonArrayRequired)
}
