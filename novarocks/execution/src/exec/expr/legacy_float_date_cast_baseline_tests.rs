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

//! Independent immutable actual arena float-to-DATE behavior, including strict false-ALLOW errors.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{Array, ArrayRef, Date32Array, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;
pub(super) fn days(year: i32, month: u32, day: u32) -> i32 {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .num_days_from_ce()
        - 719163
}
pub(super) fn input(wide: bool, values: Vec<Option<f64>>) -> ArrayRef {
    if wide {
        Arc::new(Float64Array::from(values))
    } else {
        Arc::new(Float32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    }
}
pub(super) fn actual(
    array: &ArrayRef,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![
        ty.try_to_field("original-float").unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![array.clone()]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), array.data_type().clone());
    let target = arena.push_typed(ExprNode::Cast(source, policy), DataType::Date32);
    arena.eval(target, &chunk)
}
pub(super) fn cases(wide: bool) -> Vec<(f64, &'static str)> {
    let mut cases = vec![
        (0.0, "0"),
        (-0.0, "0"),
        (f64::from_bits(1), "0"),
        (-f64::from_bits(1), "0"),
        (100.999, "100"),
        (-101.9, "-101"),
        (691232.0, "691232"),
        (700100.0, "700100"),
        (20240230.0, "20240230"),
        (20241301.0, "20241300"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "inf"),
        (f64::NEG_INFINITY, "-inf"),
        (f64::MAX, "9223372036854775807"),
        (-f64::MAX, "-9223372036854775808"),
        (9223372036854775808.0, "9223372036854775807"),
        (-9223372036854775808.0, "-9223372036854775808"),
    ];
    if wide {
        // F64-to-i64 truncation does not validate the fractional part before the original grammar.
        cases.retain(|(value, _)| *value != 20241301.0);
        cases.extend([
            (20241301.0, "20241301"),
            (20240229240000.0, "20240229240000"),
            (20240229126000.0, "20240229126000"),
            (20240229123460.0, "20240229123460"),
            (99999999999999.0, "99999999999999"),
            (100000000000000.0, "100000000000000"),
        ]);
    } else {
        // Use actual finite f32 extrema; f64::MAX would already be an infinite f32 source.
        cases.retain(|(value, _)| value.abs() != f64::MAX);
        cases.extend([
            (f32::MAX as f64, "9223372036854775807"),
            (-(f32::MAX as f64), "-9223372036854775808"),
            (20240101.0, "20240100"),
            (99991231.0, "99991232"),
        ]);
    }
    cases
}
#[test]
fn legacy_float_date_original_full_valid_literal_shapes_and_float32_rounding() {
    let wide = vec![
        (101.9, (2000, 1, 1)),
        (690101.9, (2069, 1, 1)),
        (700101.9, (1970, 1, 1)),
        (991231.0, (1999, 12, 31)),
        (10000101.9, (1000, 1, 1)),
        (20240229.999, (2024, 2, 29)),
        (99991231.0, (9999, 12, 31)),
        (101000000.9, (2000, 1, 1)),
        (690101123456.0, (2069, 1, 1)),
        (700101123456.0, (1970, 1, 1)),
        (20240229123456.0, (2024, 2, 29)),
        (99991231235959.0, (9999, 12, 31)),
    ];
    let narrow = vec![
        (101.9, (2000, 1, 1)),
        (690101.9, (2069, 1, 1)),
        (700101.9, (1970, 1, 1)),
        (991231.0, (1999, 12, 31)),
        (10000101.0, (1000, 1, 1)),
        (20240229.0, (2024, 2, 28)),
        (101000000.0, (2000, 1, 1)),
        (690101123456.0, (2069, 1, 1)),
    ];
    for (wide, rows) in [(true, wide), (false, narrow)] {
        let array = input(
            wide,
            rows.iter().map(|(v, _)| Some(*v)).chain([None]).collect(),
        );
        let expected = Date32Array::from(
            rows.iter()
                .map(|(_, date)| Some(days(date.0, date.1, date.2)))
                .chain([None])
                .collect::<Vec<_>>(),
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                assert_eq!(
                    actual(&array, policy, allow).unwrap().to_data(),
                    expected.to_data()
                );
            }
        }
    }
}
#[test]
fn legacy_float_date_original_all_invalid_nonfinite_extremes_keep_full_string_under_both_allow_values()
 {
    for wide in [false, true] {
        for (value, literal) in cases(wide) {
            let array = input(wide, vec![None, Some(value)]);
            let message = format!(
                "CAST failed: from {:?} to Date32: invalid date literal {literal}",
                array.data_type()
            );
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    assert_eq!(actual(&array, policy, allow).unwrap_err(), message);
                }
            }
        }
    }
}
#[test]
fn legacy_float_date_original_null_mask_precedes_nonfinite_and_slice_empty_visitation() {
    for wide in [false, true] {
        let hidden: ArrayRef = if wide {
            Arc::new(Float64Array::new(
                vec![f64::NAN, f64::INFINITY, 20240228.0].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false, false, true])),
            ))
        } else {
            Arc::new(Float32Array::new(
                vec![f32::NAN, f32::INFINITY, 20240228.0].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false, false, true])),
            ))
        };
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let expected = Date32Array::from(vec![None, None, Some(days(2024, 2, 28))]);
                assert_eq!(
                    actual(&hidden, policy, allow).unwrap().to_data(),
                    expected.to_data()
                );
                assert!(
                    actual(&hidden.slice(0, 1), policy, allow)
                        .unwrap()
                        .is_null(0)
                );
                assert_eq!(actual(&hidden.slice(0, 0), policy, allow).unwrap().len(), 0);
                let source = input(wide, vec![Some(f64::NAN), Some(20240228.0), None]);
                let slice = source.slice(1, 2);
                assert_eq!(
                    actual(&slice, policy, allow).unwrap().to_data(),
                    Date32Array::from(vec![Some(days(2024, 2, 28)), None]).to_data()
                );
            }
        }
    }
}
