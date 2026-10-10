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

//! Immutable real arena Date32/float CAST behavior before core extraction.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{Array, ArrayRef, Date32Array, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;

pub(super) fn day(year: i32, month: u32, day: u32) -> i32 {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .num_days_from_ce()
        - 719163
}
pub(super) fn actual(
    input: &ArrayRef,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    let ty = FunctionValueType::new(input.data_type().clone(), true);
    let schema = Arc::new(Schema::new(vec![ty.try_to_field("date_source").unwrap()]));
    let batch = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), input.data_type().clone());
    let cast = arena.push_typed(ExprNode::Cast(source, policy), target);
    arena.eval(cast, &chunk)
}
pub(super) fn original(input: &ArrayRef, target: DataType) -> Result<ArrayRef, String> {
    actual(input, target, DecimalOverflowPolicy::ReportError, true)
}
pub(super) fn assert_literal(array: &ArrayRef, row: usize, literal: i32) {
    match array.data_type() {
        DataType::Float32 => assert_eq!(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row)
                .to_bits(),
            (literal as f32).to_bits()
        ),
        DataType::Float64 => assert_eq!(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row)
                .to_bits(),
            (literal as f64).to_bits()
        ),
        _ => panic!("fixture requires float output"),
    }
}
#[test]
fn legacy_date_float_original_literals_null_slice_empty_and_float_rounding() {
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![
        Some(0),
        Some(-1),
        None,
        Some(day(2000, 2, 29)),
        Some(day(2024, 2, 29)),
        Some(day(-1, 12, 31)),
        Some(day(0, 1, 1)),
    ]));
    for target in [DataType::Float32, DataType::Float64] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let output = actual(&dates, target.clone(), policy, allow).unwrap();
                assert_eq!(output.data_type(), &target);
                for (row, value) in [
                    (0, 19700101),
                    (1, 19691231),
                    (3, 20000229),
                    (4, 20240229),
                    (5, -8769),
                    (6, 101),
                ] {
                    assert_literal(&output, row, value);
                }
                assert!(output.is_null(2));
                let sliced = original(&dates.slice(1, 3), target.clone()).unwrap();
                assert_literal(&sliced, 0, 19691231);
                assert!(sliced.is_null(1));
                assert_literal(&sliced, 2, 20000229);
                assert_eq!(
                    original(&dates.slice(0, 0), target.clone()).unwrap().len(),
                    0
                );
            }
        }
    }
}
#[test]
fn legacy_date_float_original_invalid_full_error_and_hidden_null_mask() {
    for target in [DataType::Float32, DataType::Float64] {
        let bad: ArrayRef = Arc::new(Date32Array::from(vec![None, Some(i32::MIN)]));
        assert_eq!(
            original(&bad, target.clone()).unwrap_err(),
            format!("CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648")
        );
        let hidden: ArrayRef = Arc::new(Date32Array::new(
            vec![i32::MAX, day(2024, 2, 29)].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        ));
        let output = original(&hidden, target).unwrap();
        assert!(output.is_null(0));
        assert_literal(&output, 1, 20240229);
    }
}
#[test]
fn legacy_date_float_original_extreme_days_year_multiply_panic_and_release_wrapping() {
    for target in [DataType::Float32, DataType::Float64] {
        for (days, literal) in [
            (day(262142, 12, 31), -1673546065),
            (day(-262143, 1, 1), 1673537397),
            (day(214749, 1, 1), -2147477195),
            (day(-214749, 1, 1), 2147477397),
        ] {
            let input: ArrayRef = Arc::new(Date32Array::from(vec![days]));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                original(&input, target.clone())
            }));
            if cfg!(debug_assertions) {
                assert!(
                    result.is_err(),
                    "original year multiply must panic at {days}"
                );
            } else {
                assert_literal(&result.unwrap().unwrap(), 0, literal);
            }
        }
        let maximum: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original(&maximum, target.clone())
        }));
        if cfg!(debug_assertions) {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result.unwrap().unwrap_err(),
                format!("CAST failed: from Date32 to {target:?}: invalid Date32 value 2147483647")
            );
        }
        for (year, literal) in [(214748, 2147481231), (-214748, -2147478769)] {
            let input: ArrayRef = Arc::new(Date32Array::from(vec![day(year, 12, 31)]));
            assert_literal(&original(&input, target.clone()).unwrap(), 0, literal);
        }
    }
}
#[test]
fn legacy_float_date_original_invalid_nonfinite_error_and_null_mask() {
    for wide in [false, true] {
        let source = if wide {
            DataType::Float64
        } else {
            DataType::Float32
        };
        for (value, message) in [
            (20240230.0, "20240230"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            // 20240230 is representable in f32 and keeps the same original error.
            let input: ArrayRef = if wide {
                Arc::new(Float64Array::from(vec![None, Some(value)]))
            } else {
                Arc::new(Float32Array::from(vec![None, Some(value as f32)]))
            };
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    assert_eq!(
                        actual(&input, DataType::Date32, policy, allow).unwrap_err(),
                        format!(
                            "CAST failed: from {source:?} to Date32: invalid date literal {message}"
                        )
                    );
                }
            }
        }
        let masked: ArrayRef = if wide {
            Arc::new(Float64Array::new(
                vec![f64::NAN].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false])),
            ))
        } else {
            Arc::new(Float32Array::new(
                vec![f32::NAN].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false])),
            ))
        };
        assert!(original(&masked, DataType::Date32).unwrap().is_null(0));
    }
}
