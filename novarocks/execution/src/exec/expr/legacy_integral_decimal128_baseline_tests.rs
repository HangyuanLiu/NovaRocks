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

//! Independent original Types conversion and ExprArena precision/policy evidence.
//! These new expectations are UNRUN until the root's serialized Execution window.
pub(super) use super::legacy_decimal128_rescale_baseline_tests::{OVERFLOW, actual, modes, values};
use arrow::array::{Array, ArrayRef, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
pub(super) fn integral_input(dtype: &DataType, values: Vec<Option<i64>>) -> ArrayRef {
    macro_rules! array {
        ($array:ty, $value:ty) => {
            Arc::new(<$array>::from(
                values
                    .into_iter()
                    .map(|v| {
                        v.map(|n| {
                            <$value>::try_from(n).expect("fixture value fits its original carrier")
                        })
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match dtype {
        DataType::Int8 => array!(Int8Array, i8),
        DataType::Int16 => array!(Int16Array, i16),
        DataType::Int32 => array!(Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::from(values)),
        _ => panic!("fixture must supply an actual signed carrier"),
    }
}
fn success(array: ArrayRef, target: DataType, expected: &[Option<i128>]) {
    for (policy, allow) in modes() {
        let out = actual(array.clone(), target.clone(), policy, allow).unwrap();
        assert_eq!(out.data_type(), &target);
        assert_eq!(values(&out), expected);
    }
}
#[test]
fn integral_decimal128_original_all_four_carriers_required_precision_four_scale_zero() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        success(
            integral_input(&dtype, vec![Some(-100), Some(0), Some(100), None]),
            DataType::Decimal128(4, 0),
            &[Some(-100), Some(0), Some(100), None],
        );
    }
}
#[test]
fn integral_decimal128_original_signed_scale_truncates_negative_toward_zero() {
    let raw = vec![
        Some(149),
        Some(-149),
        Some(150),
        Some(-150),
        Some(9999),
        Some(-9999),
        None,
    ];
    success(
        integral_input(&DataType::Int64, raw.clone()),
        DataType::Decimal128(7, 2),
        &[
            Some(14900),
            Some(-14900),
            Some(15000),
            Some(-15000),
            Some(999900),
            Some(-999900),
            None,
        ],
    );
    success(
        integral_input(&DataType::Int64, raw),
        DataType::Decimal128(7, -2),
        &[
            Some(1),
            Some(-1),
            Some(1),
            Some(-1),
            Some(99),
            Some(-99),
            None,
        ],
    );
}
#[test]
fn integral_decimal128_original_full_carrier_multiply_overflow_and_all_nulls() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let bounds = match dtype {
            DataType::Int8 => (i8::MIN as i64, i8::MAX as i64),
            DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
            DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
            DataType::Int64 => (i64::MIN, i64::MAX),
            _ => unreachable!(),
        };
        success(
            integral_input(&dtype, vec![Some(bounds.0), Some(bounds.1), None]),
            DataType::Decimal128(38, 0),
            &[Some(bounds.0 as i128), Some(bounds.1 as i128), None],
        );
        success(
            integral_input(&dtype, vec![None, None]),
            DataType::Decimal128(38, 38),
            &[None, None],
        );
        let input = integral_input(&dtype, vec![Some(bounds.0), Some(bounds.1), None]);
        for (policy, allow) in modes() {
            let out = actual(input.clone(), DataType::Decimal128(38, 38), policy, allow);
            if policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(out.unwrap_err(), OVERFLOW);
            } else {
                assert_eq!(values(&out.unwrap()), vec![None, None, None]);
            }
        }
    }
}
#[test]
fn integral_decimal128_original_allow_throw_does_not_override_integral_outputnull_policy() {
    let input = integral_input(
        &DataType::Int64,
        vec![Some(999), Some(1000), Some(-999), Some(-1000), None],
    );
    for (policy, allow) in modes() {
        let out = actual(input.clone(), DataType::Decimal128(3, 0), policy, allow);
        if policy == DecimalOverflowPolicy::ReportError {
            assert_eq!(out.unwrap_err(), OVERFLOW);
        } else {
            assert_eq!(
                values(&out.unwrap()),
                vec![Some(999), None, Some(-999), None, None]
            );
        }
    }
}
#[test]
fn integral_decimal128_original_types_nineteen_digit_window_and_arena_precision_are_distinct() {
    let input = integral_input(&DataType::Int64, vec![Some(i64::MAX), Some(i64::MIN), None]);
    let n = novarocks_types::arrow_cast::cast_scalar_with_special_rules(
        &input,
        &DataType::Decimal128(18, 1),
    )
    .unwrap();
    assert_eq!(values(&n), vec![None, None, None]);
    let wide = novarocks_types::arrow_cast::cast_scalar_with_special_rules(
        &input,
        &DataType::Decimal128(19, 1),
    )
    .unwrap();
    assert_eq!(
        values(&wide),
        vec![
            Some(i64::MAX as i128 * 10),
            Some(i64::MIN as i128 * 10),
            None
        ]
    );
    for (policy, allow) in modes() {
        let out = actual(input.clone(), DataType::Decimal128(19, 1), policy, allow);
        if policy == DecimalOverflowPolicy::ReportError {
            assert_eq!(out.unwrap_err(), OVERFLOW);
        } else {
            assert_eq!(values(&out.unwrap()), vec![None, None, None]);
        }
    }
    success(
        input,
        DataType::Decimal128(38, 1),
        &[
            Some(i64::MAX as i128 * 10),
            Some(i64::MIN as i128 * 10),
            None,
        ],
    );
}
#[test]
fn integral_decimal128_original_factor_data_error_precedes_null_and_empty_row_scan() {
    for scale in [-39, 39] {
        let target = DataType::Decimal128(38, scale);
        let expected = format!(
            "CAST failed: from Int64 to {:?}: decimal scale overflow while casting integral",
            target
        );
        for input in [
            integral_input(&DataType::Int64, vec![]),
            integral_input(&DataType::Int64, vec![None]),
            integral_input(&DataType::Int64, vec![Some(1)]),
        ] {
            for (policy, allow) in modes() {
                assert_eq!(
                    actual(input.clone(), target.clone(), policy, allow).unwrap_err(),
                    expected
                );
            }
        }
    }
}
#[test]
fn integral_decimal128_original_i8_negative_scale_minimum_preserves_host_panic_or_release_data() {
    let target = DataType::Decimal128(38, i8::MIN);
    for input in [
        integral_input(&DataType::Int64, vec![]),
        integral_input(&DataType::Int64, vec![None]),
    ] {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            actual(
                input.clone(),
                target.clone(),
                DecimalOverflowPolicy::OutputNull,
                false,
            )
        }));
        if cfg!(debug_assertions) {
            assert!(result.is_err());
        } else {
            assert_eq!(
                result.unwrap().unwrap_err(),
                format!(
                    "CAST failed: from Int64 to {:?}: decimal scale overflow while casting integral",
                    target
                )
            );
        }
    }
}
#[test]
fn integral_decimal128_original_sliced_null_bitmap_and_empty_success_keep_exact_metadata() {
    let input = integral_input(
        &DataType::Int16,
        vec![Some(99), None, Some(-100), Some(150), None, Some(99)],
    )
    .slice(1, 4);
    success(
        input.clone(),
        DataType::Decimal128(4, 0),
        &[None, Some(-100), Some(150), None],
    );
    let empty = input.slice(0, 0);
    let out = actual(
        empty,
        DataType::Decimal128(4, 0),
        DecimalOverflowPolicy::ReportError,
        true,
    )
    .unwrap();
    assert_eq!(out.data_type(), &DataType::Decimal128(4, 0));
    assert_eq!(out.len(), 0);
    assert!(out.as_any().is::<Decimal128Array>());
}
