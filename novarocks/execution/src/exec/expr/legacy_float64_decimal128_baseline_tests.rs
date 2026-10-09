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
//! Original actual scalar path; Expr and Project both delegate Types before dead historical scalar arms.
use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array};
use arrow::datatypes::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
pub(super) fn input(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
pub(super) fn values(a: &ArrayRef) -> Vec<Option<i128>> {
    let a = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
    (0..a.len())
        .map(|r| (!a.is_null(r)).then(|| a.value(r)))
        .collect()
}
pub(super) fn actual(
    a: ArrayRef,
    p: u8,
    s: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, String> {
    super::legacy_decimal128_rescale_baseline_tests::actual(
        a,
        DataType::Decimal128(p, s),
        policy,
        allow,
    )
}
pub(super) fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    super::legacy_decimal128_rescale_baseline_tests::modes()
}
fn project(a: &ArrayRef, p: u8, s: i8) -> Result<ArrayRef, String> {
    super::cast_array_to_target(a, &DataType::Decimal128(p, s))
}
#[test]
fn legacy_float64_decimal128_original_rounding_nonfinite_all_mode_and_true_routes() {
    let a = input(vec![
        Some(1.25),
        Some(-1.25),
        Some(1.24),
        Some(-1.24),
        Some(0.0),
        Some(-0.0),
        None,
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ]);
    let expected = vec![
        Some(13),
        Some(-13),
        Some(12),
        Some(-12),
        Some(0),
        Some(0),
        None,
        None,
        None,
        None,
    ];
    assert_eq!(values(&project(&a, 18, 1).unwrap()), expected);
    assert_eq!(
        values(
            &novarocks_types::arrow_cast::cast_scalar_with_special_rules(
                &a,
                &DataType::Decimal128(18, 1)
            )
            .unwrap()
        ),
        expected
    );
    for (policy, allow) in modes() {
        let out = actual(a.clone(), 18, 1, policy, allow).unwrap();
        assert_eq!(out.data_type(), &DataType::Decimal128(18, 1));
        assert_eq!(values(&out), expected);
    }
}
#[test]
fn legacy_float64_decimal128_original_relaxed_target_precision_and_signed_scale() {
    for (p, s, raw, expected) in [
        (
            1,
            0,
            vec![Some(12.0), Some(-12.0), Some(1e18), None],
            vec![Some(12), Some(-12), None, None],
        ),
        (
            18,
            -2,
            vec![Some(149.0), Some(150.0), Some(-149.0), Some(-150.0), None],
            vec![Some(1), Some(2), Some(-1), Some(-2), None],
        ),
    ] {
        let a = input(raw);
        // Project/Types preserve the original relaxed precision result.
        assert_eq!(values(&project(&a, p, s).unwrap()), expected);
        assert_eq!(
            values(
                &novarocks_types::arrow_cast::cast_scalar_with_special_rules(
                    &a,
                    &DataType::Decimal128(p, s)
                )
                .unwrap()
            ),
            expected
        );
        for (policy, allow) in modes() {
            let result = actual(a.clone(), p, s, policy, allow);
            if p == 1 && policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(
                    result.unwrap_err(),
                    "Expr evaluate meet error: The numeric type cast involving decimal overflows"
                );
            } else {
                let expected = if p == 1 {
                    vec![None; a.len()]
                } else {
                    expected.clone()
                };
                assert_eq!(values(&result.unwrap()), expected);
            }
        }
    }
}
#[test]
fn legacy_float64_decimal128_original_scale_error_precedes_null_and_empty() {
    for s in [-39, -127] {
        let bare = format!("decimal scale overflow while casting float to DECIMAL: scale={s}");
        for a in [input(vec![]), input(vec![None]), input(vec![Some(0.0)])] {
            assert_eq!(project(&a, 38, s).unwrap_err(), bare);
            for (policy, allow) in modes() {
                assert_eq!(
                    actual(a.clone(), 38, s, policy, allow).unwrap_err(),
                    format!("CAST failed: from Float64 to Decimal128(38, {s}): {bare}")
                );
            }
        }
    }
}
#[test]
fn legacy_float64_decimal128_original_extreme_and_minus128_panics_remain_unfixed() {
    for (policy, allow) in modes() {
        for a in [input(vec![Some(i128::MIN as f64)]), input(vec![None])] {
            let s = if a.null_count() == 1 { -128 } else { 0 };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                actual(a.clone(), 38, s, policy, allow)
            }));
            if cfg!(debug_assertions) {
                assert!(result.is_err());
            } else {
                assert!(result.is_ok());
            }
        }
    }
    let hidden = Arc::new(Float64Array::new(
        vec![i128::MIN as f64, 1.0].into(),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    )) as ArrayRef;
    for (policy, allow) in modes() {
        assert_eq!(
            values(&actual(hidden.clone(), 38, 0, policy, allow).unwrap()),
            vec![None, Some(1)]
        );
    }
    let a = input(vec![Some(99.0), Some(-1.25), None, Some(1.25)]).slice(1, 3);
    for (policy, allow) in modes() {
        assert_eq!(
            values(&actual(a.clone(), 18, 1, policy, allow).unwrap()),
            vec![Some(-13), None, Some(13)]
        );
        assert!(values(&actual(a.slice(0, 0), 18, 1, policy, allow).unwrap()).is_empty());
    }
}
