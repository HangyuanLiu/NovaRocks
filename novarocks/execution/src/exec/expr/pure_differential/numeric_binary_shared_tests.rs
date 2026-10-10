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
//! Every existing binary-numeric overload against the independent actual v1 dispatcher.
use super::{FloatComparison, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, new_empty_array, new_null_array};
use arrow::datatypes::DataType;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn profiles() -> [DataType; 6] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ]
}
fn values(ty: &DataType, nullable: bool, offset: usize) -> ArrayRef {
    if matches!(ty, DataType::Float32 | DataType::Float64) {
        let p = [
            Some(-7.9),
            Some(3.8),
            Some(0.0),
            Some(-0.0),
            Some(0.9),
            Some(-0.9),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(1)),
            None,
        ];
        let a: ArrayRef = Arc::new(Float64Array::from(
            (0..513)
                .map(|i| p[(i + offset) % p.len()].or_else(|| (!nullable).then_some(3.0)))
                .collect::<Vec<_>>(),
        ));
        return arrow::compute::cast(&a, ty).unwrap();
    }
    let (lo, hi) = match ty {
        DataType::Int8 => (i8::MIN as i64, i8::MAX as i64),
        DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
        DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    };
    let p = [
        Some(lo),
        Some(hi),
        Some(-7),
        Some(-3),
        Some(-1),
        Some(0),
        Some(1),
        Some(3),
        Some(7),
        None,
    ];
    let a: ArrayRef = Arc::new(Int64Array::from(
        (0..513)
            .map(|i| p[(i + offset) % p.len()].or_else(|| (!nullable).then_some(3)))
            .collect::<Vec<_>>(),
    ));
    arrow::compute::cast(&a, ty).unwrap()
}
fn check(spec: ScalarDiffSpec) {
    let s = assert_scalar_matches_v1(
        spec.float_comparison(FloatComparison::Exact)
            .sparse_selections(7, 0x4d4f44),
    );
    assert_eq!(s.legacy_batch_errors, 0);
    assert_eq!(s.attributed_row_errors, 0);
    assert!(s.result_type.nullable);
    assert_eq!(s.result_type.data_type, DataType::Float64);
}
fn names() -> [&'static str; 6] {
    ["atan2", "fmod", "pow", "fpow", "dpow", "power"]
}
#[test]
fn pure_differential_binary_all216_profiles_full_values_and_each_nullability_pair() {
    for name in names() {
        let mut count = 0;
        for l in profiles() {
            for r in profiles() {
                count += 1;
                for ln in [false, true] {
                    for rn in [false, true] {
                        check(
                            ScalarDiffSpec::new(name)
                                .typed_column(
                                    FunctionValueType::new(l.clone(), ln),
                                    values(&l, ln, 0),
                                )
                                .typed_column(
                                    FunctionValueType::new(r.clone(), rn),
                                    values(&r, rn, 3),
                                ),
                        );
                    }
                }
            }
        }
        assert_eq!(count, 36);
    }
}
#[test]
fn pure_differential_binary_all216_profiles_nonzero_slices_empty_and_all_null() {
    for name in names() {
        for l in profiles() {
            for r in profiles() {
                check(
                    ScalarDiffSpec::new(name)
                        .column(values(&l, true, 0).slice(3, 507))
                        .column(values(&r, true, 3).slice(3, 507)),
                );
                check(
                    ScalarDiffSpec::new(name)
                        .column(new_empty_array(&l))
                        .column(new_empty_array(&r)),
                );
                check(
                    ScalarDiffSpec::new(name)
                        .column(new_null_array(&l, 5))
                        .column(new_null_array(&r, 5)),
                );
            }
        }
    }
}
#[test]
fn pure_differential_binary_all216_profiles_constants_on_each_side() {
    for name in names() {
        for l in profiles() {
            for r in profiles() {
                let left = values(&l, false, 6).slice(0, 1);
                let right = values(&r, false, 3).slice(0, 1);
                check(
                    ScalarDiffSpec::new(name)
                        .constant_array(left.clone())
                        .column(values(&r, true, 0)),
                );
                check(
                    ScalarDiffSpec::new(name)
                        .column(values(&l, true, 0))
                        .constant_array(right.clone()),
                );
                check(
                    ScalarDiffSpec::new(name)
                        .constant_array(left)
                        .constant_array(right)
                        .constant_rows(11),
                );
            }
        }
    }
}
#[test]
fn pure_differential_binary_raw_nan_infinity_signed_zero_and_signed_integer_rounding() {
    for name in names() {
        check(
            ScalarDiffSpec::new(name)
                .column(Arc::new(Float64Array::from(vec![
                    Some(f64::NAN),
                    Some(1.0),
                    Some(f64::INFINITY),
                    Some(-f64::INFINITY),
                    Some(-0.0),
                    Some(-0.0),
                    Some(5.5),
                    None,
                ])))
                .column(Arc::new(Float64Array::from(vec![
                    Some(0.0),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(1.0),
                    Some(-1.0),
                    Some(f64::INFINITY),
                    Some(0.0),
                ]))),
        );
        check(
            ScalarDiffSpec::new(name)
                .column(Arc::new(Int64Array::from(vec![
                    Some(9007199254740993),
                    Some(i64::MAX),
                    Some(i64::MIN),
                    None,
                ])))
                .column(Arc::new(Int64Array::from(vec![
                    Some(2),
                    Some(2),
                    Some(-1),
                    Some(0),
                ]))),
        );
    }
}
