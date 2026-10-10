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

//! Existing ROUND/DROUND admitted profiles before computation extraction.
use super::{FloatComparison, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array, new_empty_array,
    new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;
const ROWS: usize = 513;
fn primitive_profiles() -> [DataType; 6] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ]
}
fn primitive_values(ty: &DataType, nullable: bool, offset: usize) -> ArrayRef {
    if matches!(ty, DataType::Float32 | DataType::Float64) {
        let pattern = [
            Some(-0.0),
            Some(0.0),
            Some(1.0),
            Some(2.0),
            Some(8.0),
            Some(0.5),
            Some(-2.0),
            Some(f64::NAN),
            Some(f64::from_bits(0x7ff8_0000_0000_0123)),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::MIN_POSITIVE),
            Some(-f64::MIN_POSITIVE),
            Some(f64::from_bits(1)),
            None,
        ];
        let values: ArrayRef = Arc::new(Float64Array::from(
            (0..ROWS)
                .map(|row| {
                    pattern[(row + offset) % pattern.len()].or_else(|| (!nullable).then_some(3.0))
                })
                .collect::<Vec<_>>(),
        ));
        return arrow::compute::cast(&values, ty).unwrap();
    }
    let pattern = [
        Some(-128),
        Some(-8),
        Some(-1),
        Some(0),
        Some(1),
        Some(2),
        Some(8),
        Some(127),
        None,
    ];
    let values: ArrayRef = Arc::new(Int64Array::from(
        (0..ROWS)
            .map(|row| pattern[(row + offset) % pattern.len()].or_else(|| (!nullable).then_some(3)))
            .collect::<Vec<_>>(),
    ));
    arrow::compute::cast(&values, ty).unwrap()
}
fn decimal_values(precision: u8, scale: i8, nullable: bool) -> ArrayRef {
    let bound = 10_i128.pow(precision.min(9) as u32) - 1;
    let pattern = [
        Some(-bound),
        Some(-100),
        Some(-1),
        Some(0),
        Some(1),
        Some(100),
        Some(bound),
        None,
    ];
    Arc::new(
        Decimal128Array::from(
            (0..ROWS)
                .map(|row| pattern[row % pattern.len()].or_else(|| (!nullable).then_some(2)))
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(precision, scale)
        .unwrap(),
    )
}

fn typed(array: &ArrayRef, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(array.data_type().clone(), nullable)
}
fn check(spec: ScalarDiffSpec) {
    let summary = assert_scalar_matches_v1(
        spec.float_comparison(FloatComparison::Exact)
            .sparse_selections(7, 2903),
    );
    assert_eq!(summary.legacy_batch_errors, 0);
    assert_eq!(summary.attributed_row_errors, 0);
}
#[test]
fn pure_differential_dround_all_eight_exact_profiles_nullable_empty_and_null() {
    for nullable in [false, true] {
        for ty in primitive_profiles() {
            let a = primitive_values(&ty, nullable, 0);
            check(ScalarDiffSpec::new("dround").typed_column(typed(&a, nullable), a));
        }
        for precision in [9u8, 18, 38] {
            for scale in [-2i8, 0, 2, precision as i8] {
                let a = decimal_values(precision, scale, nullable);
                check(ScalarDiffSpec::new("dround").typed_column(typed(&a, nullable), a));
            }
        }
        let a = primitive_values(&DataType::Float64, nullable, 0);
        let pattern = [
            Some(0),
            Some(1),
            Some(-2),
            Some(308),
            Some(-308),
            Some(i32::MAX),
            Some(i32::MIN),
            None,
        ];
        let d: ArrayRef = Arc::new(Int32Array::from(
            (0..ROWS)
                .map(|r| pattern[r % pattern.len()].or_else(|| (!nullable).then_some(0)))
                .collect::<Vec<_>>(),
        ));
        check(
            ScalarDiffSpec::new("dround")
                .typed_column(typed(&a, nullable), a)
                .typed_column(typed(&d, nullable), d),
        );
    }
    for ty in primitive_profiles()
        .into_iter()
        .chain([DataType::Decimal128(38, 38)])
    {
        for rows in [0, 513] {
            let a = if rows == 0 {
                new_empty_array(&ty)
            } else {
                new_null_array(&ty, rows)
            };
            check(ScalarDiffSpec::new("dround").typed_column(typed(&a, true), a));
        }
    }
}
#[test]
fn pure_differential_round_numeric_leaf_profiles_runtime_digits_and_error_policy() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for nullable in [false, true] {
            for ty in primitive_profiles() {
                let a = primitive_values(&ty, nullable, 0);
                check(
                    ScalarDiffSpec::new("round")
                        .typed_column(typed(&a, nullable), a.clone())
                        .decimal_overflow(policy),
                );
                let pattern = [
                    Some(0),
                    Some(1),
                    Some(-2),
                    Some(309),
                    Some(-309),
                    Some(i64::MAX),
                    Some(4294967296),
                    None,
                ];
                let d: ArrayRef = Arc::new(Int64Array::from(
                    (0..ROWS)
                        .map(|r| pattern[r % pattern.len()].or_else(|| (!nullable).then_some(0)))
                        .collect::<Vec<_>>(),
                ));
                check(
                    ScalarDiffSpec::new("round")
                        .typed_column(typed(&a, nullable), a)
                        .typed_column(typed(&d, nullable), d)
                        .decimal_overflow(policy),
                );
            }
            // This pre-extraction equality matrix uses exact nonoverflow decimal rows.
            // The separate raw oracle freezes legacy out-of-precision acceptance.
            for precision in [9u8, 18, 38] {
                for scale in [0i8, 2, 6] {
                    let a = decimal_values(precision, scale, nullable);
                    check(
                        ScalarDiffSpec::new("round")
                            .typed_column(typed(&a, nullable), a.clone())
                            .decimal_overflow(policy),
                    );
                    let d: ArrayRef = Arc::new(Int64Array::from(
                        (0..ROWS)
                            .map(|r| Some([0, 1, -2, 2, 6][r % 5]))
                            .collect::<Vec<_>>(),
                    ));
                    check(
                        ScalarDiffSpec::new("round")
                            .typed_column(typed(&a, nullable), a)
                            .typed_column(typed(&d, false), d)
                            .decimal_overflow(policy),
                    );
                }
            }
        }
    }
}
