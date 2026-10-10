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

//! Extraction baseline for all 52 installed elementary numeric profiles.
use super::{FloatComparison, LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, new_empty_array, new_null_array,
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
    let bound = 10_i128.pow(precision as u32) - 1;
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
fn check(
    name: &str,
    values: &[ArrayRef],
    nullable: &[bool],
    seed: u64,
    policy: DecimalOverflowPolicy,
) {
    let mut spec = ScalarDiffSpec::new(name)
        .decimal_overflow(policy)
        .float_comparison(FloatComparison::Exact)
        .sparse_selections(7, seed);
    for (array, nullable) in values.iter().zip(nullable) {
        spec = spec.typed_column(
            FunctionValueType::new(array.data_type().clone(), *nullable),
            array.clone(),
        );
    }
    let result_nullable = name != "sign" || nullable[0];
    let summary = assert_scalar_matches_v1(
        spec.expect_result_type(FunctionValueType::new(DataType::Float64, result_nullable)),
    );
    assert_eq!(summary.legacy_batch_errors, 0);
    assert_eq!(summary.attributed_row_errors, 0);
    println!(
        "elementary {} [{}] input={:?} nullable={nullable:?} rows={} selections={} null_results={}",
        summary.function.as_str(),
        summary.overload.as_str(),
        values.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        summary.rows,
        summary.selections,
        summary.null_results
    );
}
#[test]
fn pure_differential_elementary_all_52_profiles_nullable_and_sparse() {
    let mut profiles = 0;
    for name in ["log", "sign"] {
        for (ordinal, ty) in primitive_profiles().into_iter().enumerate() {
            for nullable in [false, true] {
                check(
                    name,
                    &[primitive_values(&ty, nullable, 0)],
                    &[nullable],
                    1101 + ordinal as u64,
                    DecimalOverflowPolicy::OutputNull,
                );
            }
            profiles += 1;
        }
        for nullable in [false, true] {
            check(
                name,
                &[decimal_values(18, 2, nullable)],
                &[nullable],
                1111,
                DecimalOverflowPolicy::OutputNull,
            );
        }
        profiles += 1;
    }
    for (left_index, left) in primitive_profiles().into_iter().enumerate() {
        for (right_index, right) in primitive_profiles().into_iter().enumerate() {
            for left_nullable in [false, true] {
                for right_nullable in [false, true] {
                    check(
                        "log",
                        &[
                            primitive_values(&left, left_nullable, 0),
                            primitive_values(&right, right_nullable, 5),
                        ],
                        &[left_nullable, right_nullable],
                        1121 + (left_index * 6 + right_index) as u64,
                        DecimalOverflowPolicy::OutputNull,
                    );
                }
            }
            profiles += 1;
        }
    }
    for name in ["e", "pi"] {
        let summary = assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .constant_rows(ROWS)
                .sparse_selections(7, 1161)
                .expect_result_type(FunctionValueType::new(DataType::Float64, true)),
        );
        assert_eq!(summary.legacy_batch_errors, 0);
        assert_eq!(summary.attributed_row_errors, 0);
        profiles += 1;
    }
    assert_eq!(profiles, 52);
}
#[test]
fn pure_differential_elementary_decimal_conversion_precision_scale_and_both_policies() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for precision in [9, 18, 38] {
            for scale in [-2, 0, 2, precision as i8] {
                for nullable in [false, true] {
                    for name in ["log", "sign"] {
                        check(
                            name,
                            &[decimal_values(precision, scale, nullable)],
                            &[nullable],
                            1171,
                            policy,
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn pure_differential_elementary_empty_and_null_all_installed_domains() {
    let types = primitive_profiles()
        .into_iter()
        .chain([DataType::Decimal128(38, 38)])
        .collect::<Vec<_>>();
    for name in ["log", "sign"] {
        for ty in &types {
            for array in [new_empty_array(ty), new_null_array(ty, ROWS)] {
                check(
                    name,
                    &[array],
                    &[true],
                    1181,
                    DecimalOverflowPolicy::OutputNull,
                );
            }
        }
    }
    for left in primitive_profiles() {
        for right in primitive_profiles() {
            for rows in [0, ROWS] {
                check(
                    "log",
                    &[new_null_array(&left, rows), new_null_array(&right, rows)],
                    &[true, true],
                    1182,
                    DecimalOverflowPolicy::OutputNull,
                );
            }
        }
    }
    for name in ["e", "pi"] {
        for rows in [0, 1] {
            let result = assert_scalar_matches_v1(ScalarDiffSpec::new(name).constant_rows(rows));
            assert_eq!(result.legacy_batch_errors, 0);
            assert_eq!(result.attributed_row_errors, 0);
            assert_eq!(result.null_results, 0);
        }
    }
}
#[test]
fn pure_differential_elementary_literal_and_pool_constants_keep_broadcast_bits() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for name in ["log", "sign"] {
                for value in [
                    Some(-0.0),
                    Some(0.0),
                    Some(2.0),
                    Some(-2.0),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    None,
                ] {
                    let summary = assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .constant_array(Arc::new(Float64Array::from(vec![value])))
                            .constant_rows(ROWS)
                            .legacy_constants(form)
                            .decimal_overflow(policy)
                            .sparse_selections(7, 1191),
                    );
                    assert_eq!(summary.legacy_batch_errors, 0);
                    assert_eq!(summary.attributed_row_errors, 0);
                }
            }
            for (base, value) in [
                (Some(f64::INFINITY), Some(2.0)),
                (Some(f64::INFINITY), Some(0.5)),
                (Some(0.5), Some(1.0)),
                (None, Some(2.0)),
                (Some(2.0), None),
            ] {
                let summary = assert_scalar_matches_v1(
                    ScalarDiffSpec::new("log")
                        .constant_array(Arc::new(Float64Array::from(vec![base])))
                        .constant_array(Arc::new(Float64Array::from(vec![value])))
                        .constant_rows(ROWS)
                        .legacy_constants(form)
                        .decimal_overflow(policy)
                        .sparse_selections(7, 1192),
                );
                assert_eq!(summary.legacy_batch_errors, 0);
                assert_eq!(summary.attributed_row_errors, 0);
            }
        }
    }
}
