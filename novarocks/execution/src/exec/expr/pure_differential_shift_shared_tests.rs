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
//! Accurate 15 shift overloads: pre-extraction v1/pure oracle and post-extraction gate.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant};
use arrow::array::{ArrayRef, Int64Array, new_empty_array, new_null_array};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const NAMES: [&str; 3] = [
    "bit_shift_left",
    "bit_shift_right",
    "bit_shift_right_logical",
];
const ROWS: usize = 513;
fn profiles(nullable: bool) -> Vec<FunctionValueType> {
    let mut values = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ]
    .into_iter()
    .map(|ty| FunctionValueType::new(ty, nullable))
    .collect::<Vec<_>>();
    values.push(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            nullable,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    values
}
fn source_values(ty: &FunctionValueType) -> ArrayRef {
    let values: ArrayRef = if ty.logical_type == ValueLogicalType::LargeInt {
        let pattern = [
            Some(i128::MIN),
            Some(i128::MAX),
            Some(-1),
            Some(0),
            Some(1),
            None,
        ];
        novarocks_types::largeint::array_from_i128(
            &(0..ROWS + 2)
                .map(|row| pattern[row % pattern.len()].or_else(|| (!ty.nullable).then_some(7)))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    } else {
        let (min, max) = match ty.data_type {
            DataType::Int8 => (i8::MIN as i64, i8::MAX as i64),
            DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
            DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
            _ => (i64::MIN, i64::MAX),
        };
        let pattern = [Some(min), Some(max), Some(-1), Some(0), Some(1), None];
        let values: ArrayRef = Arc::new(Int64Array::from(
            (0..ROWS + 2)
                .map(|row| pattern[row % pattern.len()].or_else(|| (!ty.nullable).then_some(7)))
                .collect::<Vec<_>>(),
        ));
        arrow::compute::cast(&values, &ty.data_type).unwrap()
    };
    values.slice(1, ROWS)
}
fn count_values(nullable: bool) -> ArrayRef {
    let pattern = [
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-128),
        Some(-64),
        Some(-1),
        Some(0),
        Some(1),
        Some(7),
        Some(8),
        Some(15),
        Some(16),
        Some(31),
        Some(32),
        Some(63),
        Some(64),
        Some(127),
        Some(128),
        Some(129),
        Some((1_i64 << 32) + 129),
        None,
    ];
    let values: ArrayRef = Arc::new(Int64Array::from(
        (0..ROWS + 4)
            .map(|row| pattern[row % pattern.len()].or_else(|| (!nullable).then_some(2)))
            .collect::<Vec<_>>(),
    ));
    values.slice(3, ROWS)
}
#[test]
fn pure_differential_shift_all_15_profiles_boundaries_slices_and_both_overflow_policies() {
    let mut profiles_seen = 0;
    for name in NAMES {
        for source in profiles(false) {
            profiles_seen += 1;
            for source_nullable in [false, true] {
                let source = FunctionValueType {
                    nullable: source_nullable,
                    ..source.clone()
                };
                for count_nullable in [false, true] {
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        let summary = assert_scalar_matches_v1(
                            ScalarDiffSpec::new(name)
                                .typed_column(source.clone(), source_values(&source))
                                .typed_column(
                                    FunctionValueType::new(DataType::Int64, count_nullable),
                                    count_values(count_nullable),
                                )
                                .decimal_overflow(policy)
                                .sparse_selections(7, 9271)
                                .expect_result_type(FunctionValueType {
                                    nullable: true,
                                    ..source.clone()
                                }),
                        );
                        assert_eq!(summary.legacy_batch_errors, 0);
                        assert_eq!(summary.attributed_row_errors, 0);
                    }
                }
            }
        }
    }
    assert_eq!(profiles_seen, 15);
}
#[test]
fn pure_differential_shift_all_profiles_empty_and_all_null() {
    for name in NAMES {
        for source in profiles(false) {
            for rows in [0, ROWS] {
                let nullable = rows != 0;
                let source = FunctionValueType {
                    nullable,
                    ..source.clone()
                };
                let left = if rows == 0 {
                    new_empty_array(&source.data_type)
                } else {
                    new_null_array(&source.data_type, rows)
                };
                let right = if rows == 0 {
                    new_empty_array(&DataType::Int64)
                } else {
                    new_null_array(&DataType::Int64, rows)
                };
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(source, left)
                        .typed_column(FunctionValueType::new(DataType::Int64, nullable), right)
                        .sparse_selections(5, 9272),
                );
            }
        }
    }
}
#[test]
fn pure_differential_shift_literal_and_pool_broadcast_each_source_and_count() {
    for name in NAMES {
        for source in profiles(false) {
            for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
                let left = source_values(&source);
                let right = count_values(false);
                let constant_left = constant(source.clone(), left.slice(0, 1));
                let constant_right = constant(
                    FunctionValueType::new(DataType::Int64, false),
                    right.slice(0, 1),
                );
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .constant(constant_left.clone())
                        .typed_column(FunctionValueType::new(DataType::Int64, false), right)
                        .legacy_constants(form)
                        .sparse_selections(7, 9273),
                );
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(source.clone(), left)
                        .constant(constant_right.clone())
                        .legacy_constants(form)
                        .sparse_selections(7, 9274),
                );
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .constant(constant_left)
                        .constant(constant_right)
                        .constant_rows(ROWS)
                        .legacy_constants(form)
                        .sparse_selections(7, 9275),
                );
            }
        }
    }
}
