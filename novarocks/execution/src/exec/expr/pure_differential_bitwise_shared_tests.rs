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
//! Accurate 20 bitwise overloads: pre-extraction v1/pure oracle and post-extraction gate.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1, constant};
use arrow::array::{ArrayRef, Int64Array, new_empty_array, new_null_array};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const NAMES: [&str; 4] = ["bitand", "bitor", "bitxor", "bitnot"];
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

#[test]
fn bitwise_shared_all_20_profiles_boundaries_slices_nulls_and_both_policies() {
    let mut profiles_seen = 0;
    for name in NAMES {
        for ty in profiles(false) {
            profiles_seen += 1;
            for nullable in [false, true] {
                let source = FunctionValueType {
                    nullable,
                    ..ty.clone()
                };
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    let left = source_values(&source);
                    let right = left.slice(0, ROWS - 1);
                    let left = left.slice(1, ROWS - 1);
                    let mut spec = ScalarDiffSpec::new(name)
                        .typed_column(source.clone(), left)
                        .decimal_overflow(policy)
                        .sparse_selections(7, 9281)
                        .expect_result_type(FunctionValueType {
                            nullable: true,
                            ..source.clone()
                        });
                    if name != "bitnot" {
                        spec = spec.typed_column(source.clone(), right);
                    }
                    let summary = assert_scalar_matches_v1(spec);
                    assert_eq!(summary.legacy_batch_errors, 0);
                    assert_eq!(summary.attributed_row_errors, 0);
                }
            }
        }
    }
    assert_eq!(profiles_seen, 20);
}
#[test]
fn bitwise_shared_all_profiles_empty_and_all_null() {
    for name in NAMES {
        for source in profiles(false) {
            for rows in [0, ROWS] {
                let source = FunctionValueType {
                    nullable: rows != 0,
                    ..source.clone()
                };
                let values = if rows == 0 {
                    new_empty_array(&source.data_type)
                } else {
                    new_null_array(&source.data_type, rows)
                };
                let mut spec = ScalarDiffSpec::new(name)
                    .typed_column(source.clone(), values.clone())
                    .sparse_selections(5, 9282);
                if name != "bitnot" {
                    spec = spec.typed_column(source, values);
                }
                assert_scalar_matches_v1(spec);
            }
        }
    }
}
#[test]
fn bitwise_shared_literal_and_pool_broadcast() {
    for name in NAMES {
        for source in profiles(false) {
            for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
                let values = source_values(&source);
                let const_left = constant(source.clone(), values.slice(0, 1));
                let const_right = constant(source.clone(), values.slice(1, 1));
                let mut spec = ScalarDiffSpec::new(name)
                    .constant(const_left.clone())
                    .legacy_constants(form)
                    .sparse_selections(7, 9283);
                if name != "bitnot" {
                    spec = spec.typed_column(source.clone(), values.clone());
                } else {
                    spec = spec.constant_rows(ROWS);
                }
                assert_scalar_matches_v1(spec);
                if name != "bitnot" {
                    assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .typed_column(source.clone(), values)
                            .constant(const_right.clone())
                            .legacy_constants(form)
                            .sparse_selections(7, 9284),
                    );
                    assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .constant(const_left)
                            .constant(const_right)
                            .constant_rows(ROWS)
                            .legacy_constants(form)
                            .sparse_selections(7, 9285),
                    );
                }
            }
        }
    }
}
