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

//! All eight existing ABS profiles, including exact boundary values.
use super::{FloatComparison, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, new_empty_array, new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
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
    let (minimum, maximum) = match ty {
        DataType::Int8 => (i8::MIN as i64, i8::MAX as i64),
        DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
        DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    };
    let pattern = [
        Some(minimum),
        Some(-8),
        Some(-1),
        Some(0),
        Some(1),
        Some(2),
        Some(8),
        Some(maximum),
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
    array: ArrayRef,
    nullable: bool,
    logical: ValueLogicalType,
    policy: DecimalOverflowPolicy,
) {
    let source = FunctionValueType {
        logical_type: logical,
        ..FunctionValueType::new(array.data_type().clone(), nullable)
    };
    let s = assert_scalar_matches_v1(
        ScalarDiffSpec::new("abs")
            .typed_column(source, array)
            .decimal_overflow(policy)
            .sparse_selections(7, 3907)
            .float_comparison(FloatComparison::Exact),
    );
    assert_eq!(s.legacy_batch_errors, 0);
    assert_eq!(s.attributed_row_errors, 0);
    assert_eq!(s.result_type.nullable, nullable);
}
#[test]
fn pure_differential_abs_all_eight_profiles_nullable_boundaries_and_decimal_scales() {
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for ty in primitive_profiles() {
                check(
                    primitive_values(&ty, nullable, 0),
                    nullable,
                    ValueLogicalType::Physical,
                    policy,
                );
            }
            for precision in [9u8, 18, 38] {
                for scale in [-2i8, 0, 2, precision as i8] {
                    check(
                        decimal_values(precision, scale, nullable),
                        nullable,
                        ValueLogicalType::Physical,
                        policy,
                    );
                }
            }
            let pattern = [
                Some(i128::MIN),
                Some(i128::MAX),
                Some(-1),
                Some(0),
                Some(1),
                Some(-9007199254740993),
                None,
            ];
            let values = (0..ROWS)
                .map(|r| pattern[r % pattern.len()].or_else(|| (!nullable).then_some(-2)))
                .collect::<Vec<_>>();
            check(
                novarocks_types::largeint::array_from_i128(&values).unwrap(),
                nullable,
                ValueLogicalType::LargeInt,
                policy,
            );
        }
    }
}
#[test]
fn pure_differential_abs_every_profile_empty_and_all_null() {
    for (ty, logical) in primitive_profiles()
        .into_iter()
        .map(|ty| (ty, ValueLogicalType::Physical))
        .chain([
            (DataType::Decimal128(38, 38), ValueLogicalType::Physical),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        ])
    {
        for rows in [0, 513] {
            let a = if rows == 0 {
                new_empty_array(&ty)
            } else {
                new_null_array(&ty, rows)
            };
            check(a, true, logical, DecimalOverflowPolicy::ReportError);
        }
    }
}
