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

//! Pre-extraction differential for all 182 installed unary overloads.
use super::{FloatComparison, LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    ArrayRef, Decimal128Array, Float64Array, Int64Array, new_empty_array, new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;
const NAMES: [&str; 26] = [
    "acos", "asin", "atan", "cbrt", "ceil", "ceiling", "dceil", "cos", "cot", "degress", "dlog1",
    "exp", "dexp", "floor", "dfloor", "ln", "log10", "dlog10", "log2", "radians", "sin", "sqrt",
    "dsqrt", "square", "tan", "positive",
];
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

fn check(name: &str, array: ArrayRef, nullable: bool, policy: DecimalOverflowPolicy) {
    let integer = matches!(name, "ceil" | "ceiling" | "dceil" | "floor" | "dfloor");
    let result_nullable = name != "positive"
        || nullable
        || matches!(array.data_type(), DataType::Float32 | DataType::Float64);
    let summary = assert_scalar_matches_v1(
        ScalarDiffSpec::new(name)
            .typed_column(
                FunctionValueType::new(array.data_type().clone(), nullable),
                array,
            )
            .decimal_overflow(policy)
            .sparse_selections(7, 1901)
            .float_comparison(FloatComparison::Exact)
            .expect_result_type(FunctionValueType::new(
                if integer {
                    DataType::Int64
                } else {
                    DataType::Float64
                },
                result_nullable,
            )),
    );
    assert_eq!(summary.legacy_batch_errors, 0);
    assert_eq!(summary.attributed_row_errors, 0);
}
#[test]
fn pure_differential_unary_all_182_overloads_nullable_sparse_and_leaf_failure_policies() {
    let mut overloads = 0;
    for name in NAMES {
        for ty in primitive_profiles() {
            for nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    check(name, primitive_values(&ty, nullable, 0), nullable, policy);
                }
            }
            overloads += 1;
        }
        for nullable in [false, true] {
            check(
                name,
                decimal_values(18, 2, nullable),
                nullable,
                DecimalOverflowPolicy::OutputNull,
            );
        }
        overloads += 1;
    }
    assert_eq!(overloads, 182);
}
#[test]
fn pure_differential_unary_decimal_precision_scale_and_empty_null_profiles() {
    for name in NAMES {
        for precision in [9u8, 18, 38] {
            for scale in [-2i8, 0, 2, precision as i8] {
                for nullable in [false, true] {
                    check(
                        name,
                        decimal_values(precision, scale, nullable),
                        nullable,
                        DecimalOverflowPolicy::ReportError,
                    );
                }
            }
        }
        for ty in primitive_profiles()
            .into_iter()
            .chain([DataType::Decimal128(18, 2)])
        {
            check(
                name,
                new_empty_array(&ty),
                false,
                DecimalOverflowPolicy::OutputNull,
            );
            check(
                name,
                new_null_array(&ty, ROWS),
                true,
                DecimalOverflowPolicy::OutputNull,
            );
        }
    }
}
#[test]
fn pure_differential_unary_literal_and_pool_nonfinite_constants_preserve_broadcast() {
    for name in NAMES {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for value in [
                    Some(-0.0),
                    Some(0.0),
                    Some(-1.0),
                    Some(1.0),
                    Some(2.0),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(i64::MIN as f64),
                    Some(i64::MAX as f64),
                    None,
                ] {
                    let summary = assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .constant_array(Arc::new(Float64Array::from(vec![value])))
                            .constant_rows(ROWS)
                            .legacy_constants(form)
                            .decimal_overflow(policy)
                            .sparse_selections(7, 1921)
                            .float_comparison(FloatComparison::Exact),
                    );
                    assert_eq!(summary.legacy_batch_errors, 0);
                    assert_eq!(summary.attributed_row_errors, 0);
                }
            }
        }
    }
}
