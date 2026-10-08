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

//! A1 legacy differential coverage: every numeric width and every alias.
use super::FloatComparison;
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float64Array, Int64Array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;

#[test]
fn pure_differential_a1_avg_all_numeric_widths_single_and_partial_final() {
    let types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    for (index, ty) in types.into_iter().enumerate() {
        let source = FunctionValueType::new(ty, true);
        let values =
            InputGenerator::new(801 + index as u64).column(&source, 256, &InputProfile::default());
        let groups = InputGenerator::new(81).group_ids(256, 8);
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new("avg")
                .typed_column(source, values)
                .grouped(groups, 9)
                .partitions(5, 803)
                .float_comparison(FloatComparison::Tolerance {
                    absolute: 0.0,
                    relative: 1e-9,
                }),
        );
    }
}
#[test]
fn pure_differential_a1_avg_decimal_rounding_and_empty_groups() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for scale in [0, 2, 6] {
            let values: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(1001), Some(1002), Some(-1001), Some(-1002), None])
                    .with_precision_and_scale(18, scale)
                    .unwrap(),
            );
            let wide: ArrayRef = Arc::new(
                Decimal256Array::from(vec![
                    Some(arrow_buffer::i256::from_i128(1)),
                    Some(arrow_buffer::i256::from_i128(2)),
                    Some(arrow_buffer::i256::from_i128(-1)),
                    Some(arrow_buffer::i256::from_i128(-2)),
                    None,
                ])
                .with_precision_and_scale(40, scale)
                .unwrap(),
            );
            for values in [values, wide] {
                assert_aggregate_matches_v1(
                    AggregateDiffSpec::new("avg")
                        .column(values)
                        .grouped(vec![0, 0, 1, 1, 2], 4)
                        .partitions(3, 82)
                        .semantics(super::DiffSemantics {
                            decimal_overflow_policy: policy,
                            ..super::DiffSemantics::default()
                        }),
                );
            }
        }
    }
}
#[test]
fn pure_differential_a1_boolean_aliases_preserve_null_input_and_empty_state() {
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        None,
        None,
        Some(true),
        Some(false),
    ]));
    for name in [
        "count_if",
        "bool_or",
        "boolor_agg",
        "bool_and",
        "booland_agg",
    ] {
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(values.clone())
                .grouped(vec![0, 0, 0, 1, 1, 2, 3], 5)
                .partitions(5, 84),
        );
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(Arc::new(BooleanArray::from(Vec::<Option<bool>>::new()))),
        );
    }
}
#[test]
fn pure_differential_a1_moment_aliases_every_input_width() {
    let names = [
        "variance",
        "variance_pop",
        "var_pop",
        "variance_samp",
        "var_samp",
        "stddev",
        "std",
        "stddev_pop",
        "stddev_samp",
    ];
    let values: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        None,
        Some(-1),
        Some(7),
        Some(7),
        None,
    ]));
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        let values = arrow::compute::cast(&values, &ty).unwrap();
        for name in names {
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .column(values.clone())
                    .grouped(vec![0, 0, 0, 0, 0, 1, 2, 2, 3], 5)
                    .partitions(4, 85),
            );
        }
    }
}
#[test]
fn pure_differential_a1_covar_corr_mixed_widths_pair_nulls_and_degenerate_groups() {
    let x: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        None,
        Some(4),
        Some(9),
        Some(9),
        Some(1),
        None,
    ]));
    let y: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(2.0),
        Some(4.0),
        Some(8.0),
        Some(3.0),
        None,
        Some(2.0),
        Some(3.0),
        Some(0.0),
        None,
    ]));
    for left in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        for right in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ] {
            for name in ["covar_pop", "covar_samp", "corr"] {
                assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .column(arrow::compute::cast(&x, &left).unwrap())
                        .column(arrow::compute::cast(&y, &right).unwrap())
                        .grouped(vec![0, 0, 0, 0, 0, 1, 1, 2, 3], 5)
                        .partitions(4, 86),
                );
            }
        }
    }
}
#[test]
fn pure_differential_a1_special_floats_follow_v1() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(-0.0),
        Some(0.0),
        None,
        Some(1.0),
        Some(2.0),
    ]));
    for name in ["avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp"] {
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(values.clone())
                .grouped(vec![0, 1, 2, 3, 3, 4, 5, 5], 7)
                .partitions(3, 87),
        );
    }
    for name in ["covar_pop", "covar_samp", "corr"] {
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(values.clone())
                .column(values.clone())
                .grouped(vec![0, 1, 2, 3, 3, 4, 5, 5], 7)
                .partitions(3, 88),
        );
    }
}

#[test]
fn pure_differential_a1_constant_broadcast_and_null_constant() {
    for nullable in [true, false] {
        for name in ["avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp"] {
            let value = super::constant(
                FunctionValueType::new(DataType::Int64, nullable),
                Arc::new(Int64Array::from(vec![Some(3)])),
            );
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .constant(value)
                    .constant_rows(7)
                    .grouped(vec![0, 0, 1, 1, 1, 2, 3], 5),
            );
        }
    }
    for name in ["avg", "var_pop", "stddev_samp"] {
        let value = super::constant(
            FunctionValueType::new(DataType::Int64, true),
            Arc::new(Int64Array::from(vec![None])),
        );
        assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .constant(value)
                .constant_rows(7)
                .grouped(vec![0, 0, 1, 1, 1, 2, 3], 5),
        );
    }
    for name in ["count_if", "bool_or", "bool_and"] {
        for value in [None, Some(true), Some(false)] {
            let value = super::constant(
                FunctionValueType::new(DataType::Boolean, true),
                Arc::new(BooleanArray::from(vec![value])),
            );
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .constant(value)
                    .constant_rows(7)
                    .grouped(vec![0, 0, 1, 1, 1, 2, 3], 5),
            );
        }
    }
}

#[test]
fn pure_differential_a1_decimal_scale_overflow_is_required_under_both_policies() {
    let maximum = 99_999_999_999_999_999_999_999_999_999_999_999_999i128;
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(maximum), None, Some(1)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new("avg")
                .column(values.clone())
                .grouped(vec![0, 0, 1], 3)
                .partitions(2, 89)
                .semantics(super::DiffSemantics {
                    decimal_overflow_policy: policy,
                    ..super::DiffSemantics::default()
                }),
        );
        assert_eq!(summary.matched_failures, 2);
    }
}
