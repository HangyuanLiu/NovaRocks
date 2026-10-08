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
//! Exact BY profiles after the unchanged v1 raw baseline is frozen.
use super::FloatComparison;
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, new_empty_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 513;
fn profiles(nullable: bool) -> Vec<FunctionValueType> {
    let mut types = vec![
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("UTC".into()), Some("Asia/Shanghai".into())] {
            types.push(DataType::Timestamp(unit, zone));
        }
    }
    for (precisions, wide) in [([9, 18, 38], false), ([40, 60, 76], true)] {
        for precision in precisions {
            for scale in [-2, 0, 2, precision as i8] {
                types.push(if wide {
                    DataType::Decimal256(precision, scale)
                } else {
                    DataType::Decimal128(precision, scale)
                });
            }
        }
    }
    let mut values = types
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
fn check(
    name: &str,
    value: FunctionValueType,
    values: ArrayRef,
    key: FunctionValueType,
    keys: ArrayRef,
) {
    let summary = assert_aggregate_matches_v1(
        AggregateDiffSpec::new(name)
            .typed_column(value, values)
            .typed_column(key, keys)
            .grouped((0..ROWS).map(|row| row % 6).collect(), 8)
            .partitions(7, 1711)
            .float_comparison(FloatComparison::Exact),
    );
    assert_eq!(summary.matched_failures, 0);
    assert!(summary.result_type.nullable);
    assert!(summary.null_results >= 4);
}
#[test]
fn pure_differential_by_all_flat_value_and_key_profiles_with_exact_nullable_facts() {
    for name in ["max_by", "min_by"] {
        for nullable in [false, true] {
            for source in profiles(nullable) {
                let values =
                    InputGenerator::new(1701).column(&source, ROWS, &InputProfile::default());
                for key_nullable in [false, true] {
                    let key = FunctionValueType::new(DataType::Int64, key_nullable);
                    let keys =
                        InputGenerator::new(1702).column(&key, ROWS, &InputProfile::default());
                    check(name, source.clone(), values.clone(), key, keys);
                }
                // Floating-key success profiles are finite. NaN comparison
                // refusal is a separate exact legacy baseline/differential.
                let keys = InputGenerator::new(1703).column(
                    &source,
                    ROWS,
                    &InputProfile::default().with_boundary_ratio(0.0),
                );
                for value_nullable in [false, true] {
                    let value = FunctionValueType::new(DataType::Utf8, value_nullable);
                    let values =
                        InputGenerator::new(1704).column(&value, ROWS, &InputProfile::default());
                    check(name, value, values, source.clone(), keys.clone());
                }
            }
        }
    }
}
#[test]
fn pure_differential_by_nested_values_and_keys_reuse_original_recursive_domains() {
    use crate::exec::expr::agg::{
        AggScalarValue as V, build_agg_scalar_array as build_scalar_array,
    };
    let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let map = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Binary, true),
            ])),
            false,
        )),
        false,
    );
    let structure = DataType::Struct(Fields::from(vec![
        Field::new("list", list.clone(), true),
        Field::new("map", map.clone(), true),
    ]));
    let profiles = [
        (
            list,
            Some(V::List(vec![
                Some(V::Utf8("first".into())),
                None,
                Some(V::Utf8("中文".into())),
            ])),
        ),
        (
            map,
            Some(V::Map(vec![
                (Some(V::Utf8("a".into())), Some(V::Binary(vec![0, 1, 255]))),
                (Some(V::Utf8("b".into())), None),
            ])),
        ),
        (structure, Some(V::Struct(vec![None, None]))),
    ];
    for name in ["max_by", "min_by"] {
        for (ty, payload) in &profiles {
            let nested = build_scalar_array(
                ty,
                (0..ROWS)
                    .map(|row| if row % 7 == 0 { None } else { payload.clone() })
                    .collect(),
            )
            .unwrap();
            let values: ArrayRef = Arc::new(Int64Array::from(
                (0..ROWS).map(|row| Some(row as i64)).collect::<Vec<_>>(),
            ));
            let keys = values.clone();
            check(
                name,
                FunctionValueType::new(ty.clone(), true),
                nested.clone(),
                FunctionValueType::new(DataType::Int64, false),
                keys,
            );
            check(
                name,
                FunctionValueType::new(DataType::Int64, false),
                values,
                FunctionValueType::new(ty.clone(), true),
                nested,
            );
        }
    }
}
#[test]
fn pure_differential_by_empty_null_keys_null_winning_values_and_constants() {
    for name in ["max_by", "min_by"] {
        // Physical Null is an existing reader domain; only its genuine
        // nullable profile is admitted, with each logical role explicit.
        for null_value in [false, true] {
            let actual: ArrayRef = Arc::new(Int64Array::from(vec![1; ROWS]));
            let nulls = new_null_array(&DataType::Null, ROWS);
            let (values, keys) = if null_value {
                (nulls, actual)
            } else {
                (actual, nulls)
            };
            let summary = assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .column(values)
                    .column(keys)
                    .partitions(7, 1720),
            );
            assert_eq!(summary.matched_failures, 0);
            assert_eq!(summary.null_results, 2);
        }
        for ty in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Float64,
            DataType::Decimal256(60, 2),
        ] {
            for nullable in [false, true] {
                let summary = assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .typed_column(
                            FunctionValueType::new(ty.clone(), nullable),
                            new_empty_array(&ty),
                        )
                        .typed_column(
                            FunctionValueType::new(DataType::Int64, false),
                            new_empty_array(&DataType::Int64),
                        )
                        .partitions(13, 1721),
                );
                assert_eq!(summary.matched_failures, 0);
                assert!(summary.result_type.nullable);
                assert_eq!(summary.null_results, 2);
            }
            for null_key in [false, true] {
                let keys: ArrayRef = if null_key {
                    new_null_array(&DataType::Int64, ROWS)
                } else {
                    Arc::new(Int64Array::from(
                        (0..ROWS).map(|row| row as i64).collect::<Vec<_>>(),
                    ))
                };
                let summary = assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .column(new_null_array(&ty, ROWS))
                        .column(keys)
                        .partitions(7, 1722),
                );
                assert_eq!(summary.matched_failures, 0);
                assert_eq!(summary.null_results, 2);
            }
        }
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .constant(super::constant(
                    FunctionValueType::new(DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![Some(7)])),
                ))
                .constant(super::constant(
                    FunctionValueType::new(DataType::Int64, false),
                    Arc::new(Int64Array::from(vec![Some(1)])),
                ))
                .constant_rows(ROWS)
                .grouped((0..ROWS).map(|row| row % 6).collect(), 8)
                .partitions(7, 1723),
        );
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(summary.null_results, 4);
    }
}
#[test]
fn pure_differential_by_nan_comparison_is_the_original_operational_failure() {
    for name in ["max_by", "min_by"] {
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .column(Arc::new(StringArray::from(vec!["first", "later"])))
                .column(Arc::new(Float64Array::from(vec![
                    f64::from_bits(0x7ff8_0000_0000_0041),
                    1.0,
                ])))
                .partitions(1, 1724),
        );
        assert!(summary.matched_failures > 0);
    }
}
