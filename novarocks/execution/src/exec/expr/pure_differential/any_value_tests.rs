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

//! ANY_VALUE shared owner matrix; register only after its pure owner exists.
use super::FloatComparison;
use super::aggregate::{AggregateDiffSpec, assert_aggregate_matches_v1};
use super::generate::{InputGenerator, InputProfile};
use arrow::array::{ArrayRef, Int64Array, new_empty_array, new_null_array};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;
const ROWS: usize = 513;
fn check(source: FunctionValueType, values: ArrayRef) {
    let summary = assert_aggregate_matches_v1(
        AggregateDiffSpec::new("any_value")
            .typed_column(source.clone(), values)
            .grouped((0..ROWS).map(|row| row % 6).collect(), 8)
            .partitions(7, 1601)
            .float_comparison(FloatComparison::Exact),
    );
    assert_eq!(summary.matched_failures, 0);
    assert!(summary.result_type.nullable);
    assert!(summary.null_results >= 4);
    println!(
        "any_value {} [{}] input={source:?} output={:?} rows={} groups={} nulls={}",
        summary.function.as_str(),
        summary.overload.as_str(),
        summary.result_type,
        summary.rows,
        summary.groups,
        summary.null_results
    );
}
#[test]
fn pure_differential_any_value_all_existing_scalar_profiles_nullable_and_nonnullable() {
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
    for nullable in [false, true] {
        for ty in &types {
            let source = FunctionValueType::new(ty.clone(), nullable);
            let values = InputGenerator::new(1611).column(&source, ROWS, &InputProfile::default());
            check(source, values);
        }
        let source = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            nullable,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let values = InputGenerator::new(1612).column(&source, ROWS, &InputProfile::default());
        check(source, values);
        for (precisions, wide) in [([9, 18, 38], false), ([40, 60, 76], true)] {
            for precision in precisions {
                for scale in [-2, 0, 2, precision as i8] {
                    let source = FunctionValueType::new(
                        if wide {
                            DataType::Decimal256(precision, scale)
                        } else {
                            DataType::Decimal128(precision, scale)
                        },
                        nullable,
                    );
                    let values =
                        InputGenerator::new(1613).column(&source, ROWS, &InputProfile::default());
                    check(source, values);
                }
            }
        }
    }
}
#[test]
fn pure_differential_any_value_nested_lists_structs_maps_and_null_children() {
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
    for (ty, payload) in profiles {
        let values = build_scalar_array(
            &ty,
            (0..ROWS)
                .map(|row| if row % 7 == 0 { None } else { payload.clone() })
                .collect(),
        )
        .unwrap();
        check(FunctionValueType::new(ty.clone(), true), values);
        for rows in [0, ROWS] {
            let summary = assert_aggregate_matches_v1(
                AggregateDiffSpec::new("any_value")
                    .column(new_null_array(&ty, rows))
                    .partitions(7, 1621),
            );
            assert_eq!(summary.matched_failures, 0);
            assert_eq!(summary.null_results, 2);
        }
    }
}
#[test]
fn pure_differential_any_value_empty_null_and_literal_constant_partial_final() {
    for ty in [
        DataType::Int64,
        DataType::Utf8,
        DataType::Float64,
        DataType::Decimal256(60, 2),
    ] {
        for nullable in [false, true] {
            let summary = assert_aggregate_matches_v1(
                AggregateDiffSpec::new("any_value")
                    .typed_column(
                        FunctionValueType::new(ty.clone(), nullable),
                        new_empty_array(&ty),
                    )
                    .partitions(13, 1631),
            );
            assert_eq!(summary.matched_failures, 0);
            assert!(summary.result_type.nullable);
            assert_eq!(summary.null_results, 2);
        }
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new("any_value")
                .column(new_null_array(&ty, ROWS))
                .partitions(13, 1632),
        );
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(summary.null_results, 2);
    }
    for value in [Some(7_i64), None] {
        let constant = super::constant(
            FunctionValueType::new(DataType::Int64, value.is_none()),
            Arc::new(Int64Array::from(vec![value])),
        );
        let summary = assert_aggregate_matches_v1(
            AggregateDiffSpec::new("any_value")
                .constant(constant)
                .constant_rows(ROWS)
                .grouped((0..ROWS).map(|row| row % 6).collect(), 8)
                .partitions(7, 1633),
        );
        assert_eq!(summary.matched_failures, 0);
        assert!(summary.result_type.nullable);
    }
}
