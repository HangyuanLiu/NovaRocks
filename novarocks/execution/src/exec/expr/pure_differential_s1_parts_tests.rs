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

//! Every declared calendar field profile, including formerly omitted LARGEINT.
use super::*;
use arrow::array::{
    ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray, builder::FixedSizeBinaryBuilder,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;

const PART_NAMES: [&str; 12] = [
    "year",
    "month",
    "day",
    "dayofmonth",
    "hour",
    "minute",
    "second",
    "dayofweek",
    "yearweek",
    "dayofyear",
    "weekofyear",
    "quarter",
];
fn large(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(v) => builder.append_value(v.to_be_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
fn profiles(nullable: bool) -> Vec<(FunctionValueType, ArrayRef)> {
    let stamps = [
        -1,
        -1_000_001,
        -1_000_000,
        0,
        1,
        1451651696000000,
        1709210096000000,
        i64::MIN,
        i64::MAX,
    ];
    let mut ts = stamps.into_iter().map(Some).collect::<Vec<_>>();
    let mut ds = vec![
        Some(0),
        Some(-1),
        Some(16801),
        Some(19782),
        Some(i32::MIN),
        Some(i32::MAX),
    ];
    let mut texts = vec![
        Some("20240101"),
        Some("2024-02-29"),
        Some("2016-01-01 12:34:56"),
        Some("2024-02-29 12:34:56.123456"),
        Some("invalid"),
        Some("0000-01-01"),
        Some("9999-12-31 23:59:59.999999"),
    ];
    let mut numbers = vec![
        Some(20160101123456),
        Some(20240229123456),
        Some(0),
        Some(-1),
        Some(20240230),
        Some(i128::MIN),
        Some(i128::MAX),
    ];
    if nullable {
        ts.push(None);
        ds.push(None);
        texts.push(None);
        numbers.push(None);
    }
    vec![
        (
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), nullable),
            Arc::new(TimestampMicrosecondArray::from(ts)),
        ),
        (
            FunctionValueType::new(DataType::Date32, nullable),
            Arc::new(Date32Array::from(ds)),
        ),
        (
            FunctionValueType::new(DataType::Utf8, nullable),
            Arc::new(StringArray::from(texts)),
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                nullable,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
            large(&numbers),
        ),
    ]
}
#[test]
fn pure_differential_s1_calendar_parts_all48_profiles_full_and_sparse_include_bug_boundaries() {
    for name in PART_NAMES {
        for nullable in [false, true] {
            for (ty, array) in profiles(nullable) {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(ty, array)
                        .sparse_selections(4, 0x5A11),
                );
            }
        }
    }
}
#[test]
fn pure_differential_s1_calendar_parts_all48_profiles_constants_and_sql_nulls() {
    for name in PART_NAMES {
        for (ty, array) in profiles(true) {
            for row in [0, array.len() - 1] {
                let one = array.slice(row, 1);
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .constant(constant(ty.clone(), one))
                        .constant_rows(9)
                        .sparse_selections(3, 0x5A12),
                );
            }
        }
    }
}
