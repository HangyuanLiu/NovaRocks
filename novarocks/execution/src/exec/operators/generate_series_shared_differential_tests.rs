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
//! Permanent raw-processor versus explicit sole-core projection comparisons.
use super::generate_series_original_baseline_tests::{columns, raw};
use super::*;
use novarocks_functions::generate_series_core as core;
fn shared(args: &[ArrayRef], target: &DataType, left: bool) -> Result<ArrayRef, String> {
    let rows = args[0].len();
    if rows == 0 {
        return Ok(new_empty_array(target));
    }
    let expanded = core::expand(&args[0], &args[1], args.get(2), rows, left)?;
    if expanded.total_rows == 0 {
        return Ok(new_empty_array(target));
    }
    core::result_column(expanded.values, std::slice::from_ref(target))
}
fn check(args: Vec<ArrayRef>, target: DataType, left: bool) {
    match (
        raw(args.clone(), target.clone(), left),
        shared(&args, &target, left),
    ) {
        (Ok(raw), Ok(shared)) => {
            assert_eq!(raw.data_type(), shared.data_type());
            assert_eq!(raw.to_data(), shared.to_data());
        }
        (Err(raw), Err(shared)) => assert_eq!(raw, shared),
        (a, b) => panic!("raw source and shared core differ: {a:?} / {b:?}"),
    }
}
#[test]
fn differential_generate_series_all_original_integer_argument_and_return_profiles() {
    for source in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::FixedSizeBinary(16),
    ] {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::FixedSizeBinary(16),
        ] {
            for left in [false, true] {
                let a = columns(&[Some(99), Some(1), None, Some(7)], &source).slice(1, 3);
                let b = columns(&[Some(99), Some(4), Some(4), Some(5)], &source).slice(1, 3);
                check(vec![a.clone(), b.clone()], target.clone(), left);
                check(
                    vec![a, b, columns(&[Some(1), Some(0), Some(1)], &source)],
                    target.clone(),
                    left,
                );
            }
        }
    }
}
#[test]
fn differential_generate_series_signed_extremes_errors_order_empty_and_default_plus_one() {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::FixedSizeBinary(16),
        DataType::Utf8,
        DataType::UInt64,
    ] {
        for (a, b, step) in [
            (i128::from(i64::MIN), i128::from(i64::MIN), -1),
            (i128::from(i64::MAX), i128::from(i64::MAX), 1),
            (5, 1, -2),
            (1, 2, 0),
            (0, i128::from(u32::MAX), 1),
        ] {
            check(
                vec![
                    columns(&[Some(a)], &DataType::Int64),
                    columns(&[Some(b)], &DataType::Int64),
                    columns(&[Some(step)], &DataType::Int64),
                ],
                target.clone(),
                false,
            );
        }
        check(
            vec![
                columns(&[], &DataType::Int64),
                columns(&[], &DataType::Int64),
            ],
            target.clone(),
            false,
        );
    }
    let ty = DataType::FixedSizeBinary(16);
    for (a, b, step) in [
        (i128::MAX, i128::MAX, 1),
        (i128::MIN, i128::MIN, -1),
        (0, i128::MAX - 1, 1),
        (i128::from(i64::MAX) + 1, i128::from(i64::MAX) + 1, 1),
    ] {
        check(
            vec![
                columns(&[Some(a)], &ty),
                columns(&[Some(b)], &ty),
                columns(&[Some(step)], &ty),
            ],
            DataType::Int64,
            false,
        );
    }
}
