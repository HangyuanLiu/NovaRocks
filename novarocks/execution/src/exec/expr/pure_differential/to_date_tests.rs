// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact date/to_date profiles against the original dispatcher, including its drift.
use super::{
    DifferentialFailure, ScalarDiffSpec, assert_scalar_matches_v1, run_scalar_differential,
};
use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn fixtures(name: &str, nullable: bool) -> Vec<ArrayRef> {
    let null = nullable.then_some(());
    let mut arrays = vec![
        Arc::new(StringArray::from(vec![
            Some("1970-01-01"),
            Some("19691231"),
            Some("2024-02-29T12:34:56.123456789"),
            Some(" 2024/02/29 12:34:56 "),
            Some("691231235959"),
            Some("700101000000"),
            Some("2023-02-29"),
            Some("2016-12-31 23:59:60"),
            Some("bad"),
            Some(""),
            null.map(|_| None).unwrap_or(Some("20240229")),
        ])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            Some(0),
            Some(1),
            Some(86400000000),
            Some(-86400000001),
            Some(i64::MIN),
            Some(i64::MAX),
            Some(0),
            Some(0),
            Some(0),
            null.map(|_| None).unwrap_or(Some(0)),
        ])) as ArrayRef,
    ];
    if name == "date" {
        arrays.push(Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(19782),
            Some(i32::MAX),
            Some(-719528),
            Some(0),
            Some(0),
            Some(0),
            null.map(|_| None).unwrap_or(Some(0)),
        ])));
    }
    arrays
}
#[test]
fn pure_differential_to_date_all_five_real_profiles_nullable_nonnull_and_constants() {
    for name in ["date", "to_date"] {
        for nullable in [false, true] {
            for source in fixtures(name, nullable) {
                let ty = FunctionValueType::new(source.data_type().clone(), nullable);
                let summary = assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(ty, source.clone())
                        .sparse_selections(8, 2007),
                );
                assert_eq!(summary.legacy_batch_errors, 0);
                assert_eq!(summary.attributed_row_errors, 0);
                for index in [0, source.len() - 1] {
                    assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .constant_rows(9)
                            .constant_array(source.slice(index, 1))
                            .sparse_selections(8, 2009),
                    );
                }
            }
        }
    }
}
#[test]
fn pure_differential_to_date_does_not_coerce_declared_int64_drift_into_date_parser() {
    let failure = run_scalar_differential(
        &ScalarDiffSpec::new("to_date")
            .column(Arc::new(Int64Array::from(vec![Some(20240229), None]))),
    )
    .expect_err("the original Int64 reader drift is refused at prepare");
    let DifferentialFailure::Specialization(message) = failure else {
        panic!("expected named profile refusal, got {failure}");
    };
    assert!(
        message.contains("to_date declared Int64 profile has no legacy date reader"),
        "{message}"
    );
}
#[test]
fn pure_differential_to_date_real_long_text_parser_work_keeps_exact_values() {
    let text = format!("1970{}-01-01", " ".repeat(513));
    for name in ["date", "to_date"] {
        let values = Arc::new(StringArray::from(vec![
            Some(text.as_str()),
            Some("1970-01-01"),
            Some("bad"),
            None,
        ])) as ArrayRef;
        let summary = assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(values)
                .sparse_selections(8, 2017),
        );
        assert_eq!(summary.legacy_batch_errors, 0);
        assert_eq!(summary.attributed_row_errors, 0);
    }
}
