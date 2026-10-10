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

//! Permanent actual three/four-argument temporal profiles, including original errors.
use super::*;
use arrow::array::{ArrayRef, Date32Array, Int32Array, StringArray, TimestampMicrosecondArray};
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use std::sync::Arc;
fn day(s: &str) -> i32 {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap()
        .num_days_from_ce()
        - 719163
}
fn micros(s: &str) -> i64 {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}
fn source(date: bool) -> ArrayRef {
    if date {
        Arc::new(Date32Array::from(vec![
            Some(day("0001-01-01")),
            Some(day("2024-05-17")),
            Some(day("9999-12-31")),
            None,
            Some(i32::MIN),
            Some(day("0000-12-31")),
            Some(day("2024-05-01")),
        ]))
    } else {
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(micros("0001-01-01 00:00:00")),
            Some(micros("2024-05-17 15:16:17.123456")),
            Some(micros("9999-12-31 23:59:59.999999")),
            None,
            Some(i64::MAX),
            Some(micros("0000-12-31 23:59:59")),
            Some(micros("2024-05-01 00:00:00")),
        ]))
    }
}
fn profile(date: bool, boundary: bool) {
    let name = if date { "date_slice" } else { "time_slice" };
    for unit in [
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
        "MONTHS",
        "unknown",
    ] {
        let mut spec = ScalarDiffSpec::new(name)
            .column(source(date))
            .column(Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                Some(i32::MAX),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
            ])))
            .constant_array(Arc::new(StringArray::from(vec![unit])));
        if boundary {
            spec = spec.column(Arc::new(StringArray::from(vec![
                Some("floor"),
                Some("ceil"),
                Some("ceil"),
                Some("floor"),
                Some("floor"),
                Some("floor"),
                Some("ceil"),
            ])));
        }
        assert_scalar_matches_v1(spec);
    }
    // Control errors must remain observable even on NULL temporal rows.
    let mut spec = ScalarDiffSpec::new(name)
        .column(source(date))
        .column(Arc::new(Int32Array::from(vec![
            None,
            Some(0),
            Some(-1),
            None,
            Some(1),
            Some(1),
            Some(1),
        ])))
        .column(Arc::new(StringArray::from(vec![
            None,
            Some("day"),
            Some("day"),
            Some("day"),
            None,
            Some("day"),
            Some("DAY"),
        ])));
    if boundary {
        spec = spec.column(Arc::new(StringArray::from(vec![
            Some("floor"),
            Some("ceil"),
            Some("other"),
            None,
            Some("floor"),
            Some("floor"),
            Some("CeIl"),
        ])));
    }
    assert_scalar_matches_v1(spec);
    let mut spec = ScalarDiffSpec::new(name)
        .column(source(date))
        .constant_array(Arc::new(Int32Array::from(vec![1])))
        .constant_array(Arc::new(StringArray::from(vec!["month"])));
    if boundary {
        spec = spec.constant_array(Arc::new(StringArray::from(vec!["ceil"])));
    }
    assert_scalar_matches_v1(spec);
}
#[test]
fn pure_differential_date_slice_three_actual_profile_floor_null_error_and_constant() {
    profile(true, false);
}
#[test]
fn pure_differential_date_slice_four_actual_profile_boundary_null_error_and_constant() {
    profile(true, true);
}
#[test]
fn pure_differential_time_slice_three_actual_profile_floor_null_error_and_constant() {
    profile(false, false);
}
#[test]
fn pure_differential_time_slice_four_actual_profile_boundary_null_error_and_constant() {
    profile(false, true);
}
