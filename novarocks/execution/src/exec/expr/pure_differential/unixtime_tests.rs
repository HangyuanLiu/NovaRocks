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

//! Only original declared carriers with explicit matching original timezone facts.
use super::*;
use arrow::array::{Int64Array, StringArray};
use std::sync::Arc;
#[test]
fn pure_differential_from_unixtime_int64_actual_profile_frozen_named_and_fixed_zones() {
    for zone in [
        "UTC",
        "+10:00",
        "-05:00",
        "Asia/Shanghai",
        "America/New_York",
    ] {
        assert_scalar_matches_v1(ScalarDiffSpec::new("from_unixtime").time_zone(zone).column(
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(1),
                Some(1710052200),
                Some(1710055800),
                Some(253402243199),
                Some(-1),
                Some(253402243200),
                None,
            ])),
        ));
    }
}
#[test]
fn pure_differential_from_unixtime_utf8_actual_profile_exact_valid_null_and_original_cap_formats() {
    let f128 = ":".repeat(128);
    let f129 = ":".repeat(129);
    let expand = "%Y".repeat(64);
    for fmt in [
        Some("%Y-%m-%d %H:%i:%s"),
        Some("yyyy-MM-dd"),
        Some("yyyyMMdd"),
        Some(""),
        Some("bad"),
        Some(f128.as_str()),
        Some(f129.as_str()),
        Some(expand.as_str()),
        None,
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("from_unixtime")
                .time_zone("UTC")
                .column(Arc::new(StringArray::from(vec![
                    Some("0"),
                    Some(" 1.9 "),
                    Some("bad"),
                    Some("-1"),
                    Some("253402243199"),
                    None,
                ])))
                .constant_array(Arc::new(StringArray::from(vec![fmt]))),
        );
    }
}
#[test]
fn pure_differential_from_unixtime_numeric_utf8_constant_selected_origin_and_dst() {
    for zone in ["UTC", "America/New_York"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("from_unixtime")
                .time_zone(zone)
                .constant_array(Arc::new(StringArray::from(vec!["1710055800"])))
                .constant_array(Arc::new(StringArray::from(vec!["yyyy-MM-dd HH:mm:ss"])))
                .constant_rows(5),
        );
    }
}
