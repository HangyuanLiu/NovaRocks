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

//! S1 calendar owner differential evidence against the original v1 algorithms.

use super::generate::{InputGenerator, InputProfile, TextProfile};
use super::{ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    ArrayRef, Date32Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDate;
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

fn temporal_sources(seed: u64) -> Vec<ArrayRef> {
    [
        (DataType::Date32, InputProfile::default()),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            InputProfile::default(),
        ),
        (
            DataType::Utf8,
            InputProfile::default().with_text(TextProfile::DateTimeText),
        ),
        (
            DataType::Utf8,
            InputProfile::default().with_text(TextProfile::DateText),
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (ty, profile))| {
        InputGenerator::new(seed + index as u64).column(
            &FunctionValueType::new(ty, true),
            256,
            &profile,
        )
    })
    .collect()
}

#[test]
fn pure_differential_s1_date_trunc_every_unit_and_overload() {
    for unit in [
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "WeEk",
        "QUARTER",
    ] {
        for source in temporal_sources(0xCA11) {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("date_trunc")
                    .constant_array(Arc::new(StringArray::from(vec![unit])))
                    .column(source),
            );
        }
    }
}

#[test]
fn pure_differential_s1_date_trunc_required_unit_errors_include_null_and_invalid_dates() {
    let units: ArrayRef = Arc::new(StringArray::from(vec![
        Some("invalid"),
        Some("YEAR"),
        None,
        Some(" day "),
        Some("week"),
        Some("µSECOND"),
        Some("month"),
        Some("minute"),
    ]));
    let dates: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some("invalid"),
        Some("2024-02-29"),
        Some("2024-02-29"),
        Some("0000-01-01"),
        Some("invalid"),
        Some("9999-12-31 23:59:59.999999"),
        None,
    ]));
    let summary = assert_scalar_matches_v1(
        ScalarDiffSpec::new("date_trunc")
            .column(units)
            .column(dates),
    );
    assert!(summary.legacy_batch_errors > 0);
    assert!(summary.attributed_row_errors >= 3);
}

#[test]
fn pure_differential_s1_date_trunc_preserves_date_width_and_subsecond_boundaries() {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let date_days: Vec<_> = [
        (0, 1, 1),
        (1, 1, 1),
        (1900, 2, 28),
        (2000, 2, 29),
        (2024, 2, 29),
        (9999, 12, 31),
    ]
    .into_iter()
    .map(|(y, m, d)| Some((NaiveDate::from_ymd_opt(y, m, d).unwrap() - epoch).num_days() as i32))
    .chain([None, Some(i32::MIN), Some(i32::MAX)])
    .collect();
    for unit in ["week", "month", "quarter", "year", "hour"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("date_trunc")
                .constant_array(Arc::new(StringArray::from(vec![unit])))
                .column(Arc::new(Date32Array::from(date_days.clone()))),
        );
    }
    for unit in [
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("date_trunc")
                .constant_array(Arc::new(StringArray::from(vec![unit])))
                .column(Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(999),
                    Some(1_000),
                    Some(999_999),
                    Some(1_000_001),
                    None,
                    Some(i64::MIN),
                    Some(i64::MAX),
                ]))),
        );
    }
}

#[test]
fn pure_differential_s1_duration_differences_every_overload() {
    for name in ["weeks_diff", "hours_diff", "minutes_diff", "seconds_diff"] {
        let left = temporal_sources(0xD1FF);
        let right = temporal_sources(0xD2FF);
        for (left, right) in left.into_iter().zip(right) {
            assert_scalar_matches_v1(ScalarDiffSpec::new(name).column(left).column(right));
        }
    }
}

#[test]
fn pure_differential_s1_duration_differences_truncate_negative_fractional_intervals() {
    for name in ["weeks_diff", "hours_diff", "minutes_diff", "seconds_diff"] {
        let left: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            Some(1),
            Some(-3_599_999_999),
            Some(3_599_999_999),
            Some(-86_399_999_999),
            Some(86_399_999_999),
            Some(-604_799_999_999),
            Some(604_799_999_999),
            None,
        ]));
        let right: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(0); 9]));
        assert_scalar_matches_v1(ScalarDiffSpec::new(name).column(left).column(right));
    }
}

#[test]
fn pure_differential_s1_timestamp_every_declared_overload() {
    for source in temporal_sources(0x7157).into_iter().skip(1) {
        assert_scalar_matches_v1(ScalarDiffSpec::new("timestamp").column(source));
    }
}

#[test]
fn pure_differential_s1_date_format_every_overload_and_original_mysql_tokens() {
    for format in [
        "%Y-%m-%d %H:%i:%s.%f",
        "%c/%e/%y %h %I %S",
        "%T",
        "%a %b %j %w",
        "%%f %%T",
        "trailing%",
        "中文🙂 %Y",
        "",
    ] {
        for source in temporal_sources(0xF04A) {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("date_format")
                    .column(source)
                    .constant_array(Arc::new(StringArray::from(vec![format]))),
            );
        }
    }
}

#[test]
fn pure_differential_s1_date_format_preserves_128_byte_limit_and_null_parsing() {
    let formats = [
        "x".repeat(127),
        "x".repeat(128),
        "x".repeat(129),
        "🙂".repeat(32),
        "🙂".repeat(33),
        "%Y".repeat(33),
        "%Y".into(),
        "%f".into(),
    ];
    let formats: ArrayRef = Arc::new(StringArray::from(formats.to_vec()));
    let dates: ArrayRef = Arc::new(StringArray::from(vec![
        Some("2024-02-29 12:34:56.123456"),
        Some("2024-02-29"),
        Some("2024-02-29"),
        Some("2024-02-29"),
        Some("2024-02-29"),
        Some("2024-02-29"),
        None,
        Some("invalid"),
    ]));
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("date_format")
            .column(dates)
            .column(formats),
    );
}

#[test]
fn pure_differential_s1_last_day_every_arity_and_source_overload() {
    for source in temporal_sources(0x1A57) {
        assert_scalar_matches_v1(ScalarDiffSpec::new("last_day").column(source.clone()));
        for token in ["month", "QUARTER", "year", "invalid"] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("last_day")
                    .column(source.clone())
                    .constant_array(Arc::new(StringArray::from(vec![token]))),
            );
        }
    }
}

#[test]
fn pure_differential_s1_last_day_invalid_date_suppresses_invalid_unit() {
    let dates: ArrayRef = Arc::new(StringArray::from(vec![
        Some("invalid"),
        None,
        Some("2024-02-29"),
        Some("2023-12-31"),
        Some("0000-01-01"),
        Some("9999-12-31"),
    ]));
    let units: ArrayRef = Arc::new(StringArray::from(vec![
        Some("invalid"),
        Some("invalid"),
        Some("bad"),
        Some("quarter"),
        Some("YEAR"),
        None,
    ]));
    let summary =
        assert_scalar_matches_v1(ScalarDiffSpec::new("last_day").column(dates).column(units));
    assert!(summary.attributed_row_errors > 0);
}

#[test]
fn pure_differential_s1_next_and_previous_day_every_source_and_weekday_token() {
    for name in ["next_day", "previous_day"] {
        for source in temporal_sources(0xDA7) {
            for token in [
                "Mo",
                "Tue",
                "Wednesday",
                "Th",
                "Friday",
                "Sa",
                "Sunday",
                "invalid",
                "monday",
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .column(source.clone())
                        .constant_array(Arc::new(StringArray::from(vec![token]))),
                );
            }
        }
    }
}

#[test]
fn pure_differential_s1_named_day_invalid_nonnull_date_preserves_required_token_error() {
    for name in ["next_day", "previous_day"] {
        let dates: ArrayRef = Arc::new(StringArray::from(vec![
            Some("invalid"),
            None,
            Some("2024-02-29"),
            Some("2024-02-29"),
            Some("0000-01-01"),
            Some("9999-12-31"),
        ]));
        let tokens: ArrayRef = Arc::new(StringArray::from(vec![
            Some("bad"),
            Some("bad"),
            Some("bad"),
            None,
            Some("Sunday"),
            Some("Monday"),
        ]));
        let summary =
            assert_scalar_matches_v1(ScalarDiffSpec::new(name).column(dates).column(tokens));
        assert!(summary.attributed_row_errors >= 2);
    }
}

#[test]
fn pure_differential_s1_str_to_date_original_patterns_fallback_and_sunday_weekyear() {
    let inputs: ArrayRef = Arc::new(StringArray::from(vec![
        Some("2024-02-29"),
        Some("2024-02-29 12:34:56.123456"),
        Some("20240229"),
        Some("2024-02-29"),
        Some("2023-02-29"),
        Some("202401 Sunday"),
        Some("202401 Monday"),
        Some("202453 Tuesday"),
        Some("000001 Sunday"),
        Some("999953 Saturday"),
        Some("202400 Sunday"),
        Some("202401 Sunday extra"),
        None,
        Some("2024-02-29"),
    ]));
    let formats: ArrayRef = Arc::new(StringArray::from(vec![
        Some("%Y-%m-%d"),
        Some("%Y-%m-%d %H:%i:%s.%f"),
        Some("%Y%m%d"),
        Some("not a pattern"),
        Some("%Y-%m-%d"),
        Some("%X%V %W"),
        Some("%X%V %W"),
        Some("%x%v %w"),
        Some("%X%V %W"),
        Some("%X%V %W"),
        Some("%X%V %W"),
        Some("%X%V %W"),
        Some("%Y-%m-%d"),
        None,
    ]));
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("str_to_date")
            .column(inputs)
            .column(formats),
    );
    for text in temporal_sources(0x57D).into_iter().skip(2) {
        for pattern in ["%Y-%m-%d", "%Y-%m-%d %T", "%Y-%m-%d %H:%i:%s.%f", "%Q", ""] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("str_to_date")
                    .column(text.clone())
                    .constant_array(Arc::new(StringArray::from(vec![pattern]))),
            );
        }
    }
}

#[test]
fn pure_differential_s1_from_days_preserves_sentinel_and_integer_boundaries() {
    let wide: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(i64::MIN),
        Some(i32::MIN as i64 - 1),
        Some(i32::MIN as i64),
        Some(-1),
        Some(0),
        Some(1),
        Some(3_652_424),
        Some(3_652_425),
        Some(i32::MAX as i64),
        Some(i32::MAX as i64 + 1),
        Some(i64::MAX),
        None,
    ]));
    let narrow: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(i32::MIN),
        Some(-1),
        Some(0),
        Some(1),
        Some(3_652_424),
        Some(3_652_425),
        Some(i32::MAX),
        None,
    ]));
    for array in [wide, narrow] {
        assert_scalar_matches_v1(ScalarDiffSpec::new("from_days").column(array));
    }
    for ty in [DataType::Int32, DataType::Int64] {
        let array = InputGenerator::new(0xFDD).column(
            &FunctionValueType::new(ty, true),
            256,
            &InputProfile::default(),
        );
        assert_scalar_matches_v1(ScalarDiffSpec::new("from_days").column(array));
    }
}

#[test]
fn pure_differential_s1_all_declared_profiles_with_nonnull_columns_and_broadcast_constants() {
    fn check(name: &str, arguments: Vec<ArrayRef>) {
        for constants in [false, true] {
            let mut spec = ScalarDiffSpec::new(name).constant_rows(8);
            for argument in &arguments {
                spec = if constants {
                    spec.constant_array(argument.slice(0, 1))
                } else {
                    spec.typed_column(
                        FunctionValueType::new(argument.data_type().clone(), false),
                        Arc::clone(argument),
                    )
                };
            }
            assert_scalar_matches_v1(spec);
        }
    }
    let temporal: Vec<ArrayRef> = vec![
        Arc::new(Date32Array::from(vec![0; 8])),
        Arc::new(TimestampMicrosecondArray::from(vec![0; 8])),
        Arc::new(StringArray::from(vec!["2024-02-29 12:34:56.123456"; 8])),
    ];
    for source in &temporal {
        check(
            "date_trunc",
            vec![
                Arc::new(StringArray::from(vec!["month"; 8])),
                Arc::clone(source),
            ],
        );
        for name in ["weeks_diff", "hours_diff", "minutes_diff", "seconds_diff"] {
            check(name, vec![Arc::clone(source), Arc::clone(source)]);
        }
        check(
            "date_format",
            vec![
                Arc::clone(source),
                Arc::new(StringArray::from(vec!["%Y-%m-%d %T.%f"; 8])),
            ],
        );
        check("last_day", vec![Arc::clone(source)]);
        check(
            "last_day",
            vec![
                Arc::clone(source),
                Arc::new(StringArray::from(vec!["quarter"; 8])),
            ],
        );
        for name in ["next_day", "previous_day"] {
            check(
                name,
                vec![
                    Arc::clone(source),
                    Arc::new(StringArray::from(vec!["Monday"; 8])),
                ],
            );
        }
    }
    for source in temporal.into_iter().skip(1) {
        check("timestamp", vec![source]);
    }
    check(
        "str_to_date",
        vec![
            Arc::new(StringArray::from(vec!["2024-02-29 12:34:56"; 8])),
            Arc::new(StringArray::from(vec!["%Y-%m-%d %T"; 8])),
        ],
    );
    check(
        "from_days",
        vec![Arc::new(Int32Array::from(vec![719528; 8]))],
    );
    check(
        "from_days",
        vec![Arc::new(Int64Array::from(vec![719528; 8]))],
    );
}
