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

//! Every exact regexp extraction declaration compared to its unchanged v1 oracle.
use super::*;
use arrow::array::{Int32Array, StringArray};

fn captures(name: &str) {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new(name)
            .column(Arc::new(StringArray::from(vec![
                Some("a1b2"),
                Some("a1b2"),
                Some("a1b2"),
                Some("a1b2"),
                Some("\n中\""),
                Some("b ab"),
                Some("中a"),
                Some("nomatch"),
                None,
                Some("a"),
            ])))
            .column(Arc::new(StringArray::from(vec![
                Some("([0-9])"),
                Some("([0-9])"),
                Some("([0-9])"),
                Some("([0-9])"),
                Some("(?s)."),
                Some("(a)?b"),
                Some(""),
                Some("z+"),
                Some("(masked"),
                None,
            ])))
            .column(Arc::new(Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(2),
                Some(-1),
                Some(0),
                Some(1),
                Some(0),
                Some(i32::MAX),
                Some(0),
                Some(0),
            ])))
            .sparse_selections(12, 127),
    );
}
fn errors(name: &str) {
    let long = format!("{}(", "a".repeat(900));
    for strict in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    Some("c"),
                    Some("d"),
                ])))
                .column(Arc::new(StringArray::from(vec![
                    Some("(negative"),
                    Some("[null_masked"),
                    Some(long.as_str()),
                    Some("(null_index"),
                    Some("a"),
                    Some("(later"),
                    Some("[last"),
                ])))
                .column(Arc::new(Int32Array::from(vec![
                    Some(-1),
                    Some(0),
                    Some(0),
                    None,
                    Some(0),
                    Some(0),
                    Some(i32::MIN),
                ])))
                .allow_throw_exception(strict)
                .sparse_selections(12, 128),
        );
    }
}
fn memo_slice_and_constants(name: &str) {
    let source = (0..300)
        .map(|i| (i % 11 != 0).then(|| format!("k{}=v{i};k{}=w{i}", i % 100, i % 100)))
        .collect::<Vec<_>>();
    let pattern = (0..300)
        .map(|i| (i % 13 != 0).then(|| format!("k{}=([vw][0-9]+)", i % 100)))
        .collect::<Vec<_>>();
    let arrays = [
        Arc::new(StringArray::from(
            source.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            pattern.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Int32Array::from(
            (0..300)
                .map(|i| {
                    if i % 17 == 0 {
                        None
                    } else {
                        Some(if i % 19 == 0 { -1 } else { 1 })
                    }
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    ];
    for sliced in [false, true] {
        let arrays = if sliced {
            arrays.each_ref().map(|a| a.slice(91, 107))
        } else {
            arrays.clone()
        };
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(arrays[0].clone())
                .column(arrays[1].clone())
                .column(arrays[2].clone())
                .sparse_selections(12, 129),
        );
    }
    for rows in [0, 17] {
        for (pattern, index) in [("([0-9]+)", 1), ("(invalid", -1), ("(invalid", 0)] {
            let text = constant(
                FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["中12a3"])),
            );
            let pattern = constant(
                FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec![pattern])),
            );
            let index = constant(
                FunctionValueType::new(DataType::Int32, false),
                Arc::new(Int32Array::from(vec![index])),
            );
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .constant(text)
                    .constant(pattern)
                    .constant(index)
                    .constant_rows(rows)
                    .sparse_selections(9, 130),
            );
        }
    }
}
#[test]
fn pure_differential_regexp_extract_int32_utf8_capture_profile() {
    captures("regexp_extract");
}
#[test]
fn pure_differential_regexp_extract_int32_utf8_error_profile() {
    errors("regexp_extract");
}
#[test]
fn pure_differential_regexp_extract_int32_utf8_memo_slice_constant_profile() {
    memo_slice_and_constants("regexp_extract");
}
#[test]
fn pure_differential_regexp_extract_all_int32_utf8_json_capture_profile() {
    captures("regexp_extract_all");
}
#[test]
fn pure_differential_regexp_extract_all_int32_utf8_json_error_profile() {
    errors("regexp_extract_all");
}
#[test]
fn pure_differential_regexp_extract_all_int32_utf8_json_memo_slice_constant_profile() {
    memo_slice_and_constants("regexp_extract_all");
}
