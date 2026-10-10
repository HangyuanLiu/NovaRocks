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

//! Selected/full regexp_replace comparisons after shared-core extraction.
use super::*;
use arrow::array::StringArray;

#[test]
fn pure_differential_regexp_replace_text_captures_unicode_and_nulls() {
    let source = Arc::new(StringArray::from(vec![
        Some("abc123 xyz45"),
        Some("中a"),
        Some(""),
        Some("xxxx"),
        Some("abc"),
        None,
        Some("ab"),
        Some("ab"),
    ])) as ArrayRef;
    let patterns = Arc::new(StringArray::from(vec![
        Some("(?P<word>[a-z]+)([0-9]+)"),
        Some(""),
        Some(""),
        Some("xx"),
        Some("z+"),
        Some("(invalid"),
        None,
        Some("(invalid"),
    ])) as ArrayRef;
    let replacements = Arc::new(StringArray::from(vec![
        Some("${word}:$2:$$"),
        Some("-"),
        Some("-"),
        Some("+"),
        Some("X"),
        Some("-"),
        Some("-"),
        None,
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("regexp_replace")
            .column(source)
            .column(patterns)
            .column(replacements)
            .sparse_selections(9, 117),
    );
}
#[test]
fn pure_differential_regexp_replace_invalid_pattern_rows_and_full_error() {
    let long = format!("{}(", "a".repeat(900));
    for strict in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("regexp_replace")
                .column(Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                ])))
                .column(Arc::new(StringArray::from(vec![
                    Some("(short"),
                    Some("[masked"),
                    Some(long.as_str()),
                    Some("[second"),
                    Some("c"),
                    Some("(last"),
                ])))
                .column(Arc::new(StringArray::from(vec![
                    Some("-"),
                    Some("-"),
                    Some("-"),
                    Some("-"),
                    Some("-"),
                    None,
                ])))
                .allow_throw_exception(strict)
                .sparse_selections(12, 118),
        );
    }
}
#[test]
fn pure_differential_regexp_replace_many_distinct_and_sliced_carriers() {
    let rows = 300;
    let source = (0..rows)
        .map(|i| (i % 11 != 0).then(|| format!("k{}=v{i};k{}=w{i}", i % 100, i % 100)))
        .collect::<Vec<_>>();
    let pattern = (0..rows)
        .map(|i| (i % 13 != 0).then(|| format!("k{}=", i % 100)))
        .collect::<Vec<_>>();
    let arrays = [
        Arc::new(StringArray::from(
            source.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(
            pattern.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(StringArray::from(vec![Some("<"); rows])) as ArrayRef,
    ];
    for sliced in [false, true] {
        let arrays = if sliced {
            arrays.each_ref().map(|a| a.slice(91, 107))
        } else {
            arrays.clone()
        };
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("regexp_replace")
                .column(arrays[0].clone())
                .column(arrays[1].clone())
                .column(arrays[2].clone())
                .sparse_selections(12, 119),
        );
    }
}
#[test]
fn pure_differential_regexp_replace_broadcast_constants_and_empty_rows() {
    for rows in [0, 17] {
        for pattern in ["[0-9]+", "(invalid"] {
            let text = constant(
                FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["中12a3"])),
            );
            let pattern = constant(
                FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec![pattern])),
            );
            let replacement = constant(
                FunctionValueType::new(DataType::Utf8, false),
                Arc::new(StringArray::from(vec!["<$0>"])),
            );
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("regexp_replace")
                    .constant(text)
                    .constant(pattern)
                    .constant(replacement)
                    .constant_rows(rows)
                    .sparse_selections(9, 120),
            );
        }
    }
}
