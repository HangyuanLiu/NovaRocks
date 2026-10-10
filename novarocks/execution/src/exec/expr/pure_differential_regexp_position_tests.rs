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
//! All three original regexp_position overloads compare full and selected results.
use super::*;
use arrow::array::{Int64Array, StringArray};
#[test]
fn pure_differential_regexp_position_three_overloads_unicode_null_masks_and_errors() {
    let long = format!("{}(", "a".repeat(900));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some("中a中a"),
            Some("👩‍💻x"),
            Some("a\0b"),
            Some("中a"),
            None,
            Some("a"),
            Some("中a"),
            Some("a"),
            Some("a"),
            Some(""),
        ])),
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("x"),
            Some("\0"),
            Some(""),
            Some("(masked"),
            None,
            Some("(invalid"),
            Some(long.as_str()),
            Some("(negative"),
            Some(""),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(1),
            Some(2),
            Some(1),
            Some(1),
            Some(4),
            Some(1),
            Some(0),
            Some(1),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(2),
            Some(1),
            Some(1),
            Some(2),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(0),
            Some(1),
        ])),
    ];
    for strict in [false, true] {
        for arity in 2..=4 {
            let mut spec = ScalarDiffSpec::new("regexp_position")
                .allow_throw_exception(strict)
                .sparse_selections(16, 1110 + arity as u64);
            for column in &columns[..arity] {
                spec = spec.column(column.clone());
            }
            assert_scalar_matches_v1(spec);
        }
    }
}
#[test]
fn pure_differential_regexp_position_literal_pool_null_constants_and_empty_batches() {
    for arity in 2..=4 {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            for rows in [0, 17] {
                for pattern in ["a", "", "(invalid"] {
                    let mut spec = ScalarDiffSpec::new("regexp_position")
                        .constant_array(Arc::new(StringArray::from(vec!["中a中a"])))
                        .constant_array(Arc::new(StringArray::from(vec![pattern])))
                        .constant_rows(rows)
                        .legacy_constants(form)
                        .sparse_selections(12, 1120);
                    for _ in 2..arity {
                        spec = spec.constant_array(Arc::new(Int64Array::from(vec![1])));
                    }
                    assert_scalar_matches_v1(spec);
                }
            }
            let mut spec = ScalarDiffSpec::new("regexp_position")
                .constant_array(Arc::new(StringArray::from(vec![None::<&str>])))
                .constant_array(Arc::new(StringArray::from(vec!["(masked"])))
                .constant_rows(7)
                .legacy_constants(form);
            for _ in 2..arity {
                spec = spec.constant_array(Arc::new(Int64Array::from(vec![1])));
            }
            assert_scalar_matches_v1(spec);
        }
    }
}
#[test]
fn pure_differential_regexp_position_many_recurrent_patterns_and_slices() {
    let sources = (0..300)
        .map(|i| format!("中k{}=v{i}", i % 100))
        .collect::<Vec<_>>();
    let patterns = (0..100).map(|i| format!("k{i}=")).collect::<Vec<_>>();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            sources.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            (0..300)
                .map(|i| patterns[i % 100].as_str())
                .collect::<Vec<_>>(),
        )),
    ];
    for (offset, len) in [(0, 300), (91, 107), (0, 0)] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("regexp_position")
                .column(arrays[0].slice(offset, len))
                .column(arrays[1].slice(offset, len))
                .sparse_selections(12, 1130),
        );
    }
}
