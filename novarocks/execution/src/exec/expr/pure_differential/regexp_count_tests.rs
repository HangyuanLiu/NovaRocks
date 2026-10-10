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

//! Permanent full-profile differential fixtures, first run before owner installation.
//! Current source must report MissingPureImplementation, never guessed source facts.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{ArrayRef, StringArray, new_empty_array, new_null_array};
use arrow::datatypes::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
#[test]
fn pure_differential_regexp_count_actual_utf8_profile_dynamic_slice_null_empty() {
    let source = strings(vec![
        Some("aaa"),
        Some("ababa"),
        Some("éé"),
        Some(""),
        None,
        Some("aaa"),
        Some("a\nb"),
        Some("a\0b"),
    ]);
    let pattern = strings(vec![
        Some("aa"),
        Some("aba"),
        Some("."),
        Some(""),
        Some("("),
        Some("a{,}"),
        Some("(?m)^"),
        Some("\0"),
    ]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for (s, p) in [
            (source.clone(), pattern.clone()),
            (source.slice(1, 6), pattern.slice(1, 6)),
            (new_null_array(&DataType::Utf8, 8), pattern.clone()),
            (
                new_empty_array(&DataType::Utf8),
                new_empty_array(&DataType::Utf8),
            ),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("regexp_count")
                    .column(s)
                    .column(p)
                    .decimal_overflow(policy)
                    .sparse_selections(7, 3127),
            );
        }
    }
}
#[test]
fn pure_differential_regexp_count_actual_literal_pattern_valid_invalid_null_mask_and_special_case()
{
    for p in [Some("a"), Some("(bad"), Some("a{,}"), Some(""), None] {
        for s in [
            strings(vec![Some("aaa"), None]),
            strings(vec![None, None]),
            new_empty_array(&DataType::Utf8),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("regexp_count")
                    .column(s)
                    .constant_array(strings(vec![p]))
                    .legacy_constants(LegacyConstantForm::Literal)
                    .sparse_selections(7, 121),
            );
        }
    }
}
#[test]
fn pure_differential_regexp_count_actual_pooled_pattern_is_not_literal_even_with_equal_bytes() {
    for p in [Some("a"), Some("(bad"), Some("a{,}"), Some(""), None] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("regexp_count")
                .column(strings(vec![Some("aaa"), None, Some(""), Some("中a")]))
                .constant_array(strings(vec![p]))
                .legacy_constants(LegacyConstantForm::Pool)
                .sparse_selections(7, 455),
        );
    }
}
