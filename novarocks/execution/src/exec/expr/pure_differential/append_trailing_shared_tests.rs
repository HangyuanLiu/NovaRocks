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
//! Complete declared Utf8 x Utf8 profile, with each source view exercised by the real harness.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{ArrayRef, StringArray, new_empty_array, new_null_array},
    datatypes::DataType,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
#[test]
fn pure_differential_append_trailing_complete_byte_unicode_null_slice_profiles() {
    let a = strings(vec![
        Some("guard"),
        Some(""),
        Some("a"),
        Some("ac"),
        Some("é中"),
        Some("👩\u{200d}💻"),
        None,
        Some("a"),
        Some("a"),
        Some("a\0"),
        Some("guard"),
    ]);
    let b = strings(vec![
        Some("!"),
        Some("c"),
        Some("c"),
        Some("c"),
        Some("."),
        Some("é"),
        Some("c"),
        None,
        Some("xy"),
        Some("\0"),
        Some("!"),
    ]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for (a, b) in [
            (a.clone(), b.clone()),
            (a.slice(1, 9), b.slice(1, 9)),
            (
                new_null_array(&DataType::Utf8, 4),
                strings(vec![Some("c"); 4]),
            ),
            (
                strings(vec![Some("é"); 4]),
                new_null_array(&DataType::Utf8, 4),
            ),
            (
                new_empty_array(&DataType::Utf8),
                new_empty_array(&DataType::Utf8),
            ),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("append_trailing_char_if_absent")
                    .column(a)
                    .column(b)
                    .decimal_overflow(policy)
                    .expect_result_type(FunctionValueType::new(DataType::Utf8, true))
                    .sparse_selections(7, 887),
            );
        }
    }
}
#[test]
fn pure_differential_append_trailing_complete_constants_ascii_and_long_profiles() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for a in [
            None,
            Some(""),
            Some("a"),
            Some("ac"),
            Some("é中"),
            Some("x\0"),
        ] {
            for b in [None, Some(""), Some("c"), Some("\0"), Some("é"), Some("xy")] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("append_trailing_char_if_absent")
                        .constant_array(strings(vec![a]))
                        .constant_array(strings(vec![b]))
                        .legacy_constants(form)
                        .sparse_selections(7, 993),
                );
            }
        }
        for suffix in [None, Some("."), Some("é")] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("append_trailing_char_if_absent")
                    .column(strings(vec![Some(""), Some("é"), None, Some("中.")]))
                    .constant_array(strings(vec![suffix]))
                    .legacy_constants(form)
                    .sparse_selections(7, 992),
            );
        }
        for text in [None, Some(""), Some("é")] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("append_trailing_char_if_absent")
                    .constant_array(strings(vec![text]))
                    .column(strings(vec![Some("."), Some("é"), None, Some("")]))
                    .legacy_constants(form)
                    .sparse_selections(7, 991),
            );
        }
    }
    let suffixes: Vec<String> = (0u8..=127)
        .map(|b| String::from_utf8(vec![b]).unwrap())
        .collect();
    let text = "é中\0".repeat(700);
    let a = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        text.as_str(),
        suffixes.len(),
    ))) as ArrayRef;
    let b = Arc::new(StringArray::from_iter_values(
        suffixes.iter().map(String::as_str),
    )) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("append_trailing_char_if_absent")
            .column(a)
            .column(b)
            .sparse_selections(7, 994),
    );
}
