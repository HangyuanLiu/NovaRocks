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
//! Permanent exact-profile comparisons with the actual original v1 dispatcher.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{ArrayRef, StringArray, new_empty_array, new_null_array},
    datatypes::DataType,
};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
#[test]
fn pure_differential_measure_all_three_declared_profiles_unicode_null_slices_empty() {
    let all = strings(vec![
        Some("guard"),
        Some(""),
        Some("A"),
        Some("é"),
        Some("中"),
        Some("👩\u{200d}💻"),
        Some("e\u{301}"),
        Some("\0x"),
        None,
        Some("\u{7f}\u{80}\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}"),
        Some("guard"),
    ]);
    for name in ["ascii", "length", "char_length"] {
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            for input in [
                all.clone(),
                all.slice(1, 9),
                new_null_array(&DataType::Utf8, 4),
                new_empty_array(&DataType::Utf8),
                strings(vec![Some("é"), Some(""), Some("a\0b")]),
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .column(input)
                        .decimal_overflow(policy)
                        .sparse_selections(7, 419),
                );
            }
        }
    }
}
#[test]
fn pure_differential_measure_all_profiles_literal_pool_and_long_utf8() {
    for name in ["ascii", "length", "char_length"] {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            for value in [
                Some(""),
                Some("é"),
                Some("👩\u{200d}💻"),
                Some("a\0b"),
                None,
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .constant_array(strings(vec![value]))
                        .legacy_constants(form)
                        .sparse_selections(7, 102),
                );
            }
        }
    }
    let text = "é\0👩\u{200d}💻".repeat(300);
    for name in ["ascii", "length", "char_length"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(StringArray::from(vec![
                    Some(text.as_str()),
                    None,
                    Some(""),
                ])))
                .sparse_selections(7, 412),
        );
    }
}
