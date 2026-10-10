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
//! Permanent entire declared REVERSE Utf8 profile versus the original arena dispatcher.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{ArrayRef, StringArray, new_empty_array, new_null_array},
    datatypes::DataType,
};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
#[test]
fn pure_differential_reverse_complete_utf8_profile_unicode_null_slice_and_empty() {
    let a = strings(vec![
        Some("guard"),
        Some(""),
        Some("aé中"),
        Some("a\u{301}"),
        Some("👩\u{200d}💻"),
        Some("x\0y"),
        None,
        Some("\u{0}\u{7f}\u{80}\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}"),
        Some("guard"),
    ]);
    let generated: String = (0..2048)
        .filter_map(|i| char::from_u32((i * 7919) % 0x110000))
        .collect();
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        for a in [
            a.clone(),
            a.slice(1, 7),
            new_null_array(&DataType::Utf8, 4),
            new_empty_array(&DataType::Utf8),
            strings(vec![Some(generated.as_str()), None, Some("")]),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("reverse")
                    .column(a)
                    .decimal_overflow(policy)
                    .sparse_selections(7, 771),
            );
        }
    }
}
#[test]
fn pure_differential_reverse_complete_literal_pool_constant_and_long_profile() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for v in [
            None,
            Some(""),
            Some("aé中"),
            Some("👩\u{200d}💻"),
            Some("x\0y"),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("reverse")
                    .constant_array(strings(vec![v]))
                    .legacy_constants(form)
                    .sparse_selections(7, 112),
            );
        }
    }
    let a = "aé中\0".repeat(600);
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("reverse")
            .column(strings(vec![Some(a.as_str()), None, Some("")]))
            .sparse_selections(7, 771),
    );
}
