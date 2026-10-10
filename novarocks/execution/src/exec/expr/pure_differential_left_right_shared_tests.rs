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
//! Permanent complete declared Utf8/Int64 profile comparisons using the actual v1 dispatcher.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn ints(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn check(name: &str) {
    let source = strings(vec![
        Some("guard"),
        Some("aé中🙂"),
        Some(""),
        Some("e\u{301}x"),
        Some("\0é"),
        None,
        Some("abc"),
        Some("x"),
        Some("guard"),
    ]);
    let lengths = ints(vec![
        Some(99),
        Some(2),
        Some(i64::MAX),
        Some(2),
        Some(1),
        Some(3),
        Some(i64::MIN),
        None,
        Some(99),
    ]);
    for (s, n) in [
        (source.clone(), lengths.clone()),
        (source.slice(1, 7), lengths.slice(1, 7)),
        (
            strings(vec![None; 3]),
            ints(vec![Some(0), None, Some(i64::MAX)]),
        ),
        (strings(vec![]), ints(vec![])),
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(s)
                .column(n)
                .sparse_selections(5, 419),
        );
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for count in [Some(i64::MIN), Some(0), Some(2), Some(i64::MAX), None] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .column(source.clone())
                    .constant_array(ints(vec![count]))
                    .legacy_constants(form),
            );
            for s in [Some("é中🙂"), Some(""), None] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .constant_array(strings(vec![s]))
                        .constant_array(ints(vec![count]))
                        .legacy_constants(form)
                        .constant_rows(7),
                );
            }
        }
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .constant_array(strings(vec![Some("e\u{301}中")]))
                .column(lengths.clone())
                .legacy_constants(form),
        );
    }
}
#[test]
fn left_full_declared_profile_matches_original() {
    check("left")
}
#[test]
fn strleft_full_declared_profile_matches_original() {
    check("strleft")
}
#[test]
fn right_full_declared_profile_matches_original() {
    check("right")
}
#[test]
fn strright_full_declared_profile_matches_original() {
    check("strright")
}
