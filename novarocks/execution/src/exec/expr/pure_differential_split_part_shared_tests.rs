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
//! Permanent full declared Utf8/Utf8/Int32 profile comparison with the actual original v1 dispatcher.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{ArrayRef, Int32Array, StringArray};
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn ints(v: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(v))
}
#[test]
fn split_part_full_declared_profile_unicode_null_slice_empty_literal_pool() {
    let text = strings(vec![
        Some("guard"),
        Some("a,,b,"),
        Some("aaaaa"),
        Some("é中é中x"),
        Some("e\u{301}x"),
        Some("\0é"),
        Some(""),
        None,
        Some("guard"),
    ]);
    let delimiters = strings(vec![
        Some("x"),
        Some(","),
        Some("aa"),
        Some("é中"),
        Some(""),
        Some(""),
        Some(""),
        None,
        Some("x"),
    ]);
    let indices = ints(vec![
        Some(99),
        Some(-4),
        Some(-3),
        Some(3),
        Some(-1),
        Some(1),
        Some(i32::MAX),
        Some(i32::MIN),
        Some(99),
    ]);
    for (s, d, n) in [
        (text.clone(), delimiters.clone(), indices.clone()),
        (
            text.slice(1, 7),
            delimiters.slice(1, 7),
            indices.slice(1, 7),
        ),
        (
            strings(vec![None; 3]),
            strings(vec![Some(""), None, Some(",")]),
            ints(vec![Some(i32::MIN), None, Some(i32::MAX)]),
        ),
        (strings(vec![]), strings(vec![]), ints(vec![])),
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("split_part")
                .column(s)
                .column(d)
                .column(n)
                .sparse_selections(5, 713),
        );
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for n in [
            Some(i32::MIN),
            Some(-2),
            Some(-1),
            Some(0),
            Some(1),
            Some(2),
            Some(i32::MAX),
            None,
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("split_part")
                    .column(text.clone())
                    .column(delimiters.clone())
                    .constant_array(ints(vec![n]))
                    .legacy_constants(form),
            );
            for d in [Some(""), Some(","), Some("aa"), None] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("split_part")
                        .constant_array(strings(vec![Some("a,,b,")]))
                        .constant_array(strings(vec![d]))
                        .constant_array(ints(vec![n]))
                        .legacy_constants(form)
                        .constant_rows(7),
                );
            }
        }
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("split_part")
                .constant_array(strings(vec![None]))
                .column(delimiters.clone())
                .column(indices.clone())
                .legacy_constants(form),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("split_part")
                .column(text.clone())
                .constant_array(strings(vec![Some("é")]))
                .column(indices.clone())
                .legacy_constants(form),
        );
    }
}
