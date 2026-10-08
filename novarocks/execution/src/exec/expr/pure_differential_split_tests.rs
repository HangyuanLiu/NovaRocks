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

//! The sole actual SPLIT overload compares to the original raw implementation.
use super::*;
use arrow::array::StringArray;
#[test]
fn split_differential_original_unicode_empty_fields_null_and_nonoverlap() {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("split")
            .column(Arc::new(StringArray::from(vec![
                Some("é中👩\u{200d}🔬a\0"),
                Some(""),
                Some("aaaaa"),
                Some(",a,,"),
                Some("中é中"),
                None,
                Some("a\0b\0"),
            ])))
            .column(Arc::new(StringArray::from(vec![
                Some(""),
                Some(","),
                Some("aa"),
                Some(","),
                Some("中"),
                Some(""),
                Some("\0"),
            ])))
            .sparse_selections(12, 181),
    );
}
#[test]
fn split_differential_sliced_constant_delimiter_and_long_search() {
    let texts = (0..37)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(format!("{}中x中", "aé".repeat(129 + i)))
            }
        })
        .collect::<Vec<_>>();
    let all = Arc::new(StringArray::from(texts)) as ArrayRef;
    for rows in [all.clone(), all.slice(3, 29), all.slice(0, 0)] {
        for delimiter in ["中", ""] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("split")
                    .column(rows.clone())
                    .constant_array(Arc::new(StringArray::from(vec![delimiter])))
                    .sparse_selections(12, 182),
            );
        }
    }
}
#[test]
fn split_differential_literal_and_pool_constants_empty_batches_and_typed_null() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for rows in [0, 9] {
            for (a, b) in [
                (Some("éé中"), Some("é")),
                (Some(""), Some("")),
                (None, Some("")),
                (Some("a"), None),
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("split")
                        .constant_array(Arc::new(StringArray::from(vec![a])))
                        .constant_array(Arc::new(StringArray::from(vec![b])))
                        .constant_rows(rows)
                        .legacy_constants(form),
                );
            }
        }
    }
}
