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
//! Permanent full installed SM3 overload comparisons against the actual original dispatcher.
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
fn pure_differential_sm3_exact_profile_empty_null_slice_full_sparse_unicode_long() {
    let block = "abcd".repeat(16);
    let long = "é中\0".repeat(200);
    let input = strings(vec![
        Some("guard"),
        Some(""),
        Some("abc"),
        Some(&block),
        None,
        Some("é中"),
        Some("a\0b"),
        Some(&long),
        Some("guard"),
    ]);
    for values in [
        input.clone(),
        input.slice(1, 7),
        new_null_array(&DataType::Utf8, 4),
        new_empty_array(&DataType::Utf8),
    ] {
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("sm3")
                    .column(values.clone())
                    .decimal_overflow(policy)
                    .sparse_selections(7, 381),
            );
        }
    }
    for nullable in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("sm3")
                .typed_column(
                    FunctionValueType::new(DataType::Utf8, nullable),
                    strings(vec![Some(""), Some("abc"), Some(&block), Some(&long)]),
                )
                .sparse_selections(7, 398),
        );
    }
}
#[test]
fn pure_differential_sm3_literal_pool_constants_preserve_null_and_empty_full_overload() {
    let long = "é中\0".repeat(200);
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for value in [
            Some(""),
            Some("abc"),
            Some("é中"),
            Some("a\0b"),
            Some(&long),
            None,
        ] {
            for policy in [
                DecimalOverflowPolicy::ReportError,
                DecimalOverflowPolicy::OutputNull,
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("sm3")
                        .constant_array(strings(vec![value]))
                        .legacy_constants(form)
                        .decimal_overflow(policy)
                        .sparse_selections(7, 383),
                );
            }
        }
    }
}
