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
//! Complete declared TO_BASE64 profile before/after owner installation.
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
fn pure_differential_to_base64_full_utf8_profile_null_empty_nonascii_long_slice_constants() {
    let long = "éÿĀ中\0".repeat(300);
    let values = strings(vec![
        Some("guard"),
        Some("ÿ"),
        Some("é"),
        Some("Ā"),
        Some("中"),
        Some("a\0b"),
        Some(&long),
        Some(""),
        None,
        Some("guard"),
    ]);
    for input in [
        values.clone(),
        values.slice(1, 8),
        new_null_array(&DataType::Utf8, 3),
        new_empty_array(&DataType::Utf8),
    ] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_base64")
                    .column(input.clone())
                    .decimal_overflow(policy)
                    .sparse_selections(11, 8128),
            );
        }
    }
    for nullable in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("to_base64")
                .typed_column(
                    FunctionValueType::new(DataType::Utf8, nullable),
                    strings(vec![Some("ÿ"), Some("Ā"), Some(""), Some(&long)]),
                )
                .sparse_selections(9, 877),
        );
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for value in [
            Some("ÿ"),
            Some("Ā"),
            Some("中"),
            Some(""),
            None,
            Some(&long),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_base64")
                    .constant_array(strings(vec![value]))
                    .legacy_constants(form)
                    .sparse_selections(9, 893),
            );
        }
    }
}
