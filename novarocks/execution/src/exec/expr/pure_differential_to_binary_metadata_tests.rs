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
//! Both actual declared physical TO_BINARY overloads with source and selection matrices.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{ArrayRef, StringArray, new_empty_array, new_null_array},
    datatypes::DataType,
};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
#[test]
fn pure_differential_to_binary_one_argument_complete_declared_profile() {
    let long = "00ffAb".repeat(600);
    let v = strings(vec![
        Some("guard"),
        Some("00fFaB"),
        Some(""),
        Some("F"),
        Some("xx"),
        None,
        Some("世界"),
        Some(&long),
        Some("guard"),
    ]);
    for array in [
        v.clone(),
        v.slice(1, 7),
        new_null_array(&DataType::Utf8, 4),
        new_empty_array(&DataType::Utf8),
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("to_binary")
                .column(array)
                .sparse_selections(11, 4901),
        );
    }
    for nullable in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("to_binary")
                .typed_column(
                    FunctionValueType::new(DataType::Utf8, nullable),
                    strings(vec![Some("ff"), Some(""), Some("bad")]),
                )
                .sparse_selections(9, 4902),
        );
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for value in [Some("ff"), Some(""), Some("bad"), None, Some(&long)] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_binary")
                    .constant_array(strings(vec![value]))
                    .legacy_constants(form)
                    .sparse_selections(9, 4903),
            );
        }
    }
}
#[test]
fn pure_differential_to_binary_two_arguments_complete_declared_profile() {
    let hex = "00ff".repeat(600);
    let b64 = "YWJj".repeat(600);
    let utf8 = "ÿ中\0".repeat(600);
    let a = strings(vec![
        Some(&hex),
        Some(&b64),
        Some(&utf8),
        Some("00"),
        Some(""),
        Some(""),
        Some(""),
        None,
        Some("00"),
        Some("!"),
    ]);
    let f = strings(vec![
        None,
        Some("EnCoDe64"),
        Some("UTF8"),
        Some("unknown"),
        Some("hex"),
        Some("utf8"),
        Some("encode64"),
        Some("utf8"),
        Some(" utf8 "),
        Some("encode64"),
    ]);
    for (a, f) in [
        (a.clone(), f.clone()),
        (a.slice(1, 8), f.slice(1, 8)),
        (
            new_null_array(&DataType::Utf8, 3),
            new_null_array(&DataType::Utf8, 3),
        ),
        (
            new_empty_array(&DataType::Utf8),
            new_empty_array(&DataType::Utf8),
        ),
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("to_binary")
                .column(a)
                .column(f)
                .sparse_selections(13, 4911),
        );
    }
    for input_nullable in [false, true] {
        for format_nullable in [false, true] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_binary")
                    .typed_column(
                        FunctionValueType::new(DataType::Utf8, input_nullable),
                        strings(vec![Some("ff"), Some("00"), Some("")]),
                    )
                    .typed_column(
                        FunctionValueType::new(DataType::Utf8, format_nullable),
                        strings(vec![Some("hex"), Some("unknown"), Some("utf8")]),
                    )
                    .sparse_selections(9, 4912),
            );
        }
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for fmt in [
            None,
            Some("hex"),
            Some("unknown"),
            Some("utf8"),
            Some("encode64"),
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_binary")
                    .column(a.clone())
                    .constant_array(strings(vec![fmt]))
                    .legacy_constants(form)
                    .sparse_selections(9, 4913),
            );
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_binary")
                    .constant_array(strings(vec![Some("ff")]))
                    .column(f.clone())
                    .legacy_constants(form)
                    .sparse_selections(9, 4914),
            );
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_binary")
                    .constant_array(strings(vec![None]))
                    .constant_array(strings(vec![fmt]))
                    .legacy_constants(form)
                    .sparse_selections(9, 4915),
            );
        }
    }
}
