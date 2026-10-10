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
//! Permanent full declared-profile differential against the actual v1 dispatcher.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray, new_empty_array, new_null_array},
    datatypes::DataType,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn bits(v: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
#[test]
fn pure_differential_sha2_exact_profile_all_selectors_nullable_shapes_slice_long_empty() {
    let text = strings(vec![
        Some("guard"),
        Some(""),
        Some("abc"),
        Some("é中"),
        Some("a\0b"),
        None,
        Some("bad-selector"),
        Some("abc"),
        Some("abc"),
        Some("guard"),
    ]);
    let length = bits(vec![
        Some(999),
        Some(224),
        Some(0),
        Some(256),
        Some(384),
        Some(512),
        Some(-1),
        None,
        Some(i64::MIN),
        Some(999),
    ]);
    for (t, b) in [
        (text.clone(), length.clone()),
        (text.slice(1, 8), length.slice(1, 8)),
        (
            new_null_array(&DataType::Utf8, 4),
            new_null_array(&DataType::Int64, 4),
        ),
        (
            new_empty_array(&DataType::Utf8),
            new_empty_array(&DataType::Int64),
        ),
    ] {
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("sha2")
                    .column(t.clone())
                    .column(b.clone())
                    .decimal_overflow(policy)
                    .sparse_selections(7, 712),
            );
        }
    }
    let long = "é中\0".repeat(200);
    for n in 0..4 {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("sha2")
                .typed_column(
                    FunctionValueType::new(DataType::Utf8, n & 1 != 0),
                    strings(vec![Some(&long); 7]),
                )
                .typed_column(
                    FunctionValueType::new(DataType::Int64, n & 2 != 0),
                    bits(vec![
                        Some(224),
                        Some(0),
                        Some(256),
                        Some(384),
                        Some(512),
                        Some(-1),
                        Some(i64::MAX),
                    ]),
                )
                .sparse_selections(7, 711),
        );
    }
}
#[test]
fn pure_differential_sha2_constant_columns_and_both_literal_pool_forms_every_bit_shape() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for bit in [
            Some(224),
            Some(0),
            Some(256),
            Some(384),
            Some(512),
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
        ] {
            for value in [Some(""), Some("é中\0"), None] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("sha2")
                        .constant_array(strings(vec![value]))
                        .constant_array(bits(vec![bit]))
                        .legacy_constants(form)
                        .sparse_selections(7, 761),
                );
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("sha2")
                        .column(strings(vec![value, None, Some("abc")]))
                        .constant_array(bits(vec![bit]))
                        .legacy_constants(form)
                        .sparse_selections(7, 761),
                );
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("sha2")
                        .constant_array(strings(vec![value]))
                        .column(bits(vec![bit, None, Some(256)]))
                        .legacy_constants(form)
                        .sparse_selections(7, 761),
                );
            }
        }
    }
}
