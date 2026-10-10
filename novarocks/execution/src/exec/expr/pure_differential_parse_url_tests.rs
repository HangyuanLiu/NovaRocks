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
//! Both complete declared PARSE_URL profiles compare to the original dispatcher.
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
fn profile(arity: usize) {
    let long = format!(
        "https://Example.COM/{}?q=a+b&q=second&Q=UPPER&%E4%B8%AD=%FF#Frag",
        "é".repeat(700)
    );
    let urls = strings(vec![
        Some("guard"),
        Some(&long),
        Some("https://x/?"),
        Some("https://[::1]/a%00b"),
        None,
        Some("/relative"),
        Some("https://x/a"),
        Some("mailto:a@example.com"),
        Some(""),
        Some("http://%zz"),
        Some("guard"),
    ]);
    let parts = strings(vec![
        Some("guard"),
        Some("query"),
        Some("QUERY"),
        Some("HOST"),
        Some("PATH"),
        Some("HOST"),
        None,
        Some("PROTOCOL"),
        Some("REF"),
        Some("未知"),
        Some("guard"),
    ]);
    let keys = strings(vec![
        Some("guard"),
        Some("q"),
        Some("absent"),
        None,
        None,
        Some("x"),
        None,
        Some("x"),
        Some("x"),
        Some("x"),
        Some("guard"),
    ]);
    for arrays in [
        vec![urls.clone(), parts.clone(), keys.clone()],
        vec![urls.slice(1, 9), parts.slice(1, 9), keys.slice(1, 9)],
        vec![new_null_array(&DataType::Utf8, 4); 3],
        vec![new_empty_array(&DataType::Utf8); 3],
    ] {
        for policy in [
            DecimalOverflowPolicy::ReportError,
            DecimalOverflowPolicy::OutputNull,
        ] {
            let mut spec = ScalarDiffSpec::new("parse_url")
                .decimal_overflow(policy)
                .sparse_selections(9, 726);
            for a in arrays.iter().take(arity) {
                spec = spec.column(a.clone());
            }
            assert_scalar_matches_v1(spec);
        }
    }
    // All logical nullability combinations admit the whole input domain, including successful NULL.
    for mask in 0..(1 << arity) {
        let arrays = [
            strings(vec![
                Some("https://x/?q=a+b"),
                Some("invalid"),
                Some("https://x/"),
            ]),
            strings(vec![Some("QUERY"), Some("HOST"), Some("unknown")]),
            strings(vec![Some("q"), Some("x"), Some("x")]),
        ];
        let mut spec = ScalarDiffSpec::new("parse_url").sparse_selections(5, 811);
        for (i, a) in arrays.into_iter().take(arity).enumerate() {
            spec = spec.typed_column(
                FunctionValueType::new(DataType::Utf8, mask & (1 << i) != 0),
                a,
            );
        }
        assert_scalar_matches_v1(spec);
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for part in [
            Some("HOST"),
            Some("PATH"),
            Some("PROTOCOL"),
            Some("REF"),
            Some("QUERY"),
            Some("unknown"),
            None,
        ] {
            for key in [Some("q"), Some("Q"), Some("中"), Some(""), None] {
                let mut spec = ScalarDiffSpec::new("parse_url")
                    .constant_array(strings(vec![Some(&long)]))
                    .constant_array(strings(vec![part]))
                    .legacy_constants(form)
                    .sparse_selections(7, 841);
                if arity == 3 {
                    spec = spec.constant_array(strings(vec![key]));
                }
                assert_scalar_matches_v1(spec);
            }
        }
    }
    // Every standard part also appears as a dynamic vector and each key can be NULL outside QUERY.
    let url = strings(vec![
        Some(
            "https://EXAMPLE.com/a?q=a+b&q=second&Q=UPPER&%E4%B8%AD=%FF#Frag"
        );
        8
    ]);
    let p = strings(vec![
        Some("HOST"),
        Some("PATH"),
        Some("PROTOCOL"),
        Some("REF"),
        Some("QUERY"),
        Some("QUERY"),
        Some("QUERY"),
        Some("QUERY"),
    ]);
    let k = strings(vec![
        None,
        None,
        None,
        None,
        Some("q"),
        Some("Q"),
        Some("中"),
        None,
    ]);
    let mut spec = ScalarDiffSpec::new("parse_url")
        .column(url)
        .column(p)
        .sparse_selections(9, 948);
    if arity == 3 {
        spec = spec.column(k);
    }
    assert_scalar_matches_v1(spec);
}
#[test]
fn pure_differential_parse_url_full_two_argument_overload() {
    profile(2);
}
#[test]
fn pure_differential_parse_url_full_three_argument_overload() {
    profile(3);
}
