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
//! Unicode case aliases all compare against the unchanged v1 eager shell.
use super::*;
use arrow::array::StringArray;
#[test]
fn pure_differential_string_case_unicode_context_expansion_nul_and_aliases() {
    let values = Arc::new(StringArray::from(vec![
        Some("Iİıi"),
        Some("ΟΣ"),
        Some("ΟΣΑ"),
        Some("Ο\u{301}ΣΑ"),
        Some("ΟΣ\u{301}"),
        Some("Σ"),
        Some("Straße"),
        Some("ﬃ"),
        Some("AbC\0é"),
        Some("你好👩‍💻"),
        Some(""),
        None,
    ])) as ArrayRef;
    for name in ["upper", "lower"] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(values.clone())
                .sparse_selections(20, 900),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(values.slice(1, 10))
                .sparse_selections(20, 901),
        );
    }
}
#[test]
fn pure_differential_string_case_constants_null_empty_and_full_long_context() {
    let mixed = "aİ".repeat(1025);
    let sigma = "Ο".to_string() + &"A".repeat(1025) + "Σ";
    for name in ["upper", "lower"] {
        for text in [Some(mixed.as_str()), Some(sigma.as_str()), Some(""), None] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .constant_array(Arc::new(StringArray::from(vec![text])))
                    .constant_rows(5)
                    .sparse_selections(12, 902),
            );
        }
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(StringArray::from(Vec::<Option<&str>>::new())))
                .sparse_selections(12, 903),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(StringArray::from(vec![
                    Some(mixed.as_str()),
                    Some(sigma.as_str()),
                    None,
                ])))
                .sparse_selections(12, 904),
        );
    }
}

#[test]
fn pure_differential_string_case_raw_aliases_without_declarations_are_not_invented() {
    for name in ["ucase", "lcase"] {
        let failure = run_scalar_differential(
            &ScalarDiffSpec::new(name).column(Arc::new(StringArray::from(vec!["IİΟΣ"]))),
        )
        .unwrap_err();
        assert!(
            matches!(&failure, DifferentialFailure::Resolution { name: actual, error } if actual == name && error == "function is not registered")
        );
    }
}
