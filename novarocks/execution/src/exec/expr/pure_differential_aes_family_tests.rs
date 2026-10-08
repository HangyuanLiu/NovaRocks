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
//! Complete declared AES generic signatures; actual accepted and Required-error forms.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{
        ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, NullArray,
        StringArray,
    },
    datatypes::DataType,
};
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn binary(v: Vec<Option<&[u8]>>) -> ArrayRef {
    Arc::new(BinaryArray::from(v))
}
fn assert_call(name: &str, arrays: Vec<ArrayRef>, seed: u64) {
    let mut spec = ScalarDiffSpec::new(name);
    for a in arrays {
        spec = spec.column(a);
    }
    assert_scalar_matches_v1(spec.sparse_selections(11, seed));
}
fn family(name: &str) {
    let modes =
        crate::exec::expr::function::encryption::legacy_aes_family_original_baseline_tests::MODES;
    let values = strings(vec![
        Some(""),
        Some("a\0b"),
        None,
        Some("世界"),
        Some("abcdef"),
    ]);
    let key = strings(vec![
        Some("k"),
        Some("a very long key that exercises original XOR folding"),
        Some("k"),
        Some(""),
        None,
    ]);
    assert_call(name, vec![values.clone(), key.clone()], 7301);
    for &mode in modes {
        assert_call(
            name,
            vec![
                values.clone(),
                key.clone(),
                strings(vec![None, Some("123"), Some(""), Some("iv"), None]),
                strings(vec![Some(mode); 5]),
            ],
            7302,
        );
        assert_call(
            name,
            vec![
                values.clone(),
                key.clone(),
                strings(vec![None, Some("123"), Some(""), Some("iv"), None]),
                strings(vec![Some(mode); 5]),
                strings(vec![Some(""), Some("aad"), None, Some("世界"), None]),
            ],
            7303,
        );
    }
    for ty in [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
        DataType::Null,
    ] {
        let array: ArrayRef = match ty {
            DataType::Utf8 => strings(vec![Some("a\0b"), None, Some("")]),
            DataType::Binary => binary(vec![Some(b"a\0b"), None, Some(b"\xff")]),
            DataType::LargeUtf8 => {
                Arc::new(LargeStringArray::from(vec![Some("a\0b"), None, Some("")]))
            }
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![
                Some(b"a\0b".as_slice()),
                None,
                Some(b"\xff".as_slice()),
            ])),
            DataType::Null => Arc::new(NullArray::new(3)),
            _ => unreachable!(),
        };
        for argument in 0..5 {
            let mut args = vec![
                strings(vec![Some("a"); 3]),
                strings(vec![Some("k"); 3]),
                strings(vec![Some("iv"); 3]),
                strings(vec![Some("AES_128_GCM"); 3]),
                strings(vec![Some("aad"); 3]),
            ];
            args[argument] = array.clone();
            assert_call(name, args, 7310 + argument as u64);
        }
        assert_call(
            name,
            vec![array.slice(1, 2), strings(vec![Some("k"); 2])],
            7320,
        );
        assert_call(name, vec![array.slice(0, 0), strings(vec![])], 7321);
    }
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for val in [Some(""), None, Some("ÿ中")] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .constant_array(strings(vec![val]))
                    .column(key.clone())
                    .legacy_constants(form)
                    .sparse_selections(7, 7331),
            );
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .column(values.clone())
                    .constant_array(strings(vec![val]))
                    .legacy_constants(form)
                    .sparse_selections(7, 7332),
            );
        }
    }
    for count in [1, 3, 6] {
        assert_call(
            name,
            (0..count).map(|_| strings(vec![Some("a"), None])).collect(),
            7341 + count as u64,
        );
    }
    for argument in 0..5 {
        let mut args = vec![
            strings(vec![Some("a"), None]),
            strings(vec![Some("k"), None]),
            strings(vec![Some("iv"), None]),
            strings(vec![Some("AES_128_GCM"), None]),
            strings(vec![Some("aad"), None]),
        ];
        args[argument] = Arc::new(Int64Array::from(vec![Some(1), None]));
        assert_call(name, args, 7350 + argument as u64);
    }
}
#[test]
fn pure_differential_aes_encrypt_complete_original_generic_profile() {
    family("aes_encrypt");
}
#[test]
fn pure_differential_aes_decrypt_complete_original_generic_profile() {
    family("aes_decrypt");
}
