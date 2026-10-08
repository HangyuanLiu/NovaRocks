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

//! Original CRC32 owner compares against the original shared IEEE calculation.
use super::*;
use arrow::array::StringArray;
#[test]
fn pure_differential_crc32_shared_unicode_null_slice_and_original_zlib_boundaries() {
    let texts = [0, 1, 255, 256, 257, 512, 513, 10000].map(|n| "x".repeat(n));
    let mut values = texts.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>();
    values.extend([
        Some("Iİıi"),
        Some("Straße"),
        Some("你好👩‍💻"),
        Some("a\0b"),
        None,
    ]);
    let array = Arc::new(StringArray::from(values)) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("crc32")
            .column(array.clone())
            .sparse_selections(20, 1200),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("crc32")
            .column(array.slice(3, 7))
            .sparse_selections(20, 1201),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("crc32")
            .column(array.slice(0, 0))
            .sparse_selections(20, 1202),
    );
}
#[test]
fn pure_differential_crc32_shared_literal_pool_null_and_zero_rows() {
    for rows in [0, 17] {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            for value in [Some("123456789"), Some("a\0b"), Some(""), None] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("crc32")
                        .constant_array(Arc::new(StringArray::from(vec![value])))
                        .constant_rows(rows)
                        .legacy_constants(form)
                        .sparse_selections(12, 1203),
                );
            }
        }
    }
}
