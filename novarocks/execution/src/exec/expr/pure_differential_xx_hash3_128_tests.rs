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
//! Every admitted XXH3-128 byte profile compares to the unchanged original stream.
use super::*;
use arrow::array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
#[test]
fn pure_differential_xx_hash3_128_all_small_large_byte_carriers_and_mixed_variadic() {
    let text = vec![
        Some("hello"),
        Some("é\0世界"),
        Some(""),
        None,
        Some("ab"),
        Some("👩‍💻"),
    ];
    let bytes = text
        .iter()
        .map(|t| t.map(str::as_bytes))
        .collect::<Vec<_>>();
    let profiles: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(text.clone())),
        Arc::new(BinaryArray::from(bytes.clone())),
        Arc::new(LargeStringArray::from(text)),
        Arc::new(LargeBinaryArray::from(bytes)),
    ];
    for (i, left) in profiles.iter().enumerate() {
        for (j, right) in profiles.iter().enumerate() {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("xx_hash3_128")
                    .column(left.clone())
                    .column(right.clone())
                    .sparse_selections(12, 1000 + (i * 4 + j) as u64),
            );
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("xx_hash3_128")
                    .column(left.slice(1, 4))
                    .column(right.slice(1, 4))
                    .sparse_selections(12, 1020 + (i * 4 + j) as u64),
            );
        }
    }
    let mut spec = ScalarDiffSpec::new("xx_hash3_128").sparse_selections(12, 1040);
    for value in profiles {
        spec = spec.column(value);
    }
    assert_scalar_matches_v1(spec);
}
#[test]
fn pure_differential_xx_hash3_128_existing_nominal_byte_domains_keep_exact_largeint_result() {
    use novarocks_type_contract::ValueLogicalType;
    for identity in [
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
    ] {
        let ty =
            FunctionValueType::try_with_logical_type(DataType::Binary, true, identity).unwrap();
        let values = Arc::new(BinaryArray::from(vec![
            Some(b"hello".as_slice()),
            Some(b"\xff\0".as_slice()),
            None,
            Some(b"".as_slice()),
        ])) as ArrayRef;
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("xx_hash3_128")
                .typed_column(ty, values)
                .sparse_selections(12, 1041),
        );
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("xx_hash3_128")
            .typed_column(
                json,
                Arc::new(StringArray::from(vec![
                    Some("null"),
                    Some("123"),
                    Some("{\"x\":1}"),
                    None,
                ])),
            )
            .sparse_selections(12, 1042),
    );
    let variant = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Variant,
    )
    .unwrap();
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("xx_hash3_128")
            .typed_column(
                variant,
                Arc::new(LargeBinaryArray::from(vec![
                    Some(b"opaque".as_slice()),
                    None,
                    Some(b"\0\xff".as_slice()),
                ])),
            )
            .sparse_selections(12, 1043),
    );
}
#[test]
fn pure_differential_xx_hash3_128_constants_null_empty_and_256_write_boundaries() {
    for length in [0, 1, 255, 256, 257, 512, 513, 1025] {
        let text = "a".repeat(length);
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("xx_hash3_128")
                .column(Arc::new(StringArray::from(vec![
                    Some(text.as_str()),
                    None,
                    Some("hello"),
                ])))
                .constant_array(Arc::new(BinaryArray::from(vec![b"starrocks".as_slice()])))
                .sparse_selections(12, 1050 + length as u64),
        );
    }
    for ty in [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("xx_hash3_128")
                .column(arrow::array::new_empty_array(&ty))
                .sparse_selections(12, 1060),
        );
    }
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("xx_hash3_128")
            .constant_array(Arc::new(StringArray::from(vec![None::<&str>])))
            .constant_rows(5)
            .sparse_selections(12, 1061),
    );
}
