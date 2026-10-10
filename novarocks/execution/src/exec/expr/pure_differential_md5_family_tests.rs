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
//! Each actual fixed/dynamic MD5 family overload compares against the untouched raw baseline.
use super::*;
use arrow::array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Decimal32Array, Decimal64Array,
    Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, StringArray,
    StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
fn profiles() -> Vec<ArrayRef> {
    let txt = vec![Some("é\0"), None, Some(""), Some("hello")];
    let bytes = txt.iter().map(|v| v.map(str::as_bytes)).collect::<Vec<_>>();
    vec![
        Arc::new(StringArray::from(txt.clone())),
        Arc::new(LargeStringArray::from(txt.clone())),
        Arc::new(BinaryArray::from(bytes.clone())),
        Arc::new(LargeBinaryArray::from(bytes.clone())),
        Arc::new(StringViewArray::from(txt)),
        Arc::new(BinaryViewArray::from(bytes)),
        Arc::new(NullArray::new(4)),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int8Array::from(vec![
            Some(i8::MIN),
            None,
            Some(0),
            Some(i8::MAX),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(i16::MIN),
            None,
            Some(0),
            Some(i16::MAX),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            None,
            Some(0),
            Some(i32::MAX),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(0),
            Some(i64::MAX),
        ])),
        Arc::new(UInt8Array::from(vec![
            Some(u8::MIN),
            None,
            Some(0),
            Some(u8::MAX),
        ])),
        Arc::new(UInt16Array::from(vec![
            Some(u16::MIN),
            None,
            Some(0),
            Some(u16::MAX),
        ])),
        Arc::new(UInt32Array::from(vec![
            Some(u32::MIN),
            None,
            Some(0),
            Some(u32::MAX),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(u64::MIN),
            None,
            Some(0),
            Some(u64::MAX),
        ])),
        Arc::new(Float16Array::new(
            arrow::buffer::ScalarBuffer::new(
                arrow::buffer::Buffer::from_slice_ref(&[0u16, 0x8000, 0x7c00, 0x7e00]),
                0,
                4,
            ),
            None,
        )),
        Arc::new(Float32Array::from(vec![
            Some(-0.0),
            None,
            Some(f32::INFINITY),
            Some(f32::from_bits(0x7fc0_1234)),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            None,
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0x7ff8_0000_0000_1234)),
        ])),
        Arc::new(
            Decimal32Array::from(vec![Some(123), None, Some(-1), Some(0)])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![Some(12345), None, Some(-1), Some(0)])
                .with_precision_and_scale(18, 3)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(12345), None, Some(-1), Some(0)])
                .with_precision_and_scale(30, 4)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![
                Some(arrow::datatypes::i256::from_i128(12345)),
                None,
                Some(arrow::datatypes::i256::from_i128(-1)),
                Some(arrow::datatypes::i256::ZERO),
            ])
            .with_precision_and_scale(50, 5)
            .unwrap(),
        ),
    ]
}
#[test]
fn pure_differential_md5_dedup_keeps_sole_unary_utf8_overload() {
    for len in [0, 1, 255, 256, 257, 512, 513, 1025] {
        let huge = "é".repeat(len);
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5")
                .column(Arc::new(StringArray::from(vec![
                    Some(huge.as_str()),
                    None,
                    Some("é中"),
                    Some("a\0b"),
                ])))
                .sparse_selections(12, 2100 + len as u64),
        );
    }
    for literal in [true, false] {
        for rows in [0, 7] {
            for value in [Some("hello"), None] {
                let v = Arc::new(StringArray::from(vec![value])) as ArrayRef;
                let spec = ScalarDiffSpec::new("md5")
                    .constant_array(v)
                    .constant_rows(rows)
                    .sparse_selections(12, 2200);
                assert_scalar_matches_v1(if literal {
                    spec
                } else {
                    spec.legacy_constants(LegacyConstantForm::Pool)
                });
            }
        }
    }
}
#[test]
fn pure_differential_md5sum_fixed_variadic_any_flat_cast_profiles() {
    for (i, a) in profiles().into_iter().enumerate() {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5sum")
                .column(a.clone())
                .column(Arc::new(StringArray::from(vec![
                    Some("suffix"),
                    None,
                    Some(""),
                    Some("\0"),
                ])))
                .sparse_selections(12, 2300 + i as u64),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5sum")
                .column(a.slice(1, 3))
                .sparse_selections(12, 2350 + i as u64),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5sum")
                .column(a.slice(0, 0))
                .sparse_selections(12, 2380 + i as u64),
        );
    }
}
#[test]
fn pure_differential_md5sum_numeric_dynamic_flat_cast_profiles_keep_be128() {
    for (i, a) in profiles().into_iter().enumerate() {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5sum_numeric")
                .column(a.clone())
                .column(Arc::new(BinaryArray::from(vec![
                    Some(b"suffix".as_slice()),
                    None,
                    Some(b"".as_slice()),
                    Some(b"\0".as_slice()),
                ])))
                .sparse_selections(12, 2400 + i as u64),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5sum_numeric")
                .column(a.slice(1, 3))
                .sparse_selections(12, 2450 + i as u64),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("md5sum_numeric")
                .column(a.slice(0, 0))
                .sparse_selections(12, 2480 + i as u64),
        );
    }
}
#[test]
fn pure_differential_md5sum_both_overloads_keep_existing_nominal_byte_identity() {
    use novarocks_type_contract::ValueLogicalType;
    for name in ["md5sum", "md5sum_numeric"] {
        for logical in [
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::Object,
            ValueLogicalType::Percentile,
        ] {
            let ty =
                FunctionValueType::try_with_logical_type(DataType::Binary, true, logical).unwrap();
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .typed_column(
                        ty,
                        Arc::new(BinaryArray::from(vec![
                            Some(b"\xff\0".as_slice()),
                            None,
                            Some(b"".as_slice()),
                        ])),
                    )
                    .sparse_selections(12, 2500),
            );
        }
        for (logical, ty) in [
            (ValueLogicalType::Json, DataType::Utf8),
            (ValueLogicalType::Variant, DataType::LargeBinary),
        ] {
            let full = FunctionValueType::try_with_logical_type(ty.clone(), true, logical).unwrap();
            let a = if ty == DataType::Utf8 {
                Arc::new(StringArray::from(vec![
                    Some("null"),
                    None,
                    Some("{\"x\":1}"),
                ])) as ArrayRef
            } else {
                Arc::new(LargeBinaryArray::from(vec![
                    Some(b"opaque".as_slice()),
                    None,
                    Some(b"\xff".as_slice()),
                ])) as ArrayRef
            };
            assert_scalar_matches_v1(
                ScalarDiffSpec::new(name)
                    .typed_column(full, a)
                    .sparse_selections(12, 2510),
            );
        }
    }
}
#[test]
fn pure_differential_md5sum_nullable_literal_pool_constants_and_opaque_binary_views() {
    for name in ["md5sum", "md5sum_numeric"] {
        for literal in [true, false] {
            for rows in [0, 7] {
                for a in [
                    Arc::new(NullArray::new(1)) as ArrayRef,
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
                    Arc::new(Float64Array::from(vec![Some(-0.0)])) as ArrayRef,
                ] {
                    let spec = ScalarDiffSpec::new(name)
                        .constant_array(a)
                        .constant_array(Arc::new(StringArray::from(vec![Some("x")])))
                        .constant_rows(rows)
                        .sparse_selections(12, 2600);
                    assert_scalar_matches_v1(if literal {
                        spec
                    } else {
                        spec.legacy_constants(LegacyConstantForm::Pool)
                    });
                }
            }
        }
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .column(Arc::new(BinaryViewArray::from(vec![
                    Some(b"\xff".as_slice()),
                    None,
                    Some(b"hello".as_slice()),
                ])))
                .sparse_selections(12, 2610),
        );
    }
}
