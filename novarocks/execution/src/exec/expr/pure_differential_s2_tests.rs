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

//! Wave S2 selected encoding, money and hash differential evidence.
use super::*;
use arrow::array::{BinaryArray, Decimal128Array, Float64Array, Int64Array, StringArray};
#[test]
fn pure_differential_s2_unhex_matches_v1_including_invalid_empty() {
    let values = Arc::new(StringArray::from(vec![
        Some(""),
        Some("00"),
        Some("00FFaB"),
        Some("F"),
        Some("xx"),
        None,
        Some("中"),
        Some("a b"),
        Some("414243"),
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("unhex")
            .column(values)
            .sparse_selections(9, 41),
    );
}
#[test]
fn pure_differential_s2_to_binary_formats_and_null_default() {
    let values = Arc::new(StringArray::from(vec![
        Some(""),
        Some("00"),
        Some("YWJj"),
        Some("SGVsbG8="),
        Some("世界"),
        Some("00ff"),
        Some("xx"),
        Some("12"),
        None,
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("to_binary")
            .column(Arc::clone(&values))
            .sparse_selections(9, 44),
    );
    let formats = Arc::new(StringArray::from(vec![
        Some("encode64"),
        None,
        Some("EnCoDe64"),
        Some("encode64"),
        Some("UTF8"),
        Some("unknown"),
        Some("hex"),
        Some("HEX"),
        Some("utf8"),
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("to_binary")
            .column(values)
            .column(formats)
            .sparse_selections(9, 47),
    );
}
#[test]
fn pure_differential_s2_money_matches_rounding_and_row_errors() {
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("money_format")
            .column(Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(-1234567),
                Some(0),
                Some(i64::MAX),
                None,
            ])))
            .sparse_selections(9, 51),
    );
    let floats = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(-0.001),
        Some(0.005),
        Some(-0.005),
        Some(1234567.125),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::MAX),
        None,
    ])) as ArrayRef;
    for strict in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("money_format")
                .column(Arc::clone(&floats))
                .allow_throw_exception(strict)
                .sparse_selections(9, 52),
        );
    }
    let decimal = Arc::new(
        Decimal128Array::from(vec![Some(5), Some(-5), Some(999995), Some(-999995), None])
            .with_precision_and_scale(20, 3)
            .unwrap(),
    ) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("money_format")
            .column(decimal)
            .sparse_selections(9, 54),
    );
}
#[test]
fn pure_differential_s2_murmur_text_bytes_and_chained_integer() {
    let values = Arc::new(StringArray::from(vec![
        Some(""),
        Some("abc"),
        Some("中"),
        Some("\0"),
        Some("abcd"),
        Some("abcde"),
        None,
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(Arc::clone(&values))
            .sparse_selections(9, 61),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(values)
            .column(Arc::new(Int64Array::from(vec![
                Some(0),
                Some(-1),
                Some(i64::MIN),
                Some(i64::MAX),
                None,
                Some(42),
                Some(7),
            ])))
            .sparse_selections(9, 62),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(Arc::new(BinaryArray::from(vec![
                Some(&b""[..]),
                Some(&b"\0"[..]),
                Some(&b"\0\0\0"[..]),
                Some(&b"\xff\0a"[..]),
                None,
            ])))
            .sparse_selections(9, 63),
    );
}

#[test]
fn pure_differential_s2_murmur_numeric_varchar_grammar() {
    use arrow::array::{Decimal256Array, FixedSizeBinaryArray, Float32Array};
    use arrow_buffer::i256;
    let doubles = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(12.0),
        Some(1.25e30),
        Some(0.00001),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(doubles)
            .sparse_selections(9, 64),
    );
    let floats = Arc::new(Float32Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(12.0),
        Some(1.25e30),
        Some(0.00001),
        Some(f32::NAN),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        None,
    ])) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(floats)
            .sparse_selections(9, 65),
    );
    let decimals = Arc::new(
        Decimal128Array::from(vec![
            Some(12345678901234567890123456789012345),
            Some(-12345678901234567890123456789012345),
            Some(0),
            Some(1),
            Some(-1),
            None,
        ])
        .with_precision_and_scale(38, 9)
        .unwrap(),
    ) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(decimals)
            .sparse_selections(9, 66),
    );
    let big = Arc::new(
        Decimal256Array::from(vec![
            Some(
                i256::from_string(
                    "123456789012345678901234567890123456789012345678901234567890123456789012345",
                )
                .unwrap(),
            ),
            Some(i256::from_i128(-5)),
            Some(i256::ZERO),
            None,
        ])
        .with_precision_and_scale(76, 18)
        .unwrap(),
    ) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .column(big)
            .sparse_selections(9, 67),
    );
    let integers = [i128::MIN, 0, i128::MAX, -1].map(i128::to_be_bytes);
    let array = Arc::new(
        FixedSizeBinaryArray::try_from_iter(integers.iter().map(|b| b.as_slice())).unwrap(),
    ) as ArrayRef;
    let logical = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("murmur_hash3_32")
            .typed_column(logical, array)
            .sparse_selections(9, 68),
    );
}
#[test]
fn pure_differential_s2_murmur_timestamp_varchar_grammar() {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let seconds = vec![Some(0), Some(-1), Some(1_700_000_000), None];
    for array in [
        Arc::new(TimestampSecondArray::from(seconds.clone())) as ArrayRef,
        Arc::new(TimestampMillisecondArray::from(seconds.clone())) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(seconds.clone())) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(seconds)) as ArrayRef,
    ] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("murmur_hash3_32")
                .column(array)
                .sparse_selections(9, 69),
        );
    }
}

#[test]
fn pure_differential_s2_broadcast_constants_use_selected_rows() {
    for (name, text) in [
        ("unhex", "00FF"),
        ("to_binary", "414243"),
        ("murmur_hash3_32", "Hello, 世界"),
    ] {
        let argument = constant(
            FunctionValueType::new(DataType::Utf8, false),
            Arc::new(StringArray::from(vec![text])),
        );
        assert_scalar_matches_v1(
            ScalarDiffSpec::new(name)
                .constant(argument)
                .constant_rows(17)
                .sparse_selections(9, 70),
        );
    }
    let format = constant(
        FunctionValueType::new(DataType::Utf8, false),
        Arc::new(StringArray::from(vec!["UTF8"])),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("to_binary")
            .column(Arc::new(StringArray::from(vec![
                Some("a"),
                Some("世界"),
                None,
                Some(""),
                Some("00"),
            ])))
            .constant(format)
            .sparse_selections(9, 71),
    );
    let argument = constant(
        FunctionValueType::new(DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1234567])),
    );
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("money_format")
            .constant(argument)
            .constant_rows(17)
            .sparse_selections(9, 72),
    );
}
