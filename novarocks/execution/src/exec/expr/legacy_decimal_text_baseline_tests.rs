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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, StringArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

fn original_text(array: &ArrayRef) -> ArrayRef {
    let output = super::cast::cast_with_special_rules(array, &DataType::Utf8).unwrap();
    assert_eq!(output.data_type(), &DataType::Utf8);
    output
}

fn assert_text(array: &ArrayRef, expected: Vec<Option<&str>>) {
    let output = original_text(array);
    let actual = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(actual, &StringArray::from(expected));
}

#[test]
fn original_decimal128_text_scale_negative_zero_positive_null_slice_empty() {
    for (scale, expected) in [
        (-3, vec![Some("1234"), Some("-5"), Some("0"), None]),
        (0, vec![Some("1234"), Some("-5"), Some("0"), None]),
        (2, vec![Some("12.34"), Some("-0.05"), Some("0.00"), None]),
        (
            6,
            vec![Some("0.001234"), Some("-0.000005"), Some("0.000000"), None],
        ),
    ] {
        let input: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1234), Some(-5), Some(0), None])
                .with_precision_and_scale(38, scale)
                .unwrap(),
        );
        assert_text(&input, expected.clone());
        assert_text(&input.slice(1, 3), expected[1..].to_vec());
        assert_text(&input.slice(0, 0), vec![]);
    }
}

#[test]
fn original_decimal256_text_full_width_digits_scale_and_null() {
    let wide = i256::from_string("1234567890123456789012345678901234567890123").unwrap();
    let input: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(wide), Some(-wide), Some(i256::ZERO), None])
            .with_precision_and_scale(76, 4)
            .unwrap(),
    );
    assert_text(
        &input,
        vec![
            Some("123456789012345678901234567890123456789.0123"),
            Some("-123456789012345678901234567890123456789.0123"),
            Some("0.0000"),
            None,
        ],
    );
    let zero_scale: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(wide), Some(-wide), None])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    assert_text(
        &zero_scale,
        vec![
            Some("1234567890123456789012345678901234567890123"),
            Some("-1234567890123456789012345678901234567890123"),
            None,
        ],
    );
    let negative_scale: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(wide), Some(-wide), None])
            .with_precision_and_scale(76, -3)
            .unwrap(),
    );
    assert_text(
        &negative_scale,
        vec![
            Some("1234567890123456789012345678901234567890123"),
            Some("-1234567890123456789012345678901234567890123"),
            None,
        ],
    );
}

#[test]
fn original_decimal256_min_payload_preserves_double_negative_formatter_bug() {
    // The old carrier admits this payload without a per-value precision check.
    // Its checked_neg fallback keeps the negative sign before adding another.
    let minimum = i256::from_string(
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
    )
    .unwrap();
    let input: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(minimum), None])
            .with_precision_and_scale(76, 2)
            .unwrap(),
    );
    assert_text(
        &input,
        vec![
            Some(
                "--578960446186580977117854925043439539266349923328202820197287920039565648199.68",
            ),
            None,
        ],
    );
}

#[test]
fn original_decimal128_min_payload_preserves_build_profile_panic_or_wrapping() {
    let input: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(i128::MIN)])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| original_text(&input)));
    if cfg!(debug_assertions) {
        assert!(result.is_err());
    } else {
        let actual = result.unwrap();
        assert_eq!(
            actual.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![Some("--1701411834604692317316873037158841057.28")])
        );
    }
    let scale_zero: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(i128::MIN), None])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    assert_text(
        &scale_zero,
        vec![Some("-170141183460469231731687303715884105728"), None],
    );
}

#[test]
fn original_decimal_text_slice_and_empty_do_not_visit_outside_min_payload() {
    let input128: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(i128::MIN), None, Some(7)])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    assert_text(&input128.slice(1, 2), vec![None, Some("0.07")]);
    assert_text(&input128.slice(0, 0), vec![]);
    let input256: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::from_string("-57896044618658097711785492504343953926634992332820282019728792003956564819968").unwrap()), None, Some(i256::from_i128(7))])
            .with_precision_and_scale(76, 2).unwrap(),
    );
    assert_text(&input256.slice(1, 2), vec![None, Some("0.07")]);
    assert_text(&input256.slice(0, 0), vec![]);
}
