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

use super::cast_with_special_rules;
use arrow::array::{
    Array, ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use std::sync::Arc;

#[test]
fn legacy_date_carrier_epoch_leap_text_and_timestamp_units() {
    let input: ArrayRef = Arc::new(Date32Array::from(vec![
        Some(-1),
        Some(0),
        None,
        Some(1),
        Some(19782),
    ]));
    let text = cast_with_special_rules(&input, &DataType::Utf8).unwrap();
    assert_eq!(
        text.to_data(),
        StringArray::from(vec![
            Some("1969-12-31"),
            Some("1970-01-01"),
            None,
            Some("1970-01-02"),
            Some("2024-02-29")
        ])
        .to_data()
    );
    for (unit, scale) in [
        (TimeUnit::Second, 86400),
        (TimeUnit::Millisecond, 86400000),
        (TimeUnit::Microsecond, 86400000000),
        (TimeUnit::Nanosecond, 86400000000000),
    ] {
        let result = cast_with_special_rules(&input, &DataType::Timestamp(unit, None)).unwrap();
        let expected = vec![
            Some(-scale),
            Some(0),
            None,
            Some(scale),
            Some(19782 * scale),
        ];
        let expected: ArrayRef = match unit {
            TimeUnit::Second => Arc::new(TimestampSecondArray::from(expected)),
            TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(expected)),
            TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(expected)),
            TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(expected)),
        };
        assert_eq!(result.to_data(), expected.to_data());
    }
}

#[test]
fn legacy_timestamp_date_carrier_negative_fraction_and_extreme_error() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        let values = vec![Some(-1), Some(0), None, Some(1)];
        let input: ArrayRef = match unit {
            TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
            TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
            TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
            TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
        };
        let result = cast_with_special_rules(&input, &DataType::Date32).unwrap();
        assert_eq!(
            result.to_data(),
            Date32Array::from(vec![Some(-1), Some(0), None, Some(0)]).to_data()
        );
    }
    let input: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(0), Some(i64::MAX)]));
    assert_eq!(
        cast_with_special_rules(&input, &DataType::Date32)
            .unwrap_err()
            .detail(),
        "Cast error: Cannot convert arrow_array::types::TimestampSecondType 9223372036854775807 to datetime"
    );
    let input: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
    assert_eq!(
        cast_with_special_rules(&input, &DataType::Utf8)
            .unwrap_err()
            .detail(),
        "Cast error: Failed to convert 2147483647 to temporal for Date32"
    );
}

#[test]
fn legacy_date_carrier_extreme_timestamp_is_typed_numeric_refusal() {
    for days in [i32::MIN, i32::MAX] {
        let input: ArrayRef = Arc::new(Date32Array::from(vec![days]));
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let error =
                cast_with_special_rules(&input, &DataType::Timestamp(unit, None)).unwrap_err();
            assert_eq!(
                error.failure_fact(),
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            );
            assert_eq!(error.failure_fact().class(), FailureClass::Data);
        }
    }
}

#[test]
fn legacy_date_carrier_hidden_null_payload_is_not_multiplied() {
    let input: ArrayRef = Arc::new(Date32Array::new(
        vec![i32::MAX, 0].into(),
        Some(arrow_buffer::NullBuffer::from(vec![false, true])),
    ));
    let text = cast_with_special_rules(&input, &DataType::Utf8).unwrap();
    assert_eq!(
        text.to_data(),
        StringArray::from(vec![None, Some("1970-01-01")]).to_data()
    );
    for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        let result = cast_with_special_rules(&input, &DataType::Timestamp(unit, None)).unwrap();
        assert!(result.is_null(0));
        assert!(!result.is_null(1));
        match unit {
            TimeUnit::Microsecond => assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(1),
                0
            ),
            TimeUnit::Nanosecond => assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(1),
                0
            ),
            _ => unreachable!(),
        }
    }
}
