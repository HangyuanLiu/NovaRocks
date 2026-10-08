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

//! Independent legacy text-format receipts before sharing the formatter.
use super::common::{AggScalarValue as V, scalar_to_string};
use arrow::datatypes::{DataType, Field, TimeUnit};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
#[test]
fn legacy_aggregate_formatter_negative_timestamp_units_and_zone_spelling() {
    for (unit, expected) in [
        (TimeUnit::Second, "1969-12-31 23:59:59"),
        (TimeUnit::Millisecond, "1970-01-01 00:00:00.999"),
        (TimeUnit::Microsecond, "1969-12-31 23:59:59.999999"),
        (TimeUnit::Nanosecond, "1969-12-31 23:59:59.999999999"),
    ] {
        assert_eq!(
            scalar_to_string(&V::Timestamp(-1), &DataType::Timestamp(unit, None)).unwrap(),
            expected
        );
        assert_eq!(
            scalar_to_string(
                &V::Timestamp(-1),
                &DataType::Timestamp(unit, Some("Asia/Shanghai".into()))
            )
            .unwrap(),
            format!("{expected} Asia/Shanghai")
        );
    }
    assert_eq!(
        scalar_to_string(
            &V::Timestamp(i64::MAX),
            &DataType::Timestamp(TimeUnit::Second, None)
        )
        .unwrap(),
        "1970-01-01 00:00:00"
    );
}
#[test]
fn legacy_aggregate_formatter_recursive_parent_type_and_scalar_encodings() {
    let list = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Decimal128(9, 2),
        true,
    )));
    assert_eq!(
        scalar_to_string(
            &V::List(vec![Some(V::Decimal128(123)), None, Some(V::Timestamp(-1))]),
            &list
        )
        .unwrap(),
        "[123,NULL,-1]"
    );
    assert_eq!(
        scalar_to_string(&V::Decimal128(-3), &DataType::Decimal128(9, 2)).unwrap(),
        "-0.03"
    );
    assert_eq!(
        scalar_to_string(&V::Decimal128(123), &DataType::Decimal128(9, -2)).unwrap(),
        "123"
    );
    assert_eq!(
        scalar_to_string(&V::Binary(vec![0, 127, 255]), &DataType::Binary).unwrap(),
        "007fff"
    );
    assert_eq!(
        scalar_to_string(&V::Date32(0), &DataType::Date32).unwrap(),
        "1970-01-01"
    );
    assert_eq!(
        scalar_to_string(&V::Date32(i32::MIN), &DataType::Date32).unwrap_err(),
        "invalid date32 value"
    );
}
#[test]
fn legacy_aggregate_formatter_original_debug_overflow_panics() {
    if !cfg!(debug_assertions) {
        return;
    }
    for (value, ty, expected) in [
        (
            V::Date32(i32::MAX),
            DataType::Date32,
            "attempt to add with overflow",
        ),
        (
            V::Decimal128(i128::MIN),
            DataType::Decimal128(38, 2),
            "attempt to negate with overflow",
        ),
    ] {
        let panic = catch_unwind(AssertUnwindSafe(|| scalar_to_string(&value, &ty)))
            .expect_err("original debug overflow");
        let text = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap();
        assert_eq!(text, expected);
    }
}
