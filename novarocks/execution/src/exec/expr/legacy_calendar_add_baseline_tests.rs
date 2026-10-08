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

//! Immutable pre-extraction v1 day/week shift and timestampdiff behavioral oracles.
//! Every case enters the original date dispatcher without resolving a pure owner.

use super::{ExprArena, ExprNode, LiteralValue};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

fn chunk(columns: Vec<ArrayRef>) -> Chunk {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{}", i + 1), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn eval(name: &'static str, columns: Vec<ArrayRef>, result: DataType) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(i as u32 + 1)),
                c.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date(name),
            args: args.clone(),
        },
        result,
    );
    eval_date_function(name, &arena, call, &args, &chunk(columns))
}
fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn dates(values: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Date32Array::from(values))
}
fn ints(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn assert_ints(output: ArrayRef, expected: Vec<Option<i64>>) {
    assert_eq!(output.data_type(), &DataType::Int64);
    assert_eq!(output.to_data(), Int64Array::from(expected).to_data());
}
fn assert_dates(output: ArrayRef, expected: Vec<Option<i32>>) {
    assert_eq!(output.data_type(), &DataType::Date32);
    assert_eq!(output.to_data(), Date32Array::from(expected).to_data());
}
const SHIFTS: [(&str, i64); 8] = [
    ("date_add", 1),
    ("adddate", 1),
    ("days_add", 1),
    ("weeks_add", 7),
    ("date_sub", -1),
    ("subdate", -1),
    ("days_sub", -1),
    ("weeks_sub", -7),
];

#[test]
fn legacy_calendar_add_baseline_all_aliases_normal_values_and_nulls() {
    for (name, factor) in SHIFTS {
        assert_dates(
            eval(
                name,
                vec![
                    dates(vec![Some(0), Some(0), None, Some(0)]),
                    ints(vec![Some(1), Some(-1), Some(1), None]),
                ],
                DataType::Date32,
            )
            .unwrap(),
            vec![Some(factor as i32), Some(-factor as i32), None, None],
        );
        let expected = vec![
            Some(10_000_000 + factor * 86_400_000_000),
            Some(10_000_000 - factor * 86_400_000_000),
            None,
        ];
        for source in [
            text(vec![
                Some("1970-01-01 00:00:10"),
                Some("1970-01-01 00:00:10"),
                None,
            ]),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(10_000_000),
                Some(10_000_000),
                None,
            ])) as ArrayRef,
        ] {
            let output = eval(
                name,
                vec![source, ints(vec![Some(1), Some(-1), Some(1)])],
                DataType::Timestamp(TimeUnit::Microsecond, None),
            )
            .unwrap();
            assert_eq!(
                output.to_data(),
                TimestampMicrosecondArray::from(expected.clone()).to_data()
            );
        }
    }
}

#[test]
fn legacy_calendar_add_baseline_null_skips_duration_but_present_date_panics() {
    assert_dates(
        eval(
            "days_add",
            vec![dates(vec![None]), ints(vec![Some(i64::MAX)])],
            DataType::Date32,
        )
        .unwrap(),
        vec![None],
    );
    assert_dates(
        eval(
            "days_add",
            vec![text(vec![Some("invalid")]), ints(vec![Some(i64::MAX)])],
            DataType::Date32,
        )
        .unwrap(),
        vec![None],
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval(
            "days_add",
            vec![dates(vec![Some(0)]), ints(vec![Some(i64::MAX)])],
            DataType::Date32
        )))
        .is_err()
    );
}

#[test]
#[cfg(debug_assertions)]
fn legacy_calendar_add_baseline_factor_overflow_precedes_null_date_mask() {
    // Integer multiplication precedes the date Option::map in the original algorithm.
    for (name, interval) in [
        ("weeks_add", i64::MAX),
        ("weeks_sub", i64::MAX),
        ("days_sub", i64::MIN),
    ] {
        for date in [dates(vec![None]), text(vec![Some("invalid")])] {
            assert!(
                catch_unwind(AssertUnwindSafe(|| eval(
                    name,
                    vec![date, ints(vec![Some(interval)])],
                    DataType::Date32
                )))
                .is_err(),
                "{name}"
            );
        }
    }
}

#[test]
fn legacy_calendar_add_baseline_literal_broadcast_and_requested_seconds_carrier() {
    let mut arena = ExprArena::default();
    let date = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("1970-01-01 00:00:10".into())),
        DataType::Utf8,
    );
    let interval = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let args = vec![date, interval];
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("days_add"),
            args: args.clone(),
        },
        DataType::Timestamp(TimeUnit::Second, None),
    );
    let source = chunk(vec![ints(vec![Some(1), Some(-1), None])]);
    let output = eval_date_function("days_add", &arena, call, &args, &source).unwrap();
    // Preserve the historical numeric-seconds result in its microsecond Arrow carrier.
    assert_eq!(
        output.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        output.to_data(),
        TimestampMicrosecondArray::from(vec![Some(86410), Some(-86390), None]).to_data()
    );
}

#[test]
fn legacy_calendar_add_baseline_interval_conversion_keeps_original_raw_profiles() {
    assert_dates(
        eval(
            "days_add",
            vec![
                dates(vec![Some(0); 5]),
                text(vec![
                    Some(" 2 "),
                    Some("-1.9"),
                    Some("NaN"),
                    Some("invalid"),
                    None,
                ]),
            ],
            DataType::Date32,
        )
        .unwrap(),
        vec![Some(2), Some(-1), None, None, None],
    );
    assert_dates(
        eval(
            "days_add",
            vec![
                dates(vec![Some(0); 3]),
                Arc::new(Float64Array::from(vec![1.9, f64::NAN, f64::INFINITY])),
            ],
            DataType::Date32,
        )
        .unwrap(),
        vec![Some(1), None, None],
    );
    assert_dates(
        eval(
            "days_add",
            vec![
                dates(vec![Some(0); 2]),
                Arc::new(UInt64Array::from(vec![1, u64::MAX])),
            ],
            DataType::Date32,
        )
        .unwrap(),
        vec![Some(1), None],
    );
    // The old f64 fallback saturates this rounded 2^63 text to i64::MAX.
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval(
            "days_add",
            vec![
                dates(vec![Some(0)]),
                text(vec![Some("9223372036854775808")])
            ],
            DataType::Date32
        )))
        .is_err()
    );
}

#[test]
fn legacy_calendar_add_baseline_timestampdiff_units_calendar_fields_and_null_masks() {
    for (unit, expected) in [
        ("WeEk", 1),
        ("day", 8),
        ("hour", 192),
        ("minute", 11520),
        ("second", 691200),
        ("millisecond", 691200000),
    ] {
        assert_ints(
            eval(
                "timestampdiff",
                vec![
                    text(vec![Some(unit)]),
                    dates(vec![Some(0)]),
                    dates(vec![Some(8)]),
                ],
                DataType::Int64,
            )
            .unwrap(),
            vec![Some(expected)],
        );
    }
    assert_ints(
        eval(
            "timestampdiff",
            vec![
                text(vec![
                    Some("year"),
                    Some("month"),
                    Some("second"),
                    Some("bad"),
                    Some("bad"),
                    None,
                ]),
                text(vec![
                    Some("2023-12-31 23:59:59"),
                    Some("2024-01-31"),
                    Some("1970-01-01 00:00:00.999999"),
                    Some("invalid"),
                    None,
                    Some("1970-01-01"),
                ]),
                text(vec![
                    Some("2024-01-01"),
                    Some("2024-02-01"),
                    Some("1970-01-01"),
                    Some("1970-01-01"),
                    Some("1970-01-01"),
                    Some("1970-01-02"),
                ]),
            ],
            DataType::Int64,
        )
        .unwrap(),
        vec![Some(1), Some(1), Some(0), None, None, None],
    );
}

#[test]
fn legacy_calendar_add_baseline_original_unit_and_input_error_text() {
    assert_eq!(
        eval(
            "timestampdiff",
            vec![
                text(vec![Some(" quarter ")]),
                dates(vec![Some(0)]),
                dates(vec![Some(1)])
            ],
            DataType::Int64
        )
        .unwrap_err(),
        "unit of timestampdiff must be one of year/month/week/day/hour/minute/second/millisecond"
    );
    assert_eq!(
        eval(
            "timestampdiff",
            vec![
                Arc::new(BooleanArray::from(vec![None])),
                dates(vec![None]),
                dates(vec![None])
            ],
            DataType::Int64
        )
        .unwrap_err(),
        "timestampdiff expects unit string"
    );
    assert_eq!(
        eval(
            "days_add",
            vec![dates(vec![None]), Arc::new(BooleanArray::from(vec![None]))],
            DataType::Date32
        )
        .unwrap_err(),
        "date_add expects int"
    );
    assert_eq!(
        eval(
            "days_add",
            vec![
                Arc::new(BooleanArray::from(vec![None])),
                Arc::new(BooleanArray::from(vec![None]))
            ],
            DataType::Date32
        )
        .unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
}
