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

//! Immutable pre-extraction v1 month/year shift behavioral oracles.
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
const MONTH_SHIFTS: [(&str, &str); 7] = [
    ("add_months", "2024-02-29 12:34:56.123456"),
    ("months_add", "2024-02-29 12:34:56.123456"),
    ("months_sub", "2023-12-31 12:34:56.123456"),
    ("quarters_add", "2024-04-30 12:34:56.123456"),
    ("quarters_sub", "2023-10-31 12:34:56.123456"),
    ("years_add", "2025-01-31 12:34:56.123456"),
    ("years_sub", "2023-01-31 12:34:56.123456"),
];
fn micros(value: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}
fn assert_micros(output: ArrayRef, values: Vec<Option<i64>>) {
    assert_eq!(
        output.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        output.to_data(),
        TimestampMicrosecondArray::from(values).to_data()
    );
}
fn micro_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

#[test]
fn legacy_calendar_month_baseline_all_raw_aliases_end_of_month_time_and_nulls() {
    for (name, expected) in MONTH_SHIFTS {
        for source in [
            text(vec![
                Some("2024-01-31 12:34:56.123456"),
                None,
                Some("invalid"),
                Some("2024-01-31 12:34:56.123456"),
            ]),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(micros("2024-01-31 12:34:56.123456")),
                None,
                None,
                Some(0),
            ])) as ArrayRef,
        ] {
            assert_micros(
                eval(
                    name,
                    vec![source, ints(vec![Some(1), Some(1), Some(1), None])],
                    micro_type(),
                )
                .unwrap(),
                vec![Some(micros(expected)), None, None, None],
            );
        }
    }
    assert_micros(
        eval(
            "add_months",
            vec![
                dates(vec![Some(0), Some(0), None]),
                ints(vec![Some(1), Some(-1), Some(1)]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![Some(2_678_400_000_000), Some(-2_678_400_000_000), None],
    );
    assert_micros(
        eval(
            "months_add",
            vec![
                text(vec![
                    Some("2024-01-31 12:34:56.123456"),
                    Some("2024-03-31 12:34:56.123456"),
                    Some("2024-02-29 12:34:56.123456"),
                ]),
                ints(vec![Some(1), Some(-1), Some(12)]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![
            Some(micros("2024-02-29 12:34:56.123456")),
            Some(micros("2024-02-29 12:34:56.123456")),
            Some(micros("2025-02-28 12:34:56.123456")),
        ],
    );
    for (name, date) in [
        ("months_sub", "0000-01-01 00:00:00"),
        ("months_add", "9999-12-31 23:59:59.999999"),
    ] {
        assert_micros(
            eval(
                name,
                vec![text(vec![Some(date)]), ints(vec![Some(1)])],
                micro_type(),
            )
            .unwrap(),
            vec![None],
        );
    }
}

#[test]
fn legacy_calendar_month_baseline_declared_date32_result_is_all_null_in_timestamp_carrier() {
    // Registry Date32 declarations drift from the original raw carrier. Do not normalize this oracle.
    for name in ["months_add", "months_sub", "years_add", "years_sub"] {
        assert_micros(
            eval(
                name,
                vec![
                    dates(vec![Some(0), Some(0), None]),
                    ints(vec![Some(1), None, Some(1)]),
                ],
                DataType::Date32,
            )
            .unwrap(),
            vec![None, None, None],
        );
    }
}

#[test]
fn legacy_calendar_month_baseline_i64_to_i32_wrap_and_null_skip_calendar_panics() {
    for name in [
        "add_months",
        "months_add",
        "months_sub",
        "years_add",
        "years_sub",
    ] {
        assert_micros(
            eval(
                name,
                vec![
                    text(vec![
                        Some("1970-01-01 00:00:10"),
                        Some("1970-01-01 00:00:10"),
                        None,
                        Some("invalid"),
                    ]),
                    ints(vec![
                        Some(1_i64 << 32),
                        Some(i64::MIN),
                        Some(i64::MAX),
                        Some(i64::MAX),
                    ]),
                ],
                micro_type(),
            )
            .unwrap(),
            vec![Some(10_000_000), Some(10_000_000), None, None],
        );
    }
    // A missing interval skips all multiplication and calendar construction.
    assert_micros(
        eval(
            "years_add",
            vec![text(vec![Some("invalid")]), ints(vec![None])],
            micro_type(),
        )
        .unwrap(),
        vec![None],
    );
}

#[test]
fn legacy_calendar_month_baseline_original_calendar_unwrap_panics() {
    for name in [
        "add_months",
        "months_add",
        "months_sub",
        "years_add",
        "years_sub",
    ] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval(
                name,
                vec![dates(vec![Some(0)]), ints(vec![Some(10_000_000)])],
                micro_type()
            )))
            .is_err(),
            "{name}"
        );
    }
}

#[test]
#[cfg(debug_assertions)]
fn legacy_calendar_month_baseline_factor_overflow_precedes_null_date_mask() {
    for (name, interval) in [
        ("months_sub", i32::MIN as i64),
        ("years_add", i32::MAX as i64),
        ("years_sub", i32::MAX as i64),
        ("quarters_add", i32::MAX as i64),
        ("quarters_sub", i32::MAX as i64),
    ] {
        for source in [dates(vec![None]), text(vec![Some("invalid")])] {
            assert!(
                catch_unwind(AssertUnwindSafe(|| eval(
                    name,
                    vec![source, ints(vec![Some(interval)])],
                    micro_type()
                )))
                .is_err(),
                "{name}"
            );
        }
    }
    // The first month-index addition can overflow separately from the factor.
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval(
            "months_add",
            vec![
                text(vec![Some("1970-02-01 00:00:00")]),
                ints(vec![Some(i32::MAX as i64)])
            ],
            micro_type()
        )))
        .is_err()
    );
}

#[test]
fn legacy_calendar_month_baseline_original_type_error_order_and_raw_interval_conversion() {
    let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        eval(
            "months_add",
            vec![Arc::clone(&boolean), Arc::clone(&boolean)],
            micro_type()
        )
        .unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
    assert_eq!(
        eval(
            "years_sub",
            vec![dates(vec![Some(0)]), boolean],
            micro_type()
        )
        .unwrap_err(),
        "add_months expects int"
    );
    assert_micros(
        eval(
            "add_months",
            vec![
                dates(vec![Some(0); 5]),
                text(vec![
                    Some(" 1 "),
                    Some("-1.9"),
                    Some("NaN"),
                    Some("invalid"),
                    None,
                ]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![
            Some(2_678_400_000_000),
            Some(-2_678_400_000_000),
            None,
            None,
            None,
        ],
    );
    assert_micros(
        eval(
            "months_add",
            vec![
                dates(vec![Some(0); 3]),
                Arc::new(Float64Array::from(vec![f64::NAN, f64::INFINITY, 1.9])),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![None, None, Some(2_678_400_000_000)],
    );
    assert_micros(
        eval(
            "months_add",
            vec![
                dates(vec![Some(0); 2]),
                Arc::new(UInt64Array::from(vec![u64::MAX, 0])),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![None, Some(0)],
    );
}

#[test]
fn legacy_calendar_month_baseline_literal_broadcast_and_requested_seconds_carrier() {
    let mut arena = ExprArena::default();
    let date = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("1970-01-01 00:00:10".into())),
        DataType::Utf8,
    );
    let interval = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let args = vec![date, interval];
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("months_add"),
            args: args.clone(),
        },
        DataType::Timestamp(TimeUnit::Second, None),
    );
    let source = chunk(vec![ints(vec![Some(1), Some(-1), None])]);
    let output = eval_date_function("months_add", &arena, call, &args, &source).unwrap();
    assert_micros(output, vec![Some(2_678_410), Some(-2_678_390), None]);
}
