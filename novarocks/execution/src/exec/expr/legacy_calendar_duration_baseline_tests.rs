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

//! Immutable pre-extraction v1 duration shift behavioral oracles.
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
const DURATION_SHIFTS: [(&str, i64); 10] = [
    ("seconds_add", 1_000_000),
    ("seconds_sub", -1_000_000),
    ("minutes_add", 60_000_000),
    ("minutes_sub", -60_000_000),
    ("hours_add", 3_600_000_000),
    ("hours_sub", -3_600_000_000),
    ("milliseconds_add", 1_000),
    ("milliseconds_sub", -1_000),
    ("microseconds_add", 1),
    ("microseconds_sub", -1),
];
fn micro_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}
fn assert_micros(output: ArrayRef, values: Vec<Option<i64>>) {
    assert_eq!(output.data_type(), &micro_type());
    assert_eq!(
        output.to_data(),
        TimestampMicrosecondArray::from(values).to_data()
    );
}

#[test]
fn legacy_calendar_duration_baseline_all_raw_aliases_normal_values_and_nulls() {
    for (name, scale) in DURATION_SHIFTS {
        for source in [
            text(vec![
                Some("1970-01-01 00:00:10"),
                Some("1970-01-01 00:00:10"),
                None,
                Some("invalid"),
                Some("1970-01-01"),
            ]),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(10_000_000),
                Some(10_000_000),
                None,
                None,
                Some(0),
            ])) as ArrayRef,
        ] {
            assert_micros(
                eval(
                    name,
                    vec![
                        source,
                        ints(vec![Some(1), Some(-1), Some(1), Some(1), None]),
                    ],
                    micro_type(),
                )
                .unwrap(),
                vec![
                    Some(10_000_000 + scale),
                    Some(10_000_000 - scale),
                    None,
                    None,
                    None,
                ],
            );
        }
        assert_micros(
            eval(
                name,
                vec![
                    dates(vec![Some(0), Some(0), None]),
                    ints(vec![Some(1), Some(-1), Some(1)]),
                ],
                micro_type(),
            )
            .unwrap(),
            vec![Some(scale), Some(-scale), None],
        );
    }
}

#[test]
fn legacy_calendar_duration_baseline_declared_date32_result_is_all_null_in_timestamp_carrier() {
    for name in [
        "seconds_add",
        "seconds_sub",
        "minutes_add",
        "minutes_sub",
        "hours_add",
        "hours_sub",
        "microseconds_add",
        "microseconds_sub",
    ] {
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
fn legacy_calendar_duration_baseline_duration_constructor_runs_even_for_null_dates() {
    for name in [
        "seconds_add",
        "seconds_sub",
        "minutes_add",
        "minutes_sub",
        "hours_add",
        "hours_sub",
    ] {
        for source in [dates(vec![None]), text(vec![Some("invalid")])] {
            assert!(
                catch_unwind(AssertUnwindSafe(|| eval(
                    name,
                    vec![source, ints(vec![Some(i64::MAX)])],
                    micro_type()
                )))
                .is_err(),
                "{name}"
            );
        }
    }
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval(
            "milliseconds_add",
            vec![dates(vec![None]), ints(vec![Some(i64::MIN)])],
            micro_type()
        )))
        .is_err()
    );
    // Microsecond construction is defined for the entire i64 input domain.
    assert_micros(
        eval(
            "microseconds_add",
            vec![
                dates(vec![None, None]),
                ints(vec![Some(i64::MIN), Some(i64::MAX)]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![None, None],
    );
    assert_micros(
        eval(
            "milliseconds_add",
            vec![dates(vec![None]), ints(vec![Some(i64::MAX)])],
            micro_type(),
        )
        .unwrap(),
        vec![None],
    );
    // NULL interval bypasses duration construction and date addition entirely.
    for (name, _) in DURATION_SHIFTS {
        assert_micros(
            eval(
                name,
                vec![text(vec![Some("invalid")]), ints(vec![None])],
                micro_type(),
            )
            .unwrap(),
            vec![None],
        );
    }
}

#[test]
#[cfg(debug_assertions)]
fn legacy_calendar_duration_baseline_subtraction_negation_precedes_null_date_mask() {
    for name in [
        "seconds_sub",
        "minutes_sub",
        "hours_sub",
        "milliseconds_sub",
        "microseconds_sub",
    ] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval(
                name,
                vec![dates(vec![None]), ints(vec![Some(i64::MIN)])],
                micro_type()
            )))
            .is_err(),
            "{name}"
        );
    }
}

#[test]
fn legacy_calendar_duration_baseline_existing_chrono_date_addition_panics() {
    for name in [
        "microseconds_add",
        "microseconds_sub",
        "milliseconds_add",
        "milliseconds_sub",
    ] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval(
                name,
                vec![dates(vec![Some(0)]), ints(vec![Some(i64::MAX)])],
                micro_type()
            )))
            .is_err(),
            "{name}"
        );
    }
}

#[test]
fn legacy_calendar_duration_baseline_original_error_order_and_interval_conversion() {
    let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        eval(
            "seconds_add",
            vec![Arc::clone(&boolean), Arc::clone(&boolean)],
            micro_type()
        )
        .unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
    assert_eq!(
        eval(
            "hours_sub",
            vec![dates(vec![Some(0)]), boolean],
            micro_type()
        )
        .unwrap_err(),
        "duration add expects int"
    );
    assert_micros(
        eval(
            "seconds_add",
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
        vec![Some(1_000_000), Some(-1_000_000), None, None, None],
    );
    assert_micros(
        eval(
            "minutes_add",
            vec![
                dates(vec![Some(0); 3]),
                Arc::new(Float64Array::from(vec![f64::NAN, f64::INFINITY, 1.9])),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![None, None, Some(60_000_000)],
    );
    assert_micros(
        eval(
            "hours_add",
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
fn legacy_calendar_duration_baseline_literal_broadcast_and_requested_seconds_carrier() {
    let mut arena = ExprArena::default();
    let date = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("1970-01-01 00:00:10".into())),
        DataType::Utf8,
    );
    let interval = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    let args = vec![date, interval];
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("seconds_add"),
            args: args.clone(),
        },
        DataType::Timestamp(TimeUnit::Second, None),
    );
    let source = chunk(vec![ints(vec![Some(1), Some(-1), None])]);
    assert_micros(
        eval_date_function("seconds_add", &arena, call, &args, &source).unwrap(),
        vec![Some(11), Some(9), None],
    );
}
