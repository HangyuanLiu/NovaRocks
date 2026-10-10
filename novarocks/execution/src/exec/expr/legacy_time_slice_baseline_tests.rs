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

//! Independent original DATE_SLICE/TIME_SLICE dispatcher values and errors.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use novarocks_types::SlotId;
use std::sync::Arc;
fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}
fn micros(s: &str) -> i64 {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}
fn day(s: &str) -> i32 {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .unwrap()
        .num_days_from_ce()
        - 719163
}
fn temporal(date: bool, v: Vec<Option<&str>>) -> ArrayRef {
    if date {
        Arc::new(Date32Array::from(
            v.into_iter().map(|v| v.map(day)).collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(TimestampMicrosecondArray::from(
            v.into_iter().map(|v| v.map(micros)).collect::<Vec<_>>(),
        ))
    }
}
fn count(v: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(v))
}
fn text(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn eval(name: &'static str, columns: Vec<ArrayRef>, result: DataType) -> Result<ArrayRef, String> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns.clone()).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
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
    eval_date_function(name, &arena, call, &args, &chunk)
}
fn run(
    date: bool,
    values: Vec<Option<&str>>,
    n: Vec<Option<i32>>,
    unit: Vec<Option<&str>>,
    boundary: Option<Vec<Option<&str>>>,
) -> Result<ArrayRef, String> {
    let mut cols = vec![temporal(date, values), count(n), text(unit)];
    if let Some(b) = boundary {
        cols.push(text(b));
    }
    eval(
        if date { "date_slice" } else { "time_slice" },
        cols,
        if date { DataType::Date32 } else { ts() },
    )
}
#[test]
fn original_time_slice_all_units_floor_ceil_and_origin_are_frozen() {
    for (unit, floor, ceil) in [
        ("year", "2024-01-01 00:00:00", "2025-01-01 00:00:00"),
        ("quarter", "2024-04-01 00:00:00", "2024-07-01 00:00:00"),
        ("month", "2024-05-01 00:00:00", "2024-06-01 00:00:00"),
        ("week", "2024-05-13 00:00:00", "2024-05-20 00:00:00"),
        ("day", "2024-05-17 00:00:00", "2024-05-18 00:00:00"),
        ("hour", "2024-05-17 15:00:00", "2024-05-17 16:00:00"),
        ("minute", "2024-05-17 15:16:00", "2024-05-17 15:17:00"),
        ("second", "2024-05-17 15:16:17", "2024-05-17 15:16:18"),
        (
            "millisecond",
            "2024-05-17 15:16:17.123000",
            "2024-05-17 15:16:17.124000",
        ),
        (
            "microsecond",
            "2024-05-17 15:16:17.123456",
            "2024-05-17 15:16:17.123457",
        ),
    ] {
        let actual = run(
            false,
            vec![Some("2024-05-17 15:16:17.123456"); 2],
            vec![Some(1); 2],
            vec![Some(unit); 2],
            Some(vec![Some("FLOOR"), Some("CeIl")]),
        )
        .unwrap();
        assert_eq!(
            actual.to_data(),
            temporal(false, vec![Some(floor), Some(ceil)]).to_data()
        );
    }
}
#[test]
fn original_date_slice_calendar_periods_week_origin_and_three_arg_floor_are_frozen() {
    for (unit, n, input, expected) in [
        ("years", 2, "2024-05-17", "2023-01-01"),
        ("quarters", 2, "2024-05-17", "2024-01-01"),
        ("months", 2, "2024-05-17", "2024-05-01"),
        ("weeks", 1, "1970-01-01", "1969-12-29"),
        ("days", 1, "2024-05-17", "2024-05-17"),
    ] {
        let actual = run(
            true,
            vec![Some(input)],
            vec![Some(n)],
            vec![Some(unit)],
            None,
        )
        .unwrap();
        assert_eq!(
            actual.to_data(),
            temporal(true, vec![Some(expected)]).to_data()
        );
    }
}
#[test]
fn original_slice_ceil_exact_boundary_advances_and_declared_output_stays_exact() {
    for (date, input, floor, ceil) in [
        (true, "2024-05-01", "2024-05-01", "2024-06-01"),
        (
            false,
            "2024-05-01 00:00:00",
            "2024-05-01 00:00:00",
            "2024-06-01 00:00:00",
        ),
    ] {
        let actual = run(
            date,
            vec![Some(input); 2],
            vec![Some(1); 2],
            vec![Some("month"); 2],
            Some(vec![Some("floor"), Some("ceil")]),
        )
        .unwrap();
        assert_eq!(
            actual.to_data(),
            temporal(date, vec![Some(floor), Some(ceil)]).to_data()
        );
    }
}
#[test]
fn original_slice_validates_all_control_inputs_before_sql_null_temporal() {
    for date in [false, true] {
        let name = if date { "date_slice" } else { "time_slice" };
        for (n, u, b, expected) in [
            (
                None,
                None,
                None,
                format!("{name} requires non-null interval"),
            ),
            (
                Some(0),
                None,
                None,
                format!("{name} requires non-null unit"),
            ),
            (
                Some(0),
                Some("bad"),
                None,
                format!("{name} requires second parameter must be greater than 0"),
            ),
            (
                Some(1),
                Some("UNSUPPORTED"),
                None,
                "time_slice unsupported unit: unsupported".into(),
            ),
            (
                Some(1),
                Some("day"),
                None,
                format!("{name} requires non-null boundary"),
            ),
            (
                Some(1),
                Some("day"),
                Some("OTHER"),
                "time_slice expects boundary floor/ceil, got other".into(),
            ),
        ] {
            assert_eq!(
                run(date, vec![None], vec![n], vec![u], Some(vec![b])).unwrap_err(),
                expected
            );
        }
        assert_eq!(
            run(date, vec![None], vec![Some(1)], vec![Some("day")], None)
                .unwrap()
                .to_data(),
            temporal(date, vec![None]).to_data()
        );
    }
    assert_eq!(
        run(
            true,
            vec![None],
            vec![Some(1)],
            vec![Some("millisecond")],
            Some(vec![None])
        )
        .unwrap_err(),
        "can't use time_slice for date with time(hour/minute/second)"
    );
}
#[test]
fn original_slice_before_origin_invalid_payload_and_positive_int32_overflow_are_frozen() {
    for date in [false, true] {
        let before = if date {
            "0000-12-31"
        } else {
            "0000-12-31 23:59:59"
        };
        assert_eq!(
            run(
                date,
                vec![Some(before)],
                vec![Some(1)],
                vec![Some("day")],
                None
            )
            .unwrap_err(),
            "time used with time_slice can't before 0001-01-01 00:00:00"
        );
        let input = if date {
            "9999-12-31"
        } else {
            "9999-12-31 23:59:59"
        };
        assert_eq!(
            run(
                date,
                vec![Some(input)],
                vec![Some(i32::MAX)],
                vec![Some("day")],
                Some(vec![Some("ceil")])
            )
            .unwrap()
            .to_data(),
            temporal(date, vec![None]).to_data()
        );
    }
    let source = Arc::new(Date32Array::from(vec![
        Some(i32::MIN),
        Some(i32::MAX),
        None,
    ])) as ArrayRef;
    assert_eq!(
        eval(
            "date_slice",
            vec![source, count(vec![Some(1); 3]), text(vec![Some("day"); 3])],
            DataType::Date32
        )
        .unwrap()
        .to_data(),
        Date32Array::from(vec![None, None, None]).to_data()
    );
    let source = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(i64::MAX),
        Some(i64::MIN),
        None,
    ])) as ArrayRef;
    assert_eq!(
        eval(
            "time_slice",
            vec![source, count(vec![Some(1); 3]), text(vec![Some("day"); 3])],
            ts()
        )
        .unwrap()
        .to_data(),
        TimestampMicrosecondArray::from(vec![None, None, None]).to_data()
    );
}
#[test]
fn original_slice_exact_arity_type_and_string_errors_are_frozen() {
    for date in [false, true] {
        let name = if date { "date_slice" } else { "time_slice" };
        let ty = if date { DataType::Date32 } else { ts() };
        let value = if date {
            "1970-01-01"
        } else {
            "1970-01-01 00:00:00"
        };
        let source = temporal(date, vec![Some(value)]);
        assert_eq!(
            eval(name, vec![source.clone(), count(vec![Some(1)])], ty.clone()).unwrap_err(),
            format!("{name} expects value, count, unit and optional boundary")
        );
        assert_eq!(
            eval(
                name,
                vec![
                    source.clone(),
                    count(vec![Some(1)]),
                    text(vec![Some("day")])
                ],
                DataType::Int64
            )
            .unwrap_err(),
            format!("{name} result type differs from its frozen temporal domain")
        );
        assert_eq!(
            eval(
                name,
                vec![
                    source.clone(),
                    Arc::new(Int64Array::from(vec![1])),
                    text(vec![Some("day")])
                ],
                ty.clone()
            )
            .unwrap_err(),
            format!("{name} arguments differ from their frozen temporal/INT32 domains")
        );
        assert_eq!(
            eval(
                name,
                vec![source.clone(), count(vec![Some(1)]), count(vec![Some(1)])],
                ty.clone()
            )
            .unwrap_err(),
            "time_slice expects unit string"
        );
        assert_eq!(
            eval(
                name,
                vec![
                    source,
                    count(vec![Some(1)]),
                    text(vec![Some("day")]),
                    count(vec![Some(1)])
                ],
                ty
            )
            .unwrap_err(),
            format!("{name} expects boundary string")
        );
    }
}
#[test]
fn original_slice_nonzero_arrow_slice_and_empty_are_frozen() {
    for date in [false, true] {
        let name = if date { "date_slice" } else { "time_slice" };
        let ty = if date { DataType::Date32 } else { ts() };
        let values = if date {
            vec![
                Some("1900-01-01"),
                Some("2024-05-17"),
                None,
                Some("2000-01-01"),
            ]
        } else {
            vec![
                Some("1900-01-01 00:00:00"),
                Some("2024-05-17 12:34:56"),
                None,
                Some("2000-01-01 00:00:00"),
            ]
        };
        let source = temporal(date, values).slice(1, 2);
        let output = eval(
            name,
            vec![
                source,
                count(vec![Some(1); 2]),
                text(vec![Some("month"); 2]),
            ],
            ty.clone(),
        )
        .unwrap();
        assert_eq!(
            output.to_data(),
            temporal(
                date,
                if date {
                    vec![Some("2024-05-01"), None]
                } else {
                    vec![Some("2024-05-01 00:00:00"), None]
                }
            )
            .to_data()
        );
        assert_eq!(
            eval(
                name,
                vec![temporal(date, vec![]), count(vec![]), text(vec![])],
                ty
            )
            .unwrap()
            .len(),
            0
        );
    }
}
