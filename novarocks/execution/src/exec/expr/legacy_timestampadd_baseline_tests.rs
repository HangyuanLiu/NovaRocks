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

//! Immutable actual v1 TIMESTAMPADD values, carrier drift, errors and original panics.
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
fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}
fn micros(text: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}
fn values(output: ArrayRef) -> Vec<Option<i64>> {
    assert_eq!(output.data_type(), &ts());
    output
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn original_timestampadd_three_actual_temporal_sources_keep_day_values_nulls_and_empty() {
    for source in [
        dates(vec![Some(0), Some(0), Some(0), None]),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(0),
            Some(0),
            None,
        ])) as ArrayRef,
        text(vec![
            Some("1970-01-01"),
            Some("1970-01-01"),
            Some("1970-01-01"),
            None,
        ]),
    ] {
        let units = text(vec![Some("day"); 4]);
        let interval = ints(vec![Some(1), Some(-1), None, Some(i64::MAX)]);
        assert_eq!(
            values(
                eval(
                    "timestampadd",
                    vec![units.clone(), interval.clone(), source.clone()],
                    ts()
                )
                .unwrap()
            ),
            vec![Some(86400000000), Some(-86400000000), None, None]
        );
        assert_eq!(
            values(
                eval(
                    "timestampadd",
                    vec![units.slice(0, 0), interval.slice(0, 0), source.slice(0, 0)],
                    ts()
                )
                .unwrap()
            ),
            vec![]
        );
    }
}
#[test]
fn original_timestampadd_every_unit_clips_month_and_preserves_unknown_exact_spelling() {
    let units = [
        "year",
        "month",
        "week",
        "day",
        "hour",
        "minute",
        "second",
        "millisecond",
        "microsecond",
        "WeEk",
        "quarter",
        " day ",
        "SQL_TSI_SECOND",
        "",
    ];
    let expected = [
        "2025-01-31 12:34:56.123456",
        "2024-02-29 12:34:56.123456",
        "2024-02-07 12:34:56.123456",
        "2024-02-01 12:34:56.123456",
        "2024-01-31 13:34:56.123456",
        "2024-01-31 12:35:56.123456",
        "2024-01-31 12:34:57.123456",
        "2024-01-31 12:34:56.124456",
        "2024-01-31 12:34:56.123457",
        "2024-02-07 12:34:56.123456",
        "2024-01-31 12:34:56.123456",
        "2024-01-31 12:34:56.123456",
        "2024-01-31 12:34:56.123456",
        "2024-01-31 12:34:56.123456",
    ];
    let source = text(vec![Some("2024-01-31 12:34:56.123456"); units.len()]);
    let output = eval(
        "timestampadd",
        vec![
            text(units.into_iter().map(Some).collect()),
            ints(vec![Some(1); expected.len()]),
            source,
        ],
        ts(),
    )
    .unwrap();
    assert_eq!(
        values(output),
        expected
            .into_iter()
            .map(|s| Some(micros(s)))
            .collect::<Vec<_>>()
    );
}
#[test]
fn original_timestampadd_full_errors_and_requested_type_in_microsecond_carrier_are_frozen() {
    let units = text(vec![Some("day")]);
    let interval = ints(vec![Some(1)]);
    let source = text(vec![Some("1970-01-01")]);
    assert_eq!(
        eval(
            "timestampadd",
            vec![interval.clone(), interval.clone(), source.clone()],
            ts()
        )
        .unwrap_err(),
        "timestampadd expects unit string"
    );
    assert_eq!(
        eval(
            "timestampadd",
            vec![units.clone(), units.clone(), source.clone()],
            ts()
        )
        .unwrap_err(),
        "timestampadd expects int interval"
    );
    let wrong: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        eval(
            "timestampadd",
            vec![units.clone(), interval.clone(), wrong],
            ts()
        )
        .unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
    for target in [DataType::Date32, DataType::Utf8] {
        assert_eq!(
            values(
                eval(
                    "timestampadd",
                    vec![units.clone(), interval.clone(), source.clone()],
                    target
                )
                .unwrap()
            ),
            vec![None]
        );
    }
    for (unit, expected) in [
        (TimeUnit::Second, 86400),
        (TimeUnit::Millisecond, 86400000),
        (TimeUnit::Microsecond, 86400000000),
        (TimeUnit::Nanosecond, 86400000000000),
    ] {
        assert_eq!(
            values(
                eval(
                    "timestampadd",
                    vec![units.clone(), interval.clone(), source.clone()],
                    DataType::Timestamp(unit, None)
                )
                .unwrap()
            ),
            vec![Some(expected)]
        );
    }
}
#[test]
fn original_timestampadd_null_invalid_date_masks_overflow_and_i64_month_narrowing_is_frozen() {
    let source = text(vec![
        None,
        Some("invalid"),
        Some("2024-01-31"),
        Some("2024-01-31"),
    ]);
    assert_eq!(
        values(
            eval(
                "timestampadd",
                vec![
                    text(vec![Some("year"), Some("week"), None, Some("year")]),
                    ints(vec![Some(i64::MAX), Some(i64::MAX), Some(i64::MAX), None]),
                    source
                ],
                ts()
            )
            .unwrap()
        ),
        vec![None; 4]
    );
    let result = eval(
        "timestampadd",
        vec![
            text(vec![Some("month"), Some("month"), Some("year")]),
            ints(vec![Some(i64::MAX), Some(i64::MIN), Some(i64::MAX)]),
            text(vec![Some("2024-01-31"); 3]),
        ],
        ts(),
    )
    .unwrap();
    assert_eq!(
        values(result),
        vec![
            Some(micros("2023-12-31 00:00:00")),
            Some(micros("2024-01-31 00:00:00")),
            Some(micros("2023-01-31 00:00:00"))
        ]
    );
}
#[test]
fn original_timestampadd_duration_datetime_and_wrong_declared_arity_panics_remain_original() {
    for (unit, interval) in [
        ("week", i64::MAX),
        ("day", i64::MAX),
        ("hour", i64::MAX),
        ("minute", i64::MAX),
        ("second", i64::MAX),
        ("microsecond", i64::MAX),
    ] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval(
                "timestampadd",
                vec![
                    text(vec![Some(unit)]),
                    ints(vec![Some(interval)]),
                    text(vec![Some("2024-01-31")])
                ],
                ts()
            )))
            .is_err(),
            "{unit}"
        );
    }
    let year = catch_unwind(AssertUnwindSafe(|| {
        eval(
            "timestampadd",
            vec![
                text(vec![Some("year")]),
                ints(vec![Some(i64::from(i32::MAX))]),
                text(vec![Some("2024-01-31")]),
            ],
            ts(),
        )
    }));
    if cfg!(debug_assertions) {
        assert!(year.is_err());
    } else {
        assert_eq!(
            values(year.unwrap().unwrap()),
            vec![Some(micros("2023-01-31 00:00:00"))]
        );
    }
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval(
            "timestampadd",
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![Some(0)])),
                ints(vec![Some(1)])
            ],
            ts()
        )))
        .is_err()
    );
}
