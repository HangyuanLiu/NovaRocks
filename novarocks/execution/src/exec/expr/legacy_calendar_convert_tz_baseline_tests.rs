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

//! Immutable pre-extraction v1 explicit timezone conversion behavioral oracles.
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
fn micro_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}
fn micros(value: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}
fn timestamps(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from(
        values
            .into_iter()
            .map(|value| value.map(micros))
            .collect::<Vec<_>>(),
    ))
}
fn assert_micros(output: ArrayRef, values: Vec<Option<i64>>) {
    assert_eq!(output.data_type(), &micro_type());
    assert_eq!(
        output.to_data(),
        TimestampMicrosecondArray::from(values).to_data()
    );
}

#[test]
fn legacy_calendar_convert_tz_baseline_fixed_offsets_case_insensitive_utc_and_nulls() {
    assert_micros(
        eval(
            "convert_tz",
            vec![
                timestamps(vec![Some("1970-01-01 12:34:56.123456"); 6]),
                text(vec![
                    Some("+08:00"),
                    Some("uTc"),
                    Some("UTC"),
                    Some("unknown"),
                    None,
                    Some("UTC"),
                ]),
                text(vec![
                    Some("-05:00"),
                    Some("+08:00"),
                    Some("local"),
                    Some("UTC"),
                    Some("UTC"),
                    None,
                ]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![
            Some(micros("1969-12-31 23:34:56.123456")),
            Some(micros("1970-01-01 20:34:56.123456")),
            None,
            None,
            None,
            None,
        ],
    );
}

#[test]
fn legacy_calendar_convert_tz_baseline_named_dst_gap_and_ambiguous_earliest_are_original() {
    assert_micros(
        eval(
            "convert_tz",
            vec![
                timestamps(vec![
                    Some("2024-11-03 01:30:00"),
                    Some("2024-03-10 02:30:00"),
                    Some("2024-01-01 00:00:00"),
                    None,
                ]),
                text(vec![
                    Some("America/New_York"),
                    Some("America/New_York"),
                    Some("Asia/Shanghai"),
                    Some("UTC"),
                ]),
                text(vec![
                    Some("UTC"),
                    Some("UTC"),
                    Some("America/New_York"),
                    Some("UTC"),
                ]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![
            Some(micros("2024-11-03 05:30:00")),
            None,
            Some(micros("2023-12-31 11:00:00")),
            None,
        ],
    );
}

#[test]
fn legacy_calendar_convert_tz_baseline_datetime_type_error_precedes_timezone_type_error() {
    let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        eval(
            "convert_tz",
            vec![
                Arc::clone(&boolean),
                Arc::clone(&boolean),
                Arc::clone(&boolean)
            ],
            micro_type()
        )
        .unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
    assert_eq!(
        eval(
            "convert_tz",
            vec![
                timestamps(vec![None]),
                Arc::clone(&boolean),
                text(vec![Some("UTC")])
            ],
            micro_type()
        )
        .unwrap_err(),
        "convert_tz expects string"
    );
    assert_eq!(
        eval(
            "convert_tz",
            vec![timestamps(vec![None]), text(vec![Some("UTC")]), boolean],
            micro_type()
        )
        .unwrap_err(),
        "convert_tz expects string"
    );
}

#[test]
fn legacy_calendar_convert_tz_baseline_raw_temporal_and_requested_carrier_projection() {
    for source in [
        dates(vec![Some(0)]),
        text(vec![Some("1970-01-01")]),
        timestamps(vec![Some("1970-01-01 00:00:00")]),
    ] {
        assert_micros(
            eval(
                "convert_tz",
                vec![
                    Arc::clone(&source),
                    text(vec![Some("UTC")]),
                    text(vec![Some("+01:00")]),
                ],
                DataType::Timestamp(TimeUnit::Second, None),
            )
            .unwrap(),
            vec![Some(3600)],
        );
        assert_micros(
            eval(
                "convert_tz",
                vec![source, text(vec![Some("UTC")]), text(vec![Some("+01:00")])],
                DataType::Date32,
            )
            .unwrap(),
            vec![None],
        );
    }
}
