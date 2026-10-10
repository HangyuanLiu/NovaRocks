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

//! Immutable pre-extraction v1 clock-free Unix conversion behavioral oracles.
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
fn assert_micros(output: ArrayRef, values: Vec<Option<i64>>) {
    assert_eq!(output.data_type(), &micro_type());
    assert_eq!(
        output.to_data(),
        TimestampMicrosecondArray::from(values).to_data()
    );
}
fn assert_seconds(output: ArrayRef, values: Vec<Option<i64>>) {
    assert_eq!(output.data_type(), &DataType::Int64);
    assert_eq!(output.to_data(), Int64Array::from(values).to_data());
}

#[test]
fn legacy_calendar_unix_baseline_argument_forms_are_utc_and_floor_negative_fractions() {
    assert_seconds(
        eval(
            "unix_timestamp",
            vec![dates(vec![Some(0), Some(-1), Some(1), None])],
            DataType::Int64,
        )
        .unwrap(),
        vec![Some(0), Some(-86400), Some(86400), None],
    );
    let source: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(0),
        Some(-1),
        Some(-999999),
        Some(-1000000),
        Some(-1000001),
        None,
    ]));
    assert_seconds(
        eval("unix_timestamp", vec![source], DataType::Int64).unwrap(),
        vec![Some(0), Some(-1), Some(-1), Some(-1), Some(-2), None],
    );
    assert_seconds(
        eval(
            "unix_timestamp",
            vec![text(vec![
                Some("1970-01-01"),
                Some("1969-12-31 23:59:59.999999"),
                Some("0000-01-01"),
                Some("9999-12-31 23:59:59"),
                Some("invalid"),
                None,
            ])],
            DataType::Int64,
        )
        .unwrap(),
        vec![
            Some(0),
            Some(-1),
            Some(-62_167_219_200),
            Some(253_402_300_799),
            None,
            None,
        ],
    );
}

#[test]
fn legacy_calendar_unix_baseline_argument_type_error_is_original_full_carrier_diagnostic() {
    let source: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        eval("unix_timestamp", vec![source], DataType::Int64).unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
}

#[test]
fn legacy_calendar_unix_baseline_ntz_seconds_and_starrocks_year_projection_boundaries() {
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![ints(vec![Some(0), Some(-1), Some(1), Some(i64::MAX), None])],
            micro_type(),
        )
        .unwrap(),
        vec![Some(0), Some(-1_000_000), Some(1_000_000), None, None],
    );
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![ints(vec![
                Some(-62_167_219_200),
                Some(-62_167_219_201),
                Some(253_402_300_799),
                Some(253_402_300_800),
            ])],
            micro_type(),
        )
        .unwrap(),
        vec![
            Some(-62_167_219_200_000_000),
            None,
            Some(253_402_300_799_000_000),
            None,
        ],
    );
}

#[test]
fn legacy_calendar_unix_baseline_ntz_epoch_scales_use_euclidean_split() {
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![
                ints(vec![
                    Some(-1),
                    Some(-1),
                    Some(-1),
                    Some(-1001),
                    Some(1001),
                    Some(1),
                    Some(1),
                    Some(1),
                    None,
                ]),
                ints(vec![
                    Some(0),
                    Some(3),
                    Some(6),
                    Some(3),
                    Some(3),
                    Some(-1),
                    Some(9),
                    None,
                    Some(0),
                ]),
            ],
            micro_type(),
        )
        .unwrap(),
        vec![
            Some(-1_000_000),
            Some(-1000),
            Some(-1),
            Some(-1_001_000),
            Some(1_001_000),
            None,
            None,
            None,
            None,
        ],
    );
}

#[test]
fn legacy_calendar_unix_baseline_ntz_one_argument_temporal_raw_profiles_remain_temporal() {
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![dates(vec![Some(0), Some(-1), None])],
            micro_type(),
        )
        .unwrap(),
        vec![Some(0), Some(-86_400_000_000), None],
    );
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![text(vec![
                Some("1970-01-01 00:00:01"),
                Some("1"),
                Some("invalid"),
                None,
            ])],
            micro_type(),
        )
        .unwrap(),
        vec![Some(1_000_000), None, None, None],
    );
    let source: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(-1),
        Some(1),
        None,
    ]));
    assert_micros(
        eval("to_datetime_ntz", vec![source], micro_type()).unwrap(),
        vec![Some(-1), Some(1), None],
    );
}

#[test]
fn legacy_calendar_unix_baseline_ntz_raw_interval_profiles_and_error_text_remain_original() {
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![Arc::new(Float64Array::from(vec![
                1.9,
                f64::NAN,
                f64::INFINITY,
            ]))],
            micro_type(),
        )
        .unwrap(),
        vec![Some(1_000_000), None, None],
    );
    let value: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    assert_eq!(
        eval("to_datetime_ntz", vec![Arc::clone(&value)], micro_type()).unwrap_err(),
        "to_datetime expects int"
    );
    assert_eq!(
        eval(
            "to_datetime_ntz",
            vec![ints(vec![Some(1)]), value],
            micro_type()
        )
        .unwrap_err(),
        "to_datetime expects int"
    );
}

#[test]
fn legacy_calendar_unix_baseline_ntz_requested_seconds_and_date32_keep_microsecond_carrier() {
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![ints(vec![Some(1), Some(-1), None])],
            DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap(),
        vec![Some(1), Some(-1), None],
    );
    assert_micros(
        eval(
            "to_datetime_ntz",
            vec![ints(vec![Some(1), Some(-1), None])],
            DataType::Date32,
        )
        .unwrap(),
        vec![None, None, None],
    );
}
