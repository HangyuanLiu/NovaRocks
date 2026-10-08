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

//! Pre-extraction oracles for both historical calendar-field input routes.

use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, lookup_function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, NaiveDate};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

fn eval(kind: FunctionKind, array: ArrayRef, result: DataType) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), array.data_type().clone());
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: vec![source],
        },
        result,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "input",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let metadata =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    arena.eval(call, &Chunk::new_with_chunk_schema(batch, metadata))
}
fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn ints(array: &ArrayRef) -> Vec<Option<i32>> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn legacy_calendar_parts_baseline_registration_is_general_date_after_last_writer() {
    assert!(matches!(
        lookup_function("year"),
        Some(FunctionKind::Date("year"))
    ));
    let input = text(vec![
        Some("20240101"),
        Some("2024-02-29"),
        None,
        Some("bad"),
    ]);
    assert_eq!(
        ints(
            &eval(
                lookup_function("year").unwrap(),
                input.clone(),
                DataType::Int32
            )
            .unwrap()
        ),
        vec![Some(2024), Some(2024), None, None]
    );
    assert_eq!(
        ints(&eval(FunctionKind::Year, input, DataType::Int32).unwrap()),
        vec![None, Some(2024), None, None]
    );
}

#[test]
fn legacy_calendar_parts_baseline_all_named_integer_fields_iso_and_leap() {
    for (name, want) in [
        ("year", vec![2016, 2024]),
        ("month", vec![1, 2]),
        ("day", vec![1, 29]),
        ("dayofmonth", vec![1, 29]),
        ("hour", vec![12, 12]),
        ("minute", vec![34, 34]),
        ("second", vec![56, 56]),
        ("dayofweek", vec![6, 5]),
        ("dayofweek_iso", vec![5, 4]),
        ("weekday", vec![4, 3]),
        ("dayofyear", vec![1, 60]),
        ("week", vec![53, 9]),
        ("weekofyear", vec![53, 9]),
        ("yearweek", vec![201553, 202409]),
        ("quarter", vec![1, 1]),
    ] {
        let input = text(vec![
            Some("2016-01-01 12:34:56"),
            Some("2024-02-29 12:34:56"),
            None,
            Some("invalid"),
        ]);
        let got = eval(lookup_function(name).unwrap(), input, DataType::Int32).unwrap();
        assert_eq!(
            ints(&got),
            vec![Some(want[0]), Some(want[1]), None, None],
            "{name}"
        );
    }
}

#[test]
fn legacy_calendar_parts_baseline_names_and_arbitrary_output_cast_are_retained() {
    for (name, want) in [("dayname", "Thursday"), ("monthname", "February")] {
        let got = eval(
            lookup_function(name).unwrap(),
            text(vec![Some("2024-02-29"), None, Some("bad")]),
            DataType::Utf8,
        )
        .unwrap();
        assert_eq!(
            got.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(want), None, None]
        );
    }
    let generic = eval(
        FunctionKind::Date("year"),
        text(vec![Some("2024-02-29")]),
        DataType::Float64,
    )
    .unwrap();
    assert_eq!(
        generic
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2024.0
    );
    let raw = eval(
        FunctionKind::Year,
        text(vec![Some("2024-02-29")]),
        DataType::Float64,
    )
    .unwrap();
    assert_eq!(raw.data_type(), &DataType::Int64);
    assert_eq!(
        raw.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        2024
    );
}

#[test]
fn legacy_calendar_parts_baseline_fractional_negative_epoch_keeps_distinct_raw_projection() {
    let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(-1),
        Some(-1_000_001),
        Some(-1_000_000),
        Some(0),
        None,
    ]));
    assert_eq!(
        ints(&eval(FunctionKind::Year, input.clone(), DataType::Int32).unwrap()),
        vec![Some(1970), Some(1970), Some(1969), Some(1970), None]
    );
    assert_eq!(
        ints(&eval(FunctionKind::Date("year"), input, DataType::Int32).unwrap()),
        vec![Some(1969), Some(1969), Some(1969), Some(1970), None]
    );
}

#[test]
fn legacy_calendar_parts_baseline_invalid_date32_and_original_raw_overflow_panic() {
    let invalid: ArrayRef = Arc::new(Date32Array::from(vec![Some(i32::MIN), None]));
    assert_eq!(
        ints(&eval(FunctionKind::Year, invalid.clone(), DataType::Int32).unwrap()),
        vec![Some(1970), None]
    );
    assert_eq!(
        ints(&eval(FunctionKind::Date("year"), invalid, DataType::Int32).unwrap()),
        vec![None, None]
    );
    if cfg!(debug_assertions) {
        let overflow: ArrayRef = Arc::new(Date32Array::from(vec![Some(i32::MAX)]));
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval(
                FunctionKind::Year,
                overflow,
                DataType::Int32
            )))
            .is_err()
        );
    }
    let date = NaiveDate::from_ymd_opt(40000, 1, 1).unwrap();
    let large: ArrayRef = Arc::new(Date32Array::from(vec![Some(
        date.num_days_from_ce() - 719163,
    )]));
    let output = eval(FunctionKind::Year, large, DataType::Int16).unwrap();
    assert!(output.is_null(0));
}

#[test]
fn legacy_calendar_parts_baseline_full_original_carrier_errors_remain_distinct() {
    let input: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
    assert_eq!(
        eval(FunctionKind::Year, input.clone(), DataType::Int32).unwrap_err(),
        "year: unsupported input type: Boolean"
    );
    assert_eq!(
        eval(FunctionKind::Date("year"), input, DataType::Int32).unwrap_err(),
        "unsupported datetime input type: Boolean"
    );
}

#[test]
fn legacy_calendar_parts_baseline_explicit_year_missing_argument_and_node_error() {
    let mut arena = ExprArena::default();
    let empty = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Year,
            args: vec![],
        },
        DataType::Int32,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let metadata =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, metadata);
    assert_eq!(
        arena.eval(empty, &chunk).unwrap_err(),
        "year expects 1 to 1 arguments, got 0"
    );
    assert_eq!(
        crate::exec::expr::function::eval_year(&arena, empty, &chunk).unwrap_err(),
        "year: missing argument"
    );
    let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
    assert_eq!(
        crate::exec::expr::function::eval_year(&arena, slot, &chunk).unwrap_err(),
        "Year expression node type mismatch"
    );
}
