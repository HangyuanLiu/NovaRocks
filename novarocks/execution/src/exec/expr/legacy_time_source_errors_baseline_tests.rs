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
//! Independent raw deepest-source data-error baselines before typed projection.
use super::{ExprArena, ExprId, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::sync::Arc;
fn setup(columns: Vec<ArrayRef>) -> (ExprArena, Vec<ExprId>, Chunk) {
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("v{i}"), a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut arena = ExprArena::default();
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, a)| arena.push_typed(ExprNode::SlotId(slots[i]), a.data_type().clone()))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    (arena, args, Chunk::new_with_chunk_schema(batch, schema))
}
fn raw(name: &str, columns: Vec<ArrayRef>) -> Result<ArrayRef, String> {
    let (arena, args, chunk) = setup(columns);
    eval_date_function(name, &arena, ExprId(usize::MAX), &args, &chunk)
}
fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    a.as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn identity_cast(arena: &mut ExprArena, input: ExprId, dtype: DataType) -> ExprId {
    arena.push_typed(
        ExprNode::Cast(input, DecimalOverflowPolicy::OutputNull),
        dtype,
    )
}
#[test]
fn original_datetime_out_of_range_is_null_until_deepest_phase_is_demanded() {
    for (dtype, array, expected) in [
        (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![0, i32::MAX])) as ArrayRef,
            "Cast error: Failed to convert 2147483647 to temporal for Date32",
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![0, i64::MAX])) as ArrayRef,
            "Cast error: Failed to convert 9223372036854775807 to datetime for Timestamp(µs)",
        ),
    ] {
        assert_eq!(
            ints(&raw("time_to_sec", vec![array.clone()]).unwrap()),
            vec![Some(0), None]
        );
        let (mut arena, args, chunk) = setup(vec![array]);
        let cast = identity_cast(&mut arena, args[0], dtype);
        assert_eq!(
            eval_date_function("time_to_sec", &arena, ExprId(0), &[cast], &chunk).unwrap_err(),
            expected
        );
    }
}
#[test]
fn original_deepest_error_is_whole_current_invocation_not_only_null_row() {
    let (mut arena, args, chunk) = setup(vec![
        Arc::new(Date32Array::from(vec![Some(0), Some(i32::MAX), None])),
        Arc::new(BooleanArray::from(vec![true, false, false])),
        Arc::new(Int64Array::from(vec![-1, -1, -1])),
    ]);
    let cast = identity_cast(&mut arena, args[0], DataType::Date32);
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date("time_to_sec"),
            args: vec![cast],
        },
        DataType::Int64,
    );
    assert_eq!(
        arena.eval(call, &chunk).unwrap_err(),
        "Cast error: Failed to convert 2147483647 to temporal for Date32"
    );
    let guarded = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::If,
            args: vec![args[1], call, args[2]],
        },
        DataType::Int64,
    );
    assert_eq!(
        ints(&arena.eval(guarded, &chunk).unwrap()),
        vec![Some(0), Some(-1), Some(-1)]
    );
}
#[test]
fn original_binary_deepest_arrow_safe_cast_returns_null_for_invalid_utf8() {
    let (mut arena, args, chunk) = setup(vec![Arc::new(BinaryArray::from(vec![
        Some(&b"12:34:56"[..]),
        Some(&[0xff, 0xfe][..]),
        None,
    ]))]);
    let cast = identity_cast(&mut arena, args[0], DataType::Binary);
    assert_eq!(
        crate::exec::expr::function::date::parse_from_cast_source(&arena, cast, &chunk).unwrap(),
        Some(vec![Some(45296), None, None])
    );
    // Normal admission fails first. It must not be replaced with the successful
    // deepest source computation or with an invented invalid-UTF8 data error.
    assert_eq!(
        eval_date_function("time_to_sec", &arena, ExprId(0), &[cast], &chunk).unwrap_err(),
        "unsupported datetime input type: Binary"
    );
}
#[test]
fn original_unknown_zone_deepest_error_retains_entire_long_string() {
    let zone = "invalid_zone_".repeat(60);
    let source =
        Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]).with_timezone(zone.clone()))
            as ArrayRef;
    let (mut arena, args, chunk) = setup(vec![source]);
    let cast = identity_cast(
        &mut arena,
        args[0],
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone.clone().into())),
    );
    let expected = format!(
        "Parser error: Invalid timezone \"{zone}\": only offset based timezones supported without chrono-tz feature"
    );
    assert!(expected.len() > 512);
    assert_eq!(
        crate::exec::expr::function::date::parse_from_cast_source(&arena, cast, &chunk)
            .unwrap_err(),
        expected
    );
    assert_eq!(
        eval_date_function("time_to_sec", &arena, ExprId(0), &[cast], &chunk).unwrap_err(),
        expected
    );
}
#[test]
fn original_no_null_guard_never_observes_unknown_deepest_zone() {
    let zone = "invalid_zone_".repeat(60);
    let source =
        Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone(zone.clone())) as ArrayRef;
    let (mut arena, args, chunk) = setup(vec![source]);
    let cast = identity_cast(
        &mut arena,
        args[0],
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into())),
    );
    assert_eq!(
        ints(&eval_date_function("time_to_sec", &arena, ExprId(0), &[cast], &chunk).unwrap()),
        vec![Some(0)]
    );
}
