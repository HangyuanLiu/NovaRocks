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

//! Additional raw ROUND digits/carrier boundaries before computation extraction.
use super::function::math::eval_round;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int32Array, Int64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn fixture(inputs: Vec<ArrayRef>, rows: usize) -> (ExprArena, Vec<ExprId>, Chunk) {
    let inputs = if inputs.is_empty() {
        vec![Arc::new(Int32Array::from(vec![0; rows])) as ArrayRef]
    } else {
        inputs
    };
    let fields = inputs
        .iter()
        .enumerate()
        .map(|(i, v)| Field::new(format!("v{i}"), v.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=inputs.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let types = inputs
        .iter()
        .map(|v| v.data_type().clone())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let mut arena = ExprArena::default();
    let args = slots
        .into_iter()
        .zip(types)
        .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
        .collect();
    (arena, args, Chunk::new_with_chunk_schema(batch, schema))
}

fn raw_round(inputs: Vec<ArrayRef>, output: Option<DataType>) -> Result<ArrayRef, String> {
    let rows = inputs[0].len();
    let (mut arena, args, chunk) = fixture(inputs, rows);
    let expr = output.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Round,
                args: args.clone(),
            },
            ty,
        )
    });
    eval_round(&arena, expr, args[0], args.get(1).copied(), &chunk)
}
fn decimal(values: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}
fn float(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn digits(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn raws(out: &ArrayRef) -> Vec<Option<i128>> {
    out.as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn legacy_round_wide_decimal_digits_are_arrow_safe_null_without_policy_translation() {
    let wide = 10_i128.pow(19);
    let d128 = decimal(vec![Some(0), Some(wide), None], 0);
    let d256: ArrayRef = Arc::new(
        Decimal256Array::from(vec![
            Some(arrow_buffer::i256::from_i128(0)),
            Some(arrow_buffer::i256::from_i128(wide)),
            None,
        ])
        .with_precision_and_scale(76, 0)
        .unwrap(),
    );
    for d in [d128, d256] {
        let output =
            raw_round(vec![float(vec![Some(1.25); 3]), d], Some(DataType::Float64)).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1.0), None, None]
        );
    }
}
#[test]
fn legacy_round_full_static_decimal_digits_error_precedes_null_mask() {
    // RecordBatch requires equal column lengths, so this legal fixture freezes
    // the actual static cast failure before NULL masking. The legacy length
    // guard remains source evidence, not an artificially malformed Chunk.
    let input = decimal(vec![None; 3], -39);
    let expected = arrow::compute::cast(&input, &DataType::Int64).unwrap_err();
    let output = raw_round(vec![float(vec![None; 3]), input], Some(DataType::Float64)).unwrap_err();
    assert_eq!(
        output,
        format!("round: failed to cast decimals to Int64: {expected}")
    );
}
#[test]
fn legacy_round_raw_slice_and_empty_keep_original_decimal_carrier() {
    let source = decimal(vec![Some(999), Some(155), None, Some(-155), Some(999)], 2);
    let d = digits(vec![Some(1); 5]);
    let output = raw_round(
        vec![source.slice(1, 3), d.slice(1, 3)],
        Some(DataType::Decimal128(38, 2)),
    )
    .unwrap();
    assert_eq!(raws(&output), vec![Some(160), None, Some(-160)]);
    let empty = raw_round(
        vec![source.slice(0, 0), d.slice(0, 0)],
        Some(DataType::Decimal128(38, 2)),
    )
    .unwrap();
    assert!(raws(&empty).is_empty());
    assert_eq!(empty.data_type(), &DataType::Decimal128(38, 2));
}
