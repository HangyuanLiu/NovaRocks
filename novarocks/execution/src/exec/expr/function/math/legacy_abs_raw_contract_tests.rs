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

//! Independent original raw ABS oracles, before shared computation extraction.
use super::abs::eval_abs;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, NullArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;
use novarocks_types::{SlotId, largeint};
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

fn raw(value: ArrayRef, output: Option<DataType>) -> Result<ArrayRef, String> {
    let rows = value.len();
    let (mut arena, args, chunk) = fixture(vec![value], rows);
    let expr = output.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Abs,
                args: args.clone(),
            },
            ty,
        )
    });
    eval_abs(&arena, expr, args[0], &chunk)
}
#[test]
fn legacy_abs_raw_same_width_minima_preserve_complete_old_error() {
    for (value, ty) in [
        (
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None])) as ArrayRef,
            DataType::Int8,
        ),
        (
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None])),
            DataType::Int16,
        ),
        (
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None])),
            DataType::Int32,
        ),
        (
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
            DataType::Int64,
        ),
    ] {
        assert_eq!(
            raw(value, Some(ty.clone())).unwrap_err(),
            format!("abs overflow on {ty:?} minimum; FE should promote result type")
        );
    }
    assert_eq!(
        raw(Arc::new(Int8Array::from(vec![-1])), None).unwrap_err(),
        "abs: missing output type"
    );
    assert_eq!(
        raw(Arc::new(Int8Array::from(vec![-1])), Some(DataType::Boolean)).unwrap_err(),
        "abs: unsupported output type from FE plan: Boolean"
    );
}
#[test]
fn legacy_abs_raw_plan_cast_and_largeint_null_and_twos_complement_are_unchanged() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("-7"),
        Some("bad"),
        Some("127"),
        None,
    ]));
    let out = raw(strings, Some(DataType::Int8)).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), None, Some(127), None]
    );
    let null: ArrayRef = Arc::new(NullArray::new(3));
    let out = raw(null, Some(DataType::FixedSizeBinary(16))).unwrap();
    assert_eq!(out.null_count(), 3);
    assert_eq!(out.data_type(), &DataType::FixedSizeBinary(16));
    let source =
        largeint::array_from_i128(&[Some(i128::MIN), Some(i128::MAX), Some(-1), None]).unwrap();
    let out = raw(source, Some(DataType::FixedSizeBinary(16))).unwrap();
    let arr = largeint::as_fixed_size_binary_array(&out, "oracle").unwrap();
    assert_eq!(largeint::value_at(arr, 0).unwrap(), i128::MIN);
    assert_eq!(largeint::value_at(arr, 1).unwrap(), i128::MAX);
    assert_eq!(largeint::value_at(arr, 2).unwrap(), 1);
    assert!(arr.is_null(3));
}
#[test]
fn legacy_abs_raw_float_abs_clears_sign_preserving_nan_payload_and_infinity() {
    let bits = [
        0x8000000000000000u64,
        0xfff0000000000000,
        0xfff8000000000123,
        0x7ff8000000000456,
        0xbff4000000000000,
    ];
    let src: ArrayRef = Arc::new(Float64Array::from(bits.map(f64::from_bits).to_vec()));
    let out = raw(src, Some(DataType::Float64)).unwrap();
    let a = out.as_any().downcast_ref::<Float64Array>().unwrap();
    for (row, bits) in bits.iter().enumerate() {
        assert_eq!(a.value(row).to_bits(), bits & 0x7fffffffffffffff);
    }
    let bits = [
        0x80000000u32,
        0xff800000,
        0xffc00123,
        0x7fc00456,
        0xbfa00000,
    ];
    let src: ArrayRef = Arc::new(Float32Array::from(bits.map(f32::from_bits).to_vec()));
    let out = raw(src, Some(DataType::Float32)).unwrap();
    let a = out.as_any().downcast_ref::<Float32Array>().unwrap();
    for (row, bits) in bits.iter().enumerate() {
        assert_eq!(a.value(row).to_bits(), bits & 0x7fffffff);
    }
}
#[test]
fn legacy_abs_raw_decimal128_256_precision_metadata_and_minimum_errors_are_original() {
    let src: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(-123), None])
            .with_precision_and_scale(9, -2)
            .unwrap(),
    );
    let out = raw(src, Some(DataType::Decimal128(9, -2))).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(123), None]
    );
    let src: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(i128::MIN)])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    assert_eq!(
        raw(src, Some(DataType::Decimal128(38, 0))).unwrap_err(),
        "abs overflow on Decimal128 minimum"
    );
    let src: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::from_i128(-123)), None])
            .with_precision_and_scale(76, 2)
            .unwrap(),
    );
    let out = raw(src, Some(DataType::Decimal256(76, 2))).unwrap();
    assert_eq!(
        out.as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(i256::from_i128(123)), None]
    );
    let src: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::MIN)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    );
    assert_eq!(
        raw(src, Some(DataType::Decimal256(76, 0))).unwrap_err(),
        "abs overflow on Decimal256 minimum"
    );
}
