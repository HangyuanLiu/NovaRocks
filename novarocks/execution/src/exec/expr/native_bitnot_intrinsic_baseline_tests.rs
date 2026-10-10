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
//! Original native Unary(BitwiseNot) decodes to this existing bitnot arena call.
//! This fixture freezes original typed carriers before the selected consumer exists.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode, function};
use arrow::array::{
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, NullArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::{SlotId, largeint};
use std::sync::Arc;
fn original(input: ArrayRef, nullable: bool) -> Result<ArrayRef, String> {
    let dtype = input.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "operand",
            dtype.clone(),
            nullable,
        )])),
        vec![input],
    )
    .unwrap();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(17)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, cs);
    let mut arena = ExprArena::default();
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), dtype.clone());
    // Native Adapter's original unary.rs uses this exact lookup and one argument.
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind: function::lookup_function("bitnot").unwrap(),
            args: vec![child],
        },
        dtype,
    );
    arena.eval(root, &chunk)
}
#[test]
fn native_bitnot_original_signed_all_widths_nullable_nonnullable_slices_and_empty() {
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int8Array::from(vec![i8::MAX, 0, -1, -2, i8::MIN])),
        ),
        (
            Arc::new(Int16Array::from(vec![i16::MIN, -1, 0, 1, i16::MAX])),
            Arc::new(Int16Array::from(vec![i16::MAX, 0, -1, -2, i16::MIN])),
        ),
        (
            Arc::new(Int32Array::from(vec![i32::MIN, -1, 0, 1, i32::MAX])),
            Arc::new(Int32Array::from(vec![i32::MAX, 0, -1, -2, i32::MIN])),
        ),
        (
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(Int64Array::from(vec![i64::MAX, 0, -1, -2, i64::MIN])),
        ),
    ];
    for (input, expected) in cases {
        for nullable in [false, true] {
            assert_eq!(
                original(input.clone(), nullable).unwrap().to_data(),
                expected.to_data()
            );
            assert_eq!(
                original(input.slice(1, 3), nullable).unwrap().to_data(),
                expected.slice(1, 3).to_data()
            );
            assert_eq!(original(input.slice(1, 0), nullable).unwrap().len(), 0);
        }
    }
    let input: ArrayRef = Arc::new(Int8Array::from(vec![Some(-1), None, Some(0)]));
    let expected: ArrayRef = Arc::new(Int8Array::from(vec![Some(0), None, Some(-1)]));
    assert_eq!(original(input, true).unwrap().to_data(), expected.to_data());
}
#[test]
fn native_bitnot_original_unsigned_full_values_produce_null_after_output_cast() {
    let cases: Vec<ArrayRef> = vec![
        Arc::new(UInt8Array::from(vec![0, 1, u8::MAX])),
        Arc::new(UInt16Array::from(vec![0, 1, u16::MAX])),
        Arc::new(UInt32Array::from(vec![0, 1, u32::MAX])),
        Arc::new(UInt64Array::from(vec![
            0,
            1,
            i64::MAX as u64,
            (i64::MAX as u64) + 1,
            u64::MAX,
        ])),
    ];
    for input in cases {
        for nullable in [false, true] {
            let output = original(input.clone(), nullable).unwrap();
            assert_eq!(output.data_type(), input.data_type());
            assert_eq!(output.logical_null_count(), input.len());
            assert_eq!(original(input.slice(0, 0), nullable).unwrap().len(), 0);
        }
    }
}
#[test]
fn native_bitnot_original_largeint_all128_bits_and_nulls() {
    let input = largeint::array_from_i128(&[
        Some(i128::MIN),
        Some(i128::MAX),
        Some((1_i128 << 100) + 5),
        Some(-1),
        Some(0),
        None,
    ])
    .unwrap();
    let expected = largeint::array_from_i128(&[
        Some(i128::MAX),
        Some(i128::MIN),
        Some(-(1_i128 << 100) - 6),
        Some(0),
        Some(-1),
        None,
    ])
    .unwrap();
    assert_eq!(original(input, true).unwrap().to_data(), expected.to_data());
}
#[test]
fn native_bitnot_original_null_target_full_data_error_including_empty() {
    for rows in [0, 1, 3] {
        let input: ArrayRef = Arc::new(NullArray::new(rows));
        assert_eq!(
            original(input, true).unwrap_err(),
            "bitnot: failed to cast output: Cast error: Casting from Int64 to Null not supported"
        );
    }
}

#[path = "native_bitnot_sql_source_baseline_tests.rs"]
mod sql_source_tests;
