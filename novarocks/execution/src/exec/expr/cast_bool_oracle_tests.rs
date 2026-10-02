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

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprNode};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;

fn legacy(
    source: ArrayRef,
    target: DataType,
    allow: bool,
    policy: DecimalOverflowPolicy,
) -> ArrayRef {
    let source_type = FunctionValueType::new(source.data_type().clone(), true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            source_type.try_to_field("physical_source").unwrap(),
        ])),
        vec![source],
    )
    .unwrap();
    let layout =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(17)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, layout);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), source_type.data_type);
    let cast = arena.push_typed(ExprNode::Cast(child, policy), target.clone());
    let frozen = arena.into_immutable().unwrap();
    let output = ExprArena::from_immutable(&frozen)
        .unwrap()
        .eval(cast, &chunk)
        .unwrap();
    assert_eq!(output.data_type(), &target);
    output
}
fn modes() -> impl Iterator<Item = (bool, DecimalOverflowPolicy)> {
    [false, true].into_iter().flat_map(|allow| {
        [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ]
        .into_iter()
        .map(move |policy| (allow, policy))
    })
}
fn booleans(array: &ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn legacy_bool_cast_zero_one_identity_and_sliced_nulls_are_independent_of_both_policies() {
    let source: ArrayRef = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        Some(true),
        None,
        Some(false),
    ]));
    let source = source.slice(1, 3);
    for (allow, policy) in modes() {
        for target in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ] {
            let output = legacy(source.clone(), target.clone(), allow, policy);
            assert_eq!(output.len(), 3);
            assert!(output.is_null(2));
            macro_rules! integer {
                ($ty:ty) => {{
                    let array = output.as_any().downcast_ref::<$ty>().unwrap();
                    assert_eq!(array.value(0), 0);
                    assert_eq!(array.value(1), 1);
                }};
            }
            match target {
                DataType::Boolean => {
                    assert_eq!(booleans(&output), vec![Some(false), Some(true), None])
                }
                DataType::Int8 => integer!(Int8Array),
                DataType::Int16 => integer!(Int16Array),
                DataType::Int32 => integer!(Int32Array),
                DataType::Int64 => integer!(Int64Array),
                DataType::Float32 => {
                    let array = output.as_any().downcast_ref::<Float32Array>().unwrap();
                    assert_eq!(array.value(0).to_bits(), 0);
                    assert_eq!(array.value(1).to_bits(), 0x3f800000);
                }
                DataType::Float64 => {
                    let array = output.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(array.value(0).to_bits(), 0);
                    assert_eq!(array.value(1).to_bits(), 0x3ff0000000000000);
                }
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn legacy_bool_cast_signed_extrema_and_negative_values_are_nonzero_not_sign_tests() {
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![
            Some(9),
            Some(i8::MIN),
            Some(0),
            None,
            Some(i8::MAX),
            Some(-1),
            Some(9),
        ])),
        Arc::new(Int16Array::from(vec![
            Some(9),
            Some(i16::MIN),
            Some(0),
            None,
            Some(i16::MAX),
            Some(-1),
            Some(9),
        ])),
        Arc::new(Int32Array::from(vec![
            Some(9),
            Some(i32::MIN),
            Some(0),
            None,
            Some(i32::MAX),
            Some(-1),
            Some(9),
        ])),
        Arc::new(Int64Array::from(vec![
            Some(9),
            Some(i64::MIN),
            Some(0),
            None,
            Some(i64::MAX),
            Some(-1),
            Some(9),
        ])),
    ];
    for source in sources {
        for (allow, policy) in modes() {
            let output = legacy(source.slice(1, 5), DataType::Boolean, allow, policy);
            assert_eq!(
                booleans(&output),
                vec![Some(true), Some(false), None, Some(true), Some(true)]
            );
        }
    }
}

#[test]
fn legacy_bool_cast_nan_infinity_and_subnormals_are_true_but_both_zero_signs_are_false() {
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Float32Array::from(vec![
            Some(9.0),
            Some(0.0),
            Some(-0.0),
            Some(f32::from_bits(0x7fc12345)),
            Some(f32::from_bits(0xffc54321)),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(1)),
            Some(f32::from_bits(0x80000001)),
            None,
            Some(9.0),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(9.0),
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8123456789abc)),
            Some(f64::from_bits(0xfff8543212345678)),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(1)),
            Some(f64::from_bits(0x8000000000000001)),
            None,
            Some(9.0),
        ])),
    ];
    for source in sources {
        for (allow, policy) in modes() {
            let output = legacy(source.slice(1, 9), DataType::Boolean, allow, policy);
            assert_eq!(
                booleans(&output),
                vec![
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(true),
                    None
                ]
            );
        }
    }
}
