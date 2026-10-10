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
use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use novarocks_types::SlotId;
use std::sync::Arc;

fn legacy(
    source: ArrayRef,
    nullable: bool,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> ArrayRef {
    // These roots are explicitly Physical. The old arena is a numeric oracle,
    // not evidence that a carrier establishes any nominal identity.
    let source_type = FunctionValueType::new(source.data_type().clone(), nullable);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            source_type.try_to_field("source").unwrap(),
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
    assert_eq!(frozen.allow_throw_exception(), allow);
    let thawed = ExprArena::from_immutable(&frozen).unwrap();
    assert_eq!(thawed.decimal_overflow_policy(cast), Some(policy));
    let result = thawed.eval(cast, &chunk).unwrap();
    assert_eq!(result.data_type(), &target);
    result
}

fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}

fn f32_source(bits: &[Option<u32>]) -> ArrayRef {
    let mut padded = vec![Some(19.0)];
    padded.extend(bits.iter().map(|v| v.map(f32::from_bits)));
    padded.push(Some(-19.0));
    let array: ArrayRef = Arc::new(Float32Array::from(padded));
    array.slice(1, bits.len())
}

fn f64_source(bits: &[Option<u64>]) -> ArrayRef {
    let mut padded = vec![Some(19.0)];
    padded.extend(bits.iter().map(|v| v.map(f64::from_bits)));
    padded.push(Some(-19.0));
    let array: ArrayRef = Arc::new(Float64Array::from(padded));
    array.slice(1, bits.len())
}

fn f32_bits(array: &ArrayRef) -> Vec<Option<u32>> {
    array
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .iter()
        .map(|v| v.map(f32::to_bits))
        .collect()
}

fn f64_bits(array: &ArrayRef) -> Vec<Option<u64>> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|v| v.map(f64::to_bits))
        .collect()
}

#[test]
fn legacy_float_identity_preserves_every_original_nan_payload_sign_zero_and_slice_bit() {
    let small = [
        Some(0),
        Some(0x80000000),
        Some(1),
        Some(0x80000001),
        Some(0x7f800000),
        Some(0xff800000),
        Some(0x7fc12345),
        Some(0xffc54321),
        Some(0x7f812345),
        Some(0xff812345),
        None,
    ];
    let large = [
        Some(0),
        Some(0x8000000000000000),
        Some(1),
        Some(0x8000000000000001),
        Some(0x7ff0000000000000),
        Some(0xfff0000000000000),
        Some(0x7ff8123456789abc),
        Some(0xfff8543212345678),
        Some(0x7ff0123456789abc),
        Some(0xfff0123456789abc),
        None,
    ];
    for policy in policies() {
        for allow in [false, true] {
            assert_eq!(
                f32_bits(&legacy(
                    f32_source(&small),
                    true,
                    DataType::Float32,
                    policy,
                    allow
                )),
                small
            );
            assert_eq!(
                f64_bits(&legacy(
                    f64_source(&large),
                    true,
                    DataType::Float64,
                    policy,
                    allow
                )),
                large
            );
        }
    }
}

#[test]
fn legacy_float_cross_width_uses_native_rounding_preserves_infinity_and_does_not_sanitize() {
    let small = [
        Some(0),
        Some(0x80000000),
        Some(1),
        Some(0x00800000),
        Some(0x3f800000),
        Some(0x7f7fffff),
        Some(0x7f800000),
        Some(0xff800000),
        None,
    ];
    let widened = [
        Some(0),
        Some(0x8000000000000000),
        Some(0x36a0000000000000),
        Some(0x3810000000000000),
        Some(0x3ff0000000000000),
        Some(0x47efffffe0000000),
        Some(0x7ff0000000000000),
        Some(0xfff0000000000000),
        None,
    ];
    let large = [
        Some(0),
        Some(0x8000000000000000),
        Some(1),
        Some(0x8000000000000001),
        Some(0x36a0000000000000),
        Some(0x3690000000000000),
        Some(0x36a8000000000000),
        Some(0x3ff0000010000000),
        Some(0x3ff0000030000000),
        Some(0x7fefffffffffffff),
        Some(0xffefffffffffffff),
        Some(0x7ff0000000000000),
        Some(0xfff0000000000000),
        None,
    ];
    let narrowed = [
        Some(0),
        Some(0x80000000),
        Some(0),
        Some(0x80000000),
        Some(1),
        Some(0),
        Some(2),
        Some(0x3f800000),
        Some(0x3f800002),
        Some(0x7f800000),
        Some(0xff800000),
        Some(0x7f800000),
        Some(0xff800000),
        None,
    ];
    for policy in policies() {
        for allow in [false, true] {
            assert_eq!(
                f64_bits(&legacy(
                    f32_source(&small),
                    true,
                    DataType::Float64,
                    policy,
                    allow
                )),
                widened
            );
            assert_eq!(
                f32_bits(&legacy(
                    f64_source(&large),
                    true,
                    DataType::Float32,
                    policy,
                    allow
                )),
                narrowed
            );
            // Cross-width NaN payloads follow this platform's native conversion.
            // Unlike identity, their exact payload is not a portable fixed oracle.
            let nan32 = [0x7fc12345, 0xffc54321, 0x7f812345, 0xff812345];
            let actual64 = legacy(
                f32_source(&nan32.map(Some)),
                false,
                DataType::Float64,
                policy,
                allow,
            );
            for (row, original) in nan32.iter().enumerate() {
                let expected = f32::from_bits(*original) as f64;
                assert!(expected.is_nan());
                assert_eq!(f64_bits(&actual64)[row], Some(expected.to_bits()));
            }
            let nan64 = [
                0x7ff8123456789abc,
                0xfff8543212345678,
                0x7ff0123456789abc,
                0xfff0123456789abc,
            ];
            let actual32 = legacy(
                f64_source(&nan64.map(Some)),
                false,
                DataType::Float32,
                policy,
                allow,
            );
            for (row, original) in nan64.iter().enumerate() {
                let expected = f64::from_bits(*original) as f32;
                assert!(expected.is_nan());
                assert_eq!(f32_bits(&actual32)[row], Some(expected.to_bits()));
            }
        }
    }
}

#[test]
fn legacy_all_four_float_profiles_keep_nonnullable_success_even_when_finite_narrowing_overflows() {
    let small = [Some(0x80000000), Some(0x7f7fffff), Some(0x7f800000)];
    let large = [
        Some(0x8000000000000000),
        Some(0x7fefffffffffffff),
        Some(0x7ff0000000000000),
    ];
    for policy in policies() {
        for allow in [false, true] {
            for source in [f32_source(&small), f64_source(&large)] {
                for target in [DataType::Float32, DataType::Float64] {
                    let output = legacy(source.clone(), false, target, policy, allow);
                    assert_eq!(output.null_count(), 0);
                    assert_eq!(output.len(), 3);
                    match output.data_type() {
                        DataType::Float32 => assert_eq!(f32_bits(&output)[0], Some(0x80000000)),
                        DataType::Float64 => {
                            assert_eq!(f64_bits(&output)[0], Some(0x8000000000000000))
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
}
