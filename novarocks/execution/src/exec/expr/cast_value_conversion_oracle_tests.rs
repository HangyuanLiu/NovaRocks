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
use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Float32Array, Int64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
use novarocks_types::SlotId;
use std::sync::Arc;

fn legacy(
    source: ArrayRef,
    target: DataType,
    allow: bool,
    policy: DecimalOverflowPolicy,
) -> ArrayRef {
    let source_type = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
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
    // The complete input layout authorizes LargeInt; bytes alone do not.
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

fn source(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    builder.append_value(29_i128.to_be_bytes()).unwrap();
    for value in values {
        if let Some(value) = value {
            builder.append_value(value.to_be_bytes()).unwrap();
        } else {
            builder.append_null();
        }
    }
    builder.append_value((-29_i128).to_be_bytes()).unwrap();
    let array: ArrayRef = Arc::new(builder.finish());
    array.slice(1, values.len())
}

#[test]
fn legacy_value_conversion_largeint_f32_keeps_direct_integer_rounding_and_successful_nulls() {
    // IEEE expectations come from integer exponent/significand rounding.
    // The two midpoint-plus-one inputs detect an intermediate f64 conversion.
    let values = [
        Some(i128::MIN),
        Some(i128::MAX),
        Some(-1),
        Some(0),
        Some(1),
        Some((1_i128 << 80) + (1_i128 << 56) + 1),
        Some((1_i128 << 100) + (1_i128 << 76) + 1),
        None,
    ];
    let expected = [
        Some(0xff00_0000),
        Some(0x7f00_0000),
        Some(0xbf80_0000),
        Some(0),
        Some(0x3f80_0000),
        Some(0x6780_0001),
        Some(0x7180_0001),
        None,
    ];
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let result = legacy(source(&values), DataType::Float32, allow, policy);
            let actual = result
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(f32::to_bits))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn legacy_value_conversion_largeint_signed_overflow_is_null_for_both_frozen_policies() {
    let values = [
        Some(i128::MIN),
        Some(i64::MIN as i128),
        Some(-1),
        Some(0),
        Some(i64::MAX as i128),
        Some(i128::MAX),
        None,
    ];
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let result = legacy(source(&values), DataType::Int64, allow, policy);
            let actual = result
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                [
                    None,
                    Some(i64::MIN),
                    Some(-1),
                    Some(0),
                    Some(i64::MAX),
                    None,
                    None
                ]
            );
        }
    }
}
