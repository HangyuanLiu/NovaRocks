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
    Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
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
) -> Result<ArrayRef, String> {
    let ty = FunctionValueType::new(source.data_type().clone(), true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![ty.try_to_field("source").unwrap()])),
        vec![source],
    )
    .unwrap();
    let layout =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(17)])
            .unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, layout);
    let mut arena = ExprArena::default();
    arena.set_allow_throw_exception(allow);
    let child = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), ty.data_type);
    let expr = arena.push_typed(ExprNode::Cast(child, policy), target);
    let frozen = arena.into_immutable().unwrap();
    ExprArena::from_immutable(&frozen)
        .unwrap()
        .eval(expr, &chunk)
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
fn unsigned(array: &ArrayRef) -> Vec<Option<u64>> {
    macro_rules! values {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .iter()
                .map(|v| v.map(u64::from))
                .collect()
        };
    }
    match array.data_type() {
        DataType::UInt8 => values!(UInt8Array),
        DataType::UInt16 => values!(UInt16Array),
        DataType::UInt32 => values!(UInt32Array),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("unsigned result"),
    }
}
fn signed(array: &ArrayRef) -> Vec<Option<i64>> {
    macro_rules! values {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap()
                .iter()
                .map(|v| v.map(i64::from))
                .collect()
        };
    }
    match array.data_type() {
        DataType::Int8 => values!(Int8Array),
        DataType::Int16 => values!(Int16Array),
        DataType::Int32 => values!(Int32Array),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("signed result"),
    }
}
#[test]
fn legacy_unsigned_cast_integer_limits_are_safe_null_without_allow_escalation() {
    let source: ArrayRef = Arc::new(UInt64Array::from(vec![
        Some(9),
        Some(0),
        Some(1),
        Some(127),
        Some(128),
        Some(255),
        Some(256),
        Some(32767),
        Some(32768),
        Some(65535),
        Some(65536),
        Some(i64::MAX as u64),
        Some(i64::MAX as u64 + 1),
        Some(u64::MAX),
        None,
        Some(9),
    ]));
    let source = source.slice(1, 14);
    for (allow, policy) in modes() {
        for (target, max) in [
            (DataType::UInt8, u64::from(u8::MAX)),
            (DataType::UInt16, u64::from(u16::MAX)),
            (DataType::UInt32, u64::from(u32::MAX)),
            (DataType::UInt64, u64::MAX),
        ] {
            let result = legacy(source.clone(), target, allow, policy).unwrap();
            let expected = [
                0,
                1,
                127,
                128,
                255,
                256,
                32767,
                32768,
                65535,
                65536,
                i64::MAX as u64,
                i64::MAX as u64 + 1,
                u64::MAX,
            ]
            .into_iter()
            .map(|v| if v <= max { Some(v) } else { None })
            .chain([None])
            .collect::<Vec<_>>();
            assert_eq!(unsigned(&result), expected);
        }
        for (target, max) in [
            (DataType::Int8, i64::from(i8::MAX)),
            (DataType::Int16, i64::from(i16::MAX)),
            (DataType::Int32, i64::from(i32::MAX)),
            (DataType::Int64, i64::MAX),
        ] {
            let result = legacy(source.clone(), target, allow, policy).unwrap();
            let expected = [
                0,
                1,
                127,
                128,
                255,
                256,
                32767,
                32768,
                65535,
                65536,
                i64::MAX as u64,
                i64::MAX as u64 + 1,
                u64::MAX,
            ]
            .into_iter()
            .map(|v| {
                if v <= max as u64 {
                    Some(v as i64)
                } else {
                    None
                }
            })
            .chain([None])
            .collect::<Vec<_>>();
            assert_eq!(signed(&result), expected);
        }
        let negative: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(255),
            Some(256),
            None,
        ]));
        assert_eq!(
            unsigned(&legacy(negative, DataType::UInt8, allow, policy).unwrap()),
            vec![None, None, Some(0), Some(255), None, None]
        );
    }
}
#[test]
fn legacy_unsigned_cast_float_exclusive_bounds_keep_negative_fraction_and_throw_original_range_error()
 {
    for (target, max_p1, previous, previous_integer, name) in [
        (DataType::UInt8, 256.0, 255.75, 255, "TINYINT UNSIGNED"),
        (
            DataType::UInt16,
            65536.0,
            65535.75,
            65535,
            "SMALLINT UNSIGNED",
        ),
        (
            DataType::UInt32,
            4294967296.0,
            4294967295.75,
            4294967295,
            "INT UNSIGNED",
        ),
        (
            DataType::UInt64,
            f64::from_bits(0x43f0000000000000),
            f64::from_bits(0x43efffffffffffff),
            18446744073709549568,
            "BIGINT UNSIGNED",
        ),
    ] {
        let source: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(9.0),
            Some(-0.5),
            Some(-1.0),
            Some(-0.0),
            Some(1.9),
            Some(previous),
            Some(max_p1),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
            Some(9.0),
        ]));
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let result = legacy(source.slice(1, 10), target.clone(), false, policy).unwrap();
            assert_eq!(
                unsigned(&result),
                vec![
                    Some(0),
                    None,
                    Some(0),
                    Some(1),
                    Some(previous_integer),
                    None,
                    None,
                    None,
                    None,
                    None
                ]
            );
            for value in [-1.0, max_p1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                let source: ArrayRef = Arc::new(Float64Array::from(vec![Some(value), None]));
                let error = legacy(source, target.clone(), true, policy).unwrap_err();
                assert!(error.starts_with("Expr evaluate meet error: CAST failed:"));
                assert!(error.contains(&format!("conflict with range of {name}")));
            }
            let source: ArrayRef = Arc::new(Float64Array::from(vec![
                Some(-0.5),
                Some(-0.0),
                Some(previous),
                None,
            ]));
            assert_eq!(
                unsigned(&legacy(source, target.clone(), true, policy).unwrap()),
                vec![Some(0), Some(0), Some(previous_integer), None]
            );
        }
    }
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let source: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(f32::from_bits(0x5f7fffff)),
            Some(f32::from_bits(0x5f800000)),
            Some(-0.5),
            None,
        ]));
        assert_eq!(
            unsigned(&legacy(source, DataType::UInt64, false, policy).unwrap()),
            vec![Some(18446742974197923840), None, Some(0), None]
        );
    }
}
#[test]
fn legacy_unsigned_cast_maximum_float_rounding_and_bool_are_exact_independent_oracles() {
    for (allow, policy) in modes() {
        let source: ArrayRef = Arc::new(UInt64Array::from(vec![
            Some(u64::MAX),
            Some(0),
            Some(1),
            None,
        ]));
        let f32s = legacy(source.clone(), DataType::Float32, allow, policy).unwrap();
        assert_eq!(
            f32s.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(f32::to_bits))
                .collect::<Vec<_>>(),
            vec![Some(0x5f800000), Some(0), Some(0x3f800000), None]
        );
        let f64s = legacy(source.clone(), DataType::Float64, allow, policy).unwrap();
        assert_eq!(
            f64s.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>(),
            vec![
                Some(0x43f0000000000000),
                Some(0),
                Some(0x3ff0000000000000),
                None
            ]
        );
        assert_eq!(
            legacy(source, DataType::Boolean, allow, policy)
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(true), None]
        );
        for target in [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            let source: ArrayRef =
                Arc::new(BooleanArray::from(vec![Some(false), Some(true), None]));
            assert_eq!(
                unsigned(&legacy(source, target, allow, policy).unwrap()),
                vec![Some(0), Some(1), None]
            );
        }
    }
}
