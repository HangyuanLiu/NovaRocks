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
//! Original dispatcher/ExprArena baselines; no pure owner or decoder replacement.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, object::eval_object_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use novarocks_functions::{ConstantPool, FunctionValueType};
use novarocks_type_contract::CompilePhase;
use novarocks_types::{SlotId, value::bitmap};
use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn setup(a: ArrayRef) -> (ExprArena, ExprId, Chunk) {
    let slot = SlotId::new(1);
    let ty = a.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("original", ty.clone(), true)])),
        vec![a],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let id = arena.push_typed(ExprNode::SlotId(slot), ty);
    (arena, id, chunk)
}
fn raw(a: ArrayRef, target: Option<DataType>, tails: usize) -> Result<ArrayRef, String> {
    let (mut arena, id, chunk) = setup(a);
    let mut args = vec![id];
    args.extend(std::iter::repeat_n(ExprId(usize::MAX), tails));
    let expr = target.map_or(ExprId(usize::MAX), |ty| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Object("bitmap_to_string"),
                args: args.clone(),
            },
            ty,
        )
    });
    eval_object_function("bitmap_to_string", &arena, expr, &args, &chunk)
}
fn text(a: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(a.data_type(), &DataType::Utf8);
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
fn array(values: &[Option<Vec<u8>>]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(values.iter().map(|v| v.as_deref())))
}
#[test]
fn legacy_bitmap_to_string_original_all_formats_order_duplicates_and_u64_domain() {
    let values = BTreeSet::from([0, 1, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX]);
    let expected = "0,1,4294967295,4294967296,18446744073709551615";
    for payload in [
        bitmap::encode_internal_bitmap(&values).unwrap(),
        bitmap::encode_external_bitmap(&values).unwrap(),
        bitmap::encode_bitmap_aggregate(&values).unwrap(),
        b" 18446744073709551615,0,1,1,4294967296,4294967295 ".to_vec(),
    ] {
        assert_eq!(
            text(raw(array(&[Some(payload), None]), None, 0).unwrap()),
            vec![Some(expected.into()), None]
        );
    }
    for payload in [vec![], vec![0]] {
        assert_eq!(
            text(raw(array(&[Some(payload)]), None, 0).unwrap()),
            vec![Some("".into())]
        );
    }
    for value in [0, u32::MAX as u64, u32::MAX as u64 + 1, u64::MAX] {
        assert_eq!(
            text(raw(array(&[Some(bitmap::encode_bitmap_single(value))]), None, 0).unwrap()),
            vec![Some(value.to_string())]
        );
    }
}
#[test]
fn legacy_bitmap_to_string_original_roaring_and_v2_tags() {
    for values in [
        (0u32..96).map(u64::from).collect::<BTreeSet<_>>(),
        (0..96).map(|v| (1u64 << 32) + v).collect::<BTreeSet<_>>(),
    ] {
        let expected = values
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let payload = bitmap::encode_external_bitmap(&values).unwrap();
        assert!(matches!(
            payload[0],
            bitmap::BITMAP_TYPE_BITMAP32 | bitmap::BITMAP_TYPE_BITMAP64
        ));
        let mut v2 = payload.clone();
        v2[0] = if payload[0] == bitmap::BITMAP_TYPE_BITMAP32 {
            bitmap::BITMAP_TYPE_BITMAP32_SERIV2
        } else {
            bitmap::BITMAP_TYPE_BITMAP64_SERIV2
        };
        for bytes in [payload, v2] {
            assert_eq!(
                text(raw(array(&[Some(bytes)]), None, 0).unwrap()),
                vec![Some(expected.clone())]
            );
        }
    }
}
#[test]
fn legacy_bitmap_to_string_original_null_mask_slice_and_zero_rows() {
    let a = Arc::new(BinaryArray::new(
        OffsetBuffer::new(vec![0i32, 3, 4, 4].into()),
        Buffer::from(b"bad\0".as_slice()),
        Some(NullBuffer::from(vec![false, true, true])),
    )) as ArrayRef;
    assert_eq!(
        text(raw(a.clone(), None, 0).unwrap()),
        vec![None, Some("".into()), Some("".into())]
    );
    assert_eq!(
        text(raw(a.slice(1, 2), None, 0).unwrap()),
        vec![Some("".into()), Some("".into())]
    );
    assert!(raw(a.slice(0, 0), None, 0).unwrap().is_empty());
    for n in [0, 1, 4] {
        assert_eq!(
            text(raw(Arc::new(NullArray::new(n)), None, 0).unwrap()),
            vec![None; n]
        );
    }
}
#[test]
fn legacy_bitmap_to_string_original_full_error_and_first_row_order() {
    let long = "雪".repeat(600);
    for token in ["bad", "-1", "18446744073709551616", long.as_str()] {
        let expected = format!("bitmap string contains invalid value: {token}");
        assert_eq!(
            raw(
                array(&[
                    Some(token.as_bytes().to_vec()),
                    Some(b"later-error".to_vec())
                ]),
                None,
                0
            )
            .unwrap_err(),
            expected
        );
        if token == long {
            assert!(expected.len() > 512);
        }
    }
    assert_eq!(
        raw(array(&[Some(vec![0xff])]), None, 0).unwrap_err(),
        "bitmap payload is not utf8 text"
    );
    assert_eq!(
        raw(array(&[None, Some(b"later-error".to_vec())]), None, 0).unwrap_err(),
        "bitmap string contains invalid value: later-error"
    );
}
#[test]
fn legacy_bitmap_to_string_original_downcast_before_empty_and_null_mask() {
    let nested = DataType::Struct(
        (0..80)
            .map(|i| Field::new(format!("original_full_field_{i}"), DataType::Utf8, true))
            .collect(),
    );
    for ty in [
        DataType::Utf8,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
        DataType::Int32,
        nested,
    ] {
        let expected = format!("bitmap_to_string expects BITMAP/BINARY input, got {ty:?}");
        for n in [0, 1] {
            assert_eq!(raw(new_null_array(&ty, n), None, 0).unwrap_err(), expected);
        }
        if matches!(ty, DataType::Struct(_)) {
            assert!(expected.len() > 512);
        }
    }
}
#[test]
fn legacy_bitmap_to_string_original_actual_constant_broadcast_and_nonzero_pool() {
    let (_, _, chunk) = setup(Arc::new(Int32Array::from(vec![0, 0, 0, 0])));
    let ty = FunctionValueType::new(DataType::Binary, false);
    let backing = array(&[Some(vec![0]), Some(b"7,1,7".to_vec())]);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("constant").unwrap()),
        ty,
        backing.to_data(),
        super::pure_differential::constant_policy(),
        CompilePhase::FunctionSpecialization,
        &super::pure_differential::HarnessControl,
    )
    .unwrap();
    assert_eq!(pool.value(1).unwrap().ordinal(), 1);
    for node in [
        ExprNode::Constant(pool.value(1).unwrap()),
        ExprNode::Literal(LiteralValue::Binary(b"7,1,7".to_vec())),
    ] {
        let mut arena = ExprArena::default();
        let id = arena.push_typed(node, DataType::Binary);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Object("bitmap_to_string"),
                args: vec![id],
            },
            DataType::Utf8,
        );
        assert_eq!(
            text(arena.eval(expr, &chunk).unwrap()),
            vec![Some("1,7".into()); 4]
        );
    }
}
#[test]
fn legacy_bitmap_to_string_original_raw_demand_target_and_production_arity() {
    for target in [
        None,
        Some(DataType::Utf8),
        Some(DataType::Float64),
        Some(DataType::Null),
    ] {
        assert_eq!(
            text(raw(array(&[Some(b"7,1".to_vec())]), target, 4).unwrap()),
            vec![Some("1,7".into())]
        );
    }
    let (mut arena, id, chunk) = setup(array(&[Some(vec![0])]));
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Object("bitmap_to_string"),
            args: vec![id, ExprId(usize::MAX)],
        },
        DataType::Utf8,
    );
    assert_eq!(
        arena.eval(expr, &chunk).unwrap_err(),
        "bitmap_to_string expects 1 to 1 arguments, got 2"
    );
    assert_eq!(
        eval_object_function(
            "bitmap_to_string",
            &arena,
            ExprId(usize::MAX),
            &[ExprId(usize::MAX), id],
            &chunk
        )
        .unwrap_err(),
        "invalid ExprId"
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_object_function(
            "bitmap_to_string",
            &arena,
            ExprId(usize::MAX),
            &[],
            &chunk
        )))
        .is_err()
    );
}
