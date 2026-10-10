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
//! Immutable original REVERSE raw behavior before sharing its Unicode renderer.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, string::eval_string_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeStringArray, ListArray,
        NullArray, StringArray, types::Int32Type,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
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
fn raw(a: ArrayRef, target: Option<DataType>, extra: bool) -> Result<ArrayRef, String> {
    let (mut arena, id, chunk) = setup(a);
    let mut args = vec![id];
    if extra {
        args.push(ExprId(usize::MAX));
    }
    let expr = target.map_or(ExprId(usize::MAX), |t| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("reverse"),
                args: args.clone(),
            },
            t,
        )
    });
    eval_string_function("reverse", &arena, expr, &args, &chunk)
}
fn values(a: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(a.data_type(), &DataType::Utf8);
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|x| x.map(str::to_owned))
        .collect()
}
#[test]
fn legacy_reverse_original_unicode_scalar_order_slice_null_empty_and_ignored_target() {
    let a = strings(vec![
        Some("guard"),
        Some("aé中"),
        Some("a\u{301}"),
        Some("👩\u{200d}💻"),
        Some("x\0y"),
        None,
        Some(""),
        Some("guard"),
    ])
    .slice(1, 6);
    for target in [
        None,
        Some(DataType::Utf8),
        Some(DataType::Int64),
        Some(DataType::Null),
    ] {
        assert_eq!(
            values(raw(a.clone(), target, true).unwrap()),
            vec![
                Some("中éa".into()),
                Some("\u{301}a".into()),
                Some("💻\u{200d}👩".into()),
                Some("y\0x".into()),
                None,
                Some("".into())
            ]
        );
    }
    assert!(raw(strings(vec![]), None, true).unwrap().is_empty());
}
#[test]
fn legacy_reverse_original_exact_reader_errors_precede_null_and_ignored_extra_arguments() {
    for a in [
        Arc::new(NullArray::new(1)) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![None::<&str>])),
        Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        Arc::new(Int64Array::from(vec![None::<i64>])),
    ] {
        assert_eq!(
            raw(a.clone(), None, true).unwrap_err(),
            "reverse expects string or array"
        );
        assert_eq!(
            raw(a, Some(DataType::Utf8), false).unwrap_err(),
            "reverse expects string or array"
        );
    }
    let (arena, id, chunk) = setup(strings(vec![None]));
    assert_eq!(
        eval_string_function(
            "reverse",
            &arena,
            ExprId(usize::MAX),
            &[ExprId(usize::MAX), id],
            &chunk
        )
        .unwrap_err(),
        "invalid ExprId"
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| eval_string_function(
            "reverse",
            &arena,
            ExprId(usize::MAX),
            &[],
            &chunk
        )))
        .is_err()
    );
    assert_eq!(
        values(raw(strings(vec![Some("x"), None]), None, true).unwrap()),
        vec![Some("x".into()), None]
    );
}
#[test]
fn legacy_reverse_original_unicode_boundaries_and_long_scalar_rendering() {
    assert_eq!(
        values(
            raw(
                strings(vec![Some(
                    "\u{0}\u{7f}\u{80}\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}"
                )]),
                None,
                false
            )
            .unwrap()
        ),
        vec![Some(
            "\u{10ffff}\u{10000}\u{ffff}\u{e000}\u{d7ff}\u{800}\u{7ff}\u{80}\u{7f}\u{0}".into()
        )]
    );
    let a = "aé中\0".repeat(600);
    let expected = "\0中éa".repeat(600);
    assert_eq!(
        values(raw(strings(vec![Some(a.as_str())]), None, false).unwrap()),
        vec![Some(expected)]
    );
}
#[test]
fn legacy_reverse_original_unregistered_list_helper_keeps_field_and_item_cast() {
    let a = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), None, Some(3)]),
        None,
        Some(vec![]),
    ])) as ArrayRef;
    let field = Arc::new(
        Field::new("authored-item", DataType::Int64, true).with_metadata(
            std::collections::HashMap::from([("source".into(), "original".into())]),
        ),
    );
    let out = raw(a.clone(), Some(DataType::List(field.clone())), true).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.data_type(), &DataType::List(field));
    assert_eq!(
        list.value(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), None, Some(1)]
    );
    assert!(list.is_null(1));
    assert_eq!(list.value_length(2), 0);
    let out = raw(a.clone(), Some(DataType::Utf8), true).unwrap();
    assert_eq!(out.data_type(), a.data_type());
    assert_eq!(
        out.as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), None, Some(1)]
    );
}
