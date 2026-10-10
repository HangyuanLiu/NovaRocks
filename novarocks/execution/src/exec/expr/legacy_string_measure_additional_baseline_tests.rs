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
//! Independent original measurement carriers and diagnostics before shared extraction.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, string::eval_string_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeStringArray, NullArray,
        StringArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::sync::Arc;
fn raw(name: &'static str, input: ArrayRef, target: Option<DataType>) -> Result<ArrayRef, String> {
    let slot = SlotId::new(1);
    let ty = input.data_type().clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("source", ty.clone(), true)])),
        vec![input],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot]).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let source = arena.push_typed(ExprNode::SlotId(slot), ty);
    let args = vec![source];
    let expr = target.map_or(ExprId(usize::MAX), |target| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String(name),
                args: args.clone(),
            },
            target,
        )
    });
    eval_string_function(name, &arena, expr, &args, &chunk)
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
#[test]
fn legacy_measure_ascii_ignores_original_result_type_and_preserves_slice_null_empty() {
    let input = strings(vec![
        Some("guard"),
        Some("é"),
        None,
        Some("\0x"),
        Some(""),
        Some("guard"),
    ])
    .slice(1, 4);
    for target in [
        None,
        Some(DataType::Int32),
        Some(DataType::Int64),
        Some(DataType::Boolean),
    ] {
        let out = raw("ascii", input.clone(), target).unwrap();
        assert_eq!(out.data_type(), &DataType::Int32);
        assert_eq!(
            out.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(195), None, Some(0), Some(0)]
        );
    }
    let out = raw("ascii", strings(vec![]), None).unwrap();
    assert!(out.is_empty());
    assert_eq!(out.data_type(), &DataType::Int32);
}
#[test]
fn legacy_measure_length_original_int_bigint_missing_and_wrong_target_messages() {
    let input = strings(vec![
        Some("guard"),
        Some("é"),
        Some("👩\u{200d}💻"),
        None,
        Some("guard"),
    ])
    .slice(1, 3);
    for (name, expected) in [
        ("length", vec![Some(2), Some(11), None]),
        ("char_length", vec![Some(1), Some(3), None]),
    ] {
        for target in [DataType::Int32, DataType::Int64] {
            let out = raw(name, input.clone(), Some(target.clone())).unwrap();
            let got = match target {
                DataType::Int32 => out
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(i64::from))
                    .collect::<Vec<_>>(),
                _ => out
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
            };
            assert_eq!(got, expected);
        }
        for input in [input.clone(), strings(vec![None]), strings(vec![])] {
            assert_eq!(
                raw(name, input.clone(), None).unwrap_err(),
                "length return type is missing"
            );
            assert_eq!(
                raw(name, input, Some(DataType::Boolean)).unwrap_err(),
                "length return type must be INT/BIGINT, got Boolean"
            );
        }
    }
}
#[test]
fn legacy_measure_uncoerced_static_reader_error_precedes_missing_target_and_null_mask() {
    for name in ["ascii", "length", "char_length"] {
        for input in [
            Arc::new(NullArray::new(1)) as ArrayRef,
            Arc::new(LargeStringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as ArrayRef,
        ] {
            let expected = if name == "ascii" {
                "ascii expects string"
            } else {
                "length expects string"
            };
            assert_eq!(raw(name, input.clone(), None).unwrap_err(), expected);
            assert_eq!(
                raw(name, input, Some(DataType::Boolean)).unwrap_err(),
                expected
            );
        }
    }
}
#[test]
fn legacy_measure_only_real_dispatch_names_exist_and_utf8_scalars_keep_original_width() {
    let text = "\u{0}\u{7f}\u{80}\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}";
    assert_eq!(
        raw(
            "char_length",
            strings(vec![Some(text)]),
            Some(DataType::Int64)
        )
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0),
        10
    );
    assert_eq!(
        raw("length", strings(vec![Some(text)]), Some(DataType::Int64))
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        26
    );
    for name in ["character_length", "octet_length", "bit_length", "ord"] {
        assert!(raw(name, strings(vec![Some("x")]), Some(DataType::Int32)).is_err());
    }
}
