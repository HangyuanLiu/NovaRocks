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
//! Original raw suffix semantics, evaluated through the unchanged arena dispatcher.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, string::eval_string_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::{
    array::{Array, ArrayRef, BinaryArray, BooleanArray, LargeStringArray, NullArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn setup(a: ArrayRef, b: ArrayRef) -> (ExprArena, [ExprId; 2], Chunk) {
    let slots = [SlotId::new(1), SlotId::new(2)];
    let ts = [a.data_type().clone(), b.data_type().clone()];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("source", ts[0].clone(), true),
            Field::new("suffix", ts[1].clone(), true),
        ])),
        vec![a, b],
    )
    .unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    let chunk = Chunk::new_with_chunk_schema(batch, schema);
    let mut arena = ExprArena::default();
    let ids = [
        arena.push_typed(ExprNode::SlotId(slots[0]), ts[0].clone()),
        arena.push_typed(ExprNode::SlotId(slots[1]), ts[1].clone()),
    ];
    (arena, ids, chunk)
}
fn raw(
    a: ArrayRef,
    b: ArrayRef,
    target: Option<DataType>,
    extra: bool,
) -> Result<ArrayRef, String> {
    let (mut arena, ids, chunk) = setup(a, b);
    let mut args = ids.to_vec();
    if extra {
        args.push(ExprId(usize::MAX));
    }
    let expr = target.map_or(ExprId(usize::MAX), |t| {
        arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("append_trailing_char_if_absent"),
                args: args.clone(),
            },
            t,
        )
    });
    eval_string_function(
        "append_trailing_char_if_absent",
        &arena,
        expr,
        &args,
        &chunk,
    )
}
fn values(a: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(a.data_type(), &DataType::Utf8);
    a.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
#[test]
fn legacy_append_trailing_original_bytes_unicode_null_slice_and_ignored_target() {
    let a = strings(vec![
        Some("guard"),
        Some("a"),
        Some("ac"),
        Some(""),
        Some("a"),
        Some("a"),
        Some("a"),
        None,
        Some("a"),
        Some("👩\u{200d}💻"),
        Some("x\0"),
        Some("guard"),
    ])
    .slice(1, 10);
    let b = strings(vec![
        Some("!"),
        Some("c"),
        Some("c"),
        Some("c"),
        Some(""),
        Some("xy"),
        Some("中"),
        Some("c"),
        None,
        Some("."),
        Some("\0"),
        Some("!"),
    ])
    .slice(1, 10);
    for target in [
        None,
        Some(DataType::Utf8),
        Some(DataType::Int64),
        Some(DataType::Null),
    ] {
        assert_eq!(
            values(raw(a.clone(), b.clone(), target, true).unwrap()),
            vec![
                Some("ac".into()),
                Some("ac".into()),
                Some("".into()),
                None,
                None,
                None,
                None,
                None,
                Some("👩\u{200d}💻.".into()),
                Some("x\0".into())
            ]
        );
    }
    assert!(
        raw(strings(vec![]), strings(vec![]), None, true)
            .unwrap()
            .is_empty()
    );
}
#[test]
fn legacy_append_trailing_original_reader_error_and_child_evaluation_order() {
    for bad in [
        Arc::new(NullArray::new(1)) as ArrayRef,
        Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        Arc::new(LargeStringArray::from(vec![None::<&str>])),
        Arc::new(BooleanArray::from(vec![None])),
    ] {
        for (a, b) in [
            (bad.clone(), strings(vec![None])),
            (strings(vec![None]), bad.clone()),
        ] {
            assert_eq!(
                raw(a, b, None, true).unwrap_err(),
                "append_trailing_char_if_absent expects string"
            );
        }
        let (arena, ids, chunk) = setup(bad, strings(vec![None]));
        // Both child evaluations precede either downcast in the original author.
        assert_eq!(
            eval_string_function(
                "append_trailing_char_if_absent",
                &arena,
                ExprId(usize::MAX),
                &[ids[0], ExprId(usize::MAX)],
                &chunk
            )
            .unwrap_err(),
            "invalid ExprId"
        );
        assert_eq!(
            eval_string_function(
                "append_trailing_char_if_absent",
                &arena,
                ExprId(usize::MAX),
                &[ExprId(usize::MAX), ids[1]],
                &chunk
            )
            .unwrap_err(),
            "invalid ExprId"
        );
    }
}
#[test]
fn legacy_append_trailing_original_all_ascii_suffixes_and_long_renderer() {
    let suffixes: Vec<String> = (0u8..=127)
        .map(|b| String::from_utf8(vec![b]).unwrap())
        .collect();
    let texts: Vec<String> = suffixes.iter().map(|s| format!("é中{s}")).collect();
    let a = Arc::new(StringArray::from_iter_values(
        texts.iter().map(String::as_str),
    )) as ArrayRef;
    let b = Arc::new(StringArray::from_iter_values(
        suffixes.iter().map(String::as_str),
    )) as ArrayRef;
    assert_eq!(
        values(raw(a, b, None, false).unwrap()),
        texts.into_iter().map(Some).collect::<Vec<_>>()
    );
    let s = "aé中\0".repeat(700);
    let mut expected = s.clone();
    expected.push('/');
    assert_eq!(
        values(
            raw(
                strings(vec![Some(&s), Some(&s), Some("")]),
                strings(vec![Some("/"), Some("é"), Some("/")]),
                None,
                false
            )
            .unwrap()
        ),
        vec![Some(expected), None, Some("".into())]
    );
}
#[test]
fn legacy_append_trailing_original_raw_arity_panics_and_extra_argument_is_not_evaluated() {
    let (arena, ids, chunk) = setup(strings(vec![Some("a")]), strings(vec![Some("c")]));
    for args in [vec![], vec![ids[0]]] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval_string_function(
                "append_trailing_char_if_absent",
                &arena,
                ExprId(usize::MAX),
                &args,
                &chunk
            )))
            .is_err()
        );
    }
    assert_eq!(
        values(
            eval_string_function(
                "append_trailing_char_if_absent",
                &arena,
                ExprId(usize::MAX),
                &[ids[0], ids[1], ExprId(usize::MAX)],
                &chunk
            )
            .unwrap()
        ),
        vec![Some("ac".into())]
    );
}
