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

//! Independent original LIKE/NOT LIKE values, escape and evaluation-order witnesses.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue, function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, LargeStringArray, NullArray, StringArray,
    StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_types::SlotId;
use std::sync::Arc;
fn chunk(arrays: Vec<ArrayRef>) -> Chunk {
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(i.to_string(), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    let slots = (0..batch.num_columns())
        .map(|i| SlotId::new(u32::try_from(i + 17).unwrap()))
        .collect::<Vec<_>>();
    let cs =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, cs)
}
fn call(arena: &mut ExprArena, text: ExprId, pattern: ExprId, negated: bool) -> ExprId {
    let result = arena.push_typed(
        ExprNode::FunctionCall {
            kind: function::FunctionKind::Like,
            args: vec![text, pattern],
        },
        DataType::Boolean,
    );
    if negated {
        arena.push_typed(ExprNode::Not(result), DataType::Boolean)
    } else {
        result
    }
}
pub(super) fn original(
    text: ArrayRef,
    pattern: ArrayRef,
    negated: bool,
) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    let left = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), text.data_type().clone());
    let right = arena.push_typed(
        ExprNode::SlotId(SlotId::new(18)),
        pattern.data_type().clone(),
    );
    let root = call(&mut arena, left, right, negated);
    arena.eval(root, &chunk(vec![text, pattern]))
}
fn bools(out: &ArrayRef) -> Vec<Option<bool>> {
    out.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn like_original_required_prefix_values_and_negation() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        "apple", "apricot", "banana", "azure", "avocado",
    ]));
    let patterns: ArrayRef = Arc::new(StringArray::from(vec!["a%"; 5]));
    for negated in [false, true] {
        assert_eq!(
            bools(&original(values.clone(), patterns.clone(), negated).unwrap()),
            [true, true, false, true, true]
                .iter()
                .map(|v| Some(*v != negated))
                .collect::<Vec<_>>()
        );
    }
}
#[test]
fn like_original_unicode_nul_escapes_and_trailing_backslash() {
    let pairs = [
        ("é中", "__"),
        ("é中", "_"),
        ("a\0b", "a_b"),
        ("a%b", r"a\%b"),
        ("a_b", r"a\_b"),
        (r"a\b", r"a\\b"),
        (r"a\", r"a\"),
        ("", "%"),
        ("", "_"),
        ("abc", "%b%"),
        ("abc", "a%%c"),
    ];
    let text: ArrayRef = Arc::new(StringArray::from(
        pairs.iter().map(|p| p.0).collect::<Vec<_>>(),
    ));
    let pattern: ArrayRef = Arc::new(StringArray::from(
        pairs.iter().map(|p| p.1).collect::<Vec<_>>(),
    ));
    assert_eq!(
        bools(&original(text, pattern, false).unwrap()),
        [
            true, false, true, true, true, true, true, true, false, true, true
        ]
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>()
    );
}
#[test]
fn like_original_null_slices_and_empty_preserve_value_3vl() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("sentinel"),
        Some("a"),
        None,
        Some("b"),
        Some("c"),
        Some("sentinel"),
    ]));
    let pattern: ArrayRef = Arc::new(StringArray::from(vec![
        Some("%"),
        Some("a"),
        Some("%"),
        None,
        Some("d"),
        Some("%"),
    ]));
    for negated in [false, true] {
        let out = original(text.slice(1, 4), pattern.slice(1, 4), negated).unwrap();
        assert_eq!(bools(&out), vec![Some(!negated), None, None, Some(negated)]);
        let out = original(text.slice(0, 0), pattern.slice(0, 0), negated).unwrap();
        assert_eq!(out.data_type(), &DataType::Boolean);
        assert_eq!(out.len(), 0);
    }
}
#[test]
fn like_original_wrong_type_errors_even_empty_and_bare_null() {
    for len in [0, 3] {
        let text: ArrayRef = Arc::new(StringArray::from(vec!["a"; len]));
        let wrong: ArrayRef = Arc::new(Int32Array::from(vec![1; len]));
        let null: ArrayRef = Arc::new(NullArray::new(len));
        for wrong in [wrong, null] {
            assert_eq!(
                original(wrong.clone(), text.clone(), false).unwrap_err(),
                "like: first argument must be a string array"
            );
            assert_eq!(
                original(text.clone(), wrong, false).unwrap_err(),
                "like: second argument must be a string array"
            );
        }
    }
}
fn throwing(arena: &mut ExprArena, message: String) -> ExprId {
    let condition = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(false)),
        DataType::Boolean,
    );
    let text = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(message)),
        DataType::Utf8,
    );
    arena.push_typed(
        ExprNode::FunctionCall {
            kind: function::lookup_function("assert_true").unwrap(),
            args: vec![condition, text],
        },
        DataType::Boolean,
    )
}
#[test]
fn like_original_evaluates_pattern_before_first_type_error_full_message() {
    let c = chunk(vec![Arc::new(Int32Array::from(vec![1]))]);
    let mut arena = ExprArena::default();
    let left = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int32);
    let message = format!("LIKE pattern before downcast {}", "x".repeat(2048));
    let right = throwing(&mut arena, message.clone());
    let root = call(&mut arena, left, right, false);
    assert_eq!(arena.eval(root, &c).unwrap_err(), message);
}
#[test]
fn like_original_source_first_error_never_evaluates_pattern() {
    let c = chunk(vec![Arc::new(Int32Array::from(vec![1]))]);
    let mut arena = ExprArena::default();
    let message = format!("LIKE first source {}", "x".repeat(2048));
    let left = throwing(&mut arena, message.clone());
    let right = throwing(&mut arena, "later pattern must not win".into());
    let root = call(&mut arena, left, right, true);
    assert_eq!(arena.eval(root, &c).unwrap_err(), message);
}
#[test]
fn like_original_empty_invocation_still_evaluates_missing_pattern() {
    let c = chunk(vec![Arc::new(StringArray::from(Vec::<&str>::new()))]);
    let mut arena = ExprArena::default();
    let left = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Utf8);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Utf8);
    let original_error = arena.eval(missing, &c).unwrap_err();
    for negated in [false, true] {
        let root = call(&mut arena, left, missing, negated);
        assert_eq!(arena.eval(root, &c).unwrap_err(), original_error);
    }
}

#[test]
fn like_original_largeutf8_utf8view_are_data_errors_not_silent_coercion() {
    for len in [0, 2] {
        let text: ArrayRef = Arc::new(StringArray::from(vec!["a"; len]));
        for wider in [
            Arc::new(LargeStringArray::from(vec!["a"; len])) as ArrayRef,
            Arc::new(StringViewArray::from(vec!["a"; len])) as ArrayRef,
        ] {
            assert_eq!(
                original(wider.clone(), text.clone(), false).unwrap_err(),
                "like: first argument must be a string array"
            );
            assert_eq!(
                original(text.clone(), wider, false).unwrap_err(),
                "like: second argument must be a string array"
            );
        }
    }
}
