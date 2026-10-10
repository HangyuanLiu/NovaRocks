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

//! Independent original IN/NOT IN raw semantics and evaluation ordering.
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue, function};
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, NullArray, StringArray};
use arrow::datatypes::Int32Type;
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
pub(super) fn original(arrays: Vec<ArrayRef>, negated: bool) -> Result<ArrayRef, String> {
    assert!(!arrays.is_empty());
    let mut arena = ExprArena::default();
    let children = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(u32::try_from(i + 17).unwrap())),
                a.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let root = arena.push_typed(
        ExprNode::In {
            child: children[0],
            values: children[1..].to_vec(),
            is_not_in: negated,
        },
        DataType::Boolean,
    );
    arena.eval(root, &chunk(arrays))
}
fn bools(out: &ArrayRef) -> Vec<Option<bool>> {
    out.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn inlist_original_i32_null_match_dominance_slices_and_empty() {
    let source: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        None,
        Some(4),
    ]));
    let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1); 5]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(2); 5]));
    for negated in [false, true] {
        for (start, len) in [(0, 5), (1, 3), (0, 0)] {
            let out = original(
                vec![
                    source.slice(start, len),
                    a.slice(start, len),
                    b.slice(start, len),
                ],
                negated,
            )
            .unwrap();
            let wanted = [Some(true), Some(true), Some(false), None, Some(false)];
            assert_eq!(
                bools(&out),
                wanted[start..start + len]
                    .iter()
                    .map(|v| v.map(|b| b != negated))
                    .collect::<Vec<_>>()
            );
        }
    }
    let null: ArrayRef = Arc::new(Int32Array::from(vec![None; 5]));
    assert_eq!(
        bools(&original(vec![source, a, null], false).unwrap()),
        vec![Some(true), None, None, None, None]
    );
}
#[test]
fn inlist_original_utf8_unicode_embedded_nul_and_dynamic_candidates() {
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        Some("ASIA"),
        Some("AMERICA"),
        Some("é中"),
        Some("a\0b"),
        None,
    ]));
    let a: ArrayRef = Arc::new(StringArray::from(vec![
        Some("ASIA"),
        Some("ASIA"),
        Some("é中"),
        Some("wrong"),
        Some("ASIA"),
    ]));
    let b: ArrayRef = Arc::new(StringArray::from(vec![
        Some("AMERICA"),
        Some("AMERICA"),
        Some("wrong"),
        Some("a\0b"),
        None,
    ]));
    for negated in [false, true] {
        assert_eq!(
            bools(&original(vec![source.clone(), a.clone(), b.clone()], negated).unwrap()),
            vec![
                Some(!negated),
                Some(!negated),
                Some(!negated),
                Some(!negated),
                None
            ]
        );
    }
}
#[test]
fn inlist_original_root_null_and_empty_candidate_list() {
    for negated in [false, true] {
        let null: ArrayRef = Arc::new(NullArray::new(3));
        assert_eq!(
            bools(&original(vec![null], negated).unwrap()),
            vec![None; 3]
        );
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(2)]));
        assert_eq!(
            bools(&original(vec![values], negated).unwrap()),
            vec![Some(negated), None, Some(negated)]
        );
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
fn inlist_original_matching_candidate_still_evaluates_later_full_error() {
    let c = chunk(vec![Arc::new(Int32Array::from(vec![1]))]);
    for negated in [false, true] {
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int32);
        let matched = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let message = format!("required later IN candidate {}", "x".repeat(2048));
        let tail = throwing(&mut arena, message.clone());
        let root = arena.push_typed(
            ExprNode::In {
                child: input,
                values: vec![matched, tail],
                is_not_in: negated,
            },
            DataType::Boolean,
        );
        assert_eq!(arena.eval(root, &c).unwrap_err(), message);
    }
}
#[test]
fn inlist_original_empty_invocation_skips_missing_candidate_but_not_source() {
    let c = chunk(vec![Arc::new(Int32Array::from(Vec::<i32>::new()))]);
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int32);
    let missing = arena.push_typed(ExprNode::SlotId(SlotId::new(999)), DataType::Int32);
    let message = arena.eval(missing, &c).unwrap_err();
    for negated in [false, true] {
        let root = arena.push_typed(
            ExprNode::In {
                child: input,
                values: vec![missing],
                is_not_in: negated,
            },
            DataType::Boolean,
        );
        let out = arena.eval(root, &c).unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &DataType::Boolean);
        let root = arena.push_typed(
            ExprNode::In {
                child: missing,
                values: vec![input],
                is_not_in: negated,
            },
            DataType::Boolean,
        );
        assert_eq!(arena.eval(root, &c).unwrap_err(), message);
    }
}
#[test]
fn inlist_original_first_comparison_error_precedes_later_expression() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1)])]);
    let expected = format!(
        "IN nested type mismatch: {:?} vs {:?}",
        DataType::Int32,
        list.data_type()
    );
    let candidate_type = list.data_type().clone();
    let c = chunk(vec![Arc::new(Int32Array::from(vec![1])), Arc::new(list)]);
    let mut arena = ExprArena::default();
    let input = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int32);
    let wrong = arena.push_typed(ExprNode::SlotId(SlotId::new(18)), candidate_type);
    let tail = throwing(&mut arena, "unreached later candidate".into());
    let root = arena.push_typed(
        ExprNode::In {
            child: input,
            values: vec![wrong, tail],
            is_not_in: false,
        },
        DataType::Boolean,
    );
    assert_eq!(arena.eval(root, &c).unwrap_err(), expected);
}

/// Evaluate the original required CASE operand through its existing arena owner.
pub(super) fn original_required_case_operand(input: ArrayRef) -> ArrayRef {
    let mut arena = ExprArena::default();
    let source = arena.push_typed(ExprNode::SlotId(SlotId::new(17)), DataType::Int32);
    let mut children = Vec::new();
    for (minimum, value) in [(90, "A"), (80, "B"), (70, "C"), (60, "D")] {
        let threshold = arena.push_typed(
            ExprNode::Literal(LiteralValue::Int32(minimum)),
            DataType::Int32,
        );
        let condition = arena.push_typed(ExprNode::Ge(source, threshold), DataType::Boolean);
        let branch = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8(value.into())),
            DataType::Utf8,
        );
        children.extend([condition, branch]);
    }
    let otherwise = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8("E".into())),
        DataType::Utf8,
    );
    children.push(otherwise);
    let root = arena.push_typed(
        ExprNode::Case {
            has_case_expr: false,
            has_else_expr: true,
            children,
        },
        DataType::Utf8,
    );
    arena.eval(root, &chunk(vec![input])).unwrap()
}
