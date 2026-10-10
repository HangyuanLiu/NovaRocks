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
//! Original parent-level short error prefix, before atomic production changes.
use super::legacy_aes_family_original_baseline_tests::{setup, strings};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::DataType;
use std::sync::Arc;
fn boolean_aes(arena: &mut ExprArena, args: &[ExprId], encrypt: bool) -> ExprId {
    let value = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(if encrypt {
                "aes_encrypt"
            } else {
                "aes_decrypt"
            }),
            args: args.to_vec(),
        },
        DataType::Utf8,
    );
    arena.push_typed(ExprNode::IsNull(value), DataType::Boolean)
}
fn bool_literal(arena: &mut ExprArena, value: bool) -> ExprId {
    arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(value)),
        DataType::Boolean,
    )
}
fn original_error(encrypt: bool) -> String {
    format!(
        "{}: arg0 must be VARCHAR or VARBINARY",
        if encrypt {
            "aes_encrypt"
        } else {
            "aes_decrypt"
        }
    )
}
#[test]
fn original_aes_invocation_failure_precedes_and_or_later_child() {
    for encrypt in [true, false] {
        for conjunction in [true, false] {
            let (mut arena, args, chunk) = setup(vec![
                Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef,
                strings(vec![Some("key"); 3]),
            ]);
            let left = boolean_aes(&mut arena, &args, encrypt);
            let node = if conjunction {
                ExprNode::And(left, ExprId(usize::MAX))
            } else {
                ExprNode::Or(left, ExprId(usize::MAX))
            };
            let root = arena.push_typed(node, DataType::Boolean);
            assert_eq!(
                arena.eval(root, &chunk).unwrap_err(),
                original_error(encrypt)
            );
        }
    }
}
#[test]
fn original_and_false_or_true_still_eagerly_invokes_whole_right_batch() {
    for encrypt in [true, false] {
        for conjunction in [true, false] {
            let (mut arena, args, chunk) = setup(vec![
                Arc::new(Int64Array::from(vec![Some(7); 3])) as ArrayRef,
                strings(vec![Some("key"); 3]),
            ]);
            let left = bool_literal(&mut arena, !conjunction);
            let right = boolean_aes(&mut arena, &args, encrypt);
            let node = if conjunction {
                ExprNode::And(left, right)
            } else {
                ExprNode::Or(left, right)
            };
            let root = arena.push_typed(node, DataType::Boolean);
            assert_eq!(
                arena.eval(root, &chunk).unwrap_err(),
                original_error(encrypt)
            );
        }
    }
}
#[test]
fn original_if_mixed_then_failure_stops_before_invalid_else_child() {
    for encrypt in [true, false] {
        let (mut arena, args, chunk) = setup(vec![
            Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(7); 3])) as ArrayRef,
            strings(vec![Some("key"); 3]),
        ]);
        let then = boolean_aes(&mut arena, &args[1..], encrypt);
        let root = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::If,
                args: vec![args[0], then, ExprId(usize::MAX)],
            },
            DataType::Boolean,
        );
        assert_eq!(
            arena.eval(root, &chunk).unwrap_err(),
            original_error(encrypt)
        );
    }
}
#[test]
fn original_if_all_false_omits_invalid_then_and_byte_normalizer() {
    for encrypt in [true, false] {
        let (mut arena, args, chunk) = setup(vec![
            Arc::new(BooleanArray::from(vec![false; 3])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(7); 3])) as ArrayRef,
            strings(vec![Some("key"); 3]),
        ]);
        let then = boolean_aes(&mut arena, &args[1..], encrypt);
        let otherwise = bool_literal(&mut arena, true);
        let root = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::If,
                args: vec![args[0], then, otherwise],
            },
            DataType::Boolean,
        );
        let actual = arena.eval(root, &chunk).unwrap();
        let actual = actual.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(actual.iter().collect::<Vec<_>>(), vec![Some(true); 3]);
    }
}
#[test]
fn original_aes_wrong_arity_parent_stops_before_every_argument_and_later_operand() {
    for encrypt in [true, false] {
        let (mut arena, _, chunk) = setup(vec![strings(vec![Some("anchor"); 3])]);
        let first = boolean_aes(&mut arena, &[ExprId(usize::MAX); 3], encrypt);
        let root = arena.push_typed(ExprNode::And(first, ExprId(usize::MAX)), DataType::Boolean);
        assert_eq!(
            arena.eval(root, &chunk).unwrap_err(),
            format!(
                "{} expects 2, 4, or 5 arguments",
                if encrypt {
                    "aes_encrypt"
                } else {
                    "aes_decrypt"
                }
            )
        );
    }
}
