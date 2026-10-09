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

use super::*;
fn original_call(rows: usize) -> (ExprArena, ExprId, Chunk) {
    let (mut arena, args, chunk) = setup(
        list(
            structure(Arc::new(Int32Array::from(vec![Some(7); rows])), None),
            (0..=rows).map(|r| i32::try_from(r).unwrap()).collect(),
            None,
        ),
        names(Some("chosen"), rows),
    );
    let kind = super::super::function::lookup_function("__array_struct_subfield").unwrap();
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: args.to_vec(),
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    );
    (arena, root, chunk)
}
#[test]
fn original_scalar_invocation_metadata_arity_precedes_every_undefined_child() {
    for count in [3, 5] {
        let (mut arena, _, chunk) = original_call(1);
        let kind = super::super::function::lookup_function("__array_struct_subfield").unwrap();
        let root = arena.push_typed(
            ExprNode::FunctionCall {
                kind,
                args: vec![ExprId(usize::MAX); count],
            },
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );
        assert_eq!(
            arena.eval(root, &chunk).unwrap_err(),
            format!("array_struct_subfield expects 2 to 2 arguments, got {count}")
        );
    }
}
#[test]
fn original_scalar_invocation_case_diagnostic_is_whole_call_and_not_root_null_masked() {
    let source = list(
        structure(
            Arc::new(Int32Array::from(vec![Some(777), None])),
            Some(vec![false, false]),
        ),
        vec![0, 1, 2],
        Some(vec![false, false]),
    );
    let (mut arena, args, chunk) = setup(source, names(Some("chosen"), 2));
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind: super::super::function::lookup_function("__array_struct_subfield").unwrap(),
            args: args.to_vec(),
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    );
    assert_eq!(
        arena.eval(root, &chunk).unwrap_err(),
        "__array_struct_subfield field 'chosen' does not exist"
    );
}
#[test]
fn original_scalar_invocation_empty_if_else_is_called_but_unused_then_is_not() {
    let (mut arena, projection, chunk) = original_call(0);
    let flag = arena.push_typed(
        ExprNode::Literal(super::super::LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let null = arena.push_typed(
        ExprNode::Literal(super::super::LiteralValue::Null),
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    );
    // Empty true literal broadcasts to an empty BooleanArray: original IF
    // observes any_true=false and evaluates its ELSE whole empty invocation.
    let root = arena.push_typed(
        ExprNode::FunctionCall {
            kind: super::super::function::FunctionKind::If,
            args: vec![flag, null, projection],
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    );
    assert_eq!(
        arena.eval(root, &chunk).unwrap_err(),
        "__array_struct_subfield field-name argument is empty"
    );
    let unused = arena.push_typed(
        ExprNode::FunctionCall {
            kind: super::super::function::FunctionKind::If,
            args: vec![flag, projection, null],
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    );
    assert_eq!(arena.eval(unused, &chunk).unwrap().len(), 0);
}
