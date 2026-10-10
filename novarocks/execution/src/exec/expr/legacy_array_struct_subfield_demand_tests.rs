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
//! Original actual invocation errors and absent branch demand are distinct.
use super::*;
fn bad_name_predicate() -> (ExprArena, ExprId, Chunk) {
    let (mut arena, args, chunk) = setup(
        list(
            structure(Arc::new(Int32Array::from(vec![None])), Some(vec![false])),
            vec![0, 1],
            Some(vec![false]),
        ),
        names(Some("Chosen"), 1),
    );
    let name = arena.push_typed(
        ExprNode::Literal(super::super::LiteralValue::Utf8("chosen".into())),
        DataType::Utf8,
    );
    let kind = super::super::function::lookup_function("__array_struct_subfield").unwrap();
    let projection = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: vec![args[0], name],
        },
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    );
    let predicate = arena.push_typed(ExprNode::IsNull(projection), DataType::Boolean);
    (arena, predicate, chunk)
}
#[test]
fn original_subfield_array_and_or_preserve_first_data_before_decisive_or_invalid_tail() {
    for and in [true, false] {
        for invalid in [false, true] {
            let (mut arena, predicate, chunk) = bad_name_predicate();
            let tail = if invalid {
                ExprId(usize::MAX)
            } else {
                arena.push_typed(
                    ExprNode::Literal(super::super::LiteralValue::Bool(!and)),
                    DataType::Boolean,
                )
            };
            let node = if and {
                ExprNode::And(predicate, tail)
            } else {
                ExprNode::Or(predicate, tail)
            };
            let root = arena.push_typed(node, DataType::Boolean);
            assert_eq!(
                arena.eval(root, &chunk).unwrap_err(),
                "__array_struct_subfield field 'chosen' does not exist"
            );
        }
    }
}
#[test]
fn original_subfield_array_if_condition_data_precedes_both_branches_but_unused_branch_has_no_call()
{
    let (mut arena, predicate, chunk) = bad_name_predicate();
    let kind = super::super::function::FunctionKind::If;
    let failing = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: vec![predicate, ExprId(usize::MAX), ExprId(usize::MAX)],
        },
        DataType::Int32,
    );
    assert_eq!(
        arena.eval(failing, &chunk).unwrap_err(),
        "__array_struct_subfield field 'chosen' does not exist"
    );
    let condition = arena.push_typed(
        ExprNode::Literal(super::super::LiteralValue::Bool(true)),
        DataType::Boolean,
    );
    let value = arena.push_typed(
        ExprNode::Literal(super::super::LiteralValue::Int32(7)),
        DataType::Int32,
    );
    let no_call = arena.push_typed(
        ExprNode::FunctionCall {
            kind,
            args: vec![condition, value, predicate],
        },
        DataType::Int32,
    );
    assert_eq!(
        arena.eval(no_call, &chunk).unwrap().to_data(),
        Int32Array::from(vec![7]).to_data()
    );
}
