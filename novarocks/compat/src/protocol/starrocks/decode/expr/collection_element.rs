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
use arrow::datatypes::DataType;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue, function::FunctionKind};

use crate::thrift::exprs;

/// Lower ARRAY_ELEMENT_EXPR to internal array `element_at` function.
pub(crate) fn lower_array_element_expr(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    if children.len() != 2 {
        return Err(format!(
            "ARRAY_ELEMENT_EXPR expected 2 children, got {}",
            children.len()
        ));
    }

    let array_type = arena
        .data_type(children[0])
        .ok_or_else(|| "ARRAY_ELEMENT_EXPR missing array child type".to_string())?;
    let index_type = arena
        .data_type(children[1])
        .ok_or_else(|| "ARRAY_ELEMENT_EXPR missing subscript child type".to_string())?;

    let DataType::List(item_field) = array_type else {
        return Err(format!(
            "ARRAY_ELEMENT_EXPR expects ARRAY as first argument, got {:?}",
            array_type
        ));
    };
    if item_field.data_type() != &data_type {
        return Err(format!(
            "ARRAY_ELEMENT_EXPR return type mismatch: expected {:?}, got {:?}",
            item_field.data_type(),
            data_type
        ));
    }
    if !matches!(index_type, DataType::Int32) {
        return Err(format!(
            "ARRAY_ELEMENT_EXPR expects INT subscript, got {:?}",
            index_type
        ));
    }

    let mut args = vec![children[0], children[1]];
    push_check_out_of_bounds_flag(node, arena, &mut args);
    Ok(arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Array("element_at"),
            args,
        },
        data_type,
    ))
}

/// Lower MAP_ELEMENT_EXPR to internal map `element_at` function.
/// StarRocks FE also uses MAP_ELEMENT_EXPR for struct subscript access.
pub(crate) fn lower_map_element_expr(
    node: &exprs::TExprNode,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    if children.len() != 2 {
        return Err(format!(
            "MAP_ELEMENT_EXPR expected 2 children, got {}",
            children.len()
        ));
    }

    let base_type = arena
        .data_type(children[0])
        .cloned()
        .ok_or_else(|| "MAP_ELEMENT_EXPR missing left child type".to_string())?;
    let key_type = arena
        .data_type(children[1])
        .cloned()
        .ok_or_else(|| "MAP_ELEMENT_EXPR missing subscript child type".to_string())?;

    match base_type {
        DataType::Map(entry_field, _) => {
            let DataType::Struct(entry_fields) = entry_field.data_type() else {
                return Err("MAP_ELEMENT_EXPR map entries type must be Struct".to_string());
            };
            if entry_fields.len() != 2 {
                return Err("MAP_ELEMENT_EXPR map entries type must have 2 fields".to_string());
            }
            let expected_key_type = entry_fields[0].data_type();
            let expected_value_type = entry_fields[1].data_type();
            let mut key_expr = children[1];
            if expected_key_type != &key_type {
                if is_empty_map_literal_expr(arena, children[0]) {
                    // Keep StarRocks-compatible behavior for empty map literals:
                    // map{}['x'] should return NULL instead of failing on key type check.
                } else {
                    if !can_cast_map_key_type(expected_key_type, &key_type) {
                        return Err(format!(
                            "MAP_ELEMENT_EXPR key type mismatch: expected {:?}, got {:?}",
                            expected_key_type, key_type
                        ));
                    }
                    key_expr =
                        arena.push_typed(ExprNode::Cast(children[1]), expected_key_type.clone());
                }
            }
            if expected_value_type != &data_type {
                return Err(format!(
                    "MAP_ELEMENT_EXPR return type mismatch: expected {:?}, got {:?}",
                    expected_value_type, data_type
                ));
            }
            let mut args = vec![children[0], key_expr];
            push_check_out_of_bounds_flag(node, arena, &mut args);
            Ok(arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::Map("element_at"),
                    args,
                },
                data_type,
            ))
        }
        DataType::Struct(fields) => lower_struct_element_expr(&fields, children, arena, data_type),
        other => Err(format!(
            "MAP_ELEMENT_EXPR expects MAP/STRUCT as first argument, got {:?}",
            other
        )),
    }
}

fn can_cast_map_key_type(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::Decimal128(_, expected_scale), DataType::Decimal128(_, actual_scale))
        | (DataType::Decimal128(_, expected_scale), DataType::Decimal256(_, actual_scale))
        | (DataType::Decimal256(_, expected_scale), DataType::Decimal128(_, actual_scale))
        | (DataType::Decimal256(_, expected_scale), DataType::Decimal256(_, actual_scale)) => {
            expected_scale == actual_scale
        }
        _ => false,
    }
}

fn is_empty_map_literal_expr(arena: &ExprArena, expr_id: ExprId) -> bool {
    let Some(ExprNode::FunctionCall { kind, args }) = arena.node(expr_id) else {
        return false;
    };
    if !matches!(
        kind,
        FunctionKind::Map("map") | FunctionKind::Map("map_from_arrays")
    ) {
        return false;
    }
    let Some(keys_expr) = args.first().copied() else {
        return false;
    };
    matches!(
        arena.node(keys_expr),
        Some(ExprNode::ArrayExpr { elements }) if elements.is_empty()
    )
}

fn push_check_out_of_bounds_flag(
    node: &exprs::TExprNode,
    arena: &mut ExprArena,
    args: &mut Vec<ExprId>,
) {
    let check_flag = node.check_is_out_of_bounds.unwrap_or(false);
    let check_flag_expr = arena.push_typed(
        ExprNode::Literal(LiteralValue::Bool(check_flag)),
        DataType::Boolean,
    );
    args.push(check_flag_expr);
}

fn lower_struct_element_expr(
    fields: &arrow::datatypes::Fields,
    children: &[ExprId],
    arena: &mut ExprArena,
    data_type: DataType,
) -> Result<ExprId, String> {
    let subscript = literal_struct_subscript(arena, children[1])?;
    if subscript == 0 {
        return Err("STRUCT subscript must not be zero".to_string());
    }
    let field_count =
        i64::try_from(fields.len()).map_err(|_| "STRUCT field count exceeds i64".to_string())?;
    let normalized = if subscript > 0 {
        subscript - 1
    } else {
        field_count + subscript
    };
    if normalized < 0 || normalized >= field_count {
        return Err(format!(
            "STRUCT subscript out of bounds: {}, field_count={}",
            subscript, field_count
        ));
    }
    let field_idx = usize::try_from(normalized)
        .map_err(|_| "STRUCT field index conversion failed".to_string())?;
    let field = &fields[field_idx];
    if field.data_type() != &data_type {
        return Err(format!(
            "STRUCT subscript type mismatch: expected {:?}, got {:?}",
            field.data_type(),
            data_type
        ));
    }

    let field_name_expr = arena.push_typed(
        ExprNode::Literal(LiteralValue::Utf8(field.name().to_string())),
        DataType::Utf8,
    );
    Ok(arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::StructFn("subfield"),
            args: vec![children[0], field_name_expr],
        },
        data_type,
    ))
}

fn literal_struct_subscript(arena: &ExprArena, subscript: ExprId) -> Result<i64, String> {
    match arena.node(subscript) {
        Some(ExprNode::Literal(LiteralValue::Int8(v))) => Ok(i64::from(*v)),
        Some(ExprNode::Literal(LiteralValue::Int16(v))) => Ok(i64::from(*v)),
        Some(ExprNode::Literal(LiteralValue::Int32(v))) => Ok(i64::from(*v)),
        Some(ExprNode::Literal(LiteralValue::Int64(v))) => Ok(*v),
        Some(other) => Err(format!(
            "STRUCT subscript must be integer literal, got {:?}",
            other
        )),
        None => Err("STRUCT subscript expression is missing".to_string()),
    }
}
