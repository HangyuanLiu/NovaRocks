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

//! Fragment collection literal expression lowering.

use arrow::datatypes::DataType;

use super::{decode_expr_at_with_types, lower_expr_list};
use novarocks_execution::exec::expr::function::FunctionKind;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::expr;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_array_literal(
    call: &expr::FunctionCall,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if !matches!(data_type, DataType::List(_)) {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!("ARRAY literal expects List type, got {data_type:?}"),
        ));
    }
    let elements = lower_expr_list(
        &call.args,
        path.field("args"),
        arena,
        input_layout,
        type_decoder,
    )?;
    Ok(arena.push_typed(ExprNode::ArrayExpr { elements }, data_type))
}

pub(crate) fn lower_map_constructor(
    call: &expr::FunctionCall,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
    declared: Option<&novarocks_type_contract::FunctionValueType>,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if !call.args.len().is_multiple_of(2) {
        return Err(super::NativeExpressionDecodeError::invalid_value(
            path.clone().field("args"),
            format!(
                "MAP constructor expects an even number of arguments, got {}",
                call.args.len()
            ),
        ));
    }

    let DataType::Map(entry_field, _) = &data_type else {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!("MAP constructor expects MAP output type, got {data_type:?}"),
        ));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            "MAP constructor entries type must be Struct",
        ));
    };
    if entry_fields.len() != 2 {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone(),
            format!(
                "MAP constructor entries type must have 2 fields, got {}",
                entry_fields.len()
            ),
        ));
    }

    let expected_key_type = entry_fields[0].data_type().clone();
    let expected_value_type = entry_fields[1].data_type().clone();
    let key_binding = match declared {
        Some(source) => {
            type_decoder.synthetic_child_binding(source, 0, path.clone().field("key"))?
        }
        None => None,
    };
    let value_binding = match declared {
        Some(source) => {
            type_decoder.synthetic_child_binding(source, 1, path.clone().field("value"))?
        }
        None => None,
    };
    let mut key_elements = Vec::with_capacity(call.args.len() / 2);
    let mut value_elements = Vec::with_capacity(call.args.len() / 2);

    for (idx, arg) in call.args.iter().enumerate() {
        let arg_path = path.clone().field("args").index(idx);
        let child =
            decode_expr_at_with_types(arg, arg_path.clone(), arena, input_layout, type_decoder)?;
        if idx % 2 == 0 {
            key_elements.push(coerce_map_constructor_child(
                arena,
                child,
                &expected_key_type,
                key_binding.as_ref(),
                idx,
                "key",
                arg_path,
                type_decoder,
            )?);
        } else {
            value_elements.push(coerce_map_constructor_child(
                arena,
                child,
                &expected_value_type,
                value_binding.as_ref(),
                idx,
                "value",
                arg_path,
                type_decoder,
            )?);
        }
    }

    let mut args = Vec::with_capacity(call.args.len());
    for (key, value) in key_elements.into_iter().zip(value_elements) {
        args.push(key);
        args.push(value);
    }

    Ok(arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Map("map"),
            args,
        },
        data_type,
    ))
}

fn coerce_map_constructor_child(
    arena: &mut ExprArena,
    child: ExprId,
    expected_type: &DataType,
    expected_binding: Option<&novarocks_type_contract::FunctionValueType>,
    arg_idx: usize,
    role: &str,
    path: FieldPath,
    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let child_type = arena.data_type(child).cloned().ok_or_else(|| {
        super::NativeExpressionDecodeError::invalid_value(
            path.clone(),
            format!(
                "MAP constructor missing {role} child type at pair {}",
                arg_idx / 2,
            ),
        )
    })?;
    if &child_type == expected_type || matches!(expected_type, DataType::Null) {
        if let Some(expected) = expected_binding {
            let actual = arena.value_type(child).ok_or_else(|| {
                super::NativeExpressionDecodeError::missing(
                    path.clone(),
                    "MAP argument requires its complete declaration",
                )
            })?;
            // Output key non-nullness is a per-row MAP obligation. It does not
            // narrow or re-sign the nullable argument expression itself.
            type_decoder.require_complete_binding(actual, expected, true, path)?;
        }
        return Ok(child);
    }
    let target = match expected_binding {
        Some(expected) => {
            let actual = arena.value_type(child).ok_or_else(|| {
                super::NativeExpressionDecodeError::missing(
                    path.clone(),
                    "MAP argument requires its complete declaration",
                )
            })?;
            type_decoder
                .synthetic_nullable_binding(expected, expected.nullable() || actual.nullable())?
        }
        None => None,
    };
    let id = arena.push_typed(
        ExprNode::Cast(
            child,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        ),
        expected_type.clone(),
    );
    type_decoder.save_synthetic_binding(id, target.as_ref(), arena)?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        int_lit, lower, lower_err_with_slots, null_lit, scalar_expr, string_lit,
    };
    use arrow::datatypes::{DataType, Field, Fields};
    use novarocks_execution::exec::expr::{ExprNode, LiteralValue, function::FunctionKind};
    use novarocks_proto_models::{common, expr};
    use std::sync::Arc;

    fn original(
        ty: DataType,
        nullable: bool,
        value: common::literal_value::Value,
    ) -> (
        novarocks_type_contract::FunctionValueType,
        expr::LiteralExpr,
    ) {
        (
            novarocks_type_contract::FunctionValueType::new(ty, nullable),
            expr::LiteralExpr {
                value: Some(common::LiteralValue { value: Some(value) }),
            },
        )
    }
    fn original_expressions(
        originals: &[(
            novarocks_type_contract::FunctionValueType,
            expr::LiteralExpr,
        )],
    ) -> Vec<expr::Expr> {
        originals
            .iter()
            .map(|(ty, literal)| {
                scalar_expr(ty.clone(), expr::expr::Kind::Literal(literal.clone()))
            })
            .collect()
    }
    #[test]
    fn lowers_array_literal_internal_function_to_array_expr() {
        let originals = [
            original(
                DataType::Int64,
                false,
                common::literal_value::Value::IntValue(1),
            ),
            original(
                DataType::Int64,
                false,
                common::literal_value::Value::IntValue(2),
            ),
            original(
                DataType::Int64,
                true,
                common::literal_value::Value::NullValue(true),
            ),
        ];
        let source = super::super::fixture_scalar_sources::ScalarSource::with_literals(
            "__array_literal",
            &originals,
        );
        let array_type = source.result().data_type().clone();
        let mut array = scalar_expr(
            source.result().clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "__array_literal".into(),
                args: original_expressions(&originals),
                distinct: false,
            }),
        );
        source.attach(&mut array, 710);
        let (arena, id) = source.lower(&array, &[1, 7, 42], 710);
        let Some(ExprNode::ArrayExpr { elements }) = arena.node(id) else {
            panic!("expected array literal to lower as ArrayExpr");
        };
        assert_eq!(elements.len(), 3);
        assert_eq!(arena.data_type(id), Some(&array_type));
        assert!(matches!(
            arena.node(elements[0]),
            Some(ExprNode::Constant(value)) if value.try_i64().unwrap() == Some(1)
        ));
        assert!(matches!(
            arena.node(elements[2]),
            Some(ExprNode::Constant(value)) if super::super::tests::fixture_constant_is_null(value)
        ));
        super::super::tests::assert_complete_frozen(arena);
    }

    #[test]
    fn lowers_variadic_map_constructor_to_literal_call() {
        let originals = [
            original(
                DataType::Int64,
                false,
                common::literal_value::Value::IntValue(1),
            ),
            original(
                DataType::Utf8,
                false,
                common::literal_value::Value::StringValue("a".into()),
            ),
            original(
                DataType::Int64,
                false,
                common::literal_value::Value::IntValue(2),
            ),
            original(
                DataType::Utf8,
                false,
                common::literal_value::Value::StringValue("b".into()),
            ),
        ];
        let source =
            super::super::fixture_scalar_sources::ScalarSource::with_literals("map", &originals);
        let map_type = source.result().data_type().clone();
        let mut map = scalar_expr(
            source.result().clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "map".into(),
                args: original_expressions(&originals),
                distinct: false,
            }),
        );
        source.attach(&mut map, 711);
        let (arena, id) = source.lower(&map, &[1, 7, 42], 711);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected map constructor to lower as function call");
        };
        assert_eq!(*kind, FunctionKind::Map("map"));
        assert_eq!(args.len(), 4);
        assert_eq!(arena.data_type(args[0]), Some(&DataType::Int64));
        assert_eq!(arena.data_type(args[1]), Some(&DataType::Utf8));
        assert_eq!(arena.data_type(args[2]), Some(&DataType::Int64));
        assert_eq!(arena.data_type(args[3]), Some(&DataType::Utf8));
        assert_eq!(arena.data_type(id), Some(&map_type));
        super::super::tests::assert_complete_frozen(arena);
    }

    #[test]
    fn map_constructor_casts_null_children_to_entry_types() {
        let originals = [
            original(
                DataType::Null,
                true,
                common::literal_value::Value::NullValue(true),
            ),
            original(
                DataType::Int64,
                false,
                common::literal_value::Value::IntValue(10),
            ),
            original(
                DataType::Int64,
                false,
                common::literal_value::Value::IntValue(2),
            ),
            original(
                DataType::Null,
                true,
                common::literal_value::Value::NullValue(true),
            ),
        ];
        let source =
            super::super::fixture_scalar_sources::ScalarSource::with_literals("map", &originals);
        let mut map = scalar_expr(
            source.result().clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "map".into(),
                args: original_expressions(&originals),
                distinct: false,
            }),
        );
        source.attach(&mut map, 712);
        let (arena, id) = source.lower(&map, &[1, 7, 42], 712);
        let Some(ExprNode::FunctionCall { args, .. }) = arena.node(id) else {
            panic!("expected map constructor to lower as function call");
        };

        assert_eq!(args.len(), 4);
        assert_eq!(arena.data_type(args[0]), Some(&DataType::Int64));
        assert!(matches!(arena.node(args[0]), Some(ExprNode::Cast(_, _))));
        assert_eq!(arena.data_type(args[3]), Some(&DataType::Int64));
        assert!(matches!(arena.node(args[3]), Some(ExprNode::Cast(_, _))));
        super::super::tests::assert_complete_frozen(arena);
    }

    #[test]
    fn array_literal_requires_list_result_type() {
        let originals = [original(
            DataType::Int64,
            false,
            common::literal_value::Value::IntValue(1),
        )];
        let source = super::super::fixture_scalar_sources::ScalarSource::with_literals(
            "__array_literal",
            &originals,
        );
        // The genuine constructor binding remains List. Only the actual
        // expression result declaration is corrupt, so full validation rejects it.
        let mut array = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "__array_literal".into(),
                args: original_expressions(&originals),
                distinct: false,
            }),
        );
        source.attach(&mut array, 713);
        let error = source.lower_error(&array, &[], 713);
        assert!(
            error.contains("scalar result differs from its complete frozen declaration"),
            "{error}"
        );
        assert!(
            error.contains("plan_fragment") && error.contains("invalid value"),
            "{error}"
        );
    }
}

#[cfg(test)]
mod complete_binding_tests {
    use super::super::tests::{assert_complete_frozen, json_type, scalar_expr};
    use novarocks_execution::exec::expr::ExprNode;
    use novarocks_proto_models::{common, expr};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

    #[test]
    fn map_json_value_keeps_its_actual_source_domain_and_rejects_plain_same_carrier() {
        let originals = [
            (
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),
                expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::StringValue("a".into())),
                    }),
                },
            ),
            (
                json_type().with_nullable(false).unwrap(),
                expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::StringValue(
                            "{\"v\":1}".into(),
                        )),
                    }),
                },
            ),
        ];
        let source =
            super::super::fixture_scalar_sources::ScalarSource::with_literals("map", &originals);
        let mut wire = scalar_expr(
            source.result().clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "map".into(),
                args: originals
                    .iter()
                    .map(|(ty, literal)| {
                        scalar_expr(ty.clone(), expr::expr::Kind::Literal(literal.clone()))
                    })
                    .collect(),
                distinct: false,
            }),
        );
        source.attach(&mut wire, 714);
        let (arena, id) = source.lower(&wire, &[1, 7, 42], 714);
        let Some(ExprNode::FunctionCall { args, .. }) = arena.node(id) else {
            panic!("MAP constructor");
        };
        assert_eq!(
            arena.value_type(args[1]).unwrap().logical_type(),
            ValueLogicalType::Json
        );
        assert_eq!(
            arena
                .field_schema(id)
                .unwrap()
                .map_value()
                .unwrap()
                .domain(),
            Some(ValueLogicalType::Json)
        );
        assert_complete_frozen(arena);

        let mut wrong = scalar_expr(
            source.result().clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "map".to_owned(),
                args: vec![
                    scalar_expr(
                        originals[0].0.clone(),
                        expr::expr::Kind::Literal(originals[0].1.clone()),
                    ),
                    scalar_expr(
                        FunctionValueType::new(arrow::datatypes::DataType::Utf8, false),
                        expr::expr::Kind::Literal(expr::LiteralExpr {
                            value: Some(common::LiteralValue {
                                value: Some(common::literal_value::Value::StringValue(
                                    "abc".into(),
                                )),
                            }),
                        }),
                    ),
                ],
                distinct: false,
            }),
        );
        source.attach(&mut wrong, 714);
        let error = source.lower_error(&wrong, &[], 714);
        assert!(
            error.contains("scalar actual value differs from its complete original argument"),
            "{error}"
        );
        assert!(
            error.contains("plan_fragment") && error.contains("invalid value"),
            "{error}"
        );
    }
}
