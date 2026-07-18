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

//! Proto expression lowering.

use arrow::datatypes::DataType;

use super::layout::Layout;
use super::{decode_field_type, decode_type};
use crate::common::ids::SlotId;
use crate::exec::chunk::ChunkFieldSchema;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::proto::expr;

mod binary;
mod case;
mod cast;
mod collection;
mod function_call;
mod lambda;
mod literal;
#[cfg(feature = "compat")]
mod min_max;
mod nested;
mod predicate;
mod unary;

#[cfg(feature = "compat")]
pub(crate) use min_max::extract_min_max_predicates;

#[allow(dead_code)]
pub(crate) fn decode_expr(
    e: &expr::Expr,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<ExprId, String> {
    let data_type = decode_expr_type(e)?;
    let kind = e
        .kind
        .as_ref()
        .ok_or_else(|| "Expr.kind missing".to_string())?;

    let id = match kind {
        expr::expr::Kind::ColumnRef(column) => {
            let slot_id = input_layout.resolve_column_id(column.column_id)?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Literal(literal) => {
            let value = literal::lower_literal(literal, &data_type)?;
            Ok(arena.push_typed(ExprNode::Literal(value), data_type))
        }
        expr::expr::Kind::BinaryOp(binary) => {
            binary::lower_binary_op(binary, arena, input_layout, data_type)
        }
        expr::expr::Kind::UnaryOp(unary) => unary::lower_unary_op(unary, arena, input_layout, data_type),
        expr::expr::Kind::FunctionCall(call) => {
            function_call::lower_function_call(call, arena, input_layout, data_type)
        }
        expr::expr::Kind::AggregateCall(_) => Err(
            "native scalar expr lowering does not lower AggregateCall; aggregate node handles it"
                .to_string(),
        ),
        expr::expr::Kind::WindowCall(_) => Err(
            "native scalar expr lowering does not lower WindowCall; analytic/window node handles it"
                .to_string(),
        ),
        expr::expr::Kind::Cast(cast) => cast::lower_cast(cast, arena, input_layout, data_type),
        expr::expr::Kind::IsNull(is_null) => {
            predicate::lower_is_null(is_null, arena, input_layout, data_type)
        }
        expr::expr::Kind::InList(in_list) => {
            predicate::lower_in_list(in_list, arena, input_layout, data_type)
        }
        expr::expr::Kind::Between(between) => {
            predicate::lower_between(between, arena, input_layout, data_type)
        }
        expr::expr::Kind::Like(like) => predicate::lower_like(like, arena, input_layout, data_type),
        expr::expr::Kind::CaseExpr(case_expr) => {
            case::lower_case(case_expr, arena, input_layout, data_type)
        }
        expr::expr::Kind::IsTruth(is_truth) => {
            predicate::lower_is_truth(is_truth, arena, input_layout, data_type)
        }
        expr::expr::Kind::LambdaParamRef(param) => {
            let slot_id = SlotId::try_from(param.slot_id)?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Lambda(lambda) => lambda::lower_lambda(lambda, arena, input_layout, data_type),
        expr::expr::Kind::Nested(nested) => nested::lower_nested(nested, arena, input_layout, data_type),
    }?;
    set_proto_field_schema(e, arena, id);
    Ok(id)
}

fn decode_expr_type(e: &expr::Expr) -> Result<DataType, String> {
    let desc = e
        .r#type
        .as_ref()
        .ok_or_else(|| "Expr.type missing".to_string())?;
    decode_type(desc).map_err(|err| format!("Expr.type decode failed: {err}"))
}

pub(crate) fn validate_proto_expr_shape(e: &expr::Expr) -> Result<(), String> {
    decode_expr_type(e)?;
    let kind = e
        .kind
        .as_ref()
        .ok_or_else(|| "Expr.kind missing".to_string())?;
    match kind {
        expr::expr::Kind::ColumnRef(column) => {
            if column.column_id == 0 {
                return Err("ColumnRef.column_id must be positive".to_string());
            }
        }
        expr::expr::Kind::Literal(literal) => {
            let value = literal
                .value
                .as_ref()
                .ok_or_else(|| "LiteralExpr.value missing".to_string())?;
            if value.value.is_none() {
                return Err("LiteralValue.value missing".to_string());
            }
        }
        expr::expr::Kind::BinaryOp(binary) => {
            let op = expr::BinaryOp::try_from(binary.op)
                .map_err(|_| format!("unknown BinaryOp {}", binary.op))?;
            if op == expr::BinaryOp::Unspecified {
                return Err("BinaryOp.op is unspecified".to_string());
            }
            validate_required_child(&binary.left, "BinaryOp.left")?;
            validate_required_child(&binary.right, "BinaryOp.right")?;
        }
        expr::expr::Kind::UnaryOp(unary) => {
            let op = expr::UnaryOp::try_from(unary.op)
                .map_err(|_| format!("unknown UnaryOp {}", unary.op))?;
            if op == expr::UnaryOp::Unspecified {
                return Err("UnaryOp.op is unspecified".to_string());
            }
            validate_required_child(&unary.operand, "UnaryOp.operand")?;
        }
        expr::expr::Kind::FunctionCall(call) => {
            validate_function_name(&call.function_name, "FunctionCall")?;
            validate_expr_list(&call.args)?;
        }
        expr::expr::Kind::AggregateCall(call) => {
            validate_function_name(&call.function_name, "AggregateCall")?;
            validate_expr_list(&call.args)?;
            validate_sort_items(&call.order_by)?;
        }
        expr::expr::Kind::WindowCall(call) => {
            validate_function_name(&call.function_name, "WindowCall")?;
            validate_expr_list(&call.args)?;
            validate_expr_list(&call.partition_by)?;
            validate_sort_items(&call.order_by)?;
            if let Some(frame) = &call.frame {
                validate_window_frame(frame)?;
            }
        }
        expr::expr::Kind::Cast(cast) => {
            validate_required_child(&cast.operand, "Cast.operand")?;
            let target = cast
                .target
                .as_ref()
                .ok_or_else(|| "Cast.target missing".to_string())?;
            decode_type(target).map_err(|error| format!("Cast.target decode failed: {error}"))?;
        }
        expr::expr::Kind::IsNull(is_null) => {
            validate_required_child(&is_null.operand, "IsNull.operand")?;
        }
        expr::expr::Kind::InList(in_list) => {
            validate_required_child(&in_list.operand, "InList.operand")?;
            if in_list.list.is_empty() {
                return Err("InList.list is empty".to_string());
            }
            validate_expr_list(&in_list.list)?;
        }
        expr::expr::Kind::Between(between) => {
            validate_required_child(&between.operand, "Between.operand")?;
            validate_required_child(&between.low, "Between.low")?;
            validate_required_child(&between.high, "Between.high")?;
        }
        expr::expr::Kind::Like(like) => {
            validate_required_child(&like.operand, "Like.operand")?;
            validate_required_child(&like.pattern, "Like.pattern")?;
        }
        expr::expr::Kind::CaseExpr(case_expr) => {
            if let Some(operand) = &case_expr.operand {
                validate_proto_expr_shape(operand)?;
            }
            if case_expr.when_then.is_empty() {
                return Err("CaseExpr.when_then is empty".to_string());
            }
            for (index, branch) in case_expr.when_then.iter().enumerate() {
                validate_required_unboxed_child(
                    &branch.when,
                    &format!("CaseExpr.when_then[{index}].when"),
                )?;
                validate_required_unboxed_child(
                    &branch.then,
                    &format!("CaseExpr.when_then[{index}].then"),
                )?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                validate_proto_expr_shape(else_expr)?;
            }
        }
        expr::expr::Kind::IsTruth(is_truth) => {
            validate_required_child(&is_truth.operand, "IsTruth.operand")?;
        }
        expr::expr::Kind::LambdaParamRef(param) => {
            if param.slot_id <= 0 {
                return Err("LambdaParamRef.slot_id must be positive".to_string());
            }
        }
        expr::expr::Kind::Lambda(lambda) => {
            if lambda.params.is_empty() {
                return Err("LambdaExpr.params is empty".to_string());
            }
            let mut slots = std::collections::BTreeSet::new();
            for (index, param) in lambda.params.iter().enumerate() {
                if param.slot_id <= 0 {
                    return Err(format!("Lambda.params[{index}].slot_id must be positive"));
                }
                if !slots.insert(param.slot_id) {
                    return Err(format!("Lambda.params duplicate slot_id={}", param.slot_id));
                }
                let param_type = param
                    .r#type
                    .as_ref()
                    .ok_or_else(|| format!("Lambda.params[{index}].type missing"))?;
                decode_type(param_type).map_err(|error| {
                    format!("Lambda.params[{index}].type decode failed: {error}")
                })?;
            }
            validate_required_child(&lambda.body, "Lambda.body")?;
        }
        expr::expr::Kind::Nested(nested) => {
            validate_required_child(&nested.inner, "NestedExpr.inner")?;
        }
    }
    Ok(())
}

fn validate_required_child(
    child: &Option<Box<expr::Expr>>,
    field_name: &str,
) -> Result<(), String> {
    validate_proto_expr_shape(
        child
            .as_deref()
            .ok_or_else(|| format!("{field_name} missing"))?,
    )
}

fn validate_required_unboxed_child(
    child: &Option<expr::Expr>,
    field_name: &str,
) -> Result<(), String> {
    validate_proto_expr_shape(
        child
            .as_ref()
            .ok_or_else(|| format!("{field_name} missing"))?,
    )
}

fn validate_expr_list(values: &[expr::Expr]) -> Result<(), String> {
    values.iter().try_for_each(validate_proto_expr_shape)
}

fn validate_function_name(name: &str, owner: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{owner}.function_name is empty"));
    }
    Ok(())
}

fn validate_sort_items(items: &[expr::SortItem]) -> Result<(), String> {
    for (index, item) in items.iter().enumerate() {
        validate_required_unboxed_child(&item.expr, &format!("SortItem[{index}].expr"))?;
    }
    Ok(())
}

fn validate_window_frame(frame: &expr::WindowFrame) -> Result<(), String> {
    let frame_type = expr::WindowFrameType::try_from(frame.frame_type)
        .map_err(|_| format!("unknown WindowFrameType {}", frame.frame_type))?;
    if frame_type == expr::WindowFrameType::Unspecified {
        return Err("WindowFrame.frame_type is unspecified".to_string());
    }
    validate_window_bound(
        frame
            .start
            .as_ref()
            .ok_or_else(|| "WindowFrame.start missing".to_string())?,
        "WindowFrame.start",
    )?;
    validate_window_bound(
        frame
            .end
            .as_ref()
            .ok_or_else(|| "WindowFrame.end missing".to_string())?,
        "WindowFrame.end",
    )?;
    Ok(())
}

fn validate_window_bound(bound: &expr::WindowBound, field_name: &str) -> Result<(), String> {
    match bound
        .bound
        .as_ref()
        .ok_or_else(|| format!("{field_name}.bound missing"))?
    {
        expr::window_bound::Bound::UnboundedPreceding(true)
        | expr::window_bound::Bound::CurrentRow(true)
        | expr::window_bound::Bound::UnboundedFollowing(true) => Ok(()),
        expr::window_bound::Bound::UnboundedPreceding(false)
        | expr::window_bound::Bound::CurrentRow(false)
        | expr::window_bound::Bound::UnboundedFollowing(false) => {
            Err(format!("{field_name} marker must be true"))
        }
        expr::window_bound::Bound::Preceding(offset)
        | expr::window_bound::Bound::Following(offset)
            if *offset >= 0 =>
        {
            Ok(())
        }
        expr::window_bound::Bound::Preceding(offset)
        | expr::window_bound::Bound::Following(offset) => Err(format!(
            "{field_name} offset must be nonnegative, got {offset}"
        )),
    }
}

fn set_proto_field_schema(e: &expr::Expr, arena: &mut ExprArena, id: ExprId) {
    let Some(desc) = e.r#type.as_ref() else {
        return;
    };
    let Ok(field) = decode_field_type("_expr", e.nullable, desc) else {
        return;
    };
    if let Ok(field_schema) = ChunkFieldSchema::from_field(&field) {
        arena.set_field_schema(id, field_schema);
    }
}

fn lower_required_child(
    child: &Option<Box<expr::Expr>>,
    field_name: &str,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<ExprId, String> {
    let child = child
        .as_ref()
        .ok_or_else(|| format!("{field_name} missing"))?;
    decode_expr(child, arena, input_layout)
}

fn lower_required_unboxed_child(
    child: &Option<expr::Expr>,
    field_name: &str,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<ExprId, String> {
    let child = child
        .as_ref()
        .ok_or_else(|| format!("{field_name} missing"))?;
    decode_expr(child, arena, input_layout)
}

fn lower_expr_list(
    values: &[expr::Expr],
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<ExprId>, String> {
    values
        .iter()
        .map(|value| decode_expr(value, arena, input_layout))
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue, function::FunctionKind};
    use crate::proto::{common, expr};
    use crate::types::logical::{LogicalType, field_with_logical_type};
    use crate::types::native_proto::encode_type;

    pub(crate) fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    pub(crate) fn scalar_expr(data_type: DataType, kind: expr::expr::Kind) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(kind),
        }
    }

    pub(crate) fn int_lit(value: i64) -> expr::Expr {
        scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            }),
        )
    }

    pub(crate) fn string_lit(value: &str) -> expr::Expr {
        scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            }),
        )
    }

    pub(crate) fn bool_lit(value: bool) -> expr::Expr {
        scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            }),
        )
    }

    pub(crate) fn null_lit(data_type: DataType) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            }),
        )
    }

    pub(crate) fn col(column_id: u32, data_type: DataType) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            }),
        )
    }

    pub(crate) fn map_string_json_type() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(field_with_logical_type(
                        Field::new("value", DataType::Utf8, true),
                        LogicalType::Json,
                    )),
                ])),
                false,
            )),
            false,
        )
    }

    pub(crate) fn layout_for_slots(slots: &[u32]) -> super::super::layout::Layout {
        super::super::layout::Layout::for_slots(slots.iter().copied().map(SlotId::new))
    }

    pub(crate) fn lower_with_slots(
        e: &expr::Expr,
        slots: &[u32],
    ) -> (ExprArena, crate::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        let id = decode_expr(e, &mut arena, &layout).expect("lower proto expr");
        (arena, id)
    }

    pub(crate) fn lower(e: &expr::Expr) -> (ExprArena, crate::exec::expr::ExprId) {
        lower_with_slots(e, &[1, 7, 42])
    }

    pub(crate) fn lower_err_with_slots(e: &expr::Expr, slots: &[u32]) -> String {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        decode_expr(e, &mut arena, &layout).unwrap_err()
    }

    pub(crate) fn make_i64_chunk(slot: SlotId, values: Vec<Option<i64>>) -> Chunk {
        let field = Field::new("c0", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[slot],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn lowers_column_ref_to_slot_id_with_decoded_type() {
        let (arena, id) = lower(&col(42, DataType::Int32));

        assert!(matches!(
            arena.node(id),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(42)
        ));
        assert_eq!(arena.data_type(id), Some(&DataType::Int32));
    }

    #[test]
    fn column_ref_missing_from_layout_fails() {
        let err = lower_err_with_slots(&col(42, DataType::Int32), &[7]);

        assert!(err.contains("ColumnRef column_id=42 not found in input layout"));
    }

    #[test]
    fn lowers_recursive_binary_cast_and_function_call() {
        let add = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(col(7, DataType::Int64))),
                right: Some(Box::new(int_lit(5))),
            })),
        );
        let cast = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(add)),
                target: Some(type_desc(&DataType::Utf8)),
            })),
        );
        let call = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "upper".to_string(),
                args: vec![cast],
                distinct: false,
            }),
        );

        let (arena, id) = lower(&call);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected function call");
        };
        assert_eq!(*kind, FunctionKind::Upper);
        assert_eq!(args.len(), 1);
        let Some(ExprNode::Cast(add_id)) = arena.node(args[0]) else {
            panic!("expected cast arg");
        };
        assert!(matches!(
            arena.node(*add_id),
            Some(ExprNode::Add(left, right))
                if matches!(arena.node(*left), Some(ExprNode::SlotId(SlotId(7))))
                    && matches!(arena.node(*right), Some(ExprNode::Literal(LiteralValue::Int64(5))))
        ));
    }

    #[test]
    fn lowers_case_in_like_and_nested() {
        let in_pred = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::InList(Box::new(expr::InListExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                list: vec![string_lit("a"), string_lit("b")],
                negated: false,
            })),
        );
        let like = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Like(Box::new(expr::LikeExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                pattern: Some(Box::new(string_lit("x%"))),
                negated: false,
            })),
        );
        let case_expr = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Nested(Box::new(expr::NestedExpr {
                inner: Some(Box::new(scalar_expr(
                    DataType::Utf8,
                    expr::expr::Kind::CaseExpr(Box::new(expr::CaseExpr {
                        operand: None,
                        when_then: vec![
                            expr::WhenThen {
                                when: Some(in_pred),
                                then: Some(string_lit("in")),
                            },
                            expr::WhenThen {
                                when: Some(like),
                                then: Some(string_lit("like")),
                            },
                        ],
                        else_expr: Some(Box::new(string_lit("miss"))),
                    })),
                ))),
            })),
        );

        let (arena, id) = lower(&case_expr);
        let Some(ExprNode::Case {
            has_case_expr,
            has_else_expr,
            children,
        }) = arena.node(id)
        else {
            panic!("expected CASE after nested lowering");
        };
        assert!(!has_case_expr);
        assert!(has_else_expr);
        assert_eq!(children.len(), 5);
        assert!(matches!(arena.node(children[0]), Some(ExprNode::In { .. })));
        assert!(matches!(
            arena.node(children[2]),
            Some(ExprNode::FunctionCall {
                kind: FunctionKind::Like,
                ..
            })
        ));
    }

    #[test]
    fn fails_fast_for_aggregate_and_window_calls() {
        let aggregate = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::AggregateCall(expr::AggregateCall {
                function_name: "count".to_string(),
                args: vec![int_lit(1)],
                distinct: false,
                order_by: vec![],
            }),
        );
        let window = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::WindowCall(expr::WindowCall {
                function_name: "rank".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
                ignore_nulls: false,
            }),
        );

        for (expr, needle) in [(aggregate, "AggregateCall"), (window, "WindowCall")] {
            let mut arena = ExprArena::default();
            let layout = layout_for_slots(&[]);
            let err = decode_expr(&expr, &mut arena, &layout).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }
}
