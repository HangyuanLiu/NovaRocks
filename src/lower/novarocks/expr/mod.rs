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
pub(crate) fn lower_proto_expr(
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
    lower_proto_expr(child, arena, input_layout)
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
    lower_proto_expr(child, arena, input_layout)
}

fn lower_expr_list(
    values: &[expr::Expr],
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<ExprId>, String> {
    values
        .iter()
        .map(|value| lower_proto_expr(value, arena, input_layout))
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
    use crate::sql::codegen::proto_encode::types::encode_type;
    use crate::types::logical::{LogicalType, field_with_logical_type};

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
        let id = lower_proto_expr(e, &mut arena, &layout).expect("lower proto expr");
        (arena, id)
    }

    pub(crate) fn lower(e: &expr::Expr) -> (ExprArena, crate::exec::expr::ExprId) {
        lower_with_slots(e, &[1, 7, 42])
    }

    pub(crate) fn lower_err_with_slots(e: &expr::Expr, slots: &[u32]) -> String {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        lower_proto_expr(e, &mut arena, &layout).unwrap_err()
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
            let err = lower_proto_expr(&expr, &mut arena, &layout).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }
}
