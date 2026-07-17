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

//! Lambda expression lowering.

use arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};

use super::{decode_type, lower_required_child};
use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::proto::expr;

use super::super::layout::Layout;

pub(crate) fn lower_lambda(
    lambda: &expr::LambdaExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let body = lower_required_child(&lambda.body, "Lambda.body", arena, input_layout)?;
    let mut arg_slots = Vec::with_capacity(lambda.params.len());
    for (idx, param) in lambda.params.iter().enumerate() {
        let type_desc = param
            .r#type
            .as_ref()
            .ok_or_else(|| format!("Lambda.params[{idx}].type missing"))?;
        let _param_type = decode_type(type_desc)
            .map_err(|err| format!("Lambda.params[{idx}].type decode failed: {err}"))?;
        if param.slot_id <= 0 {
            return Err(format!("Lambda.params[{idx}].slot_id must be positive"));
        }
        arg_slots.push(SlotId::try_from(param.slot_id)?);
    }
    Ok(arena.push_typed(
        ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs: Vec::new(),
            is_nondeterministic: false,
        },
        data_type,
    ))
}

fn infer_lambda_arg_slots(lambda: &expr::LambdaExpr) -> Result<Vec<SlotId>, String> {
    let body = lambda
        .body
        .as_ref()
        .ok_or_else(|| "LambdaExpr.body missing".to_string())?;
    if lambda.params.is_empty() {
        return Err("LambdaExpr.params is empty".to_string());
    }

    let mut ordered_params = Vec::with_capacity(lambda.params.len());
    let mut target_names = HashSet::with_capacity(lambda.params.len());
    for param in &lambda.params {
        let name = param
            .name
            .as_deref()
            .map(normalize_lambda_param_name)
            .unwrap_or_default();
        if name.is_empty() {
            return Err("LambdaExpr.params contains an empty parameter name".to_string());
        }
        if !target_names.insert(name.clone()) {
            return Err(format!("LambdaExpr duplicate parameter name '{name}'"));
        }
        ordered_params.push(name);
    }

    let mut slots_by_name = HashMap::new();
    collect_lambda_param_slots(body, &target_names, &HashSet::new(), &mut slots_by_name)?;

    ordered_params
        .iter()
        .map(|name| {
            let slot_id = slots_by_name.get(name).ok_or_else(|| {
                format!(
                    "LambdaExpr parameter '{name}' has no LambdaParamRef in body; native lambda lowering requires parameter slot ids"
                )
            })?;
            SlotId::try_from(*slot_id)
        })
        .collect()
}

fn collect_lambda_param_slots(
    expr: &expr::Expr,
    target_names: &HashSet<String>,
    shadowed_names: &HashSet<String>,
    slots_by_name: &mut HashMap<String, i32>,
) -> Result<(), String> {
    let Some(kind) = expr.kind.as_ref() else {
        return Ok(());
    };

    match kind {
        expr::expr::Kind::ColumnRef(_) | expr::expr::Kind::Literal(_) => Ok(()),
        expr::expr::Kind::LambdaParamRef(param) => {
            let name = match param.name.as_deref() {
                Some(name) => normalize_lambda_param_name(name),
                None if target_names.len() == 1 && shadowed_names.is_empty() => target_names
                    .iter()
                    .next()
                    .cloned()
                    .expect("target_names has one item"),
                None => {
                    return Err(
                        "LambdaParamRef.name is required for multi-parameter native lambda lowering"
                            .to_string(),
                    );
                }
            };
            if shadowed_names.contains(&name) || !target_names.contains(&name) {
                return Ok(());
            }
            if let Some(previous) = slots_by_name.insert(name.clone(), param.slot_id)
                && previous != param.slot_id
            {
                return Err(format!(
                    "LambdaExpr parameter '{name}' maps to multiple slot ids: {previous} and {}",
                    param.slot_id
                ));
            }
            Ok(())
        }
        expr::expr::Kind::BinaryOp(binary) => {
            collect_optional_box_lambda_param_slots(
                &binary.left,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            collect_optional_box_lambda_param_slots(
                &binary.right,
                target_names,
                shadowed_names,
                slots_by_name,
            )
        }
        expr::expr::Kind::UnaryOp(unary) => collect_optional_box_lambda_param_slots(
            &unary.operand,
            target_names,
            shadowed_names,
            slots_by_name,
        ),
        expr::expr::Kind::FunctionCall(call) => collect_lambda_param_slots_in_list(
            &call.args,
            target_names,
            shadowed_names,
            slots_by_name,
        ),
        expr::expr::Kind::AggregateCall(call) => {
            collect_lambda_param_slots_in_list(
                &call.args,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            for item in &call.order_by {
                collect_optional_unboxed_lambda_param_slots(
                    &item.expr,
                    target_names,
                    shadowed_names,
                    slots_by_name,
                )?;
            }
            Ok(())
        }
        expr::expr::Kind::WindowCall(call) => {
            collect_lambda_param_slots_in_list(
                &call.args,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            collect_lambda_param_slots_in_list(
                &call.partition_by,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            for item in &call.order_by {
                collect_optional_unboxed_lambda_param_slots(
                    &item.expr,
                    target_names,
                    shadowed_names,
                    slots_by_name,
                )?;
            }
            Ok(())
        }
        expr::expr::Kind::Cast(cast) => collect_optional_box_lambda_param_slots(
            &cast.operand,
            target_names,
            shadowed_names,
            slots_by_name,
        ),
        expr::expr::Kind::IsNull(is_null) => collect_optional_box_lambda_param_slots(
            &is_null.operand,
            target_names,
            shadowed_names,
            slots_by_name,
        ),
        expr::expr::Kind::InList(in_list) => {
            collect_optional_box_lambda_param_slots(
                &in_list.operand,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            collect_lambda_param_slots_in_list(
                &in_list.list,
                target_names,
                shadowed_names,
                slots_by_name,
            )
        }
        expr::expr::Kind::Between(between) => {
            collect_optional_box_lambda_param_slots(
                &between.operand,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            collect_optional_box_lambda_param_slots(
                &between.low,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            collect_optional_box_lambda_param_slots(
                &between.high,
                target_names,
                shadowed_names,
                slots_by_name,
            )
        }
        expr::expr::Kind::Like(like) => {
            collect_optional_box_lambda_param_slots(
                &like.operand,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            collect_optional_box_lambda_param_slots(
                &like.pattern,
                target_names,
                shadowed_names,
                slots_by_name,
            )
        }
        expr::expr::Kind::CaseExpr(case_expr) => {
            collect_optional_box_lambda_param_slots(
                &case_expr.operand,
                target_names,
                shadowed_names,
                slots_by_name,
            )?;
            for branch in &case_expr.when_then {
                collect_optional_unboxed_lambda_param_slots(
                    &branch.when,
                    target_names,
                    shadowed_names,
                    slots_by_name,
                )?;
                collect_optional_unboxed_lambda_param_slots(
                    &branch.then,
                    target_names,
                    shadowed_names,
                    slots_by_name,
                )?;
            }
            collect_optional_box_lambda_param_slots(
                &case_expr.else_expr,
                target_names,
                shadowed_names,
                slots_by_name,
            )
        }
        expr::expr::Kind::IsTruth(is_truth) => collect_optional_box_lambda_param_slots(
            &is_truth.operand,
            target_names,
            shadowed_names,
            slots_by_name,
        ),
        expr::expr::Kind::Lambda(lambda) => {
            let mut nested_shadowed_names = shadowed_names.clone();
            for param in &lambda.params {
                if let Some(name) = param.name.as_deref() {
                    nested_shadowed_names.insert(normalize_lambda_param_name(name));
                }
            }
            collect_optional_box_lambda_param_slots(
                &lambda.body,
                target_names,
                &nested_shadowed_names,
                slots_by_name,
            )
        }
        expr::expr::Kind::Nested(nested) => collect_optional_box_lambda_param_slots(
            &nested.inner,
            target_names,
            shadowed_names,
            slots_by_name,
        ),
    }
}

fn collect_lambda_param_slots_in_list(
    exprs: &[expr::Expr],
    target_names: &HashSet<String>,
    shadowed_names: &HashSet<String>,
    slots_by_name: &mut HashMap<String, i32>,
) -> Result<(), String> {
    for expr in exprs {
        collect_lambda_param_slots(expr, target_names, shadowed_names, slots_by_name)?;
    }
    Ok(())
}

fn collect_optional_box_lambda_param_slots(
    expr: &Option<Box<expr::Expr>>,
    target_names: &HashSet<String>,
    shadowed_names: &HashSet<String>,
    slots_by_name: &mut HashMap<String, i32>,
) -> Result<(), String> {
    if let Some(expr) = expr {
        collect_lambda_param_slots(expr, target_names, shadowed_names, slots_by_name)?;
    }
    Ok(())
}

fn collect_optional_unboxed_lambda_param_slots(
    expr: &Option<expr::Expr>,
    target_names: &HashSet<String>,
    shadowed_names: &HashSet<String>,
    slots_by_name: &mut HashMap<String, i32>,
) -> Result<(), String> {
    if let Some(expr) = expr {
        collect_lambda_param_slots(expr, target_names, shadowed_names, slots_by_name)?;
    }
    Ok(())
}

fn normalize_lambda_param_name(name: &str) -> String {
    name.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::super::tests::{col, lower, lower_with_slots, scalar_expr, type_desc};
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprNode, function::FunctionKind};
    use crate::proto::expr;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn lowers_lambda_expr_to_lambda_function() {
        let lambda_slot = 1_900_000_000;
        let item_type = DataType::Int64;
        let array_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
        let lambda_param = scalar_expr(
            item_type.clone(),
            expr::expr::Kind::LambdaParamRef(expr::LambdaParamRef {
                slot_id: lambda_slot,
                name: Some("x".to_string()),
            }),
        );
        let body = scalar_expr(
            item_type.clone(),
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(lambda_param)),
                right: Some(Box::new(col(7, item_type.clone()))),
            })),
        );
        let lambda = scalar_expr(
            item_type.clone(),
            expr::expr::Kind::Lambda(Box::new(expr::LambdaExpr {
                params: vec![expr::LambdaParam {
                    slot_id: lambda_slot,
                    name: Some("x".to_string()),
                    r#type: Some(type_desc(&item_type)),
                    nullable: true,
                }],
                body: Some(Box::new(body)),
            })),
        );
        let call = scalar_expr(
            array_type.clone(),
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "array_map".to_string(),
                args: vec![lambda, col(1, array_type)],
                distinct: false,
            }),
        );

        let (arena, id) = lower_with_slots(&call, &[1, 7]);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected array_map function call");
        };
        assert_eq!(*kind, FunctionKind::ArrayMap);
        assert_eq!(args.len(), 2);
        let Some(ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
        }) = arena.node(args[0])
        else {
            panic!("expected lowered lambda function");
        };
        assert_eq!(arg_slots, &[SlotId::new(lambda_slot as u32)]);
        assert!(common_sub_exprs.is_empty());
        assert!(!is_nondeterministic);
        let Some(ExprNode::Add(left, right)) = arena.node(*body) else {
            panic!("expected lambda body to keep captured-column add");
        };
        assert!(matches!(
            arena.node(*left),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(lambda_slot as u32)
        ));
        assert!(matches!(
            arena.node(*right),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(7)
        ));
    }

    #[test]
    fn lambda_expr_lowers_to_lambda_function() {
        let lambda = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Lambda(Box::new(expr::LambdaExpr {
                params: vec![expr::LambdaParam {
                    slot_id: 3,
                    name: Some("x".to_string()),
                    r#type: Some(type_desc(&DataType::Int64)),
                    nullable: true,
                }],
                body: Some(Box::new(scalar_expr(
                    DataType::Int64,
                    expr::expr::Kind::LambdaParamRef(expr::LambdaParamRef {
                        slot_id: 3,
                        name: Some("x".to_string()),
                    }),
                ))),
            })),
        );

        let (arena, id) = lower(&lambda);
        let Some(ExprNode::LambdaFunction {
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
            ..
        }) = arena.node(id)
        else {
            panic!("expected LambdaFunction");
        };
        assert_eq!(arg_slots, &vec![SlotId::new(3)]);
        assert!(common_sub_exprs.is_empty());
        assert!(!is_nondeterministic);
    }
}
