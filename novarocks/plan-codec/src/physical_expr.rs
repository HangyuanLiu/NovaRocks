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

use std::collections::{BTreeMap, HashMap, HashSet};

use novarocks_physical_plan::{
    BinaryOperator, ExprId, ExprKind, Fragment, FunctionId, LiteralValue, NodeId, NullOrdering,
    SortDirection, SortExpr, UnaryOperator, ValueId, WindowBound, WindowFrame,
    WindowFrameExclusion, WindowFrameUnits,
};
use novarocks_proto_models::{common, expr};

use crate::physical_type::encode_physical_type;
use crate::{WireLayout, WireSlotId};

pub(crate) const NATIVE_V1_MAX_WIRE_NESTING: usize = 96;
const NATIVE_V1_MAX_EXPANDED_EXPR_MESSAGES: usize = 262_144;
const NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct WireExpressionCost {
    messages: usize,
    dynamic_bytes: usize,
    depth: usize,
}

/// Pre-protobuf accounting for the expression trees v1 will materialize.
///
/// Physical expressions form a DAG, while v1 carries only trees. Costs therefore
/// add once per child reference, including repeated references to the same ExprId.
/// This pass allocates only bounded bookkeeping collections; it never constructs
/// or recursively expands a protobuf expression.
pub(crate) struct WireExpressionPreflight<'a> {
    fragment: &'a Fragment,
    costs: HashMap<ExprId, WireExpressionCost>,
    charged_messages: usize,
    charged_dynamic_bytes: usize,
}

impl<'a> WireExpressionPreflight<'a> {
    pub(crate) fn try_new(fragment: &'a Fragment) -> Result<Self, String> {
        let expression_count = fragment.expressions().len();
        let nodes = fragment
            .expressions()
            .iter()
            .map(|(id, expression)| (*id, expression))
            .collect::<HashMap<_, _>>();
        let mut remaining = HashMap::with_capacity(expression_count);
        let mut dependents = HashMap::<ExprId, Vec<ExprId>>::with_capacity(expression_count);
        let mut references = HashMap::<ExprId, Vec<ExprId>>::with_capacity(expression_count);
        let mut ready = Vec::new();
        for (id, expression) in fragment.expressions().iter() {
            let children = expression_children(&expression.kind);
            let mut seen = HashSet::with_capacity(children.len());
            let unique = children
                .iter()
                .copied()
                .filter(|child| seen.insert(*child))
                .collect::<Vec<_>>();
            for child in &unique {
                if !nodes.contains_key(child) {
                    return Err(format!(
                        "fragment {} expression {} references missing expression {}",
                        fragment.id().get(),
                        id.get(),
                        child.get()
                    ));
                }
                dependents.entry(*child).or_default().push(*id);
            }
            remaining.insert(*id, unique.len());
            references.insert(*id, children);
            if unique.is_empty() {
                ready.push(*id);
            }
        }

        let mut costs = HashMap::<ExprId, WireExpressionCost>::with_capacity(expression_count);
        while let Some(id) = ready.pop() {
            let expression = nodes.get(&id).copied().ok_or_else(|| {
                format!(
                    "fragment {} expression {} disappeared during wire preflight",
                    fragment.id().get(),
                    id.get()
                )
            })?;
            let children = &references[&id];
            let mut messages = local_expression_messages(&expression.kind);
            let mut dynamic_bytes = local_expression_dynamic_bytes(expression);
            let mut depth = 4_usize;
            for child in children {
                let child_cost = costs.get(child).ok_or_else(|| {
                    format!(
                        "fragment {} expression {} dependency {} was not ordered",
                        fragment.id().get(),
                        id.get(),
                        child.get()
                    )
                })?;
                messages = capped_add(
                    messages,
                    child_cost.messages,
                    NATIVE_V1_MAX_EXPANDED_EXPR_MESSAGES,
                );
                dynamic_bytes = capped_add(
                    dynamic_bytes,
                    child_cost.dynamic_bytes,
                    NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES,
                );
                let edge_messages = if matches!(&expression.kind, ExprKind::Case { .. }) {
                    3
                } else {
                    2
                };
                depth = depth.max(child_cost.depth.saturating_add(edge_messages));
            }
            costs.insert(
                id,
                WireExpressionCost {
                    messages,
                    dynamic_bytes,
                    depth,
                },
            );
            if let Some(users) = dependents.get(&id) {
                for user in users {
                    let count = remaining.get_mut(user).ok_or_else(|| {
                        format!(
                            "fragment {} expression {} is absent from wire preflight",
                            fragment.id().get(),
                            user.get()
                        )
                    })?;
                    *count -= 1;
                    if *count == 0 {
                        ready.push(*user);
                    }
                }
            }
        }
        if costs.len() != fragment.expressions().len() {
            return Err(format!(
                "fragment {} expression graph contains a cycle",
                fragment.id().get()
            ));
        }
        Ok(Self {
            fragment,
            costs,
            charged_messages: 0,
            charged_dynamic_bytes: 0,
        })
    }

    pub(crate) fn charge(
        &mut self,
        expression: ExprId,
        enclosing_depth: usize,
    ) -> Result<(), String> {
        let cost = self.costs.get(&expression).ok_or_else(|| {
            format!(
                "fragment {} is missing expression {} during wire preflight",
                self.fragment.id().get(),
                expression.get()
            )
        })?;
        let wire_depth = enclosing_depth.saturating_add(cost.depth);
        if wire_depth > NATIVE_V1_MAX_WIRE_NESTING {
            return Err(format!(
                "native wire v1 expression {} in fragment {} reaches message depth {wire_depth}, exceeding decoder-safe maximum {}",
                expression.get(),
                self.fragment.id().get(),
                NATIVE_V1_MAX_WIRE_NESTING
            ));
        }
        self.charged_messages = capped_add(
            self.charged_messages,
            cost.messages,
            NATIVE_V1_MAX_EXPANDED_EXPR_MESSAGES,
        );
        if self.charged_messages > NATIVE_V1_MAX_EXPANDED_EXPR_MESSAGES {
            return Err(format!(
                "native wire v1 expanded expressions in fragment {} exceed {} messages",
                self.fragment.id().get(),
                NATIVE_V1_MAX_EXPANDED_EXPR_MESSAGES
            ));
        }
        self.charged_dynamic_bytes = capped_add(
            self.charged_dynamic_bytes,
            cost.dynamic_bytes,
            NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES,
        );
        if self.charged_dynamic_bytes > NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES {
            return Err(format!(
                "native wire v1 expanded expressions in fragment {} exceed {} dynamic payload bytes",
                self.fragment.id().get(),
                NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES
            ));
        }
        Ok(())
    }
}

fn capped_add(left: usize, right: usize, maximum: usize) -> usize {
    left.checked_add(right)
        .map_or(maximum.saturating_add(1), |value| {
            value.min(maximum.saturating_add(1))
        })
}

fn expression_children(kind: &ExprKind) -> Vec<ExprId> {
    match kind {
        ExprKind::Value(_) | ExprKind::LambdaParameter { .. } | ExprKind::Literal(_) => Vec::new(),
        ExprKind::Unary { expr, .. }
        | ExprKind::Lambda { body: expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => vec![*expr],
        ExprKind::Binary { left, right, .. } => vec![*left, *right],
        ExprKind::FunctionCall { args, .. } => args.to_vec(),
        ExprKind::InList { expr, list, .. } => {
            std::iter::once(*expr).chain(list.iter().copied()).collect()
        }
        ExprKind::Between {
            expr, low, high, ..
        } => vec![*expr, *low, *high],
        ExprKind::Like { expr, pattern, .. } => vec![*expr, *pattern],
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => operand
            .iter()
            .copied()
            .chain(when_then.iter().flat_map(|(when, then)| [*when, *then]))
            .chain(else_expr.iter().copied())
            .collect(),
        ExprKind::WindowCall {
            args,
            function_order_by,
            frame,
            ..
        } => {
            let mut children = args.to_vec();
            children.extend(function_order_by.iter().map(|item| item.expr));
            if let Some(frame) = frame {
                for bound in [&frame.start, &frame.end] {
                    if let WindowBound::Preceding(expr) | WindowBound::Following(expr) = bound {
                        children.push(*expr);
                    }
                }
            }
            children
        }
    }
}

fn local_expression_messages(kind: &ExprKind) -> usize {
    let additional = match kind {
        ExprKind::Case { when_then, .. } => when_then.len(),
        ExprKind::Literal(LiteralValue::Decimal128(_)) => 1,
        _ => 0,
    };
    8_usize.saturating_add(additional)
}

fn local_expression_dynamic_bytes(expression: &novarocks_physical_plan::ExprNode) -> usize {
    let dynamic = match &expression.kind {
        ExprKind::Literal(LiteralValue::Utf8(value)) => value.len(),
        ExprKind::Literal(LiteralValue::Binary(value)) => value.len(),
        ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
            function.function_id.as_str().len()
        }
        _ => 0,
    };
    let type_bytes =
        timestamp_timezone_bytes(&expression.ty.data_type).saturating_add(match &expression.kind {
            ExprKind::Cast { target, .. } => timestamp_timezone_bytes(target),
            _ => 0,
        });
    dynamic.saturating_add(type_bytes)
}

fn timestamp_timezone_bytes(data_type: &arrow::datatypes::DataType) -> usize {
    match data_type {
        arrow::datatypes::DataType::Timestamp(_, Some(timezone)) => timezone.len(),
        _ => 0,
    }
}

pub(crate) enum ValueResolution<'a> {
    NodeInput,
    Exact(&'a BTreeMap<ValueId, WireSlotId>),
}

pub(crate) fn encode_physical_expr(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression: ExprId,
    resolution: ValueResolution<'_>,
) -> Result<expr::Expr, String> {
    let node = fragment.expressions().get(expression).ok_or_else(|| {
        format!(
            "fragment {} is missing expression {}",
            fragment.id().get(),
            expression.get()
        )
    })?;
    if node.owner != owner {
        return Err(format!(
            "fragment {} expression {} belongs to node {}, not node {}",
            fragment.id().get(),
            expression.get(),
            node.owner.get(),
            owner.get()
        ));
    }
    Ok(expr::Expr {
        r#type: Some(encode_physical_type(&node.ty.data_type)?),
        nullable: node.ty.nullable,
        kind: Some(encode_kind(
            fragment,
            layout,
            owner,
            &node.ty.data_type,
            &node.kind,
            &resolution,
        )?),
    })
}

fn encode_kind(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression_type: &arrow::datatypes::DataType,
    kind: &ExprKind,
    resolution: &ValueResolution<'_>,
) -> Result<expr::expr::Kind, String> {
    use expr::expr::Kind;

    let child = |id| encode_physical_expr(fragment, layout, owner, id, copy_resolution(resolution));
    Ok(match kind {
        ExprKind::Value(value) => Kind::ColumnRef(expr::ColumnRef {
            column_id: resolve_value(layout, owner, *value, resolution)?.get_u32(),
            qualifier: None,
            column: None,
        }),
        ExprKind::LambdaParameter { .. } => {
            return Err("native wire v1 cannot prove a disjoint lambda slot namespace".into());
        }
        ExprKind::Literal(value) => Kind::Literal(expr::LiteralExpr {
            value: Some(encode_literal(value, expression_type)?),
        }),
        ExprKind::Unary { op, expr: operand } => Kind::UnaryOp(Box::new(expr::UnaryOpExpr {
            op: encode_unary(*op)? as i32,
            operand: Some(Box::new(child(*operand)?)),
        })),
        ExprKind::Binary { left, op, right } => Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
            op: encode_binary(*op)? as i32,
            left: Some(Box::new(child(*left)?)),
            right: Some(Box::new(child(*right)?)),
        })),
        ExprKind::FunctionCall { function, args } => Kind::FunctionCall(expr::FunctionCall {
            function_name: builtin_function_name(&function.function_id)?.into(),
            args: encode_exprs(fragment, layout, owner, args, resolution)?,
            distinct: false,
        }),
        ExprKind::Lambda { .. } => {
            return Err("native wire v1 cannot prove a disjoint lambda slot namespace".into());
        }
        ExprKind::Cast {
            expr: operand,
            target,
        } => Kind::Cast(Box::new(expr::CastExpr {
            operand: Some(Box::new(child(*operand)?)),
            target: Some(encode_physical_type(target)?),
        })),
        ExprKind::IsNull {
            expr: operand,
            negated,
        } => Kind::IsNull(Box::new(expr::IsNullExpr {
            operand: Some(Box::new(child(*operand)?)),
            negated: *negated,
        })),
        ExprKind::InList {
            expr: operand,
            list,
            negated,
        } => Kind::InList(Box::new(expr::InListExpr {
            operand: Some(Box::new(child(*operand)?)),
            list: encode_exprs(fragment, layout, owner, list, resolution)?,
            negated: *negated,
        })),
        ExprKind::Between {
            expr: operand,
            low,
            high,
            negated,
        } => Kind::Between(Box::new(expr::BetweenExpr {
            operand: Some(Box::new(child(*operand)?)),
            low: Some(Box::new(child(*low)?)),
            high: Some(Box::new(child(*high)?)),
            negated: *negated,
        })),
        ExprKind::Like {
            expr: operand,
            pattern,
            negated,
        } => Kind::Like(Box::new(expr::LikeExpr {
            operand: Some(Box::new(child(*operand)?)),
            pattern: Some(Box::new(child(*pattern)?)),
            negated: *negated,
        })),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => Kind::CaseExpr(Box::new(expr::CaseExpr {
            operand: operand.map(child).transpose()?.map(Box::new),
            when_then: when_then
                .iter()
                .map(|(when, then)| {
                    Ok(expr::WhenThen {
                        when: Some(child(*when)?),
                        then: Some(child(*then)?),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            else_expr: else_expr.map(child).transpose()?.map(Box::new),
        })),
        ExprKind::IsTruthValue {
            expr: operand,
            value,
            negated,
        } => Kind::IsTruth(Box::new(expr::IsTruthExpr {
            operand: Some(Box::new(child(*operand)?)),
            value: *value,
            negated: *negated,
        })),
        ExprKind::WindowCall { .. } => {
            return Err(format!(
                "fragment {} node {} window call must be encoded by its Window node",
                fragment.id().get(),
                owner.get()
            ));
        }
    })
}

pub(crate) fn encode_exprs(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expressions: &[ExprId],
    resolution: &ValueResolution<'_>,
) -> Result<Vec<expr::Expr>, String> {
    expressions
        .iter()
        .map(|expression| {
            encode_physical_expr(
                fragment,
                layout,
                owner,
                *expression,
                copy_resolution(resolution),
            )
        })
        .collect()
}

pub(crate) fn encode_sort_items(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    items: &[SortExpr],
) -> Result<Vec<expr::SortItem>, String> {
    items
        .iter()
        .map(|item| {
            Ok(expr::SortItem {
                expr: Some(encode_physical_expr(
                    fragment,
                    layout,
                    owner,
                    item.expr,
                    ValueResolution::NodeInput,
                )?),
                asc: item.direction == SortDirection::Ascending,
                nulls_first: item.null_ordering == NullOrdering::First,
            })
        })
        .collect()
}

pub(crate) fn builtin_function_name(function: &FunctionId) -> Result<&str, String> {
    let identity = function.as_str();
    let name = identity
        .strip_prefix("builtin.")
        .and_then(|identity| identity.split_once('/').map(|(_, rest)| rest))
        .and_then(|identity| identity.strip_suffix("/v1"))
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            format!("native wire v1 cannot encode non-canonical function identity `{identity}`")
        })?;
    Ok(name)
}

pub(crate) fn encode_window_frame(
    fragment: &Fragment,
    frame: &WindowFrame,
) -> Result<expr::WindowFrame, String> {
    if frame.exclusion != WindowFrameExclusion::NoOthers {
        return Err("native wire v1 cannot encode window frame exclusion".into());
    }
    let frame_type = match frame.units {
        WindowFrameUnits::Rows => expr::WindowFrameType::Rows,
        WindowFrameUnits::Range => expr::WindowFrameType::Range,
        WindowFrameUnits::Groups => {
            return Err("native wire v1 cannot encode GROUPS window frames".into());
        }
    };
    Ok(expr::WindowFrame {
        frame_type: frame_type as i32,
        start: Some(encode_window_bound(fragment, &frame.start)?),
        end: Some(encode_window_bound(fragment, &frame.end)?),
    })
}

fn encode_window_bound(
    fragment: &Fragment,
    bound: &WindowBound,
) -> Result<expr::WindowBound, String> {
    use expr::window_bound::Bound;

    let bound = match bound {
        WindowBound::UnboundedPreceding => Bound::UnboundedPreceding(true),
        WindowBound::CurrentRow => Bound::CurrentRow(true),
        WindowBound::UnboundedFollowing => Bound::UnboundedFollowing(true),
        WindowBound::Preceding(expression) => {
            Bound::Preceding(window_bound_literal(fragment, *expression)?)
        }
        WindowBound::Following(expression) => {
            Bound::Following(window_bound_literal(fragment, *expression)?)
        }
    };
    Ok(expr::WindowBound { bound: Some(bound) })
}

fn window_bound_literal(fragment: &Fragment, expression: ExprId) -> Result<i64, String> {
    match fragment
        .expressions()
        .get(expression)
        .map(|node| &node.kind)
    {
        Some(ExprKind::Literal(LiteralValue::Int64(value))) if *value >= 0 => Ok(*value),
        _ => Err("native wire v1 window bound must be a non-negative Int64 literal".into()),
    }
}

fn resolve_value(
    layout: &WireLayout,
    owner: NodeId,
    value: ValueId,
    resolution: &ValueResolution<'_>,
) -> Result<WireSlotId, String> {
    match resolution {
        ValueResolution::NodeInput => layout
            .input_value_slot(owner, value)
            .map_err(|error| error.to_string()),
        ValueResolution::Exact(values) => values.get(&value).copied().ok_or_else(|| {
            format!(
                "fragment {} node {} exact expression scope does not contain value {}",
                layout.fragment().get(),
                owner.get(),
                value.get()
            )
        }),
    }
}

fn copy_resolution<'a>(resolution: &ValueResolution<'a>) -> ValueResolution<'a> {
    match resolution {
        ValueResolution::NodeInput => ValueResolution::NodeInput,
        ValueResolution::Exact(values) => ValueResolution::Exact(values),
    }
}

fn encode_binary(operator: BinaryOperator) -> Result<expr::BinaryOp, String> {
    Ok(match operator {
        BinaryOperator::Add => expr::BinaryOp::Add,
        BinaryOperator::Subtract => expr::BinaryOp::Sub,
        BinaryOperator::Multiply => expr::BinaryOp::Mul,
        BinaryOperator::Divide => expr::BinaryOp::Div,
        BinaryOperator::Modulo => expr::BinaryOp::Mod,
        BinaryOperator::Eq => expr::BinaryOp::Eq,
        BinaryOperator::EqForNull => expr::BinaryOp::EqForNull,
        BinaryOperator::NotEq => expr::BinaryOp::Ne,
        BinaryOperator::Lt => expr::BinaryOp::Lt,
        BinaryOperator::LtEq => expr::BinaryOp::Le,
        BinaryOperator::Gt => expr::BinaryOp::Gt,
        BinaryOperator::GtEq => expr::BinaryOp::Ge,
        BinaryOperator::And => expr::BinaryOp::And,
        BinaryOperator::Or => expr::BinaryOp::Or,
        BinaryOperator::BitAnd | BinaryOperator::BitOr | BinaryOperator::BitXor => {
            return Err("native wire v1 cannot encode bitwise binary operators".into());
        }
    })
}

fn encode_unary(operator: UnaryOperator) -> Result<expr::UnaryOp, String> {
    Ok(match operator {
        UnaryOperator::Minus => expr::UnaryOp::Negate,
        UnaryOperator::Not => expr::UnaryOp::Not,
        UnaryOperator::BitwiseNot => expr::UnaryOp::BitwiseNot,
        UnaryOperator::Plus => {
            return Err("native wire v1 cannot encode unary plus".into());
        }
    })
}

fn encode_literal(
    literal: &LiteralValue,
    expression_type: &arrow::datatypes::DataType,
) -> Result<common::LiteralValue, String> {
    use common::literal_value::Value;

    let value = match literal {
        LiteralValue::Null => Value::NullValue(true),
        LiteralValue::Boolean(value) => Value::BoolValue(*value),
        LiteralValue::Int64(value) => Value::IntValue(*value),
        LiteralValue::Float64Bits(value) => Value::FloatValue(f64::from_bits(*value)),
        LiteralValue::LargeInt(value) => Value::LargeintValue(value.to_be_bytes().to_vec()),
        LiteralValue::Decimal128(value) => {
            let arrow::datatypes::DataType::Decimal128(precision, scale) = expression_type else {
                return Err("Decimal128 literal has a non-decimal expression type".into());
            };
            Value::DecimalValue(common::DecimalLiteral {
                value: value.to_be_bytes().to_vec(),
                precision: u32::from(*precision),
                scale: i32::from(*scale),
            })
        }
        LiteralValue::Utf8(value) => Value::StringValue(value.to_string()),
        LiteralValue::Binary(value) => Value::BinaryValue(value.to_vec()),
        LiteralValue::Date32(value) => Value::Date32Value(*value),
        LiteralValue::UInt64(_)
        | LiteralValue::Time64(_)
        | LiteralValue::Timestamp(_)
        | LiteralValue::IntervalMonthDayNano(_) => {
            return Err("native wire v1 cannot preserve this literal kind".into());
        }
    };
    Ok(common::LiteralValue { value: Some(value) })
}
