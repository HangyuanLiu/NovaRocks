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
    BinaryOperator, ExprId, ExprKind, Fragment, FunctionId, NodeId, NullOrdering, SortDirection,
    SortExpr, UnaryOperator, ValueId, WindowBound, WindowFrame, WindowFrameExclusion,
    WindowFrameUnits,
};
use novarocks_proto_models::{common, expr};

use crate::physical_type::encode_physical_type;
use crate::{PhysicalEncodeError, WireLayout, WireSlotId};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

#[path = "cv_projection.rs"]
mod cv_projection;

#[derive(Clone, Copy)]
pub(crate) struct ExpressionEncodingContext<'a> {
    pub(crate) constants: &'a novarocks_physical_plan::ConstantPools,
    pub(crate) control: &'a dyn PureCompileControl,
}

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
    pub(crate) fn try_new(
        fragment: &'a Fragment,
        context: ExpressionEncodingContext<'_>,
    ) -> Result<Self, PhysicalEncodeError> {
        let mut work = CompileCheckpoints::try_new(context.control, CompilePhase::Encode)?;
        let result = Self::try_new_inner(fragment, context, &mut work);
        if matches!(result, Err(PhysicalEncodeError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn try_new_inner(
        fragment: &'a Fragment,
        context: ExpressionEncodingContext<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PhysicalEncodeError> {
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
                    )
                    .into());
                }
                dependents.entry(*child).or_default().push(*id);
            }
            work.step()?;
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
            let (additional, payload) = match expression.kind {
                ExprKind::Constant(reference) => {
                    cv_projection::cost(reference, &expression.ty, context)?
                }
                ExprKind::Literal(_) => {
                    return Err("native wire v1 requires a checked constant reference".into());
                }
                _ => (0, 0),
            };
            let mut messages =
                local_expression_messages(&expression.kind).saturating_add(additional);
            let mut dynamic_bytes =
                local_expression_dynamic_bytes(expression).saturating_add(payload);
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
            work.step()?;
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
                    work.step()?;
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
            )
            .into());
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

/// Project a selected checked scalar for operator-specific v1 facts.
pub(crate) fn encode_constant_payload(
    fragment: &Fragment,
    expression: ExprId,
    context: ExpressionEncodingContext<'_>,
) -> Result<Option<common::LiteralValue>, PhysicalEncodeError> {
    let Some(node) = fragment.expressions().get(expression) else {
        return Ok(None);
    };
    match node.kind {
        ExprKind::Constant(reference) => {
            cv_projection::encode(reference, &node.ty, context).map(Some)
        }
        ExprKind::Literal(_) => Err("native wire v1 requires a checked constant reference".into()),
        _ => Ok(None),
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
        ExprKind::Value(_)
        | ExprKind::LambdaParameter { .. }
        | ExprKind::Constant(_)
        | ExprKind::Literal(_) => Vec::new(),
        ExprKind::Unary { expr, .. }
        | ExprKind::Lambda { body: expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => vec![*expr],
        ExprKind::Binary { left, right, .. } => vec![*left, *right],
        ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => args.to_vec(),
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
        _ => 0,
    };
    8_usize.saturating_add(additional)
}

fn local_expression_dynamic_bytes(expression: &novarocks_physical_plan::ExprNode) -> usize {
    let dynamic = match &expression.kind {
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

fn encode_decimal_overflow_policy(policy: novarocks_type_contract::DecimalOverflowPolicy) -> i32 {
    match policy {
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull => {
            expr::DecimalOverflowPolicy::OutputNull as i32
        }
        novarocks_type_contract::DecimalOverflowPolicy::ReportError => {
            expr::DecimalOverflowPolicy::ReportError as i32
        }
    }
}

pub(crate) fn encode_physical_expr(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression: ExprId,
    resolution: ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::Expr, PhysicalEncodeError> {
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
        )
        .into());
    }
    Ok(expr::Expr {
        r#type: Some(encode_physical_type(&node.ty.data_type)?),
        nullable: node.ty.nullable,
        kind: Some(encode_kind(
            fragment,
            layout,
            owner,
            expression,
            &node.ty,
            &node.kind,
            &resolution,
            context,
        )?),
    })
}

/// Encodes an n-ary connective as a balanced binary tree.
///
/// Native wire v1 only spells `AND`/`OR` as a binary operator, so the argument
/// list has to become nesting. Shaping it as a balanced tree rather than a
/// chain keeps message nesting at log2(N), which matters because the decoder
/// enforces a nesting limit an ordinary wide predicate would otherwise blow.
///
/// This is lossless. Balancing re-parenthesises but never reorders, and with
/// left-to-right short-circuit evaluation every parenthesisation of a
/// three-valued `AND`/`OR` chain evaluates the same arguments in the same
/// order and yields the same result.
fn encode_connective(
    op: expr::BinaryOp,
    args: &[ExprId],
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression_type: &arrow::datatypes::DataType,
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::expr::Kind, PhysicalEncodeError> {
    let nullable = args.iter().try_fold(false, |nullable, arg| {
        fragment
            .expressions()
            .get(*arg)
            .map(|node| nullable || node.ty.nullable)
            .ok_or_else(|| {
                format!(
                    "fragment {} expression {} is not defined",
                    fragment.id().get(),
                    arg.get()
                )
            })
    })?;
    let build = |range: &[ExprId]| -> Result<expr::Expr, PhysicalEncodeError> {
        encode_connective_range(
            op,
            range,
            fragment,
            layout,
            owner,
            expression_type,
            nullable,
            resolution,
            context,
        )
    };
    let (left, right) = split_connective(args)?;
    Ok(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
        op: op as i32,
        decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
        left: Some(Box::new(build(left)?)),
        right: Some(Box::new(build(right)?)),
    })))
}

/// Folds a filter's conjunct list into the single predicate expression native
/// wire v1 spells, using the same balanced shape as an n-ary connective.
pub(crate) fn encode_predicate_conjunction(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    predicates: &[ExprId],
    resolution: ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::Expr, PhysicalEncodeError> {
    let Some(first) = predicates.first() else {
        return Err(format!(
            "fragment {} node {} filter has no predicate",
            fragment.id().get(),
            owner.get()
        )
        .into());
    };
    if predicates.len() == 1 {
        return encode_physical_expr(fragment, layout, owner, *first, resolution, context);
    }
    let nullable = predicates.iter().try_fold(false, |nullable, predicate| {
        fragment
            .expressions()
            .get(*predicate)
            .map(|node| nullable || node.ty.nullable)
            .ok_or_else(|| {
                format!(
                    "fragment {} expression {} is not defined",
                    fragment.id().get(),
                    predicate.get()
                )
            })
    })?;
    encode_connective_range(
        expr::BinaryOp::And,
        predicates,
        fragment,
        layout,
        owner,
        &arrow::datatypes::DataType::Boolean,
        nullable,
        &resolution,
        context,
    )
}

/// Splits an argument list into two halves, preserving order.
fn split_connective(args: &[ExprId]) -> Result<(&[ExprId], &[ExprId]), String> {
    if args.len() < 2 {
        return Err("boolean connective requires at least two arguments".into());
    }
    Ok(args.split_at(args.len() / 2))
}

fn encode_connective_range(
    op: expr::BinaryOp,
    args: &[ExprId],
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression_type: &arrow::datatypes::DataType,
    nullable: bool,
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::Expr, PhysicalEncodeError> {
    if let [single] = args {
        return encode_physical_expr(
            fragment,
            layout,
            owner,
            *single,
            copy_resolution(resolution),
            context,
        );
    }
    let (left, right) = split_connective(args)?;
    Ok(expr::Expr {
        r#type: Some(encode_physical_type(expression_type)?),
        nullable,
        kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
            op: op as i32,
            decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
            left: Some(Box::new(encode_connective_range(
                op,
                left,
                fragment,
                layout,
                owner,
                expression_type,
                nullable,
                resolution,
                context,
            )?)),
            right: Some(Box::new(encode_connective_range(
                op,
                right,
                fragment,
                layout,
                owner,
                expression_type,
                nullable,
                resolution,
                context,
            )?)),
        }))),
    })
}

fn encode_kind(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression: ExprId,
    expression_type: &novarocks_physical_plan::ValueType,
    kind: &ExprKind,
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::expr::Kind, PhysicalEncodeError> {
    use expr::expr::Kind;

    let child = |id| {
        encode_physical_expr(
            fragment,
            layout,
            owner,
            id,
            copy_resolution(resolution),
            context,
        )
    };
    Ok(match kind {
        ExprKind::Value(value) => Kind::ColumnRef(expr::ColumnRef {
            column_id: resolve_value(layout, owner, *value, resolution)?.get_u32(),
            qualifier: None,
            column: None,
        }),
        ExprKind::LambdaParameter { lambda, ordinal } => {
            Kind::LambdaParamRef(expr::LambdaParamRef {
                slot_id: lambda_parameter_slot(*lambda, *ordinal)?,
                name: None,
            })
        }
        ExprKind::Literal(_) => {
            return Err("native wire v1 requires a checked constant reference".into());
        }
        ExprKind::Constant(_) => Kind::Literal(expr::LiteralExpr {
            value: Some(cv_projection::encode(
                novarocks_physical_plan::native_v1_emitted_constant_reference(kind)
                    .expect("matched actual native v1 Constant emission"),
                expression_type,
                context,
            )?),
        }),
        ExprKind::Unary { op, expr: operand } => Kind::UnaryOp(Box::new(expr::UnaryOpExpr {
            op: encode_unary(*op)? as i32,
            operand: Some(Box::new(child(*operand)?)),
        })),
        ExprKind::Binary {
            left,
            op,
            right,
            decimal_overflow_policy,
            // Complete-plan v1 preflight checks this value against admitted
            // QueryOptions; the v2 carrier retains the reference identity.
            allow_throw_exception: _,
        } => Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
            op: encode_binary(*op)? as i32,
            decimal_overflow_policy: encode_decimal_overflow_policy(*decimal_overflow_policy),
            left: Some(Box::new(child(*left)?)),
            right: Some(Box::new(child(*right)?)),
        })),
        ExprKind::Conjunction { args } => encode_connective(
            expr::BinaryOp::And,
            args,
            fragment,
            layout,
            owner,
            &expression_type.data_type,
            resolution,
            context,
        )?,
        ExprKind::Disjunction { args } => encode_connective(
            expr::BinaryOp::Or,
            args,
            fragment,
            layout,
            owner,
            &expression_type.data_type,
            resolution,
            context,
        )?,
        ExprKind::FunctionCall { function, args } => Kind::FunctionCall(expr::FunctionCall {
            function_name: wire_function_name(&function.function_id)?.into(),
            args: encode_exprs(fragment, layout, owner, args, resolution, context)?,
            distinct: false,
        }),
        ExprKind::Lambda {
            parameter_types,
            body,
        } => Kind::Lambda(Box::new(expr::LambdaExpr {
            params: parameter_types
                .iter()
                .enumerate()
                .map(|(ordinal, ty)| {
                    let ordinal = u32::try_from(ordinal)
                        .map_err(|_| "lambda parameter ordinal exceeds u32".to_string())?;
                    Ok(expr::LambdaParam {
                        slot_id: lambda_parameter_slot(expression, ordinal)?,
                        name: None,
                        r#type: Some(encode_physical_type(&ty.data_type)?),
                        nullable: ty.nullable,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            body: Some(Box::new(child(*body)?)),
        })),
        ExprKind::Cast {
            expr: operand,
            target,
            decimal_overflow_policy,
            // Complete-plan v1 preflight checks this value against admitted
            // QueryOptions; the v2 carrier retains the reference identity.
            allow_throw_exception: _,
        } => Kind::Cast(Box::new(expr::CastExpr {
            operand: Some(Box::new(child(*operand)?)),
            target: Some(encode_physical_type(target)?),
            decimal_overflow_policy: encode_decimal_overflow_policy(*decimal_overflow_policy),
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
            list: encode_exprs(fragment, layout, owner, list, resolution, context)?,
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
                .collect::<Result<Vec<_>, PhysicalEncodeError>>()?,
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
            )
            .into());
        }
    })
}

pub(crate) fn encode_exprs(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expressions: &[ExprId],
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
) -> Result<Vec<expr::Expr>, PhysicalEncodeError> {
    expressions
        .iter()
        .map(|expression| {
            encode_physical_expr(
                fragment,
                layout,
                owner,
                *expression,
                copy_resolution(resolution),
                context,
            )
        })
        .collect()
}

pub(crate) fn encode_sort_items(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    items: &[SortExpr],
    context: ExpressionEncodingContext<'_>,
) -> Result<Vec<expr::SortItem>, PhysicalEncodeError> {
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
                    context,
                )?),
                asc: item.direction == SortDirection::Ascending,
                nulls_first: item.null_ordering == NullOrdering::First,
            })
        })
        .collect()
}

/// The base of the slot namespace a lambda parameter is addressed in.
///
/// A lambda's parameters are not the node's columns: they exist only while
/// its body runs, and the wire addresses them in the reserved range the
/// analyzer has always used for exactly this. Counting down from the base
/// keeps them clear of the slots a fragment allocates upward from one.
const LAMBDA_PARAMETER_SLOT_BASE: i32 = 1_900_000_000;

/// How many parameters one lambda may address.
///
/// A lambda's slots are derived from its own expression identity, so two
/// lambdas in one node cannot collide without a counter to keep. The width is
/// what makes that derivation total.
const LAMBDA_PARAMETER_WIDTH: u32 = 8;

/// How many parameters one lambda may declare and still travel.
pub(crate) const MAX_WIRE_LAMBDA_PARAMETERS: usize = LAMBDA_PARAMETER_WIDTH as usize;

/// The slot one lambda's parameter is read through.
fn lambda_parameter_slot(lambda: ExprId, ordinal: u32) -> Result<i32, String> {
    if ordinal >= LAMBDA_PARAMETER_WIDTH {
        return Err(format!(
            "native wire v1 addresses at most {LAMBDA_PARAMETER_WIDTH} lambda parameters, got {}",
            ordinal + 1
        ));
    }
    let offset = lambda
        .get()
        .checked_mul(LAMBDA_PARAMETER_WIDTH)
        .and_then(|base| base.checked_add(ordinal))
        .and_then(|offset| i32::try_from(offset).ok())
        // Half the base is the floor: below it the derivation would reach
        // down toward the slots a fragment allocates, and no fragment has
        // anywhere near that many expressions.
        .filter(|offset| *offset < LAMBDA_PARAMETER_SLOT_BASE / 2)
        .ok_or_else(|| {
            format!(
                "native wire v1 has no lambda slot for expression {} parameter {ordinal}",
                lambda.get()
            )
        })?;
    Ok(LAMBDA_PARAMETER_SLOT_BASE - offset)
}

/// The name the backend resolves this function by.
///
/// An identity says which namespace decides what the function means, and the
/// wire carries only the name inside it. Two namespaces reach the wire:
/// `builtin.` for the engine's own functions, and `parametric.` for one a
/// provider registered -- a connector's statistics aggregate, say -- whose
/// signature its own resolver answers. Both resolve by the same name on the
/// backend, so both strip to the same thing here.
pub(crate) fn wire_function_name(function: &FunctionId) -> Result<&str, String> {
    novarocks_physical_plan::native_v1_function_name(function).map_err(|_| {
        format!(
            "native wire v1 cannot encode non-canonical function identity `{}`",
            function.as_str()
        )
    })
}

pub(crate) fn encode_window_frame(
    fragment: &Fragment,
    frame: &WindowFrame,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::WindowFrame, PhysicalEncodeError> {
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
        start: Some(encode_window_bound(fragment, &frame.start, context)?),
        end: Some(encode_window_bound(fragment, &frame.end, context)?),
    })
}

fn encode_window_bound(
    fragment: &Fragment,
    bound: &WindowBound,
    context: ExpressionEncodingContext<'_>,
) -> Result<expr::WindowBound, PhysicalEncodeError> {
    use expr::window_bound::Bound;

    let bound = match bound {
        WindowBound::UnboundedPreceding => Bound::UnboundedPreceding(true),
        WindowBound::CurrentRow => Bound::CurrentRow(true),
        WindowBound::UnboundedFollowing => Bound::UnboundedFollowing(true),
        WindowBound::Preceding(expression) => {
            Bound::Preceding(window_bound_literal(fragment, *expression, context)?)
        }
        WindowBound::Following(expression) => {
            Bound::Following(window_bound_literal(fragment, *expression, context)?)
        }
    };
    Ok(expr::WindowBound { bound: Some(bound) })
}

fn window_bound_literal(
    fragment: &Fragment,
    expression: ExprId,
    context: ExpressionEncodingContext<'_>,
) -> Result<i64, PhysicalEncodeError> {
    let node = fragment.expressions().get(expression).ok_or_else(|| {
        PhysicalEncodeError::Invalid("native wire v1 window offset expression is missing".into())
    })?;
    let ExprKind::Constant(reference) = node.kind else {
        return Err("native wire v1 window bound must reference an exact checked constant".into());
    };
    cv_projection::window_offset(reference, &node.ty, context)
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
