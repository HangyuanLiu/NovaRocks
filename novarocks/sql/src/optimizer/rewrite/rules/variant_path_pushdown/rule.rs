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

use crate::compiler::SqlCompileError;
use arrow::datatypes::DataType;

use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::common::{LiteralValue, OutputColumn};
use crate::optimizer::operator::{FilterOp, Operator, ProjectOp, ScanOp, ScanVariantColumn};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};
use crate::planner::table::ScanSource;
use novarocks_types::value::variant::{
    VariantPathSegment, parse_variant_path, variant_get_target_type,
};

#[derive(Default)]
pub(crate) struct VariantPathPushdownRule;

#[derive(Clone, Debug, PartialEq, Eq)]
struct VariantRequest {
    source_column_id: ColumnId,
    canonical_path: String,
    requested_type: DataType,
    requested_type_literal: String,
    strict: bool,
    binding: crate::binding::SqlFunctionBinding,
}

impl LogicalRewriteRule for VariantPathPushdownRule {
    fn name(&self) -> &'static str {
        "VariantPathPushdown"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    // Keep the default Leaf pattern: candidates can be rooted at Filter,
    // Project, or Scan, while the guard is scalar/context-dependent and the
    // rewrite may mutate child scan bindings.
    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let arena_rc = ctx.scalar_arena();
        let arena = arena_rc.borrow();
        match &expr.op {
            Operator::LogicalFilter(filter) => {
                contains_variant_get_candidate_scalar(&arena, filter.predicate)
            }
            Operator::LogicalProject(project) => project
                .items
                .iter()
                .any(|item| contains_variant_get_candidate_scalar(&arena, item.expr)),
            Operator::LogicalScan(scan) => scan
                .predicates
                .iter()
                .any(|id| contains_variant_get_candidate_scalar(&arena, *id)),
            _ => false,
        }
    }

    fn apply(
        &self,
        mut expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        let Some(factory) = ctx.column_ref_factory().cloned() else {
            return Ok(RewriteResult::Unchanged);
        };
        let mut factory = factory.borrow_mut();
        let arena_rc = ctx.scalar_arena();

        let changed = match &expr.op {
            Operator::LogicalFilter(_) => {
                let filter_op = match &expr.op {
                    Operator::LogicalFilter(f) => f.clone(),
                    _ => unreachable!(),
                };
                let Some(input) = expr.children.get_mut(0) else {
                    return Ok(RewriteResult::Unchanged);
                };
                let mut arena = arena_rc.borrow_mut();
                let new_predicate = rewrite_variant_request_scalar(
                    &mut arena,
                    filter_op.predicate,
                    input,
                    &mut factory,
                    &ctx.control_view(),
                )?;
                let changed = new_predicate.is_some();
                if let Some(new_pred_id) = new_predicate {
                    expr.op = Operator::LogicalFilter(FilterOp {
                        predicate: new_pred_id,
                    });
                }
                changed
            }
            Operator::LogicalProject(_) => {
                let project_op = match &expr.op {
                    Operator::LogicalProject(p) => p.clone(),
                    _ => unreachable!(),
                };
                let Some(input) = expr.children.get_mut(0) else {
                    return Ok(RewriteResult::Unchanged);
                };
                let mut items = project_op.items;
                let mut changed = false;
                let mut arena = arena_rc.borrow_mut();
                for item in &mut items {
                    if let Some(new_expr) = rewrite_variant_request_scalar(
                        &mut arena,
                        item.expr,
                        input,
                        &mut factory,
                        &ctx.control_view(),
                    )? {
                        item.expr = new_expr;
                        changed = true;
                    }
                }
                if changed {
                    expr.op = Operator::LogicalProject(ProjectOp {
                        items,
                        output_qualifier: project_op.output_qualifier,
                    });
                }
                changed
            }
            Operator::LogicalScan(_) => {
                let scan_op = match &expr.op {
                    Operator::LogicalScan(s) => s.clone(),
                    _ => unreachable!(),
                };
                // We need &mut ScanOp to call rewrite_scan_predicates.
                // Temporarily take it out, mutate, put back.
                let mut scan = scan_op;
                let changed = rewrite_scan_predicates(
                    &mut scan,
                    &mut factory,
                    &mut arena_rc.borrow_mut(),
                    &ctx.control_view(),
                )?;
                if changed {
                    expr.op = Operator::LogicalScan(scan);
                }
                changed
            }
            _ => false,
        };

        if changed {
            Ok(RewriteResult::Changed(expr))
        } else {
            Ok(RewriteResult::Unchanged)
        }
    }
}

fn rewrite_scan_predicates(
    scan: &mut ScanOp,
    factory: &mut ColumnRefFactory,
    arena: &mut ScalarArena,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, SqlCompileError> {
    let pred_ids = std::mem::take(&mut scan.predicates);
    let mut new_pred_ids = Vec::with_capacity(pred_ids.len());
    let mut changed = false;
    for pred_id in pred_ids {
        let new_id = if let Some(new_pred) =
            rewrite_variant_request_scalar(arena, pred_id, scan, factory, control)?
        {
            changed = true;
            new_pred
        } else {
            pred_id
        };
        new_pred_ids.push(new_id);
    }
    scan.predicates = new_pred_ids;
    Ok(changed)
}

trait VariantBindings {
    fn column_ref_for(
        &mut self,
        arena: &mut ScalarArena,
        request: &VariantRequest,
        factory: &mut ColumnRefFactory,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<ScalarId>, SqlCompileError>;
}

impl VariantBindings for OptExpr {
    fn column_ref_for(
        &mut self,
        arena: &mut ScalarArena,
        request: &VariantRequest,
        factory: &mut ColumnRefFactory,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<ScalarId>, SqlCompileError> {
        find_or_create_slot(arena, self, request, factory, control)
    }
}

impl VariantBindings for ScanOp {
    fn column_ref_for(
        &mut self,
        arena: &mut ScalarArena,
        request: &VariantRequest,
        factory: &mut ColumnRefFactory,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Option<ScalarId>, SqlCompileError> {
        find_or_create_slot_on_scan(arena, self, request, factory, control)
    }
}

fn rewrite_variant_request_scalar<T: VariantBindings>(
    arena: &mut ScalarArena,
    expr: ScalarId,
    bindings: &mut T,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, SqlCompileError> {
    if let Some(request) = variant_request_scalar(arena, expr) {
        return bindings.column_ref_for(arena, &request, factory, control);
    }

    let value_type = arena.value_type(expr).clone();
    let node = arena.node(expr).clone();
    match node {
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy,
        } => {
            let new_left = rewrite_variant_request_scalar(arena, left, bindings, factory, control)?;
            let new_right =
                rewrite_variant_request_scalar(arena, right, bindings, factory, control)?;
            let changed = new_left.is_some() || new_right.is_some();
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::BinaryOp {
                            op,
                            left: new_left.unwrap_or(left),
                            right: new_right.unwrap_or(right),
                            decimal_overflow_policy,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::UnaryOp { op, child } => rewrite_unary_child(
            arena,
            child,
            bindings,
            factory,
            value_type,
            |child| ScalarNode::UnaryOp { op, child },
            control,
        ),
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            binding,
            volatility,
        } => {
            let (args, changed) = rewrite_scalar_vec(arena, &args, bindings, factory, control)?;
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::FunctionCall {
                            name,
                            args,
                            distinct,
                            binding,
                            volatility,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::LambdaFunction { params, body } => rewrite_unary_child(
            arena,
            body,
            bindings,
            factory,
            value_type,
            |body| ScalarNode::LambdaFunction { params, body },
            control,
        ),
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => {
            let (args, args_changed) =
                rewrite_scalar_vec(arena, &args, bindings, factory, control)?;
            let (order_by, order_changed) =
                rewrite_sort_keys(arena, &order_by, bindings, factory, control)?;
            let changed = args_changed || order_changed;
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::AggregateCall {
                            name,
                            args,
                            distinct,
                            order_by,
                            resolved,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::Cast {
            child,
            target,
            decimal_overflow_policy,
        } => rewrite_unary_child(
            arena,
            child,
            bindings,
            factory,
            value_type,
            |child| ScalarNode::Cast {
                child,
                target,
                decimal_overflow_policy,
            },
            control,
        ),
        ScalarNode::IsNull { child, negated } => rewrite_unary_child(
            arena,
            child,
            bindings,
            factory,
            value_type,
            |child| ScalarNode::IsNull { child, negated },
            control,
        ),
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            let new_child =
                rewrite_variant_request_scalar(arena, child, bindings, factory, control)?;
            let (list, list_changed) =
                rewrite_scalar_vec(arena, &list, bindings, factory, control)?;
            let changed = new_child.is_some() || list_changed;
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::InList {
                            child: new_child.unwrap_or(child),
                            list,
                            negated,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => {
            let new_child =
                rewrite_variant_request_scalar(arena, child, bindings, factory, control)?;
            let new_low = rewrite_variant_request_scalar(arena, low, bindings, factory, control)?;
            let new_high = rewrite_variant_request_scalar(arena, high, bindings, factory, control)?;
            let changed = new_child.is_some() || new_low.is_some() || new_high.is_some();
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::Between {
                            child: new_child.unwrap_or(child),
                            low: new_low.unwrap_or(low),
                            high: new_high.unwrap_or(high),
                            negated,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => {
            let new_child =
                rewrite_variant_request_scalar(arena, child, bindings, factory, control)?;
            let new_pattern =
                rewrite_variant_request_scalar(arena, pattern, bindings, factory, control)?;
            let changed = new_child.is_some() || new_pattern.is_some();
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::Like {
                            child: new_child.unwrap_or(child),
                            pattern: new_pattern.unwrap_or(pattern),
                            negated,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let (operand, operand_changed) =
                rewrite_optional_scalar(arena, operand, bindings, factory, control)?;
            let (when_then, pairs_changed) =
                rewrite_scalar_pairs(arena, &when_then, bindings, factory, control)?;
            let (else_expr, else_changed) =
                rewrite_optional_scalar(arena, else_expr, bindings, factory, control)?;
            let changed = operand_changed || pairs_changed || else_changed;
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::Case {
                            operand,
                            when_then,
                            else_expr,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => rewrite_unary_child(
            arena,
            child,
            bindings,
            factory,
            value_type,
            |child| ScalarNode::IsTruthValue {
                child,
                value,
                negated,
            },
            control,
        ),
        ScalarNode::Nested(child) => rewrite_unary_child(
            arena,
            child,
            bindings,
            factory,
            value_type,
            ScalarNode::Nested,
            control,
        ),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            binding,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => {
            let (args, args_changed) =
                rewrite_scalar_vec(arena, &args, bindings, factory, control)?;
            let (function_order_by, function_order_changed) =
                rewrite_sort_keys(arena, &function_order_by, bindings, factory, control)?;
            let (partition_by, partition_changed) =
                rewrite_scalar_vec(arena, &partition_by, bindings, factory, control)?;
            let (order_by, order_changed) =
                rewrite_sort_keys(arena, &order_by, bindings, factory, control)?;
            let changed =
                args_changed || function_order_changed || partition_changed || order_changed;
            changed
                .then(|| {
                    arena.intern_observed(
                        ScalarNode::WindowCall {
                            name,
                            args,
                            distinct,
                            binding,
                            function_order_by,
                            aggregate_binding,
                            partition_by,
                            order_by,
                            window_frame,
                            ignore_nulls,
                        },
                        value_type,
                        control,
                    )
                })
                .transpose()
        }
        ScalarNode::Lambda { params, body } => rewrite_unary_child(
            arena,
            body,
            bindings,
            factory,
            value_type,
            |body| ScalarNode::Lambda { params, body },
            control,
        ),
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            Ok(None)
        }
    }
}

fn rewrite_unary_child<T, F>(
    arena: &mut ScalarArena,
    child: ScalarId,
    bindings: &mut T,
    factory: &mut ColumnRefFactory,
    value_type: novarocks_type_contract::FunctionValueType,
    build: F,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, SqlCompileError>
where
    T: VariantBindings,
    F: FnOnce(ScalarId) -> ScalarNode,
{
    let Some(new_child) = rewrite_variant_request_scalar(arena, child, bindings, factory, control)?
    else {
        return Ok(None);
    };
    Ok(Some(arena.intern_observed(
        build(new_child),
        value_type,
        control,
    )?))
}

fn rewrite_scalar_vec<T: VariantBindings>(
    arena: &mut ScalarArena,
    exprs: &[ScalarId],
    bindings: &mut T,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(Vec<ScalarId>, bool), SqlCompileError> {
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        if let Some(new_expr) =
            rewrite_variant_request_scalar(arena, *expr, bindings, factory, control)?
        {
            changed = true;
            out.push(new_expr);
        } else {
            out.push(*expr);
        }
    }
    Ok((out, changed))
}

fn rewrite_optional_scalar<T: VariantBindings>(
    arena: &mut ScalarArena,
    expr: Option<ScalarId>,
    bindings: &mut T,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(Option<ScalarId>, bool), SqlCompileError> {
    let Some(expr) = expr else {
        return Ok((None, false));
    };
    match rewrite_variant_request_scalar(arena, expr, bindings, factory, control)? {
        Some(new_expr) => Ok((Some(new_expr), true)),
        None => Ok((Some(expr), false)),
    }
}

fn rewrite_scalar_pairs<T: VariantBindings>(
    arena: &mut ScalarArena,
    pairs: &[(ScalarId, ScalarId)],
    bindings: &mut T,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(Vec<(ScalarId, ScalarId)>, bool), SqlCompileError> {
    let mut changed = false;
    let mut out = Vec::with_capacity(pairs.len());
    for (left, right) in pairs {
        let new_left = rewrite_variant_request_scalar(arena, *left, bindings, factory, control)?;
        let new_right = rewrite_variant_request_scalar(arena, *right, bindings, factory, control)?;
        changed |= new_left.is_some() || new_right.is_some();
        out.push((new_left.unwrap_or(*left), new_right.unwrap_or(*right)));
    }
    Ok((out, changed))
}

fn rewrite_sort_keys<T: VariantBindings>(
    arena: &mut ScalarArena,
    keys: &[SortKey],
    bindings: &mut T,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(Vec<SortKey>, bool), SqlCompileError> {
    let mut changed = false;
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(new_expr) =
            rewrite_variant_request_scalar(arena, key.expr, bindings, factory, control)?
        {
            changed = true;
            let display = match arena.node(new_expr) {
                ScalarNode::ColumnRef(column_id) => arena.column_display(*column_id).cloned(),
                _ => None,
            };
            out.push(SortKey {
                expr: new_expr,
                asc: key.asc,
                nulls_first: key.nulls_first,
                display,
            });
        } else {
            out.push(key.clone());
        }
    }
    Ok((out, changed))
}

fn variant_request_scalar(arena: &ScalarArena, expr: ScalarId) -> Option<VariantRequest> {
    let ScalarNode::FunctionCall {
        name,
        args,
        distinct,
        binding,
        ..
    } = arena.node(expr)
    else {
        return None;
    };
    if *distinct || args.len() != 3 {
        return None;
    }

    let strict = if name.eq_ignore_ascii_case("variant_get") {
        true
    } else if name.eq_ignore_ascii_case("try_variant_get") {
        false
    } else {
        return None;
    };

    let ScalarNode::ColumnRef(column_id) = arena.node(args[0]) else {
        return None;
    };
    if *column_id == ColumnId::UNSET {
        return None;
    }
    let path = string_literal_value_scalar(arena, args[1])?;
    let requested_type_literal = string_literal_value_scalar(arena, args[2])?.to_string();
    let requested_type = requested_type_value_scalar(arena, args[2])?;
    let canonical_path = canonical_object_path(path)?;

    Some(VariantRequest {
        source_column_id: *column_id,
        canonical_path,
        requested_type,
        requested_type_literal,
        strict,
        binding: binding.clone(),
    })
}

fn string_literal_value_scalar(arena: &ScalarArena, expr: ScalarId) -> Option<&str> {
    match arena.node(expr) {
        ScalarNode::Literal(HashableLiteral(LiteralValue::String(value))) => Some(value),
        _ => None,
    }
}

fn requested_type_value_scalar(arena: &ScalarArena, expr: ScalarId) -> Option<DataType> {
    let value = string_literal_value_scalar(arena, expr)?;
    let data_type = variant_get_target_type(value).ok()?;
    match data_type {
        DataType::Boolean
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date32 => Some(data_type),
        _ => None,
    }
}

fn canonical_object_path(path: &str) -> Option<String> {
    let parsed = parse_variant_path(path).ok()?;
    if parsed.segments.is_empty() {
        return None;
    }
    let mut out = String::from("$");
    for segment in parsed.segments {
        let VariantPathSegment::ObjectKey(key) = segment else {
            return None;
        };
        append_canonical_key(&mut out, &key);
    }
    Some(out)
}

fn append_canonical_key(out: &mut String, key: &str) {
    if is_plain_path_key(key) {
        out.push('.');
        out.push_str(key);
        return;
    }
    out.push_str("['");
    for ch in key.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            _ => out.push(ch),
        }
    }
    out.push_str("']");
}

fn is_plain_path_key(key: &str) -> bool {
    !key.is_empty() && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Walk the OptExpr tree looking for a scan to add a variant column slot to.
/// Mirrors the old `find_or_create_slot` which traversed `LogicalPlanNode`.
fn find_or_create_slot(
    arena: &mut ScalarArena,
    expr: &mut OptExpr,
    request: &VariantRequest,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, SqlCompileError> {
    Ok(match &expr.op {
        Operator::LogicalScan(scan) => {
            // For strict requests, only push when the scan has no predicates of
            // its own (same condition as the pre-OptExpr implementation).
            let can_push = !request.strict
                || scan.predicates.is_empty()
                || scan
                    .predicates
                    .iter()
                    .any(|pred_id| expr_contains_variant_request_scalar(arena, *pred_id, request));
            if can_push {
                let Operator::LogicalScan(scan_mut) = &mut expr.op else {
                    return Ok(None);
                };
                find_or_create_slot_on_scan(arena, scan_mut, request, factory, control)?
            } else {
                None
            }
        }
        Operator::LogicalFilter(filter_op) => {
            // For strict requests, only descend through a Filter whose predicate
            // contains the same variant_request — this preserves the pre-OptExpr
            // semantics that prevented spurious pushdown of unrelated projections.
            if !request.strict
                || expr_contains_variant_request_scalar(arena, filter_op.predicate, request)
            {
                let Some(input) = expr.children.get_mut(0) else {
                    return Ok(None);
                };
                find_or_create_slot(arena, input, request, factory, control)?
            } else {
                None
            }
        }
        _ => None,
    })
}

fn expr_contains_variant_request_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    request: &VariantRequest,
) -> bool {
    if variant_request_scalar(arena, expr).is_some_and(|candidate| candidate == *request) {
        return true;
    }

    match arena.node(expr) {
        ScalarNode::BinaryOp { left, right, .. } => {
            expr_contains_variant_request_scalar(arena, *left, request)
                || expr_contains_variant_request_scalar(arena, *right, request)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => expr_contains_variant_request_scalar(arena, *child, request),
        ScalarNode::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| expr_contains_variant_request_scalar(arena, *arg, request)),
        ScalarNode::AggregateCall { args, order_by, .. } => {
            args.iter()
                .any(|arg| expr_contains_variant_request_scalar(arena, *arg, request))
                || order_by
                    .iter()
                    .any(|item| expr_contains_variant_request_scalar(arena, item.expr, request))
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|arg| expr_contains_variant_request_scalar(arena, *arg, request))
                || partition_by
                    .iter()
                    .any(|expr| expr_contains_variant_request_scalar(arena, *expr, request))
                || order_by
                    .iter()
                    .any(|item| expr_contains_variant_request_scalar(arena, item.expr, request))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            expr_contains_variant_request_scalar(arena, *body, request)
        }
        ScalarNode::InList { child, list, .. } => {
            expr_contains_variant_request_scalar(arena, *child, request)
                || list
                    .iter()
                    .any(|item| expr_contains_variant_request_scalar(arena, *item, request))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            expr_contains_variant_request_scalar(arena, *child, request)
                || expr_contains_variant_request_scalar(arena, *low, request)
                || expr_contains_variant_request_scalar(arena, *high, request)
        }
        ScalarNode::Like { child, pattern, .. } => {
            expr_contains_variant_request_scalar(arena, *child, request)
                || expr_contains_variant_request_scalar(arena, *pattern, request)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| expr_contains_variant_request_scalar(arena, expr, request))
                || when_then.iter().any(|(when, then)| {
                    expr_contains_variant_request_scalar(arena, *when, request)
                        || expr_contains_variant_request_scalar(arena, *then, request)
                })
                || else_expr
                    .is_some_and(|expr| expr_contains_variant_request_scalar(arena, expr, request))
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}

fn find_or_create_slot_on_scan(
    arena: &mut ScalarArena,
    scan: &mut ScanOp,
    request: &VariantRequest,
    factory: &mut ColumnRefFactory,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, SqlCompileError> {
    if !matches!(
        scan.table.source,
        ScanSource::Sql(crate::planner::table::SqlScanSource {
            kind: crate::planner::table::SqlScanKind::Data { .. }
                | crate::planner::table::SqlScanKind::FrozenInputSet { .. },
            ..
        })
    ) {
        return Ok(None);
    }

    if let Some(existing) = scan.variant_columns.iter().find(|column| {
        column.source_column_id == request.source_column_id
            && column.canonical_path == request.canonical_path
            && column.requested_type == request.requested_type
            && column.requested_type_literal == request.requested_type_literal
            && column.strict == request.strict
            && column.binding == request.binding
    }) {
        return Ok(Some(column_ref_for_variant_slot(arena, existing, control)?));
    }

    let source_column = scan
        .columns
        .iter()
        .find(|column| column.column_id == request.source_column_id);
    let Some(source_column) = source_column else {
        return Ok(None);
    };
    if source_column.value_type.data_type != DataType::LargeBinary {
        return Ok(None);
    }

    let source_name = source_column.name.clone();
    let synthetic_name = next_synthetic_column_name(scan, &source_name);
    let synthetic_column_id = factory.create(
        None,
        synthetic_name.clone(),
        novarocks_type_contract::FunctionValueType::new(request.requested_type.clone(), true),
    );
    let descriptor = ScanVariantColumn {
        source_column_id: request.source_column_id,
        source_column: source_name,
        synthetic_column_id,
        synthetic_column: synthetic_name.clone(),
        canonical_path: request.canonical_path.clone(),
        requested_type: request.requested_type.clone(),
        requested_type_literal: request.requested_type_literal.clone(),
        strict: request.strict,
        binding: request.binding.clone(),
    };
    scan.columns.push(OutputColumn {
        column_id: synthetic_column_id,
        name: synthetic_name,
        value_type: novarocks_type_contract::FunctionValueType::new(
            request.requested_type.clone(),
            true,
        ),

        // Optimizer-managed scan output must survive pruning until the
        // lowering/codegen path consumes `variant_columns`.
        is_internal: true,
    });
    scan.variant_columns.push(descriptor);
    let descriptor = scan.variant_columns.last().expect("variant descriptor");
    Ok(Some(column_ref_for_variant_slot(
        arena, descriptor, control,
    )?))
}

fn column_ref_for_variant_slot(
    arena: &mut ScalarArena,
    descriptor: &ScanVariantColumn,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, SqlCompileError> {
    arena.remember_source_column_display(
        descriptor.synthetic_column_id,
        None,
        descriptor.synthetic_column.clone(),
    );
    arena.intern_observed(
        ScalarNode::ColumnRef(descriptor.synthetic_column_id),
        novarocks_type_contract::FunctionValueType::new(descriptor.requested_type.clone(), true),
        control,
    )
}

fn next_synthetic_column_name(scan: &ScanOp, source_column: &str) -> String {
    let source = sanitize_column_name(source_column);
    let mut ordinal = scan.variant_columns.len();
    loop {
        let candidate = format!("__nr_var_{source}_{ordinal}");
        if !scan.columns.iter().any(|column| column.name == candidate) {
            return candidate;
        }
        ordinal += 1;
    }
}

fn sanitize_column_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "col".to_string()
    } else {
        out
    }
}

fn contains_variant_get_candidate_scalar(arena: &ScalarArena, expr: ScalarId) -> bool {
    if let ScalarNode::FunctionCall { name, args, .. } = arena.node(expr)
        && args.len() == 3
        && (name.eq_ignore_ascii_case("variant_get")
            || name.eq_ignore_ascii_case("try_variant_get"))
    {
        return true;
    }

    match arena.node(expr) {
        ScalarNode::BinaryOp { left, right, .. } => {
            contains_variant_get_candidate_scalar(arena, *left)
                || contains_variant_get_candidate_scalar(arena, *right)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::Cast { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. }
        | ScalarNode::Nested(child) => contains_variant_get_candidate_scalar(arena, *child),
        ScalarNode::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| contains_variant_get_candidate_scalar(arena, *arg)),
        ScalarNode::AggregateCall { args, order_by, .. } => {
            args.iter()
                .any(|arg| contains_variant_get_candidate_scalar(arena, *arg))
                || order_by
                    .iter()
                    .any(|item| contains_variant_get_candidate_scalar(arena, item.expr))
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|arg| contains_variant_get_candidate_scalar(arena, *arg))
                || partition_by
                    .iter()
                    .any(|expr| contains_variant_get_candidate_scalar(arena, *expr))
                || order_by
                    .iter()
                    .any(|item| contains_variant_get_candidate_scalar(arena, item.expr))
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            contains_variant_get_candidate_scalar(arena, *body)
        }
        ScalarNode::InList { child, list, .. } => {
            contains_variant_get_candidate_scalar(arena, *child)
                || list
                    .iter()
                    .any(|item| contains_variant_get_candidate_scalar(arena, *item))
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            contains_variant_get_candidate_scalar(arena, *child)
                || contains_variant_get_candidate_scalar(arena, *low)
                || contains_variant_get_candidate_scalar(arena, *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            contains_variant_get_candidate_scalar(arena, *child)
                || contains_variant_get_candidate_scalar(arena, *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.is_some_and(|expr| contains_variant_get_candidate_scalar(arena, expr))
                || when_then.iter().any(|(when, then)| {
                    contains_variant_get_candidate_scalar(arena, *when)
                        || contains_variant_get_candidate_scalar(arena, *then)
                })
                || else_expr.is_some_and(|expr| contains_variant_get_candidate_scalar(arena, expr))
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            false
        }
    }
}
