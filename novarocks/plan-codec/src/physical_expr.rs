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

use novarocks_physical_plan::{
    BinaryOperator, ExprId, ExprKind, Fragment, FunctionId, NodeId, NullOrdering, SortDirection,
    SortExpr, UnaryOperator, ValueId, WindowBound, WindowFrame, WindowFrameExclusion,
    WindowFrameUnits,
};
use novarocks_proto_models::{common, expr};

use crate::host_projection_v2::ProjectionFailure;
use crate::physical_encode::fragment_type_owner::{
    PreparedFragmentTypes, TypeEncodingResources, TypeEncodingResult,
};
use crate::physical_encode::fragment_type_owner::{admitted_box, admitted_vec};
use crate::{PhysicalEncodeError, WireLayout, WireSlotId};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

#[path = "cv_projection.rs"]
mod cv_projection;
#[path = "physical_expr/frozen_scalar.rs"]
mod frozen_scalar;

#[derive(Clone, Copy)]
pub(crate) struct ExpressionEncodingContext<'a> {
    pub(crate) constants: &'a novarocks_physical_plan::ConstantPools,
    pub(crate) control: &'a dyn PureCompileControl,
    pub(crate) types: Option<&'a PreparedFragmentTypes<'a>>,
    pub(crate) writer_calls: Option<
        &'a std::collections::BTreeMap<
            novarocks_physical_plan::PhysicalCallSite,
            novarocks_physical_plan::FrozenPhysicalCall,
        >,
    >,
}
impl ExpressionEncodingContext<'_> {
    pub(crate) fn type_reference<H>(
        self,
        value: &novarocks_physical_plan::ValueType,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<novarocks_proto_models::common::TypeDesc, H> {
        let types = self
            .types
            .ok_or("native expression requires its complete fragment type owner")?;
        types.reference(value, || {
            resources.work.step().map_err(ProjectionFailure::from)
        })
    }
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
    costs: Vec<(ExprId, WireExpressionCost)>,
    charged_messages: usize,
    charged_dynamic_bytes: usize,
}

impl<'a> WireExpressionPreflight<'a> {
    /// Pure expansion adapter. The production pass uses the same observed body.
    pub(crate) fn try_new(
        fragment: &'a Fragment,
        context: ExpressionEncodingContext<'_>,
    ) -> Result<Self, PhysicalEncodeError> {
        let mut request = |_: std::alloc::Layout| {
            Ok::<_, crate::host_projection_v2::AdmissionRefusal<std::convert::Infallible>>(())
        };
        let mut resources = TypeEncodingResources {
            request: &mut request,
            work: CompileCheckpoints::try_new(context.control, CompilePhase::Encode)?,
            type_policy: crate::physical_encode::fragment_type_owner::ExecutionTypeEncodingPolicy::LegacyInline,
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        };
        let result = Self::try_new_observed(fragment, context, &mut resources)
            .map_err(ProjectionFailure::without_host);
        if matches!(&result, Err(PhysicalEncodeError::Control(_))) {
            return result;
        }
        resources.work.finish()?;
        result
    }
    pub(crate) fn try_new_observed<H>(
        fragment: &'a Fragment,
        _context: ExpressionEncodingContext<'_>,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        let count = fragment.expressions().len();
        let mut nodes = resources.vector(count)?;
        let mut edge_count = 0usize;
        for (id, expression) in fragment.expressions().iter() {
            resources.work.step()?;
            nodes.push((*id, (expression, 0usize, None::<WireExpressionCost>)));
            visit_expression_children(&expression.kind, &mut |_| {
                edge_count = edge_count
                    .checked_add(1)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                resources.work.step()?;
                Ok::<_, ProjectionFailure<PhysicalEncodeError, H>>(())
            })?;
        }
        // ExprArena source iteration is the stable ID order; graph views borrow
        // genuine source nodes and never copy their type/signature trees.
        let mut edges = resources.vector(edge_count)?;
        for (id, (expression, _, _)) in &nodes {
            visit_expression_children(&expression.kind, &mut |child| {
                resources.work.step()?;
                if find_expression(&nodes, child, &mut resources.work)?.is_none() {
                    return Err(format!(
                        "fragment {} expression {} references missing expression {}",
                        fragment.id().get(),
                        id.get(),
                        child.get()
                    )
                    .into());
                }
                edges.push((child, *id));
                Ok::<_, ProjectionFailure<PhysicalEncodeError, H>>(())
            })?;
        }
        crate::physical_encode::fragment_type_owner::sort_ord_observed(
            &mut edges,
            &mut resources.work,
        )?;
        let mut write = 0usize;
        for read in 0..edges.len() {
            resources.work.step()?;
            if write == 0 || edges[read] != edges[write - 1] {
                edges[write] = edges[read];
                write += 1;
            }
        }
        edges.truncate(write);
        for (_, parent) in &edges {
            let index = find_expression(&nodes, *parent, &mut resources.work)?
                .ok_or("expression dependent disappeared")?;
            nodes[index].1.1 = nodes[index]
                .1
                .1
                .checked_add(1)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        }
        let mut ready = resources.vector(nodes.len())?;
        for (index, (_, (_, remaining, _))) in nodes.iter().enumerate() {
            resources.work.step()?;
            if *remaining == 0 {
                ready.push(index);
            }
        }
        let mut finished = 0usize;
        while let Some(index) = ready.pop() {
            resources.work.step()?;
            let (id, (expression, _, _)) = nodes[index];
            let (additional, payload) = match expression.kind {
                ExprKind::Constant(reference) => {
                    // One pool address replaces a duplicate inline literal DTO.
                    // Pool IPC and its complete source owner are emitted once
                    // by the fragment's authoritative constant manifest.
                    let _ = reference;
                    (1, 0)
                }
                ExprKind::Literal(_) => {
                    return Err("native wire v1 requires a checked constant reference".into());
                }
                _ => (0, 0),
            };
            let mut messages =
                local_expression_messages(&expression.kind).saturating_add(additional);
            let mut dynamic_bytes =
                local_expression_dynamic_bytes(expression, resources.type_policy.complete())
                    .saturating_add(payload);
            let mut depth = 4usize;
            visit_expression_children(&expression.kind, &mut |child| {
                resources.work.step()?;
                let child = find_expression(&nodes, child, &mut resources.work)?
                    .and_then(|index| nodes[index].1.2)
                    .ok_or("expression dependency was not ordered")?;
                messages = capped_add(
                    messages,
                    child.messages,
                    NATIVE_V1_MAX_EXPANDED_EXPR_MESSAGES,
                );
                dynamic_bytes = capped_add(
                    dynamic_bytes,
                    child.dynamic_bytes,
                    NATIVE_V1_MAX_EXPANDED_EXPR_DYNAMIC_BYTES,
                );
                depth = depth.max(child.depth.saturating_add(
                    if matches!(&expression.kind, ExprKind::Case { .. }) {
                        3
                    } else {
                        2
                    },
                ));
                Ok::<_, ProjectionFailure<PhysicalEncodeError, H>>(())
            })?;
            nodes[index].1.2 = Some(WireExpressionCost {
                messages,
                dynamic_bytes,
                depth,
            });
            finished += 1;
            let (mut low, mut high) = (0, edges.len());
            while low < high {
                resources.work.step()?;
                let middle = low + (high - low) / 2;
                if edges[middle].0 < id {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            while low < edges.len() && edges[low].0 == id {
                resources.work.step()?;
                let user = find_expression(&nodes, edges[low].1, &mut resources.work)?
                    .ok_or("expression dependent disappeared")?;
                nodes[user].1.1 -= 1;
                if nodes[user].1.1 == 0 {
                    ready.push(user);
                }
                low += 1;
            }
        }
        if finished != count {
            return Err(format!(
                "fragment {} expression graph contains a cycle",
                fragment.id().get()
            )
            .into());
        }
        let mut costs = resources.vector(nodes.len())?;
        for (id, (_, _, cost)) in nodes {
            resources.work.step()?;
            costs.push((id, cost.expect("complete source graph")));
        }
        Ok(Self {
            fragment,
            costs,
            charged_messages: 0,
            charged_dynamic_bytes: 0,
        })
    }
    pub(crate) fn charge_observed<H>(
        &mut self,
        expression: ExprId,
        enclosing_depth: usize,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<(), H> {
        let index = find_expression(&self.costs, expression, &mut resources.work)?
            .ok_or("expression is missing during wire preflight")?;
        self.charge_cost(expression, enclosing_depth, self.costs[index].1)
            .map_err(ProjectionFailure::from)
    }
    pub(crate) fn charge(
        &mut self,
        expression: ExprId,
        enclosing_depth: usize,
    ) -> Result<(), String> {
        let index = self
            .costs
            .binary_search_by_key(&expression, |entry| entry.0)
            .map_err(|_| {
                format!(
                    "fragment {} is missing expression {} during wire preflight",
                    self.fragment.id().get(),
                    expression.get()
                )
            })?;
        self.charge_cost(expression, enclosing_depth, self.costs[index].1)
    }
    fn charge_cost(
        &mut self,
        expression: ExprId,
        enclosing_depth: usize,
        cost: WireExpressionCost,
    ) -> Result<(), String> {
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
pub(crate) fn encode_constant_payload<H>(
    fragment: &Fragment,
    expression: ExprId,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<Option<common::LiteralValue>, H> {
    let Some(node) = fragment.expressions().get(expression) else {
        return Ok(None);
    };
    match node.kind {
        ExprKind::Constant(reference) => {
            cv_projection::encode(reference, &node.ty, context, resources).map(Some)
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

fn find_expression<T>(
    values: &[(ExprId, T)],
    id: ExprId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, novarocks_type_contract::CompileControlError> {
    let (mut low, mut high) = (0, values.len());
    while low < high {
        work.step()?;
        let middle = low + (high - low) / 2;
        match values[middle].0.cmp(&id) {
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
        }
    }
    Ok(None)
}
fn visit_expression_children<E>(
    kind: &ExprKind,
    emit: &mut impl FnMut(ExprId) -> Result<(), E>,
) -> Result<(), E> {
    match kind {
        ExprKind::Value(_)
        | ExprKind::LambdaParameter { .. }
        | ExprKind::Constant(_)
        | ExprKind::Literal(_) => {}
        ExprKind::Unary { expr, .. }
        | ExprKind::Lambda { body: expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => emit(*expr)?,
        ExprKind::Binary { left, right, .. } => {
            emit(*left)?;
            emit(*right)?;
        }
        ExprKind::Conjunction { args }
        | ExprKind::Disjunction { args }
        | ExprKind::FunctionCall { args, .. } => {
            for child in args {
                emit(*child)?;
            }
        }
        ExprKind::InList { expr, list, .. } => {
            emit(*expr)?;
            for child in list {
                emit(*child)?;
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            emit(*expr)?;
            emit(*low)?;
            emit(*high)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            emit(*expr)?;
            emit(*pattern)?;
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            for child in operand {
                emit(*child)?;
            }
            for (when, then) in when_then {
                emit(*when)?;
                emit(*then)?;
            }
            for child in else_expr {
                emit(*child)?;
            }
        }
        ExprKind::WindowCall {
            args,
            function_order_by,
            frame,
            ..
        } => {
            for child in args {
                emit(*child)?;
            }
            for item in function_order_by {
                emit(item.expr)?;
            }
            if let Some(frame) = frame {
                for bound in [&frame.start, &frame.end] {
                    if let WindowBound::Preceding(child) | WindowBound::Following(child) = bound {
                        emit(*child)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn local_expression_messages(kind: &ExprKind) -> usize {
    let additional = match kind {
        ExprKind::Case { when_then, .. } => when_then.len(),
        _ => 0,
    };
    8_usize.saturating_add(additional)
}

fn local_expression_dynamic_bytes(
    expression: &novarocks_physical_plan::ExprNode,
    complete_types: bool,
) -> usize {
    let dynamic = match &expression.kind {
        ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
            function.function_id.as_str().len()
        }
        _ => 0,
    };
    if complete_types {
        // TypeDesc now emits only a binding reference. Parameter/name bytes are
        // constructed and bounded once by their execution-table owner, rather
        // than repeated in every expanded expression occurrence.
        return dynamic;
    }
    let type_bytes = timestamp_timezone_bytes(&expression.ty.data_type()).saturating_add(
        match &expression.kind {
            ExprKind::Cast { target, .. } => timestamp_timezone_bytes(target),
            _ => 0,
        },
    );
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
    Exact(&'a [(ValueId, WireSlotId)]),
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

fn semantic_parameter_reference<H>(
    reference: &novarocks_type_contract::SemanticParameterRef,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<novarocks_proto_models::physical_semantics_v2::SemanticParameterRef, H> {
    crate::physical_semantics_v2::encode_semantic_parameter_reference_in(
        reference,
        &mut resources.work,
    )
    .map_err(|error| {
        ProjectionFailure::Codec(match error {
            crate::physical_semantics_v2::SemanticsCodecError::Control(cause) => {
                PhysicalEncodeError::Control(cause)
            }
            error => PhysicalEncodeError::Invalid(error.to_string()),
        })
    })
}

pub(crate) fn encode_physical_expr<H>(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression: ExprId,
    resolution: ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::Expr, H> {
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
        r#type: Some(context.type_reference(&node.ty, resources)?),
        nullable: node.ty.nullable(),
        original_definition_id: Some(expression.get()),
        kind: Some(encode_kind(
            fragment,
            layout,
            owner,
            expression,
            &node.ty,
            &node.kind,
            &resolution,
            context,
            resources,
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
fn encode_connective<H>(
    op: expr::BinaryOp,
    args: &[ExprId],
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression_type: &novarocks_physical_plan::ValueType,
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::expr::Kind, H> {
    let nullable = args.iter().try_fold(false, |nullable, arg| {
        fragment
            .expressions()
            .get(*arg)
            .map(|node| nullable || node.ty.nullable())
            .ok_or_else(|| {
                format!(
                    "fragment {} expression {} is not defined",
                    fragment.id().get(),
                    arg.get()
                )
            })
    })?;
    macro_rules! build {
        ($range:expr) => {
            encode_connective_range(
                op,
                $range,
                fragment,
                layout,
                owner,
                expression_type,
                nullable,
                resolution,
                context,
                resources,
            )
        };
    }
    let (left, right) = split_connective(args)?;
    Ok(expr::expr::Kind::BinaryOp(admitted_box!(
        resources,
        expr::BinaryOpExpr,
        expr::BinaryOpExpr {
            op: op as i32,
            decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
            allow_throw_exception: None,
            left: Some(admitted_box!(resources, expr::Expr, build!(left)?)),
            right: Some(admitted_box!(resources, expr::Expr, build!(right)?)),
        }
    )))
}

/// Folds a filter's conjunct list into the single predicate expression native
/// wire v1 spells, using the same balanced shape as an n-ary connective.
pub(crate) fn encode_predicate_conjunction<H>(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    predicates: &[ExprId],
    resolution: ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::Expr, H> {
    let Some(first) = predicates.first() else {
        return Err(format!(
            "fragment {} node {} filter has no predicate",
            fragment.id().get(),
            owner.get()
        )
        .into());
    };
    if predicates.len() == 1 {
        return encode_physical_expr(
            fragment, layout, owner, *first, resolution, context, resources,
        );
    }
    let nullable = predicates.iter().try_fold(false, |nullable, predicate| {
        fragment
            .expressions()
            .get(*predicate)
            .map(|node| nullable || node.ty.nullable())
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
        context
            .types
            .map(|types| {
                types.predicate_type(owner, || {
                    resources.work.step().map_err(ProjectionFailure::from)
                })
            })
            .transpose()?
            .unwrap_or(
                &fragment
                    .expressions()
                    .get(*first)
                    .expect("checked predicate author")
                    .ty,
            ),
        nullable,
        &resolution,
        context,
        resources,
    )
}

/// Splits an argument list into two halves, preserving order.
fn split_connective(args: &[ExprId]) -> Result<(&[ExprId], &[ExprId]), String> {
    if args.len() < 2 {
        return Err("boolean connective requires at least two arguments".into());
    }
    Ok(args.split_at(args.len() / 2))
}

fn encode_connective_range<H>(
    op: expr::BinaryOp,
    args: &[ExprId],
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression_type: &novarocks_physical_plan::ValueType,
    nullable: bool,
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::Expr, H> {
    if let [single] = args {
        return encode_physical_expr(
            fragment,
            layout,
            owner,
            *single,
            copy_resolution(resolution),
            context,
            resources,
        );
    }
    let (left, right) = split_connective(args)?;
    Ok(expr::Expr {
        original_definition_id: None,
        r#type: Some(context.type_reference(expression_type, resources)?),
        nullable,
        kind: Some(expr::expr::Kind::BinaryOp(admitted_box!(
            resources,
            expr::BinaryOpExpr,
            expr::BinaryOpExpr {
                op: op as i32,
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                allow_throw_exception: None,
                left: Some(admitted_box!(
                    resources,
                    expr::Expr,
                    encode_connective_range(
                        op,
                        left,
                        fragment,
                        layout,
                        owner,
                        expression_type,
                        nullable,
                        resolution,
                        context,
                        resources,
                    )?
                )),
                right: Some(admitted_box!(
                    resources,
                    expr::Expr,
                    encode_connective_range(
                        op,
                        right,
                        fragment,
                        layout,
                        owner,
                        expression_type,
                        nullable,
                        resolution,
                        context,
                        resources,
                    )?
                )),
            }
        ))),
    })
}

fn encode_kind<H>(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expression: ExprId,
    expression_type: &novarocks_physical_plan::ValueType,
    kind: &ExprKind,
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::expr::Kind, H> {
    use expr::expr::Kind;

    macro_rules! child {
        ($id:expr) => {
            encode_physical_expr(
                fragment,
                layout,
                owner,
                $id,
                copy_resolution(resolution),
                context,
                resources,
            )
        };
    }
    Ok(match kind {
        ExprKind::Value(value) => Kind::ColumnRef(expr::ColumnRef {
            column_id: resolve_value(layout, owner, *value, resolution, resources)?.get_u32(),
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
        ExprKind::Constant(reference) => {
            Kind::ConstantReference(novarocks_proto_models::call_request::ConstantReference {
                pool_id: Some(reference.pool.get()),
                row_ordinal: reference.ordinal,
            })
        }
        ExprKind::Unary { op, expr: operand } => Kind::UnaryOp(admitted_box!(
            resources,
            expr::UnaryOpExpr,
            expr::UnaryOpExpr {
                op: encode_unary(*op)? as i32,
                operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
            }
        )),
        ExprKind::Binary {
            left,
            op,
            right,
            decimal_overflow_policy,
            allow_throw_exception,
        } => Kind::BinaryOp(admitted_box!(
            resources,
            expr::BinaryOpExpr,
            expr::BinaryOpExpr {
                op: encode_binary(*op)? as i32,
                decimal_overflow_policy: encode_decimal_overflow_policy(*decimal_overflow_policy),
                allow_throw_exception: allow_throw_exception
                    .as_ref()
                    .map(|reference| semantic_parameter_reference(reference, resources))
                    .transpose()?,
                left: Some(admitted_box!(resources, expr::Expr, child!(*left)?)),
                right: Some(admitted_box!(resources, expr::Expr, child!(*right)?)),
            }
        )),
        ExprKind::Conjunction { args } => encode_connective(
            expr::BinaryOp::And,
            args,
            fragment,
            layout,
            owner,
            expression_type,
            resolution,
            context,
            resources,
        )?,
        ExprKind::Disjunction { args } => encode_connective(
            expr::BinaryOp::Or,
            args,
            fragment,
            layout,
            owner,
            expression_type,
            resolution,
            context,
            resources,
        )?,
        ExprKind::FunctionCall { function, args } => {
            let (frozen_binding, original_request_id) =
                frozen_scalar::encode_frozen_scalar(expression, function, context, resources)?;
            Kind::FunctionCall(expr::FunctionCall {
                function_name: resources.copy_string(wire_function_name(&function.function_id)?)?,
                args: encode_exprs(
                    fragment, layout, owner, args, resolution, context, resources,
                )?,
                distinct: false,
                frozen_binding: Some(frozen_binding),
                original_request_id: Some(original_request_id),
            })
        }
        ExprKind::Lambda {
            parameter_types,
            body,
        } => Kind::Lambda(admitted_box!(
            resources,
            expr::LambdaExpr,
            expr::LambdaExpr {
                params: admitted_vec!(
                    resources,
                    parameter_types.len(),
                    parameter_types.iter().enumerate().map(|(ordinal, ty)| {
                        let ordinal = u32::try_from(ordinal)
                            .map_err(|_| "lambda parameter ordinal exceeds u32".to_string())?;
                        Ok(expr::LambdaParam {
                            slot_id: lambda_parameter_slot(expression, ordinal)?,
                            name: None,
                            r#type: Some(context.type_reference(ty, resources)?),
                            nullable: ty.nullable(),
                        })
                    })
                ),
                body: Some(admitted_box!(resources, expr::Expr, child!(*body)?)),
            }
        )),
        ExprKind::Cast {
            expr: operand,
            target,
            decimal_overflow_policy,
            allow_throw_exception,
        } => {
            if !novarocks_type_contract::arrow_data_types_exact_observed(
                expression_type.data_type(),
                target,
                || resources.work.step().map_err(ProjectionFailure::from),
            )? {
                return Err("cast target differs from its exact declared binding layout".into());
            }
            Kind::Cast(admitted_box!(
                resources,
                expr::CastExpr,
                expr::CastExpr {
                    operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
                    target: Some(context.type_reference(expression_type, resources)?),
                    decimal_overflow_policy: encode_decimal_overflow_policy(
                        *decimal_overflow_policy
                    ),
                    allow_throw_exception: Some(semantic_parameter_reference(
                        allow_throw_exception,
                        resources,
                    )?),
                }
            ))
        }
        ExprKind::IsNull {
            expr: operand,
            negated,
        } => Kind::IsNull(admitted_box!(
            resources,
            expr::IsNullExpr,
            expr::IsNullExpr {
                operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
                negated: *negated,
            }
        )),
        ExprKind::InList {
            expr: operand,
            list,
            negated,
        } => Kind::InList(admitted_box!(
            resources,
            expr::InListExpr,
            expr::InListExpr {
                operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
                list: encode_exprs(
                    fragment, layout, owner, list, resolution, context, resources,
                )?,
                negated: *negated,
            }
        )),
        ExprKind::Between {
            expr: operand,
            low,
            high,
            negated,
        } => Kind::Between(admitted_box!(
            resources,
            expr::BetweenExpr,
            expr::BetweenExpr {
                operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
                low: Some(admitted_box!(resources, expr::Expr, child!(*low)?)),
                high: Some(admitted_box!(resources, expr::Expr, child!(*high)?)),
                negated: *negated,
            }
        )),
        ExprKind::Like {
            expr: operand,
            pattern,
            negated,
        } => Kind::Like(admitted_box!(
            resources,
            expr::LikeExpr,
            expr::LikeExpr {
                operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
                pattern: Some(admitted_box!(resources, expr::Expr, child!(*pattern)?)),
                negated: *negated,
            }
        )),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => Kind::CaseExpr(admitted_box!(
            resources,
            expr::CaseExpr,
            expr::CaseExpr {
                operand: operand
                    .map(|id| {
                        resources.request_fixed::<expr::Expr>()?;
                        child!(id).map(Box::new)
                    })
                    .transpose()?,
                when_then: admitted_vec!(
                    resources,
                    when_then.len(),
                    when_then.iter().map(|(when, then)| {
                        Ok(expr::WhenThen {
                            when: Some(child!(*when)?),
                            then: Some(child!(*then)?),
                        })
                    })
                ),
                else_expr: else_expr
                    .map(|id| {
                        resources.request_fixed::<expr::Expr>()?;
                        child!(id).map(Box::new)
                    })
                    .transpose()?,
            }
        )),
        ExprKind::IsTruthValue {
            expr: operand,
            value,
            negated,
        } => Kind::IsTruth(admitted_box!(
            resources,
            expr::IsTruthExpr,
            expr::IsTruthExpr {
                operand: Some(admitted_box!(resources, expr::Expr, child!(*operand)?)),
                value: *value,
                negated: *negated,
            }
        )),
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

pub(crate) fn encode_exprs<H>(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    expressions: &[ExprId],
    resolution: &ValueResolution<'_>,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<Vec<expr::Expr>, H> {
    let mut output = resources.vector(expressions.len())?;
    for expression in expressions {
        resources.work.step()?;
        output.push(encode_physical_expr(
            fragment,
            layout,
            owner,
            *expression,
            copy_resolution(resolution),
            context,
            resources,
        )?);
    }
    Ok(output)
}
pub(crate) fn encode_sort_items<H>(
    fragment: &Fragment,
    layout: &WireLayout,
    owner: NodeId,
    items: &[SortExpr],
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<Vec<expr::SortItem>, H> {
    let mut output = resources.vector(items.len())?;
    for item in items {
        resources.work.step()?;
        output.push(expr::SortItem {
            expr: Some(encode_physical_expr(
                fragment,
                layout,
                owner,
                item.expr,
                ValueResolution::NodeInput,
                context,
                resources,
            )?),
            asc: item.direction == SortDirection::Ascending,
            nulls_first: item.null_ordering == NullOrdering::First,
        });
    }
    Ok(output)
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

pub(crate) fn encode_window_frame<H>(
    fragment: &Fragment,
    frame: &WindowFrame,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::WindowFrame, H> {
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
        start: Some(encode_window_bound(
            fragment,
            &frame.start,
            context,
            resources,
        )?),
        end: Some(encode_window_bound(
            fragment, &frame.end, context, resources,
        )?),
    })
}

fn encode_window_bound<H>(
    fragment: &Fragment,
    bound: &WindowBound,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<expr::WindowBound, H> {
    use expr::window_bound::Bound;

    let bound = match bound {
        WindowBound::UnboundedPreceding => Bound::UnboundedPreceding(true),
        WindowBound::CurrentRow => Bound::CurrentRow(true),
        WindowBound::UnboundedFollowing => Bound::UnboundedFollowing(true),
        WindowBound::Preceding(expression) => Bound::Preceding(window_bound_literal(
            fragment,
            *expression,
            context,
            resources,
        )?),
        WindowBound::Following(expression) => Bound::Following(window_bound_literal(
            fragment,
            *expression,
            context,
            resources,
        )?),
    };
    Ok(expr::WindowBound { bound: Some(bound) })
}

fn window_bound_literal<H>(
    fragment: &Fragment,
    expression: ExprId,
    context: ExpressionEncodingContext<'_>,
    _resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<i64, H> {
    let node = fragment.expressions().get(expression).ok_or_else(|| {
        PhysicalEncodeError::Invalid("native wire v1 window offset expression is missing".into())
    })?;
    let ExprKind::Constant(reference) = node.kind else {
        return Err("native wire v1 window bound must reference an exact checked constant".into());
    };
    cv_projection::window_offset(reference, &node.ty, context).map_err(ProjectionFailure::from)
}

fn resolve_value<H>(
    layout: &WireLayout,
    owner: NodeId,
    value: ValueId,
    resolution: &ValueResolution<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<WireSlotId, H> {
    match resolution {
        ValueResolution::NodeInput => {
            layout.input_value_slot_observed(owner, value, &mut resources.work)
        }
        ValueResolution::Exact(values) => {
            crate::physical_encode::find_sorted_key(values, &value, &mut resources.work)?
                .map(|index| values[index].1)
                .ok_or_else(|| {
                    format!(
                        "fragment {} node {} exact expression scope does not contain value {}",
                        layout.fragment().get(),
                        owner.get(),
                        value.get()
                    )
                    .into()
                })
        }
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
