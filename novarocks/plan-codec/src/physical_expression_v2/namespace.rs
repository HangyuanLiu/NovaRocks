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

use super::{ExpressionCodecError as Error, PreparedExpressionIds, kind::encode_kind};
use crate::{
    allocation_exit_v2::reserve_exit,
    borrowed_type_resources::verify_type_binding,
    physical_aggregate_binding_v2::{EncodedAggregateBindings, verify_aggregate_signature},
    physical_binding_v2::{EncodedFunctionBindings, verify_scalar_signature},
    physical_semantics_v2::SemanticsCodecError,
    physical_type_v2::EncodedTypeTable,
};
use novarocks_physical_plan::{
    BinaryOperator, ConstantPools, ExprArena, ExprId, ExprKind, ExprNode,
};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, SemanticParameterKey, SemanticParameters,
    arrow_data_types_exact_borrowed_observed,
};
use std::alloc::Layout;

/// One ID binding per original definition, in ExprArena's canonical order.
/// This authoring order does not constrain a future receiving wire order.
#[derive(Clone, Copy)]
pub struct ExpressionTypeIds<'a> {
    pub expr: ExprId,
    pub value_type_id: u32,
    pub lambda_parameter_type_ids: &'a [u32],
    pub function_binding_id: Option<u32>,
    pub aggregate_binding_id: Option<u32>,
}
#[derive(Clone, Copy, Debug)]
pub struct ExpressionProjectionLimits {
    pub max_definitions: usize,
    pub max_type_references: usize,
    pub max_expression_references: usize,
    pub max_new_allocation_requests: usize,
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_work: usize,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct ExpressionNamespaceWriteFacts {
    pub definition_count: usize,
    pub type_reference_count: usize,
    pub expression_reference_count: usize,
    pub new_allocation_requests_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
/// Original source loans prevent changing any definition, scoped parameter or
/// namespace between preflight and emission. Preparing this owner allocates no
/// heap storage and clones no source type or selected function signature.
pub struct PreparedExpressionNamespaceWrite<'loan, 'source, 'control> {
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    _functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    _aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    _parameters: &'loan SemanticParameters,
    _pools: &'loan ConstantPools,
    facts: ExpressionNamespaceWriteFacts,
    control: &'control dyn PureCompileControl,
    source_retained_bytes: usize,
}
/// The private DTO and the original immutable source loans form one emission.
/// Borrowing this owner never clones a type, signature or constant backing.
/// Consuming its DTO ends this source-correspondence capability.
pub struct EncodedExpressions<'loan, 'source, 'control> {
    source: PreparedExpressionNamespaceWrite<'loan, 'source, 'control>,
    wire: Vec<wire::ExpressionDefinition>,
}
impl<'loan, 'source, 'control> EncodedExpressions<'loan, 'source, 'control> {
    pub fn as_wire(&self) -> &[wire::ExpressionDefinition] {
        &self.wire
    }
    pub fn into_wire(self) -> Vec<wire::ExpressionDefinition> {
        self.wire
    }
    pub const fn facts(&self) -> &ExpressionNamespaceWriteFacts {
        &self.source.facts
    }
    pub fn source_count(&self) -> usize {
        self.source.arena.len()
    }
    pub fn arena(&self) -> &'loan ExprArena {
        self.source.arena
    }
    pub fn types(&self) -> &'loan EncodedTypeTable<'source> {
        self.source.types
    }
    pub fn functions(&self) -> &'loan EncodedFunctionBindings<'loan, 'source> {
        self.source._functions
    }
    pub fn aggregates(&self) -> &'loan EncodedAggregateBindings<'loan, 'source> {
        self.source._aggregates
    }
    pub fn parameters(&self) -> &'loan SemanticParameters {
        self.source._parameters
    }
    pub fn pools(&self) -> &'loan ConstantPools {
        self.source._pools
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.source.control
    }
    /// Numeric cost of one original sparse lookup for an encompassing caller
    /// to accumulate before delegation. This is not a separate resource grant.
    pub fn lookup_work_upper_bound(&self) -> Result<usize, Error> {
        tree_lookup_work(self.source_count())
    }
    pub fn expression(&self, id: u32) -> Result<Option<&'loan ExprNode>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.expression_observed(id, &mut work);
        finish(work, result)
    }
    pub(crate) fn expression_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'loan ExprNode>, Error> {
        // The actual BTree lookup remains opaque, rather than claiming its
        // internal comparisons as completed checkpoint units.
        work.flush()?;
        let node = self.source.arena.get(ExprId::new(id));
        work.flush()?;
        work.step()?;
        Ok(node)
    }
    pub fn source_id(&self, source: &ExprNode) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.source_id_observed(source, &mut work);
        finish(work, result)
    }
    pub(crate) fn source_id_observed(
        &self,
        source: &ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, Error> {
        let actual = self.expression_observed(source.id.get(), work)?;
        let same = actual.is_some_and(|actual| std::ptr::eq(actual, source));
        work.step()?;
        if !same {
            return Err(shape("expression source owner is not in this namespace"));
        }
        Ok(source.id.get())
    }
    /// Necessary live-storage floor: original whole-source invoice, inline
    /// token and actual root DTO Vec capacity. Nested DTO backing is covered
    /// by the emitted owner's request model and the caller's whole invoice;
    /// this floor is not a complete retained-size measurement or host grant.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Encode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(work, result)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let floor = add(
            self.source.source_retained_bytes,
            add(
                std::mem::size_of::<Self>(),
                bytes::<wire::ExpressionDefinition>(self.wire.capacity())?,
            )?,
        );
        work.step()?;
        floor
    }
}
impl<'loan, 'source, 'control> PreparedExpressionNamespaceWrite<'loan, 'source, 'control> {
    pub const fn facts(&self) -> &ExpressionNamespaceWriteFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<EncodedExpressions<'loan, 'source, 'control>, Error> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = (|| {
            work.flush()?;
            let mut output = Vec::new();
            let reserved = output.try_reserve_exact(self.inputs.len());
            reserve_exit::<Error>(reserved, &mut work)?;
            for ((_, node), input) in self.arena.iter().zip(self.inputs) {
                // Only CAST consumes the actual carrier ID. All other header
                // type IDs were proved against this same immutable table.
                let carrier = if matches!(node.kind, ExprKind::Cast { .. }) {
                    Some(
                        self.types
                            .root_value_binding_observed(input.value_type_id, &mut work)?
                            .ok_or_else(|| shape("prepared expression root type is absent"))?
                            .0,
                    )
                } else {
                    None
                };
                let ids = PreparedExpressionIds {
                    root_carrier_type_id: carrier,
                    lambda_parameter_type_ids: input.lambda_parameter_type_ids,
                    function_binding_id: input.function_binding_id,
                    aggregate_binding_id: input.aggregate_binding_id,
                };
                let kind = encode_kind(node, &ids, &mut work)?;
                output.push(wire::ExpressionDefinition {
                    id: node.id.get(),
                    owner_node_id: Some(node.owner.get()),
                    lambda_scope_expr_id: node.lambda_scope.map(ExprId::get),
                    value_type_id: Some(input.value_type_id),
                    kind: Some(kind),
                });
                work.step()?;
            }
            Ok(EncodedExpressions {
                source: self,
                wire: output,
            })
        })();
        finish(work, result)
    }
}
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_add(b)
        .ok_or_else(|| shape("expression writer resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    a.checked_mul(b)
        .ok_or_else(|| shape("expression writer resource product overflow"))
}
fn bytes<T>(count: usize) -> Result<usize, Error> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| shape("expression writer layout is unrepresentable"))
}
fn cap(actual: usize, limit: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let allowed = actual <= limit;
    work.step()?;
    if !allowed {
        return Err(shape("expression writer envelope exceeded"));
    }
    Ok(())
}
fn check(
    facts: &ExpressionNamespaceWriteFacts,
    limits: ExpressionProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    cap(facts.definition_count, limits.max_definitions, work)?;
    cap(facts.type_reference_count, limits.max_type_references, work)?;
    cap(
        facts.expression_reference_count,
        limits.max_expression_references,
        work,
    )?;
    cap(
        facts.new_allocation_requests_upper_bound,
        limits.max_new_allocation_requests,
        work,
    )?;
    cap(
        facts.new_allocation_request_bytes_upper_bound,
        limits.max_new_allocation_request_bytes,
        work,
    )?;
    cap(
        facts.coexisting_source_and_request_bytes_upper_bound,
        limits.max_coexisting_source_and_request_bytes,
        work,
    )?;
    cap(
        facts.cumulative_work_upper_bound,
        limits.max_cumulative_work,
        work,
    )
}
fn vector<T>(facts: &mut ExpressionNamespaceWriteFacts, count: usize) -> Result<(), Error> {
    facts.new_allocation_request_bytes_upper_bound = add(
        facts.new_allocation_request_bytes_upper_bound,
        bytes::<T>(count)?,
    )?;
    facts.new_allocation_requests_upper_bound = add(
        facts.new_allocation_requests_upper_bound,
        usize::from(count != 0),
    )?;
    Ok(())
}
fn charge(
    facts: &mut ExpressionNamespaceWriteFacts,
    value: usize,
    limits: ExpressionProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    facts.cumulative_work_upper_bound = add(facts.cumulative_work_upper_bound, value)?;
    cap(
        facts.cumulative_work_upper_bound,
        limits.max_cumulative_work,
        work,
    )
}
fn tree_lookup_work(entries: usize) -> Result<usize, Error> {
    // Rust 1.92 BTree nodes have at most eleven keys. Even binary fanout
    // gives at most bit-length+1 visited levels; sixteen units per level
    // include key, edge and node-header work without another lookup index.
    let levels = usize::try_from(usize::BITS - entries.leading_zeros())
        .map_err(|_| shape("expression lookup depth is unrepresentable"))?;
    mul(add(levels, 1)?, 16)
}
fn signature_source_floor(
    function: &novarocks_physical_plan::BoundFunction,
    floor: &mut usize,
    source: usize,
    facts: &mut ExpressionNamespaceWriteFacts,
    limits: ExpressionProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // These Boxes belong to this original owned expression occurrence; the
    // inline BoundFunction is already included in ExprNode/aggregate Layout.
    *floor = add(
        *floor,
        add(
            function.function_id.as_str().len(),
            function.overload.as_str().len(),
        )?,
    )?;
    *floor = add(
        *floor,
        bytes::<novarocks_physical_plan::FunctionArgumentType>(function.argument_types.len())?,
    )?;
    cap(*floor, source, work)?;
    charge(facts, mul(function.argument_types.len(), 8)?, limits, work)?;
    for argument in &function.argument_types {
        if let novarocks_physical_plan::FunctionArgumentType::Lambda {
            parameter_types, ..
        } = argument
        {
            *floor = add(
                *floor,
                bytes::<novarocks_type_contract::FunctionValueType>(parameter_types.len())?,
            )?;
            cap(*floor, source, work)?;
        }
        work.step()?;
    }
    Ok(())
}
fn finish<T>(work: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// The caller supplies a truthful complete retained-source invoice. Known
/// inline/namespace floors do not prove nested strings, metadata bucket backing
/// or a formal host grant. Full graph, node, occurrence and kernel validity
/// remains the sole checked FragmentPackage/compiler chain's responsibility.
#[allow(clippy::too_many_arguments)]
pub fn prepare_expression_definitions<'loan, 'source, 'control>(
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedExpressionNamespaceWrite<'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let count = arena.len();
        cap(count, limits.max_definitions, &mut work)?;
        let same_count = count == inputs.len();
        work.step()?;
        if !same_count {
            return Err(shape(
                "expression author requires one binding per original definition",
            ));
        }
        let same_table = std::ptr::eq(types, functions.type_sources())
            && std::ptr::eq(types, aggregates.type_sources())
            && std::ptr::eq(functions, aggregates.function_sources());
        work.step()?;
        if !same_table {
            return Err(shape(
                "expression author requires one original type emission",
            ));
        }
        let mut source_floor = add(
            bytes::<ExprNode>(count)?,
            bytes::<ExpressionTypeIds<'_>>(count)?,
        )?;
        source_floor = add(
            source_floor,
            bytes::<wire::FunctionBindingDefinition>(functions.as_wire().len())?,
        )?;
        source_floor = add(
            source_floor,
            bytes::<wire::AggregateBindingDefinition>(aggregates.as_wire().len())?,
        )?;
        let table = types.as_wire();
        source_floor = add(
            source_floor,
            bytes::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>(
                table.carriers.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            bytes::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>(
                table.value_types.capacity(),
            )?,
        )?;
        source_floor = add(
            source_floor,
            bytes::<novarocks_proto_models::physical_type_v2::FieldDefinition>(
                table.fields.capacity(),
            )?,
        )?;
        cap(source_floor, source_retained_bytes, &mut work)?;
        let mut facts = ExpressionNamespaceWriteFacts {
            definition_count: count,
            type_reference_count: count,
            cumulative_work_upper_bound: add(128, mul(count, 96)?)?,
            ..Default::default()
        };
        vector::<wire::ExpressionDefinition>(&mut facts, count)?;
        cap(
            facts.cumulative_work_upper_bound,
            limits.max_cumulative_work,
            &mut work,
        )?;
        let mut function_lookups = 0usize;
        let mut aggregate_lookups = 0usize;
        let mut parameter_lookups = 0usize;
        let mut constant_lookups = 0usize;
        let mut max_lambda_id_bytes = 0usize;
        for ((id, node), input) in arena.iter().zip(inputs) {
            let id_matches = *id == input.expr && node.id == input.expr;
            work.step()?;
            if !id_matches {
                return Err(shape(
                    "expression binding order differs from original definitions",
                ));
            }
            let owned = match &node.kind {
                ExprKind::Conjunction { args }
                | ExprKind::Disjunction { args }
                | ExprKind::FunctionCall { args, .. } => bytes::<ExprId>(args.len())?,
                ExprKind::Lambda {
                    parameter_types, ..
                } => bytes::<novarocks_type_contract::FunctionValueType>(parameter_types.len())?,
                ExprKind::InList { list, .. } => bytes::<ExprId>(list.len())?,
                ExprKind::Case { when_then, .. } => bytes::<(ExprId, ExprId)>(when_then.len())?,
                ExprKind::WindowCall {
                    args,
                    function_order_by,
                    aggregate_binding,
                    ..
                } => {
                    let mut owned = add(
                        bytes::<ExprId>(args.len())?,
                        bytes::<novarocks_physical_plan::SortExpr>(function_order_by.len())?,
                    )?;
                    if aggregate_binding.is_some() {
                        owned = add(
                            owned,
                            std::mem::size_of::<novarocks_physical_plan::AggregateBinding>(),
                        )?;
                    }
                    owned
                }
                _ => 0,
            };
            source_floor = add(source_floor, owned)?;
            cap(source_floor, source_retained_bytes, &mut work)?;
            match &node.kind {
                ExprKind::FunctionCall { function, .. } => signature_source_floor(
                    function,
                    &mut source_floor,
                    source_retained_bytes,
                    &mut facts,
                    limits,
                    &mut work,
                )?,
                ExprKind::WindowCall {
                    function,
                    aggregate_binding,
                    ..
                } => {
                    signature_source_floor(
                        function,
                        &mut source_floor,
                        source_retained_bytes,
                        &mut facts,
                        limits,
                        &mut work,
                    )?;
                    if let Some(aggregate) = aggregate_binding {
                        source_floor = add(source_floor, aggregate.state_format.as_str().len())?;
                        signature_source_floor(
                            &aggregate.function,
                            &mut source_floor,
                            source_retained_bytes,
                            &mut facts,
                            limits,
                            &mut work,
                        )?;
                    }
                }
                _ => {}
            }
            let mut refs = 0usize;
            match &node.kind {
                ExprKind::Value(_) => {}
                ExprKind::LambdaParameter { .. } => refs = 1,
                ExprKind::Constant(_) => constant_lookups = add(constant_lookups, 1)?,
                ExprKind::Literal(_) => {
                    return Err(shape(
                        "legacy literal cannot be encoded as a constant reference",
                    ));
                }
                ExprKind::Unary { .. }
                | ExprKind::IsNull { .. }
                | ExprKind::IsTruthValue { .. } => refs = 1,
                ExprKind::Binary { .. } => refs = 2,
                ExprKind::Cast { .. } => {
                    refs = 1;
                    facts.type_reference_count = add(facts.type_reference_count, 1)?;
                }
                ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
                    refs = args.len();
                    vector::<u32>(&mut facts, args.len())?;
                }
                ExprKind::FunctionCall { args, .. } => {
                    refs = args.len();
                    vector::<u32>(&mut facts, args.len())?;
                    function_lookups = add(function_lookups, 1)?;
                }
                ExprKind::Lambda {
                    parameter_types, ..
                } => {
                    refs = 1;
                    vector::<u32>(&mut facts, parameter_types.len())?;
                    facts.type_reference_count =
                        add(facts.type_reference_count, parameter_types.len())?;
                    max_lambda_id_bytes = max_lambda_id_bytes
                        .max(bytes::<u32>(input.lambda_parameter_type_ids.len())?);
                }
                ExprKind::InList { list, .. } => {
                    refs = add(1, list.len())?;
                    vector::<u32>(&mut facts, list.len())?;
                }
                ExprKind::Between { .. } => refs = 3,
                ExprKind::Like { .. } => refs = 2,
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => {
                    refs = add(
                        add(usize::from(operand.is_some()), mul(2, when_then.len())?)?,
                        usize::from(else_expr.is_some()),
                    )?;
                    vector::<wire::WhenThen>(&mut facts, when_then.len())?;
                }
                ExprKind::WindowCall {
                    args,
                    function_order_by,
                    frame,
                    aggregate_binding,
                    ..
                } => {
                    refs = add(args.len(), function_order_by.len())?;
                    if let Some(frame) = frame {
                        for bound in [&frame.start, &frame.end] {
                            if matches!(
                                bound,
                                novarocks_physical_plan::WindowBound::Preceding(_)
                                    | novarocks_physical_plan::WindowBound::Following(_)
                            ) {
                                refs = add(refs, 1)?;
                            }
                        }
                    }
                    vector::<u32>(&mut facts, args.len())?;
                    vector::<wire::SortExpression>(&mut facts, function_order_by.len())?;
                    function_lookups = add(function_lookups, 1)?;
                    aggregate_lookups =
                        add(aggregate_lookups, usize::from(aggregate_binding.is_some()))?;
                }
            }
            facts.expression_reference_count = add(
                facts.expression_reference_count,
                add(refs, usize::from(node.lambda_scope.is_some()))?,
            )?;
            parameter_lookups = add(
                parameter_lookups,
                node.kind.intrinsic_parameter_references().count(),
            )?;
            work.step()?;
        }
        cap(
            add(source_floor, max_lambda_id_bytes)?,
            source_retained_bytes,
            &mut work,
        )?;
        let (value_roots, _) = types.source_counts();
        let lookups = add(
            mul(facts.type_reference_count, value_roots)?,
            add(
                mul(function_lookups, functions.source_counts())?,
                mul(aggregate_lookups, aggregates.source_counts())?,
            )?,
        )?;
        let tree_work = add(
            mul(
                parameter_lookups,
                tree_lookup_work(parameters.entries().len())?,
            )?,
            mul(constant_lookups, tree_lookup_work(pools.entries().len())?)?,
        )?;
        facts.cumulative_work_upper_bound = add(
            facts.cumulative_work_upper_bound,
            add(
                facts.new_allocation_request_bytes_upper_bound,
                add(
                    tree_work,
                    add(lookups, mul(facts.expression_reference_count, 16)?)?,
                )?,
            )?,
        )?;
        facts.coexisting_source_and_request_bytes_upper_bound = add(
            source_retained_bytes,
            facts.new_allocation_request_bytes_upper_bound,
        )?;
        check(&facts, limits, &mut work)?;
        // Every original binding is proved before the first output allocation.
        for ((_, node), input) in arena.iter().zip(inputs) {
            let (_, source_ty) = types
                .root_value_binding_observed(input.value_type_id, &mut work)?
                .ok_or_else(|| shape("expression refers to an unknown original value type"))?;
            let verified = verify_type_binding(
                &node.ty,
                source_ty,
                source_retained_bytes,
                limits.max_cumulative_work - facts.cumulative_work_upper_bound,
                &mut work,
            )?;
            charge(&mut facts, verified.work_upper_bound(), limits, &mut work)?;
            if !verified.matches() {
                return Err(shape(
                    "expression value type differs from its original root",
                ));
            }
            match &node.kind {
                ExprKind::Lambda {
                    parameter_types, ..
                } => {
                    let same = parameter_types.len() == input.lambda_parameter_type_ids.len();
                    work.step()?;
                    if !same {
                        return Err(shape(
                            "lambda type ID count differs from original parameters",
                        ));
                    }
                    for (parameter, id) in
                        parameter_types.iter().zip(input.lambda_parameter_type_ids)
                    {
                        let source =
                            types.value_type_observed(*id, &mut work)?.ok_or_else(|| {
                                shape("lambda refers to an unknown original value type")
                            })?;
                        let verified = verify_type_binding(
                            parameter,
                            source,
                            source_retained_bytes,
                            limits.max_cumulative_work - facts.cumulative_work_upper_bound,
                            &mut work,
                        )?;
                        charge(&mut facts, verified.work_upper_bound(), limits, &mut work)?;
                        if !verified.matches() {
                            return Err(shape("lambda parameter type differs from original root"));
                        }
                    }
                }
                _ => {
                    let empty = input.lambda_parameter_type_ids.is_empty();
                    work.step()?;
                    if !empty {
                        return Err(shape("non-lambda definition has lambda parameter type IDs"));
                    }
                }
            }
            let selected_function = match &node.kind {
                ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
                    Some(function)
                }
                _ => None,
            };
            let same_presence = selected_function.is_some() == input.function_binding_id.is_some();
            work.step()?;
            if !same_presence {
                return Err(shape(
                    "expression function ID presence differs from original kind",
                ));
            }
            if let Some(function) = selected_function {
                let source = functions
                    .scalar_binding_observed(
                        input
                            .function_binding_id
                            .ok_or_else(|| shape("missing expression function ID"))?,
                        &mut work,
                    )?
                    .ok_or_else(|| shape("expression refers to an unknown scalar signature"))?;
                let verified = verify_scalar_signature(
                    function,
                    source,
                    source_retained_bytes,
                    limits.max_cumulative_work - facts.cumulative_work_upper_bound,
                    &mut work,
                )?;
                charge(&mut facts, verified.work_upper_bound(), limits, &mut work)?;
                if !verified.matches() {
                    return Err(shape("expression function differs from original signature"));
                }
            }
            let aggregate = match &node.kind {
                ExprKind::WindowCall {
                    aggregate_binding, ..
                } => aggregate_binding.as_deref(),
                _ => None,
            };
            let same_presence = aggregate.is_some() == input.aggregate_binding_id.is_some();
            work.step()?;
            if !same_presence {
                return Err(shape(
                    "expression aggregate ID presence differs from original kind",
                ));
            }
            if let Some(aggregate) = aggregate {
                let source = aggregates
                    .binding_observed(
                        input
                            .aggregate_binding_id
                            .ok_or_else(|| shape("missing expression aggregate ID"))?,
                        &mut work,
                    )?
                    .ok_or_else(|| shape("expression refers to an unknown aggregate signature"))?;
                let verified = verify_aggregate_signature(
                    aggregate,
                    source,
                    source_retained_bytes,
                    limits.max_cumulative_work - facts.cumulative_work_upper_bound,
                    &mut work,
                )?;
                charge(&mut facts, verified.work_upper_bound(), limits, &mut work)?;
                if !verified.matches() {
                    return Err(shape(
                        "expression aggregate differs from original signature",
                    ));
                }
            }
            if let ExprKind::Cast { target, .. } = &node.kind {
                // Reuse the same left topology's already measured full bound.
                // A raw target has no independent nullable/logical root flags.
                charge(&mut facts, verified.work_upper_bound(), limits, &mut work)?;
                work.flush()?;
                let matched =
                    arrow_data_types_exact_borrowed_observed(&node.ty.data_type, target, || {
                        work.step()
                            .map_err(crate::physical_type_v2::TypeCodecError::from)
                    });
                let matched = match matched {
                    Err(crate::physical_type_v2::TypeCodecError::Control(cause)) => {
                        return Err(Error::Control(cause));
                    }
                    result => result,
                };
                work.flush()?;
                if !matched? {
                    return Err(shape(
                        "cast target differs from its original result carrier",
                    ));
                }
            }
            if let ExprKind::Binary {
                op,
                allow_throw_exception,
                ..
            } = &node.kind
            {
                let arithmetic = matches!(
                    op,
                    BinaryOperator::Add
                        | BinaryOperator::Subtract
                        | BinaryOperator::Multiply
                        | BinaryOperator::Divide
                        | BinaryOperator::Modulo
                );
                let same_presence = arithmetic == allow_throw_exception.is_some();
                work.step()?;
                if !same_presence {
                    return Err(shape(
                        "binary intrinsic parameter presence differs from arithmetic kind",
                    ));
                }
            }
            for reference in node.kind.intrinsic_parameter_references() {
                let key_matches =
                    reference.expected_key == SemanticParameterKey::AllowThrowException;
                work.step()?;
                if !key_matches {
                    return Err(shape("intrinsic parameter has a wrong expected key"));
                }
                work.flush()?;
                let value = parameters.require(*reference);
                work.step()?;
                work.flush()?;
                value.map_err(SemanticsCodecError::from)?;
            }
            if let ExprKind::Constant(reference) = &node.kind {
                work.flush()?;
                let pool = pools.entries().get(&reference.pool);
                work.step()?;
                work.flush()?;
                let pool = pool.ok_or_else(|| {
                    shape("expression refers to an unknown original constant pool")
                })?;
                let retained =
                    usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes).map_err(
                        |_| shape("constant expression source retained extent is unrepresentable"),
                    );
                work.step()?;
                cap(retained?, source_retained_bytes, &mut work)?;
                let ordinal_valid = u64::from(reference.ordinal) < pool.resource_facts().rows;
                work.step()?;
                if !ordinal_valid {
                    return Err(shape("constant ordinal differs from original pool extent"));
                }
                let verified = verify_type_binding(
                    &node.ty,
                    pool.value_type(),
                    source_retained_bytes,
                    limits.max_cumulative_work - facts.cumulative_work_upper_bound,
                    &mut work,
                )?;
                charge(&mut facts, verified.work_upper_bound(), limits, &mut work)?;
                if !verified.matches() {
                    return Err(shape("constant expression type differs from original pool"));
                }
            }
        }
        Ok(PreparedExpressionNamespaceWrite {
            arena,
            inputs,
            types,
            _functions: functions,
            _aggregates: aggregates,
            _parameters: parameters,
            _pools: pools,
            facts,
            control,
            source_retained_bytes,
        })
    })();
    finish(work, result)
}
#[allow(clippy::too_many_arguments)]
pub fn encode_expression_definitions<'loan, 'source, 'control>(
    arena: &'loan ExprArena,
    inputs: &'loan [ExpressionTypeIds<'source>],
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    aggregates: &'loan EncodedAggregateBindings<'loan, 'source>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<EncodedExpressions<'loan, 'source, 'control>, Error> {
    prepare_expression_definitions(
        arena,
        inputs,
        types,
        functions,
        aggregates,
        parameters,
        pools,
        source_retained_bytes,
        limits,
        control,
    )?
    .emit()
}

#[cfg(test)]
#[path = "namespace_tests.rs"]
mod tests;
