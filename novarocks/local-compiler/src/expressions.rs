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

//! Physical definition and occurrence lowering for the initial executable
//! expression slice. Unsupported shapes remain explicit compiler errors.
//! This is not the complete expression compiler or a host allocation grant.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt,
    sync::Arc,
};

use novarocks_functions::{
    CallEffectInput, ConstantError, ConstantPolicy, ConstantValue, FunctionArgument,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingSelection, FunctionLiteral,
    FunctionResultType, FunctionSpecializationFailure, KernelFailure, PureCallPreparation,
    PureCallSpecialization, PureEngineFunctionCatalog, ScopedExpressionEffects,
};
use novarocks_local_program::{
    ExpressionsCompileError, ImmutableExpressions, ProgramCallSite, ProgramExprId,
    ProgramExpressionArena, ProgramUseRef, StaticExprKind, StaticExprNode,
};
use novarocks_physical_plan::{
    ExprId, ExprKind, ExprNode, FragmentPackage, LiteralValue, PhysicalCallSite,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    EffectContractError, ExpressionUseId, FunctionArgumentType, FunctionKind,
    MAX_CONTROL_DEFINITIONS, MAX_CONTROL_DEPTH, PureCompileControl, ValueTypeError,
};

/// Dense definitions retain the complete source types independently of the
/// old carrier-only StaticExprNode projection. No caller control is retained.
pub(crate) struct LoweredExpressions {
    pub arena: Arc<ImmutableExpressions>,
    pub ids: BTreeMap<ExprId, ProgramExprId>,
    pub types: Vec<FunctionArgumentType>,
}

#[derive(Debug)]
pub(crate) enum ExpressionLoweringError {
    Control(CompileControlError),
    Constant(ConstantError),
    Binding(FunctionBindingError),
    Type(ValueTypeError),
    Expressions(ExpressionsCompileError),
    Specialization(FunctionSpecializationFailure),
    Effects(EffectContractError),
    UnsupportedExpression(ExprId),
    UnsupportedLiteral(ExprId),
    UnsupportedCall(PhysicalCallSite),
    Invalid(&'static str),
}
impl fmt::Display for ExpressionLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Expressions(e) => e.fmt(f),
            Self::Specialization(e) => e.fmt(f),
            Self::Effects(e) => e.fmt(f),
            Self::UnsupportedExpression(id) => {
                write!(f, "unsupported physical expression {}", id.get())
            }
            Self::UnsupportedLiteral(id) => write!(f, "unsupported physical literal {}", id.get()),
            Self::UnsupportedCall(site) => write!(f, "unsupported physical call {site:?}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl Error for ExpressionLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Constant(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Expressions(e) => Some(e),
            Self::Specialization(e) => Some(e),
            Self::Effects(e) => Some(e),
            _ => None,
        }
    }
}
impl From<CompileControlError> for ExpressionLoweringError {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl From<ConstantError> for ExpressionLoweringError {
    fn from(e: ConstantError) -> Self {
        match e {
            ConstantError::Control(e) => Self::Control(e),
            other => Self::Constant(other),
        }
    }
}
impl From<FunctionBindingError> for ExpressionLoweringError {
    fn from(e: FunctionBindingError) -> Self {
        match e {
            FunctionBindingError::Control(e) => Self::Control(e),
            other => Self::Binding(other),
        }
    }
}
impl From<ValueTypeError> for ExpressionLoweringError {
    fn from(e: ValueTypeError) -> Self {
        Self::Type(e)
    }
}
impl From<ExpressionsCompileError> for ExpressionLoweringError {
    fn from(e: ExpressionsCompileError) -> Self {
        match e {
            ExpressionsCompileError::Control(e) => Self::Control(e),
            other => Self::Expressions(other),
        }
    }
}
impl From<EffectContractError> for ExpressionLoweringError {
    fn from(e: EffectContractError) -> Self {
        Self::Effects(e)
    }
}
impl From<FunctionSpecializationFailure> for ExpressionLoweringError {
    fn from(e: FunctionSpecializationFailure) -> Self {
        match e {
            FunctionSpecializationFailure::Control(e) => Self::Control(e),
            FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                Self::Control(CompileControlError::Cancelled)
            }
            FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                Self::Control(CompileControlError::DeadlineExceeded)
            }
            FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                Self::Control(CompileControlError::ResourceExhausted)
            }
            other => Self::Specialization(other),
        }
    }
}

fn finish<T>(
    result: Result<T, ExpressionLoweringError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, ExpressionLoweringError> {
    if matches!(&result, Err(ExpressionLoweringError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

/// Lower every actual definition, including unused definitions, without
/// creating invocation tokens for definitions with no evaluation use. IDs are
/// topologically assigned; sparse numeric order is never dependency order.
pub(crate) fn lower_expressions(
    package: &FragmentPackage,
    policy: ConstantPolicy,
    inputs: &BTreeMap<ExprId, crate::channels::ResolvedInput>,
    control: &dyn PureCompileControl,
) -> Result<LoweredExpressions, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, policy, inputs, control, &mut work);
    finish(result, &mut work)
}

fn lower_core(
    package: &FragmentPackage,
    policy: ConstantPolicy,
    inputs: &BTreeMap<ExprId, crate::channels::ResolvedInput>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LoweredExpressions, ExpressionLoweringError> {
    let source = package.fragment().expressions();
    if source.len() > MAX_CONTROL_DEFINITIONS {
        return Err(ExpressionLoweringError::Invalid(
            "expression definition limit exceeded",
        ));
    }
    let mut ids = BTreeMap::new();
    let mut nodes = Vec::new();
    let mut types = Vec::new();
    let mut active = BTreeSet::new();
    // At most one frame per dependency depth. Child lists are borrowed and
    // walked one edge at a time, rather than cloned or enqueued wholesale.
    let mut stack = Vec::new();
    for (&root, _) in source.iter() {
        if ids.contains_key(&root) {
            work.step()?;
            continue;
        }
        active.insert(root);
        stack.push((root, 0usize));
        work.step()?;
        while let Some(&(id, next)) = stack.last() {
            let node = source.get(id).ok_or(ExpressionLoweringError::Invalid(
                "missing physical expression",
            ))?;
            if node.id != id {
                return Err(ExpressionLoweringError::Invalid(
                    "physical expression key differs from ID",
                ));
            }
            let args: &[ExprId] = match &node.kind {
                ExprKind::Literal(_) | ExprKind::Value(_) => &[],
                ExprKind::FunctionCall { args, .. } => args,
                _ => return Err(ExpressionLoweringError::UnsupportedExpression(id)),
            };
            if let Some(&child) = args.get(next) {
                let frame = stack
                    .last_mut()
                    .ok_or(ExpressionLoweringError::Invalid("missing dependency frame"))?;
                frame.1 += 1;
                if !ids.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(ExpressionLoweringError::Invalid(
                            "cyclic physical expression",
                        ));
                    }
                    if stack.len() >= MAX_CONTROL_DEPTH {
                        return Err(ExpressionLoweringError::Invalid(
                            "expression dependency depth exceeded",
                        ));
                    }
                    stack.push((child, 0));
                }
                work.step()?;
                continue;
            }
            let kind = match &node.kind {
                ExprKind::Value(_) => {
                    // The channel owner has validated the exact physical
                    // Value, child scope and complete source type. A missing
                    // mapping cannot be repaired by guessing a slot ID.
                    let input = inputs.get(&id).ok_or(ExpressionLoweringError::Invalid(
                        "missing resolved physical input",
                    ))?;
                    StaticExprKind::SlotId(input.slot)
                }
                ExprKind::Literal(literal) => {
                    // The original CV owner performs type/resource preflight
                    // before Arrow construction. Field creation/type clones
                    // remain opaque work with observations around them.
                    work.flush()?;
                    let field = Arc::new(node.ty.try_to_field("constant")?);
                    work.flush()?;
                    let value = match literal {
                        LiteralValue::Null => ConstantValue::null(
                            field,
                            node.ty.clone(),
                            policy,
                            CompilePhase::LowerProgram,
                            control,
                        )?,
                        LiteralValue::Boolean(value) => ConstantValue::from_boolean(
                            field,
                            node.ty.clone(),
                            *value,
                            policy,
                            CompilePhase::LowerProgram,
                            control,
                        )?,
                        LiteralValue::Int64(value) => ConstantValue::from_i64(
                            field,
                            node.ty.clone(),
                            *value,
                            policy,
                            CompilePhase::LowerProgram,
                            control,
                        )?,
                        _ => return Err(ExpressionLoweringError::UnsupportedLiteral(id)),
                    };
                    work.flush()?;
                    StaticExprKind::Constant(value)
                }
                ExprKind::FunctionCall { args, .. } => {
                    let mut local_args = Vec::with_capacity(args.len());
                    for child in args {
                        local_args.push(*ids.get(child).ok_or(
                            ExpressionLoweringError::Invalid(
                                "expression dependency was not lowered",
                            ),
                        )?);
                        work.step()?;
                    }
                    StaticExprKind::BoundCall { args: local_args }
                }
                _ => return Err(ExpressionLoweringError::UnsupportedExpression(id)),
            };
            work.flush()?;
            let ty = node.ty.clone();
            nodes.push(StaticExprNode::new(kind, ty.data_type.clone(), None));
            types.push(FunctionArgumentType::Value(ty));
            work.flush()?;
            ids.insert(id, ProgramExprId::new(nodes.len() - 1));
            active.remove(&id);
            stack.pop();
            work.step()?;
        }
    }
    work.flush()?;
    // No legacy exception or session-timezone capability is authored here.
    // Frozen semantic parameters remain on exact prepared call contracts.
    let arena = Arc::new(ImmutableExpressions::try_new_for_compile(
        nodes,
        false,
        HashMap::new(),
        None,
        control,
    )?);
    work.flush()?;
    Ok(LoweredExpressions { arena, ids, types })
}

/// Prepare actual expression occurrences using exact frozen identities and
/// facts. This initial Main-arena slice handles scalar eager/type-only calls;
/// guarded, lambda and relational lifecycles are explicit pending compiler
/// cases, rather than being treated as ordinary scalar calls.
pub(crate) fn prepare_calls(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = prepare_core(package, lowered, functions, control, &mut work);
    finish(result, &mut work)
}

fn prepare_core(
    package: &FragmentPackage,
    lowered: &LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<(ProgramCallSite, PureCallSpecialization)>, ExpressionLoweringError> {
    // The private intermediate is checked against this exact borrowed source
    // again. A same-sized mapping from a foreign package is not authority.
    if lowered.ids.len() != package.fragment().expressions().len()
        || lowered.types.len() != lowered.arena.nodes().len()
    {
        return Err(ExpressionLoweringError::Invalid(
            "lowered definition coverage differs",
        ));
    }
    let mut local_ids = BTreeSet::new();
    for (&physical, definition) in package.fragment().expressions().iter() {
        let local_id = *lowered
            .ids
            .get(&physical)
            .ok_or(ExpressionLoweringError::Invalid(
                "missing lowered definition",
            ))?;
        if !local_ids.insert(local_id) {
            return Err(ExpressionLoweringError::Invalid(
                "distinct definitions share a local ID",
            ));
        }
        let Some(FunctionArgumentType::Value(ty)) = lowered.types.get(local_id.index()) else {
            return Err(ExpressionLoweringError::Invalid(
                "missing complete local value type",
            ));
        };
        // The shared walker observes borrowed work before its operations.
        // These are cancellation observations, not fabricated completed units.
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<ExpressionLoweringError>(ty, || {
                control
                    .checkpoint(CompilePhase::FunctionSpecialization, 0)
                    .map_err(ExpressionLoweringError::Control)
            })?;
        work.flush()?;
        if !same {
            return Err(ExpressionLoweringError::Invalid(
                "lowered full type differs from source",
            ));
        }
        work.step()?;
    }
    // Static validation covers every declared call, including dead definitions
    // and TypeOnly children. It uses no effects or invocation demand, and must
    // not prepare a stateful implementation merely to validate its signature.
    let mut selected = BTreeMap::<ExprId, Arc<FunctionBindingSelection>>::new();
    let mut requests = BTreeMap::<ExprId, Vec<FunctionArgument>>::new();
    for (&id, definition) in package.fragment().expressions().iter() {
        if let ExprKind::FunctionCall { function, args } = &definition.kind {
            if args.len() > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            if function.kind != FunctionKind::Scalar {
                return Err(ExpressionLoweringError::UnsupportedExpression(id));
            }
            let local_id = *lowered
                .ids
                .get(&id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing local call definition",
                ))?;
            let Some(StaticExprKind::BoundCall { args: local_args }) =
                lowered.arena.node(local_id).map(StaticExprNode::kind)
            else {
                return Err(ExpressionLoweringError::Invalid(
                    "lowered call shape differs",
                ));
            };
            if args.len() != local_args.len() || args.len() != function.argument_types.len() {
                return Err(ExpressionLoweringError::Invalid(
                    "static call arity differs",
                ));
            }
            work.flush()?;
            let result_matches = function
                .result_type
                .exactly_equals_observed::<ExpressionLoweringError>(&definition.ty, || {
                    control
                        .checkpoint(CompilePhase::FunctionSpecialization, 0)
                        .map_err(ExpressionLoweringError::Control)
                })?;
            work.flush()?;
            if !result_matches {
                return Err(ExpressionLoweringError::Invalid(
                    "static call result differs from its definition",
                ));
            }
            let mut arguments = Vec::with_capacity(args.len());
            for (ordinal, child) in args.iter().enumerate() {
                let child_id = *lowered
                    .ids
                    .get(child)
                    .ok_or(ExpressionLoweringError::Invalid(
                        "missing static call argument",
                    ))?;
                if local_args[ordinal] != child_id {
                    return Err(ExpressionLoweringError::Invalid(
                        "ordered static arguments differ",
                    ));
                }
                let child_source = package.fragment().expressions().get(*child).ok_or(
                    ExpressionLoweringError::Invalid("missing physical static argument"),
                )?;
                let child_node =
                    lowered
                        .arena
                        .node(child_id)
                        .ok_or(ExpressionLoweringError::Invalid(
                            "missing local static argument",
                        ))?;
                work.flush()?;
                let constant = literal_argument(child_source, child_node, control)?;
                work.flush()?;
                arguments.push(FunctionArgument::Value {
                    value_type: child_source.ty.clone(),
                    constant,
                });
                work.flush()?;
                work.step()?;
            }
            work.flush()?;
            let selection = Arc::new(FunctionBindingSelection {
                overload: function.overload.clone(),
                argument_types: function.argument_types.clone(),
                result_type: FunctionResultType::Scalar(function.result_type.clone()),
                aggregate: None,
            });
            work.flush()?;
            functions.metadata().validate_frozen_selection(
                &function.function_id,
                function.kind,
                selection.as_ref(),
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: Some(&definition.ty),
                },
                control,
            )?;
            work.flush()?;
            selected.insert(id, selection);
            requests.insert(id, arguments);
        }
        work.step()?;
    }
    let flow = package.expression_uses().flow();
    for &site in package.calls().entries().keys() {
        if !matches!(site, PhysicalCallSite::Expression(_)) {
            return Err(ExpressionLoweringError::UnsupportedCall(site));
        }
        work.step()?;
    }
    let mut effects = BTreeMap::<ExpressionUseId, ScopedExpressionEffects>::new();
    let mut tokens = BTreeMap::new();
    let mut active = BTreeSet::new();
    let mut stack = Vec::new();
    for &root in flow.uses().keys() {
        if effects.contains_key(&root) {
            work.step()?;
            continue;
        }
        active.insert(root);
        stack.push((root, 0usize));
        work.step()?;
        while let Some(&(id, next)) = stack.last() {
            let invocation = flow
                .uses()
                .get(&id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing expression occurrence",
                ))?;
            if let Some(&child) = invocation.arguments.get(next) {
                let frame = stack
                    .last_mut()
                    .ok_or(ExpressionLoweringError::Invalid("missing invocation frame"))?;
                frame.1 += 1;
                if !effects.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(ExpressionLoweringError::Invalid(
                            "cyclic expression occurrence",
                        ));
                    }
                    if stack.len() >= MAX_CONTROL_DEPTH {
                        return Err(ExpressionLoweringError::Invalid(
                            "expression occurrence depth exceeded",
                        ));
                    }
                    stack.push((child, 0));
                }
                work.step()?;
                continue;
            }
            let source = package
                .fragment()
                .expressions()
                .get(invocation.definition)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing invocation definition",
                ))?;
            let local_id = *lowered
                .ids
                .get(&source.id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "invocation definition was not lowered",
                ))?;
            let local = lowered
                .arena
                .node(local_id)
                .ok_or(ExpressionLoweringError::Invalid("missing local definition"))?;
            let scoped = match (&source.kind, local.kind()) {
                (ExprKind::Value(_), StaticExprKind::SlotId(_)) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "input occurrence has arguments or control",
                        ));
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (ExprKind::Literal(_), StaticExprKind::Constant(_)) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "literal occurrence has arguments or control",
                        ));
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (
                    ExprKind::FunctionCall { function, args },
                    StaticExprKind::BoundCall { args: local_args },
                ) => {
                    let site = PhysicalCallSite::Expression(id);
                    if function.kind != FunctionKind::Scalar {
                        return Err(ExpressionLoweringError::UnsupportedCall(site));
                    }
                    let frozen = package.calls().entries().get(&site).ok_or(
                        ExpressionLoweringError::Invalid("missing frozen expression call"),
                    )?;
                    if frozen.context != invocation.context {
                        return Err(ExpressionLoweringError::Invalid(
                            "frozen occurrence context differs",
                        ));
                    }
                    let type_only = match (invocation.control, frozen.effects.argument_control) {
                        (ControlShape::Eager, ArgumentControl::Eager) => false,
                        (ControlShape::TypeOnly, ArgumentControl::TypeOnly) => true,
                        _ => return Err(ExpressionLoweringError::UnsupportedCall(site)),
                    };
                    if args.len() != local_args.len()
                        || function.argument_types.len() != args.len()
                        || invocation.arguments.len() != if type_only { 0 } else { args.len() }
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "call argument arity differs",
                        ));
                    }
                    let arguments =
                        requests
                            .get(&source.id)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "call definition was not statically validated",
                            ))?;
                    let selection =
                        selected
                            .get(&source.id)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "call selection was not statically validated",
                            ))?;
                    let mut argument_uses = Vec::with_capacity(args.len());
                    let mut children = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, &child) in args.iter().enumerate() {
                        if type_only {
                            argument_uses.push(None);
                        } else {
                            let child_use = invocation.arguments[ordinal];
                            let child_invocation = flow.uses().get(&child_use).ok_or(
                                ExpressionLoweringError::Invalid(
                                    "missing call argument occurrence",
                                ),
                            )?;
                            if child_invocation.definition != child {
                                return Err(ExpressionLoweringError::Invalid(
                                    "ordered physical call arguments differ",
                                ));
                            }
                            children =
                                children.join_same_domain(*effects.get(&child_use).ok_or(
                                    ExpressionLoweringError::Invalid(
                                        "argument effects were not prepared",
                                    ),
                                )?)?;
                            argument_uses.push(Some(child_use));
                        }
                        work.step()?;
                    }
                    let input = CallEffectInput {
                        context: frozen.context,
                        argument_uses: &argument_uses,
                        function_id: &function.function_id,
                        kind: function.kind,
                        selected: selection.as_ref(),
                        request: FunctionBindingRequest {
                            arguments,
                            logical_argument_count: arguments.len(),
                            expected_result_type: Some(&source.ty),
                        },
                        environment: &frozen.effects.environment,
                        parameters: package.parameters(),
                        decimal_overflow_policy: frozen.decimal_overflow_policy,
                        proof_scope: frozen.effects.proof_scope,
                    };
                    work.flush()?;
                    let options = if type_only {
                        PureCallPreparation::ControlIntrinsic {
                            arguments: children,
                        }
                    } else {
                        PureCallPreparation::Scalar {
                            arguments: children,
                        }
                    };
                    let token = functions.prepare_frozen(
                        input,
                        Arc::clone(selection),
                        &frozen.effects,
                        options,
                        control,
                    )?;
                    work.flush()?;
                    let result = token.effects();
                    tokens.insert(
                        ProgramCallSite::Expression(ProgramUseRef {
                            arena: ProgramExpressionArena::Main,
                            use_id: id,
                        }),
                        token,
                    );
                    result
                }
                _ => {
                    return Err(ExpressionLoweringError::Invalid(
                        "lowered expression shape differs from source",
                    ));
                }
            };
            effects.insert(id, scoped);
            active.remove(&id);
            stack.pop();
            work.step()?;
        }
    }
    let mut result = Vec::with_capacity(tokens.len());
    for entry in tokens {
        result.push(entry);
        work.step()?;
    }
    Ok(result)
}

fn literal_argument(
    source: &ExprNode,
    node: &StaticExprNode,
    control: &dyn PureCompileControl,
) -> Result<Option<FunctionLiteral>, ExpressionLoweringError> {
    if let StaticExprKind::Constant(value) = node.kind()
        && !source
            .ty
            .exactly_equals_observed::<ExpressionLoweringError>(value.value_type(), || {
                control
                    .checkpoint(CompilePhase::FunctionSpecialization, 0)
                    .map_err(ExpressionLoweringError::Control)
            })?
    {
        return Err(ExpressionLoweringError::Invalid(
            "constant full type differs from source",
        ));
    }
    match (&source.kind, node.kind()) {
        (ExprKind::Literal(LiteralValue::Null), StaticExprKind::Constant(value)) => {
            if !value.is_null_observed(CompilePhase::FunctionSpecialization, control)? {
                return Err(ExpressionLoweringError::Invalid(
                    "lowered NULL differs from source",
                ));
            }
            Ok(Some(FunctionLiteral::Null))
        }
        (ExprKind::Literal(LiteralValue::Boolean(expected)), StaticExprKind::Constant(value)) => {
            if value.try_boolean()? != Some(*expected) {
                return Err(ExpressionLoweringError::Invalid(
                    "lowered Boolean differs from source",
                ));
            }
            Ok(Some(FunctionLiteral::Boolean(*expected)))
        }
        (ExprKind::Literal(LiteralValue::Int64(expected)), StaticExprKind::Constant(value)) => {
            if value.try_i64()? != Some(*expected) {
                return Err(ExpressionLoweringError::Invalid(
                    "lowered Int64 differs from source",
                ));
            }
            Ok(Some(FunctionLiteral::Int64(*expected)))
        }
        // A resolved input is not a constant, even if this particular runtime
        // batch happens to broadcast one scalar value (notably RAND seeds).
        (ExprKind::Value(_), StaticExprKind::SlotId(_)) => Ok(None),
        (ExprKind::FunctionCall { .. }, StaticExprKind::BoundCall { .. }) => Ok(None),
        _ => Err(ExpressionLoweringError::Invalid(
            "unsupported call argument projection",
        )),
    }
}
