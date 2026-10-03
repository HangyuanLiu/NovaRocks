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
    FunctionBindingError, FunctionBindingRequest, FunctionBindingSelection, FunctionResultType,
    FunctionSpecializationFailure, KernelFailure, PureCallPreparation, PureCallSpecialization,
    PureEngineFunctionCatalog, ScopedExpressionEffects,
};
use novarocks_local_program::{
    ExpressionsCompileError, ImmutableExpressions, ProgramCallSite, ProgramExprId,
    ProgramExpressionArena, ProgramUseRef, StaticExprKind, StaticExprNode,
};
use novarocks_physical_plan::{
    ConstantReferenceError, ExprId, ExprKind, ExprNode, FragmentPackage, LiteralValue,
    PhysicalCallSite,
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
    Reference(ConstantReferenceError),
    Binding(FunctionBindingError),
    Type(ValueTypeError),
    Expressions(ExpressionsCompileError),
    Specialization(FunctionSpecializationFailure),
    Effects(EffectContractError),
    Arithmetic(novarocks_functions::ArithmeticPrepareError),
    Cast(novarocks_functions::CastPrepareError),
    Comparison(novarocks_functions::ComparisonPrepareError),
    UnsupportedExpression(ExprId),
    UnsupportedCall(PhysicalCallSite),
    Invalid(&'static str),
}
impl fmt::Display for ExpressionLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Constant(e) => e.fmt(f),
            Self::Reference(e) => e.fmt(f),
            Self::Binding(e) => e.fmt(f),
            Self::Type(e) => e.fmt(f),
            Self::Expressions(e) => e.fmt(f),
            Self::Specialization(e) => e.fmt(f),
            Self::Effects(e) => e.fmt(f),
            Self::Arithmetic(e) => e.fmt(f),
            Self::Cast(e) => e.fmt(f),
            Self::Comparison(e) => e.fmt(f),
            Self::UnsupportedExpression(id) => {
                write!(f, "unsupported physical expression {}", id.get())
            }
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
            Self::Reference(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Type(e) => Some(e),
            Self::Expressions(e) => Some(e),
            Self::Specialization(e) => Some(e),
            Self::Effects(e) => Some(e),
            Self::Arithmetic(e) => Some(e),
            Self::Cast(e) => Some(e),
            Self::Comparison(e) => Some(e),
            _ => None,
        }
    }
}
impl From<novarocks_functions::ArithmeticPrepareError> for ExpressionLoweringError {
    fn from(value: novarocks_functions::ArithmeticPrepareError) -> Self {
        if let Some(cause) = value.control_error() {
            Self::Control(cause)
        } else {
            Self::Arithmetic(value)
        }
    }
}
impl From<novarocks_functions::CastPrepareError> for ExpressionLoweringError {
    fn from(value: novarocks_functions::CastPrepareError) -> Self {
        if let Some(cause) = value.control_error() {
            Self::Control(cause)
        } else {
            Self::Cast(value)
        }
    }
}
impl From<novarocks_functions::ComparisonPrepareError> for ExpressionLoweringError {
    fn from(value: novarocks_functions::ComparisonPrepareError) -> Self {
        if let Some(cause) = value.control_error() {
            Self::Control(cause)
        } else {
            Self::Comparison(value)
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
impl From<ConstantReferenceError> for ExpressionLoweringError {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(cause)
            | ConstantReferenceError::Constant(ConstantError::Control(cause)) => {
                Self::Control(cause)
            }
            ConstantReferenceError::Constant(ConstantError::Limit(_)) => {
                Self::Control(CompileControlError::ResourceExhausted)
            }
            other => Self::Reference(other),
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

fn case_arity(
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
) -> Result<usize, ExpressionLoweringError> {
    when_then
        .len()
        .checked_mul(2)
        .and_then(|count| {
            count.checked_add(usize::from(operand.is_some()) + usize::from(else_expr.is_some()))
        })
        .filter(|count| {
            !when_then.is_empty() && *count <= novarocks_type_contract::MAX_CONTROL_USE_REFERENCES
        })
        .ok_or(ExpressionLoweringError::Invalid(
            "CASE arity is invalid or exceeds the expression bound",
        ))
}

// Constant-time access preserves the physical tuple order without cloning its
// edge list on every dependency continuation (which would be quadratic).
fn case_child(
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
    ordinal: usize,
) -> Option<ExprId> {
    if operand.is_some() && ordinal == 0 {
        return operand;
    }
    let ordinal = ordinal.checked_sub(usize::from(operand.is_some()))?;
    match when_then.get(ordinal / 2) {
        Some(&(when, then)) => Some(if ordinal.is_multiple_of(2) {
            when
        } else {
            then
        }),
        None if when_then.len().checked_mul(2) == Some(ordinal) => else_expr,
        _ => None,
    }
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
            let child = match &node.kind {
                ExprKind::Literal(_) | ExprKind::Constant(_) | ExprKind::Value(_) => None,
                ExprKind::FunctionCall { args, .. }
                | ExprKind::Conjunction { args }
                | ExprKind::Disjunction { args } => args.get(next).copied(),
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Not,
                    expr,
                }
                | ExprKind::IsNull { expr, .. }
                | ExprKind::Cast { expr, .. } => (next == 0).then_some(*expr),
                ExprKind::Binary {
                    op:
                        novarocks_physical_plan::BinaryOperator::Eq
                        | novarocks_physical_plan::BinaryOperator::EqForNull
                        | novarocks_physical_plan::BinaryOperator::NotEq
                        | novarocks_physical_plan::BinaryOperator::Lt
                        | novarocks_physical_plan::BinaryOperator::LtEq
                        | novarocks_physical_plan::BinaryOperator::Gt
                        | novarocks_physical_plan::BinaryOperator::GtEq
                        | novarocks_physical_plan::BinaryOperator::Add
                        | novarocks_physical_plan::BinaryOperator::Subtract
                        | novarocks_physical_plan::BinaryOperator::Multiply
                        | novarocks_physical_plan::BinaryOperator::Divide
                        | novarocks_physical_plan::BinaryOperator::Modulo,
                    left,
                    right,
                    ..
                } => [*left, *right].get(next).copied(),
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => case_child(*operand, when_then, *else_expr, next),
                _ => return Err(ExpressionLoweringError::UnsupportedExpression(id)),
            };
            if let Some(child) = child {
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
                ExprKind::Constant(reference) => StaticExprKind::Constant(
                    package
                        .constants()
                        .resolve_observed(*reference, &node.ty, work)?,
                ),
                ExprKind::Literal(literal) => {
                    // The original CV owner performs type/resource preflight
                    // before Arrow construction. Field creation/type clones
                    // remain opaque work with observations around them.
                    work.flush()?;
                    let field = Arc::new(node.ty.try_to_field("constant")?);
                    work.flush()?;
                    macro_rules! scalar {
                        ($factory:ident, $value:expr) => {
                            ConstantValue::$factory(
                                field,
                                node.ty.clone(),
                                $value,
                                policy,
                                CompilePhase::LowerProgram,
                                control,
                            )?
                        };
                    }
                    let value = match literal {
                        LiteralValue::Null => ConstantValue::null(
                            field,
                            node.ty.clone(),
                            policy,
                            CompilePhase::LowerProgram,
                            control,
                        )?,
                        LiteralValue::Boolean(value) => scalar!(from_boolean, *value),
                        LiteralValue::Int64(value) => scalar!(from_i64, *value),
                        LiteralValue::UInt64(value) => scalar!(from_u64, *value),
                        LiteralValue::Float64Bits(value) => scalar!(from_f64_bits, *value),
                        LiteralValue::LargeInt(value) => scalar!(from_largeint, *value),
                        LiteralValue::Decimal128(value) => scalar!(from_decimal128, *value),
                        LiteralValue::Decimal256(value) => scalar!(from_decimal256_be, *value),
                        LiteralValue::Utf8(value) => scalar!(from_utf8, value.as_ref()),
                        LiteralValue::Binary(value) => scalar!(from_binary, value.as_ref()),
                        LiteralValue::Date32(value) => scalar!(from_date32, *value),
                        LiteralValue::Time64(value) => scalar!(from_time64, *value),
                        LiteralValue::Timestamp(value) => scalar!(from_timestamp, *value),
                        LiteralValue::IntervalMonthDayNano {
                            months,
                            days,
                            nanoseconds,
                        } => {
                            scalar!(from_interval_month_day_nano, (*months, *days, *nanoseconds))
                        }
                    };
                    work.flush()?;
                    StaticExprKind::Constant(value)
                }
                ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => {
                    if args.is_empty()
                        || node.ty.data_type != arrow_schema::DataType::Boolean
                        || node.ty.logical_type
                            != novarocks_type_contract::ValueLogicalType::Physical
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "Boolean connective has an invalid exact result or arity",
                        ));
                    }
                    let mut local_args = Vec::with_capacity(args.len());
                    for child in args {
                        let child_source = source
                            .get(*child)
                            .ok_or(ExpressionLoweringError::Invalid("missing Boolean operand"))?;
                        if child_source.ty.data_type != arrow_schema::DataType::Boolean
                            || child_source.ty.logical_type
                                != novarocks_type_contract::ValueLogicalType::Physical
                        {
                            return Err(ExpressionLoweringError::Invalid(
                                "Boolean operand differs from its exact physical Boolean type",
                            ));
                        }
                        local_args.push(*ids.get(child).ok_or(
                            ExpressionLoweringError::Invalid("Boolean operand was not lowered"),
                        )?);
                        work.step()?;
                    }
                    if matches!(node.kind, ExprKind::Conjunction { .. }) {
                        StaticExprKind::NaryAnd { args: local_args }
                    } else {
                        StaticExprKind::NaryOr { args: local_args }
                    }
                }
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Not,
                    expr,
                }
                | ExprKind::IsNull { expr, .. } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "unary operand was not lowered",
                    ))?;
                    match &node.kind {
                        ExprKind::Unary { .. } => StaticExprKind::Not(child),
                        ExprKind::IsNull { negated: false, .. } => StaticExprKind::IsNull(child),
                        ExprKind::IsNull { negated: true, .. } => StaticExprKind::IsNotNull(child),
                        _ => unreachable!("checked unary kind"),
                    }
                }
                ExprKind::Cast {
                    expr,
                    target,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } => {
                    let child = *ids.get(expr).ok_or(ExpressionLoweringError::Invalid(
                        "cast operand was not lowered",
                    ))?;
                    let authored_allow = package
                        .parameters()
                        .require(*allow_throw_exception)
                        .map_err(|_| {
                            ExpressionLoweringError::Invalid("missing cast parameter source")
                        })?;
                    let novarocks_type_contract::SemanticParameterValue::AllowThrowException(allow) =
                        authored_allow
                    else {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast parameter has a foreign key",
                        ));
                    };
                    if !novarocks_type_contract::arrow_data_types_exact_observed::<
                        ExpressionLoweringError,
                    >(target, &node.ty.data_type, || {
                        work.step().map_err(ExpressionLoweringError::Control)
                    })? {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast target differs from result",
                        ));
                    }
                    work.flush()?;
                    novarocks_functions::PreparedCastRecipe::try_new(
                        novarocks_functions::CastOperation::Carrier,
                        &source
                            .get(*expr)
                            .ok_or(ExpressionLoweringError::Invalid("missing cast source"))?
                            .ty,
                        &node.ty,
                        *decimal_overflow_policy,
                        *allow,
                        control,
                    )?;
                    StaticExprKind::PreparedCast {
                        operation: novarocks_functions::CastOperation::Carrier,
                        child,
                        decimal_overflow_policy: *decimal_overflow_policy,
                        allow_throw_exception: *allow,
                    }
                }
                ExprKind::Binary {
                    op,
                    left,
                    right,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } if arithmetic_operator(*op).is_some() => {
                    let local_left = *ids.get(left).ok_or(ExpressionLoweringError::Invalid(
                        "arithmetic left operand was not lowered",
                    ))?;
                    let local_right = *ids.get(right).ok_or(ExpressionLoweringError::Invalid(
                        "arithmetic right operand was not lowered",
                    ))?;
                    let reference =
                        allow_throw_exception.ok_or(ExpressionLoweringError::Invalid(
                            "arithmetic has no admitted ALLOW_THROW reference",
                        ))?;
                    let novarocks_type_contract::SemanticParameterValue::AllowThrowException(
                        authored_allow,
                    ) = package.parameters().require(reference).map_err(|_| {
                        ExpressionLoweringError::Invalid("arithmetic ALLOW_THROW source differs")
                    })?
                    else {
                        return Err(ExpressionLoweringError::Invalid(
                            "arithmetic ALLOW_THROW key differs",
                        ));
                    };
                    let operator = arithmetic_operator(*op).expect("checked arithmetic operator");
                    work.step()?;
                    work.flush()?;
                    // Every definition is prepared, even when it has no actual use.
                    // The source table, never the legacy arena flag, authors this value.
                    novarocks_functions::PreparedArithmeticRecipe::try_new(
                        operator,
                        &source
                            .get(*left)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic left source",
                            ))?
                            .ty,
                        &source
                            .get(*right)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic right source",
                            ))?
                            .ty,
                        &node.ty,
                        *decimal_overflow_policy,
                        *authored_allow,
                        control,
                    )?;
                    StaticExprKind::PreparedArithmetic {
                        operator,
                        left: local_left,
                        right: local_right,
                        decimal_overflow_policy: *decimal_overflow_policy,
                        allow_throw_exception: *authored_allow,
                    }
                }
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::EqForNull,
                    left,
                    right,
                    ..
                } => {
                    let local_left = *ids.get(left).ok_or(ExpressionLoweringError::Invalid(
                        "null-safe left operand was not lowered",
                    ))?;
                    let local_right = *ids.get(right).ok_or(ExpressionLoweringError::Invalid(
                        "null-safe right operand was not lowered",
                    ))?;
                    if node.ty.data_type != arrow_schema::DataType::Boolean
                        || node.ty.logical_type
                            != novarocks_type_contract::ValueLogicalType::Physical
                        || node.ty.nullable
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "null-safe comparison requires nonnullable Physical Boolean result",
                        ));
                    }
                    work.flush()?;
                    novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(
                        &source
                            .get(*left)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing null-safe left source",
                            ))?
                            .ty,
                        &source
                            .get(*right)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing null-safe right source",
                            ))?
                            .ty,
                        control,
                    )?;
                    StaticExprKind::PreparedNullSafeComparison {
                        left: local_left,
                        right: local_right,
                    }
                }
                ExprKind::Binary {
                    op:
                        novarocks_physical_plan::BinaryOperator::Eq
                        | novarocks_physical_plan::BinaryOperator::NotEq
                        | novarocks_physical_plan::BinaryOperator::Lt
                        | novarocks_physical_plan::BinaryOperator::LtEq
                        | novarocks_physical_plan::BinaryOperator::Gt
                        | novarocks_physical_plan::BinaryOperator::GtEq,
                    left,
                    right,
                    ..
                } => {
                    let left = *ids.get(left).ok_or(ExpressionLoweringError::Invalid(
                        "comparison left operand was not lowered",
                    ))?;
                    let right = *ids.get(right).ok_or(ExpressionLoweringError::Invalid(
                        "comparison right operand was not lowered",
                    ))?;
                    let ExprKind::Binary { op, .. } = &node.kind else {
                        unreachable!("checked comparison kind")
                    };
                    StaticExprKind::from_comparison(
                        comparison_operator(*op).expect("checked ordinary comparison operator"),
                        left,
                        right,
                    )
                }
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => {
                    let count = case_arity(*operand, when_then, *else_expr)?;
                    let mut children = Vec::with_capacity(count);
                    for ordinal in 0..count {
                        let child = case_child(*operand, when_then, *else_expr, ordinal).ok_or(
                            ExpressionLoweringError::Invalid("missing actual CASE child"),
                        )?;
                        children.push(*ids.get(&child).ok_or(ExpressionLoweringError::Invalid(
                            "CASE child was not lowered",
                        ))?);
                        work.step()?;
                    }
                    StaticExprKind::Case {
                        has_case_expr: operand.is_some(),
                        has_else_expr: else_expr.is_some(),
                        children,
                    }
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
/// facts. Ordinary eager/type-only and installed IF/COALESCE control calls
/// keep their exact lifecycle. Other guarded, lambda and relational protocols
/// remain explicit pending cases rather than becoming ordinary scalar calls.
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
        if let ExprKind::Constant(reference) = &definition.kind {
            let node = lowered
                .arena
                .node(local_id)
                .ok_or(ExpressionLoweringError::Invalid(
                    "missing lowered constant definition",
                ))?;
            let StaticExprKind::Constant(value) = node.kind() else {
                return Err(ExpressionLoweringError::Invalid(
                    "constant reference lost its checked source",
                ));
            };
            let source = package
                .constants()
                .resolve_observed(*reference, &definition.ty, work)?;
            work.flush()?;
            let same_type = definition
                .ty
                .exactly_equals_observed::<ExpressionLoweringError>(value.value_type(), || {
                    control
                        .checkpoint(CompilePhase::FunctionSpecialization, 0)
                        .map_err(ExpressionLoweringError::Control)
                })?;
            work.flush()?;
            // This is exact source retention, not semantic selected-value
            // equality: a private mapping cannot substitute another backing.
            if !same_type
                || value.pool().backing_identity() != source.pool().backing_identity()
                || value.ordinal() != source.ordinal()
            {
                return Err(ConstantReferenceError::InvalidConsumer(
                    "lowered constant reference differs from its checked source",
                )
                .into());
            }
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
                (ExprKind::Literal(_) | ExprKind::Constant(_), StaticExprKind::Constant(_)) => {
                    if invocation.control != ControlShape::Eager || !invocation.arguments.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "literal occurrence has arguments or control",
                        ));
                    }
                    ScopedExpressionEffects::pure_value(invocation.context)
                }
                (
                    ExprKind::Unary {
                        op: novarocks_physical_plan::UnaryOperator::Not,
                        expr,
                    },
                    StaticExprKind::Not(local_child),
                )
                | (
                    ExprKind::IsNull {
                        expr,
                        negated: false,
                    },
                    StaticExprKind::IsNull(local_child),
                )
                | (
                    ExprKind::IsNull {
                        expr,
                        negated: true,
                    },
                    StaticExprKind::IsNotNull(local_child),
                ) => {
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(local_child)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual unary control or operand differs",
                        ));
                    }
                    let child = invocation.arguments[0];
                    if flow.uses()[&child].definition != *expr {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual unary operand occurrence differs",
                        ));
                    }
                    ScopedExpressionEffects::pure_value(invocation.context).join_control_argument(
                        *effects.get(&child).ok_or(ExpressionLoweringError::Invalid(
                            "unary operand effects were not prepared",
                        ))?,
                        flow,
                        0,
                    )?
                }
                (
                    ExprKind::Cast {
                        expr,
                        target,
                        decimal_overflow_policy,
                        allow_throw_exception,
                    },
                    StaticExprKind::PreparedCast {
                        operation,
                        child,
                        decimal_overflow_policy: local_policy,
                        allow_throw_exception: local_allow,
                    },
                ) => {
                    let source_allow = package
                        .parameters()
                        .require(*allow_throw_exception)
                        .map_err(|_| {
                            ExpressionLoweringError::Invalid("cast parameter source differs")
                        })?;
                    if *operation != novarocks_functions::CastOperation::Carrier
                        || decimal_overflow_policy != local_policy
                        || source_allow
                            != &novarocks_type_contract::SemanticParameterValue::AllowThrowException(
                                *local_allow,
                            )
                        || invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 1
                        || lowered.ids.get(expr) != Some(child)
                        || flow.uses()[&invocation.arguments[0]].definition != *expr
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast occurrence differs from its frozen source",
                        ));
                    }
                    if !novarocks_type_contract::arrow_data_types_exact_observed::<
                        ExpressionLoweringError,
                    >(target, &source.ty.data_type, || {
                        work.step().map_err(ExpressionLoweringError::Control)
                    })? {
                        return Err(ExpressionLoweringError::Invalid(
                            "cast result differs from frozen target",
                        ));
                    }
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedCastRecipe::try_new(
                        *operation,
                        &package
                            .fragment()
                            .expressions()
                            .get(*expr)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing cast operand source",
                            ))?
                            .ty,
                        &source.ty,
                        *local_policy,
                        *local_allow,
                        control,
                    )?;
                    recipe
                        .own_effects(invocation.context)
                        .join_control_argument(
                            *effects.get(&invocation.arguments[0]).ok_or(
                                ExpressionLoweringError::Invalid(
                                    "cast child effects were not prepared",
                                ),
                            )?,
                            flow,
                            0,
                        )?
                }
                (
                    ExprKind::Binary {
                        op,
                        left,
                        right,
                        decimal_overflow_policy,
                        allow_throw_exception,
                    },
                    StaticExprKind::PreparedArithmetic {
                        operator,
                        left: local_left,
                        right: local_right,
                        decimal_overflow_policy: local_policy,
                        allow_throw_exception: local_allow,
                    },
                ) => {
                    let reference = allow_throw_exception.ok_or(
                        ExpressionLoweringError::Invalid("missing arithmetic parameter reference"),
                    )?;
                    let source_allow = package.parameters().require(reference).map_err(|_| {
                        ExpressionLoweringError::Invalid("arithmetic parameter source differs")
                    })?;
                    if arithmetic_operator(*op) != Some(*operator)
                        || decimal_overflow_policy != local_policy
                        || source_allow
                            != &novarocks_type_contract::SemanticParameterValue::AllowThrowException(
                                *local_allow,
                            )
                        || invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || lowered.ids.get(left) != Some(local_left)
                        || lowered.ids.get(right) != Some(local_right)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "arithmetic occurrence differs from its frozen source",
                        ));
                    }
                    let definitions = package.fragment().expressions();
                    work.flush()?;
                    let recipe = novarocks_functions::PreparedArithmeticRecipe::try_new(
                        *operator,
                        &definitions
                            .get(*left)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic left source",
                            ))?
                            .ty,
                        &definitions
                            .get(*right)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing arithmetic right source",
                            ))?
                            .ty,
                        &source.ty,
                        *local_policy,
                        *local_allow,
                        control,
                    )?;
                    let mut combined = recipe.own_effects(invocation.context);
                    for (ordinal, physical) in [*left, *right].into_iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != physical {
                            return Err(ExpressionLoweringError::Invalid(
                                "actual ordered arithmetic use differs",
                            ));
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "arithmetic child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::Binary {
                        op, left, right, ..
                    },
                    kind,
                ) if (kind.ordinary_comparison().is_some()
                    && comparison_operator(*op)
                        == kind.ordinary_comparison().map(|parts| parts.0))
                    || (*op == novarocks_physical_plan::BinaryOperator::EqForNull
                        && matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. })) =>
                {
                    let (local_left, local_right) = match kind {
                        StaticExprKind::PreparedNullSafeComparison { left, right } => {
                            (*left, *right)
                        }
                        _ => {
                            let (_, left, right) =
                                kind.ordinary_comparison().expect("checked comparison kind");
                            (left, right)
                        }
                    };
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || lowered.ids.get(left) != Some(&local_left)
                        || lowered.ids.get(right) != Some(&local_right)
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual comparison control or operands differ",
                        ));
                    }
                    let mut combined =
                        if matches!(kind, StaticExprKind::PreparedNullSafeComparison { .. }) {
                            work.flush()?;
                            novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(
                                &package
                                    .fragment()
                                    .expressions()
                                    .get(*left)
                                    .ok_or(ExpressionLoweringError::Invalid(
                                        "missing null-safe left source",
                                    ))?
                                    .ty,
                                &package
                                    .fragment()
                                    .expressions()
                                    .get(*right)
                                    .ok_or(ExpressionLoweringError::Invalid(
                                        "missing null-safe right source",
                                    ))?
                                    .ty,
                                control,
                            )?
                            .own_effects(invocation.context)
                        } else {
                            ScopedExpressionEffects::pure_value(invocation.context)
                        };
                    for (ordinal, physical) in [*left, *right].into_iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != physical {
                            return Err(ExpressionLoweringError::Invalid(
                                "actual ordered comparison use differs",
                            ));
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "comparison child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (
                    ExprKind::Case {
                        operand,
                        when_then,
                        else_expr,
                    },
                    StaticExprKind::Case {
                        has_case_expr,
                        has_else_expr,
                        children,
                    },
                ) => {
                    let count = case_arity(*operand, when_then, *else_expr)?;
                    let expected = ControlShape::Case {
                        simple: operand.is_some(),
                        arms: u32::try_from(when_then.len()).map_err(|_| {
                            ExpressionLoweringError::Invalid("CASE arm count is not representable")
                        })?,
                        has_else: else_expr.is_some(),
                    };
                    if invocation.control != expected
                        || *has_case_expr != operand.is_some()
                        || *has_else_expr != else_expr.is_some()
                        || children.len() != count
                        || invocation.arguments.len() != count
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual CASE control or children differ",
                        ));
                    }
                    let mut combined = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, local_child) in children.iter().enumerate() {
                        let source_child = case_child(*operand, when_then, *else_expr, ordinal)
                            .ok_or(ExpressionLoweringError::Invalid(
                                "missing actual CASE operand",
                            ))?;
                        let child_use = invocation.arguments[ordinal];
                        if lowered.ids.get(&source_child) != Some(local_child)
                            || flow.uses()[&child_use].definition != source_child
                        {
                            return Err(ExpressionLoweringError::Invalid(
                                "ordered CASE occurrence differs",
                            ));
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "CASE child effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
                }
                (ExprKind::Conjunction { args }, StaticExprKind::NaryAnd { args: local_args })
                | (ExprKind::Disjunction { args }, StaticExprKind::NaryOr { args: local_args }) => {
                    let expected = if matches!(source.kind, ExprKind::Conjunction { .. }) {
                        ControlShape::Conjunction
                    } else {
                        ControlShape::Disjunction
                    };
                    if invocation.control != expected
                        || args.len() != invocation.arguments.len()
                        || args.len() != local_args.len()
                        || args.is_empty()
                    {
                        return Err(ExpressionLoweringError::Invalid(
                            "actual Boolean connective control or arity differs",
                        ));
                    }
                    let mut combined = ScopedExpressionEffects::pure_value(invocation.context);
                    for (ordinal, &child) in args.iter().enumerate() {
                        let child_use = invocation.arguments[ordinal];
                        if flow.uses()[&child_use].definition != child {
                            return Err(ExpressionLoweringError::Invalid(
                                "ordered Boolean operand occurrences differ",
                            ));
                        }
                        combined = combined.join_control_argument(
                            *effects
                                .get(&child_use)
                                .ok_or(ExpressionLoweringError::Invalid(
                                    "Boolean operand effects were not prepared",
                                ))?,
                            flow,
                            ordinal,
                        )?;
                        work.step()?;
                    }
                    combined
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
                    let (type_only, control_intrinsic) =
                        match (invocation.control, frozen.effects.argument_control) {
                            (ControlShape::Eager, ArgumentControl::Eager) => (false, false),
                            (ControlShape::TypeOnly, ArgumentControl::TypeOnly) => (true, false),
                            (ControlShape::If, ArgumentControl::If)
                            | (ControlShape::Coalesce, ArgumentControl::Coalesce) => (false, true),
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
                            children = children.join_control_argument(
                                *effects.get(&child_use).ok_or(
                                    ExpressionLoweringError::Invalid(
                                        "argument effects were not prepared",
                                    ),
                                )?,
                                flow,
                                ordinal,
                            )?;
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
                    let options = if control_intrinsic {
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

// Static constants retain the actual checked source owner through binding;
// no scalar metadata enum is reconstructed from an Arrow value.
fn literal_argument(
    source: &ExprNode,
    node: &StaticExprNode,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = (|| {
        if let StaticExprKind::Constant(value) = node.kind()
            && !source
                .ty
                .exactly_equals_observed::<ExpressionLoweringError>(value.value_type(), || {
                    work.step().map_err(ExpressionLoweringError::Control)
                })?
        {
            return Err(ExpressionLoweringError::Invalid(
                "constant full type differs from source",
            ));
        }
        match (&source.kind, node.kind()) {
            (ExprKind::Literal(_) | ExprKind::Constant(_), StaticExprKind::Constant(value)) => {
                Ok(Some(value.clone()))
            }
            // A resolved input is not a constant, even if this particular runtime
            // batch happens to broadcast one scalar value (notably RAND seeds).
            (ExprKind::Value(_), StaticExprKind::SlotId(_)) => Ok(None),
            (ExprKind::FunctionCall { .. }, StaticExprKind::BoundCall { .. }) => Ok(None),
            (ExprKind::Conjunction { .. }, StaticExprKind::NaryAnd { .. })
            | (ExprKind::Disjunction { .. }, StaticExprKind::NaryOr { .. }) => Ok(None),
            (
                ExprKind::Unary {
                    op: novarocks_physical_plan::UnaryOperator::Not,
                    ..
                },
                StaticExprKind::Not(_),
            )
            | (ExprKind::IsNull { negated: false, .. }, StaticExprKind::IsNull(_))
            | (ExprKind::IsNull { negated: true, .. }, StaticExprKind::IsNotNull(_)) => Ok(None),
            (ExprKind::Case { .. }, StaticExprKind::Case { .. }) => Ok(None),
            (ExprKind::Cast { .. }, StaticExprKind::PreparedCast { .. }) => Ok(None),
            (ExprKind::Binary { op, .. }, StaticExprKind::PreparedArithmetic { operator, .. })
                if arithmetic_operator(*op) == Some(*operator) =>
            {
                Ok(None)
            }
            (
                ExprKind::Binary {
                    op: novarocks_physical_plan::BinaryOperator::EqForNull,
                    ..
                },
                StaticExprKind::PreparedNullSafeComparison { .. },
            ) => Ok(None),
            (ExprKind::Binary { op, .. }, kind)
                if kind.ordinary_comparison().is_some()
                    && comparison_operator(*op)
                        == kind.ordinary_comparison().map(|parts| parts.0) =>
            {
                Ok(None)
            }
            _ => Err(ExpressionLoweringError::Invalid(
                "unsupported call argument projection",
            )),
        }
    })();
    finish(result, &mut work)
}

fn comparison_operator(
    operator: novarocks_physical_plan::BinaryOperator,
) -> Option<novarocks_functions::ComparisonOperator> {
    use novarocks_functions::ComparisonOperator;
    use novarocks_physical_plan::BinaryOperator;
    Some(match operator {
        BinaryOperator::Eq => ComparisonOperator::Eq,
        BinaryOperator::NotEq => ComparisonOperator::Ne,
        BinaryOperator::Lt => ComparisonOperator::Lt,
        BinaryOperator::LtEq => ComparisonOperator::Le,
        BinaryOperator::Gt => ComparisonOperator::Gt,
        BinaryOperator::GtEq => ComparisonOperator::Ge,
        _ => return None,
    })
}

#[cfg(test)]
#[path = "literal_metadata_tests.rs"]
mod literal_metadata_tests;

fn arithmetic_operator(
    operator: novarocks_physical_plan::BinaryOperator,
) -> Option<novarocks_type_contract::ArithmeticOperator> {
    use novarocks_physical_plan::BinaryOperator as B;
    use novarocks_type_contract::ArithmeticOperator as A;
    Some(match operator {
        B::Add => A::Add,
        B::Subtract => A::Subtract,
        B::Multiply => A::Multiply,
        B::Divide => A::Divide,
        B::Modulo => A::Modulo,
        _ => return None,
    })
}

#[cfg(test)]
#[path = "checked_reference_tests.rs"]
mod checked_reference_tests;
