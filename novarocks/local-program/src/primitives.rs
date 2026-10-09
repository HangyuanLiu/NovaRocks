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

//! Mandatory primitive recipes derived from the same checked use/type chain.
//! No external side table, name resolver or runtime preparation is accepted.

use crate::{ProgramLexicalBindings, ProgramUseRef, StaticExprKind};
use novarocks_functions::{
    ComparisonOperator, ComparisonPrepareError, FunctionArgumentType, FunctionValueType,
    PreparedComparisonRecipe,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, PureCompileControl,
    ValueLogicalType,
};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ProgramComparisonSite {
    Binary(ProgramUseRef),
    CaseWhen { occurrence: ProgramUseRef, arm: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramPrimitiveError {
    Control(CompileControlError),
    Comparison(ComparisonPrepareError),
    Arithmetic(novarocks_functions::ArithmeticPrepareError),
    Cast(novarocks_functions::CastPrepareError),
    Invalid(&'static str),
}
impl From<CompileControlError> for ProgramPrimitiveError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ComparisonPrepareError> for ProgramPrimitiveError {
    fn from(value: ComparisonPrepareError) -> Self {
        if let Some(cause) = value.control_error() {
            Self::Control(cause)
        } else {
            Self::Comparison(value)
        }
    }
}
impl fmt::Display for ProgramPrimitiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Comparison(e) => e.fmt(f),
            Self::Arithmetic(e) => e.fmt(f),
            Self::Cast(e) => e.fmt(f),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for ProgramPrimitiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Comparison(e) => Some(e),
            Self::Arithmetic(e) => Some(e),
            Self::Cast(e) => Some(e),
            Self::Invalid(_) => None,
        }
    }
}

pub(crate) fn compile_comparisons(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<ProgramComparisonSite, PreparedComparisonRecipe>, ProgramPrimitiveError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = compile_core(checked, control, &mut work);
    if matches!(result, Err(ProgramPrimitiveError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn compile_core(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeMap<ProgramComparisonSite, PreparedComparisonRecipe>, ProgramPrimitiveError> {
    let typed = checked.channels().expressions();
    let snapshot = typed.resolved_calls().snapshot();
    let mut recipes = BTreeMap::new();
    for (&arena, flow) in snapshot.flows() {
        let definitions = &snapshot.roots().arenas()[&arena];
        let types = &typed.types()[&arena];
        let value =
            |id: crate::ProgramExprId| -> Result<&FunctionValueType, ProgramPrimitiveError> {
                match types.get(id.index()) {
                    Some(FunctionArgumentType::Value(ty)) => Ok(ty),
                    _ => Err(ProgramPrimitiveError::Invalid(
                        "primitive requires an exact value type",
                    )),
                }
            };
        for (&use_id, invocation) in flow.uses() {
            work.step()?;
            let occurrence = ProgramUseRef { arena, use_id };
            let kind = definitions
                .node(invocation.definition)
                .ok_or(ProgramPrimitiveError::Invalid(
                    "missing primitive definition",
                ))?
                .kind();
            match kind {
                kind if kind.ordinary_comparison().is_some() => {
                    let (operator, left, right) =
                        kind.ordinary_comparison().expect("checked comparison kind");
                    if invocation.control != ControlShape::Eager
                        || invocation.arguments.len() != 2
                        || flow.uses()[&invocation.arguments[0]].definition != left
                        || flow.uses()[&invocation.arguments[1]].definition != right
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "comparison differs from its actual ordered occurrence",
                        ));
                    }
                    work.flush()?;
                    let recipe = PreparedComparisonRecipe::try_new(
                        operator,
                        value(left)?,
                        value(right)?,
                        control,
                    )?;
                    let result = value(invocation.definition)?;
                    if result.data_type != arrow_schema::DataType::Boolean
                        || result.logical_type != ValueLogicalType::Physical
                        || (recipe.nullable_result() && !result.nullable)
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "comparison result loses successful SQL NULL or its Boolean domain",
                        ));
                    }
                    recipes.insert(ProgramComparisonSite::Binary(occurrence), recipe);
                    work.step()?;
                }
                StaticExprKind::Case {
                    has_case_expr: true,
                    has_else_expr,
                    children,
                } => {
                    let ControlShape::Case {
                        simple: true,
                        arms,
                        has_else,
                    } = invocation.control
                    else {
                        return Err(ProgramPrimitiveError::Invalid(
                            "simple CASE differs from actual control shape",
                        ));
                    };
                    let count = (arms as usize)
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(1 + usize::from(has_else)))
                        .ok_or(ProgramPrimitiveError::Invalid("CASE arity overflow"))?;
                    if arms == 0
                        || has_else != *has_else_expr
                        || count != children.len()
                        || invocation.arguments.len() != count
                    {
                        return Err(ProgramPrimitiveError::Invalid(
                            "simple CASE differs from its ordered children",
                        ));
                    }
                    for (ordinal, definition) in children.iter().enumerate() {
                        if flow.uses()[&invocation.arguments[ordinal]].definition != *definition {
                            return Err(ProgramPrimitiveError::Invalid(
                                "simple CASE child differs from actual use",
                            ));
                        }
                        work.step()?;
                    }
                    for arm in 0..arms {
                        work.flush()?;
                        let recipe = PreparedComparisonRecipe::try_new(
                            ComparisonOperator::Eq,
                            value(children[0])?,
                            value(children[1 + arm as usize * 2])?,
                            control,
                        )?;
                        recipes.insert(ProgramComparisonSite::CaseWhen { occurrence, arm }, recipe);
                        work.step()?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(recipes)
}

impl From<novarocks_functions::ArithmeticPrepareError> for ProgramPrimitiveError {
    fn from(value: novarocks_functions::ArithmeticPrepareError) -> Self {
        if let Some(cause) = value.control_error() {
            Self::Control(cause)
        } else {
            Self::Arithmetic(value)
        }
    }
}

pub(crate) fn compile_arithmetic(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<
    BTreeMap<ProgramUseRef, novarocks_functions::PreparedArithmeticRecipe>,
    ProgramPrimitiveError,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "arithmetic requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing arithmetic definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedArithmetic {
                    operator,
                    left,
                    right,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } = kind
                else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 2
                    || flow.uses()[&invocation.arguments[0]].definition != *left
                    || flow.uses()[&invocation.arguments[1]].definition != *right
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "arithmetic differs from its actual ordered occurrence",
                    ));
                }
                work.flush()?;
                let recipe = novarocks_functions::PreparedArithmeticRecipe::try_new(
                    *operator,
                    value(*left)?,
                    value(*right)?,
                    value(invocation.definition)?,
                    *decimal_overflow_policy,
                    *allow_throw_exception,
                    control,
                )?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
                work.step()?;
            }
        }
        Ok(recipes)
    })();
    if matches!(result, Err(ProgramPrimitiveError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

impl From<novarocks_functions::CastPrepareError> for ProgramPrimitiveError {
    fn from(value: novarocks_functions::CastPrepareError) -> Self {
        if let Some(cause) = value.control_error() {
            Self::Control(cause)
        } else {
            Self::Cast(value)
        }
    }
}

pub(crate) fn compile_casts(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<ProgramUseRef, novarocks_functions::PreparedCastRecipe>, ProgramPrimitiveError>
{
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "cast requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid("missing cast definition"))?
                    .kind();
                let StaticExprKind::PreparedCast {
                    operation,
                    child,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } = kind
                else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 1
                    || flow.uses()[&invocation.arguments[0]].definition != *child
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "cast differs from its actual operand occurrence",
                    ));
                }
                work.flush()?;
                let recipe = novarocks_functions::PreparedCastRecipe::try_new(
                    *operation,
                    value(*child)?,
                    value(invocation.definition)?,
                    *decimal_overflow_policy,
                    *allow_throw_exception,
                    control,
                )?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
                work.step()?;
            }
        }
        Ok(recipes)
    })();
    if matches!(result, Err(ProgramPrimitiveError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn compile_null_safe_comparisons(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<
    BTreeMap<ProgramUseRef, novarocks_functions::PreparedNullSafeComparisonRecipe>,
    ProgramPrimitiveError,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "null-safe comparison requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing null-safe comparison definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedNullSafeComparison { left, right } = kind else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 2
                    || flow.uses()[&invocation.arguments[0]].definition != *left
                    || flow.uses()[&invocation.arguments[1]].definition != *right
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "null-safe comparison differs from its actual ordered occurrence",
                    ));
                }
                let result = value(invocation.definition)?;
                if result.data_type != arrow_schema::DataType::Boolean
                    || result.logical_type != ValueLogicalType::Physical
                    || result.nullable
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "null-safe comparison requires nonnullable Physical Boolean result",
                    ));
                }
                work.flush()?;
                let recipe = novarocks_functions::PreparedNullSafeComparisonRecipe::try_new(
                    value(*left)?,
                    value(*right)?,
                    control,
                )?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
                work.step()?;
            }
        }
        Ok(recipes)
    })();
    if matches!(result, Err(ProgramPrimitiveError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn compile_native_negate(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<
    BTreeMap<ProgramUseRef, novarocks_functions::PreparedNativeNegateRecipe>,
    ProgramPrimitiveError,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "arithmetic requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing arithmetic definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedNativeNegate(child) = kind else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 1
                    || flow.uses()[&invocation.arguments[0]].definition != *child
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "native negate differs from its actual ordered occurrence",
                    ));
                }
                work.flush()?;
                let recipe = novarocks_functions::PreparedNativeNegateRecipe::try_new(
                    value(*child)?,
                    value(invocation.definition)?,
                    control,
                )?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
                work.step()?;
            }
        }
        Ok(recipes)
    })();
    if matches!(result, Err(ProgramPrimitiveError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn compile_native_bitnot(
    checked: &ProgramLexicalBindings,
    control: &dyn PureCompileControl,
) -> Result<
    BTreeMap<ProgramUseRef, novarocks_functions::PreparedNativeBitNotRecipe>,
    ProgramPrimitiveError,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let mut recipes = BTreeMap::new();
        for (&arena, flow) in snapshot.flows() {
            let definitions = &snapshot.roots().arenas()[&arena];
            let types = &typed.types()[&arena];
            let value = |id: crate::ProgramExprId| match types.get(id.index()) {
                Some(FunctionArgumentType::Value(value)) => Ok(value),
                _ => Err(ProgramPrimitiveError::Invalid(
                    "arithmetic requires complete value types",
                )),
            };
            for (&use_id, invocation) in flow.uses() {
                work.step()?;
                let kind = definitions
                    .node(invocation.definition)
                    .ok_or(ProgramPrimitiveError::Invalid(
                        "missing arithmetic definition",
                    ))?
                    .kind();
                let StaticExprKind::PreparedNativeBitNot(child) = kind else {
                    continue;
                };
                if invocation.control != ControlShape::Eager
                    || invocation.arguments.len() != 1
                    || flow.uses()[&invocation.arguments[0]].definition != *child
                {
                    return Err(ProgramPrimitiveError::Invalid(
                        "native BitwiseNot differs from its actual ordered occurrence",
                    ));
                }
                work.flush()?;
                let recipe = novarocks_functions::PreparedNativeBitNotRecipe::try_new(
                    value(*child)?,
                    value(invocation.definition)?,
                    control,
                )?;
                recipes.insert(ProgramUseRef { arena, use_id }, recipe);
                work.step()?;
            }
        }
        Ok(recipes)
    })();
    if matches!(result, Err(ProgramPrimitiveError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
