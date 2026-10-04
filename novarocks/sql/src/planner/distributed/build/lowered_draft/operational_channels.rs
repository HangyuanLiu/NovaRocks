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

//! Operational request channels recorded by the original SQL emitter.
//! This projection neither selects an overload nor constructs effect facts.

use std::{alloc::Layout, sync::Arc};

use novarocks_functions::{
    ConstantError, ConstantValue, FunctionArgument, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_physical_plan::{
    ConstantPools, ConstantReferenceError, ExprId, ExprKind, ExprNode, Fragment, NodeId,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionArgumentType, FunctionValueType,
    ValueTypeError,
};

use super::{
    CheckedExpressionLogicalSourceEntry, CheckedTableLogicalSourceEntry,
    LoweredExpressionLogicalSource, SqlExpressionCallKind, SqlSourceJournalError,
};
use crate::{binding::CapturedLogicalCallArguments, common::variant_source::DerivedVariantSource};

#[derive(Debug)]
pub(in crate::planner::distributed::build) struct LoweredOperationalChannel {
    pub(in crate::planner::distributed::build) expression: ExprId,
    pub(in crate::planner::distributed::build) role: SqlOperationalChannelRole,
}

/// These roles are recorded at emission, never inferred from a later Constant.
#[derive(Debug)]
pub(in crate::planner::distributed::build) enum SqlOperationalChannelRole {
    ValueWithoutConstant,
    CapturedConstant,
    CanonicalNull { value: ConstantValue },
    Lambda,
    DerivedVariantPath,
    DerivedVariantTypeLiteral,
}

#[derive(Debug)]
pub(crate) enum SqlOperationalProjectionError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Constant(ConstantError),
    Reference(ConstantReferenceError),
    Journal(SqlSourceJournalError),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for SqlOperationalProjectionError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for SqlOperationalProjectionError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl From<ConstantError> for SqlOperationalProjectionError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(cause) => Self::Control(cause),
            ConstantError::Limit(_) => Self::Control(CompileControlError::ResourceExhausted),
            error => Self::Constant(error),
        }
    }
}
impl From<ConstantReferenceError> for SqlOperationalProjectionError {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(cause) => Self::Control(cause),
            ConstantReferenceError::Constant(error) => Self::from(error),
            error => Self::Reference(error),
        }
    }
}
impl From<SqlSourceJournalError> for SqlOperationalProjectionError {
    fn from(error: SqlSourceJournalError) -> Self {
        match error {
            SqlSourceJournalError::Control(cause) => Self::Control(cause),
            error => Self::Journal(error),
        }
    }
}
impl std::fmt::Display for SqlOperationalProjectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Type(error) => error.fmt(formatter),
            Self::Constant(error) => error.fmt(formatter),
            Self::Reference(error) => error.fmt(formatter),
            Self::Journal(error) => write!(formatter, "operational source journal: {error:?}"),
            Self::InvalidSource(detail) => formatter.write_str(detail),
        }
    }
}
impl std::error::Error for SqlOperationalProjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Type(error) => Some(error),
            Self::Constant(error) => Some(error),
            Self::Reference(error) => Some(error),
            Self::Journal(_) | Self::InvalidSource(_) => None,
        }
    }
}

impl<'a> CheckedExpressionLogicalSourceEntry<'a> {
    pub(in crate::planner::distributed::build) fn channels(
        &self,
    ) -> &'a [LoweredOperationalChannel] {
        &self.entry.channels
    }

    /// Caller owns entry/footer and admission of source, delegated comparison
    /// scratch, type clones and retained coexistence. A checked Layout and
    /// fallible request here do not constitute a host allocation grant.
    pub(crate) fn operational_arguments_observed(
        &self,
        pools: &ConstantPools,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
        let context = Projection {
            fragment: self.fragment,
            owner: self.entry.owner,
            lambda_scope: self.entry.lambda_scope,
            kind: Some(self.entry.kind),
            captured: self.entry.captured.captured(),
            derived: match &self.entry.captured {
                LoweredExpressionLogicalSource::DerivedVariant(source) => Some(source.as_ref()),
                LoweredExpressionLogicalSource::Owned(_) => None,
            },
            arguments: &self.entry.arguments,
            channels: &self.entry.channels,
        };
        context.check_extent(work)?;
        super::validate_expression_source_entry_observed(self.entry, self.source, work)?;
        context.project(pools, work)
    }
}
impl<'a> CheckedTableLogicalSourceEntry<'a> {
    pub(in crate::planner::distributed::build) fn channels(
        &self,
    ) -> &'a [LoweredOperationalChannel] {
        &self.entry.channels
    }

    /// The same projection preserves the whole captured Relation request;
    /// there is no scalar result projection or Table NULL canonicalization.
    pub(crate) fn operational_arguments_observed(
        &self,
        pools: &ConstantPools,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
        let context = Projection {
            fragment: self.fragment,
            owner: self.source.id,
            lambda_scope: None,
            kind: None,
            captured: &self.entry.captured,
            derived: None,
            arguments: &self.entry.arguments,
            channels: &self.entry.channels,
        };
        context.check_extent(work)?;
        super::validate_table_source_entry_observed(self.entry, self.source, work)?;
        context.project(pools, work)
    }
}

struct Projection<'a> {
    fragment: &'a Fragment,
    owner: NodeId,
    lambda_scope: Option<ExprId>,
    kind: Option<SqlExpressionCallKind>,
    captured: &'a CapturedLogicalCallArguments,
    derived: Option<&'a DerivedVariantSource>,
    arguments: &'a [ExprId],
    channels: &'a [LoweredOperationalChannel],
}
impl Projection<'_> {
    fn check_extent(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlOperationalProjectionError> {
        let count = self.channels.len();
        if count > MAX_CALL_EFFECT_ARGUMENTS
            || self.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
            || self.captured.request().arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        Layout::array::<FunctionArgument>(count)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        let exact =
            count == self.arguments.len() && count == self.captured.request().arguments.len();
        work.step()?;
        require(
            exact,
            "operational channels differ from their original request extent",
        )
    }

    fn expression<'a>(
        &'a self,
        id: ExprId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a ExprNode, SqlOperationalProjectionError> {
        work.flush()?;
        let expression = self.fragment.expressions().get(id);
        work.step()?;
        work.flush()?;
        expression.ok_or(SqlOperationalProjectionError::InvalidSource(
            "operational channel has no actual emitted expression",
        ))
    }

    fn project(
        &self,
        pools: &ConstantPools,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
        work.flush()?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(self.channels.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        for (ordinal, ((channel, id), original)) in self
            .channels
            .iter()
            .zip(self.arguments)
            .zip(self.captured.request().arguments)
            .enumerate()
        {
            let same_id = channel.expression == *id;
            work.step()?;
            require(
                same_id,
                "operational channel differs from its recorded expression identity",
            )?;
            let expression = self.expression(*id, work)?;
            let same_scope =
                expression.owner == self.owner && expression.lambda_scope == self.lambda_scope;
            work.step()?;
            require(
                same_scope,
                "operational argument belongs to a different owner or Lambda scope",
            )?;
            let argument = match (&channel.role, original) {
                (
                    SqlOperationalChannelRole::ValueWithoutConstant,
                    FunctionArgument::Value { constant: None, .. },
                ) => {
                    let scalar = !matches!(expression.kind, ExprKind::Lambda { .. });
                    work.step()?;
                    require(scalar, "nonconstant Value role refers to a Lambda")?;
                    FunctionArgument::Value {
                        value_type: clone_type(&expression.ty, work)?,
                        constant: None,
                    }
                }
                (
                    SqlOperationalChannelRole::CapturedConstant,
                    FunctionArgument::Value {
                        value_type,
                        constant: Some(value),
                    },
                ) => {
                    exact_type(value_type, value.value_type(), work)?;
                    verify_constant(expression, value, pools, work)?;
                    constant_argument(value, work)?
                }
                (
                    SqlOperationalChannelRole::CanonicalNull { value },
                    FunctionArgument::Value { value_type, .. },
                ) => {
                    let lawful =
                        matches!(
                            self.kind,
                            Some(SqlExpressionCallKind::Scalar | SqlExpressionCallKind::Window)
                        ) && matches!(value_type.data_type, arrow::datatypes::DataType::Null);
                    work.step()?;
                    require(
                        lawful,
                        "canonical NULL role is not an original scalar or Window NULL source",
                    )?;
                    let expected = self
                        .captured
                        .binding()
                        .resolved()
                        .selected
                        .argument_types
                        .get(ordinal);
                    work.step()?;
                    let Some(FunctionArgumentType::Value(expected)) = expected else {
                        return Err(SqlOperationalProjectionError::InvalidSource(
                            "canonical NULL has no original selected Value target",
                        ));
                    };
                    let nonnull_carrier =
                        !matches!(expected.data_type, arrow::datatypes::DataType::Null);
                    work.step()?;
                    require(
                        nonnull_carrier,
                        "canonical NULL target is still the Null carrier",
                    )?;
                    let mut expected = clone_type(expected, work)?;
                    expected.nullable = true;
                    work.step()?;
                    exact_type(&expected, value.value_type(), work)?;
                    work.flush()?;
                    let outcome = value
                        .is_null_observed(CompilePhase::FunctionSpecialization, work.control())
                        .map_err(SqlOperationalProjectionError::from);
                    let null = completed_delegate(outcome, work)?;
                    require(
                        null,
                        "canonical NULL role retains a non-NULL selected value",
                    )?;
                    verify_constant(expression, value, pools, work)?;
                    constant_argument(value, work)?
                }
                (
                    SqlOperationalChannelRole::DerivedVariantPath
                    | SqlOperationalChannelRole::DerivedVariantTypeLiteral,
                    FunctionArgument::Value { .. },
                ) => {
                    let lawful = self.kind == Some(SqlExpressionCallKind::DerivedVariant);
                    work.step()?;
                    require(
                        lawful,
                        "derived operational channel has a different original producer",
                    )?;
                    let source =
                        self.derived
                            .ok_or(SqlOperationalProjectionError::InvalidSource(
                                "derived operational channel lacks its original descriptor",
                            ))?;
                    let (expected_ordinal, value) = match channel.role {
                        SqlOperationalChannelRole::DerivedVariantPath => {
                            (1, source.canonical_path())
                        }
                        SqlOperationalChannelRole::DerivedVariantTypeLiteral => {
                            (2, source.type_literal())
                        }
                        _ => unreachable!("matched derived roles"),
                    };
                    let exact_ordinal = ordinal == expected_ordinal;
                    work.step()?;
                    require(
                        exact_ordinal,
                        "derived operational role is at a different request ordinal",
                    )?;
                    verify_constant(expression, value, pools, work)?;
                    constant_argument(value, work)?
                }
                (
                    SqlOperationalChannelRole::Lambda,
                    FunctionArgument::Lambda {
                        parameter_types, ..
                    },
                ) => self.lambda_argument(expression, parameter_types, work)?,
                _ => {
                    work.step()?;
                    return Err(SqlOperationalProjectionError::InvalidSource(
                        "operational role differs from its original captured argument shape",
                    ));
                }
            };
            result.push(argument);
            work.step()?;
        }
        work.flush()?;
        let result = result.into_boxed_slice();
        work.step()?;
        work.flush()?;
        Ok(result)
    }

    fn lambda_argument(
        &self,
        expression: &ExprNode,
        original_parameters: &[FunctionValueType],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionArgument, SqlOperationalProjectionError> {
        let ExprKind::Lambda {
            parameter_types,
            body,
        } = &expression.kind
        else {
            work.step()?;
            return Err(SqlOperationalProjectionError::InvalidSource(
                "Lambda role refers to a non-Lambda expression",
            ));
        };
        if parameter_types.len() > MAX_CALL_EFFECT_ARGUMENTS
            || original_parameters.len() > MAX_CALL_EFFECT_ARGUMENTS
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        Layout::array::<FunctionValueType>(parameter_types.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        let same_count = parameter_types.len() == original_parameters.len();
        work.step()?;
        require(
            same_count,
            "Lambda parameter extent differs from its original captured signature",
        )?;
        for (actual, original) in parameter_types.iter().zip(original_parameters) {
            exact_type(actual, original, work)?;
        }
        let body = self.expression(*body, work)?;
        let same_scope = body.owner == expression.owner && body.lambda_scope == Some(expression.id);
        work.step()?;
        require(
            same_scope,
            "Lambda body belongs to a different actual scope",
        )?;
        exact_type(&expression.ty, &body.ty, work)?;
        work.flush()?;
        let mut parameters = Vec::new();
        parameters
            .try_reserve_exact(parameter_types.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        work.flush()?;
        for parameter in parameter_types.iter() {
            parameters.push(clone_type(parameter, work)?);
            work.step()?;
        }
        work.flush()?;
        let parameter_types = parameters.into_boxed_slice();
        work.step()?;
        work.flush()?;
        Ok(FunctionArgument::Lambda {
            parameter_types,
            result_type: clone_type(&body.ty, work)?,
        })
    }
}

fn require(condition: bool, detail: &'static str) -> Result<(), SqlOperationalProjectionError> {
    if condition {
        Ok(())
    } else {
        Err(SqlOperationalProjectionError::InvalidSource(detail))
    }
}
fn clone_type(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, SqlOperationalProjectionError> {
    work.flush()?;
    let value = value.clone();
    work.step()?;
    work.flush()?;
    Ok(value)
}
fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlOperationalProjectionError> {
    let exact = left.exactly_equals_observed(right, || {
        work.step().map_err(SqlOperationalProjectionError::from)
    })?;
    require(
        exact,
        "operational channel differs from its complete authored value type",
    )
}
fn verify_constant(
    expression: &ExprNode,
    value: &ConstantValue,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlOperationalProjectionError> {
    exact_type(&expression.ty, value.value_type(), work)?;
    let ExprKind::Constant(reference) = expression.kind else {
        work.step()?;
        return Err(SqlOperationalProjectionError::InvalidSource(
            "captured constant has no actual ConstantReference source",
        ));
    };
    work.step()?;
    work.flush()?;
    let outcome = pools
        .resolve_source_observed(reference, work)
        .map_err(SqlOperationalProjectionError::from);
    let actual = completed_delegate(outcome, work)?;
    let same_source = actual.ordinal() == value.ordinal()
        && actual.pool().backing_identity() == value.pool().backing_identity()
        && Arc::ptr_eq(actual.pool().field_ref(), value.pool().field_ref());
    work.step()?;
    require(
        same_source,
        "operational constant has a different original Field, backing or ordinal",
    )?;
    exact_type(actual.value_type(), value.value_type(), work)
}
fn constant_argument(
    value: &ConstantValue,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionArgument, SqlOperationalProjectionError> {
    let value_type = clone_type(value.value_type(), work)?;
    work.flush()?;
    let constant = value.clone();
    work.step()?;
    work.flush()?;
    Ok(FunctionArgument::Value {
        value_type,
        constant: Some(constant),
    })
}

// A delegated ordinary outcome completes a bounded operation. An originating
// control/resource refusal returns before any further caller observation.
fn completed_delegate<T>(
    outcome: Result<T, SqlOperationalProjectionError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<T, SqlOperationalProjectionError> {
    if matches!(outcome, Err(SqlOperationalProjectionError::Control(_))) {
        return outcome;
    }
    work.step()?;
    work.flush()?;
    outcome
}
