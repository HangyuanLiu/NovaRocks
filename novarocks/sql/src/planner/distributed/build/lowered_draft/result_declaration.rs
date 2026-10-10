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
//! Original ordered result declaration from the same unpublished SQL emission.
//! This receipt is an output-check obligation, not a computed kernel type.
use novarocks_physical_plan::{PlanVersionId, ResultField, ResultPort};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
    arrow_data_types_exact_borrowed_observed,
};

#[derive(Debug)]
pub struct SqlResultDeclaration {
    version: PlanVersionId,
    original: ResultPort,
}

#[derive(Debug)]
pub enum ResultDeclarationError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Association(&'static str),
}
impl std::fmt::Display for ResultDeclarationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Type(cause) => cause.fmt(f),
            Self::Association(detail) => f.write_str(detail),
        }
    }
}
impl std::error::Error for ResultDeclarationError {}
impl From<CompileControlError> for ResultDeclarationError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for ResultDeclarationError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}

/// Constructed only by the same source owner that retains this declaration
/// and the computed port. No caller can supply an unverified public schema.
pub struct CheckedSqlResultDeclaration<'a> {
    original: &'a ResultPort,
}
impl<'a> CheckedSqlResultDeclaration<'a> {
    pub const fn original_port(&self) -> &'a ResultPort {
        self.original
    }
    pub(super) const fn from_original_port(original: &'a ResultPort) -> Self {
        Self { original }
    }

    pub fn fields(&self) -> &[ResultField] {
        &self.original.fields
    }
}

/// Only the same completed owner can construct this publication proof.
/// The original port stays owned here once; no second public schema is cached.
#[derive(Debug)]
pub(super) struct PublishedSqlResultDeclaration {
    declaration: SqlResultDeclaration,
}
impl PublishedSqlResultDeclaration {
    pub(super) fn checked_loan(&self) -> CheckedSqlResultDeclaration<'_> {
        CheckedSqlResultDeclaration {
            original: &self.declaration.original,
        }
    }
    pub(super) fn recheck_observed(
        &self,
        version: PlanVersionId,
        computed: &ResultPort,
        control: &dyn PureCompileControl,
    ) -> Result<CheckedSqlResultDeclaration<'_>, ResultDeclarationError> {
        self.declaration
            .check_computed_port(version, computed, control)
    }
}

impl SqlResultDeclaration {
    pub(in crate::planner::distributed::build) const fn original_port(&self) -> &ResultPort {
        &self.original
    }
    pub(super) fn publish(
        self,
        version: PlanVersionId,
        computed: &ResultPort,
        control: &dyn PureCompileControl,
    ) -> Result<PublishedSqlResultDeclaration, ResultDeclarationError> {
        self.check_computed_port(version, computed, control)?;
        Ok(PublishedSqlResultDeclaration { declaration: self })
    }

    /// Move the actual root ResultPort just authored by the original lowering.
    /// This neither clones a plan nor reads TypedExpr nullability again.
    pub(in crate::planner::distributed::build) fn capture(
        version: PlanVersionId,
        original: ResultPort,
    ) -> Self {
        Self { version, original }
    }

    /// Called by the owning SqlAuthoredPhysicalPlan, never from an arbitrary
    /// same-version external plan. The computed port must be its actual port.
    pub(in crate::planner::distributed::build) fn check_computed_port<'a>(
        &'a self,
        version: PlanVersionId,
        computed: &ResultPort,
        control: &dyn PureCompileControl,
    ) -> Result<CheckedSqlResultDeclaration<'a>, ResultDeclarationError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = self.check_inner(version, computed, &mut work);
        if matches!(&result, Err(ResultDeclarationError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn check_inner<'a>(
        &'a self,
        version: PlanVersionId,
        computed: &ResultPort,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedSqlResultDeclaration<'a>, ResultDeclarationError> {
        work.step()?;
        if version != self.version
            || computed.fragment != self.original.fragment
            || computed.output.node != self.original.output.node
            || computed.fields.len() != self.original.fields.len()
            || computed.output.columns.len() != self.original.output.columns.len()
            || computed.fields.len() != computed.output.columns.len()
            || self.original.fields.len() != self.original.output.columns.len()
        {
            return Err(ResultDeclarationError::Association(
                "source result declaration differs from its completed port identity",
            ));
        }
        for (ordinal, (declared, actual)) in self
            .original
            .fields
            .iter()
            .zip(computed.fields.iter())
            .enumerate()
        {
            work.step()?;
            if declared.value != actual.value
                || declared.value != self.original.output.columns[ordinal]
                || actual.value != computed.output.columns[ordinal]
                || !text_equal(&declared.name, &actual.name, work)?
                || !alias_equal(declared.alias.as_deref(), actual.alias.as_deref(), work)?
                || declared.ty.logical_type != actual.ty.logical_type
                // Only root false -> true admission widening is permitted.
                // Nested nullable/dictionary/metadata facts remain exact below.
                || (declared.ty.nullable && !actual.ty.nullable)
                || !arrow_data_types_exact_borrowed_observed::<ResultDeclarationError>(
                    &declared.ty.data_type,
                    &actual.ty.data_type,
                    || work.step().map_err(ResultDeclarationError::from),
                )?
            {
                return Err(ResultDeclarationError::Association(
                    "source result declaration differs beyond root nullable admission",
                ));
            }
        }
        Ok(CheckedSqlResultDeclaration {
            original: &self.original,
        })
    }
}

fn text_equal(
    a: &str,
    b: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ResultDeclarationError> {
    work.step()?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (left, right) in a.bytes().zip(b.bytes()) {
        work.step()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
fn alias_equal(
    a: Option<&str>,
    b: Option<&str>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, ResultDeclarationError> {
    work.step()?;
    match (a, b) {
        (Some(a), Some(b)) => text_equal(a, b, work),
        (None, None) => Ok(true),
        _ => Ok(false),
    }
}
