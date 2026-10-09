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

//! All stored call definitions, borrowed through the original source visitor
//! and same-emission journal. No prepare, payload evaluation, or name lookup.
use super::lowered_draft::{
    SqlAuthoredPhysicalPlan, SqlCallDependencyLoan, SqlCallDependencySite, SqlSourceJournalError,
};
use super::physical_fragment_effects::{
    PhysicalFragmentEffectsError, aggregate_source, writer_source,
};
use crate::compiler::SqlPhysicalEmissionMode;
use novarocks_functions::{FunctionBindingError, PureCallLifecycle};
use novarocks_physical_plan::{
    ExprKind, FragmentDefinitionSource, PhysicalCallDefinition, PhysicalCallSite,
    visit_fragment_definitions_observed,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

#[derive(Debug)]
pub(crate) enum SqlSourceSupportError {
    Binding(FunctionBindingError),
    Journal(SqlSourceJournalError),
    Relational(PhysicalFragmentEffectsError),
    InvalidSource(&'static str),
}
impl SqlSourceSupportError {
    pub(crate) fn into_compile_error(self) -> crate::compiler::SqlCompileError {
        match self {
            Self::Binding(error) => error.into(),
            Self::Journal(SqlSourceJournalError::Control(cause)) => cause.into(),
            Self::Relational(PhysicalFragmentEffectsError::Control(cause)) => cause.into(),
            other => crate::compiler::SqlCompileError::Compilation(format!(
                "SQL call definition support: {other:?}"
            )),
        }
    }
}
impl From<CompileControlError> for SqlSourceSupportError {
    fn from(cause: CompileControlError) -> Self {
        Self::Binding(FunctionBindingError::Control(cause))
    }
}

fn admit_loan(
    functions: &dyn crate::compiler::SqlFunctionCatalog,
    loan: SqlCallDependencyLoan<'_>,
    lifecycle: PureCallLifecycle,
    control: &dyn PureCompileControl,
) -> Result<(), SqlSourceSupportError> {
    // The canonical request, when actually authored, owns the emitted exact
    // types. Original declaration and provenance remain retained by this loan.
    let binding = loan
        .canonical()
        .map(|canonical| canonical.binding())
        .unwrap_or_else(|| loan.original_binding());
    functions
        .admit_bound_lifecycle_observed(binding.resolved(), lifecycle, control)
        .map_err(SqlSourceSupportError::Binding)
}

pub(crate) fn admit_all_call_definitions_observed(
    owner: &SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> Result<(), SqlSourceSupportError> {
    if owner.emission_mode() == SqlPhysicalEmissionMode::OriginalNativeV1 {
        return Ok(());
    }
    check_source_definitions(owner, control)
}

/// Explicit read-only inspection of a completed original SQL source using its
/// same installed owner loan. This is lifecycle/static profile admission only.
pub fn check_pure_call_definitions_observed(
    owner: &SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> Result<(), crate::compiler::SqlCompileError> {
    check_source_definitions(owner, control).map_err(SqlSourceSupportError::into_compile_error)
}
fn check_source_definitions(
    owner: &SqlAuthoredPhysicalPlan,
    control: &dyn PureCompileControl,
) -> Result<(), SqlSourceSupportError> {
    let functions = owner.function_catalog().snapshot_for_scalar_presence();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    for fragment in owner.plan().fragments().values() {
        visit_fragment_definitions_observed(fragment, &mut work, |source, work| {
            match source {
                FragmentDefinitionSource::Value { .. } => Ok(()),
                FragmentDefinitionSource::Expression { expression, .. } => {
                    let lifecycle = match &expression.kind {
                        ExprKind::FunctionCall { .. } => PureCallLifecycle::Scalar,
                        ExprKind::WindowCall { function, .. } => {
                            if function.kind == novarocks_functions::FunctionKind::Aggregate {
                                PureCallLifecycle::AggregateWindow
                            } else {
                                PureCallLifecycle::Window
                            }
                        }
                        _ => return Ok(()),
                    };
                    let loan = owner
                        .borrow_call_dependency_observed(
                            SqlCallDependencySite::Expression {
                                fragment,
                                source: expression,
                            },
                            work,
                        )
                        .map_err(SqlSourceSupportError::Journal)?;
                    admit_loan(functions.as_ref(), loan, lifecycle, control)
                }
                FragmentDefinitionSource::Request {
                    definition: PhysicalCallDefinition::Expression(_),
                    ..
                } => {
                    // The expression callback admitted this definition once;
                    // builder validation preserves its exact request association.
                    Ok(())
                }
                FragmentDefinitionSource::Request {
                    definition: PhysicalCallDefinition::Relational(site),
                    ..
                } => {
                    let node_id = match site {
                        PhysicalCallSite::Table { node }
                        | PhysicalCallSite::Aggregate { node, .. }
                        | PhysicalCallSite::TopNState { node, .. }
                        | PhysicalCallSite::WriterPartial { node, .. }
                        | PhysicalCallSite::WriterFinal { node, .. } => node,
                        PhysicalCallSite::Expression(_) => {
                            return Err(SqlSourceSupportError::InvalidSource(
                                "relational request names an expression use",
                            ));
                        }
                    };
                    let node = fragment.nodes().get(&node_id).ok_or(
                        SqlSourceSupportError::InvalidSource(
                            "call definition names an absent original node",
                        ),
                    )?;
                    let source = match site {
                        PhysicalCallSite::Table { .. } => SqlCallDependencySite::Table {
                            fragment,
                            source: node,
                        },
                        PhysicalCallSite::Aggregate { .. } | PhysicalCallSite::TopNState { .. } => {
                            let source = aggregate_source(node, site)
                                .map_err(SqlSourceSupportError::Relational)?;
                            SqlCallDependencySite::Aggregate {
                                fragment,
                                node,
                                site,
                                source,
                            }
                        }
                        PhysicalCallSite::WriterPartial { .. }
                        | PhysicalCallSite::WriterFinal { .. } => {
                            let source = writer_source(node, site)
                                .map_err(SqlSourceSupportError::Relational)?;
                            SqlCallDependencySite::WriterAggregate {
                                fragment,
                                node,
                                site,
                                source,
                            }
                        }
                        PhysicalCallSite::Expression(_) => unreachable!(),
                    };
                    let lifecycle = if matches!(site, PhysicalCallSite::Table { .. }) {
                        PureCallLifecycle::Table
                    } else {
                        PureCallLifecycle::Aggregate
                    };
                    let loan = owner
                        .borrow_call_dependency_observed(source, work)
                        .map_err(SqlSourceSupportError::Journal)?;
                    admit_loan(functions.as_ref(), loan, lifecycle, control)
                }
            }
        })?;
    }
    work.finish()?;
    Ok(())
}
