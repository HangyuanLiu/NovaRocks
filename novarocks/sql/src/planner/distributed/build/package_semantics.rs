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

//! Production per-fragment package semantics of one completed SQL owner:
//! expression root uses, the complete frozen call table, and the pruning
//! table. Each call's source scope comes from the owner's original SQL
//! journal entry; no policy is guessed from a first call or a package default.
//!
//! Scope facts and their sources:
//! - decimal overflow policy: the call's captured SQL binding;
//! - constant policy: the statement policy supplied by the caller, which
//!   every captured call must equal exactly;
//! - environment: SQL authors no lexical environment for ordinary calls yet
//!   (the statement snapshot author is pending), so it is empty. An owner whose
//!   call needs an environment refuses during refinement; nothing is invented;
//! - proof scope: the call's actual evaluation domain, never Unconditional.
//!
//! Pruning: SQL authors no pruning witness, so each fragment publishes the
//! empty table, which grants no pruning authority.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan::{
    ExprKind, Fragment, FragmentId, FrozenFragmentCalls, FrozenFragmentPruning, FrozenPruningError,
    PhysicalCallSite, PhysicalRootUses,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase, ExpressionUseId,
    PureCompileControl,
};

use super::expression_occurrences::{
    ExpressionOccurrenceError, author_physical_occurrences_observed,
};
use super::lowered_draft::{SqlAuthoredPhysicalPlan, SqlSourceJournalError};
use super::physical_expression_effects::PhysicalCallSourceScope;
use super::physical_fragment_effects::{
    PhysicalFragmentEffectsError, PhysicalFragmentEffectsInput, PhysicalRelationalCallSourceScope,
    aggregate_source, author_sql_fragment_effects_observed, writer_source,
};

/// One fragment's package semantics, ready for package extraction.
#[derive(Debug)]
pub struct FragmentPackageSemantics {
    pub expression_uses: PhysicalRootUses,
    pub calls: FrozenFragmentCalls,
    pub pruning: FrozenFragmentPruning,
}

#[derive(Debug)]
pub enum PackageSemanticsError {
    Control(CompileControlError),
    Occurrence(String),
    Journal(String),
    Effects(String),
    Pruning(String),
    /// A captured call's constant policy differs from the statement policy.
    PolicyMismatch(PhysicalCallSite),
    InvalidSource(&'static str),
}
impl fmt::Display for PackageSemanticsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Occurrence(detail) => write!(f, "package expression occurrences: {detail}"),
            Self::Journal(detail) => write!(f, "package call source: {detail}"),
            Self::Effects(detail) => write!(f, "package call effects: {detail}"),
            Self::Pruning(detail) => write!(f, "package pruning: {detail}"),
            Self::PolicyMismatch(site) => write!(
                f,
                "captured constant policy differs from the statement policy at {site:?}"
            ),
            Self::InvalidSource(detail) => f.write_str(detail),
        }
    }
}
impl std::error::Error for PackageSemanticsError {}
impl From<CompileControlError> for PackageSemanticsError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ExpressionOccurrenceError> for PackageSemanticsError {
    fn from(error: ExpressionOccurrenceError) -> Self {
        match error {
            ExpressionOccurrenceError::Control(cause) => Self::Control(cause),
            other => Self::Occurrence(format!("{other:?}")),
        }
    }
}
impl From<SqlSourceJournalError> for PackageSemanticsError {
    fn from(error: SqlSourceJournalError) -> Self {
        match error {
            SqlSourceJournalError::Control(cause) => Self::Control(cause),
            other => Self::Journal(format!("{other:?}")),
        }
    }
}
impl From<PhysicalFragmentEffectsError> for PackageSemanticsError {
    fn from(error: PhysicalFragmentEffectsError) -> Self {
        match error {
            PhysicalFragmentEffectsError::Control(cause) => Self::Control(cause),
            other => Self::Effects(format!("{other:?}")),
        }
    }
}
impl From<FrozenPruningError> for PackageSemanticsError {
    fn from(error: FrozenPruningError) -> Self {
        match error {
            FrozenPruningError::Control(cause) => Self::Control(cause),
            other => Self::Pruning(format!("{other:?}")),
        }
    }
}

/// Author every fragment's package semantics from the owner's journal.
pub fn author_fragment_package_semantics(
    owner: &SqlAuthoredPhysicalPlan,
    statement_constant_policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, FragmentPackageSemantics>, PackageSemanticsError> {
    let mut output = BTreeMap::new();
    for (id, fragment) in owner.plan().fragments() {
        let semantics = author_one(owner, fragment, statement_constant_policy, control)?;
        output.insert(*id, semantics);
    }
    Ok(output)
}

fn author_one(
    owner: &SqlAuthoredPhysicalPlan,
    fragment: &Fragment,
    policy: ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<FragmentPackageSemantics, PackageSemanticsError> {
    let occurrences =
        author_physical_occurrences_observed(fragment, owner.function_catalog().as_ref(), control)?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let environments = (|| {
        let mut aggregate_environments = BTreeMap::new();
        for &(site, _) in &occurrences.relational_contexts {
            if let PhysicalCallSite::Aggregate { node, .. }
            | PhysicalCallSite::TopNState { node, .. } = site
            {
                let node =
                    fragment
                        .nodes()
                        .get(&node)
                        .ok_or(PackageSemanticsError::InvalidSource(
                            "aggregate node is absent",
                        ))?;
                let source = aggregate_source(node, site)?;
                let entry = owner
                    .checked_aggregate_source_observed(fragment, node, site, source, &mut work)?;
                if let Some(facts) = entry.captured().binding().group_concat_source() {
                    let refs = facts.environment();
                    for reference in refs {
                        owner.plan().parameters().require(reference).map_err(|_| {
                            PackageSemanticsError::InvalidSource(
                                "group_concat admitted parameter is absent",
                            )
                        })?;
                        work.step()?;
                    }
                    aggregate_environments.insert(site, refs);
                }
            }
        }
        Ok(aggregate_environments)
    })();
    let aggregate_environments = match environments {
        Ok(environments) => environments,
        Err(PackageSemanticsError::Control(cause)) => return Err(cause.into()),
        Err(error) => {
            work.finish()?;
            return Err(error);
        }
    };
    let scopes = (|| {
        let mut expressions: BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'_>> =
            BTreeMap::new();
        for (&id, invocation) in occurrences.root_uses.flow().uses() {
            work.step()?;
            let source = fragment.expressions().get(invocation.definition).ok_or(
                PackageSemanticsError::InvalidSource(
                    "root use names an absent expression definition",
                ),
            )?;
            if !matches!(
                source.kind,
                ExprKind::FunctionCall { .. } | ExprKind::WindowCall { .. }
            ) {
                continue;
            }
            let entry =
                owner.checked_expression_call_source_observed(fragment, source, &mut work)?;
            let captured = entry.captured();
            if captured.constant_policy() != policy {
                return Err(PackageSemanticsError::PolicyMismatch(
                    PhysicalCallSite::Expression(id),
                ));
            }
            expressions.insert(
                id,
                PhysicalCallSourceScope {
                    source,
                    decimal_overflow_policy: captured.binding().decimal_overflow_policy(),
                    environment: &[],
                    proof_scope: CallProofScope::Domain(invocation.context.domain),
                },
            );
        }
        let mut relations: BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'_>> =
            BTreeMap::new();
        for &(site, context) in &occurrences.relational_contexts {
            work.step()?;
            let node_id = match site {
                PhysicalCallSite::Table { node }
                | PhysicalCallSite::Aggregate { node, .. }
                | PhysicalCallSite::TopNState { node, .. }
                | PhysicalCallSite::WriterPartial { node, .. }
                | PhysicalCallSite::WriterFinal { node, .. } => node,
                PhysicalCallSite::Expression(_) => {
                    return Err(PackageSemanticsError::InvalidSource(
                        "relational context names an expression call site",
                    ));
                }
            };
            let node =
                fragment
                    .nodes()
                    .get(&node_id)
                    .ok_or(PackageSemanticsError::InvalidSource(
                        "relational call site names an absent node",
                    ))?;
            // (decimal overflow policy, captured constant policy)
            let (decimal, constant) = match site {
                PhysicalCallSite::Table { .. } => {
                    let entry = owner.checked_table_source_observed(fragment, node, &mut work)?;
                    let captured = entry.captured();
                    (
                        captured.binding().decimal_overflow_policy(),
                        captured.constant_policy(),
                    )
                }
                PhysicalCallSite::Aggregate { .. } | PhysicalCallSite::TopNState { .. } => {
                    let source = aggregate_source(node, site)?;
                    let entry = owner.checked_aggregate_source_observed(
                        fragment, node, site, source, &mut work,
                    )?;
                    let captured = entry.captured();
                    (
                        captured.binding().decimal_overflow_policy(),
                        captured.constant_policy(),
                    )
                }
                PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. } => {
                    let source = writer_source(node, site)?;
                    let entry = owner.checked_writer_aggregate_source_observed(
                        fragment, node, site, source, &mut work,
                    )?;
                    let captured = entry.captured();
                    (
                        captured.binding().decimal_overflow_policy(),
                        captured.constant_policy(),
                    )
                }
                PhysicalCallSite::Expression(_) => unreachable!("refused above"),
            };
            if constant != policy {
                return Err(PackageSemanticsError::PolicyMismatch(site));
            }
            relations.insert(
                site,
                PhysicalRelationalCallSourceScope {
                    source: node,
                    decimal_overflow_policy: decimal,
                    environment: aggregate_environments
                        .get(&site)
                        .map_or(&[][..], |refs| refs.as_slice()),
                    proof_scope: CallProofScope::Domain(context.domain),
                },
            );
        }
        Ok((expressions, relations))
    })();
    let (expressions, relations) = match scopes {
        Ok(scopes) => scopes,
        Err(PackageSemanticsError::Control(cause)) => return Err(cause.into()),
        Err(error) => {
            work.finish()?;
            return Err(error);
        }
    };
    work.finish()?;
    let effects = author_sql_fragment_effects_observed(
        owner,
        PhysicalFragmentEffectsInput {
            fragment,
            occurrences: &occurrences,
            constants: owner.plan().constants(),
            parameters: owner.plan().parameters(),
            literal_policy: policy,
            expression_scopes: &expressions,
            relational_scopes: &relations,
        },
        control,
    )?;
    let calls = effects.calls;
    drop(expressions);
    drop(relations);
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], control)?;
    Ok(FragmentPackageSemantics {
        expression_uses: occurrences.root_uses,
        calls,
        pruning,
    })
}
