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
    ExpressionOccurrenceError, author_physical_occurrences_from_journal_observed,
};
use super::lowered_draft::{SqlAuthoredPhysicalPlan, SqlSourceJournalError};
use super::physical_expression_effects::{PhysicalCallSourceScope, PhysicalExpressionEffectsError};
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
    /// The original constant admission owner, without reclassifying its limit.
    ConstantSource(novarocks_functions::ConstantError),
    /// A comparison author retained with its original fact and diagnostic.
    Source(novarocks_failure_contract::SharedFailureCause),
    Occurrence(String),
    Journal(String),
    Effects(String),
    /// An exact prepared-cast capability refusal, before package extraction.
    UnsupportedCapability(String),
    Pruning(String),
    /// A captured call's constant policy differs from the statement policy.
    PolicyMismatch(PhysicalCallSite),
    InvalidSource(&'static str),
}
impl fmt::Display for PackageSemanticsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::ConstantSource(cause) => write!(f, "constant admission limit: {cause}"),
            Self::Source(cause) => cause.fmt(f),
            Self::Occurrence(detail) => write!(f, "package expression occurrences: {detail}"),
            Self::Journal(detail) => write!(f, "package call source: {detail}"),
            Self::Effects(detail) => write!(f, "package call effects: {detail}"),
            Self::UnsupportedCapability(detail) => f.write_str(detail),
            Self::Pruning(detail) => write!(f, "package pruning: {detail}"),
            Self::PolicyMismatch(site) => write!(
                f,
                "captured constant policy differs from the statement policy at {site:?}"
            ),
            Self::InvalidSource(detail) => f.write_str(detail),
        }
    }
}
impl std::error::Error for PackageSemanticsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::ConstantSource(cause) => Some(cause),
            Self::Source(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for PackageSemanticsError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        let (class, code) = match self {
            Self::Control(cause) => return cause.failure_fact(),
            Self::ConstantSource(cause) => return cause.failure_fact(),
            Self::Source(cause) => return cause.failure_fact(),
            Self::Occurrence(_) => (FailureClass::Invariant, 7),
            Self::Journal(_) => (FailureClass::Invariant, 8),
            Self::Effects(_) => (FailureClass::Invariant, 9),
            Self::UnsupportedCapability(_) => (FailureClass::Unsupported, 10),
            Self::Pruning(_) => (FailureClass::Invariant, 11),
            Self::PolicyMismatch(_) => (FailureClass::Invariant, 12),
            Self::InvalidSource(_) => (FailureClass::Invariant, 13),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::SQL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
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
/// Only the typed constant owner can identify structural constant limits.
/// Similar diagnostic text from invalid sources or real control never qualifies.
fn constant_limit_from_effects(
    error: PhysicalFragmentEffectsError,
) -> Result<novarocks_functions::ConstantError, PhysicalFragmentEffectsError> {
    use super::physical_aggregate_requests::PhysicalAggregateRequestError as A;
    use super::physical_expression_effects::PhysicalExpressionEffectsError as E;
    use super::physical_scalar_requests::PhysicalScalarRequestError as S;
    use super::physical_table_requests::PhysicalTableRequestError as T;
    use super::physical_window_requests::PhysicalWindowRequestError as W;
    use super::physical_writer_requests::PhysicalWriterRequestError as R;
    match error {
        PhysicalFragmentEffectsError::Expressions(E::Argument(argument)) => argument
            .into_constant_limit()
            .map_err(|argument| PhysicalFragmentEffectsError::Expressions(E::Argument(argument))),
        PhysicalFragmentEffectsError::Expressions(E::Request(S::Argument(argument))) => {
            argument.into_constant_limit().map_err(|argument| {
                PhysicalFragmentEffectsError::Expressions(E::Request(S::Argument(argument)))
            })
        }
        PhysicalFragmentEffectsError::Expressions(E::WindowRequest(W::Argument(argument))) => {
            argument.into_constant_limit().map_err(|argument| {
                PhysicalFragmentEffectsError::Expressions(E::WindowRequest(W::Argument(argument)))
            })
        }
        PhysicalFragmentEffectsError::AggregateRequest(A::Argument(argument)) => {
            argument.into_constant_limit().map_err(|argument| {
                PhysicalFragmentEffectsError::AggregateRequest(A::Argument(argument))
            })
        }
        PhysicalFragmentEffectsError::TableRequest(T::Argument(argument)) => argument
            .into_constant_limit()
            .map_err(|argument| PhysicalFragmentEffectsError::TableRequest(T::Argument(argument))),
        PhysicalFragmentEffectsError::WriterRequest(R::Aggregate(A::Argument(argument))) => {
            argument.into_constant_limit().map_err(|argument| {
                PhysicalFragmentEffectsError::WriterRequest(R::Aggregate(A::Argument(argument)))
            })
        }
        other => Err(other),
    }
}

impl From<PhysicalFragmentEffectsError> for PackageSemanticsError {
    fn from(error: PhysicalFragmentEffectsError) -> Self {
        let error = match constant_limit_from_effects(error) {
            Ok(cause) => return Self::ConstantSource(cause),
            Err(error) => error,
        };
        match error {
            PhysicalFragmentEffectsError::Control(cause) => Self::Control(cause),
            PhysicalFragmentEffectsError::Expressions(
                PhysicalExpressionEffectsError::Comparison(cause),
            ) => match cause.control_error() {
                Some(control) => Self::Control(control),
                None => Self::Source(novarocks_failure_contract::SharedFailureCause::new(cause)),
            },
            error @ PhysicalFragmentEffectsError::Expressions(
                PhysicalExpressionEffectsError::UnsupportedCast { .. },
            ) => Self::UnsupportedCapability(format!("package call effects: {error:?}")),
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
    let occurrences = author_physical_occurrences_from_journal_observed(owner, fragment, control)?;
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

#[cfg(test)]
mod capability_projection_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_functions::CastPrepareError;
    use std::sync::Arc;

    #[test]
    fn package_semantics_keeps_exact_cast_refusals_separate_from_invalid_programs() {
        let from = DataType::List(Arc::new(Field::new("item", DataType::Int8, true)));
        let to = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let error = PackageSemanticsError::from(PhysicalFragmentEffectsError::Expressions(
            PhysicalExpressionEffectsError::UnsupportedCast { from, to },
        ));
        let PackageSemanticsError::UnsupportedCapability(detail) = error else {
            panic!("exact cast capability identity was lost");
        };
        assert!(detail.contains("Int8") && detail.contains("Int64"));
        for error in [
            PhysicalExpressionEffectsError::Cast(CastPrepareError::TypeMismatch),
            PhysicalExpressionEffectsError::InvalidSource("UnsupportedCast diagnostic"),
        ] {
            assert!(matches!(
                PackageSemanticsError::from(PhysicalFragmentEffectsError::Expressions(error)),
                PackageSemanticsError::Effects(_)
            ));
        }
    }

    #[test]
    fn package_semantics_capability_projection_preserves_original_control_causes() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(matches!(
                PackageSemanticsError::from(PhysicalFragmentEffectsError::Control(cause.clone())),
                PackageSemanticsError::Control(actual) if actual == cause
            ));
        }
    }
}

#[cfg(test)]
mod constant_limit_projection_tests {
    use super::super::physical_aggregate_requests::PhysicalAggregateRequestError as A;
    use super::super::physical_call_arguments::PhysicalArgumentError;
    use super::super::physical_scalar_requests::PhysicalScalarRequestError as S;
    use super::super::physical_table_requests::PhysicalTableRequestError as T;
    use super::super::physical_window_requests::PhysicalWindowRequestError as W;
    use super::super::physical_writer_requests::PhysicalWriterRequestError as R;
    use super::*;
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };
    use novarocks_functions::ConstantError;
    use std::{error::Error, num::NonZeroU16};

    fn argument(reference: bool) -> PhysicalArgumentError {
        let original = ConstantError::Limit("rows exceed policy");
        if reference {
            PhysicalArgumentError::from(novarocks_physical_plan::ConstantReferenceError::Constant(
                original,
            ))
        } else {
            PhysicalArgumentError::from(original)
        }
    }

    #[test]
    fn typed_limits_survive_all_physical_call_argument_wrappers() {
        use PhysicalExpressionEffectsError as E;
        let expected = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::CONSTANT, NonZeroU16::new(2).unwrap()),
        );
        for reference in [false, true] {
            let errors = [
                PhysicalFragmentEffectsError::Expressions(E::Argument(argument(reference))),
                PhysicalFragmentEffectsError::Expressions(E::Request(S::Argument(argument(
                    reference,
                )))),
                PhysicalFragmentEffectsError::Expressions(E::WindowRequest(W::Argument(argument(
                    reference,
                )))),
                PhysicalFragmentEffectsError::AggregateRequest(A::Argument(argument(reference))),
                PhysicalFragmentEffectsError::TableRequest(T::Argument(argument(reference))),
                PhysicalFragmentEffectsError::WriterRequest(R::Aggregate(A::Argument(argument(
                    reference,
                )))),
            ];
            for error in errors {
                let error = PackageSemanticsError::from(error);
                let original = ConstantError::Limit("rows exceed policy");
                assert!(
                    matches!(&error, PackageSemanticsError::ConstantSource(cause) if cause == &original)
                );
                assert_eq!(
                    error.source().unwrap().downcast_ref::<ConstantError>(),
                    Some(&original)
                );
                assert_eq!(error.failure_fact(), expected);
            }
        }
        for argument in [
            PhysicalArgumentError::from(ConstantError::Invalid("rows exceed policy")),
            PhysicalArgumentError::from(novarocks_physical_plan::ConstantReferenceError::Constant(
                ConstantError::Invalid("rows exceed policy"),
            )),
            PhysicalArgumentError::from(ConstantError::Type(
                novarocks_type_contract::ValueTypeError::TooDeep,
            )),
        ] {
            let error = PhysicalFragmentEffectsError::Expressions(E::Argument(argument));
            assert!(matches!(
                PackageSemanticsError::from(error),
                PackageSemanticsError::Effects(_)
            ));
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error =
                PackageSemanticsError::from(PhysicalFragmentEffectsError::Control(cause.clone()));
            assert!(matches!(&error, PackageSemanticsError::Control(actual) if actual == &cause));
            assert_eq!(error.failure_fact(), cause.failure_fact());
        }
    }
}

#[cfg(test)]
mod comparison_source_projection_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    use novarocks_functions::{ComparisonPrepareError, PreparedComparisonRecipe};
    use novarocks_type_contract::{ComparisonOperator, FunctionValueType};
    use std::{error::Error, sync::Arc};

    fn domain(data_type: DataType) -> FunctionValueType {
        FunctionValueType::from_legacy_declared_projection(data_type, true)
    }

    fn package(original: ComparisonPrepareError) -> PackageSemanticsError {
        PackageSemanticsError::from(PhysicalFragmentEffectsError::from(
            PhysicalExpressionEffectsError::from(original),
        ))
    }

    #[test]
    fn actual_comparison_recipe_refusal_and_mismatch_keep_distinct_original_sources() {
        let nested = domain(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        let left = domain(DataType::Int32);
        let right = domain(DataType::Int64);
        for (left, right, expected_original, expected_class) in [
            (
                &nested,
                &nested,
                ComparisonPrepareError::Unsupported,
                FailureClass::Unsupported,
            ),
            (
                &left,
                &right,
                ComparisonPrepareError::TypeMismatch,
                FailureClass::Invariant,
            ),
        ] {
            let original = PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                left,
                right,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap_err();
            assert_eq!(original, expected_original);
            let expected = original.failure_fact();
            assert_eq!(expected.class(), expected_class);
            let error = package(original.clone());
            assert_eq!(error.failure_fact(), expected);
            let PackageSemanticsError::Source(source) = &error else {
                panic!("comparison source was replaced by a SQL diagnostic");
            };
            assert_eq!(
                source.downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            assert_eq!(source.detail(), original.to_string());
            assert_eq!(error.to_string(), original.to_string());
            let retained = error
                .source()
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap();
            assert_eq!(retained, source);
            assert_eq!(
                retained
                    .source()
                    .unwrap()
                    .downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
        }
        let invalid = PackageSemanticsError::from(PhysicalFragmentEffectsError::Expressions(
            PhysicalExpressionEffectsError::InvalidSource("comparison unsupported"),
        ));
        assert!(matches!(invalid, PackageSemanticsError::Effects(_)));
        assert_eq!(invalid.failure_fact().class(), FailureClass::Invariant);
    }

    struct Refuse(CompileControlError);
    impl PureCompileControl for Refuse {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }

    #[test]
    fn actual_comparison_control_refusal_keeps_all_original_control_causes() {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        let source = SharedFailureCause::new(LayoutResourceError::SourceModel);
        let value = domain(DataType::Int32);
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(source.clone()),
        ] {
            let original = PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &value,
                &value,
                &Refuse(control.clone()),
            )
            .unwrap_err();
            assert_eq!(original.control_error(), Some(control.clone()));
            let error = package(original);
            assert_eq!(error.failure_fact(), control.failure_fact());
            assert!(matches!(error, PackageSemanticsError::Control(actual) if actual == control));
            // The package boundary must also normalize a directly nested control.
            let nested = PackageSemanticsError::from(PhysicalFragmentEffectsError::Expressions(
                PhysicalExpressionEffectsError::Comparison(ComparisonPrepareError::Control(
                    control.clone(),
                )),
            ));
            assert!(matches!(nested, PackageSemanticsError::Control(actual) if actual == control));
        }
    }

    #[test]
    fn comparison_kernel_source_keeps_its_real_invalid_author() {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        let kernel =
            novarocks_functions::KernelFailure::from_domain(LayoutResourceError::SourceModel);
        let original = ComparisonPrepareError::Kernel(kernel);
        let expected = original.failure_fact();
        let error = package(original.clone());
        assert_eq!(error.failure_fact(), expected);
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let PackageSemanticsError::Source(source) = error else {
            panic!("the original kernel source was not retained");
        };
        assert_eq!(
            source.downcast_ref::<ComparisonPrepareError>(),
            Some(&original)
        );
        let comparison = source.downcast_ref::<ComparisonPrepareError>().unwrap();
        let retained_kernel = comparison
            .source()
            .unwrap()
            .downcast_ref::<novarocks_functions::KernelFailure>()
            .unwrap();
        assert!(
            matches!(comparison, ComparisonPrepareError::Kernel(expected) if expected == retained_kernel)
        );
        assert_eq!(retained_kernel.failure_fact(), expected);
    }
}
