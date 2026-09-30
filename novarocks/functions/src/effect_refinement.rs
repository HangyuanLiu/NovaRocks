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

//! Exact-owner call refinement. Binding validation and semantic refinement
//! share the implementation owner; this interface never performs name lookup.
use crate::{FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind};
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, EffectContractError, FunctionEffectDeclaration, PureCompileControl,
    SemanticParameterRef, SemanticParameters,
};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_CALL_EFFECT_ARGUMENTS: usize = 4096;

#[derive(Clone, Copy, Debug)]
pub struct CallEffectInput<'a> {
    pub context: novarocks_type_contract::ExpressionEffectContext,
    pub argument_uses: &'a [Option<novarocks_type_contract::ExpressionUseId>],
    pub function_id: &'a FunctionId,
    pub kind: FunctionKind,
    pub selected: &'a FunctionBindingSelection,
    pub request: FunctionBindingRequest<'a>,
    pub environment: &'a [SemanticParameterRef],
    pub parameters: &'a SemanticParameters,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    pub proof_scope: CallProofScope,
}

/// Required operations of the exact installed immutable implementation owner.
/// A refiner validates the already-selected signature, never reselects it, and
/// uses only exact constants/types/policies/frozen environment. It proves the
/// complete actual dependency set and exact lexical refs (not just a subset of
/// possible keys); a Stable call may not leave a live environment dependency.
/// Its own loops
/// obey control/work limits. Invalid constant data is not executed here: later
/// specialization may retain a delayed row-error recipe.
pub trait FunctionEffectOwner: Send + Sync {
    type Error: Error;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error>;
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>>;
}

/// Implementation control failures remain typed at the neutral boundary.
#[derive(Debug)]
pub enum FunctionEffectOwnerError<E: Error> {
    Control(CompileControlError),
    Owner(E),
}
impl<E: Error> From<E> for FunctionEffectOwnerError<E> {
    fn from(error: E) -> Self {
        Self::Owner(error)
    }
}

#[derive(Debug)]
pub enum CallEffectRefinementError<E: Error> {
    Owner(E),
    Control(CompileControlError),
    Contract(EffectContractError),
    InvalidInput(&'static str),
}
impl<E: Error> fmt::Display for CallEffectRefinementError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
            Self::Contract(e) => fmt::Display::fmt(e, f),
            Self::InvalidInput(e) => f.write_str(e),
        }
    }
}
impl<E: Error> Error for CallEffectRefinementError<E> {}

/// Recompute local call effects from the exact implementation. A BE compares
/// this result with frozen call facts, rather than trusting a FE never-fails bit.
pub fn refine_call_effects<'a, O: FunctionEffectOwner + ?Sized>(
    owner: &O,
    input: CallEffectInput<'a>,
    control: &dyn PureCompileControl,
) -> Result<RefinedCallEffects<'a>, CallEffectRefinementError<O::Error>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(CallEffectRefinementError::Control)?;
    if input.environment.len() > novarocks_type_contract::MAX_SEMANTIC_PARAMETERS {
        return Err(CallEffectRefinementError::Control(
            CompileControlError::ResourceExhausted,
        ));
    }
    if input.request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
        || input.selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CallEffectRefinementError::Control(
            CompileControlError::ResourceExhausted,
        ));
    }
    if input.argument_uses.len() != input.request.arguments.len()
        || input.request.arguments.len() != input.selected.argument_types.len()
        || input.request.logical_argument_count > input.request.arguments.len()
        || (input.kind != FunctionKind::Aggregate
            && input.request.logical_argument_count != input.request.arguments.len())
    {
        return Err(CallEffectRefinementError::InvalidInput(
            "call effect input differs from selected argument shape",
        ));
    }
    if input.proof_scope != CallProofScope::Unconditional
        && input.proof_scope != CallProofScope::Domain(input.context.domain)
    {
        return Err(CallEffectRefinementError::Contract(
            EffectContractError::ProofScopeMismatch,
        ));
    }
    let declaration = owner
        .declaration(input.function_id, input.selected)
        .map_err(CallEffectRefinementError::Owner)?;
    declaration
        .validate(input.kind)
        .map_err(CallEffectRefinementError::Contract)?;
    for use_id in input.argument_uses {
        if use_id.is_none()
            != (declaration.argument_control == novarocks_type_contract::ArgumentControl::TypeOnly)
        {
            return Err(CallEffectRefinementError::InvalidInput(
                "call argument demand differs from its exact owner control",
            ));
        }
        work.step().map_err(CallEffectRefinementError::Control)?;
    }
    work.step().map_err(CallEffectRefinementError::Control)?;
    let mut refs = BTreeSet::new();
    // One active lexical setting per key in one call. Different call scopes may
    // still use different refs of the same key in the package's sparse table.
    let mut keys = BTreeSet::new();
    for reference in input.environment {
        if !declaration
            .environment_dependencies
            .contains(&reference.expected_key)
            || !refs.insert(*reference)
            || !keys.insert(reference.expected_key)
            || input.parameters.require(*reference).is_err()
        {
            return Err(CallEffectRefinementError::InvalidInput(
                "call effect environment is not an exact frozen declared dependency",
            ));
        }
        work.step().map_err(CallEffectRefinementError::Control)?;
    }
    // Flush wrapper work before entering the implementation owner's scope.
    work.finish().map_err(CallEffectRefinementError::Control)?;
    let effects = owner
        .validate_and_refine(input, control)
        .map_err(|error| match error {
            FunctionEffectOwnerError::Control(error) => CallEffectRefinementError::Control(error),
            FunctionEffectOwnerError::Owner(error) => CallEffectRefinementError::Owner(error),
        })?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(CallEffectRefinementError::Control)?;
    effects
        .validate_refinement(declaration, input.proof_scope)
        .map_err(CallEffectRefinementError::Contract)?;
    for reference in &effects.environment {
        if !refs.contains(reference) {
            return Err(CallEffectRefinementError::InvalidInput(
                "refiner introduced an unfrozen semantic dependency",
            ));
        }
        work.step().map_err(CallEffectRefinementError::Control)?;
    }
    work.finish().map_err(CallEffectRefinementError::Control)?;
    Ok(RefinedCallEffects { input, effects })
}

/// Only exact-owner recomputation constructs this token. In particular, Stable
/// eligibility requires the owner to prove a complete frozen environment; the
/// structural possible-key subset check alone cannot construct the token.
#[derive(Clone, Debug)]
pub struct RefinedCallEffects<'a> {
    input: CallEffectInput<'a>,
    effects: CallEffects,
}
impl RefinedCallEffects<'_> {
    pub const fn facts(&self) -> &CallEffects {
        &self.effects
    }
    pub fn compose_for_use(
        &self,
        input: CallEffectInput<'_>,
        arguments: ScopedExpressionEffects,
    ) -> Result<ScopedExpressionEffects, EffectContractError> {
        if input.context != arguments.context() {
            return Err(EffectContractError::CallIdentityMismatch);
        }
        self.validate_input(input)?;
        arguments.with_verified_call(&self.effects)
    }
    /// Validate the exact immutable input borrow before owning specialization
    /// facts. This does not compose or erase any child expression effects.
    pub fn validate_input(&self, input: CallEffectInput<'_>) -> Result<(), EffectContractError> {
        let original = self.input;
        // The receipt borrows one immutable compilation input. Requiring those
        // exact borrows prevents attaching a proof to a different call without
        // repeatedly comparing/copying recursive schemas and constant backing.
        // These local addresses are never a wire, plan or profile identity.
        if original.context != input.context
            || !std::ptr::eq(original.argument_uses, input.argument_uses)
            || !std::ptr::eq(original.function_id, input.function_id)
            || original.kind != input.kind
            || !std::ptr::eq(original.selected, input.selected)
            || !std::ptr::eq(original.request.arguments, input.request.arguments)
            || original.request.logical_argument_count != input.request.logical_argument_count
            || !std::ptr::eq(original.environment, input.environment)
            || !std::ptr::eq(original.parameters, input.parameters)
            || original.decimal_overflow_policy != input.decimal_overflow_policy
            || original.proof_scope != input.proof_scope
        {
            return Err(EffectContractError::CallIdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopedExpressionEffects {
    context: novarocks_type_contract::ExpressionEffectContext,
    effects: novarocks_type_contract::ExpressionEffects,
}
impl ScopedExpressionEffects {
    pub const fn pure_value(context: novarocks_type_contract::ExpressionEffectContext) -> Self {
        Self {
            context,
            effects: novarocks_type_contract::ExpressionEffects::PURE_VALUE,
        }
    }
    /// The compiler owns non-function primitive effects (cast/comparison/etc.).
    pub const fn primitive(
        context: novarocks_type_contract::ExpressionEffectContext,
        effects: novarocks_type_contract::ExpressionEffects,
    ) -> Self {
        Self { context, effects }
    }
    pub const fn context(self) -> novarocks_type_contract::ExpressionEffectContext {
        self.context
    }
    pub fn for_use(
        self,
        context: novarocks_type_contract::ExpressionEffectContext,
    ) -> Result<novarocks_type_contract::ExpressionEffects, EffectContractError> {
        if self.context != context {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(self.effects)
    }
    fn with_verified_call(self, call: &CallEffects) -> Result<Self, EffectContractError> {
        if !matches!(call.proof_scope, CallProofScope::Unconditional)
            && call.proof_scope != CallProofScope::Domain(self.context.domain)
        {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(Self {
            effects: self
                .effects
                .join(novarocks_type_contract::ExpressionEffects {
                    value_stability: call.value_stability,
                    may_raise_row_error: call.own_row_error
                        == novarocks_type_contract::FunctionIntrinsicRowError::MayRaise,
                    has_instance_state: call.instance_state
                        != novarocks_type_contract::FunctionInstanceState::None,
                    observable_effects: call.observable_effects,
                }),
            ..self
        })
    }
    /// Eager children must be in this same evaluation domain. Strong guarded
    /// children require the compiler's separate checked control-domain join.
    pub fn join_same_domain(self, child: Self) -> Result<Self, EffectContractError> {
        if self.context.domain != child.context.domain {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        Ok(Self {
            effects: self.effects.join(child.effects),
            ..self
        })
    }
}

pub fn validate_frozen_call_effects<O: FunctionEffectOwner + ?Sized>(
    owner: &O,
    input: CallEffectInput<'_>,
    frozen: &CallEffects,
    control: &dyn PureCompileControl,
) -> Result<(), CallEffectRefinementError<O::Error>> {
    if refine_call_effects(owner, input, control)?.facts() != frozen {
        return Err(CallEffectRefinementError::InvalidInput(
            "frozen call effects differ from exact local refinement",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionOverloadId,
        FunctionResultType, FunctionValueType,
    };
    use novarocks_type_contract::{
        ArgumentControl, FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
        FunctionNullBehavior, FunctionVolatility, ObservableEffects, SemanticParameterId,
        SemanticParameterKey, SemanticParameterValue,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Owner {
        declaration: FunctionEffectDeclaration,
        calls: AtomicUsize,
        control_failure: Option<CompileControlError>,
    }
    impl Owner {
        fn new() -> Self {
            Self {
                declaration: FunctionEffectDeclaration {
                    value_stability: FunctionVolatility::Stable,
                    own_row_error: FunctionIntrinsicRowError::MayRaise,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    null_behavior: FunctionNullBehavior::CalledOnNull,
                    argument_control: ArgumentControl::Eager,
                    instance_state: FunctionInstanceState::None,
                    observable_effects: ObservableEffects::NONE,
                    environment_dependencies: vec![SemanticParameterKey::AllowThrowException]
                        .into_boxed_slice(),
                },
                calls: AtomicUsize::new(0),
                control_failure: None,
            }
        }
    }
    impl FunctionEffectOwner for Owner {
        type Error = FunctionBindingError;
        fn declaration(
            &self,
            function: &FunctionId,
            selected: &FunctionBindingSelection,
        ) -> Result<&FunctionEffectDeclaration, Self::Error> {
            if function.as_str() != "fixture/effect"
                || selected.overload.as_str() != "fixture/effect/i64"
            {
                return Err(FunctionBindingError::UnknownFunction);
            }
            Ok(&self.declaration)
        }
        fn validate_and_refine(
            &self,
            input: CallEffectInput<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
            if let Some(error) = self.control_failure {
                return Err(FunctionEffectOwnerError::Control(error));
            }
            self.calls.fetch_add(1, Ordering::Relaxed);
            let ty = FunctionValueType::new(arrow_schema::DataType::Int64, true);
            if input.selected.argument_types.as_ref() != [FunctionArgumentType::Value(ty.clone())]
                || input.selected.result_type != FunctionResultType::Scalar(ty)
                || input.environment.len() != 1
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact call differs".into(),
                )
                .into());
            }
            let reference = input.environment[0];
            let Some(SemanticParameterValue::AllowThrowException(strict)) =
                input.parameters.get(reference.id)
            else {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture requires exact strict policy".into(),
                )
                .into());
            };
            Ok(CallEffects {
                value_stability: self.declaration.value_stability,
                own_row_error: if *strict {
                    FunctionIntrinsicRowError::MayRaise
                } else {
                    FunctionIntrinsicRowError::NoRowError
                },
                failure_behavior: self.declaration.failure_behavior,
                null_behavior: self.declaration.null_behavior,
                argument_control: self.declaration.argument_control,
                instance_state: self.declaration.instance_state,
                observable_effects: self.declaration.observable_effects,
                environment: input.environment.into(),
                proof_scope: input.proof_scope,
            })
        }
    }
    struct Control(bool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.0 {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    fn selected() -> FunctionBindingSelection {
        let ty = FunctionValueType::new(arrow_schema::DataType::Int64, true);
        FunctionBindingSelection {
            overload: FunctionOverloadId::try_new("fixture/effect/i64").unwrap(),
            argument_types: vec![FunctionArgumentType::Value(ty.clone())].into_boxed_slice(),
            result_type: FunctionResultType::Scalar(ty),
            aggregate: None,
        }
    }
    #[test]
    fn exact_refinement_recomputes_policy_and_refuses_forged_never_fails() {
        let owner = Owner::new();
        let id = FunctionId::try_new("fixture/effect").unwrap();
        let selected = selected();
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::new(arrow_schema::DataType::Int64, true),
            constant: None,
        }];
        let environment = [SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::AllowThrowException,
        }];
        let parameters = SemanticParameters::try_new([(
            environment[0].id,
            SemanticParameterValue::AllowThrowException(true),
        )])
        .unwrap();
        let argument_uses = [Some(novarocks_type_contract::ExpressionUseId::new(1))];
        let context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(0),
            domain: novarocks_type_contract::EvaluationDomainId::new(0),
            demand: novarocks_type_contract::EvaluationDemand::Value,
        };
        let input = CallEffectInput {
            context,
            argument_uses: &argument_uses,
            function_id: &id,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 1,
            },
            environment: &environment,
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        };
        let result = refine_call_effects(&owner, input, &Control(false)).unwrap();
        let arguments = ScopedExpressionEffects::pure_value(context);
        let composed = result.compose_for_use(input, arguments).unwrap();
        assert!(composed.for_use(context).unwrap().may_raise_row_error);
        let other_context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(8),
            ..context
        };
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    context: other_context,
                    ..input
                },
                ScopedExpressionEffects::pure_value(other_context)
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        let other_arguments = [Some(novarocks_type_contract::ExpressionUseId::new(8))];
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    argument_uses: &other_arguments,
                    ..input
                },
                arguments
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        let other_function = FunctionId::try_new("fixture/other").unwrap();
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    function_id: &other_function,
                    ..input
                },
                arguments
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        let other_args = [FunctionArgument::Value {
            value_type: FunctionValueType::new(arrow_schema::DataType::Int64, true),
            constant: Some(crate::FunctionLiteral::Int64(1)),
        }];
        assert_eq!(
            result.compose_for_use(
                CallEffectInput {
                    request: FunctionBindingRequest {
                        arguments: &other_args,
                        logical_argument_count: 1
                    },
                    ..input
                },
                arguments
            ),
            Err(EffectContractError::CallIdentityMismatch)
        );
        assert!(
            composed
                .for_use(novarocks_type_contract::ExpressionEffectContext {
                    demand: novarocks_type_contract::EvaluationDemand::TruthOnly,
                    ..context
                })
                .is_err()
        );
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let mut failing = Owner::new();
            failing.control_failure = Some(error);
            assert!(
                matches!(refine_call_effects(&failing, input, &Control(false)), Err(CallEffectRefinementError::Control(actual)) if actual == error)
            );
        }

        assert_eq!(
            result.facts().own_row_error,
            FunctionIntrinsicRowError::MayRaise
        );
        validate_frozen_call_effects(&owner, input, result.facts(), &Control(false)).unwrap();
        let forged = CallEffects {
            own_row_error: FunctionIntrinsicRowError::NoRowError,
            ..result.facts().clone()
        };
        assert!(validate_frozen_call_effects(&owner, input, &forged, &Control(false)).is_err());
        let relaxed = SemanticParameters::try_new([(
            environment[0].id,
            SemanticParameterValue::AllowThrowException(false),
        )])
        .unwrap();
        let relaxed_input = CallEffectInput {
            parameters: &relaxed,
            ..input
        };
        let no_own_error = refine_call_effects(&owner, relaxed_input, &Control(false)).unwrap();
        let child_context = novarocks_type_contract::ExpressionEffectContext {
            use_id: argument_uses[0].unwrap(),
            ..context
        };
        let child = ScopedExpressionEffects::primitive(
            child_context,
            novarocks_type_contract::ExpressionEffects {
                may_raise_row_error: true,
                ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
            },
        );
        let arguments = ScopedExpressionEffects::pure_value(context)
            .join_same_domain(child)
            .unwrap();
        assert!(
            no_own_error
                .compose_for_use(relaxed_input, arguments)
                .unwrap()
                .for_use(context)
                .unwrap()
                .may_raise_row_error
        );
        assert!(
            arguments
                .join_same_domain(ScopedExpressionEffects::pure_value(
                    novarocks_type_contract::ExpressionEffectContext {
                        domain: novarocks_type_contract::EvaluationDomainId::new(1),
                        ..child_context
                    }
                ))
                .is_err()
        );

        assert_eq!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    parameters: &relaxed,
                    ..input
                },
                &Control(false)
            )
            .unwrap()
            .facts()
            .own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        let count = owner.calls.load(Ordering::Relaxed);
        assert!(matches!(
            refine_call_effects(&owner, input, &Control(true)),
            Err(CallEffectRefinementError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(owner.calls.load(Ordering::Relaxed), count);
        let absent = SemanticParameters::default();
        assert!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    parameters: &absent,
                    ..input
                },
                &Control(false)
            )
            .is_err()
        );
        assert_eq!(owner.calls.load(Ordering::Relaxed), count);
        let wrong_overload = FunctionBindingSelection {
            overload: FunctionOverloadId::try_new("fixture/other").unwrap(),
            ..selected.clone()
        };
        assert!(
            refine_call_effects(
                &owner,
                CallEffectInput {
                    selected: &wrong_overload,
                    ..input
                },
                &Control(false)
            )
            .is_err()
        );
    }
    #[test]
    fn same_key_in_two_lexical_refs_is_not_one_call_environment() {
        let owner = Owner::new();
        let id = FunctionId::try_new("fixture/effect").unwrap();
        let selected = selected();
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::new(arrow_schema::DataType::Int64, true),
            constant: None,
        }];
        let environment = [
            SemanticParameterRef {
                id: SemanticParameterId::new(1),
                expected_key: SemanticParameterKey::AllowThrowException,
            },
            SemanticParameterRef {
                id: SemanticParameterId::new(2),
                expected_key: SemanticParameterKey::AllowThrowException,
            },
        ];
        let parameters = SemanticParameters::try_new([
            (
                environment[0].id,
                SemanticParameterValue::AllowThrowException(true),
            ),
            (
                environment[1].id,
                SemanticParameterValue::AllowThrowException(false),
            ),
        ])
        .unwrap();
        let argument_uses = [Some(novarocks_type_contract::ExpressionUseId::new(1))];
        let context = novarocks_type_contract::ExpressionEffectContext {
            use_id: novarocks_type_contract::ExpressionUseId::new(0),
            domain: novarocks_type_contract::EvaluationDomainId::new(0),
            demand: novarocks_type_contract::EvaluationDemand::Value,
        };
        let input = CallEffectInput {
            context,
            argument_uses: &argument_uses,
            function_id: &id,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 1,
            },
            environment: &environment,
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        };
        assert!(refine_call_effects(&owner, input, &Control(false)).is_err());
        assert_eq!(owner.calls.load(Ordering::Relaxed), 0);
    }
}
