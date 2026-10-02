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

//! Explicit function declarations and exact, carrier-neutral binding.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use sha2::{Digest, Sha256};

use crate::{
    AggregateOverloadDeclaration, AggregateStateFormatIdentity, EngineFunctionCatalog,
    EngineFunctionCatalogBuilder, FunctionArgumentEvaluation, FunctionArgumentType,
    FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior, FunctionId, FunctionKind,
    FunctionOverloadId, FunctionValueType, FunctionVisibility, FunctionVolatility, digest_text,
};

/// Semantics declared by the implementation owner, never inferred from a name.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FunctionSemantics {
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
    pub intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError,
}
impl FunctionSemantics {
    /// Lossy discovery projection only. Exact controls, NULL, state,
    /// observables and environment facts remain in the full declaration.
    pub fn from_effects(effects: &novarocks_type_contract::FunctionEffectDeclaration) -> Self {
        use novarocks_type_contract::ArgumentControl;
        let argument_evaluation = match effects.argument_control {
            ArgumentControl::If
            | ArgumentControl::Coalesce
            | ArgumentControl::SimpleCase
            | ArgumentControl::SearchedCase => FunctionArgumentEvaluation::ShortCircuit,
            ArgumentControl::Eager
            | ArgumentControl::TypeOnly
            | ArgumentControl::HigherOrder { .. }
            | ArgumentControl::Aggregate
            | ArgumentControl::Window
            | ArgumentControl::Table => FunctionArgumentEvaluation::Eager,
        };
        Self {
            volatility: effects.value_stability,
            argument_evaluation,
            failure_behavior: effects.failure_behavior,
            intrinsic_row_error: effects.own_row_error,
        }
    }
}

/// The aggregate state contract remains separate from its Arrow carrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateBindingDeclaration {
    pub intermediate_pattern: Box<str>,
    pub state_format: AggregateStateFormatIdentity,
}

/// One stable overload family. Patterns describe the owner's binder contract;
/// the catalog does not implement a second pattern language or rank candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionOverloadDeclaration {
    pub identity: FunctionOverloadId,
    pub semantics: FunctionSemantics,
    /// None is an explicit legacy-only declaration, never authority to infer
    /// missing base facts. Pure installation requires every overload's Some.
    pub effects: Option<novarocks_type_contract::FunctionEffectDeclaration>,
    pub argument_pattern: Box<str>,
    pub result_pattern: Box<str>,
    pub aggregate: Option<AggregateBindingDeclaration>,
}
impl FunctionOverloadDeclaration {
    pub fn from_effects(
        identity: FunctionOverloadId,
        argument_pattern: impl Into<Box<str>>,
        result_pattern: impl Into<Box<str>>,
        aggregate: Option<AggregateBindingDeclaration>,
        effects: novarocks_type_contract::FunctionEffectDeclaration,
    ) -> Self {
        Self {
            identity,
            semantics: FunctionSemantics::from_effects(&effects),
            effects: Some(effects),
            argument_pattern: argument_pattern.into(),
            result_pattern: result_pattern.into(),
            aggregate,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionBindingDeclaration {
    function_id: FunctionId,
    kind: FunctionKind,
    overloads: Box<[FunctionOverloadDeclaration]>,
}

impl FunctionBindingDeclaration {
    /// A complete metadata declaration, not proof of installed CPU coverage.
    /// Legacy facts are never expanded into guessed NULL/state/control facts.
    pub fn try_new_complete(
        function_id: FunctionId,
        kind: FunctionKind,
        overloads: impl IntoIterator<Item = FunctionOverloadDeclaration>,
    ) -> Result<Self, FunctionBindingError> {
        let declaration = Self::try_new(function_id, kind, overloads)?;
        declaration.validate_complete_effects()?;
        Ok(declaration)
    }

    pub fn validate_complete_effects(&self) -> Result<(), FunctionBindingError> {
        for overload in &self.overloads {
            if overload.effects.is_none() {
                return Err(FunctionBindingError::MissingEffectDeclaration(
                    overload.identity.clone(),
                ));
            }
        }
        Ok(())
    }

    pub fn effect_declaration(
        &self,
        identity: &FunctionOverloadId,
    ) -> Result<&novarocks_type_contract::FunctionEffectDeclaration, FunctionBindingError> {
        self.overload(identity)?
            .effects
            .as_ref()
            .ok_or_else(|| FunctionBindingError::MissingEffectDeclaration(identity.clone()))
    }

    pub fn try_new(
        function_id: FunctionId,
        kind: FunctionKind,
        overloads: impl IntoIterator<Item = FunctionOverloadDeclaration>,
    ) -> Result<Self, FunctionBindingError> {
        let mut overloads = overloads.into_iter().collect::<Vec<_>>();
        if overloads.is_empty() {
            return Err(invalid("function has no declared overloads"));
        }
        overloads.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        for overload in &mut overloads {
            if let Some(effects) = &mut overload.effects {
                effects
                    .validate(kind)
                    .map_err(|_| invalid("invalid complete function effect declaration"))?;
                if overload.semantics != FunctionSemantics::from_effects(effects) {
                    return Err(invalid(
                        "legacy semantics differ from full declaration projection",
                    ));
                }
                // Environment dependencies are a set. Keep one canonical
                // stored source for exact-owner lookup and digest material.
                effects.environment_dependencies.sort_unstable();
            }
        }
        let mut patterns = BTreeSet::new();
        for (index, overload) in overloads.iter().enumerate() {
            if !overload
                .semantics
                .intrinsic_row_error
                .is_valid_for_kind(kind)
            {
                return Err(invalid(
                    "intrinsic row-error fact differs from the function kind",
                ));
            }
            if index > 0 && overloads[index - 1].identity == overload.identity {
                return Err(FunctionBindingError::DuplicateOverload(
                    overload.identity.clone(),
                ));
            }
            validate_pattern(&overload.argument_pattern)?;
            validate_pattern(&overload.result_pattern)?;
            if !patterns.insert(overload.argument_pattern.as_ref()) {
                return Err(invalid(
                    "multiple overloads declare the same argument pattern",
                ));
            }
            if (kind == FunctionKind::Aggregate) != overload.aggregate.is_some() {
                return Err(invalid(
                    "aggregate state declaration differs from the function kind",
                ));
            }
            if let Some(aggregate) = &overload.aggregate {
                validate_pattern(&aggregate.intermediate_pattern)?;
            }
        }
        Ok(Self {
            function_id,
            kind,
            overloads: overloads.into_boxed_slice(),
        })
    }

    pub fn function_id(&self) -> &FunctionId {
        &self.function_id
    }
    pub const fn kind(&self) -> FunctionKind {
        self.kind
    }
    /// Conservative metadata for name-based discovery. Exact calls consume
    /// only their selected overload's semantics.
    pub fn volatility(&self) -> FunctionVolatility {
        self.overloads
            .iter()
            .map(|overload| overload.semantics.volatility)
            .max()
            .expect("a declaration always contains an overload")
    }
    pub fn overloads(&self) -> &[FunctionOverloadDeclaration] {
        &self.overloads
    }

    fn overload(
        &self,
        identity: &FunctionOverloadId,
    ) -> Result<&FunctionOverloadDeclaration, FunctionBindingError> {
        self.overloads
            .binary_search_by(|candidate| candidate.identity.cmp(identity))
            .map(|index| &self.overloads[index])
            .map_err(|_| FunctionBindingError::UnknownOverload(identity.clone()))
    }
}

fn validate_pattern(pattern: &str) -> Result<(), FunctionBindingError> {
    if pattern.trim().is_empty() || pattern.len() > u16::MAX as usize {
        return Err(invalid(
            "function signature pattern is empty or exceeds 65535 bytes",
        ));
    }
    Ok(())
}

/// Compile-time scalar values required by literal-dependent binders. The
/// argument's exact Arrow type specifies widths, decimal scales and time units.
/// `None` on FunctionArgument::Value means nonconstant; `Some(Null)` is a
/// constant NULL. Lambdas cannot carry a scalar constant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionLiteral {
    Null,
    Boolean(bool),
    Int64(i64),
    LargeInt(i128),
    UInt64(u64),
    Float64Bits(u64),
    Decimal128(i128),
    Utf8(Box<str>),
    Binary(Box<[u8]>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionArgument {
    Value {
        value_type: FunctionValueType,
        constant: Option<FunctionLiteral>,
    },
    Lambda {
        parameter_types: Box<[FunctionValueType]>,
        result_type: FunctionValueType,
    },
}

impl FunctionArgument {
    pub fn argument_type(&self) -> FunctionArgumentType {
        match self {
            Self::Value { value_type, .. } => FunctionArgumentType::Value(value_type.clone()),
            Self::Lambda {
                parameter_types,
                result_type,
            } => FunctionArgumentType::Lambda {
                parameter_types: parameter_types.clone(),
                result_type: result_type.clone(),
            },
        }
    }

    fn matches_type_observed(
        &self,
        expected: &FunctionArgumentType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FunctionBindingError> {
        match (self, expected) {
            (Self::Value { value_type, .. }, FunctionArgumentType::Value(expected)) => {
                work.step()?;
                if value_type.logical_type != expected.logical_type
                    || (value_type.nullable && !expected.nullable)
                {
                    return Ok(false);
                }
                novarocks_type_contract::fits_nested_nullability_observed(
                    &value_type.data_type,
                    &expected.data_type,
                    || work.step().map_err(FunctionBindingError::from),
                )
            }
            (
                Self::Lambda {
                    parameter_types,
                    result_type,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: expected_parameters,
                    result_type: expected_result,
                },
            ) => {
                if parameter_types.len() != expected_parameters.len() {
                    return Ok(false);
                }
                for (actual, expected) in parameter_types.iter().zip(expected_parameters) {
                    if !actual.exactly_equals_observed(expected, || {
                        work.step().map_err(FunctionBindingError::from)
                    })? {
                        return Ok(false);
                    }
                }
                result_type.exactly_equals_observed(expected_result, || {
                    work.step().map_err(FunctionBindingError::from)
                })
            }
            _ => Ok(false),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FunctionBindingRequest<'a> {
    /// Logical arguments followed by aggregate-owned ORDER BY update channels.
    pub arguments: &'a [FunctionArgument],
    /// Equals arguments.len() for every non-aggregate function.
    pub logical_argument_count: usize,
    /// An explicit syntax result constraint for an owner that declares a
    /// context-typed result, such as a zero-element typed array. This never
    /// authorizes a consumer to replace an already selected result type.
    pub expected_result_type: Option<&'a FunctionValueType>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FunctionResultType {
    Scalar(FunctionValueType),
    /// Only the function's produced columns, excluding outer pass-throughs.
    Relation(Box<[FunctionValueType]>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregateBindingSelection {
    pub intermediate_type: FunctionValueType,
    pub state_format: AggregateStateFormatIdentity,
}

/// Concrete instantiation of a declared overload, including explicit coercion
/// targets for values and lambda bodies/parameters. The caller must materialize
/// these coercions before exact validation. A value cannot coerce to a lambda,
/// and a lambda cannot gain or lose parameters through coercion.
/// No process handle or implementation pointer is part of this value.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FunctionBindingSelection {
    pub overload: FunctionOverloadId,
    pub argument_types: Box<[FunctionArgumentType]>,
    pub result_type: FunctionResultType,
    pub aggregate: Option<AggregateBindingSelection>,
}

/// FE resolution and BE selected-binding validation are distinct operations.
/// Implementations must validate the requested overload directly; validation
/// must not call resolution to choose a possibly different candidate.
pub trait FunctionBindingResolver: Send + Sync {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError>;

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResolvedFunctionBinding {
    pub function_id: FunctionId,
    pub kind: FunctionKind,
    pub semantics: FunctionSemantics,
    pub logical_argument_count: usize,
    pub selected: FunctionBindingSelection,
}

#[derive(Clone)]
pub(crate) struct FunctionBindingDefinition {
    pub(crate) declaration: Arc<FunctionBindingDeclaration>,
    resolver: Arc<dyn FunctionBindingResolver>,
    pub(crate) pure: Option<crate::pure_catalogue::PureFunctionAttachment>,
}

impl FunctionBindingDefinition {
    pub(crate) fn new(
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
    ) -> Self {
        Self {
            declaration: Arc::new(declaration),
            resolver,
            pure: None,
        }
    }
}

/// Derive an aggregate's binding contract from the overloads it already
/// declares.
///
/// An aggregate overload states its identity, what it takes, what it returns
/// and what its state looks like - which is everything a binding declaration
/// holds. Deriving it means the two halves of one function cannot disagree,
/// and that registering an aggregate cannot leave it resolvable through only
/// one of them.
pub(crate) fn parametric_aggregate_binding(
    canonical_name: &str,
    volatility: crate::FunctionVolatility,
    overloads: &[crate::AggregateOverloadDeclaration],
    aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
) -> Result<FunctionBindingDefinition, FunctionCatalogError> {
    let invalid_identity = |error: &dyn fmt::Display| FunctionCatalogError::InvalidStableIdentity {
        subject: "parametric aggregate binding declaration",
        value: error.to_string().into(),
    };
    let function_id = FunctionId::try_new(format!("parametric.aggregate/{canonical_name}/v1"))
        .map_err(|error| invalid_identity(&error))?;
    let declared = overloads
        .iter()
        .map(|overload| {
            Ok(FunctionOverloadDeclaration {
                effects: None,
                semantics: FunctionSemantics {
                    volatility,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
                },
                identity: FunctionOverloadId::try_new(overload.identity.as_str())
                    .map_err(|error| invalid_identity(&error))?,
                argument_pattern: overload.argument_pattern.clone(),
                result_pattern: overload.output_pattern.clone(),
                aggregate: Some(AggregateBindingDeclaration {
                    intermediate_pattern: overload.intermediate_pattern.clone(),
                    state_format: overload.state_format.clone(),
                }),
            })
        })
        .collect::<Result<Vec<_>, FunctionCatalogError>>()?;
    let declaration =
        FunctionBindingDeclaration::try_new(function_id, crate::FunctionKind::Aggregate, declared)
            .map_err(|error| invalid_identity(&error))?;
    Ok(FunctionBindingDefinition::new(
        declaration,
        Arc::new(ParametricAggregateBindingResolver { aggregate_resolver }),
    ))
}

/// Answers binding questions for an aggregate through the same typed contract
/// its signatures are resolved with, so the two can never disagree.
struct ParametricAggregateBindingResolver {
    aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
}

impl ParametricAggregateBindingResolver {
    fn argument_types(
        &self,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<arrow_schema::DataType>, FunctionBindingError> {
        let values = request
            .arguments
            .iter()
            .map(|argument| {
                work.step()?;
                match argument {
                    FunctionArgument::Value { value_type, .. } => Ok(value_type.clone()),
                    FunctionArgument::Lambda { .. } => {
                        Err(FunctionBindingError::NoMatchingOverload)
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        work.flush()?;
        self.aggregate_resolver
            .validate_value_arguments(&values)
            .map_err(FunctionBindingError::from)?;
        Ok(values.into_iter().map(|value| value.data_type).collect())
    }

    fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
        resolved: &crate::ResolvedAggregateSignature,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let nullable = self.aggregate_resolver.produces_null();
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(resolved.overload.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                resolved.output_type.clone(),
                nullable,
            )),
            aggregate: Some(crate::AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(
                    resolved.intermediate_type.clone(),
                    nullable,
                ),
                state_format: resolved.state_format.clone(),
            }),
        })
    }
}

impl FunctionBindingResolver for ParametricAggregateBindingResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let argument_types = self.argument_types(request, &mut work)?;
            work.flush()?;
            let logical = self
                .aggregate_resolver
                .resolve_aggregate(&argument_types[..request.logical_argument_count])
                .map_err(FunctionBindingError::from)?;
            let resolved = if request.logical_argument_count == argument_types.len() {
                logical
            } else {
                work.step()?;
                work.flush()?;
                self.aggregate_resolver
                    .resolve_update_signature(&logical.overload, &argument_types)
                    .map_err(FunctionBindingError::from)?
            };
            for _ in request.arguments {
                work.step()?;
            }
            self.selection(request, &resolved)
        })();
        finish_binding_work(result, work)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let argument_types = self.argument_types(request, &mut work)?;
            let selected_overload =
                crate::AggregateOverloadIdentity::try_new(selected.overload.as_str())
                    .map_err(|error| invalid(&error.to_string()))?;
            work.flush()?;
            let resolved = self
                .aggregate_resolver
                .resolve_update_signature(&selected_overload, &argument_types)
                .map_err(FunctionBindingError::from)?;
            for _ in request.arguments {
                work.step()?;
            }
            if &self.selection(request, &resolved)? == selected {
                Ok(())
            } else {
                Err(invalid(
                    "selected aggregate overload differs from its typed contract",
                ))
            }
        })();
        finish_binding_work(result, work)
    }
}

impl fmt::Debug for FunctionBindingDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionBindingDefinition")
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl FunctionDefinition {
    /// Register an explicit binding contract. Legacy resolution APIs reject
    /// this definition rather than discarding its selected identity.
    /// Register a non-aggregate function from its binding declaration.
    ///
    /// An aggregate is refused here on purpose. Resolving one needs a typed
    /// signature contract that this constructor has no way to obtain, and
    /// building an aggregate without one produced a definition that named the
    /// function everywhere but could not be resolved anywhere - which is not a
    /// failure any caller can see until something tries to resolve it. Use
    /// `try_new_bound_aggregate`, which cannot be called without one.
    pub fn try_new_bound(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        if declaration.kind == crate::FunctionKind::Aggregate {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "aggregate function without a typed signature contract",
                value: canonical_name.as_ref().into(),
            });
        }
        Self::bound(canonical_name, visibility, declaration, resolver, None)
    }

    /// Register an aggregate from its binding declaration and the contract that
    /// resolves its typed signature.
    pub fn try_new_bound_aggregate(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
        aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        Self::bound(
            canonical_name,
            visibility,
            declaration,
            resolver,
            Some(aggregate_resolver),
        )
    }

    pub(crate) fn bound(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
        aggregate_resolver: Option<Arc<dyn crate::AggregateSignatureResolver>>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        super::validate_canonical_name(canonical_name)?;
        let aggregate_overloads = declaration
            .overloads
            .iter()
            .filter_map(|overload| {
                overload.aggregate.as_ref().map(|aggregate| {
                    AggregateOverloadDeclaration::try_new(
                        overload.identity.as_str(),
                        &overload.argument_pattern,
                        &aggregate.intermediate_pattern,
                        &overload.result_pattern,
                        aggregate.state_format.as_str(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_signatures = declaration
            .overloads
            .iter()
            .map(|overload| overload.argument_pattern.clone())
            .collect();
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind: declaration.kind,
            visibility,
            volatility: declaration.volatility(),
            canonical_signatures,
            aggregate_overloads: aggregate_overloads.into_boxed_slice(),
            exact_aggregate_overloads: Box::default(),
            aggregate_resolver,
            resolver: None,
            binding: Some(FunctionBindingDefinition {
                declaration: Arc::new(declaration),
                resolver,
                pure: None,
            }),
        })
    }

    pub fn binding_declaration(&self) -> Option<&FunctionBindingDeclaration> {
        self.binding
            .as_ref()
            .map(|binding| binding.declaration.as_ref())
    }
}

impl EngineFunctionCatalogBuilder {
    /// Final-plan consumers require every entry to have an explicit identity.
    /// An incomplete migration cannot silently invent identities or semantics.
    pub fn seal_bound(self) -> Result<EngineFunctionCatalog, FunctionCatalogError> {
        for definition in self.definitions.values() {
            if definition.binding.is_none() {
                return Err(FunctionCatalogError::MissingBindingDeclaration {
                    name: definition.canonical_name.clone(),
                    kind: definition.kind,
                });
            }
        }
        self.seal()
    }
}

impl EngineFunctionCatalog {
    pub fn definition_by_id(&self, identity: &FunctionId) -> Option<&FunctionDefinition> {
        self.identities
            .get(identity)
            .map(|index| &self.definitions[*index])
    }

    pub fn resolve_bound_user(
        &self,
        name: &str,
        kind: FunctionKind,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            let definition = self
                .definition(name, kind)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            if definition.visibility == FunctionVisibility::Hidden {
                return Err(FunctionBindingError::HiddenFunction);
            }
            resolve_definition(definition, request, &mut work)
        })();
        finish_binding_work(result, work)
    }

    pub fn resolve_bound_trusted(
        &self,
        name: &str,
        kind: FunctionKind,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            let definition = self
                .definition(name, kind)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            resolve_definition(definition, request, &mut work)
        })();
        finish_binding_work(result, work)
    }

    /// Validate a selected definition by its exact installed identity, even
    /// when it has no runtime occurrence. This does not resolve a user name,
    /// authenticate frozen effects, prepare an implementation, or create state.
    /// Legacy semantic metadata is deliberately outside this selected-type port.
    pub fn validate_frozen_selection(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let definition = self
                .definition_by_id(function)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            let binding = exact_definition(definition)?;
            if kind != binding.declaration.kind {
                return Err(invalid("frozen function kind differs from the declaration"));
            }
            work.step()?;
            validate_selected_definition(binding, selected, request, &mut work)
        })();
        finish_binding_work(result, work)
    }

    /// Check one frozen binding and its already-coerced argument expressions.
    /// The executable implementation is selected by identity by its owner.
    pub fn validate_bound(
        &self,
        bound: &ResolvedFunctionBinding,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            work.step()?;
            let definition = self
                .definition_by_id(&bound.function_id)
                .ok_or(FunctionBindingError::UnknownFunction)?;
            let binding = exact_definition(definition)?;
            let overload = binding.declaration.overload(&bound.selected.overload)?;
            if bound.kind != binding.declaration.kind || bound.semantics != overload.semantics {
                return Err(invalid(
                    "frozen function kind or semantics differ from the declaration",
                ));
            }
            if bound.logical_argument_count != request.logical_argument_count {
                return Err(invalid(
                    "frozen logical argument count differs from the expression",
                ));
            }
            validate_selected_definition(binding, &bound.selected, request, &mut work)
        })();
        finish_binding_work(result, work)
    }
}

/// The selected-type algorithm is shared with legacy validation without
/// changing that entry's separate semantics/count checks or error order.
fn validate_selected_definition(
    binding: &FunctionBindingDefinition,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    validate_selection(&binding.declaration, selected, request, work)?;
    for (argument, expected) in request.arguments.iter().zip(&selected.argument_types) {
        work.step()?;
        if !argument.matches_type_observed(expected, work)? {
            return Err(invalid(
                "frozen argument types differ from the already-coerced expressions",
            ));
        }
    }
    work.flush()?;
    binding
        .resolver
        .validate_selected(selected, request, work.control())
}

fn exact_definition(
    definition: &FunctionDefinition,
) -> Result<&FunctionBindingDefinition, FunctionBindingError> {
    definition
        .binding
        .as_ref()
        .ok_or(FunctionBindingError::MissingBindingDeclaration)
}

fn resolve_definition(
    definition: &FunctionDefinition,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    let binding = exact_definition(definition)?;
    validate_request(binding.declaration.kind, request, work)?;
    work.flush()?;
    let selected = binding.resolver.resolve(request, work.control())?;
    validate_selection(&binding.declaration, &selected, request, work)?;
    Ok(ResolvedFunctionBinding {
        function_id: binding.declaration.function_id.clone(),
        kind: binding.declaration.kind,
        semantics: binding.declaration.overload(&selected.overload)?.semantics,
        logical_argument_count: request.logical_argument_count,
        selected,
    })
}

fn validate_request(
    kind: FunctionKind,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    if request.arguments.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    if let Some(expected) = request.expected_result_type {
        validate_value_type(expected, work)?;
    }
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value { value_type, .. } => validate_value_type(value_type, work)?,
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                if parameter_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                for ty in parameter_types {
                    work.step()?;
                    validate_value_type(ty, work)?;
                }
                validate_value_type(result_type, work)?;
            }
        }
    }
    if request.logical_argument_count > request.arguments.len()
        || (kind != FunctionKind::Aggregate
            && request.logical_argument_count != request.arguments.len())
    {
        return Err(invalid(
            "logical argument count differs from the function kind or update channels",
        ));
    }
    for argument in &request.arguments[request.logical_argument_count..] {
        work.step()?;
        if matches!(argument, FunctionArgument::Lambda { .. }) {
            return Err(invalid(
                "aggregate ORDER BY update channels must be values, not lambdas",
            ));
        }
    }
    Ok(())
}

fn validate_selection(
    declaration: &FunctionBindingDeclaration,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    validate_request(declaration.kind, request, work)?;
    if selected.argument_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    for argument in &selected.argument_types {
        work.step()?;
        validate_argument_type(argument, work)?;
    }
    match &selected.result_type {
        FunctionResultType::Scalar(ty) => validate_value_type(ty, work)?,
        FunctionResultType::Relation(types) => {
            if types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            for ty in types {
                work.step()?;
                validate_value_type(ty, work)?;
            }
        }
    }
    if let Some(aggregate) = &selected.aggregate {
        validate_value_type(&aggregate.intermediate_type, work)?;
    }
    let overload = declaration.overload(&selected.overload)?;
    if selected.argument_types.len() != request.arguments.len() {
        return Err(invalid(
            "selected signature argument count differs from the request",
        ));
    }
    for (argument, selected_type) in request.arguments.iter().zip(&selected.argument_types) {
        work.step()?;
        match (argument, selected_type) {
            (FunctionArgument::Value { .. }, FunctionArgumentType::Value(_)) => {}
            (
                FunctionArgument::Lambda {
                    parameter_types, ..
                },
                FunctionArgumentType::Lambda {
                    parameter_types: selected_parameters,
                    ..
                },
            ) if parameter_types.len() == selected_parameters.len() => {}
            _ => {
                return Err(invalid(
                    "selected argument shape or lambda arity differs from the request",
                ));
            }
        }
    }
    if (declaration.kind == FunctionKind::Table)
        != matches!(selected.result_type, FunctionResultType::Relation(_))
    {
        return Err(invalid(
            "selected result shape differs from the function kind",
        ));
    }
    match (&overload.aggregate, &selected.aggregate) {
        (None, None) => {}
        (Some(declared), Some(resolved)) => {
            if resolved.state_format != declared.state_format {
                return Err(invalid(
                    "aggregate state format differs from its declaration",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "selected aggregate state contract differs from the declaration",
            ));
        }
    }
    Ok(())
}

fn binding_kernel_failure(error: crate::KernelFailure) -> FunctionBindingError {
    match error {
        crate::KernelFailure::Cancelled => CompileControlError::Cancelled.into(),
        crate::KernelFailure::DeadlineExceeded => CompileControlError::DeadlineExceeded.into(),
        crate::KernelFailure::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
        other => invalid(&other.to_string()),
    }
}

fn validate_value_type(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    crate::kernel_input::validate_type_observed(value, work).map_err(binding_kernel_failure)
}

fn validate_argument_type(
    argument: &FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    match argument {
        FunctionArgumentType::Value(value) => validate_value_type(value, work),
        FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } => {
            if parameter_types.len() > crate::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            for ty in parameter_types {
                work.step()?;
                validate_value_type(ty, work)?;
            }
            validate_value_type(result_type, work)
        }
    }
}

fn finish_binding_work<T>(
    result: Result<T, FunctionBindingError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, FunctionBindingError> {
    if matches!(result, Err(FunctionBindingError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn digest_binding_definition(
    hasher: &mut Sha256,
    binding: Option<&FunctionBindingDefinition>,
) {
    let Some(binding) = binding else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let declaration = &binding.declaration;
    digest_text(hasher, declaration.function_id.as_str());
    hasher.update(
        u32::try_from(declaration.overloads.len())
            .expect("overload count fits u32")
            .to_be_bytes(),
    );
    for overload in &declaration.overloads {
        digest_text(hasher, overload.identity.as_str());
        hasher.update([match overload.semantics.volatility {
            FunctionVolatility::Immutable => 1,
            FunctionVolatility::Stable => 2,
            FunctionVolatility::Volatile => 3,
        }]);
        hasher.update([match overload.semantics.argument_evaluation {
            FunctionArgumentEvaluation::Eager => 1,
            FunctionArgumentEvaluation::ShortCircuit => 2,
        }]);
        hasher.update([match overload.semantics.failure_behavior {
            FunctionFailureBehavior::Propagate => 1,
            FunctionFailureBehavior::ReturnsNull => 2,
        }]);
        hasher.update([match overload.semantics.intrinsic_row_error {
            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError => 1,
            novarocks_type_contract::FunctionIntrinsicRowError::MayRaise => 2,
            novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated => 3,
        }]);
        digest_text(hasher, &overload.argument_pattern);
        digest_text(hasher, &overload.result_pattern);
        if let Some(effects) = &overload.effects {
            hasher.update([1]);
            crate::effect_metadata::digest_effect_declaration(hasher, effects);
        } else {
            hasher.update([0]);
        }
        if let Some(aggregate) = &overload.aggregate {
            hasher.update([1]);
            digest_text(hasher, &aggregate.intermediate_pattern);
            digest_text(hasher, aggregate.state_format.as_str());
        } else {
            hasher.update([0]);
        }
    }
    crate::pure_catalogue::digest_pure_attachment(hasher, binding.pure.as_ref());
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionBindingError {
    Control(CompileControlError),
    MissingEffectDeclaration(FunctionOverloadId),
    UnknownFunction,
    HiddenFunction,
    MissingBindingDeclaration,
    UnknownOverload(FunctionOverloadId),
    DuplicateOverload(FunctionOverloadId),
    NoMatchingOverload,
    AmbiguousOverload,
    InvalidBinding(Box<str>),
}

fn invalid(message: &str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

impl fmt::Display for FunctionBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::MissingEffectDeclaration(identity) => write!(
                formatter,
                "selected overload `{}` has no complete effect declaration",
                identity.as_str()
            ),
            Self::UnknownFunction => formatter.write_str("function is not registered"),
            Self::HiddenFunction => formatter.write_str("function is hidden from user SQL"),
            Self::MissingBindingDeclaration => {
                formatter.write_str("function has no exact binding declaration")
            }
            Self::UnknownOverload(identity) => write!(
                formatter,
                "unknown selected overload `{}`",
                identity.as_str()
            ),
            Self::DuplicateOverload(identity) => write!(
                formatter,
                "duplicate overload identity `{}`",
                identity.as_str()
            ),
            Self::NoMatchingOverload => formatter.write_str("no matching declared overload"),
            Self::AmbiguousOverload => {
                formatter.write_str("multiple declared overloads match ambiguously")
            }
            Self::InvalidBinding(message) => {
                write!(formatter, "invalid function binding: {message}")
            }
        }
    }
}

impl std::error::Error for FunctionBindingError {}

impl From<CompileControlError> for FunctionBindingError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl FunctionBindingError {
    pub fn control_error(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(error) => Some(*error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod effect_metadata_tests;

impl From<novarocks_type_contract::ValueTypeError> for FunctionBindingError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        invalid(&error.to_string())
    }
}

impl From<crate::FunctionResolutionError> for FunctionBindingError {
    fn from(error: crate::FunctionResolutionError) -> Self {
        match error {
            crate::FunctionResolutionError::Control(error) => error.into(),
            other => invalid(&other.to_string()),
        }
    }
}
