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

//! Exact, internal value-domain conversions selected by the FE type owner.
//!
//! This owner does not authorize a carrier CAST to change logical identity.
//! Binding requires an explicit complete target; there is no default target,
//! name-based source classification or decimal error-policy default. The
//! structural binder uses the existing bounded FVT validation, not a resource
//! admission or cooperative compilation receipt.

use super::binding_control;
use novarocks_type_contract::{CompileCheckpoints, PureCompileControl};
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    ArgumentControl, FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior,
    NR_LOGICAL_TYPE_KEY, ObservableEffects, ValueLogicalType, arrow_data_types_exact,
    arrow_fields_exact, field_logical_type,
};

use crate::{
    FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadDeclaration, FunctionOverloadId,
    FunctionResultType, FunctionValueType, FunctionVolatility,
};

pub const VALUE_CONVERSION_NAME: &str = "__value_domain_conversion";
pub const VALUE_CONVERSION_FUNCTION_ID: &str = "builtin.scalar/value_domain_conversion/v1";

pub(super) const JSON_TEXT: &str =
    "builtin.scalar/value_domain_conversion/json_text_same_structure/v1";
pub(super) const SIGNED_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/signed_to_largeint/v1";
pub(super) const LARGEINT_SIGNED: &str =
    "builtin.scalar/value_domain_conversion/largeint_to_signed_null_overflow/v1";
pub(super) const LARGEINT_FLOAT: &str =
    "builtin.scalar/value_domain_conversion/largeint_to_float_round/v1";
pub(super) const NULL_LIFT: &str =
    "builtin.scalar/value_domain_conversion/null_to_typed_nullable/v1";

/// Full effects shared by these total value conversions. Overflow NULL is a
/// result of the signed narrowing overload, not suppression of a child,
/// control, resource, contract or lifecycle failure. JSON bytes are preserved;
/// signed integers extend exactly; floats use the existing i128 `as` rounding.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

/// Install the exact internal binder and all of its pure scalar families.
pub fn value_conversion_definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    let (declaration, resolver) = definition_parts()?;
    super::value_conversion_owner::definition(declaration, resolver)
}

pub(super) fn definition_parts()
-> Result<(FunctionBindingDeclaration, ValueConversionResolver), FunctionCatalogError> {
    let invalid = |error: &dyn std::fmt::Display| FunctionCatalogError::InvalidStableIdentity {
        subject: "internal value-domain conversion declaration",
        value: error.to_string().into(),
    };
    let overloads = [
        (JSON_TEXT, "Json->PhysicalUtf8, or identical List/LargeList/FixedSizeList/Struct/Map shape with Json fields->PhysicalUtf8; preserve all other field facts", "explicit exact target; NULL preserved; JSON bytes unchanged"),
        (SIGNED_LARGEINT, "Physical(Int8|Int16|Int32|Int64)->LargeInt(FixedSizeBinary16)", "explicit exact LargeInt target; signed i128 extension; NULL preserved"),
        (LARGEINT_SIGNED, "LargeInt(FixedSizeBinary16)->Physical(Int8|Int16|Int32|Int64)", "explicit nullable exact target; out-of-range value returns NULL; NULL preserved"),
        (LARGEINT_FLOAT, "LargeInt(FixedSizeBinary16)->Physical(Float32|Float64)", "explicit exact target; existing i128 as float rounding; NULL preserved"),
        (NULL_LIFT, "Physical Null(nullable=true)->explicit complete nullable target", "evaluate child eagerly; strict NULL protocol produces typed NULL without row implementation"),
    ]
    .into_iter()
    .map(|(id, argument_pattern, result_pattern)| {
        Ok(FunctionOverloadDeclaration::from_effects(
            FunctionOverloadId::try_new(id).map_err(|error| invalid(&error))?,
            argument_pattern,
            result_pattern,
            None,
            effects(),
        ))
    })
    .collect::<Result<Vec<_>, FunctionCatalogError>>()?;
    let declaration = FunctionBindingDeclaration::try_new_complete(
        FunctionId::try_new(VALUE_CONVERSION_FUNCTION_ID).map_err(|error| invalid(&error))?,
        FunctionKind::Scalar,
        overloads,
    )
    .map_err(|error| invalid(&error))?;
    Ok((declaration, ValueConversionResolver))
}

pub(super) struct ValueConversionResolver;

#[cfg(test)]
#[path = "value_conversion_exact_tests.rs"]
mod exact_tests;

/// Check an assignment through this exact implementation-owner binder. The
/// returned selection still needs the registered function identity, FE
/// materialization and installed kernel coverage before execution.
pub fn resolve_value_conversion(
    source: &FunctionValueType,
    target: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    binding_control::scope(control, |work| {
        binding_control::value_type(source, work)?;
        binding_control::value_type(target, work)?;
        let arguments = [FunctionArgument::Value {
            value_type: source.clone(),
            constant: None,
        }];
        work.flush()?;
        ValueConversionResolver.resolve(
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: Some(target),
            },
            work.control(),
        )
    })
}

// A pure declaration recipe is not an invocation of a catalogue resolver. It
// preserves the existing logical-coercion predicates without minting a control
// capability or pretending their legacy recursive work is cooperative.
pub(super) fn recipe_selection(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    let arguments = [FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    }];
    let (source, target) = inputs(FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: Some(target),
    })?;
    let id = [
        JSON_TEXT,
        SIGNED_LARGEINT,
        LARGEINT_SIGNED,
        LARGEINT_FLOAT,
        NULL_LIFT,
    ]
    .into_iter()
    .find(|id| pair_matches(id, source, target))
    .ok_or(FunctionBindingError::NoMatchingOverload)?;
    selection(id, source, target)
}

fn selection(
    id: &str,
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    Ok(FunctionBindingSelection {
        overload: FunctionOverloadId::try_new(id)
            .map_err(|_| invalid("invalid conversion overload identity"))?,
        argument_types: vec![FunctionArgumentType::Value(source.clone())].into_boxed_slice(),
        result_type: FunctionResultType::Scalar(target.clone()),
        aggregate: None,
    })
}

/// Same-domain carrier coercion remains with the existing type owner. A
/// decided logical-domain change must be admitted by the exact conversion
/// owner; untyped NULL does not establish a domain and follows the variable.
pub(crate) fn assignment_domains_authorized(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> bool {
    conversion_intermediate_type(source, target).is_ok()
}

/// Return the exact first conversion result for a two-stage assignment. The
/// source-owned field names, nullability and metadata survive this first stage;
/// an ordinary same-domain carrier CAST may then reach `target`. `None` means
/// no decided domain changes. Physical NULL has an explicit lift binding too:
/// the eager child is evaluated before the strict typed-NULL result protocol.
/// The FE still supplies its actual decimal policy
/// to that CAST; this helper does not choose one or install either kernel.
pub fn conversion_intermediate_type(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<Option<FunctionValueType>, FunctionBindingError> {
    source
        .validate()
        .map_err(|_| invalid("invalid assignment source type"))?;
    target
        .validate()
        .map_err(|_| invalid("invalid assignment target type"))?;
    if source.nullable && !target.nullable {
        return Err(invalid(
            "assignment target cannot narrow source nullability",
        ));
    }
    if source.data_type == DataType::Null && source.logical_type == ValueLogicalType::Physical {
        recipe_selection(source, target)?;
        return Ok(Some(target.clone()));
    }
    if source.logical_type == target.logical_type
        && novarocks_type_contract::preserves_nested_logical_identity(
            &source.data_type,
            &target.data_type,
        )
        && dictionary_identity_preserved(&source.data_type, &target.data_type)
    {
        return Ok(None);
    }
    if recipe_selection(source, target).is_ok() {
        return Ok(Some(target.clone()));
    }
    let logical_type = match (source.logical_type, target.logical_type) {
        (ValueLogicalType::Json, ValueLogicalType::Physical) => ValueLogicalType::Physical,
        (a, b) if a == b => a,
        _ => return Err(FunctionBindingError::NoMatchingOverload),
    };
    let intermediate = FunctionValueType::try_with_logical_type(
        json_intermediate_carrier(&source.data_type, &target.data_type)?,
        source.nullable,
        logical_type,
    )
    .map_err(|_| invalid("invalid conversion intermediate type"))?;
    recipe_selection(source, &intermediate)?;
    if intermediate.logical_type != target.logical_type
        || (intermediate.nullable && !target.nullable)
        || !novarocks_type_contract::preserves_nested_logical_identity(
            &intermediate.data_type,
            &target.data_type,
        )
        || !dictionary_identity_preserved(&intermediate.data_type, &target.data_type)
        || !(arrow_data_types_exact(&intermediate.data_type, &target.data_type)
            || arrow_cast::can_cast_types(&intermediate.data_type, &target.data_type))
    {
        return Err(invalid(
            "conversion intermediate cannot reach the selected same-domain carrier target",
        ));
    }
    Ok(Some(intermediate))
}

fn json_intermediate_field(
    source: &Field,
    target: &Field,
) -> Result<Arc<Field>, FunctionBindingError> {
    let source_domain =
        field_logical_type(source).map_err(|_| invalid("invalid source field identity"))?;
    let target_domain =
        field_logical_type(target).map_err(|_| invalid("invalid target field identity"))?;
    let changed = json_domain_pair(source_domain, target_domain)
        .ok_or(FunctionBindingError::NoMatchingOverload)?;
    let mut metadata = source.metadata().clone();
    if changed {
        metadata.remove(NR_LOGICAL_TYPE_KEY);
    }
    Ok(Arc::new(
        source
            .clone()
            .with_data_type(json_intermediate_carrier(
                source.data_type(),
                target.data_type(),
            )?)
            .with_metadata(metadata),
    ))
}

fn json_intermediate_carrier(
    source: &DataType,
    target: &DataType,
) -> Result<DataType, FunctionBindingError> {
    let no_match = || FunctionBindingError::NoMatchingOverload;
    Ok(match (source, target) {
        (DataType::List(a), DataType::List(b) | DataType::LargeList(b)) => {
            DataType::List(json_intermediate_field(a, b)?)
        }
        (DataType::LargeList(a), DataType::List(b) | DataType::LargeList(b)) => {
            DataType::LargeList(json_intermediate_field(a, b)?)
        }
        (
            DataType::FixedSizeList(a, n),
            DataType::FixedSizeList(b, _) | DataType::List(b) | DataType::LargeList(b),
        ) => DataType::FixedSizeList(json_intermediate_field(a, b)?, *n),
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            let by_name = a
                .iter()
                .all(|field| b.iter().any(|other| field.name() == other.name()));
            DataType::Struct(
                a.iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let target = if by_name {
                            b.iter()
                                .find(|other| field.name() == other.name())
                                .ok_or_else(no_match)?
                        } else {
                            &b[index]
                        };
                        json_intermediate_field(field, target)
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )
        }
        (DataType::Map(a, sorted), DataType::Map(b, _)) => {
            DataType::Map(json_intermediate_field(a, b)?, *sorted)
        }
        _ if novarocks_type_contract::preserves_nested_logical_identity(source, target) => {
            source.clone()
        }
        _ => return Err(no_match()),
    })
}

/// Dictionary identities belong to fields even when Arrow's schema equality
/// ignores them. Reaching a same-domain target cannot silently replace them.
fn dictionary_identity_preserved(source: &DataType, target: &DataType) -> bool {
    fn field(a: &Field, b: &Field) -> bool {
        arrow_fields_exact(
            &a.clone()
                .with_name(b.name())
                .with_nullable(b.is_nullable())
                .with_data_type(b.data_type().clone())
                .with_metadata(b.metadata().clone()),
            b,
        ) && dictionary_identity_preserved(a.data_type(), b.data_type())
    }
    match (source, target) {
        (DataType::Dictionary(..), _) | (_, DataType::Dictionary(..)) => {
            arrow_data_types_exact(source, target)
        }
        (
            DataType::List(a) | DataType::LargeList(a) | DataType::FixedSizeList(a, _),
            DataType::List(b) | DataType::LargeList(b) | DataType::FixedSizeList(b, _),
        )
        | (DataType::Map(a, _), DataType::Map(b, _)) => field(a, b),
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            a.iter().zip(b).all(|(a, b)| field(a, b))
        }
        // These encodings are outside the semantic conversion families.
        (
            DataType::Union(..)
            | DataType::RunEndEncoded(..)
            | DataType::ListView(..)
            | DataType::LargeListView(..),
            _,
        )
        | (
            _,
            DataType::Union(..)
            | DataType::RunEndEncoded(..)
            | DataType::ListView(..)
            | DataType::LargeListView(..),
        ) => arrow_data_types_exact(source, target),
        _ => true,
    }
}

fn invalid(message: &str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

fn inputs<'a>(
    request: FunctionBindingRequest<'a>,
) -> Result<(&'a FunctionValueType, &'a FunctionValueType), FunctionBindingError> {
    let (source, target) = input_shape(request)?;
    source
        .validate()
        .map_err(|_| invalid("invalid conversion source type"))?;
    target
        .validate()
        .map_err(|_| invalid("invalid conversion target type"))?;
    Ok((source, target))
}

fn input_shape<'a>(
    request: FunctionBindingRequest<'a>,
) -> Result<(&'a FunctionValueType, &'a FunctionValueType), FunctionBindingError> {
    if request.logical_argument_count != 1 || request.arguments.len() != 1 {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let FunctionArgument::Value {
        value_type: source, ..
    } = &request.arguments[0]
    else {
        return Err(FunctionBindingError::NoMatchingOverload);
    };
    let target = request.expected_result_type.ok_or_else(|| {
        invalid("internal value-domain conversion requires an explicit complete target")
    })?;
    if source.nullable && !target.nullable {
        return Err(invalid(
            "conversion target cannot narrow source nullability",
        ));
    }
    Ok((source, target))
}

fn signed(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// Match the exact selected family directly. No resolution replay is used by
/// frozen validation. In particular FixedSizeBinary16 is not a numeric source.
fn pair_matches(id: &str, source: &FunctionValueType, target: &FunctionValueType) -> bool {
    match id {
        JSON_TEXT => {
            let Some(root_changed) = json_domain_pair(source.logical_type, target.logical_type)
            else {
                return false;
            };
            json_carriers(&source.data_type, &target.data_type)
                .is_some_and(|nested_changed| root_changed || nested_changed)
        }
        SIGNED_LARGEINT => {
            source.logical_type == ValueLogicalType::Physical
                && signed(&source.data_type)
                && target.logical_type == ValueLogicalType::LargeInt
                && target.data_type == DataType::FixedSizeBinary(16)
        }
        LARGEINT_SIGNED => {
            source.logical_type == ValueLogicalType::LargeInt
                && source.data_type == DataType::FixedSizeBinary(16)
                && target.logical_type == ValueLogicalType::Physical
                && signed(&target.data_type)
                && target.nullable
        }
        LARGEINT_FLOAT => {
            source.logical_type == ValueLogicalType::LargeInt
                && source.data_type == DataType::FixedSizeBinary(16)
                && target.logical_type == ValueLogicalType::Physical
                && matches!(target.data_type, DataType::Float32 | DataType::Float64)
        }
        NULL_LIFT => {
            source.logical_type == ValueLogicalType::Physical
                && source.data_type == DataType::Null
                && source.nullable
                && target.nullable
        }
        _ => false,
    }
}

fn json_domain_pair(source: ValueLogicalType, target: ValueLogicalType) -> Option<bool> {
    if source == ValueLogicalType::Json && target == ValueLogicalType::Physical {
        Some(true)
    } else if source == target {
        Some(false)
    } else {
        None
    }
}

fn json_field(source: &Field, target: &Field) -> Option<bool> {
    if !arrow_fields_exact(
        &source
            .clone()
            .with_data_type(target.data_type().clone())
            .with_metadata(target.metadata().clone()),
        target,
    ) {
        return None;
    }
    let changed = json_domain_pair(
        field_logical_type(source).ok()?,
        field_logical_type(target).ok()?,
    )?;
    // Only the precise JSON domain tag is removed. No unrelated metadata can
    // be added, removed or changed by this conversion.
    if source.metadata().iter().any(|(key, value)| {
        if changed && key == NR_LOGICAL_TYPE_KEY {
            target.metadata().contains_key(key)
        } else {
            target.metadata().get(key) != Some(value)
        }
    }) || target
        .metadata()
        .keys()
        .any(|key| !source.metadata().contains_key(key))
    {
        return None;
    }
    let nested_changed = json_carriers(source.data_type(), target.data_type())?;
    Some(changed || nested_changed)
}

/// Only the container shapes already supported by SQL's existing nested
/// conversion path are declared here. Other encodings may remain unchanged,
/// but this overload does not authorize rebuilding their identities.
fn json_carriers(source: &DataType, target: &DataType) -> Option<bool> {
    match (source, target) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => json_field(a, b),
        (DataType::FixedSizeList(a, an), DataType::FixedSizeList(b, bn)) if an == bn => {
            json_field(a, b)
        }
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            let mut changed = false;
            for (a, b) in a.iter().zip(b) {
                changed |= json_field(a, b)?;
            }
            Some(changed)
        }
        (DataType::Map(a, sorted_a), DataType::Map(b, sorted_b)) if sorted_a == sorted_b => {
            json_field(a, b)
        }
        _ if arrow_data_types_exact(source, target) => Some(false),
        _ => None,
    }
}

fn input_pair_observed<'a>(
    request: FunctionBindingRequest<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a FunctionValueType, &'a FunctionValueType), FunctionBindingError> {
    binding_control::request_types(request, work)?;
    input_shape(request)
}

fn require_selected_pair_observed(
    overload: &FunctionOverloadId,
    source: &FunctionValueType,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    if !pair_matches_observed(overload.as_str(), source, target, work)? {
        return Err(invalid(
            "frozen value-domain conversion differs from its exact source, target or overload",
        ));
    }
    Ok(())
}

impl FunctionBindingResolver for ValueConversionResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let (source, target) = input_shape(request)?;
            for id in [
                JSON_TEXT,
                SIGNED_LARGEINT,
                LARGEINT_SIGNED,
                LARGEINT_FLOAT,
                NULL_LIFT,
            ] {
                work.step()?;
                if pair_matches_observed(id, source, target, work)? {
                    return selection(id, source, target);
                }
            }
            Err(FunctionBindingError::NoMatchingOverload)
        })
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            let (source, target) = input_pair_observed(request, work)?;
            require_selected_pair_observed(overload, source, target, work)?;
            selection(overload.as_str(), source, target)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let (source, target) = input_pair_observed(request, work)?;
            let arguments_match = match selected.argument_types.as_ref() {
                [FunctionArgumentType::Value(actual)] => {
                    binding_control::exact_type(actual, source, work)?
                }
                _ => false,
            };
            let result_matches = match &selected.result_type {
                FunctionResultType::Scalar(actual) => {
                    binding_control::exact_type(actual, target, work)?
                }
                _ => false,
            };
            if selected.aggregate.is_some() || !arguments_match || !result_matches {
                return Err(invalid(
                    "frozen value-domain conversion differs from its exact source, target or overload",
                ));
            }
            require_selected_pair_observed(&selected.overload, source, target, work)
        })
    }
}

fn pair_matches_observed(
    id: &str,
    source: &FunctionValueType,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    work.step()?;
    if id != JSON_TEXT {
        return Ok(pair_matches(id, source, target));
    }
    let Some(changed) = json_domain_pair(source.logical_type, target.logical_type) else {
        return Ok(false);
    };
    Ok(
        json_carriers_observed(&source.data_type, &target.data_type, work)?
            .is_some_and(|nested| changed || nested),
    )
}

fn json_field_observed(
    source: &Field,
    target: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<bool>, FunctionBindingError> {
    work.step()?;
    let shape = source
        .clone()
        .with_data_type(target.data_type().clone())
        .with_metadata(target.metadata().clone());
    if !novarocks_type_contract::arrow_fields_exact_observed::<crate::KernelFailure>(
        &shape,
        target,
        || work.step().map_err(crate::kernel_control::compile_failure),
    )
    .map_err(binding_control::type_error)?
    {
        return Ok(None);
    }
    let (Ok(source_domain), Ok(target_domain)) =
        (field_logical_type(source), field_logical_type(target))
    else {
        return Ok(None);
    };
    let Some(changed) = json_domain_pair(source_domain, target_domain) else {
        return Ok(None);
    };
    for (key, value) in source.metadata() {
        work.step()?;
        if if changed && key == NR_LOGICAL_TYPE_KEY {
            target.metadata().contains_key(key)
        } else {
            target.metadata().get(key) != Some(value)
        } {
            return Ok(None);
        }
    }
    for key in target.metadata().keys() {
        work.step()?;
        if !source.metadata().contains_key(key) {
            return Ok(None);
        }
    }
    Ok(
        json_carriers_observed(source.data_type(), target.data_type(), work)?
            .map(|nested| changed || nested),
    )
}

fn json_carriers_observed(
    source: &DataType,
    target: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<bool>, FunctionBindingError> {
    work.step()?;
    match (source, target) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => json_field_observed(a, b, work),
        (DataType::FixedSizeList(a, an), DataType::FixedSizeList(b, bn)) if an == bn => {
            json_field_observed(a, b, work)
        }
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            let mut changed = false;
            for (a, b) in a.iter().zip(b) {
                work.step()?;
                let Some(nested) = json_field_observed(a, b, work)? else {
                    return Ok(None);
                };
                changed |= nested;
            }
            Ok(Some(changed))
        }
        (DataType::Map(a, sa), DataType::Map(b, sb)) if sa == sb => json_field_observed(a, b, work),
        _ => Ok(
            novarocks_type_contract::arrow_data_types_exact_observed::<crate::KernelFailure>(
                source,
                target,
                || work.step().map_err(crate::kernel_control::compile_failure),
            )
            .map_err(binding_control::type_error)?
            .then_some(false),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FunctionVisibility;
    use crate::{EngineFunctionCatalog, EngineFunctionCatalogBuilder};

    fn catalog() -> EngineFunctionCatalog {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(value_conversion_definition().unwrap())
            .unwrap();
        builder.seal().unwrap()
    }
    fn value(ty: FunctionValueType) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: ty,
            constant: None,
        }
    }
    fn physical(carrier: DataType, nullable: bool) -> FunctionValueType {
        FunctionValueType::new(carrier, nullable)
    }
    fn logical(carrier: DataType, nullable: bool, domain: ValueLogicalType) -> FunctionValueType {
        FunctionValueType::try_with_logical_type(carrier, nullable, domain).unwrap()
    }
    fn request<'a>(
        args: &'a [FunctionArgument],
        target: &'a FunctionValueType,
    ) -> FunctionBindingRequest<'a> {
        FunctionBindingRequest {
            arguments: args,
            logical_argument_count: 1,
            expected_result_type: Some(target),
        }
    }
    fn freeze(source: FunctionValueType, target: &FunctionValueType, overload: &str) {
        let catalog = catalog();
        let args = [value(source)];
        let binding = catalog
            .resolve_bound_trusted(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                request(&args, target),
                crate::binding_test_control(),
            )
            .unwrap();
        assert_eq!(binding.function_id.as_str(), VALUE_CONVERSION_FUNCTION_ID);
        assert_eq!(binding.selected.overload.as_str(), overload);
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Scalar(target.clone())
        );
        catalog
            .validate_bound(
                &binding,
                request(&args, target),
                crate::binding_test_control(),
            )
            .unwrap();
    }

    #[test]
    fn exact_hidden_owner_and_full_effects_are_declared() {
        let definition = value_conversion_definition().unwrap();
        let declaration = definition.binding_declaration().unwrap();
        declaration.validate_complete_effects().unwrap();
        assert_eq!(declaration.overloads().len(), 5);
        for overload in declaration.overloads() {
            assert_eq!(overload.effects.as_ref().unwrap(), &effects());
        }
        let mut changed_overloads = declaration.overloads().to_vec();
        changed_overloads[0].result_pattern = "different declared NULL/overflow policy".into();
        let changed = FunctionBindingDeclaration::try_new_complete(
            declaration.function_id().clone(),
            FunctionKind::Scalar,
            changed_overloads,
        )
        .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                FunctionDefinition::try_new_bound(
                    VALUE_CONVERSION_NAME,
                    FunctionVisibility::Hidden,
                    changed,
                    Arc::new(ValueConversionResolver),
                )
                .unwrap(),
            )
            .unwrap();
        assert_ne!(catalog().digest(), builder.seal().unwrap().digest());
        let source = [value(logical(DataType::Utf8, true, ValueLogicalType::Json))];
        let target = physical(DataType::Utf8, true);
        assert!(matches!(
            catalog().resolve_bound_user(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                request(&source, &target),
                crate::binding_test_control()
            ),
            Err(FunctionBindingError::HiddenFunction)
        ));
    }

    #[test]
    fn actual_supported_root_pairs_resolve_and_freeze() {
        freeze(
            logical(DataType::Utf8, false, ValueLogicalType::Json),
            &physical(DataType::Utf8, true),
            JSON_TEXT,
        );
        for carrier in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            freeze(
                physical(carrier.clone(), false),
                &logical(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                ),
                SIGNED_LARGEINT,
            );
            freeze(
                logical(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                ),
                &physical(carrier, true),
                LARGEINT_SIGNED,
            );
        }
        for carrier in [DataType::Float32, DataType::Float64] {
            freeze(
                logical(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                ),
                &physical(carrier, false),
                LARGEINT_FLOAT,
            );
        }
    }

    fn json_field_type(domain: bool, name: &str, nullable: bool) -> Arc<Field> {
        let mut metadata =
            std::collections::HashMap::from([("source".to_string(), "user-column".to_string())]);
        if domain {
            metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), "json".to_string());
        }
        Arc::new(Field::new(name, DataType::Utf8, nullable).with_metadata(metadata))
    }

    #[test]
    fn nested_same_structure_preserves_every_other_field_fact() {
        let source = physical(
            DataType::Struct(
                vec![Field::new(
                    "items",
                    DataType::List(json_field_type(true, "item", true)),
                    false,
                )]
                .into(),
            ),
            false,
        );
        let target = physical(
            DataType::Struct(
                vec![Field::new(
                    "items",
                    DataType::List(json_field_type(false, "item", true)),
                    false,
                )]
                .into(),
            ),
            false,
        );
        freeze(source.clone(), &target, JSON_TEXT);
        for child in [
            json_field_type(false, "renamed", true),
            json_field_type(false, "item", false),
            Arc::new(Field::new("item", DataType::Utf8, true)),
        ] {
            let wrong = physical(
                DataType::Struct(vec![Field::new("items", DataType::List(child), false)].into()),
                false,
            );
            assert!(
                catalog()
                    .resolve_bound_trusted(
                        VALUE_CONVERSION_NAME,
                        FunctionKind::Scalar,
                        request(&[value(source.clone())], &wrong),
                        crate::binding_test_control()
                    )
                    .is_err()
            );
        }
    }

    #[test]
    fn invalid_domains_policies_and_missing_explicit_target_are_rejected() {
        let large = logical(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        );
        for (source, target) in [
            (
                physical(DataType::FixedSizeBinary(16), false),
                physical(DataType::Int64, true),
            ),
            (
                logical(DataType::FixedSizeBinary(16), false, ValueLogicalType::Uuid),
                physical(DataType::Int64, true),
            ),
            (
                physical(DataType::Utf8, false),
                logical(DataType::Utf8, false, ValueLogicalType::Json),
            ),
            (physical(DataType::UInt64, false), large.clone()),
            (large.clone(), physical(DataType::Int64, false)),
            (large.clone(), physical(DataType::Decimal128(18, 2), true)),
            (
                logical(DataType::Utf8, true, ValueLogicalType::Json),
                physical(DataType::Utf8, false),
            ),
            (
                logical(DataType::Utf8, false, ValueLogicalType::Json),
                physical(DataType::LargeUtf8, false),
            ),
        ] {
            assert!(
                catalog()
                    .resolve_bound_trusted(
                        VALUE_CONVERSION_NAME,
                        FunctionKind::Scalar,
                        request(&[value(source)], &target),
                        crate::binding_test_control()
                    )
                    .is_err()
            );
        }
        let arguments = [value(large)];
        assert!(
            catalog()
                .resolve_bound_trusted(
                    VALUE_CONVERSION_NAME,
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: 1,
                        expected_result_type: None
                    },
                    crate::binding_test_control()
                )
                .is_err()
        );
    }

    #[test]
    fn frozen_validation_cannot_reselect_or_replace_full_target() {
        let catalog = catalog();
        let source = [value(logical(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        ))];
        let target = physical(DataType::Int64, true);
        let binding = catalog
            .resolve_bound_trusted(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                request(&source, &target),
                crate::binding_test_control(),
            )
            .unwrap();
        let mut forged = binding.clone();
        forged.selected.overload = FunctionOverloadId::try_new(LARGEINT_FLOAT).unwrap();
        assert!(
            catalog
                .validate_bound(
                    &forged,
                    request(&source, &target),
                    crate::binding_test_control()
                )
                .is_err()
        );
        let wrong = physical(DataType::Int32, true);
        assert!(
            catalog
                .validate_bound(
                    &binding,
                    request(&source, &wrong),
                    crate::binding_test_control()
                )
                .is_err()
        );
        let uuid = [value(logical(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        ))];
        assert!(
            catalog
                .validate_bound(
                    &binding,
                    request(&uuid, &target),
                    crate::binding_test_control()
                )
                .is_err()
        );
    }

    #[test]
    fn nested_container_parameters_and_dictionary_identity_are_not_rewritten() {
        let a = json_field_type(true, "item", true);
        let b = json_field_type(false, "item", true);
        for (source, target) in [
            (
                DataType::LargeList(a.clone()),
                DataType::LargeList(b.clone()),
            ),
            (
                DataType::FixedSizeList(a.clone(), 3),
                DataType::FixedSizeList(b.clone(), 3),
            ),
            (
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Int64, false),
                                a.as_ref().clone(),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Int64, false),
                                b.as_ref().clone(),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
            ),
        ] {
            freeze(physical(source, false), &physical(target, false), JSON_TEXT);
        }
        let source = [value(physical(
            DataType::FixedSizeList(a.clone(), 3),
            false,
        ))];
        let wrong_width = physical(DataType::FixedSizeList(b.clone(), 4), false);
        assert!(
            catalog()
                .resolve_bound_trusted(
                    VALUE_CONVERSION_NAME,
                    FunctionKind::Scalar,
                    request(&source, &wrong_width),
                    crate::binding_test_control()
                )
                .is_err()
        );

        let dictionary = Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let source = [value(physical(
            DataType::Struct(vec![a.as_ref().clone(), dictionary.clone()].into()),
            false,
        ))];
        let target = physical(
            DataType::Struct(vec![b.as_ref().clone(), dictionary.clone()].into()),
            false,
        );
        let catalog = catalog();
        let binding = catalog
            .resolve_bound_trusted(
                VALUE_CONVERSION_NAME,
                FunctionKind::Scalar,
                request(&source, &target),
                crate::binding_test_control(),
            )
            .unwrap();
        let changed_order = physical(
            DataType::Struct(
                vec![b.as_ref().clone(), dictionary.with_dict_is_ordered(true)].into(),
            ),
            false,
        );
        assert!(
            catalog
                .resolve_bound_trusted(
                    VALUE_CONVERSION_NAME,
                    FunctionKind::Scalar,
                    request(&source, &changed_order),
                    crate::binding_test_control()
                )
                .is_err()
        );
        assert_ne!(target, changed_order);
        assert!(
            catalog
                .validate_bound(
                    &binding,
                    request(&source, &changed_order),
                    crate::binding_test_control()
                )
                .is_err()
        );
        let mut forged = binding.clone();
        forged.selected.result_type = FunctionResultType::Scalar(changed_order);
        assert!(
            catalog
                .validate_bound(
                    &forged,
                    request(&source, &target),
                    crate::binding_test_control()
                )
                .is_err()
        );
    }

    #[test]
    fn two_stage_json_assignment_preserves_source_fields_before_carrier_cast() {
        let source_field = json_field_type(true, "provider-item", false);
        let source = physical(DataType::List(source_field.clone()), false);
        let target = physical(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        );
        let intermediate = conversion_intermediate_type(&source, &target)
            .unwrap()
            .unwrap();
        let DataType::List(field) = &intermediate.data_type else {
            panic!("list");
        };
        assert_eq!(field.name(), "provider-item");
        assert!(!field.is_nullable());
        assert_eq!(field.metadata().get("source").unwrap(), "user-column");
        assert!(!field.metadata().contains_key(NR_LOGICAL_TYPE_KEY));
        assert!(!intermediate.nullable);
        freeze(source, &intermediate, JSON_TEXT);
        assert!(novarocks_type_contract::preserves_nested_logical_identity(
            &intermediate.data_type,
            &target.data_type
        ));
        assert!(arrow_cast::can_cast_types(
            &intermediate.data_type,
            &target.data_type
        ));
        assert!(
            conversion_intermediate_type(&intermediate, &target)
                .unwrap()
                .is_none()
        );
        assert!(
            conversion_intermediate_type(
                &physical(DataType::Utf8, true),
                &physical(DataType::Utf8, false)
            )
            .is_err(),
            "the same-domain None branch cannot narrow source nullability"
        );
    }

    #[test]
    fn nonliteral_null_lift_has_an_exact_eager_strict_owner_binding() {
        let source = physical(DataType::Null, true);
        for target in [
            logical(DataType::Utf8, true, ValueLogicalType::Json),
            physical(DataType::List(json_field_type(true, "item", false)), true),
        ] {
            assert_eq!(
                conversion_intermediate_type(&source, &target).unwrap(),
                Some(target.clone())
            );
            freeze(source.clone(), &target, NULL_LIFT);
            assert!(
                resolve_value_conversion(
                    &source,
                    &FunctionValueType {
                        nullable: false,
                        ..target
                    },
                    crate::binding_test_control()
                )
                .is_err()
            );
        }
        assert!(
            conversion_intermediate_type(
                &physical(DataType::Null, false),
                &physical(DataType::Int64, true)
            )
            .is_err()
        );
    }
}
