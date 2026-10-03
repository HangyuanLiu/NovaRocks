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

//! Aggregate definitions borrowed from the same type and function emissions.
//! Occurrence effects, sequence closure and whole-package coverage remain with
//! their existing owners. Numerical envelopes are not formal MEM grants.

use crate::{
    borrowed_type_resources::verify_type_binding,
    physical_binding_v2::{
        BindingCodecError, BindingProjectionFacts, BindingProjectionLimits,
        EncodedFunctionBindings, FunctionBindingInput, verify_scalar_signature,
    },
    physical_type_v2::EncodedTypeTable,
};
use novarocks_physical_plan::{AggregateBinding, AggregatePhase, BoundFunction};
use novarocks_proto_models::{physical_control_v2, physical_package_v2 as wire};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, FunctionKind, PureCompileControl};
use std::{alloc::Layout, mem::size_of};

mod read;
pub use read::{PreparedAggregateBindingHeaders, prepare_aggregate_binding_headers};

#[derive(Clone, Copy)]
pub struct AggregateBindingInput<'source> {
    pub id: u32,
    pub source: &'source AggregateBinding,
    pub function_binding_id: u32,
    pub intermediate_value_type_id: u32,
}

/// Retains immutable loans to all original sources and their same emissions.
/// Singular zero/MAX references are present values, never absence sentinels.
pub struct EncodedAggregateBindings<'loan, 'source> {
    definitions: Vec<wire::AggregateBindingDefinition>,
    inputs: &'loan [AggregateBindingInput<'source>],
    types: &'loan EncodedTypeTable<'source>,
    _functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    facts: BindingProjectionFacts,
}
impl<'loan, 'source> EncodedAggregateBindings<'loan, 'source> {
    pub fn as_wire(&self) -> &[wire::AggregateBindingDefinition] {
        &self.definitions
    }
    pub fn into_wire(self) -> Vec<wire::AggregateBindingDefinition> {
        self.definitions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    pub(crate) fn type_sources(&self) -> &'loan EncodedTypeTable<'source> {
        self.types
    }
    pub(crate) fn function_sources(&self) -> &'loan EncodedFunctionBindings<'loan, 'source> {
        self._functions
    }
    pub(crate) fn source_counts(&self) -> usize {
        self.inputs.len()
    }
    pub(crate) fn binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source AggregateBinding>, BindingCodecError> {
        for input in self.inputs {
            let matches = input.id == id;
            work.step()?;
            if matches {
                return Ok(Some(input.source));
            }
        }
        Ok(None)
    }
}

pub(crate) struct VerifiedAggregateSignature {
    matches: bool,
    work: usize,
}
impl VerifiedAggregateSignature {
    pub(crate) fn matches(&self) -> bool {
        self.matches
    }
    pub(crate) fn work_upper_bound(&self) -> usize {
        self.work
    }
}

/// Compares original full aggregate facts without projecting legacy effects.
/// The caller admits both original source backings and owns final publication.
pub(crate) fn verify_aggregate_signature(
    left: &AggregateBinding,
    right: &AggregateBinding,
    source: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedAggregateSignature, BindingCodecError> {
    let result = verify_aggregate_inner(left, right, source, max_work, work);
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}
fn verify_aggregate_inner(
    left: &AggregateBinding,
    right: &AggregateBinding,
    source: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedAggregateSignature, BindingCodecError> {
    let roots = if std::ptr::eq(left, right) { 1 } else { 2 };
    let minimum = mul(roots, size_of::<AggregateBinding>())?;
    let covers_roots = source >= minimum;
    work.step()?;
    if !covers_roots {
        return Err(invalid("aggregate source invoice omits original roots"));
    }
    let bound = add(
        4,
        add(
            left.state_format.as_str().len(),
            right.state_format.as_str().len(),
        )?,
    )?;
    if bound > max_work {
        return Err(invalid("aggregate signature work exceeds its envelope"));
    }
    let mut facts = VerifiedAggregateSignature {
        matches: false,
        work: bound,
    };
    let same_headers = left.phase == right.phase
        && left.logical_argument_count == right.logical_argument_count
        && left.state_format.as_str() == right.state_format.as_str();
    // Stable state-format owners bound each complete comparison to 1024 bytes.
    work.step()?;
    if !same_headers {
        return Ok(facts);
    }
    let verified = verify_scalar_signature(
        &left.function,
        &right.function,
        source,
        remaining(max_work, facts.work)?,
        work,
    )?;
    facts.work = add(facts.work, verified.work_upper_bound())?;
    if !verified.matches() {
        return Ok(facts);
    }
    let verified = verify_type_binding(
        &left.intermediate_type,
        &right.intermediate_type,
        source,
        remaining(max_work, facts.work)?,
        work,
    )?;
    facts.work = add(facts.work, verified.work_upper_bound())?;
    facts.matches = verified.matches();
    Ok(facts)
}

pub fn encode_aggregate_bindings<'loan, 'source>(
    types: &'loan EncodedTypeTable<'source>,
    functions: &'loan EncodedFunctionBindings<'loan, 'source>,
    inputs: &'loan [AggregateBindingInput<'source>],
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<EncodedAggregateBindings<'loan, 'source>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode(
        types,
        functions,
        inputs,
        source_retained_bytes,
        limits,
        &mut work,
    );
    if let Err(BindingCodecError::Control(cause)) = &result {
        return Err(BindingCodecError::Control(*cause));
    }
    work.finish()?;
    let (definitions, facts) = result?;
    Ok(EncodedAggregateBindings {
        definitions,
        inputs,
        types,
        _functions: functions,
        facts,
    })
}

fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
fn add(left: usize, right: usize) -> Result<usize, BindingCodecError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("aggregate projection arithmetic overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, BindingCodecError> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("aggregate projection arithmetic overflow"))
}
fn remaining(maximum: usize, used: usize) -> Result<usize, BindingCodecError> {
    maximum
        .checked_sub(used)
        .ok_or_else(|| invalid("aggregate work exceeds its envelope"))
}
fn bytes<T>(count: usize) -> Result<usize, BindingCodecError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid("aggregate projection layout is unrepresentable"))
}
fn request<T>(count: usize, facts: &mut BindingProjectionFacts) -> Result<(), BindingCodecError> {
    let size = bytes::<T>(count)?;
    facts.request_bytes_upper_bound = add(facts.request_bytes_upper_bound, size)?;
    if size != 0 {
        facts.allocation_requests_upper_bound = add(facts.allocation_requests_upper_bound, 1)?;
    }
    Ok(())
}
fn preflight(
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    inputs: &[AggregateBindingInput<'_>],
    source: usize,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    let same_types = std::ptr::eq(types, functions.type_sources());
    work.step()?;
    if !same_types {
        return Err(invalid(
            "aggregate and function bindings use different type emissions",
        ));
    }
    if inputs.len() > limits.max_definitions {
        return Err(invalid("aggregate definition count exceeds its envelope"));
    }
    let table = types.as_wire();
    // Known simultaneous source storage, before any output reservation. These
    // visible namespace floors cannot prove a host's full strings/metadata invoice.
    let visible_wire_floor = add(
        bytes::<AggregateBindingInput<'_>>(inputs.len())?,
        add(
            bytes::<wire::FunctionBindingDefinition>(functions.as_wire().len())?,
            add(
                bytes::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>(
                    table.carriers.capacity(),
                )?,
                add(
                    bytes::<novarocks_proto_models::physical_type_v2::FieldDefinition>(
                        table.fields.capacity(),
                    )?,
                    bytes::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>(
                        table.value_types.capacity(),
                    )?,
                )?,
            )?,
        )?,
    )?;
    let (value_roots, field_roots) = types.source_counts();
    let original_namespace_floor = add(
        bytes::<FunctionBindingInput<'_>>(functions.source_counts())?,
        add(
            bytes::<(u32, novarocks_type_contract::FunctionValueType)>(value_roots)?,
            bytes::<(u32, std::sync::Arc<arrow::datatypes::Field>)>(field_roots)?,
        )?,
    )?;
    let floor = add(visible_wire_floor, original_namespace_floor)?;
    if source < floor {
        return Err(invalid(
            "aggregate source invoice omits borrowed namespace storage",
        ));
    }
    let mut facts = BindingProjectionFacts {
        definition_count: inputs.len(),
        type_reference_count: inputs.len(),
        allocation_requests_upper_bound: 0,
        request_bytes_upper_bound: 0,
        coexisting_source_and_request_bytes_upper_bound: 0,
        cumulative_work_upper_bound: 0,
    };
    request::<wire::AggregateBindingDefinition>(inputs.len(), &mut facts)?;
    let lookups = mul(
        inputs.len(),
        add(types.source_counts().0, functions.source_counts())?,
    )?;
    let base = add(128, add(mul(inputs.len(), 20)?, lookups)?)?;
    if base > limits.max_work {
        return Err(invalid("aggregate work exceeds its envelope"));
    }
    let mut previous = None;
    let mut chunks = 0;
    for input in inputs {
        let ordered = previous.is_none_or(|id| id < input.id);
        previous = Some(input.id);
        let is_aggregate = input.source.function.kind == FunctionKind::Aggregate;
        work.step()?;
        if !ordered {
            return Err(invalid("aggregate input IDs must be unique and ascending"));
        }
        if !is_aggregate {
            return Err(invalid("aggregate source has non-aggregate function kind"));
        }
        let state = input.source.state_format.as_str();
        // Aliased aggregate owners are not summed as independent backing.
        if source < add(size_of::<AggregateBinding>(), state.len())? {
            return Err(invalid(
                "aggregate source invoice omits original aggregate backing",
            ));
        }
        request::<u8>(state.len(), &mut facts)?;
        chunks = add(chunks, state.len().div_ceil(1024))?;
    }
    if facts.type_reference_count > limits.max_type_references {
        return Err(invalid("aggregate type references exceed their envelope"));
    }
    if facts.request_bytes_upper_bound > limits.max_request_bytes
        || facts.allocation_requests_upper_bound > limits.max_allocation_requests
    {
        return Err(invalid("aggregate output requests exceed their envelope"));
    }
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.request_bytes_upper_bound)?;
    if facts.coexisting_source_and_request_bytes_upper_bound
        > limits.max_coexisting_source_and_request_bytes
    {
        return Err(invalid("aggregate coexistence exceeds its envelope"));
    }
    facts.cumulative_work_upper_bound = add(
        base,
        add(
            mul(facts.request_bytes_upper_bound, 4)?,
            add(facts.allocation_requests_upper_bound, mul(chunks, 2)?)?,
        )?,
    )?;
    if facts.cumulative_work_upper_bound > limits.max_work {
        return Err(invalid("aggregate work exceeds its envelope"));
    }
    Ok(facts)
}
fn validate(
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    inputs: &[AggregateBindingInput<'_>],
    source: usize,
    limits: BindingProjectionLimits,
    facts: &mut BindingProjectionFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    for input in inputs {
        let function: &BoundFunction = functions
            .scalar_binding_observed(input.function_binding_id, work)?
            .ok_or_else(|| invalid("aggregate function binding ID is absent or relation-valued"))?;
        let signature = verify_scalar_signature(
            &input.source.function,
            function,
            source,
            remaining(limits.max_work, facts.cumulative_work_upper_bound)?,
            work,
        )?;
        facts.cumulative_work_upper_bound = add(
            facts.cumulative_work_upper_bound,
            signature.work_upper_bound(),
        )?;
        if !signature.matches() {
            return Err(invalid(
                "aggregate function differs from its original source",
            ));
        }
        let intermediate = types
            .value_type_observed(input.intermediate_value_type_id, work)?
            .ok_or_else(|| invalid("aggregate intermediate value type ID is absent"))?;
        let compared = verify_type_binding(
            &input.source.intermediate_type,
            intermediate,
            source,
            remaining(limits.max_work, facts.cumulative_work_upper_bound)?,
            work,
        )?;
        facts.cumulative_work_upper_bound = add(
            facts.cumulative_work_upper_bound,
            compared.work_upper_bound(),
        )?;
        if !compared.matches() {
            return Err(invalid(
                "aggregate intermediate value type differs from its original source",
            ));
        }
        work.step()?;
    }
    Ok(())
}
fn encode(
    types: &EncodedTypeTable<'_>,
    functions: &EncodedFunctionBindings<'_, '_>,
    inputs: &[AggregateBindingInput<'_>],
    source: usize,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        Vec<wire::AggregateBindingDefinition>,
        BindingProjectionFacts,
    ),
    BindingCodecError,
> {
    let mut facts = preflight(types, functions, inputs, source, limits, work)?;
    validate(types, functions, inputs, source, limits, &mut facts, work)?;
    work.flush()?;
    let mut definitions = Vec::new();
    let reserved = definitions.try_reserve_exact(inputs.len());
    crate::allocation_exit_v2::reserve_exit::<BindingCodecError>(reserved, work)?;
    for input in inputs {
        work.flush()?;
        let mut state_format = String::new();
        let reserved = state_format.try_reserve_exact(input.source.state_format.as_str().len());
        crate::allocation_exit_v2::reserve_exit::<BindingCodecError>(reserved, work)?;
        // This identity's sole constructor admits at most 1024 ASCII bytes.
        state_format.push_str(input.source.state_format.as_str());
        work.step()?;
        work.flush()?;
        let kind = match input.source.phase {
            AggregatePhase::Single => {
                wire::aggregate_phase::Kind::Single(physical_control_v2::Empty {})
            }
            AggregatePhase::Partial { sequence } => {
                wire::aggregate_phase::Kind::PartialSequenceId(sequence.get())
            }
            AggregatePhase::Intermediate { sequence } => {
                wire::aggregate_phase::Kind::IntermediateSequenceId(sequence.get())
            }
            AggregatePhase::Final { sequence } => {
                wire::aggregate_phase::Kind::FinalSequenceId(sequence.get())
            }
        };
        definitions.push(wire::AggregateBindingDefinition {
            id: input.id,
            function_binding_id: Some(input.function_binding_id),
            phase: Some(wire::AggregatePhase { kind: Some(kind) }),
            logical_argument_count: input.source.logical_argument_count,
            intermediate_value_type_id: Some(input.intermediate_value_type_id),
            state_format,
        });
        work.step()?;
    }
    Ok((definitions, facts))
}

#[cfg(test)]
#[path = "physical_aggregate_binding_v2/tests.rs"]
mod tests;
