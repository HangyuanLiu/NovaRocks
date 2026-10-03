// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{binding_index_v2::BindingIndex, physical_binding_v2::PreparedFunctionBindingHeaders};
use novarocks_type_contract::{AggregateStateFormatId, CompileControlError, FunctionIdentityError};

/// Borrowed receiving facts, not a decoded aggregate or an effect declaration.
/// The same function/type owner and original control remain mandatory. The
/// only new heap backing is the admitted sparse index.
pub struct PreparedAggregateBindingHeaders<'loan, 'source> {
    definitions: &'loan [wire::AggregateBindingDefinition],
    functions: &'loan PreparedFunctionBindingHeaders<'source>,
    index: BindingIndex,
    facts: BindingProjectionFacts,
}
impl<'loan, 'source> PreparedAggregateBindingHeaders<'loan, 'source> {
    pub fn as_wire(&self) -> &'loan [wire::AggregateBindingDefinition] {
        self.definitions
    }
    pub fn functions(&self) -> &'loan PreparedFunctionBindingHeaders<'source> {
        self.functions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    /// Each lookup borrows the original definition and receiving control.
    /// The consuming scope admits its own repeated lookup work.
    pub fn definition(
        &self,
        id: u32,
    ) -> Result<Option<&'loan wire::AggregateBindingDefinition>, BindingCodecError> {
        let mut work =
            CompileCheckpoints::try_new(self.functions.original_control(), CompilePhase::Decode)?;
        let result = self
            .index
            .find(id, |at| self.definitions[at].id, &mut work)?;
        work.finish()?;
        Ok(result.map(|at| &self.definitions[at]))
    }
}
fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, BindingCodecError> {
    a.checked_add(b)
        .ok_or_else(|| invalid("aggregate header arithmetic overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, BindingCodecError> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("aggregate header arithmetic overflow"))
}
fn bytes<T>(count: usize) -> Result<usize, BindingCodecError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| invalid("aggregate header layout is unrepresentable"))
}
fn height(count: usize) -> usize {
    (usize::BITS - count.leading_zeros()) as usize
}
fn source_floor(source: usize, known: usize) -> Result<(), BindingCodecError> {
    if source < known {
        return Err(invalid(
            "aggregate header source invoice omits original backing",
        ));
    }
    Ok(())
}
fn admit_work(
    facts: &mut BindingProjectionFacts,
    state_bytes: usize,
    lookup_work: usize,
    limits: BindingProjectionLimits,
) -> Result<(), BindingCodecError> {
    // The common index performs at most four operations per descended heap
    // level. Its initialization, build/extraction and duplicate pass fit this
    // conservative O(N log N) term. Function lookup uses its original index;
    // type lookup is bounded by the whole value map without private layouts.
    let n = facts.definition_count;
    let own = mul(n, add(64, mul(add(height(n), 1)?, 16)?)?)?;
    facts.cumulative_work_upper_bound = add(
        128,
        add(
            own,
            add(
                mul(n, lookup_work)?,
                add(
                    mul(state_bytes, 4)?,
                    mul(facts.request_bytes_upper_bound, 4)?,
                )?,
            )?,
        )?,
    )?;
    if facts.cumulative_work_upper_bound > limits.max_work {
        return Err(invalid("aggregate header work exceeds its envelope"));
    }
    Ok(())
}
fn preflight(
    definitions: &[wire::AggregateBindingDefinition],
    functions: &PreparedFunctionBindingHeaders<'_>,
    source: usize,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    let n = definitions.len();
    if n > limits.max_definitions {
        return Err(invalid(
            "aggregate header definition count exceeds its envelope",
        ));
    }
    if n > limits.max_type_references {
        return Err(invalid(
            "aggregate header type references exceed their envelope",
        ));
    }
    let requested = bytes::<usize>(n)?;
    let mut facts = BindingProjectionFacts {
        definition_count: n,
        type_reference_count: n,
        allocation_requests_upper_bound: usize::from(requested != 0),
        request_bytes_upper_bound: requested,
        coexisting_source_and_request_bytes_upper_bound: add(source, requested)?,
        cumulative_work_upper_bound: 0,
    };
    if requested > limits.max_request_bytes
        || facts.allocation_requests_upper_bound > limits.max_allocation_requests
        || facts.coexisting_source_and_request_bytes_upper_bound
            > limits.max_coexisting_source_and_request_bytes
    {
        return Err(invalid(
            "aggregate header index requests exceed their envelope",
        ));
    }
    // The previous original source invoice, token and actual index capacity
    // coexist once. Aggregate DTO roots and each owned state String are
    // independent backing; no source invoice is multiplied by lookup count.
    let mut known = add(
        functions.retained_invoice_floor()?,
        bytes::<wire::AggregateBindingDefinition>(n)?,
    )?;
    source_floor(source, known)?;
    let lookup_work = add(
        height(functions.as_wire().len()),
        add(functions.type_table().value_types().len(), 1)?,
    )?;
    let mut state_bytes = 0;
    admit_work(&mut facts, state_bytes, lookup_work, limits)?;
    for definition in definitions {
        known = add(known, definition.state_format.capacity())?;
        state_bytes = add(state_bytes, definition.state_format.len())?;
        source_floor(source, known)?;
        admit_work(&mut facts, state_bytes, lookup_work, limits)?;
        work.step()?;
    }
    Ok(facts)
}
enum StateValidationFailure {
    Identity,
    Control(CompileControlError),
}
impl From<FunctionIdentityError> for StateValidationFailure {
    fn from(_: FunctionIdentityError) -> Self {
        Self::Identity
    }
}
impl From<CompileControlError> for StateValidationFailure {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
fn validate(
    definitions: &[wire::AggregateBindingDefinition],
    functions: &PreparedFunctionBindingHeaders<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    for definition in definitions {
        let function_id = definition
            .function_binding_id
            .ok_or_else(|| invalid("aggregate header function reference is absent"))?;
        let function = functions
            .definition_observed(function_id, work)?
            .ok_or_else(|| invalid("aggregate header function reference is unknown"))?;
        let kind_is_aggregate = function.kind == wire::FunctionKind::Aggregate as i32;
        work.step()?;
        if !kind_is_aggregate {
            return Err(invalid("aggregate header function has non-aggregate kind"));
        }
        let intermediate_id = definition
            .intermediate_value_type_id
            .ok_or_else(|| invalid("aggregate header intermediate type is absent"))?;
        work.flush()?;
        let present = functions.type_table().value_type(intermediate_id).is_some();
        work.step()?;
        work.flush()?;
        if !present {
            return Err(invalid("aggregate header intermediate type is unknown"));
        }
        let phase = definition
            .phase
            .as_ref()
            .ok_or_else(|| invalid("aggregate header phase is absent"))?;
        let phase_present = match phase.kind.as_ref() {
            Some(kind) => match kind {
                wire::aggregate_phase::Kind::Single(_)
                | wire::aggregate_phase::Kind::PartialSequenceId(_)
                | wire::aggregate_phase::Kind::IntermediateSequenceId(_)
                | wire::aggregate_phase::Kind::FinalSequenceId(_) => true,
            },
            None => false,
        };
        work.step()?;
        if !phase_present {
            return Err(invalid("aggregate header phase kind is absent"));
        }
        // The generated oneof already represents all four closed phases.
        // Sequence IDs and logical_argument_count are retained as authored;
        // sequence closure and aggregate channels belong to semantic owners.
        AggregateStateFormatId::validate_str_observed::<StateValidationFailure>(
            &definition.state_format,
            || work.step().map_err(StateValidationFailure::from),
        )
        .map_err(|error| match error {
            StateValidationFailure::Control(cause) => BindingCodecError::Control(cause),
            StateValidationFailure::Identity => invalid("aggregate header state format is invalid"),
        })?;
    }
    Ok(())
}
/// Prepare borrowed receiving headers without materializing legacy effect
/// fields, signature copies or identities. The caller supplies a complete
/// coexisting source invoice and explicit limits; neither is a formal grant.
/// The existing function token supplies the only accepted original control.
pub fn prepare_aggregate_binding_headers<'loan, 'source>(
    definitions: &'loan [wire::AggregateBindingDefinition],
    functions: &'loan PreparedFunctionBindingHeaders<'source>,
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
) -> Result<PreparedAggregateBindingHeaders<'loan, 'source>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(functions.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = preflight(
            definitions,
            functions,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        validate(definitions, functions, &mut work)?;
        let index = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, &mut work)?;
        Ok(PreparedAggregateBindingHeaders {
            definitions,
            functions,
            index,
            facts,
        })
    })();
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests;
