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

//! Owned aggregate signature vocabulary from the original receiving owners.
//! This grants no installed implementation, phase graph, effects or MEM proof.

use super::{
    AggregateBinding, BindingCodecError, BindingProjectionFacts, BindingProjectionLimits,
    PreparedAggregateBindingHeaders, decode_state_argument_contract,
};
use crate::{
    physical_binding_v2::{
        MaterializationModel, MaterializedFunctionBinding, MaterializedFunctionBindings, add,
        boxed, completed, copy_scalar_signature_observed, finish, mul,
        preflight_scalar_signature_copy, reserve,
    },
    physical_type_v2::{DecodedTypeTable, clone_value_type_observed},
    physical_value_origin_v2::decode_phase_with,
};
use novarocks_physical_plan::BoundFunction;
use novarocks_type_contract::{
    AggregateStateFormatId, CompileCheckpoints, CompilePhase, FunctionKind, FunctionValueType,
    PureCompileControl,
};
use std::mem::size_of;

type Error = BindingCodecError;
fn shape(message: &'static str) -> Error {
    Error::InvalidShape(message)
}

/// All seven original aggregate fields, in original definition order. The
/// function signature is an owned copy with explicitly absent legacy metadata.
/// Consumption of definitions ends the loans and does not confer provenance.
pub struct MaterializedAggregateBindings<'loan, 'headers, 'source> {
    definitions: Box<[(u32, AggregateBinding)]>,
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
}
impl<'loan, 'headers, 'source> MaterializedAggregateBindings<'loan, 'headers, 'source> {
    pub fn definitions(&self) -> &[(u32, AggregateBinding)] {
        &self.definitions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    pub(crate) fn headers(&self) -> &'loan PreparedAggregateBindingHeaders<'headers, 'source> {
        self.headers
    }
    pub(crate) fn functions(&self) -> &'loan MaterializedFunctionBindings<'headers, 'source> {
        self.functions
    }
    pub(crate) fn original_control(&self) -> &'source dyn PureCompileControl {
        self.headers.original_control()
    }
    /// The consuming caller admits repeated count-sized lookup work and owns
    /// its entry/footer. Sparse IDs never size a dense allocation.
    pub fn definition_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&AggregateBinding>, Error> {
        let same = std::ptr::eq(work.control(), self.original_control());
        work.step()?;
        if !same {
            return Err(shape(
                "materialized aggregate lookup has a different original control",
            ));
        }
        for (candidate, binding) in &self.definitions {
            let matches = *candidate == id;
            work.step()?;
            if matches {
                return Ok(Some(binding));
            }
        }
        Ok(None)
    }
    pub fn into_definitions(self) -> Box<[(u32, AggregateBinding)]> {
        self.definitions
    }
    /// This owned output only; cumulative Vec-to-Box request bytes are not its
    /// final retained backing. The original source invoice is excluded here.
    pub(crate) fn retained_output_floor(&self) -> Result<usize, Error> {
        add(size_of::<Self>(), self.retained_bytes)
    }
    /// Necessary composition floor, not a complete measured backing or grant.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        add(self.source_invoice, self.retained_output_floor()?)
    }
}

/// Preparation keeps the same aggregate/function/type/control loans through
/// the sole consuming operation. It allocates no output signature.
pub struct PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source> {
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_invoice: usize,
    retained_bytes: usize,
    facts: BindingProjectionFacts,
}
impl PreparedAggregateBindingsMaterialization<'_, '_, '_> {
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
}

fn function<'a>(
    functions: &'a MaterializedFunctionBindings<'_, '_>,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a BoundFunction, Error> {
    let found = functions.definition_observed(id, work)?;
    let result = match found {
        Some(MaterializedFunctionBinding::Scalar(binding))
            if binding.kind == FunctionKind::Aggregate =>
        {
            Ok(binding)
        }
        _ => Err(shape(
            "materialized aggregate function is absent or non-aggregate",
        )),
    };
    completed(result, work)
}
fn value<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    // The original decoded table uses a standard-library BTree lookup. Its
    // admitted comparison ceiling is numerical, not synthetic checkpoints.
    work.flush()?;
    let found = types.value_type(id);
    work.step()?;
    work.flush()?;
    found.ok_or_else(|| shape("materialized aggregate intermediate type is absent"))
}

fn preflight(
    headers: &PreparedAggregateBindingHeaders<'_, '_>,
    functions: &MaterializedFunctionBindings<'_, '_>,
    source: usize,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(BindingProjectionFacts, usize), Error> {
    let same = std::ptr::eq(functions.headers(), headers.functions());
    work.step()?;
    if !same {
        return Err(shape(
            "aggregate materialization has a different original function namespace",
        ));
    }
    // The header invoice already includes the original function/type source.
    // Only the separately owned function output is added to that source once.
    let known = add(
        headers.retained_invoice_floor()?,
        functions.retained_output_floor()?,
    )?;
    let types = headers.functions().type_table();
    // Every aggregate owns at least its intermediate clone and the function's
    // result clone. This per-reference ceiling covers both function linear
    // lookups and both original decoded-type lookups without a second model.
    let lookup = add(
        mul(functions.definitions().len(), 2)?,
        add(types.value_types().len(), 1)?,
    )?;
    let mut model =
        MaterializationModel::for_composition(headers.as_wire().len(), lookup, source, known);
    // Admit one actual intermediate root per definition before the first
    // namespace lookup, including zero-argument aggregates.
    model.facts.type_reference_count = headers.as_wire().len();
    model.request::<(u32, AggregateBinding)>(headers.as_wire().len(), 2)?;
    model.check(limits)?;
    for raw in headers.as_wire() {
        model.request::<u8>(raw.state_format.len(), 1)?;
        model.check(limits)?;
        let function_id = completed(
            raw.function_binding_id
                .ok_or_else(|| shape("materialized aggregate function reference is absent")),
            work,
        )?;
        let source = function(functions, function_id, work)?;
        preflight_scalar_signature_copy(source, &mut model, limits, work)?;
        let id = completed(
            raw.intermediate_value_type_id
                .ok_or_else(|| shape("materialized aggregate intermediate reference is absent")),
            work,
        )?;
        model.count_owned_type_clone(value(types, id, work)?, limits, work)?;
        model.check(limits)?;
        work.step()?;
    }
    Ok((model.facts, model.retained))
}

pub fn prepare_aggregate_bindings_materialization<'loan, 'headers, 'source>(
    headers: &'loan PreparedAggregateBindingHeaders<'headers, 'source>,
    functions: &'loan MaterializedFunctionBindings<'headers, 'source>,
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
) -> Result<PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>, Error> {
    let mut work = CompileCheckpoints::try_new(headers.original_control(), CompilePhase::Decode)?;
    let result = preflight(headers, functions, source_retained_bytes, limits, &mut work).map(
        |(facts, retained_bytes)| PreparedAggregateBindingsMaterialization {
            headers,
            functions,
            source_invoice: source_retained_bytes,
            retained_bytes,
            facts,
        },
    );
    finish(work, result)
}

pub fn materialize_aggregate_bindings<'loan, 'headers, 'source>(
    prepared: PreparedAggregateBindingsMaterialization<'loan, 'headers, 'source>,
) -> Result<MaterializedAggregateBindings<'loan, 'headers, 'source>, Error> {
    let mut work =
        CompileCheckpoints::try_new(prepared.headers.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let types = prepared.headers.functions().type_table();
        let mut definitions = reserve(prepared.headers.as_wire().len(), &mut work)?;
        for raw in prepared.headers.as_wire() {
            let function_id = completed(
                raw.function_binding_id
                    .ok_or_else(|| shape("materialized aggregate function reference is absent")),
                &mut work,
            )?;
            let source = function(prepared.functions, function_id, &mut work)?;
            let function = copy_scalar_signature_observed(source, &mut work)?;
            let intermediate_id = completed(
                raw.intermediate_value_type_id.ok_or_else(|| {
                    shape("materialized aggregate intermediate reference is absent")
                }),
                &mut work,
            )?;
            let source = value(types, intermediate_id, &mut work)?;
            work.flush()?;
            let intermediate_type = clone_value_type_observed(source, &mut work)?;
            work.step()?;
            work.flush()?;
            let phase = completed(
                raw.phase
                    .as_ref()
                    .ok_or_else(|| shape("materialized aggregate phase is absent")),
                &mut work,
            )?;
            let phase = completed(decode_phase_with(phase, shape), &mut work)?;
            let state_argument_contract = completed(
                decode_state_argument_contract(raw.state_argument_contract),
                &mut work,
            )?;
            // Original identity construction owns its bounded (<=1024-byte)
            // grammar and Box allocation. No copied state-format grammar.
            work.flush()?;
            let state_format = completed(
                AggregateStateFormatId::try_new(&raw.state_format)
                    .map_err(|_| shape("materialized aggregate state format is invalid")),
                &mut work,
            )?;
            work.flush()?;
            definitions.push((
                raw.id,
                AggregateBinding {
                    state_argument_contract,
                    function,
                    phase,
                    logical_argument_count: raw.logical_argument_count,
                    intermediate_type,
                    state_format,
                },
            ));
            work.step()?;
        }
        Ok(MaterializedAggregateBindings {
            definitions: boxed(definitions, &mut work)?,
            headers: prepared.headers,
            functions: prepared.functions,
            source_invoice: prepared.source_invoice,
            retained_bytes: prepared.retained_bytes,
            facts: prepared.facts,
        })
    })();
    finish(work, result)
}

#[cfg(test)]
#[path = "materialize_tests.rs"]
mod tests;
