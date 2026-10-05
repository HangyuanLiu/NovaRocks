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

use crate::{
    physical_binding_v2::{
        BindingCodecError, BindingProjectionLimits, MaterializationModel, add, completed,
        copy_scalar_signature_observed, preflight_scalar_signature_copy,
    },
    physical_type_v2::clone_value_type_observed,
};
use novarocks_physical_plan::AggregateBinding;
use novarocks_type_contract::{AggregateStateFormatId, CompileCheckpoints, FunctionKind};

fn header(
    source: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let aggregate = source.function.kind == FunctionKind::Aggregate;
    work.step()?;
    if !aggregate {
        return Err(BindingCodecError::InvalidShape(
            "aggregate copy requires an aggregate signature",
        ));
    }
    let exact = source.function.legacy_metadata.is_none();
    work.step()?;
    if !exact {
        return Err(BindingCodecError::InvalidShape(
            "aggregate copy requires an exact binding without legacy metadata",
        ));
    }
    Ok(())
}

/// Charge this actual owned aggregate occurrence into the caller's sole model.
/// The original namespace association and outer AggregateBinding Box belong to
/// the caller. This port neither authorizes compatibility nor grants resources.
pub(crate) fn preflight_aggregate_binding_copy(
    source: &AggregateBinding,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    model.check(limits)?;
    header(source, work)?;
    // Admit the intermediate root and state identity before any clone grammar.
    // The original signature helper admits all of its own argument/result roots.
    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
    model.request::<u8>(source.state_format.as_str().len(), 1)?;
    model.check(limits)?;
    preflight_scalar_signature_copy(&source.function, model, limits, work)?;
    model.count_owned_type_clone(&source.intermediate_type, limits, work)?;
    work.step()?;
    model.check(limits)
}

/// Copy an already admitted occurrence with the original signature/type and
/// state identity authors. No entry/footer is added to the caller's scope.
pub(crate) fn copy_aggregate_binding_observed(
    source: &AggregateBinding,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AggregateBinding, BindingCodecError> {
    header(source, work)?;
    let function = copy_scalar_signature_observed(&source.function, work)?;
    work.flush()?;
    let intermediate_type = clone_value_type_observed(&source.intermediate_type, work)?;
    work.step()?;
    work.flush()?;
    // AggregateStateFormatId owns the bounded identity grammar and allocation.
    let state_format = completed(
        AggregateStateFormatId::try_new(source.state_format.as_str()).map_err(|_| {
            BindingCodecError::InvalidShape("copied aggregate state format is invalid")
        }),
        work,
    )?;
    work.flush()?;
    let output = AggregateBinding {
        function,
        phase: source.phase,
        logical_argument_count: source.logical_argument_count,
        intermediate_type,
        state_format,
        state_argument_contract: source.state_argument_contract,
    };
    work.step()?;
    Ok(output)
}

#[cfg(test)]
#[path = "signature_copy_tests.rs"]
mod tests;
