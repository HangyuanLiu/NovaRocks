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

use super::*;
use crate::borrowed_type_resources::verify_type_binding;
use novarocks_type_contract::{
    CompileControlError, FunctionArgumentType, FunctionKind, FunctionValueType,
};
use std::{alloc::Layout, mem::size_of};

fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
pub(super) fn add(left: usize, right: usize) -> Result<usize, BindingCodecError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("binding projection arithmetic overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, BindingCodecError> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("binding projection arithmetic overflow"))
}
fn names<'a>(source: BindingSource<'a>) -> (&'a str, &'a str) {
    match source {
        BindingSource::Scalar(value) => (value.function_id.as_str(), value.overload.as_str()),
        BindingSource::Table(value) => (value.function_id.as_str(), value.overload.as_str()),
    }
}
fn arguments<'a>(source: BindingSource<'a>) -> &'a [FunctionArgumentType] {
    match source {
        BindingSource::Scalar(value) => &value.argument_types,
        BindingSource::Table(value) => &value.argument_types,
    }
}
fn request<T>(count: usize, facts: &mut BindingProjectionFacts) -> Result<(), BindingCodecError> {
    let bytes = Layout::array::<T>(count)
        .map_err(|_| invalid("binding output layout is unrepresentable"))?
        .size();
    facts.request_bytes_upper_bound = add(facts.request_bytes_upper_bound, bytes)?;
    if bytes != 0 {
        facts.allocation_requests_upper_bound = add(facts.allocation_requests_upper_bound, 1)?;
    }
    Ok(())
}
fn preflight(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    source: usize,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    if inputs.len() > limits.max_definitions {
        return Err(invalid("binding definition count exceeds its envelope"));
    }
    let mut facts = BindingProjectionFacts {
        definition_count: inputs.len(),
        type_reference_count: 0,
        allocation_requests_upper_bound: 0,
        request_bytes_upper_bound: 0,
        coexisting_source_and_request_bytes_upper_bound: 0,
        cumulative_work_upper_bound: 0,
    };
    let root_bytes = Layout::array::<FunctionBindingInput<'_>>(inputs.len())
        .map_err(|_| invalid("binding source layout is unrepresentable"))?
        .size();
    if source < root_bytes {
        return Err(invalid("binding source invoice omits original input roots"));
    }
    // Admit fixed header/tail work and the original definition walk before it
    // starts. Empty tables still perform bounded projection bookkeeping.
    let own_prefix = add(128, mul(inputs.len(), 12)?)?;
    if own_prefix > limits.max_work {
        return Err(invalid("binding work exceeds its envelope"));
    }
    request::<wire::FunctionBindingDefinition>(inputs.len(), &mut facts)?;
    let mut previous = None;
    let mut name_chunks = 0usize;
    for input in inputs {
        let ordered = previous.is_none_or(|id| id < input.id);
        previous = Some(input.id);
        work.step()?;
        if !ordered {
            return Err(invalid("binding input IDs must be unique and ascending"));
        }
        let args = arguments(input.source);
        if args.len() != input.arguments.len() {
            return Err(invalid("binding argument ID shape differs from its source"));
        }
        if matches!(input.source, BindingSource::Scalar(value) if value.kind == FunctionKind::Table)
        {
            return Err(invalid("scalar binding source has table kind"));
        }
        let (function, overload) = names(input.source);
        let identity_bytes = add(function.len(), overload.len())?;
        // Reused source owners/ID slices can alias. Only actual root-slice
        // storage plus a known individual backing lower bound is required;
        // summing every aliased source occurrence would invent retention.
        let mut known = match input.source {
            BindingSource::Scalar(_) => size_of::<BoundFunction>(),
            BindingSource::Table(value) => add(
                size_of::<BoundTableFunction>(),
                Layout::array::<FunctionValueType>(value.result_types.len())
                    .map_err(|_| invalid("binding result source layout is unrepresentable"))?
                    .size(),
            )?,
        };
        known = add(
            add(known, identity_bytes)?,
            Layout::array::<FunctionArgumentType>(args.len())
                .map_err(|_| invalid("binding argument source layout is unrepresentable"))?
                .size(),
        )?;
        let ids_bytes = Layout::array::<ArgumentTypeIds<'_>>(input.arguments.len())
            .map_err(|_| invalid("binding argument source layout is unrepresentable"))?
            .size();
        let mut u32_ids_bytes = 0usize;
        for name in [function, overload] {
            request::<u8>(name.len(), &mut facts)?;
            name_chunks = add(name_chunks, name.len().div_ceil(1024))?;
        }
        request::<wire::FunctionArgumentType>(args.len(), &mut facts)?;
        for (argument, ids) in args.iter().zip(input.arguments) {
            let references = match (argument, ids) {
                (FunctionArgumentType::Value(_), ArgumentTypeIds::Value(_)) => 1,
                (
                    FunctionArgumentType::Lambda {
                        parameter_types, ..
                    },
                    ArgumentTypeIds::Lambda { parameters, .. },
                ) if parameter_types.len() == parameters.len() => {
                    known = add(
                        known,
                        Layout::array::<FunctionValueType>(parameter_types.len())
                            .map_err(|_| {
                                invalid("binding lambda source layout is unrepresentable")
                            })?
                            .size(),
                    )?;
                    u32_ids_bytes = u32_ids_bytes.max(
                        Layout::array::<u32>(parameters.len())
                            .map_err(|_| invalid("binding lambda ID layout is unrepresentable"))?
                            .size(),
                    );
                    request::<u32>(parameters.len(), &mut facts)?;
                    add(parameters.len(), 1)?
                }
                _ => return Err(invalid("binding argument ID shape differs from its source")),
            };
            facts.type_reference_count = add(facts.type_reference_count, references)?;
            work.step()?;
            if facts.type_reference_count > limits.max_type_references {
                return Err(invalid("binding type references exceed their envelope"));
            }
        }
        let results = match (input.source, input.result) {
            (BindingSource::Scalar(_), ResultTypeIds::Scalar(_)) => 1,
            (BindingSource::Table(value), ResultTypeIds::Relation(ids))
                if value.result_types.len() == ids.len() =>
            {
                u32_ids_bytes = u32_ids_bytes.max(
                    Layout::array::<u32>(ids.len())
                        .map_err(|_| invalid("binding relation ID layout is unrepresentable"))?
                        .size(),
                );
                request::<u32>(ids.len(), &mut facts)?;
                ids.len()
            }
            _ => return Err(invalid("binding result ID shape differs from its source")),
        };
        facts.type_reference_count = add(facts.type_reference_count, results)?;
        work.step()?;
        // Root inputs, owned signature storage and typed ID slices are
        // distinct live storage. The u32 slices can alias one another, so
        // only their maximum known extent is added for this source.
        if source < add(add(root_bytes, known)?, add(ids_bytes, u32_ids_bytes)?)? {
            return Err(invalid(
                "binding source invoice omits original signature backing",
            ));
        }
    }
    if facts.type_reference_count > limits.max_type_references {
        return Err(invalid("binding type references exceed their envelope"));
    }
    if facts.request_bytes_upper_bound > limits.max_request_bytes
        || facts.allocation_requests_upper_bound > limits.max_allocation_requests
    {
        return Err(invalid("binding output requests exceed their envelope"));
    }
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.request_bytes_upper_bound)?;
    if facts.coexisting_source_and_request_bytes_upper_bound
        > limits.max_coexisting_source_and_request_bytes
    {
        return Err(invalid("binding coexistence exceeds its envelope"));
    }
    // Model both owned passes and every linear type-root lookup before any
    // output reservation. Opaque allocation/copy work is conservatively
    // included by requested bytes; it is not claimed cooperative internally.
    let lookups = mul(facts.type_reference_count, types.source_counts().0)?;
    let owned = add(own_prefix, mul(facts.type_reference_count, 8)?)?;
    let copies = add(
        mul(facts.request_bytes_upper_bound, 4)?,
        facts.allocation_requests_upper_bound,
    )?;
    facts.cumulative_work_upper_bound =
        add(add(lookups, owned)?, add(copies, mul(name_chunks, 2)?)?)?;
    if facts.cumulative_work_upper_bound > limits.max_work {
        return Err(invalid("binding work exceeds its envelope"));
    }
    Ok(facts)
}
fn verify_id(
    types: &EncodedTypeTable<'_>,
    id: u32,
    expected: &FunctionValueType,
    source: usize,
    limits: BindingProjectionLimits,
    facts: &mut BindingProjectionFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let actual = types
        .value_type_observed(id, work)?
        .ok_or_else(|| invalid("binding value type ID is absent"))?;
    let remaining = limits
        .max_work
        .checked_sub(facts.cumulative_work_upper_bound)
        .ok_or_else(|| invalid("binding work exceeds its envelope"))?;
    let verified = verify_type_binding(expected, actual, source, remaining, work)?;
    facts.cumulative_work_upper_bound = add(
        facts.cumulative_work_upper_bound,
        verified.work_upper_bound(),
    )?;
    if !verified.matches() {
        return Err(invalid(
            "binding value type differs from its original source",
        ));
    }
    Ok(())
}
fn validate(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    source: usize,
    limits: BindingProjectionLimits,
    facts: &mut BindingProjectionFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    for input in inputs {
        for (argument, ids) in arguments(input.source).iter().zip(input.arguments) {
            match (argument, ids) {
                (FunctionArgumentType::Value(value), ArgumentTypeIds::Value(id)) => {
                    verify_id(types, *id, value, source, limits, facts, work)?
                }
                (
                    FunctionArgumentType::Lambda {
                        parameter_types,
                        result_type,
                    },
                    ArgumentTypeIds::Lambda { parameters, result },
                ) => {
                    for (value, id) in parameter_types.iter().zip(*parameters) {
                        verify_id(types, *id, value, source, limits, facts, work)?;
                    }
                    verify_id(types, *result, result_type, source, limits, facts, work)?;
                }
                _ => return Err(invalid("binding argument ID shape differs from its source")),
            }
            work.step()?;
        }
        match (input.source, input.result) {
            (BindingSource::Scalar(value), ResultTypeIds::Scalar(id)) => {
                verify_id(types, id, &value.result_type, source, limits, facts, work)?
            }
            (BindingSource::Table(value), ResultTypeIds::Relation(ids)) => {
                for (value, id) in value.result_types.iter().zip(ids) {
                    verify_id(types, *id, value, source, limits, facts, work)?;
                }
            }
            _ => return Err(invalid("binding result ID shape differs from its source")),
        }
        work.step()?;
    }
    Ok(())
}
fn vector<T>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, BindingCodecError> {
    work.flush()?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(count)
        .map_err(|_| BindingCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(value)
}
fn string(source: &str, work: &mut CompileCheckpoints<'_>) -> Result<String, BindingCodecError> {
    work.flush()?;
    let mut output = String::new();
    output
        .try_reserve_exact(source.len())
        .map_err(|_| BindingCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    // Identity owners enforce at most 1024 UTF-8 bytes. The complete
    // copy is therefore one bounded operation, bracketed on this meter.
    output.push_str(source);
    work.step()?;
    work.flush()?;
    Ok(output)
}
pub(super) fn encode(
    types: &EncodedTypeTable<'_>,
    inputs: &[FunctionBindingInput<'_>],
    source: usize,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<wire::FunctionBindingDefinition>, BindingProjectionFacts), BindingCodecError> {
    let mut facts = preflight(types, inputs, source, limits, work)?;
    validate(types, inputs, source, limits, &mut facts, work)?;
    let mut output = vector(inputs.len(), work)?;
    for input in inputs {
        let mut args = vector(input.arguments.len(), work)?;
        for ids in input.arguments {
            let kind = match ids {
                ArgumentTypeIds::Value(id) => wire::function_argument_type::Kind::ValueTypeId(*id),
                ArgumentTypeIds::Lambda { parameters, result } => {
                    let mut ids = vector(parameters.len(), work)?;
                    for id in *parameters {
                        ids.push(*id);
                        work.step()?;
                    }
                    wire::function_argument_type::Kind::Lambda(wire::LambdaArgumentType {
                        parameter_value_type_ids: ids,
                        result_value_type_id: Some(*result),
                    })
                }
            };
            args.push(wire::FunctionArgumentType { kind: Some(kind) });
            work.step()?;
        }
        let result = match input.result {
            ResultTypeIds::Scalar(id) => {
                wire::function_binding_definition::Result::ScalarValueTypeId(id)
            }
            ResultTypeIds::Relation(ids) => {
                let mut values = vector(ids.len(), work)?;
                for id in ids {
                    values.push(*id);
                    work.step()?;
                }
                wire::function_binding_definition::Result::Relation(wire::RelationResultTypes {
                    value_type_ids: values,
                })
            }
        };
        let kind = match input.source {
            BindingSource::Scalar(value) => match value.kind {
                FunctionKind::Scalar => wire::FunctionKind::Scalar,
                FunctionKind::Aggregate => wire::FunctionKind::Aggregate,
                FunctionKind::Window => wire::FunctionKind::Window,
                FunctionKind::Table => return Err(invalid("scalar binding source has table kind")),
            },
            BindingSource::Table(_) => wire::FunctionKind::Table,
        };
        let (function, overload) = names(input.source);
        // All FIVE legacy fields are deliberately excluded: volatility,
        // argument_evaluation, failure_behavior, intrinsic_row_error and
        // semantic_parameters belong to migration/occurrence owners.
        output.push(wire::FunctionBindingDefinition {
            id: input.id,
            function_id: string(function, work)?,
            overload_id: string(overload, work)?,
            kind: kind as i32,
            arguments: args,
            result: Some(result),
        });
        work.step()?;
    }
    Ok((output, facts))
}

pub(crate) fn verify_scalar_signature(
    left: &BoundFunction,
    right: &BoundFunction,
    source: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedSignature, BindingCodecError> {
    let result = verify_signature_inner(left, right, source, max_work, work);
    if matches!(&result, Err(BindingCodecError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}
fn verify_signature_inner(
    left: &BoundFunction,
    right: &BoundFunction,
    source: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<VerifiedSignature, BindingCodecError> {
    let same = std::ptr::eq(left, right);
    work.step()?;
    let headers = if same {
        size_of::<BoundFunction>()
    } else {
        mul(2, size_of::<BoundFunction>())?
    };
    if source < headers {
        return Err(invalid(
            "binding source invoice omits original signature roots",
        ));
    }
    if max_work == 0 {
        return Err(invalid("binding signature work exceeds its envelope"));
    }
    if same {
        return Ok(VerifiedSignature {
            matches: true,
            work: 1,
        });
    }
    let mut bound = 8usize;
    for name in [
        left.function_id.as_str(),
        right.function_id.as_str(),
        left.overload.as_str(),
        right.overload.as_str(),
    ] {
        bound = add(bound, name.len())?;
    }
    for argument in left
        .argument_types
        .iter()
        .chain(right.argument_types.iter())
    {
        let count = match argument {
            FunctionArgumentType::Value(_) => 1,
            FunctionArgumentType::Lambda {
                parameter_types, ..
            } => add(parameter_types.len(), 1)?,
        };
        bound = add(bound, mul(count, 4)?)?;
        work.step()?;
    }
    if bound > max_work {
        return Err(invalid("binding signature work exceeds its envelope"));
    }
    let mut facts = VerifiedSignature {
        matches: false,
        work: bound,
    };
    for (left, right) in [
        (left.function_id.as_str(), right.function_id.as_str()),
        (left.overload.as_str(), right.overload.as_str()),
    ] {
        // Both identity owners already bounded each complete UTF-8 byte
        // comparison to 1024. No formatting or payload hashing is used.
        let equal = left == right;
        work.step()?;
        if !equal {
            return Ok(facts);
        }
    }
    let same_shape =
        left.kind == right.kind && left.argument_types.len() == right.argument_types.len();
    work.step()?;
    if !same_shape {
        return Ok(facts);
    }
    let mut compare =
        |left: &FunctionValueType, right: &FunctionValueType| -> Result<bool, BindingCodecError> {
            let remaining = max_work
                .checked_sub(facts.work)
                .ok_or_else(|| invalid("binding signature work exceeds its envelope"))?;
            let compared = verify_type_binding(left, right, source, remaining, work)?;
            facts.work = add(facts.work, compared.work_upper_bound())?;
            Ok(compared.matches())
        };
    for (left, right) in left.argument_types.iter().zip(&right.argument_types) {
        match (left, right) {
            (FunctionArgumentType::Value(left), FunctionArgumentType::Value(right)) => {
                if !compare(left, right)? {
                    return Ok(facts);
                }
            }
            (
                FunctionArgumentType::Lambda {
                    parameter_types: left,
                    result_type: lr,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: right,
                    result_type: rr,
                },
            ) => {
                if left.len() != right.len() {
                    return Ok(facts);
                }
                for (left, right) in left.iter().zip(right) {
                    if !compare(left, right)? {
                        return Ok(facts);
                    }
                }
                if !compare(lr, rr)? {
                    return Ok(facts);
                }
            }
            _ => return Ok(facts),
        }
    }
    if !compare(&left.result_type, &right.result_type)? {
        return Ok(facts);
    }
    facts.matches = true;
    Ok(facts)
}
