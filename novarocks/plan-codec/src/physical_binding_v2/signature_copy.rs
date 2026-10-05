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

use super::{
    BindingCodecError, BindingProjectionLimits, MaterializationModel, add, boxed, completed,
    reserve,
};
use crate::physical_type_v2::clone_value_type_observed;
use novarocks_physical_plan::BoundFunction;
use novarocks_type_contract::{
    CompileCheckpoints, FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionValueType,
};

fn header(
    source: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    let exact = source.legacy_metadata.is_none();
    work.step()?;
    if !exact {
        return Err(BindingCodecError::InvalidShape(
            "signature copy requires an exact binding without legacy metadata",
        ));
    }
    let scalar_result = matches!(
        source.kind,
        FunctionKind::Scalar | FunctionKind::Aggregate | FunctionKind::Window
    );
    work.step()?;
    if !scalar_result {
        return Err(BindingCodecError::InvalidShape(
            "signature copy requires a scalar-result binding kind",
        ));
    }
    Ok(())
}

/// Charge every owned signature occurrence into the caller's original model.
/// The caller lends the binding from the actual materialized namespace and
/// admits the cumulative model before copying; this port does not certify that
/// association, an installed owner, or an allocation grant.
pub(crate) fn preflight_scalar_signature_copy(
    source: &BoundFunction,
    model: &mut MaterializationModel,
    limits: BindingProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    model.check(limits)?;
    header(source, work)?;
    model.items = add(model.items, source.argument_types.len())?;
    model.request::<FunctionArgumentType>(source.argument_types.len(), 2)?;
    model.request::<u8>(source.function_id.as_str().len(), 1)?;
    model.request::<u8>(source.overload.as_str().len(), 1)?;
    model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
    model.check(limits)?;
    // Admit all references and collection requests before the sole type walk.
    for argument in &source.argument_types {
        match argument {
            FunctionArgumentType::Value(_) => {
                model.facts.type_reference_count = add(model.facts.type_reference_count, 1)?;
            }
            FunctionArgumentType::Lambda {
                parameter_types, ..
            } => {
                model.items = add(model.items, parameter_types.len())?;
                model.facts.type_reference_count = add(
                    model.facts.type_reference_count,
                    add(parameter_types.len(), 1)?,
                )?;
                model.request::<FunctionValueType>(parameter_types.len(), 2)?;
            }
        }
        model.check(limits)?;
        work.step()?;
    }
    for argument in &source.argument_types {
        match argument {
            FunctionArgumentType::Value(value) => {
                model.count_owned_type_clone(value, limits, work)?;
            }
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                for parameter in parameter_types {
                    model.count_owned_type_clone(parameter, limits, work)?;
                    work.step()?;
                }
                model.count_owned_type_clone(result_type, limits, work)?;
            }
        }
        work.step()?;
    }
    model.count_owned_type_clone(&source.result_type, limits, work)?;
    work.step()?;
    Ok(())
}

fn copy_type(
    source: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, BindingCodecError> {
    work.flush()?;
    let output = clone_value_type_observed(source, work)?;
    work.step()?;
    work.flush()?;
    Ok(output)
}

/// Copy an already admitted exact signature using the original identity and
/// type authors. The caller must preflight this occurrence in the same model;
/// it owns the ordinary/success footer on the original checkpoint scope.
pub(crate) fn copy_scalar_signature_observed(
    source: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BoundFunction, BindingCodecError> {
    header(source, work)?;
    work.flush()?;
    let function_id = completed(
        FunctionId::try_new(source.function_id.as_str())
            .map_err(|_| BindingCodecError::InvalidShape("copied function identity is invalid")),
        work,
    )?;
    work.flush()?;
    let overload = completed(
        FunctionOverloadId::try_new(source.overload.as_str())
            .map_err(|_| BindingCodecError::InvalidShape("copied overload identity is invalid")),
        work,
    )?;
    work.flush()?;
    let mut arguments = reserve(source.argument_types.len(), work)?;
    for argument in &source.argument_types {
        let copied = match argument {
            FunctionArgumentType::Value(value) => {
                FunctionArgumentType::Value(copy_type(value, work)?)
            }
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                let mut parameters = reserve(parameter_types.len(), work)?;
                for parameter in parameter_types {
                    parameters.push(copy_type(parameter, work)?);
                    work.step()?;
                }
                FunctionArgumentType::Lambda {
                    parameter_types: boxed(parameters, work)?,
                    result_type: copy_type(result_type, work)?,
                }
            }
        };
        arguments.push(copied);
        work.step()?;
    }
    let argument_types = boxed(arguments, work)?;
    let result_type = copy_type(&source.result_type, work)?;
    let output = BoundFunction::from_exact_signature(
        function_id,
        overload,
        source.kind,
        argument_types,
        result_type,
    );
    work.step()?;
    Ok(output)
}

#[cfg(test)]
#[path = "signature_copy_tests.rs"]
mod tests;
