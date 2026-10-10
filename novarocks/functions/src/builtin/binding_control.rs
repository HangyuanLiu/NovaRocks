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

//! Original-request control for the exact builtin binding path.
use crate::{
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingSelection, FunctionResultType, FunctionValueType, KernelFailure,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

pub(super) fn type_error(error: KernelFailure) -> FunctionBindingError {
    match error {
        KernelFailure::Cancelled => CompileControlError::Cancelled.into(),
        KernelFailure::DeadlineExceeded => CompileControlError::DeadlineExceeded.into(),
        KernelFailure::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
        other => FunctionBindingError::InvalidBinding(other.to_string().into()),
    }
}

pub(super) fn scope<T>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, FunctionBindingError>,
) -> Result<T, FunctionBindingError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = body(&mut work);
    // Preserve an originating typed control failure. Ordinary refusals and
    // successful outputs still observe their completed tail before return.
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.control_error().is_some())
    {
        return result;
    }
    work.finish()?;
    result
}

pub(super) fn value_type(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    crate::kernel_input::validate_type_observed(value, work).map_err(type_error)
}

pub(super) fn request_types(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    if request.logical_argument_count > request.arguments.len() {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value { value_type: ty, .. } => value_type(ty, work)?,
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                for parameter in parameter_types {
                    work.step()?;
                    value_type(parameter, work)?;
                }
                value_type(result_type, work)?;
            }
        }
    }
    if let Some(expected) = request.expected_result_type {
        value_type(expected, work)?;
    }
    Ok(())
}

pub(super) fn scalar_types(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<FunctionValueType>, FunctionBindingError> {
    let mut types = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value { value_type, .. } => types.push(value_type.clone()),
            FunctionArgument::Lambda { .. } => {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
    }
    Ok(types)
}

pub(super) fn argument_types(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[FunctionArgumentType]>, FunctionBindingError> {
    let mut result = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        work.step()?;
        result.push(argument.argument_type());
    }
    Ok(result.into_boxed_slice())
}

pub(super) fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    left.exactly_equals_observed::<KernelFailure>(right, || {
        work.step().map_err(crate::kernel_control::compile_failure)
    })
    .map_err(type_error)
}

fn exact_argument(
    left: &FunctionArgumentType,
    right: &FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    work.step()?;
    match (left, right) {
        (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) => exact_type(a, b, work),
        (
            FunctionArgumentType::Lambda {
                parameter_types: a,
                result_type: ar,
            },
            FunctionArgumentType::Lambda {
                parameter_types: b,
                result_type: br,
            },
        ) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !exact_type(a, b, work)? {
                    return Ok(false);
                }
            }
            exact_type(ar, br, work)
        }
        _ => Ok(false),
    }
}

pub(crate) fn same_selection(
    left: &FunctionBindingSelection,
    right: &FunctionBindingSelection,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    if left.overload != right.overload || left.argument_types.len() != right.argument_types.len() {
        return Ok(false);
    }
    for (a, b) in left.argument_types.iter().zip(&right.argument_types) {
        if !exact_argument(a, b, work)? {
            return Ok(false);
        }
    }
    match (&left.result_type, &right.result_type) {
        (FunctionResultType::Scalar(a), FunctionResultType::Scalar(b)) => {
            if !exact_type(a, b, work)? {
                return Ok(false);
            }
        }
        (FunctionResultType::Relation(a), FunctionResultType::Relation(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !exact_type(a, b, work)? {
                    return Ok(false);
                }
            }
        }
        _ => return Ok(false),
    }
    match (&left.aggregate, &right.aggregate) {
        (None, None) => Ok(true),
        (Some(a), Some(b)) => Ok(a.state_format == b.state_format
            && a.state_argument_contract == b.state_argument_contract
            && exact_type(&a.intermediate_type, &b.intermediate_type, work)?),
        _ => Ok(false),
    }
}
