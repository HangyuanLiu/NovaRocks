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

//! Materialize one original neutral request from the admitted pool namespace.
//! The caller owns source/type/allocation admission and entry/completion on the
//! same meter. This is neither physical-shape inference nor pure preparation.

use crate::expressions::ExpressionLoweringError;
#[cfg(test)]
use novarocks_functions::ConstantPolicy;
use novarocks_functions::{FunctionArgument, FunctionBindingRequest, MAX_CALL_EFFECT_ARGUMENTS};
use novarocks_physical_plan::{ConstantPools, PhysicalCallRequest, StaticFunctionArgument};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use std::alloc::Layout;

/// Original source and optional result constraint stay borrowed. Owned argument
/// headers retain admitted constants without reconstructing their backing.
/// Type clones, Vec-to-Box shrink and allocator internals remain caller-admitted
/// opaque operations; checked Layout extents do not grant memory.
#[derive(Debug)]
pub(crate) struct MaterializedCallRequest<'a> {
    source: &'a PhysicalCallRequest,
    arguments: Box<[FunctionArgument]>,
}
impl MaterializedCallRequest<'_> {
    pub(crate) fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.source.logical_argument_count,
            expected_result_type: self.source.expected_result_type.as_ref(),
        }
    }
    #[cfg(test)]
    pub(crate) fn constant_policy(&self) -> ConstantPolicy {
        self.source.constant_policy
    }
}

pub(crate) fn materialize_call_request_observed<'a>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedCallRequest<'a>, ExpressionLoweringError> {
    let count = source.arguments.len();
    if count > MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    Layout::array::<FunctionArgument>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
    let invalid_count = source.logical_argument_count > count;
    work.step()?;
    if invalid_count {
        return Err(ExpressionLoweringError::Invalid(
            "original request logical argument count exceeds channels",
        ));
    }
    // Admit all owned parameter-header extents before the first output reserve.
    // The original package owns complete type/resource validity; this does not
    // duplicate its recursive grammar or invent a second retained-byte budget.
    for argument in &source.arguments {
        if let StaticFunctionArgument::Lambda {
            parameter_types, ..
        } = argument
        {
            if parameter_types.len() > MAX_CALL_EFFECT_ARGUMENTS {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            Layout::array::<FunctionValueType>(parameter_types.len())
                .map_err(|_| CompileControlError::ResourceExhausted)?;
        }
        work.step()?;
    }
    let mut arguments = Vec::new();
    reserve(&mut arguments, count, work)?;
    for argument in &source.arguments {
        let argument = match argument {
            StaticFunctionArgument::Value {
                value_type,
                constant,
            } => {
                let constant = match constant {
                    Some(reference) => {
                        work.flush()?;
                        let value = pools.resolve_observed(*reference, value_type, work)?;
                        work.flush()?;
                        Some(value)
                    }
                    None => None,
                };
                FunctionArgument::Value {
                    value_type: clone_type(value_type, work)?,
                    constant,
                }
            }
            StaticFunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                let mut parameters = Vec::new();
                reserve(&mut parameters, parameter_types.len(), work)?;
                for parameter in parameter_types {
                    parameters.push(clone_type(parameter, work)?);
                    work.step()?;
                }
                FunctionArgument::Lambda {
                    parameter_types: boxed(parameters, work)?,
                    result_type: clone_type(result_type, work)?,
                }
            }
        };
        arguments.push(argument);
        work.step()?;
    }
    Ok(MaterializedCallRequest {
        source,
        arguments: boxed(arguments, work)?,
    })
}

fn clone_type(
    source: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, ExpressionLoweringError> {
    work.flush()?;
    let copied = source.clone();
    work.step()?;
    work.flush()?;
    Ok(copied)
}
fn reserve<T>(
    values: &mut Vec<T>,
    count: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionLoweringError> {
    if count == 0 {
        return Ok(());
    }
    Layout::array::<T>(count).map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    values
        .try_reserve_exact(count)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    Ok(())
}
fn boxed<T>(
    values: Vec<T>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Box<[T]>, ExpressionLoweringError> {
    if values.is_empty() {
        return Ok(Box::default());
    }
    work.flush()?;
    let values = values.into_boxed_slice();
    work.step()?;
    work.flush()?;
    Ok(values)
}

#[cfg(test)]
#[path = "original_requests/tests.rs"]
mod tests;
