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

//! Exact scalar signature emission through the existing binding field body.
use super::ExpressionEncodingContext;
use crate::physical_binding_v2::{
    ArgumentTypeIds, BindingCodecError, BindingEmission, BindingSource, FunctionBindingInput,
    ResultTypeIds, emit_binding_definition,
};
use crate::physical_encode::fragment_type_owner::{TypeEncodingResources, TypeEncodingResult};
use crate::{PhysicalEncodeError, host_projection_v2::ProjectionFailure};
use novarocks_physical_plan::{BoundFunction, ExprId, PhysicalCallDefinition};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{FunctionArgumentType, FunctionKind};

impl<H> From<BindingCodecError> for ProjectionFailure<PhysicalEncodeError, H> {
    fn from(error: BindingCodecError) -> Self {
        Self::Codec(match error {
            BindingCodecError::Control(cause) => PhysicalEncodeError::Control(cause),
            error => PhysicalEncodeError::Invalid(error.to_string()),
        })
    }
}
struct Emission<'a, 'c, H>(&'a mut TypeEncodingResources<'c, H>);
impl<H> BindingEmission for Emission<'_, '_, H> {
    type Error = ProjectionFailure<PhysicalEncodeError, H>;
    fn vector<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error> {
        self.0.vector(count)
    }
    fn string(&mut self, source: &str) -> Result<String, Self::Error> {
        self.0.copy_string(source)
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.work.step().map_err(Into::into)
    }
}
/// Every referenced type is borrowed from the actual fragment owner. Identity
/// spelling, selected arguments and result are not derived from runtime tags.
pub(super) fn encode_frozen_scalar<H>(
    definition: ExprId,
    function: &BoundFunction,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<(wire::FunctionBindingDefinition, u32), H> {
    if function.kind != FunctionKind::Scalar {
        return Err("ordinary FunctionCall has a non-scalar frozen owner".into());
    }
    let types = context
        .types
        .ok_or("frozen scalar lacks its complete type owner")?;
    let request =
        types.request_reference(PhysicalCallDefinition::Expression(definition), resources)?;
    let mut parameter_ids = resources.vector(function.argument_types.len())?;
    for argument in &function.argument_types {
        let mut parameters = Vec::new();
        if let FunctionArgumentType::Lambda {
            parameter_types, ..
        } = argument
        {
            parameters = resources.vector(parameter_types.len())?;
            for ty in parameter_types {
                parameters.push(binding_id(ty, context, resources)?);
            }
        }
        parameter_ids.push(parameters);
        resources.work.step()?;
    }
    let mut arguments = resources.vector(function.argument_types.len())?;
    for (index, argument) in function.argument_types.iter().enumerate() {
        arguments.push(match argument {
            FunctionArgumentType::Value(ty) => {
                ArgumentTypeIds::Value(binding_id(ty, context, resources)?)
            }
            FunctionArgumentType::Lambda { result_type, .. } => ArgumentTypeIds::Lambda {
                parameters: &parameter_ids[index],
                result: binding_id(result_type, context, resources)?,
            },
        });
        resources.work.step()?;
    }
    let input = FunctionBindingInput {
        id: definition.get(),
        source: BindingSource::Scalar(function),
        arguments: &arguments,
        result: ResultTypeIds::Scalar(binding_id(&function.result_type, context, resources)?),
    };
    let binding = emit_binding_definition(&input, &mut Emission(resources))?;
    Ok((binding, request))
}
fn binding_id<H>(
    ty: &novarocks_type_contract::FunctionValueType,
    context: ExpressionEncodingContext<'_>,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<u32, H> {
    context
        .type_reference(ty, resources)?
        .value_binding_id
        .ok_or_else(|| "frozen scalar type is outside its complete namespace".into())
}
