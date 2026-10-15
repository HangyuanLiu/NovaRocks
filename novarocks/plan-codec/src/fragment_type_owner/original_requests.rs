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

//! Original request and pool loans for one explicit PlanTree fragment owner.

use super::{
    PreparedFragmentTypes, TypeEncodingResources, TypeEncodingResult, invalid, sort_ord_observed,
};
use novarocks_constant_contract::ConstantPool;
use novarocks_physical_plan::{
    ConstantPoolId, ConstantPools, Fragment, PhysicalCallDefinition, PhysicalCallRequest,
    StaticFunctionArgument as Argument, ValueType,
};
use novarocks_proto_models::call_request as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};

pub(crate) struct PreparedOriginalRequests<'source> {
    entries: Vec<(PhysicalCallDefinition, &'source PhysicalCallRequest)>,
    pub(crate) pools: Vec<(ConstantPoolId, &'source ConstantPool)>,
}
impl<'source> PreparedOriginalRequests<'source> {
    pub(crate) fn prepare<H>(
        fragment: &'source Fragment,
        constants: &'source ConstantPools,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        let mut entries = resources.vector(fragment.call_requests().entries().len())?;
        let mut count = 0usize;
        for (definition, request) in fragment.call_requests().entries() {
            resources.work.step()?;
            entries.push((*definition, request));
            for argument in &request.arguments {
                resources.work.step()?;
                if matches!(
                    argument,
                    Argument::Value {
                        constant: Some(_),
                        ..
                    }
                ) {
                    count = count
                        .checked_add(1)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                }
            }
        }
        if entries.len() > u32::MAX as usize {
            return Err(invalid("original request identities are unrepresentable"));
        }
        for (_, expression) in fragment.expressions().iter() {
            resources.work.step()?;
            if matches!(
                expression.kind,
                novarocks_physical_plan::ExprKind::Constant(_)
            ) {
                count = count
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?;
            }
        }
        let mut references = resources.vector(count)?;
        for (_, request) in &entries {
            for argument in &request.arguments {
                resources.work.step()?;
                if let Argument::Value {
                    constant: Some(reference),
                    ..
                } = argument
                {
                    references.push(reference.pool);
                }
            }
        }
        // Every actual constant definition has one admitted source pool,
        // including definitions with no function-request occurrence.
        for (_, expression) in fragment.expressions().iter() {
            resources.work.step()?;
            if let novarocks_physical_plan::ExprKind::Constant(reference) = expression.kind {
                references.push(reference.pool);
            }
        }
        sort_ord_observed(&mut references, &mut resources.work)?;
        let mut distinct = 0usize;
        for index in 0..references.len() {
            resources.work.step()?;
            if distinct == 0 || references[index] != references[distinct - 1] {
                references[distinct] = references[index];
                distinct += 1;
            }
        }
        references.truncate(distinct);
        let mut pools = resources.vector(references.len())?;
        for id in references {
            resources.work.flush()?;
            let pool = constants.entries().get(&id);
            resources.work.step()?;
            resources.work.flush()?;
            let pool = pool.ok_or_else(|| {
                invalid("original request constant pool is absent from its source owner")
            })?;
            pools.push((id, pool));
        }
        Ok(Self { entries, pools })
    }
    pub(crate) fn emit_pure_constants<H>(
        &self,
        types: &PreparedFragmentTypes<'_>,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<
        (
            Vec<wire::IpcConstantPool>,
            novarocks_proto_models::physical_type_v2::SourceFieldTable,
        ),
        H,
    > {
        // The explicit Host signature currently carries only exact type layouts,
        // not IPC's opaque library construction observer. It must never silently
        // use the FE pure writer as an admission substitute.
        if !self.pools.is_empty()
            && matches!(
                resources.type_policy,
                super::ExecutionTypeEncodingPolicy::Explicit(_)
            )
        {
            return Err(invalid(
                "constant IPC encoding requires its explicit construction observer",
            ));
        }
        let mut fields = resources.vector(self.pools.len())?;
        for (id, pool) in &self.pools {
            resources.work.step()?;
            fields.push((id.get(), pool.field_ref()));
        }
        let profile =
            crate::physical_type_v2::TypeProjectionLimits::for_validated_source_root_count(
                fields.len(),
            )?;
        let source = crate::physical_type_v2::encode_source_field_table_pure_in(
            &fields,
            profile,
            &mut resources.work,
        )
        .map_err(type_failure)?;
        let mut records = resources.vector(self.pools.len())?;
        for (id, pool) in &self.pools {
            resources.work.step()?;
            let value_id = binding_id(types, pool.value_type(), resources)?;
            let prepared = crate::physical_constant_v2::prepare_constant_record_write_pure_in(
                *id,
                value_id,
                id.get(),
                pool,
                &mut resources.work,
            )
            .map_err(constant_failure)?;
            records.push(
                prepared
                    .emit_in(&mut resources.work)
                    .map_err(constant_failure)?,
            );
        }
        Ok((records, source))
    }
    pub(crate) fn visit_types<H>(
        &self,
        work: &mut CompileCheckpoints<'_>,
        emit: &mut impl FnMut(
            &'source ValueType,
            &mut CompileCheckpoints<'_>,
        ) -> TypeEncodingResult<(), H>,
    ) -> TypeEncodingResult<(), H> {
        for (_, request) in &self.entries {
            work.step()?;
            for argument in &request.arguments {
                work.step()?;
                match argument {
                    Argument::Value { value_type: ty, .. } => emit(ty, work)?,
                    Argument::Lambda {
                        parameter_types,
                        result_type,
                    } => {
                        for ty in parameter_types {
                            work.step()?;
                            emit(ty, work)?;
                        }
                        emit(result_type, work)?;
                    }
                }
            }
            if let Some(value) = &request.expected_result_type {
                emit(value, work)?;
            }
        }
        for (_, pool) in &self.pools {
            work.step()?;
            emit(pool.value_type(), work)?;
        }
        Ok(())
    }
    pub(crate) fn reference<H>(
        &self,
        definition: PhysicalCallDefinition,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<u32, H> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            resources.work.step()?;
            let middle = low + (high - low) / 2;
            if self.entries[middle].0 < definition {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        resources.work.step()?;
        if self
            .entries
            .get(low)
            .is_none_or(|entry| entry.0 != definition)
        {
            return Err(invalid("aggregate has no frozen original binding request"));
        }
        u32::try_from(low).map_err(|_| invalid("original request identity is unrepresentable"))
    }
    pub(crate) fn emit<H>(
        &self,
        types: &PreparedFragmentTypes<'_>,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<wire::FragmentCallRequests, H> {
        let mut entries = resources.vector(self.entries.len())?;
        for (ordinal, (definition, request)) in self.entries.iter().enumerate() {
            resources.work.step()?;
            let kind = match definition {
                PhysicalCallDefinition::Expression(id) => {
                    wire::call_request_definition::Kind::ExpressionDefinitionId(id.get())
                }
                PhysicalCallDefinition::Relational(site) => {
                    wire::call_request_definition::Kind::Relational(
                        crate::physical_semantics_v2::encode_call_site(*site),
                    )
                }
            };
            let mut arguments = resources.vector(request.arguments.len())?;
            for argument in &request.arguments {
                resources.work.step()?;
                let kind = match argument {
                    Argument::Value {
                        value_type: ty,
                        constant,
                    } => {
                        wire::original_function_argument::Kind::Value(wire::OriginalValueArgument {
                            value_type_id: Some(binding_id(types, ty, resources)?),
                            constant: constant.map(|r| wire::ConstantReference {
                                pool_id: Some(r.pool.get()),
                                row_ordinal: r.ordinal,
                            }),
                        })
                    }
                    Argument::Lambda {
                        parameter_types,
                        result_type,
                    } => {
                        let mut parameter_value_type_ids =
                            resources.vector(parameter_types.len())?;
                        for ty in parameter_types {
                            resources.work.step()?;
                            parameter_value_type_ids.push(binding_id(types, ty, resources)?);
                        }
                        wire::original_function_argument::Kind::Lambda(wire::LambdaArgumentType {
                            parameter_value_type_ids,
                            result_value_type_id: Some(binding_id(types, result_type, resources)?),
                        })
                    }
                };
                arguments.push(wire::OriginalFunctionArgument { kind: Some(kind) });
            }
            entries.push(wire::OriginalCallRequest {
                definition: Some(wire::CallRequestDefinition { kind: Some(kind) }),
                arguments,
                logical_argument_count: Some(
                    u32::try_from(request.logical_argument_count).map_err(|_| {
                        invalid("original logical argument count is unrepresentable")
                    })?,
                ),
                expected_result_value_type_id: request
                    .expected_result_type
                    .as_ref()
                    .map(|ty| binding_id(types, ty, resources))
                    .transpose()?,
                constant_policy: Some(crate::physical_call_requests_v2::encode_source_policy(
                    request.constant_policy,
                )),
                request_id: Some(
                    u32::try_from(ordinal)
                        .map_err(|_| invalid("original request identity is unrepresentable"))?,
                ),
            });
        }
        Ok(wire::FragmentCallRequests { entries })
    }
}
fn binding_id<H>(
    types: &PreparedFragmentTypes<'_>,
    ty: &ValueType,
    resources: &mut TypeEncodingResources<'_, H>,
) -> TypeEncodingResult<u32, H> {
    types
        .reference::<crate::host_projection_v2::ProjectionFailure<crate::PhysicalEncodeError, H>>(
            ty,
            || resources.work.step().map_err(Into::into),
        )?
        .value_binding_id
        .ok_or_else(|| invalid("original request type has no complete fragment binding"))
}

fn type_failure<H>(
    error: crate::physical_type_v2::TypeCodecError,
) -> crate::host_projection_v2::ProjectionFailure<crate::PhysicalEncodeError, H> {
    match error {
        crate::physical_type_v2::TypeCodecError::Control(cause) => cause.into(),
        error => invalid(&error.to_string()),
    }
}
fn constant_failure<H>(
    error: crate::physical_constant_v2::PhysicalConstantCodecError,
) -> crate::host_projection_v2::ProjectionFailure<crate::PhysicalEncodeError, H> {
    match error {
        crate::physical_constant_v2::PhysicalConstantCodecError::Control(cause) => cause.into(),
        error => invalid(&error.to_string()),
    }
}
