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

//! An explicit immutable type owner and linear BE construction resources.

use crate::{
    fragment_error::NativeFragmentDecodeError,
    native_type_construction::{
        NativeTypeConstruction, NativeTypeConstructionError, NativeTypeFactory,
    },
};
use novarocks_functions::FunctionValueType;
#[cfg(test)]
use novarocks_plan_codec::execution_type_table::{
    decode_execution_type_table_observed, decode_execution_type_table_with_factory_in,
};
use novarocks_plan_codec::{
    execution_type_table::{DecodedExecutionTypeTable, ExecutionTypeTableLimits},
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    value_type_table::LogicalTypeCodecError,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::{common, plan};
#[cfg(test)]
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::{CompileCheckpoints, ValueSchemaCreation, ValueTypeStorageLifetime};
use std::alloc::Layout;

/// Legacy fragment catalog loan around the shared native type-work owner.
pub(crate) struct NativeDecodeResources<'a> {
    shared: crate::native_type_decode::NativeTypeDecodeResources<'a>,
    pub(crate) function_catalog: Option<&'a novarocks_functions::EngineFunctionCatalog>,
}
impl<'a> NativeDecodeResources<'a> {
    pub(crate) fn try_new(
        creation: &'a NativeTypeConstruction<'a>,
    ) -> Result<Self, NativeFragmentDecodeError> {
        Ok(Self {
            shared: crate::native_type_decode::NativeTypeDecodeResources::try_new(creation)?,
            function_catalog: None,
        })
    }
    pub(crate) fn bind_function_catalog(
        &mut self,
        catalog: &'a novarocks_functions::EngineFunctionCatalog,
    ) {
        self.function_catalog = Some(catalog);
    }
    pub(crate) fn storage(&self) -> ValueTypeStorageLifetime {
        self.shared.storage()
    }
    pub(crate) fn request(&mut self, layout: Layout) -> Result<(), NativeFragmentDecodeError> {
        self.shared.request(layout).map_err(Into::into)
    }
    pub(crate) fn finish(self) -> Result<(), NativeFragmentDecodeError> {
        self.shared.finish().map_err(Into::into)
    }
}
impl<'a> std::ops::Deref for NativeDecodeResources<'a> {
    type Target = crate::native_type_decode::NativeTypeDecodeResources<'a>;
    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}
impl std::ops::DerefMut for NativeDecodeResources<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shared
    }
}

pub(crate) fn table_error(
    error: LogicalTypeCodecError,
    path: FieldPath,
) -> NativeFragmentDecodeError {
    crate::native_type_decode::table_error(error, path).into()
}
pub(crate) fn table_failure(
    error: ProjectionFailure<LogicalTypeCodecError, NativeTypeConstructionError>,
    path: FieldPath,
) -> NativeFragmentDecodeError {
    crate::native_type_decode::table_failure(error, path).into()
}

pub(crate) use crate::native_type_decode::NativeSourceTypes as NativeFragmentSourceTypes;

pub(crate) fn decode_fragment_type_owner(
    fragment: &plan::PlanFragment,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<NativeFragmentSourceTypes, NativeFragmentDecodeError> {
    let path = FieldPath::root("plan_fragment").field("execution_types");
    resources
        .work
        .step()
        .map_err(NativeTypeConstructionError::Control)?;
    let table = fragment.execution_types.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone(),
            "native fragment requires complete execution type owner",
        )
    })?;
    crate::native_type_decode::decode_type_owner(table, path, resources).map_err(Into::into)
}

pub(crate) fn resolve_type<'a>(
    table: &'a DecodedExecutionTypeTable,
    desc: &common::TypeDesc,
    path: FieldPath,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<&'a FunctionValueType, NativeFragmentDecodeError> {
    crate::native_type_decode::resolve_type(table, desc, path, resources).map_err(Into::into)
}
pub(crate) fn clone_type(
    source: &FunctionValueType,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<FunctionValueType, NativeFragmentDecodeError> {
    crate::native_type_decode::clone_type(source, resources).map_err(Into::into)
}

pub(crate) fn request_port<'a>(
    creation: &'a NativeTypeConstruction<'_>,
) -> impl FnMut(Layout) -> Result<(), AdmissionRefusal<NativeTypeConstructionError>> + 'a {
    move |layout| creation.request(layout)
}

/// The sole mutable type resolver passed through node and expression lowering.
/// Every caller borrows the exact complete table and its actual creation owner;
/// no inline type grammar or alternate construction path exists.
pub(crate) enum NativeTypeDecoder<'types, 'borrow, 'creator> {
    Strong {
        types: &'types DecodedExecutionTypeTable,
        original_requests:
            Option<&'types crate::fragment_original_requests::NativeOriginalRequests>,
        resources: &'borrow mut NativeDecodeResources<'creator>,
    },
}
impl NativeTypeDecoder<'_, '_, '_> {
    /// Validate complete selected identity before its original default actor.
    /// No pure kernel is initialized or discarded.
    pub(crate) fn validate_default_scalar(
        &mut self,
        expression: &novarocks_proto_models::expr::Expr,
        call: &novarocks_proto_models::expr::FunctionCall,
        declared: Option<&FunctionValueType>,
        path: FieldPath,
    ) -> Result<
        crate::fragment_original_requests::scalar::NativeScalarActor,
        NativeFragmentDecodeError,
    > {
        let Self::Strong {
            types,
            original_requests,
            resources,
        } = self;
        let declared = declared.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "ordinary scalar complete result declaration is absent",
            )
        })?;
        let catalog = resources.function_catalog.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "default scalar installed catalog loan is absent",
            )
        })?;
        let requests = original_requests.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("original_request_id"),
                "default scalar original request owner is absent",
            )
        })?;
        crate::fragment_original_requests::scalar::validate_default_scalar(
            expression, call, declared, types, requests, catalog, resources, path,
        )
    }

    /// Prepare from the original source receipt and installed frozen owner.
    /// A materialized Value is not an expression invocation or graph authority.
    pub(crate) fn prepare_writer_aggregate(
        &mut self,
        signature: &plan::ResolvedAggregateSignature,
        frozen: &novarocks_local_program::FrozenAggregateSelection,
        actual_input: &novarocks_type_contract::FunctionValueType,
        context: &crate::fragment_decode_context::NativePlanDecodeContext,
        occurrence: crate::fragment_original_requests::OriginalOccurrence,
        phase: novarocks_functions::AggregateKernelPhase,
        path: FieldPath,
    ) -> Result<novarocks_functions::PreparedAggregateHandle, NativeFragmentDecodeError> {
        use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        let Self::Strong {
            original_requests,
            resources,
            ..
        } = self;
        let requests = original_requests.ok_or_else(|| {
            NativeFragmentDecodeError::missing(path.clone(), "writer request owner is absent")
        })?;
        if context
            .original_requests()
            .is_none_or(|source| !std::ptr::eq(source, requests))
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "writer request source differs from its fragment owner",
            ));
        }
        let parameters = context.writer_parameters().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "writer semantic parameter source is absent",
            )
        })?;
        let catalog = context.function_catalog().ok_or_else(|| {
            NativeFragmentDecodeError::missing(path.clone(), "writer installed catalog is absent")
        })?;
        let raw = signature.writer_preparation_call.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "original frozen writer preparation call is absent",
            )
        })?;
        let novarocks_physical_plan::PhysicalCallDefinition::Relational(site) =
            occurrence.definition()?
        else {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "writer source is not a relational call",
            ));
        };
        let host = resources.creation;
        let mut request = |layout| host.request_layout(layout);
        let mut opaque = |bytes| host.request_opaque_upper_bound(bytes);
        let mut creator = ValueSchemaCreation::new(resources.storage(), &mut request, &mut opaque);
        let source =
            novarocks_plan_codec::physical_semantics_v2::decode_frozen_writer_call_with_creator_in(
                raw,
                site,
                &mut creator,
                &mut resources.work,
            )
            .map_err(|error| match error {
                ProjectionFailure::Host(cause) => NativeFragmentDecodeError::from(cause),
                ProjectionFailure::Codec(
                    novarocks_plan_codec::physical_semantics_v2::SemanticsCodecError::Control(
                        cause,
                    ),
                ) => NativeTypeConstructionError::Control(cause).into(),
                ProjectionFailure::Codec(cause) => {
                    NativeFragmentDecodeError::invalid_value(path.clone(), cause)
                }
            })?;
        let call = source.call();
        let materialized_input_context = call.materialized_writer_input.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "writer materialized Value source is absent",
            )
        })?;
        // This is the true newly allocated empty Arc used by the same options
        // body; it is not a guessed invoice for the implementation's kernel.
        resources.request(arc_layout(
            Layout::array::<novarocks_functions::AggregateOrderKey>(0).map_err(|_| {
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            })?,
        )?)?;
        let order_keys: std::sync::Arc<[novarocks_functions::AggregateOrderKey]> =
            std::sync::Arc::from([]);
        let id = signature.original_request_id.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "original writer request reference is absent",
            )
        })?;
        let policy = requests.request(id, resources)?.constant_policy;
        let factory = NativeTypeFactory::new(host);
        let value = requests.with_catalog_request(id, resources, |request| {
            use novarocks_type_contract::ValueSchemaCreationFactory;
            let mut scope = factory.begin_scope().map_err(NativeFragmentDecodeError::from)?;
            novarocks_functions::prepare_frozen_writer_aggregate_with_empty_order_keys_and_creator_in(
                catalog,
                novarocks_functions::FrozenWriterAggregatePreparation {
                    function_id: frozen.function_id(),
                    selected: frozen.selected_owner(),
                    original_request: request,
                    call_context: call.context,
                    materialized_input_context,
                    frozen_effects: &call.effects,
                    decimal_overflow_policy: call.decimal_overflow_policy,
                    parameters,
                    actual_input_type: actual_input,
                    phase,
                },
                order_keys,
                &policy,
                &mut scope,
                host,
            )
            .map_err(|cause| match cause {
                novarocks_functions::CheckedCallPreparationFailure::Binding(cause) => {
                    crate::fragment_original_requests::frozen_validation_failure(cause, path.clone())
                }
                novarocks_functions::CheckedCallPreparationFailure::Preparation(
                    novarocks_functions::CheckedOwnerFailure::Original(cause),
                ) => NativeFragmentDecodeError::from(cause),
                novarocks_functions::CheckedCallPreparationFailure::Preparation(
                    novarocks_functions::CheckedOwnerFailure::UnconvertedOwner,
                ) => NativeFragmentDecodeError::inconsistent(
                    path.clone(), "installed writer preparation owner is not converted",
                ),
            })
        })?;
        match value.into_prepared() {
            novarocks_functions::PreparedPureKernel::Aggregate(handle) => Ok(handle),
            _ => Err(NativeFragmentDecodeError::inconsistent(
                path,
                "writer source prepared a different installed ABI",
            )),
        }
    }

    pub(crate) fn observe_writer_source_layout(
        &mut self,
        layout: std::alloc::Layout,
    ) -> Result<(), NativeFragmentDecodeError> {
        {
            let Self::Strong { resources, .. } = self;
            resources.request(layout)
        }
    }
    pub(crate) fn writer_source_storage(
        &self,
    ) -> Option<novarocks_type_contract::ValueTypeStorageLifetime> {
        {
            let Self::Strong { resources, .. } = self;
            Some(resources.storage())
        }
    }
    pub(crate) fn create_prepared_writer_calls(
        &mut self,
        calls: Vec<novarocks_local_program::PreparedWriterAggregateCall>,
        phase: novarocks_functions::AggregateKernelPhase,
    ) -> Result<
        Option<novarocks_local_program::PreparedWriterAggregateCalls>,
        NativeFragmentDecodeError,
    > {
        let Self::Strong { resources, .. } = self;
        if calls.is_empty() {
            return Ok(None);
        }
        if calls.capacity() != calls.len() {
            resources.request(std::alloc::Layout::array::<novarocks_local_program::PreparedWriterAggregateCall>(calls.len()).map_err(|_| novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?)?;
        }
        resources
            .work
            .flush()
            .map_err(NativeTypeConstructionError::Control)?;
        let calls = calls.into_boxed_slice();
        let host = resources.creation;
        let storage = resources.storage();
        novarocks_local_program::PreparedWriterAggregateCalls::try_new_observed::<
            NativeFragmentDecodeError,
        >(
            calls,
            phase,
            storage,
            |layout| host.request_layout(layout).map_err(Into::into),
            || {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
                resources
                    .work
                    .flush()
                    .map_err(NativeTypeConstructionError::Control)?;
                Ok(())
            },
        )
        .map(Some)
    }

    pub(crate) fn frozen_aggregate(
        &mut self,
        signature: Option<&novarocks_proto_models::plan::ResolvedAggregateSignature>,
        context: &crate::fragment_decode_context::NativePlanDecodeContext,
        occurrence: crate::fragment_original_requests::OriginalOccurrence,
        path: FieldPath,
    ) -> Result<
        Option<std::sync::Arc<novarocks_local_program::FrozenAggregateSelection>>,
        NativeFragmentDecodeError,
    > {
        {
            let Self::Strong {
                types,
                resources,
                original_requests,
            } = self;
            {
                let signature = signature.ok_or_else(|| {
                    NativeFragmentDecodeError::missing(
                        path.clone(),
                        "aggregate frozen signature is absent",
                    )
                })?;
                let requests = context.original_requests().ok_or_else(|| {
                    NativeFragmentDecodeError::missing(
                        path.clone(),
                        "aggregate original request owner is absent",
                    )
                })?;
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
                if original_requests.is_none_or(|source| !std::ptr::eq(source, requests)) {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.clone(),
                        "aggregate request and expression constant namespace owners differ",
                    ));
                }
                let catalog = context.function_catalog().ok_or_else(|| {
                    NativeFragmentDecodeError::missing(
                        path.clone(),
                        "aggregate process catalog is absent",
                    )
                })?;
                crate::fragment_original_requests::decode_frozen_selection(
                    signature,
                    types,
                    requests,
                    catalog,
                    path,
                    occurrence.definition()?,
                    resources,
                )
                .map(Some)
            }
        }
    }

    pub(crate) fn constant_value(
        &mut self,
        reference: &novarocks_proto_models::call_request::ConstantReference,
        expression: &novarocks_proto_models::expr::Expr,
        path: FieldPath,
    ) -> Result<novarocks_functions::ConstantValue, NativeFragmentDecodeError> {
        let Self::Strong {
            types,
            original_requests,
            resources,
        } = self;
        let source = original_requests.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "fragment constant source namespace is absent",
            )
        })?;
        let desc = expression.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(path.clone(), "constant expression type is absent")
        })?;
        let value = resolve_type(types, desc, path.clone(), resources)?;
        source.constant(reference, value, resources)
    }
    pub(crate) fn reject_strong_inline_literal(
        &mut self,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        Err(NativeFragmentDecodeError::unsupported(
            path,
            "strong expression requires an admitted constant reference",
        ))
    }
    pub(crate) fn validate_actual_aggregate_arguments<'a>(
        &mut self,
        signature: &novarocks_proto_models::plan::ResolvedAggregateSignature,
        actual: impl Iterator<Item = &'a novarocks_proto_models::expr::Expr>,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong {
            original_requests,
            resources,
            ..
        } = self;
        let source = original_requests.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone(),
                "fragment original request namespace is absent",
            )
        })?;
        let id = signature.original_request_id.ok_or_else(|| {
            NativeFragmentDecodeError::missing(path, "original request reference is absent")
        })?;
        source.validate_actual_arguments(id, actual, resources)
    }

    pub(crate) fn aggregate_projection(
        &mut self,
        frozen: &novarocks_local_program::FrozenAggregateSelection,
    ) -> Result<novarocks_functions::ResolvedAggregateSignature, NativeFragmentDecodeError> {
        {
            let Self::Strong { resources, .. } = self;
            crate::fragment_original_requests::physical_projection(frozen, resources)
        }
    }

    pub(crate) fn validate_aggregate_name(
        &mut self,
        frozen: &novarocks_local_program::FrozenAggregateSelection,
        context: &crate::fragment_decode_context::NativePlanDecodeContext,
        wire_name: &str,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        resources
            .work
            .flush()
            .map_err(NativeTypeConstructionError::Control)?;
        let declaration = context
            .function_catalog()
            .and_then(|catalog| catalog.definition_by_id(frozen.function_id()));
        resources
            .work
            .step()
            .map_err(NativeTypeConstructionError::Control)?;
        let declaration = declaration.ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                path.clone(),
                "frozen aggregate installed declaration is absent",
            )
        })?;
        let canonical = declaration.canonical_name();
        let mut equal = wire_name.len() == canonical.len();
        if equal {
            for (actual, expected) in wire_name.bytes().zip(canonical.bytes()) {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
                if !actual.eq_ignore_ascii_case(&expected) {
                    equal = false;
                    break;
                }
            }
        }
        if !equal {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                "aggregate executable name differs from its installed function identity",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_argument_binding(
        &mut self,
        actual: &FunctionValueType,
        formal: &FunctionValueType,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        let fits = actual.fits_physical_binding_observed(formal, || {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        })?;
        if !fits {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                "aggregate actual value does not fit its complete formal binding",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_argument_slot_binding(
        &mut self,
        actual: &novarocks_execution::exec::chunk::ChunkSlotSchema,
        formal: &FunctionValueType,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        if !actual.is_typed() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                "aggregate actual input slot has no complete binding",
            ));
        }
        let value = actual.typed_binding().clone_observed(|| {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        })?;
        let actual = FunctionValueType::try_from_typed_binding(value)?;
        self.require_argument_binding(&actual, formal, path)
    }
    pub(crate) fn require_argument_descriptor_binding(
        &mut self,
        actual: &common::TypeDesc,
        formal: &FunctionValueType,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong {
            types, resources, ..
        } = self;
        let actual = resolve_type(types, actual, path.clone(), resources)?;
        let fits = actual.fits_physical_binding_observed(formal, || {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        })?;
        if !fits {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                "aggregate actual argument does not fit its complete formal binding",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_complete_binding(
        &mut self,
        actual: &FunctionValueType,
        expected: &FunctionValueType,
        ignore_root_nullability: bool,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        Self::require_complete_binding_in(
            actual,
            expected,
            ignore_root_nullability,
            resources,
            path,
        )
    }

    fn require_complete_binding_in(
        actual: &FunctionValueType,
        expected: &FunctionValueType,
        ignore_root_nullability: bool,
        resources: &mut NativeDecodeResources<'_>,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let mut observe = || {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        };
        let equal = if ignore_root_nullability {
            actual.same_physical_binding_ignoring_root_nullability_observed(expected, &mut observe)
        } else {
            actual.exactly_equals_checked_observed(expected, &mut observe)
        }?;
        if !equal {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                "aggregate execution port differs from its complete frozen binding",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_slot_binding(
        &mut self,
        actual: &novarocks_execution::exec::chunk::ChunkSlotSchema,
        expected: &FunctionValueType,
        ignore_root_nullability: bool,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        if !actual.is_typed() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                "aggregate execution slot has no complete binding",
            ));
        }
        let value = actual.typed_binding().clone_observed(|| {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        })?;
        let actual = FunctionValueType::try_from_typed_binding(value)?;
        self.require_complete_binding(&actual, expected, ignore_root_nullability, path)
    }

    pub(crate) fn require_merge_state_descriptor_binding(
        &mut self,
        actual: &common::TypeDesc,
        intermediate: &FunctionValueType,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong {
            types, resources, ..
        } = self;
        let actual = resolve_type(types, actual, path.clone(), resources)?;
        // This is an input port: the original state owner permits a wider
        // root NULL demand than its emission, never the reverse. The sole
        // author preserves domain and nested NULLs without constructing types.
        novarocks_functions::check_aggregate_merge_state_type_observed(
            actual,
            intermediate,
            &mut resources.work,
        )
        .map_err(|failure| match failure {
            novarocks_functions::KernelFailure::Cancelled => NativeTypeConstructionError::Control(
                novarocks_type_contract::CompileControlError::Cancelled,
            )
            .into(),
            novarocks_functions::KernelFailure::DeadlineExceeded => {
                NativeTypeConstructionError::Control(
                    novarocks_type_contract::CompileControlError::DeadlineExceeded,
                )
                .into()
            }
            novarocks_functions::KernelFailure::ResourceExhausted => {
                NativeTypeConstructionError::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                )
                .into()
            }
            failure => NativeFragmentDecodeError::inconsistent(path.clone(), failure),
        })?;
        // Keep Native's original exact physical-layout gate in addition to
        // the state author's input NULL law. Root widening cannot hide a
        // different width, timezone, dictionary encoding or named child.
        Self::require_complete_binding_in(actual, intermediate, true, resources, path)
    }

    pub(crate) fn require_descriptor_binding(
        &mut self,
        actual: &common::TypeDesc,
        expected: &FunctionValueType,
        path: FieldPath,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong {
            types, resources, ..
        } = self;
        let actual_type_id = actual.value_binding_id;
        let actual = resolve_type(types, actual, path.clone(), resources)?;
        let equal = actual.exactly_equals_checked_observed(expected, || {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        })?;
        if !equal {
            return Err(NativeFragmentDecodeError::inconsistent(
                path,
                format!(
                    "aggregate result differs from its complete frozen binding (binding_id={:?}, actual_nullable={}, expected_nullable={})",
                    actual_type_id,
                    actual.nullable(),
                    expected.nullable(),
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn indexed_name(
        &mut self,
        prefix: &str,
        index: usize,
    ) -> Result<String, NativeFragmentDecodeError> {
        use std::fmt::Write;
        let mut digits = 1usize;
        let mut remaining = index;
        while remaining >= 10 {
            {
                let Self::Strong { resources, .. } = self;
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
            }
            remaining /= 10;
            digits += 1;
        }
        let length =
            prefix
                .len()
                .checked_add(digits)
                .ok_or(NativeTypeConstructionError::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?;
        {
            let Self::Strong { resources, .. } = self;
            for _ in 0..length {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
            }
            resources.request(Layout::array::<u8>(length).map_err(|_| {
                NativeTypeConstructionError::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
            })?)?;
        }
        let mut name = String::with_capacity(length);
        name.push_str(prefix);
        write!(&mut name, "{index}").expect("writing to String");
        Ok(name)
    }

    pub(crate) fn source_slot_expression(
        &mut self,
        source: &novarocks_execution::exec::chunk::ChunkSlotSchema,
        arena: &mut novarocks_execution::exec::expr::ExprArena,
    ) -> Result<novarocks_execution::exec::expr::ExprId, NativeFragmentDecodeError> {
        use novarocks_execution::exec::expr::ExprNode;
        {
            let Self::Strong { resources, .. } = self;
            {
                if !source.is_typed() {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        FieldPath::root("plan_fragment"),
                        "native typed projection requires a complete source slot binding",
                    ));
                }
                let binding = source.typed_binding().clone_observed(|| {
                    resources
                        .work
                        .step()
                        .map_err(NativeTypeConstructionError::Control)
                })?;
                let value = FunctionValueType::try_from_typed_binding(binding)?;
                Ok(arena.push_value_typed(ExprNode::SlotId(source.slot_id()), value))
            }
        }
    }

    pub(crate) fn data_type(
        &mut self,
        desc: &common::TypeDesc,
        path: FieldPath,
    ) -> Result<arrow::datatypes::DataType, NativeFragmentDecodeError> {
        let Self::Strong {
            types, resources, ..
        } = self;
        crate::native_type_decode::NativeTypeTableDecoder { types, resources }
            .data_type(desc, path)
            .map_err(Into::into)
    }

    pub(crate) fn value_type(
        &mut self,
        desc: &common::TypeDesc,
        path: FieldPath,
    ) -> Result<Option<FunctionValueType>, NativeFragmentDecodeError> {
        let Self::Strong {
            types, resources, ..
        } = self;
        crate::native_type_decode::NativeTypeTableDecoder { types, resources }
            .value_type(desc, path)
            .map_err(Into::into)
    }

    pub(crate) fn synthetic_child_binding(
        &mut self,
        parent: &FunctionValueType,
        index: usize,
        path: FieldPath,
    ) -> Result<Option<FunctionValueType>, NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        let scope = resources.creation.begin_scope()?;
        let binding = parent
            .typed_binding()
            .child_at_observed::<NativeFragmentDecodeError>(
                index,
                scope.storage_lifetime(),
                |layout| scope.request_layout(layout).map_err(Into::into),
                |units| {
                    for _ in 0..units {
                        resources
                            .work
                            .step()
                            .map_err(NativeTypeConstructionError::Control)?;
                    }
                    Ok(())
                },
            )?
            .ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    path,
                    "synthetic target child declaration is absent",
                )
            })?;
        Ok(Some(FunctionValueType::try_from_typed_binding(binding)?))
    }

    pub(crate) fn synthetic_nullable_binding(
        &mut self,
        source: &FunctionValueType,
        nullable: bool,
    ) -> Result<Option<FunctionValueType>, NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        if source.nullable() == nullable {
            return clone_type(source, resources).map(Some);
        }
        let scope = resources.creation.begin_scope()?;
        source
            .with_nullable_observed::<NativeFragmentDecodeError>(
                nullable,
                scope.storage_lifetime(),
                |layout| scope.request_layout(layout).map_err(Into::into),
                |units| {
                    for _ in 0..units {
                        resources
                            .work
                            .step()
                            .map_err(NativeTypeConstructionError::Control)?;
                    }
                    Ok(())
                },
            )
            .map(Some)
    }

    pub(crate) fn save_synthetic_binding(
        &mut self,
        id: novarocks_execution::exec::expr::ExprId,
        source: Option<&FunctionValueType>,
        arena: &mut novarocks_execution::exec::expr::ExprArena,
    ) -> Result<(), NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        let source = source.ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                FieldPath::root("expr"),
                "synthetic expression requires its complete frozen target",
            )
        })?;
        let source = clone_type(source, resources)?;
        arena.set_value_type_observed::<NativeFragmentDecodeError>(id, source, || {
            resources
                .work
                .step()
                .map_err(|error| NativeTypeConstructionError::Control(error).into())
        })
    }

    pub(crate) fn save_expression_binding(
        &mut self,
        expression: &novarocks_proto_models::expr::Expr,
        path: FieldPath,
        id: novarocks_execution::exec::expr::ExprId,
        arena: &mut novarocks_execution::exec::expr::ExprArena,
    ) -> Result<(), NativeFragmentDecodeError> {
        {
            let Self::Strong {
                types, resources, ..
            } = self;
            {
                let desc = expression.r#type.as_ref().ok_or_else(|| {
                    NativeFragmentDecodeError::missing(
                        path.clone().field("type"),
                        "native Expr requires type",
                    )
                })?;
                let value = resolve_type(types, desc, path.clone().field("type"), resources)?;
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
                if value.nullable() != expression.nullable {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.field("nullable"),
                        "expression nullability differs from its complete binding",
                    ));
                }
                let value = clone_type(value, resources)?;
                arena.set_value_type_observed::<NativeFragmentDecodeError>(id, value, || {
                    resources
                        .work
                        .step()
                        .map_err(|error| NativeTypeConstructionError::Control(error).into())
                })
            }
        }
    }

    pub(crate) fn slot_schema(
        &mut self,
        desc: &common::TypeDesc,
        name: &str,
        nullable: bool,
        slot_id: novarocks_types::SlotId,
        unique_id: Option<i32>,
        path: &FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSlotSchema, NativeFragmentDecodeError> {
        {
            let Self::Strong {
                types, resources, ..
            } = self;
            {
                let value = resolve_type(types, desc, path.clone(), resources)?;
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
                if value.nullable() != nullable {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.clone(),
                        "field nullability differs from its complete binding",
                    ));
                }
                resources
                    .work
                    .flush()
                    .map_err(NativeTypeConstructionError::Control)?;
                let entity = resources.creation.begin_scope()?;
                let storage = entity.storage_lifetime();
                let mut port = |layout| entity.request(layout);
                crate::fragment_layout::slot_schema_from_binding_typed_observed(
                    slot_id,
                    name,
                    value.typed_binding(),
                    unique_id,
                    path,
                    storage,
                    &mut port,
                    &mut resources.work,
                )
                .map_err(layout_failure)
            }
        }
    }

    pub(crate) fn bindings_match(
        &mut self,
        actual: &novarocks_type_contract::TypedValueBinding,
        expected: &novarocks_type_contract::TypedValueBinding,
        ignore_root_nullability: bool,
    ) -> Result<bool, NativeFragmentDecodeError> {
        let Self::Strong {
            types, resources, ..
        } = self;
        crate::native_type_decode::NativeTypeTableDecoder { types, resources }
            .bindings_match(actual, expected, ignore_root_nullability)
            .map_err(Into::into)
    }

    pub(crate) fn exchange_contracts(
        &mut self,
        root: &plan::DistributedNode,
        path: FieldPath,
    ) -> Result<
        novarocks_execution::exec::fragment::program::ExchangeInputContracts,
        NativeFragmentDecodeError,
    > {
        use novarocks_execution::exec::fragment::program::ExchangeInputContracts;
        {
            let Self::Strong {
                types, resources, ..
            } = self;
            {
                let storage = resources.storage();
                let mut port = request_port(resources.creation);
                let contracts = crate::fragment_layout::decode_exchange_contracts_typed_observed(
                    root,
                    path,
                    types,
                    storage,
                    &mut port,
                    &mut resources.work,
                )
                .map_err(layout_failure)?;
                let (entries, storage) = contracts.into_parts();
                ExchangeInputContracts::try_from_sorted_observed(entries, storage, || {
                    resources
                        .work
                        .step()
                        .map_err(|error| NativeTypeConstructionError::Control(error).into())
                })
            }
        }
    }

    /// Decode one actual execution relation's independent logical namespace.
    /// The borrowed schema-root mode shares the original type/layout parser;
    /// no temporary wire table or metadata semantic importer participates.
    pub(crate) fn execution_relation_schema(
        &mut self,
        columns: &[plan::ArrowPhysicalColumn],
        logical_types: &[novarocks_proto_models::value_type::LogicalTypeDefinition],
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        self.execution_relation_schema_core(columns, logical_types, path, true)
    }

    pub(crate) fn unpivot_relation_schema(
        &mut self,
        columns: &[plan::ArrowPhysicalColumn],
        logical_types: &[novarocks_proto_models::value_type::LogicalTypeDefinition],
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        self.execution_relation_schema_core(columns, logical_types, path, false)
    }

    fn execution_relation_schema_core(
        &mut self,
        columns: &[plan::ArrowPhysicalColumn],
        logical_types: &[novarocks_proto_models::value_type::LogicalTypeDefinition],
        path: FieldPath,
        require_internal: bool,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        use novarocks_execution::exec::chunk::ChunkSlotSchema;

        let Self::Strong { resources, .. } = self;
        resources
            .work
            .step()
            .map_err(NativeTypeConstructionError::Control)?;

        for (ordinal, column) in columns.iter().enumerate() {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)?;
            if require_internal && !column.is_internal {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone()
                        .field("columns")
                        .index(ordinal)
                        .field("is_internal"),
                    "write relation columns must be internal",
                ));
            }
        }
        let limits = ExecutionTypeTableLimits::for_validated_root_count(columns.len())
            .map_err(NativeTypeConstructionError::Control)?;
        let factory = NativeTypeFactory::new(resources.creation);
        let types = novarocks_plan_codec::execution_type_table::decode_execution_schema_roots_with_factory_in(
            columns, logical_types, limits, &factory, &mut resources.work,
        ).map_err(|error|table_failure(error,path.clone()))?;
        let storage = resources.storage();
        resources.request(
            Layout::array::<ChunkSlotSchema>(columns.len()).map_err(|_| {
                NativeTypeConstructionError::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                )
            })?,
        )?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(columns.len()).map_err(|_| {
            NativeTypeConstructionError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        for (ordinal, column) in columns.iter().enumerate() {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)?;
            let field = column.field.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("columns").index(ordinal),
                    "execution relation field is required",
                )
            })?;
            let value = types
                .get_observed(column.slot_id, &mut resources.work)
                .map_err(|error| table_error(error, path.clone()))?
                .ok_or_else(|| {
                    NativeFragmentDecodeError::inconsistent(
                        path.clone(),
                        "execution relation root binding is absent",
                    )
                })?;
            resources
                .work
                .flush()
                .map_err(NativeTypeConstructionError::Control)?;
            let field_owner = resources.creation.begin_scope()?;
            let mut port = |layout| field_owner.request(layout);
            slots.push(
                crate::fragment_layout::slot_schema_from_binding_typed_observed(
                    novarocks_types::SlotId::new(column.slot_id),
                    &field.name,
                    value.typed_binding(),
                    None,
                    &path,
                    field_owner.storage_lifetime(),
                    &mut port,
                    &mut resources.work,
                )
                .map_err(layout_failure)?,
            );
        }
        create_typed_chunk_schema(resources, slots, storage, path)
    }

    /// Build the SPI writer contract from the already decoded complete roots.
    /// Additional names, fields and sets use the same actual BE creator;
    /// the original decoder hold is passed into the independent schema owner.
    pub(crate) fn writer_relation_contract(
        &mut self,
        schema: &novarocks_execution::exec::chunk::ChunkSchema,
        path: FieldPath,
    ) -> Result<
        novarocks_spi::connector::write_stack::WriterMultiplexSchema,
        NativeFragmentDecodeError,
    > {
        use novarocks_spi::connector::write_stack::relation::WriterSchemaCreationFailure as Failure;
        use novarocks_spi::connector::write_stack::{
            WRITE_RELATION_COLUMN_COUNT, WriterAuxiliaryChannel, WriterMultiplexSchema,
        };
        use novarocks_type_contract::ValueSchemaCreation;
        let Self::Strong { resources, .. } = self;
        let count = schema
            .slots()
            .len()
            .checked_sub(WRITE_RELATION_COLUMN_COUNT)
            .ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    path.clone(),
                    "writer multiplex schema lacks fixed prefix",
                )
            })?;
        resources.request(Layout::array::<WriterAuxiliaryChannel>(count).map_err(|_| {
            NativeTypeConstructionError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?)?;
        let mut channels = Vec::new();
        channels.try_reserve_exact(count).map_err(|_| {
            NativeTypeConstructionError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        let host = resources.creation;
        let mut request = |layout| host.request_layout(layout);
        let mut opaque = |bytes| host.request_opaque_upper_bound(bytes);
        let mut creation = ValueSchemaCreation::new(resources.storage(), &mut request, &mut opaque);
        let map = |error| {
            match error {
            Failure::Creator(error) => NativeFragmentDecodeError::from(error),
            Failure::Control(error) => NativeTypeConstructionError::Control(error).into(),
            Failure::Layout(error) => NativeTypeConstructionError::Resource(error).into(),
            Failure::HashMap(error) => NativeTypeConstructionError::Resource(match error {
                novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::SourceModel(_)=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
                novarocks_type_contract::owned_resources::hashmap::HashMapResourceError::Arithmetic(_)=>novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            }).into(),
            Failure::Contract(error) => {
                NativeFragmentDecodeError::invalid_value(path.clone(), error)
            }
        }
        };
        for slot in schema.slots().iter().skip(WRITE_RELATION_COLUMN_COUNT) {
            let value = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(|error| NativeFragmentDecodeError::invalid_value(path.clone(), error))?;
            channels.push(
                WriterAuxiliaryChannel::try_new_with_creator_in(
                    slot.slot_id().0,
                    slot.field().name(),
                    value,
                    Some(&mut creation),
                    Some(&mut resources.work),
                )
                .map_err(&map)?,
            );
        }
        WriterMultiplexSchema::try_new_with_creator_in(
            channels,
            Some(&mut creation),
            Some(&mut resources.work),
        )
        .map_err(map)
    }

    pub(crate) fn synthetic_binding_slot(
        &mut self,
        source: &FunctionValueType,
        name: &str,
        slot: novarocks_types::SlotId,
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSlotSchema, NativeFragmentDecodeError> {
        let Self::Strong { resources, .. } = self;
        let scope = resources.creation.begin_scope()?;
        let mut port = |layout| scope.request(layout);
        crate::fragment_layout::slot_schema_from_binding_typed_observed(
            slot,
            name,
            source.typed_binding(),
            None,
            &path,
            scope.storage_lifetime(),
            &mut port,
            &mut resources.work,
        )
        .map_err(layout_failure)
    }

    pub(crate) fn packed_struct_expression(
        &mut self,
        args: Vec<novarocks_execution::exec::expr::ExprId>,
        arena: &mut novarocks_execution::exec::expr::ExprArena,
        reject_null: bool,
        path: FieldPath,
    ) -> Result<Vec<novarocks_execution::exec::expr::ExprId>, NativeFragmentDecodeError> {
        use arrow::datatypes::{DataType, Field, Fields};
        use novarocks_type_contract::{
            ValueField, ValueKind, ValueLayout, ValueTypeRef, owned_resources::layout::arc_layout,
        };
        use std::{alloc::Layout, fmt::Write, sync::Arc};
        if args.len() <= 1 {
            return Ok(args);
        }
        let Self::Strong { resources, .. } = self;
        let scope = resources.creation.begin_scope()?;
        let mut storage = scope.storage_lifetime();
        let arithmetic = || {
            NativeTypeConstructionError::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        };
        scope.request_layout(Layout::array::<ValueField>(args.len()).map_err(|_| arithmetic())?)?;
        let temporary = resources.creation.begin_scope()?;
        temporary
            .request_layout(Layout::array::<Arc<Field>>(args.len()).map_err(|_| arithmetic())?)?;
        let mut semantic = Vec::with_capacity(args.len());
        let mut physical = Vec::with_capacity(args.len());
        for (index, id) in args.iter().enumerate() {
            let source = arena.value_type(*id).ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().index(index),
                    "packed input argument requires its complete declaration",
                )
            })?;
            if reject_null && source.data_type() == &DataType::Null {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().index(index),
                    "window function input type is null",
                ));
            }
            storage = storage.retaining_observed(
                &source.typed_binding().layout().storage_lifetime(),
                |layout| scope.request_layout(layout),
            )?;
            // Preserve the original nullable Field contract, without changing
            // any child's nominal domain, layout or nested NULL declaration.
            let child = source.with_nullable_observed::<NativeFragmentDecodeError>(
                true,
                scope.storage_lifetime(),
                |layout| scope.request_layout(layout).map_err(Into::into),
                |units| {
                    for _ in 0..units {
                        resources
                            .work
                            .step()
                            .map_err(NativeTypeConstructionError::Control)?;
                    }
                    Ok(())
                },
            )?;
            let name_bytes = 2 + if index == 0 {
                0
            } else {
                index.ilog10() as usize
            };
            scope.request_layout(Layout::array::<u8>(name_bytes).map_err(|_| arithmetic())?)?;
            let mut name = String::with_capacity(name_bytes);
            write!(&mut name, "f{index}").expect("writing to an exact-capacity String");
            scope.request_layout(Layout::array::<u8>(name_bytes).map_err(|_| arithmetic())?)?;
            scope.request_layout(ValueField::storage_allocation_layout()?)?;
            semantic.push(ValueField::new(
                name.clone().into_boxed_str(),
                child.value_type().clone(),
                scope.storage_lifetime(),
            ));
            scope.request_layout(arc_layout(Layout::new::<Field>())?)?;
            physical.push(Arc::new(Field::new(name, child.data_type().clone(), true)));
        }
        scope.request_layout(arc_layout(
            Layout::array::<Arc<Field>>(physical.len()).map_err(|_| arithmetic())?,
        )?)?;
        let carrier = DataType::Struct(Fields::from(physical));
        let map = |failure| match failure {
            ProjectionFailure::Host(cause) => NativeFragmentDecodeError::from(cause),
            ProjectionFailure::Codec(cause) => {
                NativeFragmentDecodeError::invalid_value(path.clone(), cause)
            }
        };
        scope.request_layout(ValueTypeRef::storage_allocation_layout()?)?;
        let value = ValueTypeRef::try_new_observed::<
            ProjectionFailure<
                novarocks_type_contract::ExecutionValueTypeError,
                NativeTypeConstructionError,
            >,
        >(
            ValueKind::Struct(semantic.into_boxed_slice()),
            true,
            scope.storage_lifetime(),
            |units| {
                for _ in 0..units {
                    resources
                        .work
                        .step()
                        .map_err(NativeTypeConstructionError::Control)
                        .map_err(ProjectionFailure::Host)?;
                }
                Ok(())
            },
        )
        .map_err(&map)?;
        scope.request_layout(ValueLayout::storage_allocation_layout()?)?;
        let layout = ValueLayout::try_from_normalized_arrow_observed::<
            ProjectionFailure<
                novarocks_type_contract::ExecutionValueTypeError,
                NativeTypeConstructionError,
            >,
        >(carrier, storage, |units| {
            for _ in 0..units {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)
                    .map_err(ProjectionFailure::Host)?;
            }
            Ok(())
        })
        .map_err(map)?;
        let complete = FunctionValueType::try_from_value(value, layout)
            .map_err(|cause| NativeFragmentDecodeError::invalid_value(path.clone(), cause))?;
        let expression = arena.push_value_typed(
            novarocks_execution::exec::expr::ExprNode::StructExpr { fields: args },
            complete,
        );
        drop(temporary);
        Ok(vec![expression])
    }

    /// The GenerateSeries wire fields themselves declare Plain Int64. This
    /// synthetic schema has no transported expression/type reference to infer.
    pub(crate) fn declared_i64_schema(
        &mut self,
        slots: &[(novarocks_types::SlotId, &str)],
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        use arrow::datatypes::DataType;
        use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef};
        let Self::Strong { resources, .. } = self;
        let scope = resources.creation.begin_scope()?;
        let storage = scope.storage_lifetime();
        scope.request_layout(ValueTypeRef::storage_allocation_layout()?)?;
        let semantic = ValueTypeRef::try_new_observed::<
            ProjectionFailure<
                novarocks_type_contract::ExecutionValueTypeError,
                NativeTypeConstructionError,
            >,
        >(
            ValueKind::Scalar(DataType::Int64),
            false,
            storage.clone(),
            |units| {
                for _ in 0..units {
                    resources
                        .work
                        .step()
                        .map_err(NativeTypeConstructionError::Control)
                        .map_err(ProjectionFailure::Host)?;
                }
                Ok(())
            },
        )
        .map_err(|failure| match failure {
            ProjectionFailure::Host(cause) => NativeFragmentDecodeError::from(cause),
            ProjectionFailure::Codec(cause) => {
                NativeFragmentDecodeError::invalid_value(path.clone(), cause)
            }
        })?;
        scope.request_layout(ValueLayout::storage_allocation_layout()?)?;
        let layout = ValueLayout::try_from_normalized_arrow_observed::<
            ProjectionFailure<
                novarocks_type_contract::ExecutionValueTypeError,
                NativeTypeConstructionError,
            >,
        >(DataType::Int64, storage, |units| {
            for _ in 0..units {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)
                    .map_err(ProjectionFailure::Host)?;
            }
            Ok(())
        })
        .map_err(|failure| match failure {
            ProjectionFailure::Host(cause) => NativeFragmentDecodeError::from(cause),
            ProjectionFailure::Codec(cause) => {
                NativeFragmentDecodeError::invalid_value(path.clone(), cause)
            }
        })?;
        let declared = FunctionValueType::try_from_value(semantic, layout)
            .map_err(|cause| NativeFragmentDecodeError::invalid_value(path.clone(), cause))?;
        resources.request(std::alloc::Layout::array::<novarocks_execution::exec::chunk::ChunkSlotSchema>(slots.len())
            .map_err(|_| NativeTypeConstructionError::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut schemas = Vec::with_capacity(slots.len());
        for (index, (slot, name)) in slots.iter().enumerate() {
            let entity = resources.creation.begin_scope()?;
            let mut port = |layout| entity.request(layout);
            schemas.push(
                crate::fragment_layout::slot_schema_from_binding_typed_observed(
                    *slot,
                    name,
                    declared.typed_binding(),
                    None,
                    &path.clone().index(index),
                    entity.storage_lifetime(),
                    &mut port,
                    &mut resources.work,
                )
                .map_err(layout_failure)?,
            );
        }
        let output = resources.storage();
        create_typed_chunk_schema(resources, schemas, output, path)
    }

    /// Author the exact join scope from complete side declarations. Only the
    /// prescribed outer-side root NULL changes; children and source IDs remain.
    pub(crate) fn join_scope_schema(
        &mut self,
        left: &novarocks_execution::exec::chunk::ChunkSchemaRef,
        right: &novarocks_execution::exec::chunk::ChunkSchemaRef,
        nullable_left: bool,
        nullable_right: bool,
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        use novarocks_execution::exec::chunk::ChunkSlotSchema;
        let Self::Strong { resources, .. } = self;
        let count =
            left.slots()
                .len()
                .checked_add(right.slots().len())
                .ok_or(NativeTypeConstructionError::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?;
        resources
            .work
            .flush()
            .map_err(NativeTypeConstructionError::Control)?;
        let output_owner = resources.creation.begin_scope()?;
        output_owner.request_layout(Layout::array::<ChunkSlotSchema>(count).map_err(|_| {
            NativeTypeConstructionError::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(count).map_err(|_| {
            NativeTypeConstructionError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        for (side, widen) in [(left, nullable_left), (right, nullable_right)] {
            for slot in side.slots() {
                resources
                    .work
                    .step()
                    .map_err(NativeTypeConstructionError::Control)?;
                if !slot.is_typed() {
                    return Err(NativeFragmentDecodeError::inconsistent(
                        path.clone(),
                        "join input has no complete source binding",
                    ));
                }
                if !widen || slot.nullable() {
                    slots.push(slot.clone());
                    continue;
                }
                resources
                    .work
                    .flush()
                    .map_err(NativeTypeConstructionError::Control)?;
                let entity = resources.creation.begin_scope()?;
                let source =
                    FunctionValueType::try_from_typed_binding(slot.typed_binding().clone())?;
                let declared = source.with_nullable_observed::<NativeFragmentDecodeError>(
                    true,
                    entity.storage_lifetime(),
                    |layout| entity.request_layout(layout).map_err(Into::into),
                    |units| {
                        for _ in 0..units {
                            resources
                                .work
                                .step()
                                .map_err(NativeTypeConstructionError::Control)?;
                        }
                        Ok(())
                    },
                )?;
                let mut port = |layout| entity.request(layout);
                slots.push(
                    crate::fragment_layout::slot_schema_from_binding_typed_observed(
                        slot.slot_id(),
                        slot.name(),
                        declared.typed_binding(),
                        slot.unique_id(),
                        &path,
                        entity.storage_lifetime(),
                        &mut port,
                        &mut resources.work,
                    )
                    .map_err(layout_failure)?,
                );
            }
        }
        let mut request = |layout| output_owner.request_layout(layout);
        create_typed_chunk_schema_with_owner(
            &mut resources.work,
            &mut request,
            slots,
            output_owner.storage_lifetime(),
            path,
        )
    }

    pub(crate) fn assemble_chunk_schema(
        &mut self,
        slots: Vec<novarocks_execution::exec::chunk::ChunkSlotSchema>,
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        {
            let Self::Strong { resources, .. } = self;
            {
                let storage = resources.storage();
                create_typed_chunk_schema(resources, slots, storage, path)
            }
        }
    }

    pub(crate) fn chunk_schema(
        &mut self,
        columns: &[common::OutputColumn],
        path: FieldPath,
    ) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
        self.output_layout(columns, path)
            .map(|layout| layout.chunk_schema())
    }

    pub(crate) fn output_layout(
        &mut self,
        columns: &[common::OutputColumn],
        path: FieldPath,
    ) -> Result<crate::fragment_layout::NativeOutputLayout, NativeFragmentDecodeError> {
        {
            let Self::Strong {
                types, resources, ..
            } = self;
            {
                let factory = NativeTypeFactory::new(resources.creation);
                crate::fragment_layout::decode_output_layout_typed_with_factory_in(
                    columns,
                    path,
                    types,
                    &factory,
                    &mut resources.work,
                )
                .map_err(layout_failure)
            }
        }
    }
}

fn create_typed_chunk_schema(
    resources: &mut NativeDecodeResources<'_>,
    slots: Vec<novarocks_execution::exec::chunk::ChunkSlotSchema>,
    _original_storage: ValueTypeStorageLifetime,
    path: FieldPath,
) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
    // The incoming Vec is a temporary caller backing. A final schema cannot
    // relabel that already-created backing onto a fresh reservation. Create
    // its actual independent reference Vec before consuming the original.
    resources
        .work
        .flush()
        .map_err(NativeTypeConstructionError::Control)?;
    let container = resources.creation.begin_scope()?;
    container.request_layout(
        Layout::array::<novarocks_execution::exec::chunk::ChunkSlotSchema>(slots.len()).map_err(
            |_| {
                NativeTypeConstructionError::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
            },
        )?,
    )?;
    let mut owned = Vec::new();
    owned.try_reserve_exact(slots.len()).map_err(|_| {
        NativeTypeConstructionError::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    for slot in &slots {
        resources
            .work
            .step()
            .map_err(NativeTypeConstructionError::Control)?;
        owned.push(slot.clone());
    }
    drop(slots);
    let mut request = |layout| container.request_layout(layout);
    create_typed_chunk_schema_with_owner(
        &mut resources.work,
        &mut request,
        owned,
        container.storage_lifetime(),
        path,
    )
}

fn create_typed_chunk_schema_with_owner(
    work: &mut CompileCheckpoints<'_>,
    request: &mut impl FnMut(Layout) -> Result<(), NativeTypeConstructionError>,
    slots: Vec<novarocks_execution::exec::chunk::ChunkSlotSchema>,
    storage: ValueTypeStorageLifetime,
    path: FieldPath,
) -> Result<novarocks_execution::exec::chunk::ChunkSchemaRef, NativeFragmentDecodeError> {
    use novarocks_execution::exec::chunk::ChunkSchema;
    use std::{cell::RefCell, sync::Arc};
    enum BuildError {
        Source(String),
        Caller(NativeFragmentDecodeError),
    }
    impl From<String> for BuildError {
        fn from(error: String) -> Self {
            Self::Source(error)
        }
    }
    let work = RefCell::new(work);
    let schema = ChunkSchema::try_new_typed_observed(
        slots,
        storage,
        |layout| {
            work.borrow_mut().flush().map_err(|error| {
                BuildError::Caller(NativeTypeConstructionError::Control(error).into())
            })?;
            request(layout).map_err(|error| BuildError::Caller(error.into()))
        },
        |units| {
            if units == 0 {
                work.borrow_mut().flush().map_err(|error| {
                    BuildError::Caller(NativeTypeConstructionError::Control(error).into())
                })?;
            }
            for _ in 0..units {
                work.borrow_mut().step().map_err(|error| {
                    BuildError::Caller(NativeTypeConstructionError::Control(error).into())
                })?;
            }
            Ok(())
        },
    )
    .map_err(|error| match error {
        BuildError::Caller(error) => error,
        BuildError::Source(error) => NativeFragmentDecodeError::inconsistent(path.clone(), error),
    })?;
    work.borrow_mut()
        .flush()
        .map_err(NativeTypeConstructionError::Control)?;
    drop(work);
    request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<ChunkSchema>())
            .map_err(NativeTypeConstructionError::Resource)?,
    )?;
    Ok(Arc::new(schema))
}

pub(crate) fn layout_failure(
    failure: ProjectionFailure<
        crate::fragment_layout::NativeLayoutProjectionError,
        NativeTypeConstructionError,
    >,
) -> NativeFragmentDecodeError {
    use crate::fragment_layout::NativeLayoutProjectionError as Error;
    match failure {
        ProjectionFailure::Host(error) => error.into(),
        ProjectionFailure::Codec(Error::Control(error)) => {
            NativeTypeConstructionError::Control(error).into()
        }
        ProjectionFailure::Codec(Error::Resource(error)) => {
            NativeTypeConstructionError::Resource(error).into()
        }
        ProjectionFailure::Codec(Error::Protocol(error)) => error.into(),
        ProjectionFailure::Codec(Error::TypeTable { path, source }) => table_error(source, path),
    }
}

pub(crate) fn lowering_failure(
    failure: novarocks_execution::exec::node::LocalProgramCreationFailure<
        NativeTypeConstructionError,
    >,
    path: FieldPath,
) -> NativeFragmentDecodeError {
    use novarocks_execution::exec::node::LocalProgramCreationFailure as Error;
    match failure {
        Error::Creator(error) => error.into(),
        Error::Control(error) => NativeTypeConstructionError::Control(error).into(),
        Error::Resource(error) => NativeTypeConstructionError::Resource(error).into(),
        Error::Lowering(error) => NativeFragmentDecodeError::invalid_value(path, error),
    }
}

#[cfg(test)]
mod formal_argument_tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use novarocks_type_contract::{CompileControlError, PureCompileControl, ValueLogicalType};
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    #[test]
    fn actual_argument_uses_original_full_nullable_covariance() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "native formal argument test",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker, &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let types = decode_execution_type_table_observed(
            &novarocks_proto_models::plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &types,
            original_requests: None,
            resources: &mut resources,
        };
        let path = FieldPath::root("argument");
        let actual = FunctionValueType::new(DataType::Utf8, false);
        let formal = FunctionValueType::new(DataType::Utf8, true);
        decoder
            .require_argument_binding(&actual, &formal, path.clone())
            .unwrap();
        assert!(
            decoder
                .require_argument_binding(&formal, &actual, path.clone())
                .is_err()
        );
        let nominal =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        assert!(
            decoder
                .require_argument_binding(&actual, &nominal, path.clone())
                .is_err()
        );
        let wide = FunctionValueType::new(DataType::LargeUtf8, true);
        assert!(
            decoder
                .require_argument_binding(&actual, &wide, path.clone())
                .is_err()
        );
        let a = FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        );
        let b = FunctionValueType::new(
            DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
            true,
        );
        assert!(decoder.require_argument_binding(&a, &b, path).is_err());
        drop(decoder);
        resources.finish().unwrap();
    }
}

#[cfg(test)]
mod scoped_type_owner_tests {
    use super::*;
    use crate::native_type_construction::NativeTypeScope;
    use arrow::datatypes::{DataType, Field};
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder,
    };
    use novarocks_type_contract::{
        PureCompileControl, TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef,
    };
    use std::sync::Arc;

    #[test]
    fn execution_type_source_invoice_matches_final_map_owners_without_temporary_entries_cache() {
        let source = FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Int64, false)),
                            Arc::new(Field::new("value", DataType::Int64, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        );
        let unused = FunctionValueType::new(
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some("z".repeat(1024).into()),
            ),
            true,
        );
        let mut encode = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let mut table = ExecutionTypeTableBuilder::new(&[
            ExecutionTypeRoot {
                id: 0,
                value: &source,
            },
            ExecutionTypeRoot {
                id: u32::MAX,
                value: &unused,
            },
        ])
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(2).unwrap(),
            &mut |_| Ok(()),
            &mut encode,
        )
        .unwrap()
        .into_wire();
        encode.finish().unwrap();
        table.value_bindings.retain(|binding| binding.id == Some(0));
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "retained source Map invoice",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let factory = NativeTypeFactory::new(&creation);
        let mut work = CompileCheckpoints::try_new(&creation, CompilePhase::Decode).unwrap();
        let decoded = decode_execution_type_table_with_factory_in(
            &table,
            ExecutionTypeTableLimits::for_validated_root_count(2).unwrap(),
            &factory,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let upper = decoded.retained_source_bytes_upper_bound();
        assert_eq!(
            decoded
                .get_observed(
                    0,
                    &mut CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap()
                )
                .unwrap()
                .unwrap(),
            &source
        );
        drop(factory);
        drop(creation);
        assert_eq!(
            tracker.current(),
            i64::try_from(upper).unwrap(),
            "this exact-layout fixture has no retained transient reallocations; unused timezone, graph scratch and entries cache have exited"
        );
        drop(decoded);
        assert_eq!(tracker.current(), 0);
    }

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(
            &self,
            _: CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    fn fragment() -> plan::PlanFragment {
        // These are genuine pure FE declarations; their complete fields and
        // values are emitted by the existing strong namespace encoder body.
        let root = FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "reachable integer",
                    DataType::Int64,
                    false,
                ))]
                .into(),
            ),
            false,
        );
        let unused = FunctionValueType::new(
            DataType::Struct(
                vec![
                    Arc::new(Field::new("unused string", DataType::Utf8, true)),
                    Arc::new(Field::new("unused integer", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        );
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let execution = ExecutionTypeTableBuilder::new(&[
            ExecutionTypeRoot {
                id: 0,
                value: &root,
            },
            ExecutionTypeRoot {
                id: u32::MAX,
                value: &unused,
            },
        ])
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(2).unwrap(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap()
        .into_wire();
        work.finish().unwrap();
        plan::PlanFragment {
            execution_types: Some(execution),
            ..Default::default()
        }
    }

    #[test]
    fn native_scoped_table_late_child_releases_unused_fields_layouts_containers_and_scratch() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "native scoped table child",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_fragment_type_owner(&fragment(), &mut resources).unwrap();
        let live = tracker.current();
        assert!(
            decoded.construction_request_bytes_upper_bound > usize::try_from(live).unwrap(),
            "completed request upper still records actual exited scratch work"
        );
        let root = decoded
            .execution
            .get_observed(0, &mut resources.work)
            .unwrap()
            .unwrap();
        let late = root.value_type().child_type_at(0).unwrap().clone();
        assert!(matches!(late.kind(), ValueKind::Scalar(DataType::Int64)));
        drop(decoded);
        resources.finish().unwrap();
        drop(creation);
        let own = NativeTypeScope::reservation_allocation_layout()
            .unwrap()
            .size()
            + ValueTypeRef::storage_allocation_layout().unwrap().size();
        assert_eq!(
            tracker.current(),
            i64::try_from(own).unwrap(),
            "one semantic child retains only its own reservation and holder"
        );
        assert!(tracker.current() < live);
        drop(late);
        assert_eq!(tracker.current(), 0);
    }

    fn late_slot_after_real_main_scratch(
        scratch_len: usize,
    ) -> (
        novarocks_execution::exec::chunk::ChunkSlotSchema,
        std::sync::Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    ) {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "late named slot owner",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let scratch_layout = std::alloc::Layout::array::<u64>(scratch_len).unwrap();
        creation.request_layout(scratch_layout).unwrap();
        let scratch = Vec::<u64>::with_capacity(scratch_len);
        let source = fragment();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_fragment_type_owner(&source, &mut resources).unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &decoded.execution,
            original_requests: None,
            resources: &mut resources,
        };
        let late = decoder
            .slot_schema(
                &common::TypeDesc {
                    value_binding_id: Some(0),
                },
                "late",
                false,
                novarocks_types::SlotId::new(7),
                None,
                &FieldPath::root("late_slot"),
            )
            .unwrap();
        drop(decoder);
        drop(decoded);
        resources.finish().unwrap();
        drop(scratch);
        drop(creation);
        (late, tracker)
    }

    #[test]
    fn late_native_named_slot_does_not_keep_unrelated_main_construction_charge() {
        let (baseline, baseline_tracker) = late_slot_after_real_main_scratch(0);
        let (late, tracker) = late_slot_after_real_main_scratch(512);
        assert_eq!(late.typed_binding(), baseline.typed_binding());
        assert_eq!(late.field().metadata().capacity(), 0);
        assert!(
            tracker.current() > 0,
            "the actual named Field and shared binding are still live"
        );
        assert_eq!(
            tracker.current(),
            baseline_tracker.current(),
            "the late slot retains its own Field and reachable source types, not the real unrelated main Vec charge"
        );
        drop(late);
        assert_eq!(tracker.current(), 0);
        drop(baseline);
        assert_eq!(baseline_tracker.current(), 0);
    }

    fn late_schema_after_real_main_scratch(
        scratch_len: usize,
    ) -> (
        novarocks_execution::exec::chunk::ChunkSchemaRef,
        std::sync::Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    ) {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "late output schema owner",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        creation
            .request_layout(std::alloc::Layout::array::<u64>(scratch_len).unwrap())
            .unwrap();
        let scratch = Vec::<u64>::with_capacity(scratch_len);
        let source = fragment();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_fragment_type_owner(&source, &mut resources).unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &decoded.execution,
            original_requests: None,
            resources: &mut resources,
        };
        let layout = decoder
            .output_layout(
                &[common::OutputColumn {
                    column_id: 7,
                    name: "late".into(),
                    nullable: false,
                    is_internal: false,
                    r#type: Some(common::TypeDesc {
                        value_binding_id: Some(0),
                    }),
                }],
                FieldPath::root("late_schema"),
            )
            .unwrap();
        let late = layout.chunk_schema();
        drop(layout);
        drop(decoder);
        drop(decoded);
        resources.finish().unwrap();
        drop(scratch);
        drop(creation);
        (late, tracker)
    }

    #[test]
    fn late_native_output_schema_keeps_only_its_container_and_reachable_named_slots() {
        let (baseline, baseline_tracker) = late_schema_after_real_main_scratch(0);
        let (late, tracker) = late_schema_after_real_main_scratch(512);
        assert_eq!(
            late.slots()[0].typed_binding(),
            baseline.slots()[0].typed_binding()
        );
        assert!(late.is_typed());
        assert!(tracker.current() > 0);
        assert_eq!(
            tracker.current(),
            baseline_tracker.current(),
            "scratch indexes, unused type definitions and unrelated main construction have exited"
        );
        drop(late);
        assert_eq!(tracker.current(), 0);
        drop(baseline);
        assert_eq!(baseline_tracker.current(), 0);
    }

    fn complete_late_child(
        sibling_name_bytes: usize,
    ) -> (
        TypedValueBinding,
        Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    ) {
        let root = FunctionValueType::new(
            DataType::Struct(
                vec![
                    Arc::new(Field::new("kept", DataType::Int64, false)),
                    Arc::new(Field::new(
                        "s".repeat(sibling_name_bytes),
                        DataType::Utf8,
                        true,
                    )),
                ]
                .into(),
            ),
            false,
        );
        let mut encode = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let execution = ExecutionTypeTableBuilder::new(&[ExecutionTypeRoot {
            id: 0,
            value: &root,
        }])
        .encode_observed::<std::convert::Infallible>(
            ExecutionTypeTableLimits::for_validated_root_count(1).unwrap(),
            &mut |_| Ok(()),
            &mut encode,
        )
        .unwrap()
        .into_wire();
        encode.finish().unwrap();
        let tracker =
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("complete late child");
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_fragment_type_owner(
            &plan::PlanFragment {
                execution_types: Some(execution),
                ..Default::default()
            },
            &mut resources,
        )
        .unwrap();
        let root = decoded
            .execution
            .get_observed(0, &mut resources.work)
            .unwrap()
            .unwrap();
        let late = root
            .typed_binding()
            .child_at_observed(
                0,
                resources.storage(),
                |_| panic!("cached child projection must not allocate"),
                |units| {
                    for _ in 0..units {
                        resources
                            .work
                            .step()
                            .map_err(NativeTypeConstructionError::Control)?;
                    }
                    Ok::<_, NativeTypeConstructionError>(())
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            late.layout().comparison_work_upper_bound(),
            ValueLayout::try_new(&DataType::Int64, ValueTypeStorageLifetime::pure())
                .unwrap()
                .comparison_work_upper_bound()
        );
        drop(decoded);
        resources.finish().unwrap();
        drop(creation);
        (late, tracker)
    }

    #[test]
    fn native_complete_child_layout_releases_parent_names_pointer_backing_and_main() {
        let (small, small_tracker) = complete_late_child(1);
        let (late, tracker) = complete_late_child(1024);
        assert_eq!(late, small);
        let own = 2 * NativeTypeScope::reservation_allocation_layout()
            .unwrap()
            .size()
            + ValueTypeRef::storage_allocation_layout().unwrap().size()
            + ValueLayout::storage_allocation_layout().unwrap().size();
        assert_eq!(tracker.current(), i64::try_from(own).unwrap());
        assert_eq!(
            tracker.current(),
            small_tracker.current(),
            "unrelated parent name/Field/container fees must exit"
        );
        drop(late);
        assert_eq!(tracker.current(), 0);
        drop(small);
        assert_eq!(small_tracker.current(), 0);
    }

    #[test]
    fn flat_loan_pending_control_precedes_real_tracker_refusal_and_first_host_has_no_tail() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct SwitchControl {
            stopped: AtomicBool,
            calls: AtomicUsize,
        }
        impl PureCompileControl for SwitchControl {
            fn checkpoint(
                &self,
                _: CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.stopped.load(Ordering::SeqCst) {
                    Err(novarocks_type_contract::CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        for pending_control in [true, false] {
            let control = SwitchControl {
                stopped: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
            };
            let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                "flat loan first cause",
            );
            let creation =
                NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
            let source = creation.begin_scope().unwrap();
            let owner = creation.begin_scope().unwrap();
            let loans = [source.storage_lifetime()];
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let allocated = tracker.allocated();
            let deallocated = tracker.deallocated();
            let prior = tracker.current();
            tracker.install_limit_once(prior).unwrap();
            control.stopped.store(pending_control, Ordering::SeqCst);
            let mut requests = 0;
            let mut first_host_calls = None;
            let error = owner
                .storage_lifetime()
                .retaining_flat_observed(
                    loans.iter().map(Ok::<_, NativeTypeConstructionError>),
                    |layout| {
                        requests += 1;
                        match owner.request_layout(layout) {
                            Ok(()) => Ok(()),
                            Err(error) => {
                                first_host_calls = Some(control.calls.load(Ordering::SeqCst));
                                control.stopped.store(true, Ordering::SeqCst);
                                Err(error)
                            }
                        }
                    },
                    &mut work,
                )
                .unwrap_err();
            if pending_control {
                assert_eq!(
                    error,
                    NativeTypeConstructionError::Control(
                        novarocks_type_contract::CompileControlError::Cancelled
                    )
                );
                assert_eq!(
                    requests, 0,
                    "pending control must win before the real tracker request"
                );
                assert_eq!(first_host_calls, None);
            } else {
                assert!(matches!(error, NativeTypeConstructionError::Capacity(_)));
                assert_eq!(requests, 1);
                assert_eq!(
                    Some(control.calls.load(Ordering::SeqCst)),
                    first_host_calls,
                    "the originating host refusal has no control tail"
                );
            }
            // MemTracker records prospective consumption even on failure.
            // NativeScope releases that unconstructed request immediately;
            // cumulative counters are responsibility events, not allocations.
            let refused = if pending_control {
                0
            } else {
                i64::try_from(
                    std::alloc::Layout::array::<ValueTypeStorageLifetime>(loans.len())
                        .unwrap()
                        .size(),
                )
                .unwrap()
            };
            assert_eq!(tracker.allocated() - allocated, refused);
            assert_eq!(tracker.deallocated() - deallocated, refused);
            assert_eq!(
                tracker.current(),
                prior,
                "the original prospective request is fully rolled back"
            );
            drop(work);
            drop(loans);
            drop(source);
            drop(owner);
            drop(creation);
            assert_eq!(tracker.current(), 0);
        }
    }

    fn late_source_field(
        unrelated_bytes: usize,
    ) -> (
        Arc<Field>,
        ValueTypeStorageLifetime,
        Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    ) {
        use novarocks_plan_codec::physical_type_v2::{
            TypeProjectionLimits, decode_source_field_table_input_derived_with_factory,
            encode_source_field_table_pure_in,
        };
        let kept = Arc::new(
            Field::new("kept source", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([(
                    "provider.tag".to_owned(),
                    "exact original source".to_owned(),
                )]),
            ),
        );
        let unused = Arc::new(Field::new("unused", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                "provider.unused".to_owned(),
                "u".repeat(unrelated_bytes),
            )]),
        ));
        let mut encode = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let table = encode_source_field_table_pure_in(
            &[(0, &kept), (u32::MAX, &unused)],
            TypeProjectionLimits::for_validated_source_root_count(2).unwrap(),
            &mut encode,
        )
        .unwrap();
        encode.finish().unwrap();
        let tracker =
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("source Field loan");
        let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        actual
            .request_layout(std::alloc::Layout::array::<u64>(unrelated_bytes).unwrap())
            .unwrap();
        let scratch = Vec::<u64>::with_capacity(unrelated_bytes);
        let mut resources = NativeDecodeResources::try_new(&actual).unwrap();
        let raw = plan::PlanFragment {
            constant_source_fields: Some(table),
            ..Default::default()
        };
        let source =
            crate::fragment_original_sources::raw_source_bytes(&raw, &mut resources).unwrap();
        let factory = NativeTypeFactory::new(&actual);
        let decoded = {
            let mut exact = |layout| actual.request_layout(layout);
            let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
            let mut creation =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            decode_source_field_table_input_derived_with_factory(
                raw.constant_source_fields.as_ref().unwrap(),
                source,
                &mut creation,
                &factory,
                &mut resources.work,
            )
            .unwrap()
        };
        let borrow = decoded
            .field_borrow_observed(0, &mut resources.work)
            .unwrap()
            .unwrap();
        let field = borrow.field().clone();
        let loan = borrow.storage_lifetime();
        assert_eq!(field.as_ref(), kept.as_ref());
        drop(decoded);
        resources.finish().unwrap();
        drop(factory);
        drop(scratch);
        drop(actual);
        (field, loan, tracker)
    }

    #[test]
    fn original_source_field_loan_does_not_retain_unused_fields_table_or_main_charge() {
        let (small, small_loan, small_tracker) = late_source_field(1);
        let (late, loan, tracker) = late_source_field(1024);
        assert_eq!(late.as_ref(), small.as_ref());
        assert_eq!(
            late.metadata().get("provider.tag").unwrap(),
            "exact original source"
        );
        assert!(tracker.current() > 0);
        assert_eq!(
            tracker.current(),
            small_tracker.current(),
            "unrelated original source attrs and main scratch must exit"
        );
        drop(late);
        drop(loan);
        assert_eq!(tracker.current(), 0);
        drop(small);
        drop(small_loan);
        assert_eq!(small_tracker.current(), 0);
    }

    #[test]
    fn wide_backing_loans_drop_flat_without_width_dependent_recursion() {
        use novarocks_type_contract::ValueTypeStorageLifetime;
        let tracker =
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("flat scope loans");
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let temporary = creation.begin_scope().unwrap();
        let width = 4096usize;
        temporary
            .request_layout(std::alloc::Layout::array::<ValueTypeStorageLifetime>(width).unwrap())
            .unwrap();
        let mut loans = Vec::with_capacity(width);
        for _ in 0..width {
            // The real reservation Arc itself is this source's actual backing.
            let source = creation.begin_scope().unwrap();
            loans.push(source.storage_lifetime());
        }
        let owner = creation.begin_scope().unwrap();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode).unwrap();
        let retained = owner
            .storage_lifetime()
            .retaining_flat_observed(
                loans
                    .iter()
                    .map(|loan| Ok::<_, NativeTypeConstructionError>(loan)),
                |layout| owner.request_layout(layout),
                &mut work,
            )
            .unwrap();
        work.finish().unwrap();
        drop(loans);
        drop(temporary);
        drop(owner);
        drop(creation);
        assert!(
            tracker.current() > 0,
            "only the flat owner and actual source reservation backings remain"
        );
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn(move || drop(retained))
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(
            tracker.current(),
            0,
            "last flat reference releases every actual tracker responsibility"
        );
    }

    #[test]
    fn native_output_header_refuses_before_the_first_entity_scope() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "output header precedes creator",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_fragment_type_owner(&fragment(), &mut resources).unwrap();
        let current = tracker.current();
        tracker.install_limit_once(current).unwrap();
        let allocated = tracker.allocated();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &decoded.execution,
            original_requests: None,
            resources: &mut resources,
        };
        let error = decoder
            .output_layout(
                &vec![
                    common::OutputColumn::default();
                    novarocks_type_contract::MAX_VALUE_TYPE_NODES + 1
                ],
                FieldPath::root("oversized_output"),
            )
            .unwrap_err();
        match error {
            NativeFragmentDecodeError::Protocol(error) => {
                assert_eq!(
                    error.kind(),
                    novarocks_proto_codec::ProtocolErrorKind::Capacity
                );
                assert_eq!(error.detail(), "native output column count exceeds profile");
            }
            other => panic!("original header refusal was replaced: {other}"),
        }
        assert_eq!(
            tracker.allocated(),
            allocated,
            "no prospective scope or Field was requested"
        );
        assert_eq!(tracker.current(), current);
        drop(decoder);
        drop(decoded);
        resources.finish().unwrap();
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn native_named_slot_first_tracker_refusal_rolls_back_before_field_allocation() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "late slot first admission",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let decoded = decode_fragment_type_owner(&fragment(), &mut resources).unwrap();
        let current = tracker.current();
        let allocated = tracker.allocated();
        tracker.install_limit_once(current).unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &decoded.execution,
            original_requests: None,
            resources: &mut resources,
        };
        let error = decoder
            .slot_schema(
                &common::TypeDesc {
                    value_binding_id: Some(0),
                },
                "not allocated",
                false,
                novarocks_types::SlotId::new(7),
                None,
                &FieldPath::root("first_refusal"),
            )
            .unwrap_err();
        assert!(
            matches!(
                &error,
                NativeFragmentDecodeError::Construction(NativeTypeConstructionError::Capacity(_))
            ),
            "{error}"
        );
        assert_eq!(
            tracker.current(),
            current,
            "the refused prospective scope request was balanced"
        );
        assert_eq!(
            tracker.allocated() - allocated,
            i64::try_from(
                NativeTypeScope::reservation_allocation_layout()
                    .unwrap()
                    .size()
            )
            .unwrap(),
            "only the first actual reservation request reached the tracker; no later name or Field request ran"
        );
        drop(decoder);
        drop(decoded);
        drop(resources);
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn native_scoped_logical_graph_keeps_total_expansion_bound_and_rolls_back_invalid_unused_definition()
     {
        use novarocks_plan_codec::value_type_table::{
            LogicalTypeTableLimits, decode_logical_type_table_with_factory_in,
        };
        use novarocks_proto_models::value_type::{
            LogicalTypeDefinition, NominalValue, logical_type_definition::Kind,
        };
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "native scoped total graph",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let parent = tracker.current();
        let factory = NativeTypeFactory::new(&creation);
        let mut work = CompileCheckpoints::try_new(&creation, CompilePhase::Decode).unwrap();
        let mut definitions = vec![
            LogicalTypeDefinition {
                id: Some(0),
                nullable: false,
                kind: Some(Kind::Nominal(NominalValue::Json as i32)),
            },
            LogicalTypeDefinition {
                id: Some(u32::MAX),
                nullable: true,
                kind: Some(Kind::Nominal(NominalValue::Variant as i32)),
            },
        ];
        let limits = LogicalTypeTableLimits {
            max_definitions: 2,
            max_expanded_nodes: 1,
            max_string_bytes: 0,
        };
        assert!(
            decode_logical_type_table_with_factory_in(&definitions, limits, &factory, &mut work)
                .is_err(),
            "independent scopes do not reset the complete graph's expansion bound"
        );
        assert_eq!(tracker.current(), parent);
        definitions[1].kind = None;
        let limits = LogicalTypeTableLimits {
            max_expanded_nodes: 2,
            ..limits
        };
        assert!(
            decode_logical_type_table_with_factory_in(&definitions, limits, &factory, &mut work)
                .is_err(),
            "unused definitions remain validated by the sole graph body"
        );
        assert_eq!(tracker.current(), parent);
        drop(work);
        drop(factory);
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
mod writer_relation_creator_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_plan_codec::execution_type_table::{
        ExecutionTypeRoot, ExecutionTypeTableBuilder,
    };
    use novarocks_spi::connector::write_stack::{
        WRITE_RELATION_COLUMN_COUNT, WriterAuxiliaryChannel, WriterMultiplexSchema,
        write_relation_column_id,
    };
    use novarocks_type_contract::{
        CompileControlError, PureCompileControl, TypedValueBinding, ValueKind, ValueLayout,
        ValueLogicalType, ValueTypeRef,
    };
    use std::sync::Arc;
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    fn complete_values() -> Vec<FunctionValueType> {
        let json = ValueTypeRef::try_new(
            ValueKind::Nominal(ValueLogicalType::Json),
            false,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let list = ValueTypeRef::try_new(
            ValueKind::List(json),
            true,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let layout = ValueLayout::try_new(
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        vec![
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int32, false),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::try_from_typed_binding(
                TypedValueBinding::try_new(list, layout).unwrap(),
            )
            .unwrap(),
        ]
    }
    fn wire(
        values: &[FunctionValueType],
    ) -> (
        Vec<plan::ArrowPhysicalColumn>,
        Vec<novarocks_proto_models::value_type::LogicalTypeDefinition>,
    ) {
        // The original complete FE authors emit the real independent namespace.
        let roots = values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ExecutionTypeRoot {
                id: if ordinal < WRITE_RELATION_COLUMN_COUNT {
                    write_relation_column_id(ordinal)
                } else {
                    77
                },
                value,
            })
            .collect::<Vec<_>>();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let table = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed::<std::convert::Infallible>(
                ExecutionTypeTableLimits::for_validated_root_count(values.len()).unwrap(),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap()
            .into_wire();
        work.finish().unwrap();
        let names = [
            "kind",
            "write_target_ordinal",
            "row_count",
            "commit_fragment",
            "json_items",
        ];
        let columns = roots
            .iter()
            .enumerate()
            .map(|(ordinal, root)| {
                let binding = table
                    .value_bindings
                    .iter()
                    .find(|binding| binding.id == Some(root.id))
                    .unwrap();
                plan::ArrowPhysicalColumn {
                    slot_id: root.id,
                    is_internal: true,
                    field: Some(plan::ArrowPhysicalField {
                        name: names[ordinal].into(),
                        nullable: values[ordinal].nullable(),
                        r#type: binding.value_layout.clone().map(Box::new),
                        logical_type_id: binding.logical_type_id,
                        ..Default::default()
                    }),
                }
            })
            .collect();
        (columns, table.logical_types)
    }
    #[test]
    fn actual_writer_schema_and_independent_field_keep_original_creation_to_last_projection() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "writer relation independent projection",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let empty = decode_execution_type_table_observed(
            &plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &empty,
            original_requests: None,
            resources: &mut resources,
        };
        let values = complete_values();
        let (columns, logical) = wire(&values);
        let schema = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("writer"))
            .unwrap();
        let contract = decoder
            .writer_relation_contract(&schema, FieldPath::root("writer"))
            .unwrap();
        assert_eq!(contract.auxiliary_channels()[0].value_type(), &values[4]);
        assert_eq!(contract.arrow_schema().metadata().capacity(), 0);
        assert!(
            contract
                .arrow_schema()
                .fields()
                .iter()
                .all(|field| field.metadata().capacity() == 0)
        );
        let projection = contract.schema_projection();
        let field = projection.field(4).unwrap();
        drop(contract);
        drop(schema);
        drop(decoder);
        drop(empty);
        resources.finish().unwrap();
        drop(creation);
        assert!(tracker.current() > 0);
        assert_eq!(field.field().name(), "json_items");
        drop(projection);
        assert!(
            tracker.current() > 0,
            "independent field retains the original nested layout creator"
        );
        drop(field);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn actual_writer_contract_first_memory_refusal_preserves_installed_charge() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "writer contract first refusal",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let empty = decode_execution_type_table_observed(
            &plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &empty,
            original_requests: None,
            resources: &mut resources,
        };
        let (columns, logical) = wire(&complete_values());
        let schema = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("writer"))
            .unwrap();
        let before = tracker.current();
        tracker.install_limit_once(before + 1).unwrap();
        let error = decoder
            .writer_relation_contract(&schema, FieldPath::root("writer"))
            .unwrap_err();
        assert!(matches!(
            error.construction_cause(),
            Some(NativeTypeConstructionError::Capacity(_))
        ));
        assert_eq!(
            tracker.current(),
            before,
            "failed prospective Vec request rolls back before any field or set construction"
        );
        drop(schema);
        drop(decoder);
        drop(empty);
        resources.finish().unwrap();
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn actual_writer_consumer_rejects_same_layout_nested_json_text_drift() {
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "writer semantic drift",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let empty = decode_execution_type_table_observed(
            &plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &empty,
            original_requests: None,
            resources: &mut resources,
        };
        let values = complete_values();
        let (columns, logical) = wire(&values);
        let schema = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("writer"))
            .unwrap();
        let plain = FunctionValueType::new(values[4].data_type().clone(), true);
        let wrong = WriterMultiplexSchema::try_new(vec![
            WriterAuxiliaryChannel::try_new(77, "json_items", plain).unwrap(),
        ])
        .unwrap();
        assert!(
            wrong
                .validate_exact_arrow_schema(&schema.arrow_schema_ref())
                .is_ok(),
            "physical layout is intentionally identical"
        );
        assert!(novarocks_execution::exec::node::table_write_relation::WriterMultiplexRelationSchema::try_new_with_chunk_schema(wrong,schema.clone()).is_err());
        let correct = decoder
            .writer_relation_contract(&schema, FieldPath::root("writer"))
            .unwrap();
        assert!(novarocks_execution::exec::node::table_write_relation::WriterMultiplexRelationSchema::try_new_with_chunk_schema(correct,schema).is_ok());
    }
    #[test]
    fn join_scope_widens_only_root_and_keeps_finite_slot_until_last_reference() {
        let tracker =
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("join scope holder");
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let empty = decode_execution_type_table_observed(
            &plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &empty,
            original_requests: None,
            resources: &mut resources,
        };
        let mut values = complete_values();
        values[4] = values[4].with_nullable(false).unwrap();
        let (columns, logical) = wire(&values);
        let source = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("join_source"))
            .unwrap();
        let right = decoder
            .execution_relation_schema(&[], &[], FieldPath::root("empty_side"))
            .unwrap();
        let joined = decoder
            .join_scope_schema(&source, &right, true, false, FieldPath::root("join_scope"))
            .unwrap();
        assert!(!source.slots()[0].nullable());
        assert!(joined.slots()[0].nullable());
        assert!(
            decoder
                .bindings_match(
                    source.slots()[0].typed_binding(),
                    joined.slots()[0].typed_binding(),
                    true
                )
                .unwrap()
        );
        assert_eq!(joined.slots()[0].unique_id(), source.slots()[0].unique_id());
        assert!(
            decoder
                .bindings_match(
                    source.slots()[4].typed_binding(),
                    joined.slots()[4].typed_binding(),
                    true
                )
                .unwrap()
        );
        assert_eq!(joined.arrow_schema_ref().metadata().capacity(), 0);
        assert!(!source.slots()[4].nullable());
        assert!(joined.slots()[4].nullable());
        let nested =
            FunctionValueType::try_from_typed_binding(joined.slots()[4].typed_binding().clone())
                .unwrap();
        assert_eq!(
            nested.child_at(0).unwrap().logical_type(),
            ValueLogicalType::Json
        );
        assert!(!nested.child_at(0).unwrap().nullable());
        drop(nested);
        let retained = joined.slots()[4].clone();
        drop(joined);
        drop(source);
        drop(right);
        drop(decoder);
        drop(empty);
        resources.finish().unwrap();
        drop(creation);
        assert!(
            tracker.current() > 0,
            "independent slot keeps its actual finite creators"
        );
        drop(retained);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn join_scope_first_actual_memory_refusal_preserves_installed_source() {
        let tracker =
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("join scope refusal");
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let empty = decode_execution_type_table_observed(
            &plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &empty,
            original_requests: None,
            resources: &mut resources,
        };
        let (columns, logical) = wire(&complete_values());
        let source = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("join_source"))
            .unwrap();
        let right = decoder
            .execution_relation_schema(&[], &[], FieldPath::root("empty_side"))
            .unwrap();
        let before = tracker.current();
        tracker.install_limit_once(before + 1).unwrap();
        let error = decoder
            .join_scope_schema(&source, &right, true, false, FieldPath::root("join_scope"))
            .unwrap_err();
        assert!(matches!(
            error.construction_cause(),
            Some(NativeTypeConstructionError::Capacity(_))
        ));
        assert_eq!(tracker.current(), before);
        assert!(
            !source.slots()[0].nullable(),
            "failed construction leaves source unchanged"
        );
        drop(source);
        drop(right);
        drop(decoder);
        drop(empty);
        resources.finish().unwrap();
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn nested_join_same_slot_cannot_reauthorize_text_as_json() {
        let actual =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let wrong = FunctionValueType::new(DataType::Utf8, false);
        let roots = [
            ExecutionTypeRoot {
                id: 900,
                value: &actual,
            },
            ExecutionTypeRoot {
                id: 901,
                value: &wrong,
            },
        ];
        let mut sender = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let table = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed::<std::convert::Infallible>(
                ExecutionTypeTableLimits::for_validated_root_count(2).unwrap(),
                &mut |_| Ok(()),
                &mut sender,
            )
            .unwrap()
            .into_wire();
        sender.finish().unwrap();
        let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
            "nested join exact domain",
        );
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &Control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let types = decode_execution_type_table_observed(
            &table,
            ExecutionTypeTableLimits::for_validated_root_count(2).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &types,
            original_requests: None,
            resources: &mut resources,
        };
        let (columns, logical) = wire(&[actual]);
        let source = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("join_source"))
            .unwrap();
        let physical = plan::PlanNode {
            output_columns: vec![common::OutputColumn {
                column_id: columns[0].slot_id,
                name: "kind".into(),
                nullable: false,
                is_internal: true,
                r#type: Some(common::TypeDesc {
                    value_binding_id: Some(901),
                }),
            }],
            ..Default::default()
        };
        let error = crate::fragment_plan_node::join_output_chunk_schema(
            &physical,
            source.clone(),
            FieldPath::root("join_output"),
            &mut decoder,
        )
        .unwrap_err();
        assert!(error.contains("join output differs from its complete source binding"));
        assert_eq!(
            source.slot_ids(),
            &[novarocks_types::SlotId::new(columns[0].slot_id)]
        );
        drop(source);
        drop(decoder);
        drop(types);
        resources.finish().unwrap();
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn join_scope_pending_control_precedes_first_memory_request() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct PendingControl(AtomicBool);
        impl PureCompileControl for PendingControl {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                if units > 0 && self.0.load(Ordering::SeqCst) {
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        let control = PendingControl(AtomicBool::new(false));
        let tracker =
            novarocks_execution::runtime::mem_tracker::MemTracker::new_root("join pending control");
        let creation = NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
        let mut resources = NativeDecodeResources::try_new(&creation).unwrap();
        let empty = decode_execution_type_table_observed(
            &plan::ExecutionTypeTable::default(),
            ExecutionTypeTableLimits::for_validated_root_count(0).unwrap(),
            resources.storage(),
            &mut request_port(&creation),
            &mut resources.work,
        )
        .unwrap();
        let mut decoder = NativeTypeDecoder::Strong {
            types: &empty,
            original_requests: None,
            resources: &mut resources,
        };
        let (columns, logical) = wire(&complete_values());
        let source = decoder
            .execution_relation_schema(&columns, &logical, FieldPath::root("join_source"))
            .unwrap();
        let right = decoder
            .execution_relation_schema(&[], &[], FieldPath::root("empty_side"))
            .unwrap();
        {
            let NativeTypeDecoder::Strong { resources, .. } = &mut decoder;
            resources.work.step().unwrap();
        }
        let before = tracker.current();
        tracker.install_limit_once(before + 1).unwrap();
        control.0.store(true, Ordering::SeqCst);
        let error = decoder
            .join_scope_schema(&source, &right, true, false, FieldPath::root("join_scope"))
            .unwrap_err();
        assert!(matches!(
            error.construction_cause(),
            Some(NativeTypeConstructionError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(
            tracker.current(),
            before,
            "original pending control wins before scope reservation"
        );
        drop(source);
        drop(right);
        drop(decoder);
        drop(empty);
        assert!(resources.finish().is_err());
        drop(creation);
        assert_eq!(tracker.current(), 0);
    }
}
