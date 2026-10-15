// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
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

//! Fragment-local original requests and admitted constant sources. IDs address
//! this immutable owner only; no state-phase input can reconstruct a request.

use crate::{
    fragment_error::NativeFragmentDecodeError as Error,
    fragment_type_decode::{NativeDecodeResources, clone_type},
    native_type_construction::NativeTypeConstructionError as Construction,
};
use novarocks_physical_plan::{ConstantPools, PhysicalCallRequest};
use novarocks_plan_codec::physical_call_requests_v2::DecodedCallRequests;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::call_request;
use novarocks_type_contract::{
    ValueTypeStorageLifetime, owned_resources::layout::LayoutResourceError,
};
use std::alloc::Layout;

#[path = "fragment_original_requests/scalar.rs"]
pub(crate) mod scalar;
pub(crate) enum OriginalOccurrence {
    Aggregate { node: i32, call: usize },
    Window { expression: Option<u32> },
    WriterPartial { node: i32, call: usize },
    WriterFinal { node: i32, call: usize },
}
impl OriginalOccurrence {
    pub(crate) fn definition(
        self,
    ) -> Result<novarocks_physical_plan::PhysicalCallDefinition, Error> {
        use novarocks_physical_plan::{
            ExprId, NodeId, PhysicalCallDefinition as Definition, PhysicalCallSite as Site,
        };
        let relational = |node: i32, call: usize| -> Result<(NodeId, u32), Error> {
            Ok((
                NodeId::new(
                    u32::try_from(node)
                        .map_err(|_| invalid("original occurrence node identity is negative"))?,
                ),
                u32::try_from(call).map_err(|_| arithmetic())?,
            ))
        };
        Ok(match self {
            Self::Window { expression } => Definition::Expression(ExprId::new(
                expression
                    .ok_or_else(|| invalid("window original expression definition is absent"))?,
            )),
            Self::Aggregate { node, call } => {
                let (node, call) = relational(node, call)?;
                Definition::Relational(Site::Aggregate { node, call })
            }
            Self::WriterPartial { node, call } => {
                let (node, call) = relational(node, call)?;
                Definition::Relational(Site::WriterPartial { node, call })
            }
            Self::WriterFinal { node, call } => {
                let (node, call) = relational(node, call)?;
                Definition::Relational(Site::WriterFinal { node, call })
            }
        })
    }
}

pub(crate) struct NativeOriginalRequests {
    requests: DecodedCallRequests,
    pools: ConstantPools,
    ids: Vec<(u32, usize)>,
    _storage: ValueTypeStorageLifetime,
}
fn arithmetic() -> Error {
    Construction::Resource(LayoutResourceError::Arithmetic).into()
}
fn invalid(message: &'static str) -> Error {
    Error::invalid_value(
        FieldPath::root("plan_fragment").field("original_call_requests"),
        message,
    )
}
/// Preserve the actual creator/control cause; resource-model errors are
/// projected by their typed variants, never through diagnostic inspection.
pub(crate) fn frozen_validation_failure(
    cause: novarocks_functions::FunctionBindingCreationFailure<Construction>,
    path: FieldPath,
) -> Error {
    use novarocks_functions::{
        FunctionBindingConstructionResource as R, FunctionBindingCreationFailure as F,
    };
    use novarocks_type_contract::{
        ControlResourceError, owned_resources::hashmap::HashMapResourceError,
    };
    match cause {
        F::Creator(cause) => Error::from(cause),
        F::Binding(novarocks_functions::FunctionBindingError::Control(cause)) => {
            Error::from(Construction::Control(cause))
        }
        F::Binding(cause) => Error::invalid_value(path, cause),
        F::Resource(R::Layout(cause)) => Error::from(Construction::Resource(cause)),
        F::Resource(R::Vec(ControlResourceError::Control(cause))) => {
            Error::from(Construction::Control(cause))
        }
        F::Resource(R::Vec(ControlResourceError::SourceModel(_)))
        | F::Resource(R::HashMap(HashMapResourceError::SourceModel(_))) => {
            Error::from(Construction::Resource(LayoutResourceError::SourceModel))
        }
        F::Resource(R::HashMap(HashMapResourceError::Arithmetic(_))) => {
            Error::from(Construction::Resource(LayoutResourceError::Arithmetic))
        }
        F::UnconvertedResolverOwner => Error::invalid_value(
            path,
            "frozen validation construction owner is not converted",
        ),
    }
}
fn step(resources: &mut NativeDecodeResources<'_>) -> Result<(), Error> {
    resources.work.step().map_err(Construction::Control)?;
    Ok(())
}
fn vector<T>(count: usize, resources: &mut NativeDecodeResources<'_>) -> Result<Vec<T>, Error> {
    if count == 0 {
        return Ok(Vec::new());
    }
    resources.request(Layout::array::<T>(count).map_err(|_| arithmetic())?)?;
    let mut values = Vec::new();
    let result = values.try_reserve_exact(count);
    if result.is_ok() {
        step(resources)?;
    }
    resources.work.flush().map_err(Construction::Control)?;
    result.map_err(|cause| Construction::Capacity(cause.to_string()))?;
    Ok(values)
}
impl NativeOriginalRequests {
    /// Publish the successfully decoded entries and pools without cloning either
    /// namespace. All IDs, including unused sparse IDs and zero, are validated.
    pub(crate) fn try_new(
        source: &call_request::FragmentCallRequests,
        requests: DecodedCallRequests,
        pools: ConstantPools,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<Self, Error> {
        step(resources)?;
        if source.entries.len() != requests.entries().len() {
            return Err(invalid(
                "original request source and decoded entries differ",
            ));
        }
        let mut ids = vector(source.entries.len(), resources)?;
        for (ordinal, entry) in source.entries.iter().enumerate() {
            step(resources)?;
            let id = entry
                .request_id
                .ok_or_else(|| invalid("original request identity is absent"))?;
            ids.push((id, ordinal));
        }
        fn sift(
            values: &mut [(u32, usize)],
            mut root: usize,
            end: usize,
            resources: &mut NativeDecodeResources<'_>,
        ) -> Result<(), Error> {
            while root < end / 2 {
                let mut child = root * 2 + 1;
                if child + 1 < end {
                    step(resources)?;
                    if values[child].0 < values[child + 1].0 {
                        child += 1;
                    }
                }
                step(resources)?;
                if values[root].0 >= values[child].0 {
                    break;
                }
                values.swap(root, child);
                step(resources)?;
                root = child;
            }
            Ok(())
        }
        let len = ids.len();
        for root in (0..len / 2).rev() {
            sift(&mut ids, root, len, resources)?;
        }
        for end in (1..len).rev() {
            ids.swap(0, end);
            step(resources)?;
            sift(&mut ids, 0, end, resources)?;
        }
        for pair in ids.windows(2) {
            step(resources)?;
            if pair[0].0 == pair[1].0 {
                return Err(invalid("duplicate original request identity"));
            }
        }
        Ok(Self {
            requests,
            pools,
            ids,
            _storage: resources.storage(),
        })
    }
    fn entry(
        &self,
        id: u32,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<
        &(
            novarocks_physical_plan::PhysicalCallDefinition,
            PhysicalCallRequest,
        ),
        Error,
    > {
        let (mut low, mut high) = (0, self.ids.len());
        while low < high {
            let middle = low + (high - low) / 2;
            step(resources)?;
            match self.ids[middle].0.cmp(&id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    return Ok(&self.requests.entries()[self.ids[middle].1]);
                }
            }
        }
        Err(invalid("dangling original request identity"))
    }
    pub(crate) fn request(
        &self,
        id: u32,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<&PhysicalCallRequest, Error> {
        Ok(&self.entry(id, resources)?.1)
    }
    pub(crate) fn validate_definition(
        &self,
        id: u32,
        expected: novarocks_physical_plan::PhysicalCallDefinition,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<(), Error> {
        let actual = self.entry(id, resources)?.0;
        step(resources)?;
        if actual != expected {
            return Err(invalid(
                "aggregate original request belongs to a different definition",
            ));
        }
        Ok(())
    }
    pub(crate) fn constant(
        &self,
        reference: &call_request::ConstantReference,
        expected: &novarocks_functions::FunctionValueType,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<novarocks_functions::ConstantValue, Error> {
        step(resources)?;
        let pool = reference
            .pool_id
            .ok_or_else(|| invalid("constant pool reference is absent"))?;
        self.pools
            .resolve_observed(
                novarocks_physical_plan::ConstantReference {
                    pool: novarocks_physical_plan::ConstantPoolId::new(pool),
                    ordinal: reference.row_ordinal,
                },
                expected,
                &mut resources.work,
            )
            .map_err(|cause| match cause {
                novarocks_physical_plan::ConstantReferenceError::Control(cause)
                | novarocks_physical_plan::ConstantReferenceError::Constant(
                    novarocks_functions::ConstantError::Control(cause),
                ) => Construction::Control(cause).into(),
                other => Error::invalid_value(
                    novarocks_proto_codec::FieldPath::root("expr").field("constant_reference"),
                    other,
                ),
            })
    }
    /// Direct constant addresses are actual value sources. Computed values,
    /// including casts, remain None under the physical argument author's law.
    pub(crate) fn validate_actual_arguments<'a>(
        &self,
        id: u32,
        actual: impl Iterator<Item = &'a novarocks_proto_models::expr::Expr>,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<(), Error> {
        let original = self.request(id, resources)?;
        let mut actual = actual;
        for expected in original.arguments.iter() {
            step(resources)?;
            let expression = actual
                .next()
                .ok_or_else(|| invalid("aggregate actual argument is absent"))?;
            let novarocks_physical_plan::StaticFunctionArgument::Value { constant, .. } = expected
            else {
                return Err(invalid("installed aggregate argument cannot be a lambda"));
            };
            let actual = match &expression.kind {
                Some(novarocks_proto_models::expr::expr::Kind::ConstantReference(reference)) => {
                    let pool = reference
                        .pool_id
                        .ok_or_else(|| invalid("actual constant pool reference is absent"))?;
                    Some(novarocks_physical_plan::ConstantReference {
                        pool: novarocks_physical_plan::ConstantPoolId::new(pool),
                        ordinal: reference.row_ordinal,
                    })
                }
                Some(novarocks_proto_models::expr::expr::Kind::Literal(_)) => {
                    return Err(invalid(
                        "strong aggregate argument cannot use an inline literal",
                    ));
                }
                _ => None,
            };
            step(resources)?;
            if *constant != actual {
                return Err(invalid(
                    "aggregate actual constant address differs from its original request",
                ));
            }
        }
        step(resources)?;
        if actual.next().is_some() {
            return Err(invalid("aggregate has extra actual arguments"));
        }
        Ok(())
    }
    /// Validate the wire-selected full binding against this exact original
    /// request once before publishing its neutral frozen selection owner.
    pub(crate) fn validate_selection(
        &self,
        id: u32,
        catalog: &novarocks_functions::EngineFunctionCatalog,
        function_id: &novarocks_functions::FunctionId,
        selected: &novarocks_functions::FunctionBindingSelection,
        logical_argument_count: usize,
        resources: &mut NativeDecodeResources<'_>,
    ) -> Result<(), Error> {
        let creation = resources.creation;
        self.with_catalog_request(id, resources, |request| {
            if request.logical_argument_count != logical_argument_count {
                return Err(invalid(
                    "frozen signature logical count differs from original request",
                ));
            }
            // Stack-only factory loans the original BE tracker/control. The
            // catalog's same header body must finish before it begins the
            // validation-only scope at the actual resolver-construction slot.
            let factory = crate::native_type_construction::NativeTypeFactory::new(creation);
            catalog
                .validate_frozen_selection_with_factory_in(
                    function_id,
                    novarocks_functions::FunctionKind::Aggregate,
                    selected,
                    request,
                    &factory,
                    creation,
                )
                .map_err(|cause| {
                    frozen_validation_failure(
                        cause,
                        FieldPath::root("plan_fragment").field("original_call_requests"),
                    )
                })
        })
    }

    /// Borrow genuine Value/NULL/None, Lambda and optional-result facts. Only
    /// checked pool addresses become admitted ConstantValues; state arguments
    /// never substitute for the original logical request.
    pub(crate) fn with_catalog_request<T>(
        &self,
        id: u32,
        resources: &mut NativeDecodeResources<'_>,
        consume: impl FnOnce(novarocks_functions::FunctionBindingRequest<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        use novarocks_functions::FunctionArgument as Argument;
        use novarocks_physical_plan::StaticFunctionArgument as Source;
        let original = self.request(id, resources)?;
        let mut arguments = vector(original.arguments.len(), resources)?;
        for argument in original.arguments.iter() {
            step(resources)?;
            arguments.push(match argument {
                Source::Value {
                    value_type,
                    constant,
                } => Argument::Value {
                    value_type: clone_type(value_type, resources)?,
                    constant: constant
                        .map(|reference| {
                            self.pools
                                .resolve_observed(reference, value_type, &mut resources.work)
                                .map_err(|error| match error {
                                    novarocks_physical_plan::ConstantReferenceError::Control(
                                        cause,
                                    ) => Construction::Control(cause).into(),
                                    novarocks_physical_plan::ConstantReferenceError::Constant(
                                        novarocks_functions::ConstantError::Control(cause),
                                    ) => Construction::Control(cause).into(),
                                    other => Error::invalid_value(
                                        FieldPath::root("plan_fragment").field("constants"),
                                        other,
                                    ),
                                })
                        })
                        .transpose()?,
                },
                Source::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    let mut parameters = vector(parameter_types.len(), resources)?;
                    for parameter in parameter_types.iter() {
                        parameters.push(clone_type(parameter, resources)?);
                    }
                    if parameters.capacity() > parameters.len() {
                        resources.request(
                            Layout::array::<novarocks_functions::FunctionValueType>(
                                parameters.len(),
                            )
                            .map_err(|_| arithmetic())?,
                        )?;
                    }
                    resources.work.flush().map_err(Construction::Control)?;
                    let parameters = parameters.into_boxed_slice();
                    let parameters =
                        novarocks_type_contract::FunctionParameterTypes::try_from_boxed_observed(
                            parameters,
                            resources.storage(),
                            |layout| resources.request(layout),
                        )?;
                    step(resources)?;
                    Argument::Lambda {
                        parameter_types: parameters,
                        result_type: clone_type(result_type, resources)?,
                    }
                }
            });
        }
        consume(novarocks_functions::FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: original.logical_argument_count,
            expected_result_type: original.expected_result_type.as_ref(),
        })
    }
}

/// Decode the complete wire selection, validate it against the admitted
/// original request, then retain exactly this selection through local lowering.
pub(crate) fn decode_frozen_selection(
    signature: &novarocks_proto_models::plan::ResolvedAggregateSignature,
    types: &novarocks_plan_codec::execution_type_table::DecodedExecutionTypeTable,
    requests: &NativeOriginalRequests,
    catalog: &novarocks_functions::EngineFunctionCatalog,
    path: FieldPath,
    definition: novarocks_physical_plan::PhysicalCallDefinition,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<std::sync::Arc<novarocks_local_program::FrozenAggregateSelection>, Error> {
    use novarocks_functions::{
        AggregateBindingSelection, FunctionArgumentType, FunctionBindingSelection,
        FunctionResultType,
    };
    use novarocks_local_program::FrozenAggregateSelection;
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    use novarocks_type_contract::{AggregateStateFormatId, FunctionId, FunctionOverloadId};
    use std::sync::Arc;
    enum IdentityCreationError {
        Grammar(novarocks_type_contract::FunctionIdentityError),
        Original(Construction),
    }
    impl From<novarocks_type_contract::FunctionIdentityError> for IdentityCreationError {
        fn from(value: novarocks_type_contract::FunctionIdentityError) -> Self {
            Self::Grammar(value)
        }
    }
    impl From<LayoutResourceError> for IdentityCreationError {
        fn from(value: LayoutResourceError) -> Self {
            Self::Original(Construction::Resource(value))
        }
    }
    fn identity<T>(
        value: &str,
        path: FieldPath,
        resources: &mut NativeDecodeResources<'_>,
        construct: impl FnOnce(
            &str,
            ValueTypeStorageLifetime,
            &mut dyn FnMut(Layout) -> Result<(), IdentityCreationError>,
            &mut dyn FnMut(usize) -> Result<(), IdentityCreationError>,
        ) -> Result<T, IdentityCreationError>,
    ) -> Result<T, Error> {
        resources.work.flush().map_err(Construction::Control)?;
        let original = resources.creation;
        let storage = resources.storage();
        let mut request = |layout| {
            original
                .request_layout(layout)
                .map_err(IdentityCreationError::Original)
        };
        let mut work = |units| {
            for _ in 0..units {
                resources.work.step().map_err(|cause| {
                    IdentityCreationError::Original(Construction::Control(cause))
                })?;
            }
            Ok(())
        };
        let result = construct(value, storage, &mut request, &mut work);
        match result {
            // The identity author returned an original refusal. No completed
            // footer may replace it with a subsequent control callback.
            Err(IdentityCreationError::Original(cause)) => Err(cause.into()),
            result => {
                resources.work.flush().map_err(Construction::Control)?;
                result.map_err(|cause| match cause {
                    IdentityCreationError::Grammar(cause) => Error::invalid_value(path, cause),
                    IdentityCreationError::Original(_) => unreachable!("handled original refusal"),
                })
            }
        }
    }
    let request_id = signature.original_request_id.ok_or_else(|| {
        Error::missing(
            path.clone().field("original_request_id"),
            "aggregate original request reference is absent",
        )
    })?;
    requests.validate_definition(request_id, definition, resources)?;
    let logical_count = signature.logical_argument_count.ok_or_else(|| {
        Error::missing(
            path.clone().field("logical_argument_count"),
            "aggregate original logical count is absent",
        )
    })? as usize;
    let function_id = identity(
        &signature.function_id,
        path.clone().field("function_id"),
        resources,
        |value, storage, request, work| FunctionId::try_new_observed(value, storage, request, work),
    )?;
    let overload = identity(
        &signature.overload_identity,
        path.clone().field("overload_identity"),
        resources,
        |value, storage, request, work| {
            FunctionOverloadId::try_new_observed(value, storage, request, work)
        },
    )?;
    let state_format = identity(
        &signature.state_format_identity,
        path.clone().field("state_format_identity"),
        resources,
        |value, storage, request, work| {
            AggregateStateFormatId::try_new_observed(value, storage, request, work)
        },
    )?;
    let contract =
        novarocks_plan_codec::physical_aggregate_binding_v2::decode_state_argument_contract(
            signature.state_argument_contract,
        )
        .map_err(|error| {
            Error::invalid_value(path.clone().field("state_argument_contract"), error)
        })?;
    let mut arguments = vector(signature.argument_types.len(), resources)?;
    for (index, desc) in signature.argument_types.iter().enumerate() {
        let value = crate::fragment_type_decode::resolve_type(
            types,
            desc,
            path.clone().field("argument_types").index(index),
            resources,
        )?;
        arguments.push(FunctionArgumentType::Value(clone_type(value, resources)?));
    }
    if arguments.capacity() > arguments.len() {
        resources.request(
            Layout::array::<FunctionArgumentType>(arguments.len()).map_err(|_| arithmetic())?,
        )?;
    }
    resources.work.flush().map_err(Construction::Control)?;
    let arguments = arguments.into_boxed_slice();
    let arguments = novarocks_type_contract::FunctionArgumentTypes::try_from_boxed_observed(
        arguments,
        resources.storage(),
        |layout| resources.request(layout),
    )
    .map_err(|cause: Error| cause)?;
    step(resources)?;
    let output = signature.output_type.as_ref().ok_or_else(|| {
        Error::missing(
            path.clone().field("output_type"),
            "aggregate output binding is absent",
        )
    })?;
    let intermediate = signature.intermediate_type.as_ref().ok_or_else(|| {
        Error::missing(
            path.clone().field("intermediate_type"),
            "aggregate intermediate binding is absent",
        )
    })?;
    let output = crate::fragment_type_decode::resolve_type(
        types,
        output,
        path.clone().field("output_type"),
        resources,
    )?;
    let output = clone_type(output, resources)?;
    let intermediate = crate::fragment_type_decode::resolve_type(
        types,
        intermediate,
        path.clone().field("intermediate_type"),
        resources,
    )?;
    let intermediate = clone_type(intermediate, resources)?;
    let selected = FunctionBindingSelection {
        overload,
        argument_types: arguments,
        result_type: FunctionResultType::Scalar(output),
        aggregate: Some(AggregateBindingSelection {
            state_argument_contract: contract,
            intermediate_type: intermediate,
            state_format,
        }),
    };
    requests.validate_selection(
        request_id,
        catalog,
        &function_id,
        &selected,
        logical_count,
        resources,
    )?;
    // Only publish after genuine catalog validation; representation construction
    // cannot establish semantics or substitute phase-state channels.
    resources.request(
        arc_layout(Layout::new::<FunctionBindingSelection>()).map_err(Construction::Resource)?,
    )?;
    let selected = Arc::new(selected);
    let frozen = FrozenAggregateSelection::try_new(
        function_id,
        selected,
        logical_count,
        resources.storage(),
    )
    .map_err(|error| Error::invalid_value(path, error))?;
    resources.request(
        FrozenAggregateSelection::storage_allocation_layout().map_err(Construction::Resource)?,
    )?;
    Ok(Arc::new(frozen))
}

/// The kernel's carrier specialization is a projection of the validated full
/// selection. It cannot select an overload or establish a nominal domain.
pub(crate) fn physical_projection(
    frozen: &novarocks_local_program::FrozenAggregateSelection,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<novarocks_functions::ResolvedAggregateSignature, Error> {
    use novarocks_functions::{
        AggregateOverloadIdentity, AggregateStateFormatIdentity, FunctionArgumentType,
        FunctionResultType, ResolvedAggregateSignature,
    };
    let selected = frozen.selected();
    let aggregate = selected
        .aggregate
        .as_ref()
        .ok_or_else(|| invalid("frozen aggregate state is absent"))?;
    let mut arguments = vector(selected.argument_types.len(), resources)?;
    for argument in selected.argument_types.iter() {
        step(resources)?;
        let FunctionArgumentType::Value(value) = argument else {
            return Err(invalid(
                "installed aggregate kernel cannot specialize a lambda argument",
            ));
        };
        // Normalized layout children are shared Arcs, with no Dictionary/REE Box.
        arguments.push(value.data_type().clone());
    }
    let FunctionResultType::Scalar(output) = &selected.result_type else {
        return Err(invalid(
            "installed aggregate kernel requires a scalar result",
        ));
    };
    for identity in [selected.overload.as_str(), aggregate.state_format.as_str()] {
        for _ in identity.bytes() {
            step(resources)?;
        }
        resources.request(Layout::array::<u8>(identity.len()).map_err(|_| arithmetic())?)?;
    }
    let overload = AggregateOverloadIdentity::try_new(selected.overload.as_str())
        .map_err(|error| invalid_projection(error))?;
    let state_format = AggregateStateFormatIdentity::try_new(aggregate.state_format.as_str())
        .map_err(|error| invalid_projection(error))?;
    step(resources)?;
    Ok(ResolvedAggregateSignature {
        overload,
        argument_types: arguments,
        intermediate_type: aggregate.intermediate_type.data_type().clone(),
        output_type: output.data_type().clone(),
        state_format,
    })
}
fn invalid_projection(error: impl std::fmt::Display) -> Error {
    Error::invalid_value(
        FieldPath::root("plan_fragment").field("resolved_signature"),
        error,
    )
}
