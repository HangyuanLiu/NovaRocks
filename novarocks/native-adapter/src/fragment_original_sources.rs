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

//! Actual public backing of the flat source-only generated DTOs. This observer
//! covers live input, not historical protobuf requests or allocator usable size.
use crate::{
    fragment_error::NativeFragmentDecodeError as Error,
    fragment_type_decode::NativeDecodeResources,
    native_type_construction::NativeTypeConstructionError as CreationError,
};
use novarocks_proto_models::{call_request, physical_type_v2 as wire, plan};
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use std::{alloc::Layout, mem::size_of_val};
fn arithmetic() -> Error {
    CreationError::Resource(LayoutResourceError::Arithmetic).into()
}
fn step(resources: &mut NativeDecodeResources<'_>) -> Result<(), Error> {
    resources.work.step().map_err(CreationError::Control)?;
    Ok(())
}
fn add(total: &mut usize, bytes: usize) -> Result<(), Error> {
    *total = total.checked_add(bytes).ok_or_else(arithmetic)?;
    Ok(())
}
fn vector<T>(
    value: &Vec<T>,
    total: &mut usize,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<(), Error> {
    step(resources)?;
    add(
        total,
        Layout::array::<T>(value.capacity())
            .map_err(|_| arithmetic())?
            .size(),
    )
}
fn string(
    value: &String,
    total: &mut usize,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<(), Error> {
    step(resources)?;
    add(total, value.capacity())
}
pub(crate) fn raw_source_bytes(
    fragment: &plan::PlanFragment,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<usize, Error> {
    let mut total = 0;
    if let Some(source_table) = &fragment.constant_source_fields {
        step(resources)?;
        add(&mut total, size_of_val(source_table))?;
        vector(&source_table.field_facts, &mut total, resources)?;
        for facts in &source_table.field_facts {
            step(resources)?;
            vector(&facts.metadata, &mut total, resources)?;
            for entry in &facts.metadata {
                string(&entry.key, &mut total, resources)?;
                string(&entry.value, &mut total, resources)?;
            }
        }
        let table = source_table.shape.as_ref().ok_or_else(|| {
            Error::inconsistent(
                novarocks_proto_codec::FieldPath::root("plan_fragment")
                    .field("constant_source_fields")
                    .field("shape"),
                "source shape is absent",
            )
        })?;
        // The isolated source namespace has no logical/value author, even for
        // unused entries. Reject before interpreting any forbidden dynamic tree.
        if !table.logical_types.is_empty() || !table.value_types.is_empty() {
            return Err(Error::inconsistent(
                novarocks_proto_codec::FieldPath::root("plan_fragment")
                    .field("constant_source_fields"),
                "constant source owner contains execution definitions",
            ));
        }
        add(&mut total, size_of_val(table))?;
        vector(&table.carriers, &mut total, resources)?;
        vector(&table.fields, &mut total, resources)?;
        vector(&table.value_types, &mut total, resources)?;
        vector(&table.logical_types, &mut total, resources)?;
        for carrier in &table.carriers {
            step(resources)?;
            use wire::carrier_type_definition::Kind;
            match &carrier.kind {
                Some(Kind::Timestamp(value)) => {
                    if let Some(zone) = &value.timezone {
                        string(zone, &mut total, resources)?;
                    }
                }
                Some(Kind::StructType(value)) => vector(&value.field_ids, &mut total, resources)?,
                Some(Kind::UnionType(value)) => vector(&value.fields, &mut total, resources)?,
                Some(
                    Kind::Primitive(_)
                    | Kind::Time32(_)
                    | Kind::Time64(_)
                    | Kind::Duration(_)
                    | Kind::Interval(_)
                    | Kind::FixedSizeBinary(_)
                    | Kind::Decimal32(_)
                    | Kind::Decimal64(_)
                    | Kind::Decimal128(_)
                    | Kind::Decimal256(_)
                    | Kind::ListFieldId(_)
                    | Kind::ListViewFieldId(_)
                    | Kind::FixedSizeList(_)
                    | Kind::LargeListFieldId(_)
                    | Kind::LargeListViewFieldId(_)
                    | Kind::Dictionary(_)
                    | Kind::Map(_)
                    | Kind::RunEndEncoded(_),
                )
                | None => {}
            }
        }
        for field in &table.fields {
            step(resources)?;
            string(&field.name, &mut total, resources)?;
        }
    }
    vector(&fragment.constants, &mut total, resources)?;
    for record in &fragment.constants {
        vector(&record.arrow_ipc, &mut total, resources)?;
    }
    if let Some(requests) = &fragment.original_call_requests {
        add(&mut total, size_of_val(requests))?;
        vector(&requests.entries, &mut total, resources)?;
        for entry in &requests.entries {
            step(resources)?;
            // Definition, relational CallSite, ConstantPolicy and ConstantRef
            // are generated inline Option/oneof scalar-only objects.
            vector(&entry.arguments, &mut total, resources)?;
            for argument in &entry.arguments {
                step(resources)?;
                match &argument.kind {
                    Some(call_request::original_function_argument::Kind::Lambda(value)) => {
                        vector(&value.parameter_value_type_ids, &mut total, resources)?
                    }
                    Some(call_request::original_function_argument::Kind::Value(_)) | None => {}
                }
            }
        }
    }
    Ok(total)
}

fn with_creation<'control, T, C>(
    resources: &mut NativeDecodeResources<'control>,
    body: impl FnOnce(
        &mut novarocks_type_contract::ValueSchemaCreation<'_, CreationError>,
        &mut novarocks_type_contract::CompileCheckpoints<'control>,
    ) -> Result<
        T,
        novarocks_plan_codec::host_projection_v2::ProjectionFailure<C, CreationError>,
    >,
) -> Result<(T, usize), novarocks_plan_codec::host_projection_v2::ProjectionFailure<C, CreationError>>
{
    use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
    resources
        .work
        .flush()
        .map_err(|error| ProjectionFailure::Host(CreationError::Control(error)))?;
    let original = resources.creation;
    let (mut exact, mut opaque) = (0usize, 0usize);
    let arithmetic = || CreationError::Resource(LayoutResourceError::Arithmetic);
    let mut request = |layout: Layout| {
        let next = exact.checked_add(layout.size()).ok_or_else(arithmetic)?;
        original.request_layout(layout)?;
        exact = next;
        Ok(())
    };
    let mut upper = |bytes: usize| {
        let next = opaque.checked_add(bytes).ok_or_else(arithmetic)?;
        original.request_opaque_upper_bound(bytes)?;
        opaque = next;
        Ok(())
    };
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        resources.storage(),
        &mut request,
        &mut upper,
    );
    let value = body(&mut creation, &mut resources.work)?;
    drop(creation);
    Ok((
        value,
        exact
            .checked_add(opaque)
            .ok_or_else(|| ProjectionFailure::Host(arithmetic()))?,
    ))
}

/// One fragment's original request namespace borrows the genuine independent
/// source fields and complete execution types. The reader is the shared sole
/// IPC body; all new arrays, pool/table/index holders retain actual BE storage.
pub(crate) fn decode_original_requests(
    fragment: &plan::PlanFragment,
    source_types: &crate::fragment_type_decode::NativeFragmentSourceTypes,
    resources: &mut NativeDecodeResources<'_>,
) -> Result<std::sync::Arc<crate::fragment_original_requests::NativeOriginalRequests>, Error> {
    use novarocks_physical_plan::{ConstantPool, ConstantPoolId, ConstantPools};
    use novarocks_plan_codec::{
        host_projection_v2::ProjectionFailure, physical_call_requests_v2, physical_constant_v2,
        physical_type_v2,
    };
    use novarocks_proto_codec::FieldPath;
    let path = FieldPath::root("plan_fragment");
    let mut source = raw_source_bytes(fragment, resources)?;
    add(&mut source, size_of_val(&source_types.execution))?;
    add(
        &mut source,
        source_types.construction_request_bytes_upper_bound,
    )?;
    let raw_fields = fragment.constant_source_fields.as_ref().ok_or_else(|| {
        Error::missing(
            path.clone().field("constant_source_fields"),
            "fragment constant source field owner is absent",
        )
    })?;
    let source_factory =
        crate::native_type_construction::NativeTypeFactory::new(resources.creation);
    let (fields, _main_field_requests) = with_creation(resources, |creation, work| {
        physical_type_v2::decode_source_field_table_input_derived_with_factory(
            raw_fields,
            source,
            creation,
            &source_factory,
            work,
        )
    })
    .map_err(|error| match error {
        ProjectionFailure::Host(cause) => Error::from(cause),
        ProjectionFailure::Codec(physical_type_v2::TypeCodecError::Control(cause)) => {
            CreationError::Control(cause).into()
        }
        ProjectionFailure::Codec(cause) => {
            Error::invalid_value(path.clone().field("constant_source_fields"), cause)
        }
    })?;
    add(&mut source, size_of_val(&fields))?;
    let field_requests = fields.creation_request_bytes_upper_bound().ok_or_else(|| {
        Error::inconsistent(
            path.clone().field("constant_source_fields"),
            "actual source creator construction receipt is absent",
        )
    })?;
    add(&mut source, field_requests)?;
    let mut pools = ConstantPools::empty();
    // This new insertion-only table has a known actual construction history.
    // Locked opaque node geometry is independent of exact Layout callbacks.
    let tree = novarocks_type_contract::owned_resources::btree::insertion_only::<
        ConstantPoolId,
        ConstantPool,
    >(fragment.constants.len())
    .map_err(|cause| match cause {
        novarocks_type_contract::owned_resources::btree::BTreeResourceError::Arithmetic(_) => {
            arithmetic()
        }
        novarocks_type_contract::owned_resources::btree::BTreeResourceError::SourceModel(_) => {
            CreationError::Resource(LayoutResourceError::SourceModel).into()
        }
    })?;
    resources.work.flush().map_err(CreationError::Control)?;
    resources
        .creation
        .request_opaque_upper_bound(tree.request_bytes_upper_bound)?;
    add(&mut source, tree.request_bytes_upper_bound)?;
    add(&mut source, size_of_val(&pools))?;
    for (ordinal, record) in fragment.constants.iter().enumerate() {
        step(resources)?;
        let record_path = path.clone().field("constants").index(ordinal);
        let id = ConstantPoolId::new(record.id);
        resources.work.flush().map_err(CreationError::Control)?;
        let duplicate = pools.entries().contains_key(&id);
        step(resources)?;
        if duplicate {
            return Err(Error::inconsistent(
                record_path.clone().field("id"),
                "duplicate constant pool identity",
            ));
        }
        let value_id = record.value_type_id.ok_or_else(|| {
            Error::missing(
                record_path.clone().field("value_type_id"),
                "constant value reference is absent",
            )
        })?;
        let field_id = record.source_field_id.ok_or_else(|| {
            Error::missing(
                record_path.clone().field("source_field_id"),
                "constant source reference is absent",
            )
        })?;
        let value = source_types
            .execution
            .get_observed(value_id, &mut resources.work)
            .map_err(|cause| crate::fragment_type_decode::table_error(cause, record_path.clone()))?
            .ok_or_else(|| {
                Error::inconsistent(record_path.clone(), "constant value reference is dangling")
            })?;
        let field = fields
            .field_borrow_observed(field_id, &mut resources.work)
            .map_err(|cause| match cause {
                physical_type_v2::TypeCodecError::Control(cause) => {
                    CreationError::Control(cause).into()
                }
                cause => Error::invalid_value(record_path.clone(), cause),
            })?
            .ok_or_else(|| {
                Error::inconsistent(
                    record_path.clone(),
                    "constant source field reference is dangling",
                )
            })?;
        let metadata = fields.metadata_namespace().ok_or_else(|| {
            Error::inconsistent(
                record_path.clone(),
                "constant source metadata provenance is absent",
            )
        })?;
        let factory = crate::native_type_construction::NativeTypeFactory::new(resources.creation);
        let mut admit = |_: &physical_constant_v2::ConstantRecordConstructionFacts| Ok(());
        let prepared = physical_constant_v2::prepare_constant_record_from_sources_input_derived_with_factory_in(
            record, value, field.field(), field.storage_lifetime(), metadata,
            source, source, &factory, &mut admit, &mut resources.work,
        ).map_err(|error| match error {
            ProjectionFailure::Host(cause) => Error::from(cause),
            ProjectionFailure::Codec(physical_constant_v2::PhysicalConstantCodecError::Control(cause)) => CreationError::Control(cause).into(),
            ProjectionFailure::Codec(cause) => Error::invalid_value(record_path.clone(), cause),
        })?;
        let (pool, facts) = prepared
            .materialize_with_factory_in(&factory, &mut admit, &mut resources.work)
            .map_err(|error| match error {
                ProjectionFailure::Host(cause) => Error::from(cause),
                ProjectionFailure::Codec(
                    physical_constant_v2::PhysicalConstantCodecError::Control(cause),
                ) => CreationError::Control(cause).into(),
                ProjectionFailure::Codec(cause) => Error::invalid_value(record_path.clone(), cause),
            })?;
        // Only the completed retained construction contribution joins the
        // original live source union. Temporary reader/receipt scratch is gone.
        add(&mut source, facts.retained_new_bytes_upper_bound)?;
        resources.work.flush().map_err(CreationError::Control)?;
        let inserted = pools.insert(id, pool);
        step(resources)?;
        inserted.map_err(|cause| Error::invalid_value(record_path, cause))?;
    }
    let requests = fragment.original_call_requests.as_ref().ok_or_else(|| {
        Error::missing(
            path.clone().field("original_call_requests"),
            "fragment original request owner is absent",
        )
    })?;
    let (decoded, _new_requests) = with_creation(resources, |creation, work| {
        let prepared =
            physical_call_requests_v2::prepare_call_requests_decode_input_derived_with_creator_in(
                Some(requests),
                physical_call_requests_v2::CallRequestTypeOwner::PlanTree(&source_types.execution),
                &pools,
                source,
                creation,
                work,
            )?;
        physical_call_requests_v2::decode_call_requests_with_creator_in::<CreationError>(
            prepared, work,
        )
    })
    .map_err(|error| match error {
        ProjectionFailure::Host(cause) => Error::from(cause),
        ProjectionFailure::Codec(physical_call_requests_v2::CallRequestCodecError::Control(
            cause,
        )) => CreationError::Control(cause).into(),
        ProjectionFailure::Codec(cause) => {
            Error::invalid_value(path.clone().field("original_call_requests"), cause)
        }
    })?;
    let owner = crate::fragment_original_requests::NativeOriginalRequests::try_new(
        requests, decoded, pools, resources,
    )?;
    resources.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
            crate::fragment_original_requests::NativeOriginalRequests,
        >())
        .map_err(CreationError::Resource)?,
    )?;
    Ok(std::sync::Arc::new(owner))
}

#[cfg(test)]
#[path = "fragment_original_requests_tests.rs"]
mod tests;
