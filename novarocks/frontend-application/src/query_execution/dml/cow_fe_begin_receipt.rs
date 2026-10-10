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

//! Request construction bound for the two closed FE COW begin callers.
//! The completed match query has exited, and request fields and names use
//! exact capacities. Provider and compiler growth require subsequent bounds.

use novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest;
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogProperty, ConnectorError, ConnectorErrorKind,
    ConnectorMutationMatchContract, ConnectorMutationSourceField, ConnectorMutationTargetField,
    ConnectorRowConversionFootprint, ConnectorRowMutationPreparation,
    ConnectorRowMutationSelection, ConnectorWriteFieldRequest, ConnectorWriteFieldToken,
};
use std::mem::{align_of, size_of};

#[derive(Clone, Copy, Debug)]
pub(super) struct FeCowBeginReceipt {
    pub existing_upper: u64,
    pub fresh_request_peak_upper: u64,
}
impl FeCowBeginReceipt {
    pub(super) fn through_request_peak_upper(self) -> Result<u64, ConnectorError> {
        self.existing_upper
            .checked_add(self.fresh_request_peak_upper)
            .ok_or_else(exhausted)
    }
}

fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW FE begin footprint arithmetic exceeds the original construction profile",
    )
}
fn add(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_add(b).ok_or_else(exhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, ConnectorError> {
    a.checked_mul(b).ok_or_else(exhausted)
}
fn u64_bytes(value: usize) -> Result<u64, ConnectorError> {
    u64::try_from(value).map_err(|_| exhausted())
}
fn arc_header_upper<T>() -> Result<usize, ConnectorError> {
    let alignment = align_of::<T>().max(align_of::<usize>());
    add(
        add(mul(2, size_of::<usize>())?, alignment - 1)?,
        add(size_of::<T>(), alignment - 1)?,
    )
}

fn contract_clone_upper(
    contract: &ConnectorMutationMatchContract,
) -> Result<usize, ConnectorError> {
    // Vec::clone requests exactly len initialized elements. Original spare
    // capacities are not used as a new clone's requested capacity. Field
    // names/metadata and recursive type allocations are covered by the existing
    // borrowed footprint; shared nested Arc pointees can be overcharged.
    let mut bytes = size_of::<ConnectorMutationMatchContract>();
    bytes = add(
        bytes,
        mul(
            contract.identity_fields().len(),
            size_of::<ConnectorMutationSourceField>(),
        )?,
    )?;
    bytes = add(
        bytes,
        mul(
            contract.before_fields().len(),
            size_of::<ConnectorMutationTargetField>(),
        )?,
    )?;
    bytes = add(
        bytes,
        mul(
            contract.after_fields().len(),
            size_of::<ConnectorMutationTargetField>(),
        )?,
    )?;
    bytes = add(
        bytes,
        mul(
            contract.uniqueness_tokens().len(),
            size_of::<ConnectorWriteFieldToken>(),
        )?,
    )?;
    for upper in [
        ConnectorRowConversionFootprint::for_fields(
            contract.identity_fields().iter().map(|f| f.field()),
        )?
        .schema_bytes,
        ConnectorRowConversionFootprint::for_fields(
            contract.before_fields().iter().map(|f| f.field()),
        )?
        .schema_bytes,
        ConnectorRowConversionFootprint::for_fields(
            contract.after_fields().iter().map(|f| f.field()),
        )?
        .schema_bytes,
        ConnectorRowConversionFootprint::for_fields(std::iter::once(
            contract.effect_field().field(),
        ))?
        .schema_bytes,
    ] {
        bytes = add(bytes, upper)?;
    }
    // The clone's owner, table Bytes and base Bytes share the original pinned
    // planning owner. Their inline headers are already in the struct above.
    Ok(bytes)
}

pub(super) fn borrowed_fe_begin_receipt(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    namespace: &str,
    table: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Option<FeCowBeginReceipt>, ConnectorError> {
    let Some(source) = selection.owned_source_backing_upper()? else {
        // Missing safe source provenance is not a zero-byte source.
        return Ok(None);
    };
    let containers = selection.retained_container_bytes()?;
    // Only the actual production CowMatchRootConsumer::try_new_with_capacity
    // creates this one guard allocation. Its clones share the allocation. This
    // closed-callgraph precondition is not proof for arbitrary public guards.
    let guard = arc_header_upper::<novarocks_workload_control::ResultWindowAlias>()?;
    let existing = add(add(source, containers)?, guard)?;

    let contract = preparation.match_contract();
    let width = contract.after_fields().len();
    let mut fresh = size_of::<ConnectorWriteBeginRequest>();
    // Context/collection/Vec headers are already inline in BeginRequest.
    // The actual collection Clone allocates only these two new Vec backings;
    // property key/value and static name/generation remain shared Arc<str>.
    // No collection is a known absent clone, not unknown provenance counted 0.
    if let Some(collection) = context.vended_credential_lease_collection() {
        let properties = collection.catalog_properties();
        fresh = add(
            fresh,
            mul(
                properties.execution_properties().len(),
                size_of::<CatalogProperty>(),
            )?,
        )?;
        fresh = add(
            fresh,
            mul(
                properties.credential_bindings().len(),
                size_of::<CatalogCredentialBinding>(),
            )?,
        )?;
    }
    fresh = add(fresh, mul(width, size_of::<ConnectorWriteFieldRequest>())?)?;
    fresh = add(
        fresh,
        ConnectorRowConversionFootprint::for_fields(
            contract.after_fields().iter().map(|f| f.field()),
        )?
        .schema_bytes,
    )?;

    // Two freshly named Int64 identity fields use an exact-size vector and
    // Field::new (empty metadata). The constants are the existing caller's.
    fresh = add(fresh, mul(2, size_of::<ConnectorWriteFieldRequest>())?)?;
    fresh = add(
        fresh,
        novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL.len(),
    )?;
    fresh = add(
        fresh,
        novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.len(),
    )?;

    // Build one String::with_capacity(name_bytes), append borrowed parts, then
    // Arc::from(owned.as_str()). Both blocks coexist during Arc construction.
    // No format! growth, table String clone, or guessed identifier limit.
    let name = add(add(namespace.len(), 1)?, table.len())?;
    let arc_name = add(
        add(mul(2, size_of::<usize>())?, name)?,
        align_of::<usize>() - 1,
    )?;
    fresh = add(fresh, add(name, arc_name)?)?;
    fresh = add(fresh, contract_clone_upper(contract)?)?;
    Ok(Some(FeCowBeginReceipt {
        existing_upper: u64_bytes(existing)?,
        fresh_request_peak_upper: u64_bytes(fresh)?,
    }))
}

/// The original scope and window authorize this request before its first copy.
/// No provider or compiler allocation is authorized by this boundary.
pub(super) fn before_request(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    namespace: &str,
    table: &str,
    binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<FeCowBeginReceipt, super::cow_necessary_before_begin::Error> {
    use super::cow_necessary_before_begin::Error;
    use novarocks_spi::connector::ConnectorOperationControl;
    context.check_active().map_err(Error::Control)?;
    binding.scope().check().map_err(Error::Scope)?;
    let receipt = borrowed_fe_begin_receipt(preparation, selection, namespace, table, context)
        .map_err(Error::Control)?
        .ok_or_else(|| {
            Error::Control(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW request requires original owned-source receipts",
            ))
        })?;
    let total = receipt
        .through_request_peak_upper()
        .map_err(Error::Control)?;
    if total > crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES {
        return Err(Error::ResourceExhausted(exhausted()));
    }
    binding
        .window_alias()
        .check_backing_total(total)
        .map_err(Error::Scope)?;
    context.check_active().map_err(Error::Control)?;
    binding.scope().check().map_err(Error::Scope)?;
    Ok(receipt)
}
