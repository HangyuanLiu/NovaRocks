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

//! Request/work admission for the checked recursive safe-reader recipe.
//! Geometry scratch belongs to the earlier raw stage; its completed request
//! remains in this cumulative projection. These facts do not grant memory.

use super::{reader_allocations, reader_diagnostics, reader_work};
use crate::{
    ipc_flat_batch_v2::{Layout as FlatLayout, layout},
    ipc_flat_stream_v2::FlatPoolResourceError,
    ipc_recursive_batch_v2::{RecursiveBatchGeometry, RecursiveNodeGeometry},
    physical_type_v2::TypeCodecError,
};
use arrow::datatypes::{DataType, Field};
use novarocks_constant_contract::{
    ConstantPolicy, ConstantResourceFacts, RecursiveConstantResourceInput,
    preflight_recursive_pool_resources,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, FunctionValueType};
use std::alloc::Layout;

/// All fields are borrowed from the same checked schema/batch/body snapshot.
/// The original geometry allocation has already been admitted by its owner.
pub(crate) struct ReaderInput<'a, 'b> {
    pub(crate) field: &'a Field,
    pub(crate) batch: arrow::ipc::RecordBatch<'b>,
    pub(crate) body: &'b [u8],
    pub(crate) nodes: &'a [RecursiveNodeGeometry<'a>],
    pub(crate) geometry: RecursiveBatchGeometry,
    pub(crate) geometry_scratch_request_bytes: usize,
    pub(crate) geometry_scratch_request_count: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct RecursiveReaderProjectionLimits {
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecursiveReaderResourceFacts {
    pub source_retained_bytes: usize,
    pub payload_request_bytes_upper_bound: usize,
    pub structural_request_bytes_upper_bound: usize,
    pub diagnostic_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
    pub pool: ConstantResourceFacts,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PayloadRequests {
    pub body_capacity: usize,
    pub repair_capacity: usize,
    pub repair_count: usize,
    pub empty_offsets_capacity: usize,
    pub empty_offsets_count: usize,
    pub utf8_fallback: usize,
}

pub(super) fn invalid(message: &'static str) -> FlatPoolResourceError {
    TypeCodecError::InvalidShape(message).into()
}
pub(super) fn add(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_add(b)
        .ok_or_else(|| invalid("recursive reader resource sum overflow"))
}
pub(super) fn mul(a: usize, b: usize) -> Result<usize, FlatPoolResourceError> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("recursive reader resource product overflow"))
}
fn u64_extent(n: usize) -> Result<u64, FlatPoolResourceError> {
    u64::try_from(n).map_err(|_| invalid("recursive reader resource extent exceeds u64"))
}
fn capacity(bytes: usize) -> Result<usize, FlatPoolResourceError> {
    let rounded = add(bytes, 63)? & !63;
    Layout::from_size_align(rounded, arrow_buffer::alloc::ALIGNMENT)
        .map_err(|_| invalid("recursive reader payload layout is not representable"))?;
    Ok(rounded)
}
fn cap(actual: usize, limit: usize, message: &'static str) -> Result<(), FlatPoolResourceError> {
    if actual > limit {
        Err(invalid(message))
    } else {
        Ok(())
    }
}

pub(super) fn descriptor(
    input: &ReaderInput<'_, '_>,
    index: usize,
) -> Result<(usize, usize), FlatPoolResourceError> {
    let buffers = input
        .batch
        .buffers()
        .ok_or_else(|| invalid("recursive reader lacks buffers"))?;
    if index >= buffers.len() {
        return Err(invalid("recursive reader buffer index exceeds geometry"));
    }
    let item = buffers.get(index);
    let offset =
        usize::try_from(item.offset()).map_err(|_| invalid("recursive reader buffer offset"))?;
    let length =
        usize::try_from(item.length()).map_err(|_| invalid("recursive reader buffer length"))?;
    if add(offset, length)? > input.body.len() {
        return Err(invalid("recursive reader buffer range"));
    }
    Ok((offset, length))
}

pub(super) fn payload(
    input: &ReaderInput<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PayloadRequests, FlatPoolResourceError> {
    let mut requests = PayloadRequests {
        body_capacity: capacity(input.body.len())?,
        ..Default::default()
    };
    work.step()?;
    for node in input.nodes {
        let ty = node.field.data_type();
        let offsets = match ty {
            DataType::List(_) | DataType::Map(_, _) => Some(4),
            DataType::LargeList(_) => Some(8),
            DataType::Struct(_) => None,
            _ => match layout(ty)? {
                FlatLayout::Offsets(width) => Some(width),
                _ => None,
            },
        };
        let typed_width = if let Some(width) = offsets {
            Some(width)
        } else {
            match ty {
                DataType::Struct(_) => None,
                _ => match layout(ty)? {
                    FlatLayout::Views => Some(std::mem::size_of::<u128>()),
                    FlatLayout::Fixed(width)
                        if width > 1 && !matches!(ty, DataType::FixedSizeBinary(_)) =>
                    {
                        Some(width)
                    }
                    _ => None,
                },
            }
        };
        if let Some(width) = typed_width {
            let (offset, length) = descriptor(input, add(node.buffer_start, 1)?)?;
            if !arrow_buffer::alloc::ALIGNMENT.is_multiple_of(width)
                || !offset.is_multiple_of(width)
            {
                requests.repair_capacity = add(requests.repair_capacity, capacity(length)?)?;
                requests.repair_count = add(requests.repair_count, 1)?;
            }
            if let Some(width) = offsets
                && node.rows == 0
                && length == 0
            {
                Layout::from_size_align(width, arrow_buffer::alloc::ALIGNMENT)
                    .map_err(|_| invalid("recursive reader empty offset layout"))?;
                requests.empty_offsets_capacity = add(requests.empty_offsets_capacity, width)?;
                requests.empty_offsets_count = add(requests.empty_offsets_count, 1)?;
            }
        }
        if matches!(ty, DataType::Utf8 | DataType::LargeUtf8) {
            requests.utf8_fallback = add(
                requests.utf8_fallback,
                descriptor(input, add(node.buffer_start, 2)?)?.1,
            )?;
        }
        work.step()?;
    }
    Ok(requests)
}

/// Parent owns entry/ordinary/success finishing. Every direct typed refusal is
/// propagated before another callback, including nested constant admission.
pub(crate) fn preflight(
    input: &ReaderInput<'_, '_>,
    value_type: &FunctionValueType,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    limits: RecursiveReaderProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveReaderResourceFacts, FlatPoolResourceError> {
    cap(
        input.body.len(),
        source_retained_bytes,
        "recursive reader source retention below body",
    )?;
    let source_work = reader_work::source_metadata_work(input.nodes, source_retained_bytes, work)?;
    cap(
        source_work.total,
        limits.max_cumulative_library_work,
        "recursive reader source metadata work envelope exceeded",
    )?;
    let payload = payload(input, work)?;
    let requested_payload = add(
        add(payload.body_capacity, payload.repair_capacity)?,
        payload.empty_offsets_capacity,
    )?;
    let retained = if matches!(input.field.data_type(), DataType::Null) {
        0
    } else {
        requested_payload
    };
    let visits = add(
        input.geometry.described_buffer_bytes,
        payload.empty_offsets_capacity,
    )?;
    // Validate conversion before handing the lazy DFS sequence to the sole
    // constant author; no copied length vector or secondary backing is made.
    for node in input.nodes {
        u64_extent(node.rows)?;
        work.step()?;
    }
    work.flush()?;
    let pool = preflight_recursive_pool_resources(
        input.field,
        value_type,
        input.nodes.iter().map(|node| node.rows as u64),
        RecursiveConstantResourceInput {
            buffer_count_upper_bound: u64_extent(input.geometry.buffer_descriptors)?,
            buffer_visits_bytes_upper_bound: u64_extent(visits)?,
            retained_buffer_capacity_bytes_upper_bound: u64_extent(retained)?,
            view_validation_bytes_upper_bound: u64_extent(input.geometry.view_validation_bytes)?,
            utf8_fallback_validation_bytes_upper_bound: u64_extent(payload.utf8_fallback)?,
        },
        policy,
        CompilePhase::Decode,
        work.control(),
    )?;
    let structures = reader_allocations::preflight(input, &payload, work)?;
    let diagnostics = reader_diagnostics::preflight(input, work)?;
    let structural = add(
        structures.structural_request_bytes_upper_bound,
        input.geometry_scratch_request_bytes,
    )?;
    let payload_count = add(
        add(
            usize::from(payload.body_capacity != 0),
            payload.repair_count,
        )?,
        payload.empty_offsets_count,
    )?;
    let count = add(
        add(
            add(structures.allocation_requests_upper_bound, payload_count)?,
            diagnostics.allocation_requests_upper_bound,
        )?,
        input.geometry_scratch_request_count,
    )?;
    let requested = add(
        add(requested_payload, structural)?,
        diagnostics.request_bytes_upper_bound,
    )?;
    let coexisting = add(source_retained_bytes, requested)?;
    let cumulative = reader_work::preflight(
        input,
        &payload,
        &pool,
        &structures,
        &diagnostics,
        &source_work,
        work,
    )?;
    cap(
        requested,
        limits.max_new_allocation_request_bytes,
        "recursive reader allocation request envelope exceeded",
    )?;
    cap(
        coexisting,
        limits.max_coexisting_source_and_request_bytes,
        "recursive reader coexistence envelope exceeded",
    )?;
    cap(
        cumulative,
        limits.max_cumulative_library_work,
        "recursive reader library work envelope exceeded",
    )?;
    work.step()?;
    Ok(RecursiveReaderResourceFacts {
        source_retained_bytes,
        payload_request_bytes_upper_bound: requested_payload,
        structural_request_bytes_upper_bound: structural,
        diagnostic_request_bytes_upper_bound: diagnostics.request_bytes_upper_bound,
        allocation_request_count_upper_bound: count,
        new_allocation_request_bytes_upper_bound: requested,
        coexisting_source_and_request_bytes_upper_bound: coexisting,
        cumulative_library_work_upper_bound: cumulative,
        pool,
    })
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
