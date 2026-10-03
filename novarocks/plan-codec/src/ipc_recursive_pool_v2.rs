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

//! Checked-pool recursive IPC writer. Source retention is a caller invoice;
//! request/work bounds are projection admission, not a formal MEM grant.
//! No general writer, temporary ArrayData tree, or sliced data is constructed.

use crate::{
    ipc_flat_pool_v2::{
        self, FlatPoolWriteFacts, FlatPoolWriteLimits,
        allocations::batch_and_stream_requests,
        geometry::{add, mul},
    },
    ipc_schema_v2::{self, preflight_schema_writer_resources, schema_writer_prefix_resources},
    physical_type_v2::TypeCodecError,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use flatbuffers::FlatBufferBuilder;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

mod body;
mod geometry;
mod header;

/// Explicit recursive admission; no application defaults are selected here.
#[derive(Clone, Copy, Debug)]
pub struct RecursivePoolWriteLimits {
    pub flat: FlatPoolWriteLimits,
    pub max_field_nodes: usize,
    /// Sum of the selected length of every actual FieldNode occurrence.
    pub max_total_rows: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecursivePoolWriteFacts {
    pub flat: FlatPoolWriteFacts,
    pub field_nodes: usize,
    pub total_rows: usize,
    pub view_fields: usize,
}

struct Prepared {
    geometry: geometry::Geometry,
    facts: RecursivePoolWriteFacts,
}

/// One immutable checked pool paired with the admitted geometry and limits.
/// Consumption emits that snapshot without repeating resource preparation.
pub(crate) struct PreparedRecursivePoolWriter<'pool> {
    pool: &'pool ConstantPool,
    limits: RecursivePoolWriteLimits,
    prepared: Prepared,
}
impl PreparedRecursivePoolWriter<'_> {
    pub(crate) const fn facts(&self) -> &RecursivePoolWriteFacts {
        &self.prepared.facts
    }

    pub(crate) fn emit(self, control: &dyn PureCompileControl) -> Result<Vec<u8>, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = emit_prepared(self.pool, &self.prepared, self.limits, &mut work);
        finish(work, result)
    }
}

fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}

fn gate_requests(
    bytes: usize,
    source: usize,
    work_bound: usize,
    limits: FlatPoolWriteLimits,
) -> Result<usize, TypeCodecError> {
    let coexist = add(source, bytes)?;
    if bytes > limits.max_new_allocation_request_bytes
        || coexist > limits.max_coexisting_source_and_request_bytes
        || work_bound > limits.max_cumulative_library_work
    {
        return Err(shape("recursive writer resource envelope exceeded"));
    }
    Ok(coexist)
}

fn prepare(
    pool: &ConstantPool,
    source: usize,
    limits: RecursivePoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, TypeCodecError> {
    if !LOCKED_FAMILY
        || !LOCKED_TOOLCHAIN
        || arrow::ARROW_VERSION != "58.2.0"
        || !cfg!(target_endian = "little")
    {
        return Err(shape(
            "recursive writer source model does not match this target",
        ));
    }
    if source
        < usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
            .map_err(|_| shape("source retention is not representable"))?
    {
        return Err(shape(
            "recursive writer source retention omits checked backing",
        ));
    }
    work.step()?;
    // This O(1) source-owner bound precedes its first type-walker allocation.
    let prefix = schema_writer_prefix_resources(
        pool.field(),
        source,
        limits.flat.schema,
        limits.flat.max_cumulative_library_work,
        work,
    )?;
    gate_requests(
        prefix.request_bytes,
        source,
        prefix.work_upper_bound,
        limits.flat,
    )?;
    work.step()?;
    let geometry = geometry::inspect(pool.data(), limits, work)?;
    let schema_resources =
        preflight_schema_writer_resources(pool.field(), source, limits.flat.schema, prefix, work)?;
    let schema = &schema_resources.schema;
    let batch_backing = ipc_flat_pool_v2::header::backing_counts(
        geometry.nodes,
        geometry.buffers,
        geometry.view_fields,
    )?;
    let schema_capacity = ipc_flat_pool_v2::metadata_capacity(schema.backing)?;
    let batch_capacity = ipc_flat_pool_v2::metadata_capacity(batch_backing)?;
    let stream_capacity = add(
        add(add(schema_capacity, batch_capacity)?, geometry.body_bytes)?,
        24,
    )?;
    if stream_capacity > limits.flat.max_encoded_stream_bytes {
        return Err(shape("recursive writer stream envelope exceeded"));
    }
    std::alloc::Layout::array::<u8>(stream_capacity)
        .map_err(|_| shape("recursive writer stream request is not representable"))?;
    work.step()?;
    let batch_requests = batch_and_stream_requests(batch_backing, stream_capacity, work)?;
    let request_bytes = add(schema_resources.request_bytes, batch_requests.bytes)?;
    let request_count = add(schema_resources.request_count, batch_requests.count)?;
    // There are at most five complete borrowed data walks: geometry, buffers,
    // nodes, optional variadic counts, and final body. Per walk a node performs
    // four node checks, one edge check, and at most two flat-leaf checks plus
    // one operation per descriptor; selected validity recounts visit <=N rows.
    let visits = mul(
        5,
        add(
            add(geometry.total_rows, mul(7, geometry.nodes)?)?,
            geometry.buffers,
        )?,
    )?;
    let profile = mul(2, geometry.nodes)?; // type node and child edge
    let summary = geometry.nodes;
    let header_pushes = add(add(geometry.nodes, geometry.buffers)?, geometry.view_fields)?;
    // Each node's body work includes validity/range checks and <=N offsets;
    // byte copies/initialization and opaque builder work use the same locked
    // four-storage-visits source cover as the existing flat writer.
    let body_checks = add(geometry.total_rows, mul(8, geometry.nodes)?)?;
    let storage = add(mul(4, request_bytes)?, request_count)?;
    let own_work = add(
        add(add(visits, profile)?, add(summary, header_pushes)?)?,
        add(body_checks, storage)?,
    )?;
    let cumulative_work = add(schema_resources.work_upper_bound, own_work)?;
    let coexist = gate_requests(request_bytes, source, cumulative_work, limits.flat)?;
    work.step()?;
    let facts = RecursivePoolWriteFacts {
        flat: FlatPoolWriteFacts {
            source_retained_bytes: source,
            rows: geometry.rows,
            buffer_descriptors: geometry.buffers,
            variadic_buffers: geometry.variadic,
            body_bytes: geometry.body_bytes,
            schema_backing_bytes_upper_bound: schema.backing,
            batch_backing_bytes_upper_bound: batch_backing,
            encoded_stream_bytes_upper_bound: stream_capacity,
            new_allocation_request_bytes_upper_bound: request_bytes,
            allocation_request_count_upper_bound: request_count,
            coexisting_source_and_request_bytes_upper_bound: coexist,
            cumulative_library_work_upper_bound: cumulative_work,
        },
        field_nodes: geometry.nodes,
        total_rows: geometry.total_rows,
        view_fields: geometry.view_fields,
    };
    Ok(Prepared { geometry, facts })
}

pub fn preflight_recursive_pool_write(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: RecursivePoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<RecursivePoolWriteFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result =
        prepare(pool, source_retained_bytes, limits, &mut work).map(|prepared| prepared.facts);
    finish(work, result)
}

pub(crate) fn prepare_recursive_pool_write<'pool>(
    pool: &'pool ConstantPool,
    source_retained_bytes: usize,
    limits: RecursivePoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<PreparedRecursivePoolWriter<'pool>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work).map(|prepared| {
        PreparedRecursivePoolWriter {
            pool,
            limits,
            prepared,
        }
    });
    finish(work, result)
}

/// Preserves the complete original pool and ordinals. Only the final original
/// success-tail observation permits publication of the assembled stream.
pub fn encode_recursive_pool(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: RecursivePoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work)
        .and_then(|prepared| emit_prepared(pool, &prepared, limits, &mut work));
    finish(work, result)
}

fn emit_prepared(
    pool: &ConstantPool,
    prepared: &Prepared,
    limits: RecursivePoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    work.flush()?;
    let schema = ipc_schema_v2::encode_single_field_schema(
        pool.field(),
        limits.flat.schema,
        work.control(),
    )?;
    work.flush()?;
    let mut batch =
        ipc_flat_pool_v2::reserve(prepared.facts.flat.batch_backing_bytes_upper_bound, work)?;
    ipc_flat_pool_v2::initialize_to(
        &mut batch,
        prepared.facts.flat.batch_backing_bytes_upper_bound,
        work,
    )?;
    work.flush()?;
    let mut builder = FlatBufferBuilder::from_vec(batch);
    work.flush()?;
    header::emit(pool.data(), &prepared.geometry, &mut builder, work)?;
    if builder.mut_finished_buffer().0.len() != prepared.facts.flat.batch_backing_bytes_upper_bound
    {
        return Err(shape(
            "recursive batch builder grew beyond admitted backing",
        ));
    }
    let mut output =
        ipc_flat_pool_v2::reserve(prepared.facts.flat.encoded_stream_bytes_upper_bound, work)?;
    ipc_flat_pool_v2::frame(&mut output, &schema, work)?;
    ipc_flat_pool_v2::frame(&mut output, builder.finished_data(), work)?;
    let body_start = output.len();
    let body_end = add(body_start, prepared.geometry.body_bytes)?;
    ipc_flat_pool_v2::initialize_to(&mut output, body_end, work)?;
    body::emit(
        pool.data(),
        &prepared.geometry,
        &mut output[body_start..body_end],
        work,
    )?;
    ipc_flat_pool_v2::append(&mut output, &[255, 255, 255, 255, 0, 0, 0, 0], work)?;
    if output.len() > prepared.facts.flat.encoded_stream_bytes_upper_bound {
        return Err(shape(
            "recursive writer result exceeds admitted stream capacity",
        ));
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
