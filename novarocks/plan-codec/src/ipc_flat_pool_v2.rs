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

//! Exact one-pool flat IPC encoding from the sole immutable constant owner.
//! Requests and retained source facts are host inputs, not Account/grant proof.
//! Source-preflight scratch is admitted before that phase; the full combined
//! gate precedes all schema/batch/output construction. No general writer fallback.

use crate::{
    ipc_schema_v2::{self, IpcSchemaProjectionLimits, SchemaPreflight},
    physical_type_v2::TypeCodecError,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
use flatbuffers::FlatBufferBuilder;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::alloc::Layout;
pub(crate) mod allocations;
pub(crate) mod body;
pub(crate) mod geometry;
pub(crate) mod header;
use geometry::{Geometry, add, aligned, mul};

#[derive(Clone, Copy, Debug)]
pub struct FlatPoolWriteLimits {
    pub max_rows: usize,
    pub max_buffer_descriptors: usize,
    pub max_body_bytes: usize,
    pub max_encoded_stream_bytes: usize,
    pub schema: IpcSchemaProjectionLimits,
    pub max_new_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_cumulative_library_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatPoolWriteFacts {
    pub source_retained_bytes: usize,
    pub rows: usize,
    pub buffer_descriptors: usize,
    pub variadic_buffers: usize,
    pub body_bytes: usize,
    pub schema_backing_bytes_upper_bound: usize,
    pub batch_backing_bytes_upper_bound: usize,
    pub encoded_stream_bytes_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub allocation_request_count_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}
struct Prepared {
    geometry: Geometry,
    facts: FlatPoolWriteFacts,
}
fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn cap(actual: usize, maximum: usize, message: &'static str) -> Result<(), TypeCodecError> {
    if actual > maximum {
        Err(shape(message))
    } else {
        Ok(())
    }
}
pub(crate) fn source_work(source: usize, entries: usize) -> Result<usize, TypeCodecError> {
    // Two preflight passes each scan metadata twice; emission scans once;
    // verification can scan original metadata once for each emitted entry.
    // Two logical-tag lookups also probe the original raw table. Its whole
    // retained backing, including tombstones, bounds bucket/control visits.
    add(
        mul(source, add(entries, 7)?)?,
        mul(2, novarocks_type_contract::NR_LOGICAL_TYPE_KEY.len())?,
    )
}
pub(crate) fn metadata_capacity(bytes: usize) -> Result<usize, TypeCodecError> {
    let padded = aligned(bytes)?;
    if padded > i32::MAX as usize || bytes >= (1usize << 31) {
        return Err(shape(
            "flat pool writer metadata framing is not representable",
        ));
    }
    Layout::array::<u8>(bytes)
        .map_err(|_| shape("flat pool writer backing layout is not representable"))?;
    Ok(padded)
}
fn work_bound(
    source: usize,
    schema: &SchemaPreflight,
    geometry: &Geometry,
    requests: &allocations::Requests,
) -> Result<usize, TypeCodecError> {
    // Own sorting, duplicate rejection and source association comparison each
    // visit borrowed source bytes. Four K²*(L+1) covers both operand byte
    // visits and scalar comparisons, even with empty keys/values.
    let associations = mul(
        mul(4, mul(schema.metadata_entries, schema.metadata_entries)?)?,
        add(schema.string_bytes, 1)?,
    )?;
    // At most 18 vtable bytes per operand plus u32 insertion moves, for at
    // most T² comparisons/moves. Batch adds its two five-slot tables.
    let tables = add(schema.tables, 2)?;
    let vtables = mul(40, mul(tables, tables)?)?;
    // Four visits per requested byte cover initialization, construction/copy,
    // output copies and teardown. Owned nonaligned bitmap reads additionally
    // visit at most N source bytes per bitmap, independently for Bool/validity.
    let storage = add(mul(4, requests.bytes)?, requests.count)?;
    let elements = add(add(mul(3, geometry.rows)?, 1)?, mul(4, geometry.buffers)?)?;
    add(
        add(
            add(source_work(source, schema.metadata_entries)?, associations)?,
            vtables,
        )?,
        add(storage, elements)?,
    )
}
fn prepare(
    pool: &ConstantPool,
    source: usize,
    limits: FlatPoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Prepared, TypeCodecError> {
    if !(LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == "58.2.0"
        && cfg!(target_endian = "little"))
    {
        return Err(shape("flat pool writer source or endian model changed"));
    }
    let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
        .map_err(|_| shape("flat pool source retention is not representable"))?;
    cap(
        retained,
        source,
        "flat pool source retention is below original buffer backing",
    )?;
    work.step()?;
    let geometry = geometry::inspect(
        pool,
        limits.max_rows,
        limits.max_buffer_descriptors,
        limits.max_body_bytes,
        work,
    )?;
    if geometry.buffers > u32::MAX as usize {
        return Err(shape("flat pool buffer vector is not representable"));
    }
    cap(
        source_work(source, pool.field().metadata().len())?,
        limits.max_cumulative_library_work,
        "flat pool source metadata work envelope exceeded",
    )?;
    // The sole schema author's source counters use two exact flat walker Vecs.
    // Admit this explicit source-preflight scratch before that phase starts.
    let prefix = allocations::prefix_request_bytes();
    cap(
        prefix,
        limits.max_new_allocation_request_bytes,
        "flat pool source preflight request envelope exceeded",
    )?;
    cap(
        add(source, prefix)?,
        limits.max_coexisting_source_and_request_bytes,
        "flat pool source preflight coexistence envelope exceeded",
    )?;
    work.flush()?;
    let schema = ipc_schema_v2::preflight_writer(pool.field(), limits.schema, work)?;
    let batch = header::backing(&geometry)?;
    let capacity = add(
        add(
            add(
                metadata_capacity(schema.backing)?,
                metadata_capacity(batch)?,
            )?,
            geometry.body_bytes,
        )?,
        24,
    )?;
    cap(
        capacity,
        limits.max_encoded_stream_bytes,
        "flat pool encoded stream envelope exceeded",
    )?;
    Layout::array::<u8>(capacity)
        .map_err(|_| shape("flat pool output layout is not representable"))?;
    let requests = allocations::preflight(&schema, batch, capacity, work)?;
    let coexisting = add(source, requests.bytes)?;
    let library_work = work_bound(source, &schema, &geometry, &requests)?;
    cap(
        requests.bytes,
        limits.max_new_allocation_request_bytes,
        "flat pool allocation request envelope exceeded",
    )?;
    cap(
        coexisting,
        limits.max_coexisting_source_and_request_bytes,
        "flat pool source coexistence envelope exceeded",
    )?;
    cap(
        library_work,
        limits.max_cumulative_library_work,
        "flat pool cumulative library work envelope exceeded",
    )?;
    work.step()?;
    let facts = FlatPoolWriteFacts {
        source_retained_bytes: source,
        rows: geometry.rows,
        buffer_descriptors: geometry.buffers,
        variadic_buffers: geometry.variadic,
        body_bytes: geometry.body_bytes,
        schema_backing_bytes_upper_bound: schema.backing,
        batch_backing_bytes_upper_bound: batch,
        encoded_stream_bytes_upper_bound: capacity,
        new_allocation_request_bytes_upper_bound: requests.bytes,
        allocation_request_count_upper_bound: requests.count,
        coexisting_source_and_request_bytes_upper_bound: coexisting,
        cumulative_library_work_upper_bound: library_work,
    };
    Ok(Prepared { geometry, facts })
}

/// Sealed geometry/model preparation bound to the original checked pool.
/// The preparation scratch and host authorization remain earlier obligations.
pub(crate) struct PreparedFlatPoolWriter<'p> {
    pool: &'p ConstantPool,
    limits: FlatPoolWriteLimits,
    prepared: Prepared,
}
impl PreparedFlatPoolWriter<'_> {
    pub(crate) fn facts(&self) -> &FlatPoolWriteFacts {
        &self.prepared.facts
    }
    pub(crate) fn emit(self, control: &dyn PureCompileControl) -> Result<Vec<u8>, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = emit_prepared(self.pool, self.prepared, self.limits, &mut work);
        finish_writer(work, result)
    }
}
fn finish_writer<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, TypeCodecError>,
) -> Result<T, TypeCodecError> {
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
/// Keep the same immutable snapshot through later namespace admission. No
/// output is allocated here and emission does not repeat numerical preparation.
pub(crate) fn prepare_flat_pool_write<'p>(
    pool: &'p ConstantPool,
    source_retained_bytes: usize,
    limits: FlatPoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<PreparedFlatPoolWriter<'p>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let prepared = prepare(pool, source_retained_bytes, limits, &mut work);
    let prepared = finish_writer(work, prepared)?;
    Ok(PreparedFlatPoolWriter {
        pool,
        limits,
        prepared,
    })
}

/// The mandatory trusted invoice includes all retained original pool, Field,
/// type, buffer and raw HashMap backing, including spare/deleted buckets.
/// This projection cannot establish that host fact or create a memory wallet.
pub fn preflight_flat_pool_write(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: FlatPoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<FlatPoolWriteFacts, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare(pool, source_retained_bytes, limits, &mut work).map(|p| p.facts);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
pub(crate) fn reserve(
    capacity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    work.flush()?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(capacity)
        .map_err(|_| TypeCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    Ok(result)
}
pub(crate) fn initialize_to(
    output: &mut Vec<u8>,
    length: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if length > output.capacity() || length < output.len() {
        return Err(shape(
            "flat pool initialized output exceeds admitted capacity",
        ));
    }
    while output.len() < length {
        let end = add(output.len(), (length - output.len()).min(1024))?;
        output.resize(end, 0);
        work.step()?;
    }
    Ok(())
}
pub(crate) fn append(
    output: &mut Vec<u8>,
    bytes: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if add(output.len(), bytes.len())? > output.capacity() {
        return Err(shape("flat pool output exceeds admitted capacity"));
    }
    for chunk in bytes.chunks(1024) {
        output.extend_from_slice(chunk);
        work.step()?;
    }
    Ok(())
}
pub(crate) fn frame(
    output: &mut Vec<u8>,
    metadata: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let padded = metadata_capacity(metadata.len())?;
    let length = i32::try_from(padded)
        .map_err(|_| shape("flat pool metadata continuation is not representable"))?;
    append(output, &[255; 4], work)?;
    append(output, &length.to_le_bytes(), work)?;
    append(output, metadata, work)?;
    initialize_to(output, add(output.len(), padded - metadata.len())?, work)
}
/// Encodes the whole original pool, preserving every ordinal. All output stays
/// unpublished until the final original control observation succeeds.
pub fn encode_flat_pool(
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: FlatPoolWriteLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let prepared = prepare(pool, source_retained_bytes, limits, &mut work)?;
        emit_prepared(pool, prepared, limits, &mut work)
    })();
    finish_writer(work, result)
}

fn emit_prepared(
    pool: &ConstantPool,
    prepared: Prepared,
    limits: FlatPoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    work.flush()?;
    let schema =
        ipc_schema_v2::encode_single_field_schema(pool.field(), limits.schema, work.control())?;
    work.flush()?;
    let mut batch = reserve(prepared.facts.batch_backing_bytes_upper_bound, work)?;
    initialize_to(
        &mut batch,
        prepared.facts.batch_backing_bytes_upper_bound,
        work,
    )?;
    work.flush()?;
    let mut builder = FlatBufferBuilder::from_vec(batch);
    work.flush()?;
    header::emit(pool, &prepared.geometry, &mut builder, work)?;
    // Witness the complete primary backing, not just the finished message.
    let backing = builder.mut_finished_buffer().0.len();
    if backing != prepared.facts.batch_backing_bytes_upper_bound {
        return Err(shape(
            "flat pool batch builder grew beyond its admitted backing",
        ));
    }
    let mut output = reserve(prepared.facts.encoded_stream_bytes_upper_bound, work)?;
    frame(&mut output, &schema, work)?;
    frame(&mut output, builder.finished_data(), work)?;
    let body_start = output.len();
    let body_end = add(body_start, prepared.geometry.body_bytes)?;
    initialize_to(&mut output, body_end, work)?;
    body::emit(
        pool,
        &prepared.geometry,
        &mut output[body_start..body_end],
        work,
    )?;
    append(&mut output, &[255, 255, 255, 255, 0, 0, 0, 0], work)?;
    if output.len() > prepared.facts.encoded_stream_bytes_upper_bound {
        return Err(shape("flat pool encoded result exceeds admitted capacity"));
    }
    Ok(output)
}

#[cfg(test)]
mod envelope_tests {
    use super::*;

    #[test]
    fn signed_continuation_limit_is_checked_after_padding_without_allocation() {
        let maximum = (i32::MAX as usize) & !7;
        assert_eq!(metadata_capacity(maximum).unwrap(), maximum);
        assert_eq!(metadata_capacity(maximum - 7).unwrap(), maximum);
        for length in [maximum + 1, i32::MAX as usize, 1usize << 31, usize::MAX] {
            assert!(metadata_capacity(length).is_err());
        }
    }

    #[test]
    fn descriptor_backing_arithmetic_refuses_before_builder_vector_multiplication() {
        let geometry = Geometry {
            rows: 0,
            buffers: usize::MAX,
            variadic: 0,
            views: false,
            body_bytes: 0,
            payload_bytes: 0,
            values_start: 0,
            values_bytes: 0,
            offset_base: 0,
        };
        assert!(header::backing(&geometry).is_err());
        assert!(aligned(usize::MAX).is_err());
        assert!(geometry::native_offset(&[0; 8], usize::MAX, 8).is_err());
    }
}

#[cfg(test)]
mod tests;
