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

//! Complete borrowed framing for a flat non-dictionary constant pool stream.
//! Schema identity and batch geometry are checked by their existing authors.
//! No array/schema materialization or reader entry is performed here. This
//! projection does not authorize those allocations or replace the subsequent
//! complete resource model, Field/FVT pairing and ConstantPool value admission.

use crate::{
    ipc_flat_batch_v2::{
        FlatBatchGeometry, FlatBatchProjectionLimits, preflight_verified_flat_record_batch,
    },
    ipc_schema_v2::{
        IpcSchemaProjectionLimits, verified_message_observed, verify_single_field_schema_with_work,
    },
    physical_type_v2::TypeCodecError,
};
use arrow::datatypes::Field;
use novarocks_arrow_ipc_frame::{
    ContinuationPrefix, VerifierOptions, checked_range, continuation_prefix, nonnegative_length,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

mod pool_resources;
pub use pool_resources::{FlatPoolResourceError, FlatPoolResourceProjection};
mod reader;
pub(crate) mod reader_allocations;
pub(crate) mod reader_diagnostics;
mod reader_work;
pub(crate) use reader::PreparedFlatReader;
pub use reader::{FlatReaderError, FlatReaderProjectionLimits, FlatReaderResourceFacts};

/// All limits originate with the admitted caller, not the stream or defaults.
#[derive(Clone, Copy, Debug)]
pub struct FlatStreamProjectionLimits {
    pub max_input_bytes: usize,
    pub schema: IpcSchemaProjectionLimits,
    pub batch: FlatBatchProjectionLimits,
}

/// This owner can only be constructed by successful complete stream preflight.
/// The input and exact original Field remain borrowed; no values are retagged.
#[derive(Debug)]
pub struct FlatConstantStream<'a, 'f> {
    input: &'a [u8],
    field: &'f Field,
    batch_metadata: &'a [u8],
    batch_body: &'a [u8],
    batch: arrow::ipc::RecordBatch<'a>,
    version: arrow::ipc::MetadataVersion,
    geometry: FlatBatchGeometry,
}
impl<'a, 'f> FlatConstantStream<'a, 'f> {
    pub fn input(&self) -> &'a [u8] {
        self.input
    }
    pub fn field(&self) -> &'f Field {
        self.field
    }
    pub fn batch_metadata(&self) -> &'a [u8] {
        self.batch_metadata
    }
    pub fn batch_body(&self) -> &'a [u8] {
        self.batch_body
    }
    pub fn geometry(&self) -> FlatBatchGeometry {
        self.geometry
    }
    /// Reuses the safe header from the sole official verification. This does
    /// not admit reader allocations, work or ConstantPool construction.
    pub fn record_batch(&self) -> arrow::ipc::RecordBatch<'a> {
        self.batch
    }
    pub fn metadata_version(&self) -> arrow::ipc::MetadataVersion {
        self.version
    }
}

pub(crate) fn require(
    condition: bool,
    message: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    work.step()?;
    if condition {
        Ok(())
    } else {
        Err(TypeCodecError::InvalidShape(message))
    }
}

pub(crate) fn metadata<'a>(
    input: &'a [u8],
    offset: usize,
    limit: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), TypeCodecError> {
    let prefix = continuation_prefix(input, offset);
    work.step()?;
    let prefix = prefix
        .map_err(|_| TypeCodecError::InvalidShape("invalid constant IPC continuation prefix"))?;
    let ContinuationPrefix::Metadata { start, len } = prefix else {
        return Err(TypeCodecError::InvalidShape(
            "constant IPC stream ended before its required message",
        ));
    };
    // MessageReader interprets this wire length as signed i32, even though
    // the neutral prefix primitive can project every u32 without allocating.
    require(
        len <= i32::MAX as usize && len <= limit,
        "constant IPC metadata length exceeds envelope",
        work,
    )?;
    // Standard writer alignments 8/16/32/64 all include metadata padding in
    // this length. Keep the minimum standard alignment, not NRX1's fixed64.
    require(
        len % 8 == 0,
        "constant IPC metadata framing is not eight-byte aligned",
        work,
    )?;
    let range = checked_range(input.len(), start, len)
        .map_err(|_| TypeCodecError::InvalidShape("constant IPC metadata is truncated"))?;
    work.step()?;
    let end = range.end;
    Ok((&input[range], end))
}

/// Requires continuation framing, V5/Little schema, one uncompressed flat
/// RecordBatch, and an exact EOS with no trailing data. Every metadata Message
/// is officially verified once, on the same original Decode work scope.
pub fn preflight_flat_constant_stream<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight(input, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn preflight<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    require(
        input.len() <= limits.max_input_bytes,
        "constant IPC input envelope exceeded",
        work,
    )?;
    let (schema, schema_end) = metadata(input, 0, limits.schema.max_flatbuffer_bytes, work)?;
    // The schema author also proves bodyLength==0, so the next frame starts
    // exactly at metadata_end. No speculative body skip or schema conversion.
    verify_single_field_schema_with_work(schema, expected, limits.schema, verifier, work)?;
    let (batch_metadata, body_start) =
        metadata(input, schema_end, limits.batch.max_metadata_bytes, work)?;
    let message = verified_message_observed(batch_metadata, verifier, work)?;
    require(
        message.header_type() == arrow::ipc::MessageHeader::RecordBatch,
        "constant IPC schema must precede exactly one RecordBatch",
        work,
    )?;
    let body_length = nonnegative_length(message.bodyLength()).map_err(|_| {
        TypeCodecError::InvalidShape("negative or unrepresentable constant IPC body length")
    })?;
    require(
        body_length <= limits.batch.max_body_bytes,
        "constant IPC body envelope exceeded",
        work,
    )?;
    require(
        body_length % 8 == 0,
        "constant IPC body framing is not eight-byte aligned",
        work,
    )?;
    let range = checked_range(input.len(), body_start, body_length)
        .map_err(|_| TypeCodecError::InvalidShape("constant IPC body is truncated"))?;
    work.step()?;
    // bodyLength already includes the writer's body padding. Rounding again
    // could skip hostile bytes or a genuine EOS and change the format verdict.
    let body_end = range.end;
    let batch_body = &input[range];
    let geometry =
        preflight_verified_flat_record_batch(message, batch_body, expected, limits.batch, work)?;
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "constant IPC batch header is missing",
        ))?;
    let end = continuation_prefix(input, body_end);
    work.step()?;
    let end =
        end.map_err(|_| TypeCodecError::InvalidShape("constant IPC stream is missing EOS"))?;
    require(
        matches!(end, ContinuationPrefix::End { next_offset } if next_offset == input.len()),
        "constant IPC EOS has extra messages or trailing data",
        work,
    )?;
    Ok(FlatConstantStream {
        input,
        field: expected,
        batch_metadata,
        batch_body,
        batch,
        version: message.version(),
        geometry,
    })
}

#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod tests;
