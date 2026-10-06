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

pub(crate) mod progress;
pub(crate) mod resource_work;
pub(crate) use progress::IpcReaderProgressFacts;
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
    progress: Option<IpcReaderProgressFacts>,
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

pub(crate) fn framing_step(
    admission: &mut Option<&mut progress::Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if let Some(a) = admission.as_deref_mut() {
        a.batch_step(work)?;
    } else {
        work.step()?;
    }
    Ok(())
}
pub(crate) fn framing_require(
    condition: bool,
    message: &'static str,
    mut admission: Option<&mut progress::Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    framing_step(&mut admission, work)?;
    if condition {
        Ok(())
    } else {
        Err(TypeCodecError::InvalidShape(message))
    }
}
pub(crate) fn framing_limit(
    condition: bool,
    message: &'static str,
    admission: Option<&mut progress::Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if admission.is_some() && !condition {
        return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
    }
    framing_require(condition, message, admission, work)
}
#[cfg(test)]
pub(crate) fn metadata<'a>(
    input: &'a [u8],
    offset: usize,
    limit: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), TypeCodecError> {
    metadata_in(input, offset, limit, None, None, work)
}
pub(crate) fn metadata_in<'a>(
    input: &'a [u8],
    offset: usize,
    limit: usize,
    verifier_apparent: Option<usize>,
    admission: Option<&mut progress::Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), TypeCodecError> {
    metadata_core(input, offset, limit, verifier_apparent, admission, work)
}
fn metadata_core<'a>(
    input: &'a [u8],
    offset: usize,
    limit: usize,
    verifier_apparent: Option<usize>,
    mut admission: Option<&mut progress::Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), TypeCodecError> {
    let prefix = continuation_prefix(input, offset);
    if let (Some(a), Some(apparent), Ok(ContinuationPrefix::Metadata { len, .. })) =
        (admission.as_deref_mut(), verifier_apparent, &prefix)
    {
        let upper = apparent
            .checked_add(*len)
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        a.batch_work_add(upper)?;
    }
    framing_step(&mut admission, work)?;
    let prefix = prefix
        .map_err(|_| TypeCodecError::InvalidShape("invalid constant IPC continuation prefix"))?;
    let ContinuationPrefix::Metadata { start, len } = prefix else {
        return Err(TypeCodecError::InvalidShape(
            "constant IPC stream ended before its required message",
        ));
    };
    // MessageReader interprets this wire length as signed i32, even though
    // the neutral prefix primitive can project every u32 without allocating.
    framing_limit(
        len <= i32::MAX as usize && len <= limit,
        "constant IPC metadata length exceeds envelope",
        admission.as_deref_mut(),
        work,
    )?;
    // Standard writer alignments 8/16/32/64 all include metadata padding in
    // this length. Keep the minimum standard alignment, not NRX1's fixed64.
    framing_require(
        len % 8 == 0,
        "constant IPC metadata framing is not eight-byte aligned",
        admission.as_deref_mut(),
        work,
    )?;
    let range = checked_range(input.len(), start, len)
        .map_err(|_| TypeCodecError::InvalidShape("constant IPC metadata is truncated"))?;
    framing_step(&mut admission, work)?;
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

pub(crate) fn preflight_flat_constant_stream_in<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    source_retained_bytes: usize,
    admit: &mut progress::ReaderAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    if input.len() > source_retained_bytes {
        return Err(TypeCodecError::InvalidShape(
            "flat stream source retention is below visible input",
        ));
    }
    let mut admission = progress::Admission::new(source_retained_bytes, admit);
    admission.check()?;
    preflight_core(
        input,
        expected,
        limits,
        verifier,
        Some(&mut admission),
        work,
    )
}

fn preflight<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    preflight_core(input, expected, limits, verifier, None, work)
}
fn preflight_core<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    mut admission: Option<&mut progress::Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    if admission.is_some() {
        if input.len() > limits.max_input_bytes {
            return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
        }
    } else {
        require(
            input.len() <= limits.max_input_bytes,
            "constant IPC input envelope exceeded",
            work,
        )?;
    }
    let (schema, schema_end) = metadata_in(
        input,
        0,
        limits.schema.max_flatbuffer_bytes,
        Some(verifier.max_apparent_size),
        admission.as_deref_mut(),
        work,
    )?;
    if admission.is_some() {
        framing_step(&mut admission, work)?;
    }
    // The schema author also proves bodyLength==0, so the next frame starts
    // exactly at metadata_end. No speculative body skip or schema conversion.
    if let Some(a) = admission.as_deref_mut() {
        let source = a.source();
        let mut capture = |f: &crate::ipc_schema_v2::SchemaWriterRequestFacts| {
            a.requests(4, f.request_bytes, f.request_count)?;
            a.schema_work(f.work_upper_bound)
        };
        crate::ipc_schema_v2::verify_single_field_schema_in(
            schema,
            expected,
            limits.schema,
            verifier,
            source,
            usize::MAX,
            &mut capture,
            work,
        )?;
    } else {
        verify_single_field_schema_with_work(schema, expected, limits.schema, verifier, work)?;
    }
    let (batch_metadata, body_start) = metadata_in(
        input,
        schema_end,
        limits.batch.max_metadata_bytes,
        Some(verifier.max_apparent_size),
        admission.as_deref_mut(),
        work,
    )?;
    let message = verified_message_observed(batch_metadata, verifier, work)?;
    framing_require(
        message.header_type() == arrow::ipc::MessageHeader::RecordBatch,
        "constant IPC schema must precede exactly one RecordBatch",
        admission.as_deref_mut(),
        work,
    )?;
    let body_length = nonnegative_length(message.bodyLength()).map_err(|_| {
        TypeCodecError::InvalidShape("negative or unrepresentable constant IPC body length")
    })?;
    framing_limit(
        body_length <= limits.batch.max_body_bytes,
        "constant IPC body envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    framing_require(
        body_length % 8 == 0,
        "constant IPC body framing is not eight-byte aligned",
        admission.as_deref_mut(),
        work,
    )?;
    let range = checked_range(input.len(), body_start, body_length)
        .map_err(|_| TypeCodecError::InvalidShape("constant IPC body is truncated"))?;
    framing_step(&mut admission, work)?;
    // bodyLength already includes the writer's body padding. Rounding again
    // could skip hostile bytes or a genuine EOS and change the format verdict.
    let body_end = range.end;
    let batch_body = &input[range];
    let geometry = if let Some(a) = admission.as_deref_mut() {
        crate::ipc_flat_batch_v2::preflight_verified_flat_record_batch_in(
            message,
            batch_body,
            expected,
            limits.batch,
            a,
            work,
        )?
    } else {
        preflight_verified_flat_record_batch(message, batch_body, expected, limits.batch, work)?
    };
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "constant IPC batch header is missing",
        ))?;
    let end = continuation_prefix(input, body_end);
    framing_step(&mut admission, work)?;
    let end =
        end.map_err(|_| TypeCodecError::InvalidShape("constant IPC stream is missing EOS"))?;
    framing_require(
        matches!(end, ContinuationPrefix::End { next_offset } if next_offset == input.len()),
        "constant IPC EOS has extra messages or trailing data",
        admission.as_deref_mut(),
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
        progress: admission.as_deref().map(progress::Admission::facts),
    })
}

#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod tests;
