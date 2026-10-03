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

//! Checked recursive constant IPC framing and array-output materialization.
//! The numerical request/work projection precedes the public safe reader;
//! preparation hosts retain grant, allocation-origin and free ownership.

use crate::{
    ipc_flat_stream_v2::{FlatPoolResourceError, metadata, require},
    ipc_recursive_batch_v2::{CheckedRecursiveBatch, preflight_verified_recursive_record_batch},
    ipc_schema_v2::{
        IpcSchemaProjectionLimits, verified_message_observed, verify_single_field_schema_with_work,
    },
    physical_type_v2::TypeCodecError,
};
use arrow::{
    datatypes::{Field, Schema},
    ipc::reader::read_record_batch,
};
use arrow_buffer::Buffer;
use novarocks_arrow_ipc_frame::{
    ContinuationPrefix, VerifierOptions, checked_range, continuation_prefix, nonnegative_length,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{collections::HashMap, sync::Arc};

mod reader_allocations;
mod reader_diagnostics;
mod reader_resources;
mod reader_work;
pub use crate::ipc_flat_stream_v2::FlatReaderError as RecursiveReaderError;
pub use crate::ipc_recursive_batch_v2::RecursiveBatchProjectionLimits;
pub use reader_resources::{RecursiveReaderProjectionLimits, RecursiveReaderResourceFacts};

#[derive(Clone, Copy, Debug)]
pub struct RecursiveStreamProjectionLimits {
    pub max_input_bytes: usize,
    pub schema: IpcSchemaProjectionLimits,
    pub batch: RecursiveBatchProjectionLimits,
}

/// Immutable borrowed framing and admitted geometry for the exact source Field.
/// Geometry scratch is owned here; values and the input body remain borrowed.
pub struct RecursiveConstantStream<'a, 'f> {
    input: &'a [u8],
    field: &'f Field,
    body: &'a [u8],
    batch: arrow::ipc::RecordBatch<'a>,
    version: arrow::ipc::MetadataVersion,
    checked: CheckedRecursiveBatch<'f>,
}

pub fn preflight_recursive_constant_stream<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<RecursiveConstantStream<'a, 'f>, TypeCodecError> {
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
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, TypeCodecError> {
    require(
        input.len() <= limits.max_input_bytes,
        "recursive IPC input envelope exceeded",
        work,
    )?;
    let (schema, schema_end) = metadata(input, 0, limits.schema.max_flatbuffer_bytes, work)?;
    verify_single_field_schema_with_work(schema, expected, limits.schema, verifier, work)?;
    let (batch_metadata, body_start) = metadata(
        input,
        schema_end,
        limits.batch.flat.max_metadata_bytes,
        work,
    )?;
    let message = verified_message_observed(batch_metadata, verifier, work)?;
    require(
        message.header_type() == arrow::ipc::MessageHeader::RecordBatch,
        "recursive IPC schema must precede exactly one RecordBatch",
        work,
    )?;
    let body_length = nonnegative_length(message.bodyLength()).map_err(|_| {
        TypeCodecError::InvalidShape("recursive IPC body length is negative or unrepresentable")
    })?;
    require(
        body_length <= limits.batch.flat.max_body_bytes && body_length % 8 == 0,
        "recursive IPC body framing exceeds its aligned envelope",
        work,
    )?;
    let range = checked_range(input.len(), body_start, body_length)
        .map_err(|_| TypeCodecError::InvalidShape("recursive IPC body is truncated"))?;
    work.step()?;
    let body_end = range.end;
    let body = &input[range];
    let checked =
        preflight_verified_recursive_record_batch(message, body, expected, limits.batch, work)?;
    let end = continuation_prefix(input, body_end);
    work.step()?;
    let end = end.map_err(|_| TypeCodecError::InvalidShape("recursive IPC EOS is missing"))?;
    require(
        matches!(end, ContinuationPrefix::End { next_offset } if next_offset == input.len()),
        "recursive IPC EOS has extra messages or trailing data",
        work,
    )?;
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "recursive IPC RecordBatch is missing",
        ))?;
    Ok(RecursiveConstantStream {
        input,
        field: expected,
        body,
        batch,
        version: message.version(),
        checked,
    })
}

impl RecursiveConstantStream<'_, '_> {
    fn reader_input(&self) -> reader_resources::ReaderInput<'_, '_> {
        reader_resources::ReaderInput {
            field: self.field,
            batch: self.batch,
            body: self.body,
            nodes: &self.checked.nodes,
            geometry: self.checked.geometry,
            geometry_scratch_request_bytes: self.checked.scratch_request_bytes,
            geometry_scratch_request_count: self.checked.scratch_request_count,
        }
    }
    fn reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<RecursiveReaderResourceFacts, FlatPoolResourceError> {
        require(
            self.input.len() <= source_retained_bytes,
            "recursive reader source retention below visible input",
            work,
        )?;
        reader_resources::preflight(
            &self.reader_input(),
            value_type,
            source_retained_bytes,
            policy,
            limits,
            work,
        )
    }
    /// Source retention includes the complete input allocation, original Field
    /// and both original Field and supplied value-type backing, including
    /// their names/metadata and HashMap removed-entry capacity.
    /// Earlier framing and geometry retain their separate admission boundary.
    /// Numerical grouping uses fixed stack indices under the admitted type-node
    /// bound; stack-scratch admission remains the caller's separate obligation.
    pub fn preflight_reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<RecursiveReaderResourceFacts, FlatPoolResourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result =
            self.reader_resources(value_type, source_retained_bytes, policy, limits, &mut work);
        if matches!(&result, Err(FlatPoolResourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    /// The original Field Arc and admitted full value type are retained. No
    /// array, Schema or body backing is created before the complete output gate.
    pub fn materialize_pool(
        &self,
        field: Arc<Field>,
        value_type: FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            require(
                std::ptr::eq(self.field, field.as_ref()),
                "recursive reader requires the original source Field Arc",
                &mut work,
            )
            .map_err(FlatPoolResourceError::from)?;
            self.reader_resources(
                &value_type,
                source_retained_bytes,
                policy,
                limits,
                &mut work,
            )?;
            work.flush()?;
            let schema = Arc::new(Schema::new([Arc::clone(&field)]));
            work.flush()?;
            let body = Buffer::from_slice_ref(self.body);
            work.flush()?;
            let decoded = read_record_batch(
                &body,
                self.batch,
                schema,
                &HashMap::new(),
                None,
                &self.version,
            )
            .map_err(|error| RecursiveReaderError::Arrow(error.to_string()));
            work.flush()?;
            let decoded = decoded?;
            let data = decoded.column(0).to_data();
            work.flush()?;
            let pool = ConstantPool::try_new(
                field,
                value_type,
                data,
                policy,
                CompilePhase::Decode,
                work.control(),
            )
            .map_err(FlatPoolResourceError::from)?;
            Ok(pool)
        })();
        if matches!(
            &result,
            Err(RecursiveReaderError::Projection(
                FlatPoolResourceError::Control(_)
            ))
        ) {
            return result;
        }
        work.finish()?;
        result
    }
}

#[cfg(test)]
#[path = "ipc_recursive_stream_v2_tests.rs"]
mod tests;
