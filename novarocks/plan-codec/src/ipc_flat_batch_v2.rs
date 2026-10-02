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

//! Borrowed flat, non-dictionary constant RecordBatch geometry.
//!
//! This component allocates no arrays and invokes no Arrow reader. Its measured
//! buffer and repeated-view extents are inputs to admission, not a complete
//! first-allocation/resource model or a formal memory grant. The stream owner
//! must separately check schema identity, framing/EOS, Field/FVT correspondence,
//! and reader/schema/header/alignment/validation allocation and work bounds.

use crate::{
    ipc_schema_v2::verified_message_observed,
    physical_type_v2::{TypeCodecError, validate_field, validate_type},
};
use arrow::datatypes::{DataType, Field};
use novarocks_arrow_ipc_frame::{VerifierOptions, checked_range, nonnegative_length};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

/// Explicit geometric admission, with no application defaults.
#[derive(Clone, Copy, Debug)]
pub struct FlatBatchProjectionLimits {
    pub max_metadata_bytes: usize,
    pub max_body_bytes: usize,
    pub max_rows: usize,
    pub max_buffer_descriptors: usize,
    /// Sum of every view's declared byte length, including NULL and repeated
    /// references. This is inspection work, not unique retained backing.
    pub max_view_validation_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlatBatchGeometry {
    pub rows: usize,
    pub null_count: usize,
    pub buffer_descriptors: usize,
    pub variadic_buffers: usize,
    pub body_bytes: usize,
    /// Sum of descriptor extents, counting overlap/repeated descriptions.
    /// Unreferenced body padding remains in body_bytes.
    pub described_buffer_bytes: usize,
    pub view_validation_bytes: usize,
}

enum Layout {
    Null,
    Bits,
    Fixed(usize),
    Offsets(usize),
    Views,
}

fn layout(ty: &DataType) -> Result<Layout, TypeCodecError> {
    match ty {
        DataType::Null => Ok(Layout::Null),
        DataType::Boolean => Ok(Layout::Bits),
        DataType::FixedSizeBinary(width) => usize::try_from(*width)
            .map(Layout::Fixed)
            .map_err(|_| TypeCodecError::InvalidShape("negative fixed binary width")),
        DataType::Utf8 | DataType::Binary => Ok(Layout::Offsets(4)),
        DataType::LargeUtf8 | DataType::LargeBinary => Ok(Layout::Offsets(8)),
        DataType::Utf8View | DataType::BinaryView => Ok(Layout::Views),
        _ => ty
            .primitive_width()
            .map(Layout::Fixed)
            .ok_or(TypeCodecError::InvalidShape(
                "constant RecordBatch is not a flat non-dictionary carrier",
            )),
    }
}

fn add(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_add(right)
        .ok_or(TypeCodecError::InvalidShape("IPC batch extent overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_mul(right)
        .ok_or(TypeCodecError::InvalidShape("IPC batch extent overflow"))
}
fn length(value: i64) -> Result<usize, TypeCodecError> {
    nonnegative_length(value)
        .map_err(|_| TypeCodecError::InvalidShape("negative or unrepresentable IPC batch extent"))
}
fn require(
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
fn buffer<'a>(
    batch: arrow::ipc::RecordBatch<'_>,
    body: &'a [u8],
    index: usize,
) -> Result<&'a [u8], TypeCodecError> {
    let buffers = batch.buffers().ok_or(TypeCodecError::InvalidShape(
        "IPC batch buffer descriptors are missing",
    ))?;
    if index >= buffers.len() {
        return Err(TypeCodecError::InvalidShape(
            "IPC batch buffer index is invalid",
        ));
    }
    let descriptor = buffers.get(index);
    let range = checked_range(
        body.len(),
        length(descriptor.offset())?,
        length(descriptor.length())?,
    )
    .map_err(|_| TypeCodecError::InvalidShape("IPC batch buffer range exceeds body"))?;
    Ok(&body[range])
}

/// Checks one V5 uncompressed metadata/body pair without copying body bytes.
/// Overlapping, nonsequential and unaligned legal descriptors remain allowed;
/// this is independent of the NRX1 result writer's alignment profile. Exact
/// values, validity popcount, offsets and UTF8 are still Arrow's later verdict.
pub fn preflight_flat_record_batch(
    metadata: &[u8],
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight(metadata, body, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn preflight(
    metadata: &[u8],
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    require(
        metadata.len() <= limits.max_metadata_bytes,
        "IPC batch metadata envelope exceeded",
        work,
    )?;
    require(
        body.len() <= limits.max_body_bytes,
        "IPC batch body envelope exceeded",
        work,
    )?;
    let layout = source_layout(expected, work)?;
    let message = verified_message_observed(metadata, verifier, work)?;
    preflight_message(message, body, limits, layout, work)
}

fn source_layout(
    expected: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Layout, TypeCodecError> {
    validate_field(expected, work)?;
    novarocks_type_contract::field_logical_type(expected)?;
    validate_type(expected.data_type(), work)?;
    let layout = layout(expected.data_type())?;
    work.step()?;
    Ok(layout)
}

/// The stream owner has already verified this borrowed Message and checked
/// metadata admission. Reuse that exact parse, without a second opaque call.
pub(crate) fn preflight_verified_flat_record_batch(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    require(
        body.len() <= limits.max_body_bytes,
        "IPC batch body envelope exceeded",
        work,
    )?;
    let layout = source_layout(expected, work)?;
    preflight_message(message, body, limits, layout, work)
}

fn preflight_message(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    limits: FlatBatchProjectionLimits,
    layout: Layout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    require(
        message.version() == arrow::ipc::MetadataVersion::V5
            && message.header_type() == arrow::ipc::MessageHeader::RecordBatch
            && message
                .custom_metadata()
                .is_none_or(|entries| entries.is_empty()),
        "unsupported constant IPC RecordBatch message profile",
        work,
    )?;
    require(
        length(message.bodyLength())? == body.len(),
        "IPC batch body length mismatch",
        work,
    )?;
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "IPC RecordBatch payload is missing",
        ))?;
    require(
        batch.compression().is_none(),
        "compressed constant IPC batch is unsupported",
        work,
    )?;
    let rows = length(batch.length())?;
    require(
        rows <= limits.max_rows,
        "IPC batch row envelope exceeded",
        work,
    )?;
    let nodes = batch
        .nodes()
        .ok_or(TypeCodecError::InvalidShape("IPC batch nodes are missing"))?;
    require(
        nodes.len() == 1,
        "flat constant IPC batch must have exactly one node",
        work,
    )?;
    let node = nodes.get(0);
    require(
        length(node.length())? == rows,
        "IPC batch node length differs from rows",
        work,
    )?;
    let null_count = length(node.null_count())?;
    require(
        null_count <= rows,
        "IPC batch null count exceeds rows",
        work,
    )?;
    let buffers = batch.buffers().ok_or(TypeCodecError::InvalidShape(
        "IPC batch buffer descriptors are missing",
    ))?;
    require(
        buffers.len() <= limits.max_buffer_descriptors,
        "IPC batch buffer envelope exceeded",
        work,
    )?;
    let variadic = batch.variadicBufferCounts();
    let variadic_buffers = if matches!(layout, Layout::Views) {
        require(
            variadic.is_some_and(|counts| counts.len() == 1),
            "IPC view batch requires exactly one variadic count",
            work,
        )?;
        let count = variadic
            .ok_or(TypeCodecError::InvalidShape(
                "IPC view variadic count is missing",
            ))?
            .get(0);
        // The locked reader adds two in i64 before collecting Buffer headers.
        count.checked_add(2).ok_or(TypeCodecError::InvalidShape(
            "IPC view buffer count overflow",
        ))?;
        length(count)?
    } else {
        require(
            variadic.is_none_or(|counts| counts.is_empty()),
            "non-view IPC batch has extra variadic counts",
            work,
        )?;
        0
    };
    let count = match layout {
        Layout::Null => 0,
        Layout::Bits | Layout::Fixed(_) => 2,
        Layout::Offsets(_) => 3,
        Layout::Views => add(2, variadic_buffers)?,
    };
    require(
        buffers.len() == count,
        "IPC batch descriptor count differs from carrier",
        work,
    )?;
    let mut described_buffer_bytes = 0;
    for index in 0..buffers.len() {
        let bytes = buffer(batch, body, index)?;
        described_buffer_bytes = add(described_buffer_bytes, bytes.len())?;
        work.step()?;
    }
    let bitmap_bytes = add(rows / 8, usize::from(rows % 8 != 0))?;
    if !matches!(layout, Layout::Null) && null_count != 0 {
        require(
            buffer(batch, body, 0)?.len() >= bitmap_bytes,
            "IPC batch validity bitmap is too short",
            work,
        )?;
    }
    let mut view_validation_bytes = 0;
    match layout {
        Layout::Null => require(
            null_count == rows,
            "IPC Null node must mark every row null",
            work,
        )?,
        Layout::Bits => require(
            buffer(batch, body, 1)?.len() >= bitmap_bytes,
            "IPC Boolean values are too short",
            work,
        )?,
        Layout::Fixed(width) => require(
            buffer(batch, body, 1)?.len() >= mul(rows, width)?,
            "IPC fixed-width values are too short",
            work,
        )?,
        Layout::Offsets(width) => {
            let offsets = buffer(batch, body, 1)?;
            // Arrow's typed_offsets converts the entire descriptor before
            // selecting N+1 entries. A partial trailing element would panic
            // in Buffer::typed_data even when the useful prefix is complete.
            require(
                offsets.len() % width == 0,
                "IPC byte offsets contain a partial element",
                work,
            )?;
            // Arrow accepts an empty offsets buffer for an empty array and
            // constructs its empty OffsetBuffer later. Do not reject it here.
            let minimum = if rows == 0 && offsets.is_empty() {
                0
            } else {
                mul(add(rows, 1)?, width)?
            };
            require(
                offsets.len() >= minimum,
                "IPC byte offsets are too short",
                work,
            )?;
        }
        Layout::Views => {
            let views = buffer(batch, body, 1)?;
            // The same whole-buffer typed conversion is used for u128 views.
            // Extra complete records are allowed; a partial one is unsafe.
            require(
                views.len() % 16 == 0,
                "IPC view records contain a partial element",
                work,
            )?;
            let extent = mul(rows, 16)?;
            require(
                views.len() >= extent,
                "IPC view records are too short",
                work,
            )?;
            for record in views[..extent].chunks_exact(16) {
                let read = |start| {
                    u32::from_le_bytes([
                        record[start],
                        record[start + 1],
                        record[start + 2],
                        record[start + 3],
                    ])
                };
                let bytes = usize::try_from(read(0)).map_err(|_| {
                    TypeCodecError::InvalidShape("IPC view length is unrepresentable")
                })?;
                view_validation_bytes = add(view_validation_bytes, bytes)?;
                require(
                    view_validation_bytes <= limits.max_view_validation_bytes,
                    "IPC view validation byte envelope exceeded",
                    work,
                )?;
                if bytes > 12 {
                    let index = usize::try_from(read(8)).map_err(|_| {
                        TypeCodecError::InvalidShape("IPC view index is unrepresentable")
                    })?;
                    require(
                        index < variadic_buffers,
                        "IPC view references an invalid buffer",
                        work,
                    )?;
                    let payload = buffer(batch, body, add(index, 2)?)?;
                    let start = usize::try_from(read(12)).map_err(|_| {
                        TypeCodecError::InvalidShape("IPC view offset is unrepresentable")
                    })?;
                    checked_range(payload.len(), start, bytes).map_err(|_| {
                        TypeCodecError::InvalidShape("IPC view range exceeds its buffer")
                    })?;
                    work.step()?;
                }
            }
        }
    }
    Ok(FlatBatchGeometry {
        rows,
        null_count,
        buffer_descriptors: buffers.len(),
        variadic_buffers,
        body_bytes: body.len(),
        described_buffer_bytes,
        view_validation_bytes,
    })
}

#[cfg(test)]
mod tests;
