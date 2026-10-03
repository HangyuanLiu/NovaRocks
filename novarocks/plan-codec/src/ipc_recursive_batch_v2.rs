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

//! Borrowed recursive IPC geometry. Resource admission precedes owned geometry.

use arrow::datatypes::Field;

/// One actual FieldNode occurrence in declaration DFS order. All extents are
/// checked against the original batch/body before publication.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RecursiveNodeGeometry<'a> {
    pub(crate) field: &'a Field,
    pub(crate) rows: usize,
    pub(crate) null_count: usize,
    pub(crate) depth: usize,
    pub(crate) list_map_ancestors: usize,
    pub(crate) children: usize,
    pub(crate) subtree_nodes: usize,
    pub(crate) subtree_buffer_descriptors: usize,
    pub(crate) buffer_start: usize,
    pub(crate) buffer_count: usize,
    pub(crate) variadic_buffers: usize,
    pub(crate) described_buffer_bytes: usize,
    pub(crate) view_validation_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecursiveBatchGeometry {
    pub(crate) rows: usize,
    pub(crate) null_count: usize,
    pub(crate) field_nodes: usize,
    pub(crate) total_rows: usize,
    pub(crate) maximum_depth: usize,
    pub(crate) buffer_descriptors: usize,
    pub(crate) variadic_buffers: usize,
    pub(crate) view_fields: usize,
    pub(crate) body_bytes: usize,
    pub(crate) described_buffer_bytes: usize,
    pub(crate) view_validation_bytes: usize,
}

#[cfg(test)]
use crate::ipc_schema_v2::verified_message_observed;
use crate::{
    ipc_flat_batch_v2::{self, FlatBatchProjectionLimits},
    physical_type_v2::{TypeCodecError, validate_field, validate_type},
};
use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_arrow_ipc_frame::nonnegative_length;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};

#[cfg(test)]
use novarocks_type_contract::{CompilePhase, PureCompileControl};

/// Geometry scratch has its own explicit request envelope. The preceding
/// framing/type stage and subsequent reader host retain their own admission.
#[derive(Clone, Copy, Debug)]
pub struct RecursiveBatchProjectionLimits {
    pub flat: FlatBatchProjectionLimits,
    pub max_field_nodes: usize,
    pub max_total_rows: usize,
    pub max_geometry_request_bytes: usize,
}

pub(crate) struct CheckedRecursiveBatch<'a> {
    pub(crate) nodes: Vec<RecursiveNodeGeometry<'a>>,
    pub(crate) geometry: RecursiveBatchGeometry,
    pub(crate) scratch_request_bytes: usize,
    pub(crate) scratch_request_count: usize,
}
fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_add(b)
        .ok_or(shape("recursive IPC extent overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_mul(b)
        .ok_or(shape("recursive IPC extent overflow"))
}
fn length(n: i64) -> Result<usize, TypeCodecError> {
    nonnegative_length(n).map_err(|_| shape("recursive IPC extent is negative or unrepresentable"))
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
        Err(shape(message))
    }
}

#[cfg(test)]
pub(crate) fn preflight_recursive_record_batch<'a>(
    metadata: &[u8],
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        require(
            metadata.len() <= limits.flat.max_metadata_bytes,
            "recursive IPC metadata envelope exceeded",
            &mut work,
        )?;
        let message = verified_message_observed(metadata, verifier, &mut work)?;
        preflight_verified_recursive_record_batch(message, body, expected, limits, &mut work)
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub(crate) fn preflight_verified_recursive_record_batch<'a>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    require(
        body.len() <= limits.flat.max_body_bytes,
        "recursive IPC body envelope exceeded",
        work,
    )?;
    validate_field(expected, work)?;
    novarocks_type_contract::field_logical_type(expected)?;
    validate_type(expected.data_type(), work)?;
    require(
        message.version() == arrow::ipc::MetadataVersion::V5
            && message.header_type() == arrow::ipc::MessageHeader::RecordBatch
            && message.custom_metadata().is_none_or(|m| m.is_empty()),
        "unsupported recursive IPC message profile",
        work,
    )?;
    require(
        length(message.bodyLength())? == body.len(),
        "recursive IPC body length mismatch",
        work,
    )?;
    let batch = message
        .header_as_record_batch()
        .ok_or(shape("recursive IPC RecordBatch is missing"))?;
    require(
        batch.compression().is_none(),
        "compressed recursive IPC batch is unsupported",
        work,
    )?;
    let rows = length(batch.length())?;
    require(
        rows <= limits.flat.max_rows,
        "recursive IPC root row envelope exceeded",
        work,
    )?;
    let raw_nodes = batch
        .nodes()
        .ok_or(shape("recursive IPC nodes are missing"))?;
    let buffers = batch
        .buffers()
        .ok_or(shape("recursive IPC buffers are missing"))?;
    require(
        !raw_nodes.is_empty() && raw_nodes.len() <= limits.max_field_nodes,
        "recursive IPC node envelope exceeded",
        work,
    )?;
    require(
        buffers.len() <= limits.flat.max_buffer_descriptors,
        "recursive IPC buffer envelope exceeded",
        work,
    )?;
    let scratch = std::alloc::Layout::array::<RecursiveNodeGeometry<'_>>(raw_nodes.len())
        .map_err(|_| shape("recursive IPC geometry layout is not representable"))?;
    require(
        scratch.size() <= limits.max_geometry_request_bytes,
        "recursive IPC geometry request envelope exceeded",
        work,
    )?;
    work.flush()?;
    let mut nodes = Vec::new();
    nodes
        .try_reserve_exact(raw_nodes.len())
        .map_err(|_| TypeCodecError::Control(CompileControlError::ResourceExhausted))?;
    work.flush()?;
    let mut walker = Walker {
        batch,
        body,
        limits,
        nodes,
        node_cursor: 0,
        buffer_cursor: 0,
        view_cursor: 0,
        total_rows: 0,
        described: 0,
        view_bytes: 0,
        total_variadic: 0,
        max_depth: 0,
    };
    let root = walker.field(expected, 1, 0, work)?;
    require(
        root.rows == rows,
        "recursive IPC root node length differs from batch",
        work,
    )?;
    require(
        walker.node_cursor == raw_nodes.len(),
        "recursive IPC batch has extra FieldNodes",
        work,
    )?;
    require(
        walker.buffer_cursor == buffers.len(),
        "recursive IPC batch has extra buffers",
        work,
    )?;
    let view_counts = batch.variadicBufferCounts();
    require(
        view_counts.map_or(0, |c| c.len()) == walker.view_cursor,
        "recursive IPC batch has extra variadic counts",
        work,
    )?;
    let geometry = RecursiveBatchGeometry {
        rows,
        null_count: root.null_count,
        field_nodes: walker.node_cursor,
        total_rows: walker.total_rows,
        maximum_depth: walker.max_depth,
        buffer_descriptors: walker.buffer_cursor,
        variadic_buffers: walker.total_variadic,
        view_fields: walker.view_cursor,
        body_bytes: body.len(),
        described_buffer_bytes: walker.described,
        view_validation_bytes: walker.view_bytes,
    };
    Ok(CheckedRecursiveBatch {
        nodes: walker.nodes,
        geometry,
        scratch_request_bytes: scratch.size(),
        scratch_request_count: usize::from(scratch.size() != 0),
    })
}
struct Walker<'b, 'a> {
    batch: arrow::ipc::RecordBatch<'b>,
    body: &'b [u8],
    limits: RecursiveBatchProjectionLimits,
    nodes: Vec<RecursiveNodeGeometry<'a>>,
    node_cursor: usize,
    buffer_cursor: usize,
    view_cursor: usize,
    total_rows: usize,
    described: usize,
    view_bytes: usize,
    total_variadic: usize,
    max_depth: usize,
}
impl<'b, 'a> Walker<'b, 'a> {
    fn field(
        &mut self,
        field: &'a Field,
        depth: usize,
        list_map_ancestors: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<RecursiveNodeGeometry<'a>, TypeCodecError> {
        let raw = self
            .batch
            .nodes()
            .ok_or(shape("recursive IPC nodes are missing"))?;
        require(
            self.node_cursor < raw.len(),
            "recursive IPC FieldNode is missing",
            work,
        )?;
        let node = raw.get(self.node_cursor);
        let rows = length(node.length())?;
        let null_count = length(node.null_count())?;
        require(
            null_count <= rows,
            "recursive IPC node NULL count exceeds its rows",
            work,
        )?;
        self.total_rows = add(self.total_rows, rows)?;
        require(
            self.total_rows <= self.limits.max_total_rows,
            "recursive IPC total row envelope exceeded",
            work,
        )?;
        self.max_depth = self.max_depth.max(depth);
        let index = self.node_cursor;
        self.node_cursor = add(self.node_cursor, 1)?;
        let start = self.buffer_cursor;
        let mut item = RecursiveNodeGeometry {
            field,
            rows,
            null_count,
            depth,
            list_map_ancestors,
            children: 0,
            subtree_nodes: 0,
            subtree_buffer_descriptors: 0,
            buffer_start: start,
            buffer_count: 0,
            variadic_buffers: 0,
            described_buffer_bytes: 0,
            view_validation_bytes: 0,
        };
        // The checked node envelope reserves this exact DFS capacity once.
        self.nodes.push(item);
        work.step()?;
        match field.data_type() {
            DataType::Struct(fields) => {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 1;
                item.children = fields.len();
                for child in fields {
                    let child = self.field(child, add(depth, 1)?, list_map_ancestors, work)?;
                    require(
                        child.rows == rows,
                        "recursive IPC Struct child length differs from parent",
                        work,
                    )?;
                }
            }
            DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 2;
                item.children = 1;
                let width = if matches!(field.data_type(), DataType::LargeList(_)) {
                    8
                } else {
                    4
                };
                let offsets = ipc_flat_batch_v2::buffer(self.batch, self.body, self.buffer_cursor)?;
                self.described = add(self.described, offsets.len())?;
                item.described_buffer_bytes = add(item.described_buffer_bytes, offsets.len())?;
                self.buffer_cursor = add(self.buffer_cursor, 1)?;
                require(
                    offsets.len() % width == 0,
                    "recursive IPC list offsets contain a partial element",
                    work,
                )?;
                let entries = if rows == 0 && offsets.is_empty() {
                    0
                } else {
                    add(rows, 1)?
                };
                require(
                    offsets.len() >= mul(entries, width)?,
                    "recursive IPC list offsets are too short",
                    work,
                )?;
                let mut previous = 0;
                for offset in offsets[..mul(entries, width)?].chunks_exact(width) {
                    let signed = if width == 4 {
                        i64::from(i32::from_le_bytes(
                            offset
                                .try_into()
                                .map_err(|_| shape("recursive IPC offset width mismatch"))?,
                        ))
                    } else {
                        i64::from_le_bytes(
                            offset
                                .try_into()
                                .map_err(|_| shape("recursive IPC offset width mismatch"))?,
                        )
                    };
                    let current = length(signed)?;
                    require(
                        current >= previous,
                        "recursive IPC list offsets are not monotone",
                        work,
                    )?;
                    previous = current;
                }
                let child = self.field(child, add(depth, 1)?, add(list_map_ancestors, 1)?, work)?;
                require(
                    previous <= child.rows,
                    "recursive IPC list offsets exceed child extent",
                    work,
                )?;
            }
            DataType::Dictionary(..)
            | DataType::Union(..)
            | DataType::RunEndEncoded(..)
            | DataType::FixedSizeList(..)
            | DataType::ListView(..)
            | DataType::LargeListView(..) => {
                return Err(shape(
                    "recursive IPC carrier is outside the frozen supported profile",
                ));
            }
            ty => {
                let layout = ipc_flat_batch_v2::layout(ty)?;
                let variadic = if matches!(layout, ipc_flat_batch_v2::Layout::Views) {
                    let counts = self
                        .batch
                        .variadicBufferCounts()
                        .ok_or(shape("recursive IPC view variadic count is missing"))?;
                    require(
                        self.view_cursor < counts.len(),
                        "recursive IPC view variadic count is missing",
                        work,
                    )?;
                    let count = counts.get(self.view_cursor);
                    count
                        .checked_add(2)
                        .ok_or(shape("recursive IPC view count overflow"))?;
                    self.view_cursor = add(self.view_cursor, 1)?;
                    length(count)?
                } else {
                    0
                };
                let leaf = ipc_flat_batch_v2::inspect_leaf_at(
                    self.batch,
                    self.body,
                    layout,
                    rows,
                    null_count,
                    start,
                    variadic,
                    self.limits.flat,
                    work,
                )?;
                item.described_buffer_bytes = leaf.described_buffer_bytes;
                item.view_validation_bytes = leaf.view_validation_bytes;
                item.buffer_count = leaf.buffer_descriptors;
                item.variadic_buffers = variadic;
                self.total_variadic = add(self.total_variadic, variadic)?;
                self.buffer_cursor = add(self.buffer_cursor, leaf.buffer_descriptors)?;
                self.described = add(self.described, leaf.described_buffer_bytes)?;
                self.view_bytes = add(self.view_bytes, leaf.view_validation_bytes)?;
                require(
                    self.view_bytes <= self.limits.flat.max_view_validation_bytes,
                    "recursive IPC cumulative view byte envelope exceeded",
                    work,
                )?;
            }
        }
        item.subtree_nodes = self.node_cursor - index;
        item.subtree_buffer_descriptors = self.buffer_cursor - start;
        self.nodes[index] = item;
        work.step()?;
        Ok(item)
    }
    fn container_validity(
        &mut self,
        rows: usize,
        null_count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, TypeCodecError> {
        let validity = ipc_flat_batch_v2::buffer(self.batch, self.body, self.buffer_cursor)?;
        self.described = add(self.described, validity.len())?;
        self.buffer_cursor = add(self.buffer_cursor, 1)?;
        if null_count != 0 {
            require(
                validity.len() >= add(rows / 8, usize::from(!rows.is_multiple_of(8)))?,
                "recursive IPC container validity bitmap is too short",
                work,
            )?;
        }
        work.step()?;
        Ok(validity.len())
    }
}

#[cfg(test)]
#[path = "ipc_recursive_batch_v2_tests.rs"]
mod tests;
