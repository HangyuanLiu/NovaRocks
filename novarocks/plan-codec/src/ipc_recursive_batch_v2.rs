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

use crate::ipc_flat_stream_v2::progress::Admission;
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
#[cfg(test)]
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

fn numeric_parent<T>(parent: bool, result: Result<T, TypeCodecError>) -> Result<T, TypeCodecError> {
    if parent {
        result.map_err(|_| CompileControlError::ResourceExhausted.into())
    } else {
        result
    }
}
fn parent_step(
    admission: &mut Option<&mut Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if let Some(a) = admission.as_deref_mut() {
        a.batch_step(work)?;
    } else {
        work.step()?;
    }
    Ok(())
}
fn require_parent(
    condition: bool,
    message: &'static str,
    mut admission: Option<&mut Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    parent_step(&mut admission, work)?;
    if condition {
        Ok(())
    } else {
        Err(shape(message))
    }
}
fn limit_parent(
    condition: bool,
    message: &'static str,
    admission: Option<&mut Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if admission.is_some() && !condition {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    require_parent(condition, message, admission, work)
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
    preflight_core(message, body, expected, limits, None, work)
}
pub(crate) fn preflight_verified_recursive_record_batch_in<'a>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    preflight_core(message, body, expected, limits, Some(admission), work)
}
fn preflight_core<'a>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    mut admission: Option<&mut Admission<'_, '_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    // Capture this actual borrowed RecordBatch's known geometry allocation
    // before any header completion can invoke the caller's controller.
    if let Some(a) = admission.as_deref_mut()
        && let Some(raw) = message.header_as_record_batch().and_then(|b| b.nodes())
    {
        let layout = std::alloc::Layout::array::<RecursiveNodeGeometry<'_>>(raw.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        if raw.len() > limits.max_field_nodes || layout.size() > limits.max_geometry_request_bytes {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        a.requests(0, layout.size(), usize::from(layout.size() != 0))?;
        a.batch_work_add(layout.size())?;
    }
    limit_parent(
        body.len() <= limits.flat.max_body_bytes,
        "recursive IPC body envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    if let Some(a) = admission.as_deref_mut() {
        a.batch_work_add(
            a.source()
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?,
        )?;
    }
    validate_field(expected, work)?;
    novarocks_type_contract::field_logical_type(expected)?;
    if let Some(a) = admission.as_deref_mut() {
        let shared = std::cell::RefCell::new(a);
        let mut scratch =
            |layout: std::alloc::Layout| shared.borrow_mut().batch_work_add(layout.size());
        let mut capture = |visit| {
            let mut a = shared.borrow_mut();
            let upper = if matches!(visit, novarocks_type_contract::ValueTypeVisit::Field(_)) {
                a.source()
                    .checked_add(1)
                    .ok_or(CompileControlError::ResourceExhausted)?
            } else {
                1
            };
            a.batch_work_add(upper)
        };
        crate::physical_type_v2::validate_type_with_scratch_observed(
            expected.data_type(),
            &mut scratch,
            &mut capture,
            work,
        )?;
    } else {
        validate_type(expected.data_type(), work)?;
    }
    require_parent(
        message.version() == arrow::ipc::MetadataVersion::V5
            && message.header_type() == arrow::ipc::MessageHeader::RecordBatch
            && message.custom_metadata().is_none_or(|m| m.is_empty()),
        "unsupported recursive IPC message profile",
        admission.as_deref_mut(),
        work,
    )?;
    require_parent(
        length(message.bodyLength())? == body.len(),
        "recursive IPC body length mismatch",
        admission.as_deref_mut(),
        work,
    )?;
    let batch = message
        .header_as_record_batch()
        .ok_or(shape("recursive IPC RecordBatch is missing"))?;
    require_parent(
        batch.compression().is_none(),
        "compressed recursive IPC batch is unsupported",
        admission.as_deref_mut(),
        work,
    )?;
    let rows = length(batch.length())?;
    limit_parent(
        rows <= limits.flat.max_rows,
        "recursive IPC root row envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    let raw_nodes = batch
        .nodes()
        .ok_or(shape("recursive IPC nodes are missing"))?;
    let buffers = batch
        .buffers()
        .ok_or(shape("recursive IPC buffers are missing"))?;
    require_parent(
        !raw_nodes.is_empty() && raw_nodes.len() <= limits.max_field_nodes,
        "recursive IPC node envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    limit_parent(
        buffers.len() <= limits.flat.max_buffer_descriptors,
        "recursive IPC buffer envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    let scratch =
        std::alloc::Layout::array::<RecursiveNodeGeometry<'_>>(raw_nodes.len()).map_err(|_| {
            if admission.is_some() {
                CompileControlError::ResourceExhausted.into()
            } else {
                shape("recursive IPC geometry layout is not representable")
            }
        })?;
    if let Some(a) = admission.as_deref_mut() {
        if scratch.size() > limits.max_geometry_request_bytes {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        a.requests(0, scratch.size(), usize::from(scratch.size() != 0))?;
    }
    limit_parent(
        scratch.size() <= limits.max_geometry_request_bytes,
        "recursive IPC geometry request envelope exceeded",
        admission.as_deref_mut(),
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
        admission,
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
    require_parent(
        root.rows == rows,
        "recursive IPC root node length differs from batch",
        walker.admission.as_deref_mut(),
        work,
    )?;
    require_parent(
        walker.node_cursor == raw_nodes.len(),
        "recursive IPC batch has extra FieldNodes",
        walker.admission.as_deref_mut(),
        work,
    )?;
    require_parent(
        walker.buffer_cursor == buffers.len(),
        "recursive IPC batch has extra buffers",
        walker.admission.as_deref_mut(),
        work,
    )?;
    let view_counts = batch.variadicBufferCounts();
    require_parent(
        view_counts.map_or(0, |c| c.len()) == walker.view_cursor,
        "recursive IPC batch has extra variadic counts",
        walker.admission.as_deref_mut(),
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
struct Walker<'b, 'a, 'p, 'c, 'd> {
    admission: Option<&'p mut Admission<'c, 'd>>,
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
impl<'b, 'a, 'p, 'c, 'd> Walker<'b, 'a, 'p, 'c, 'd> {
    fn field(
        &mut self,
        field: &'a Field,
        depth: usize,
        list_map_ancestors: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<RecursiveNodeGeometry<'a>, TypeCodecError> {
        let parent = self.admission.is_some();
        let raw = self
            .batch
            .nodes()
            .ok_or(shape("recursive IPC nodes are missing"))?;
        require_parent(
            self.node_cursor < raw.len(),
            "recursive IPC FieldNode is missing",
            self.admission.as_deref_mut(),
            work,
        )?;
        let node = raw.get(self.node_cursor);
        let rows = length(node.length())?;
        let null_count = length(node.null_count())?;
        require_parent(
            null_count <= rows,
            "recursive IPC node NULL count exceeds its rows",
            self.admission.as_deref_mut(),
            work,
        )?;
        self.total_rows = numeric_parent(parent, add(self.total_rows, rows))?;
        limit_parent(
            self.total_rows <= self.limits.max_total_rows,
            "recursive IPC total row envelope exceeded",
            self.admission.as_deref_mut(),
            work,
        )?;
        self.max_depth = self.max_depth.max(depth);
        let index = self.node_cursor;
        self.node_cursor = numeric_parent(parent, add(self.node_cursor, 1))?;
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
        parent_step(&mut self.admission, work)?;
        match field.data_type() {
            DataType::Struct(fields) => {
                item.described_buffer_bytes = self.container_validity(rows, null_count, work)?;
                item.buffer_count = 1;
                item.children = fields.len();
                for child in fields {
                    let child = self.field(
                        child,
                        numeric_parent(parent, add(depth, 1))?,
                        list_map_ancestors,
                        work,
                    )?;
                    require_parent(
                        child.rows == rows,
                        "recursive IPC Struct child length differs from parent",
                        self.admission.as_deref_mut(),
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
                self.described = numeric_parent(parent, add(self.described, offsets.len()))?;
                item.described_buffer_bytes =
                    numeric_parent(parent, add(item.described_buffer_bytes, offsets.len()))?;
                self.buffer_cursor = numeric_parent(parent, add(self.buffer_cursor, 1))?;
                require_parent(
                    offsets.len() % width == 0,
                    "recursive IPC list offsets contain a partial element",
                    self.admission.as_deref_mut(),
                    work,
                )?;
                let entries = if rows == 0 && offsets.is_empty() {
                    0
                } else {
                    numeric_parent(parent, add(rows, 1))?
                };
                require_parent(
                    offsets.len() >= numeric_parent(parent, mul(entries, width))?,
                    "recursive IPC list offsets are too short",
                    self.admission.as_deref_mut(),
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
                    require_parent(
                        current >= previous,
                        "recursive IPC list offsets are not monotone",
                        self.admission.as_deref_mut(),
                        work,
                    )?;
                    previous = current;
                }
                let child = self.field(
                    child,
                    numeric_parent(parent, add(depth, 1))?,
                    numeric_parent(parent, add(list_map_ancestors, 1))?,
                    work,
                )?;
                require_parent(
                    previous <= child.rows,
                    "recursive IPC list offsets exceed child extent",
                    self.admission.as_deref_mut(),
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
                    require_parent(
                        self.view_cursor < counts.len(),
                        "recursive IPC view variadic count is missing",
                        self.admission.as_deref_mut(),
                        work,
                    )?;
                    let count = counts.get(self.view_cursor);
                    numeric_parent(
                        parent,
                        count
                            .checked_add(2)
                            .ok_or(shape("recursive IPC view count overflow")),
                    )?;
                    self.view_cursor = numeric_parent(parent, add(self.view_cursor, 1))?;
                    length(count)?
                } else {
                    0
                };
                let leaf = ipc_flat_batch_v2::inspect_leaf_at_core(
                    self.batch,
                    self.body,
                    layout,
                    rows,
                    null_count,
                    start,
                    variadic,
                    self.limits.flat,
                    self.admission.as_deref_mut(),
                    work,
                )?;
                item.described_buffer_bytes = leaf.described_buffer_bytes;
                item.view_validation_bytes = leaf.view_validation_bytes;
                item.buffer_count = leaf.buffer_descriptors;
                item.variadic_buffers = variadic;
                self.total_variadic = numeric_parent(parent, add(self.total_variadic, variadic))?;
                self.buffer_cursor =
                    numeric_parent(parent, add(self.buffer_cursor, leaf.buffer_descriptors))?;
                self.described =
                    numeric_parent(parent, add(self.described, leaf.described_buffer_bytes))?;
                self.view_bytes =
                    numeric_parent(parent, add(self.view_bytes, leaf.view_validation_bytes))?;
                limit_parent(
                    self.view_bytes <= self.limits.flat.max_view_validation_bytes,
                    "recursive IPC cumulative view byte envelope exceeded",
                    self.admission.as_deref_mut(),
                    work,
                )?;
            }
        }
        item.subtree_nodes = self.node_cursor - index;
        item.subtree_buffer_descriptors = self.buffer_cursor - start;
        self.nodes[index] = item;
        parent_step(&mut self.admission, work)?;
        Ok(item)
    }
    fn container_validity(
        &mut self,
        rows: usize,
        null_count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, TypeCodecError> {
        let parent = self.admission.is_some();
        let validity = ipc_flat_batch_v2::buffer(self.batch, self.body, self.buffer_cursor)?;
        self.described = numeric_parent(parent, add(self.described, validity.len()))?;
        self.buffer_cursor = numeric_parent(parent, add(self.buffer_cursor, 1))?;
        if null_count != 0 {
            require_parent(
                validity.len()
                    >= numeric_parent(parent, add(rows / 8, usize::from(!rows.is_multiple_of(8))))?,
                "recursive IPC container validity bitmap is too short",
                self.admission.as_deref_mut(),
                work,
            )?;
        }
        parent_step(&mut self.admission, work)?;
        Ok(validity.len())
    }
}

#[cfg(test)]
#[path = "ipc_recursive_batch_v2_tests.rs"]
mod tests;
