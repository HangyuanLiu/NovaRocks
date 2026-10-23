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

//! Borrowed geometry of Arrow 58.4's default StreamWriter operations.
//! The schema uses the shared numerical author. Batch spans follow writer.rs
//! write_array_data/get_byte_array_buffers/get_list_array_buffers exactly;
//! source stock is never substituted for selected copied bytes. Dynamic
//! library growth remains an opaque operation upper bound, not an allocator
//! invoice or a value/encoding capability.

use super::{
    IpcGeometryError, SchemaGeometry, SchemaRequestAccumulator, SchemaResourceSource,
    locked_field_location_layout, schema_backing_upper_bound, schema_writer_allocation_metrics,
    type_header_geometry,
};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelEvaluationControl, KernelFailure};
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, DictionaryArray, FixedSizeListArray, LargeBinaryArray,
    LargeListArray, LargeListViewArray, LargeStringArray, ListArray, ListViewArray, MapArray,
    RecordBatch, RunArray, StringArray, StringViewArray, StructArray, UnionArray,
};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{
    MAX_VALUE_TYPE_NODES, ValueSchemaCreation, ValueTypeError, ValueTypeVisit,
    validate_arrow_layout_structure_with_scratch_observed,
};
use std::alloc::Layout;

#[derive(Debug)]
pub enum IpcConstructionError<H> {
    Creator(H),
    Geometry(IpcGeometryError),
    Shape(ValueTypeError),
    Control(KernelFailure),
    HashMap(novarocks_type_contract::owned_resources::hashmap::HashMapResourceError),
}
impl<H> From<IpcGeometryError> for IpcConstructionError<H> {
    fn from(cause: IpcGeometryError) -> Self {
        Self::Geometry(cause)
    }
}
impl<H> From<ValueTypeError> for IpcConstructionError<H> {
    fn from(cause: ValueTypeError) -> Self {
        Self::Shape(cause)
    }
}
impl<H> From<KernelFailure> for IpcConstructionError<H> {
    fn from(cause: KernelFailure) -> Self {
        Self::Control(cause)
    }
}
impl<H> From<novarocks_type_contract::owned_resources::hashmap::HashMapResourceError>
    for IpcConstructionError<H>
{
    fn from(
        cause: novarocks_type_contract::owned_resources::hashmap::HashMapResourceError,
    ) -> Self {
        Self::HashMap(cause)
    }
}
impl<H: std::fmt::Display> std::fmt::Display for IpcConstructionError<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Creator(cause) => cause.fmt(f),
            Self::Geometry(cause) => cause.fmt(f),
            Self::Shape(cause) => write!(f, "Arrow IPC carrier structure: {cause:?}"),
            Self::Control(cause) => write!(f, "Arrow IPC construction control: {cause:?}"),
            Self::HashMap(cause) => {
                write!(f, "Arrow IPC dictionary tracker source model: {cause:?}")
            }
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for IpcConstructionError<H> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Creator(cause) => Some(cause),
            Self::Geometry(cause) => Some(cause),
            _ => None,
        }
    }
}
fn add(a: usize, b: usize) -> Result<usize, IpcGeometryError> {
    a.checked_add(b).ok_or(IpcGeometryError::Overflow)
}
fn mul(a: usize, b: usize) -> Result<usize, IpcGeometryError> {
    a.checked_mul(b).ok_or(IpcGeometryError::Overflow)
}
fn align(bytes: usize) -> Result<usize, IpcGeometryError> {
    Ok(add(bytes, 63)? & !63)
}
fn bits(rows: usize) -> Result<usize, IpcGeometryError> {
    Ok(add(rows, 7)? / 8)
}

/// Sealed same-operation schema geometry. This contains no source authority,
/// semantic binding, allocation measurement, budget or memory permission.
pub struct IpcStreamSchemaFacts<'a> {
    source: &'a Schema,
    fields: usize,
    dictionaries: usize,
    views: usize,
    types: usize,
    string_bytes: usize,
    wire: IpcStreamWireFacts,
    construction_upper: usize,
}
impl IpcStreamSchemaFacts<'_> {
    pub fn wire(&self) -> IpcStreamWireFacts {
        self.wire
    }
    pub fn field_occurrences(&self) -> usize {
        self.fields
    }
    pub fn type_occurrences(&self) -> usize {
        self.types
    }
    pub fn string_bytes(&self) -> usize {
        self.string_bytes
    }
    pub fn dictionary_occurrences(&self) -> usize {
        self.dictionaries
    }
    pub fn construction_upper_bound(&self) -> usize {
        self.construction_upper
    }
}

/// Size-only outputs of the SAME pinned writer traversal. Framing includes
/// alignment and continuation prefixes; the terminal marker is stream-owned.
#[derive(Clone, Copy, Debug, Default)]
pub struct IpcStreamWireFacts {
    wire_bytes: usize,
    metadata_bytes: usize,
    max_message_metadata: usize,
    dictionary_messages: usize,
    variadic_buffers: usize,
    body_bytes: usize,
    messages: usize,
    field_nodes: usize,
    buffers: usize,
    variadic_counts: usize,
}
impl IpcStreamWireFacts {
    pub fn wire_bytes_upper_bound(self) -> usize {
        self.wire_bytes
    }
    pub fn metadata_bytes_upper_bound(self) -> usize {
        self.metadata_bytes
    }
    pub fn body_bytes_upper_bound(self) -> usize {
        self.body_bytes
    }
    pub fn message_count_upper_bound(self) -> usize {
        self.messages
    }
    pub fn field_nodes_upper_bound(self) -> usize {
        self.field_nodes
    }
    pub fn buffers_upper_bound(self) -> usize {
        self.buffers
    }
    pub fn variadic_counts_upper_bound(self) -> usize {
        self.variadic_counts
    }
    pub fn max_message_metadata_upper_bound(self) -> usize {
        self.max_message_metadata
    }
    pub fn dictionary_messages_upper_bound(self) -> usize {
        self.dictionary_messages
    }
    pub fn variadic_buffers_upper_bound(self) -> usize {
        self.variadic_buffers
    }
    fn message(
        &mut self,
        metadata: usize,
        body: &Body,
        dictionary: bool,
    ) -> Result<(), IpcGeometryError> {
        self.wire_bytes = add(
            self.wire_bytes,
            add(align(add(metadata, 8)?)?, body.copied)?,
        )?;
        self.metadata_bytes = add(self.metadata_bytes, metadata)?;
        self.max_message_metadata = self.max_message_metadata.max(metadata);
        self.dictionary_messages = add(self.dictionary_messages, usize::from(dictionary))?;
        self.variadic_buffers = add(self.variadic_buffers, body.variadic_buffers)?;
        self.body_bytes = add(self.body_bytes, body.copied)?;
        self.messages = add(self.messages, 1)?;
        self.field_nodes = add(self.field_nodes, body.nodes)?;
        self.buffers = add(self.buffers, body.buffers)?;
        self.variadic_counts = add(self.variadic_counts, body.variadic_counts)?;
        Ok(())
    }
}
#[derive(Default)]
struct Counts {
    fields: usize,
    types: usize,
    strings: usize,
    string_bytes: usize,
    children: usize,
    child_requests: usize,
    unions: usize,
    union_requests: usize,
    dictionaries: usize,
    views: usize,
}
impl Counts {
    fn field(&mut self, field: &Field) -> Result<(), IpcGeometryError> {
        // This consumer is the typed execution stream. Provider/source schemas
        // retain their separate original annotation/provenance codec.
        if field.metadata().capacity() != 0 {
            return Err(IpcGeometryError::SourceModel);
        }
        self.fields = add(self.fields, 1)?;
        self.strings = add(self.strings, 1)?;
        self.string_bytes = add(self.string_bytes, field.name().len())?;
        if matches!(field.data_type(), DataType::Dictionary(..)) {
            self.dictionaries = add(self.dictionaries, 1)?;
        }
        Ok(())
    }
    fn ty(&mut self, ty: &DataType) -> Result<(), IpcGeometryError> {
        self.types = add(self.types, 1)?;
        if matches!(ty, DataType::Utf8View | DataType::BinaryView) {
            self.views = add(self.views, 1)?;
        }
        let arity = type_header_geometry(ty);
        self.children = add(self.children, arity.children)?;
        self.child_requests = add(self.child_requests, usize::from(arity.children != 0))?;
        self.unions = add(self.unions, arity.union_ids)?;
        self.union_requests = add(self.union_requests, usize::from(arity.union_ids != 0))?;
        if let Some(bytes) = arity.timezone_bytes {
            self.strings = add(self.strings, 1)?;
            self.string_bytes = add(self.string_bytes, bytes)?;
        }
        Ok(())
    }
}

pub fn observe_stream_writer_schema_construction<'a, H>(
    schema: &'a Schema,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<IpcStreamSchemaFacts<'a>, IpcConstructionError<H>> {
    control.checkpoint(0)?;
    if schema.metadata().capacity() != 0 {
        return Err(IpcGeometryError::SourceModel.into());
    }
    let mut work = EvaluationCheckpoints::new(control);
    let mut counts = Counts::default();
    for field in schema.fields() {
        work.step()?;
        counts.field(field)?;
        // Stack initialization is bracketed by control, not billed as a fake
        // completed field visit loop. The sole type-contract grammar owns the
        // validated carrier walk; this model does not interpret annotations.
        work.flush()?;
        let mut scratch = [None; MAX_VALUE_TYPE_NODES];
        work.flush()?;
        validate_arrow_layout_structure_with_scratch_observed::<IpcConstructionError<H>>(
            field.data_type(),
            &mut scratch,
            |visit| {
                work.step()?;
                match visit {
                    ValueTypeVisit::Field(field) => counts.field(field)?,
                    ValueTypeVisit::TypeNode(ty) => counts.ty(ty)?,
                    _ => {}
                }
                Ok(())
            },
        )?;
    }
    let backing = schema_backing_upper_bound(
        counts.fields,
        counts.types,
        0,
        counts.strings,
        counts.string_bytes,
    )?;
    let metrics = schema_writer_allocation_metrics::<IpcConstructionError<H>>(
        SchemaGeometry {
            backing,
            tables: add(add(counts.fields, counts.types)?, 2)?,
            metadata_entries: 0,
            string_bytes: counts.string_bytes,
            field_occurrences: counts.fields,
            type_occurrences: counts.types,
            metadata_nonempty_fields: 0,
            child_offset_items: counts.children,
            child_offset_requests: counts.child_requests,
            union_id_items: counts.unions,
            union_id_requests: counts.union_requests,
        },
        SchemaResourceSource {
            // Preserve this Arrow library owner's existing conservative model.
            // Codec-owned source walkers supply their captured node count instead.
            walker_node_bound: if counts.types == counts.fields {
                1
            } else {
                MAX_VALUE_TYPE_NODES
            },
            // Actual copied name/parameter byte operations are known here.
            // This is a work bound; it is never an original source invoice.
            source_retained_bytes: counts.string_bytes,
            scratch_work: 0,
        },
        locked_field_location_layout(),
        &mut || work.step().map_err(Into::into),
    )?;
    let mut dynamic = SchemaRequestAccumulator::default();
    // StreamWriter uses FlatBufferBuilder::new, unlike the codec emitter's
    // explicitly reserved builder. Its grow_downwards doubles from one byte;
    // include the complete prospective growth history independently.
    dynamic.geometric::<u8, IpcConstructionError<H>>(backing, 1, 1, &mut || {
        work.step().map_err(Into::into)
    })?;
    dynamic.geometric::<i64, IpcConstructionError<H>>(counts.dictionaries, 0, 1, &mut || {
        work.step().map_err(Into::into)
    })?;
    work.flush()?;
    creation
        .observe_opaque_upper_bound(add(metrics.request_bytes, dynamic.bytes)?)
        .map_err(IpcConstructionError::Creator)?;
    let mut wire = IpcStreamWireFacts::default();
    wire.message(backing, &Body::default(), false)?;
    Ok(IpcStreamSchemaFacts {
        source: schema,
        fields: counts.fields,
        dictionaries: counts.dictionaries,
        views: counts.views,
        types: counts.types,
        string_bytes: counts.string_bytes,
        wire,
        construction_upper: add(metrics.request_bytes, dynamic.bytes)?,
    })
}

#[derive(Default)]
struct Body {
    variadic_sequence: Vec<i64>,
    variadic_buffers: usize,
    nodes: usize,
    buffers: usize,
    copied: usize,
    scratch: usize,
    variadic_counts: usize,
}
impl Body {
    fn buffer(&mut self, bytes: usize) -> Result<(), IpcGeometryError> {
        self.buffers = add(self.buffers, 1)?;
        self.copied = add(self.copied, align(bytes)?)?;
        Ok(())
    }
    fn bitmap(&mut self, rows: usize) -> Result<(), IpcGeometryError> {
        let bytes = bits(rows)?;
        self.buffer(bytes)?;
        // The original all-valid or misaligned BooleanBuffer::sliced branch
        // can construct this MutableBuffer; its locked capacity rounds to 64.
        self.scratch = add(self.scratch, align(bytes)?)?;
        Ok(())
    }
    fn offsets(&mut self, rows: usize, width: usize, start: usize) -> Result<(), IpcGeometryError> {
        let bytes = mul(add(rows, 1)?, width)?;
        self.buffer(bytes)?;
        if rows == 0 || start != 0 {
            self.scratch = add(self.scratch, align(bytes)?)?;
        }
        Ok(())
    }
}
fn offsets_span<O: TryInto<usize> + Copy>(
    offsets: &[O],
    start: usize,
    rows: usize,
) -> Result<(usize, usize), IpcGeometryError> {
    let end = add(start, rows)?;
    let first = offsets
        .get(start)
        .copied()
        .ok_or(IpcGeometryError::SourceModel)?
        .try_into()
        .map_err(|_| IpcGeometryError::SourceModel)?;
    let last = offsets
        .get(end)
        .copied()
        .ok_or(IpcGeometryError::SourceModel)?
        .try_into()
        .map_err(|_| IpcGeometryError::SourceModel)?;
    if last < first {
        return Err(IpcGeometryError::SourceModel);
    }
    Ok((first, last - first))
}
fn concrete<T: 'static>(array: &dyn Array) -> Result<&T, IpcGeometryError> {
    array
        .as_any()
        .downcast_ref()
        .ok_or(IpcGeometryError::SourceModel)
}

fn window<H>(
    array: &dyn Array,
    start: usize,
    rows: usize,
    body: &mut Body,
    work: &mut EvaluationCheckpoints<'_>,
    depth: usize,
) -> Result<(), IpcConstructionError<H>> {
    work.step()?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH || add(start, rows)? > array.len() {
        return Err(IpcGeometryError::SourceModel.into());
    }
    body.nodes = add(body.nodes, 1)?;
    if !matches!(
        array.data_type(),
        DataType::Null | DataType::Union(..) | DataType::RunEndEncoded(..)
    ) {
        body.bitmap(rows)?;
    }
    let ty = array.data_type();
    if let Some(width) = ty.primitive_width() {
        body.buffer(mul(rows, width)?)?;
        return Ok(());
    }
    match ty {
        DataType::Null => {}
        DataType::Boolean => body.bitmap(rows)?,
        DataType::FixedSizeBinary(width) => {
            body.buffer(mul(
                rows,
                usize::try_from(*width).map_err(|_| IpcGeometryError::SourceModel)?,
            )?)?;
        }
        DataType::Utf8 | DataType::Binary => {
            let offsets = if matches!(ty, DataType::Utf8) {
                concrete::<StringArray>(array)?.value_offsets()
            } else {
                concrete::<BinaryArray>(array)?.value_offsets()
            };
            let (offset, bytes) = offsets_span(offsets, start, rows)?;
            body.offsets(rows, 4, offset)?;
            body.buffer(if rows == 0 { 0 } else { bytes })?;
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let offsets = if matches!(ty, DataType::LargeUtf8) {
                concrete::<LargeStringArray>(array)?.value_offsets()
            } else {
                concrete::<LargeBinaryArray>(array)?.value_offsets()
            };
            let (offset, bytes) = offsets_span(offsets, start, rows)?;
            body.offsets(rows, 8, offset)?;
            body.buffer(if rows == 0 { 0 } else { bytes })?;
        }
        DataType::Utf8View | DataType::BinaryView => {
            let buffers = if matches!(ty, DataType::Utf8View) {
                concrete::<StringViewArray>(array)?.data_buffers()
            } else {
                concrete::<BinaryViewArray>(array)?.data_buffers()
            };
            // Pinned writer truncates the u128 views buffer, but deliberately
            // writes every existing external data buffer in its visible extent.
            body.buffer(mul(rows, std::mem::size_of::<u128>())?)?;
            for buffer in buffers {
                work.step()?;
                body.buffer(buffer.len())?;
            }
            if body.variadic_sequence.len() == body.variadic_sequence.capacity() {
                return Err(IpcGeometryError::SourceModel.into());
            }
            body.variadic_sequence
                .push(i64::try_from(buffers.len()).map_err(|_| IpcGeometryError::Overflow)?);
            body.variadic_counts = add(body.variadic_counts, 1)?;
            body.variadic_buffers = add(body.variadic_buffers, buffers.len())?;
        }
        DataType::ListView(_) | DataType::LargeListView(_) => {
            let (width, child) = if matches!(ty, DataType::ListView(_)) {
                (4, concrete::<ListViewArray>(array)?.values())
            } else {
                (8, concrete::<LargeListViewArray>(array)?.values())
            };
            body.buffer(mul(rows, width)?)?;
            body.buffer(mul(rows, width)?)?;
            // Original get_list_view_array_buffers keeps its full child for
            // nonempty views; only empty input slices the child to zero rows.
            window::<H>(
                child.as_ref(),
                0,
                if rows == 0 { 0 } else { child.len() },
                body,
                work,
                depth + 1,
            )?;
        }
        DataType::Union(fields, mode) => {
            let union = concrete::<UnionArray>(array)?;
            // write_array_data's generic Union branch copies the original
            // visible buffers; it does not optimize child dictionaries/arrays.
            body.buffer(union.type_ids().inner().len())?;
            if *mode == arrow_schema::UnionMode::Dense {
                body.buffer(
                    union
                        .offsets()
                        .ok_or(IpcGeometryError::SourceModel)?
                        .inner()
                        .len(),
                )?;
            }
            for (id, _) in fields.iter() {
                work.step()?;
                let child = union.child(id);
                window::<H>(child.as_ref(), 0, child.len(), body, work, depth + 1)?;
            }
        }
        DataType::RunEndEncoded(ends, _) => {
            macro_rules! runs {
                ($key:ty, $width:expr) => {{
                    let run = concrete::<RunArray<$key>>(array)?;
                    work.flush()?;
                    let (first, count) = if rows == 0 {
                        (0, 0)
                    } else {
                        let first = run.get_physical_index(start);
                        let last = run.get_physical_index(add(start, rows)? - 1);
                        (
                            first,
                            add(
                                last.checked_sub(first)
                                    .ok_or(IpcGeometryError::SourceModel)?,
                                1,
                            )?,
                        )
                    };
                    work.flush()?;
                    // Original unslice_run_array authors a primitive run-end
                    // child, then the physically selected values slice. It
                    // rebases end positions in a BufferBuilder on the sliced
                    // path; it never expands logical rows into plain values.
                    body.nodes = add(body.nodes, 1)?;
                    body.bitmap(count)?;
                    body.buffer(mul(count, $width)?)?;
                    if start != 0 || run.offset() != 0 || rows != run.run_ends().max_value() {
                        body.scratch = add(body.scratch, align(mul(count, $width)?)?)?;
                    }
                    window::<H>(run.values().as_ref(), first, count, body, work, depth + 1)?;
                }};
            }
            match ends.data_type() {
                DataType::Int16 => runs!(Int16Type, 2),
                DataType::Int32 => runs!(Int32Type, 4),
                DataType::Int64 => runs!(Int64Type, 8),
                _ => return Err(IpcGeometryError::SourceModel.into()),
            }
        }
        DataType::List(_) => {
            let list = concrete::<ListArray>(array)?;
            let (offset, count) = offsets_span(list.value_offsets(), start, rows)?;
            body.offsets(rows, 4, offset)?;
            window::<H>(
                list.values().as_ref(),
                if rows == 0 { 0 } else { offset },
                if rows == 0 { 0 } else { count },
                body,
                work,
                depth + 1,
            )?;
        }
        DataType::LargeList(_) => {
            let list = concrete::<LargeListArray>(array)?;
            let (offset, count) = offsets_span(list.value_offsets(), start, rows)?;
            body.offsets(rows, 8, offset)?;
            window::<H>(
                list.values().as_ref(),
                if rows == 0 { 0 } else { offset },
                if rows == 0 { 0 } else { count },
                body,
                work,
                depth + 1,
            )?;
        }
        DataType::Map(_, _) => {
            let map = concrete::<MapArray>(array)?;
            let (offset, count) = offsets_span(map.value_offsets(), start, rows)?;
            body.offsets(rows, 4, offset)?;
            window::<H>(
                map.entries(),
                if rows == 0 { 0 } else { offset },
                if rows == 0 { 0 } else { count },
                body,
                work,
                depth + 1,
            )?;
        }
        DataType::FixedSizeList(_, width) => {
            let list = concrete::<FixedSizeListArray>(array)?;
            let width = usize::try_from(*width).map_err(|_| IpcGeometryError::SourceModel)?;
            window::<H>(
                list.values().as_ref(),
                mul(start, width)?,
                mul(rows, width)?,
                body,
                work,
                depth + 1,
            )?;
        }
        DataType::Struct(_) => {
            for child in concrete::<StructArray>(array)?.columns() {
                window::<H>(child.as_ref(), start, rows, body, work, depth + 1)?;
            }
        }
        DataType::Dictionary(keys, values)
            if keys.as_ref() == &DataType::Int32
                && matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let dict = concrete::<DictionaryArray<Int32Type>>(array)?;
            body.buffer(mul(rows, 4)?)?;
            // RecordBatch nodes contain only the physical keys. Values have
            // their separate DictionaryBatch message, observed below.
            let _ = dict;
        }
        _ => return Err(IpcGeometryError::SourceModel.into()),
    }
    Ok(())
}

fn message_upper<H>(
    body: &Body,
    dictionary: bool,
    requests: &mut SchemaRequestAccumulator,
    wire: &mut IpcStreamWireFacts,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), IpcConstructionError<H>> {
    // The generated FieldNode and Buffer wire structs each contain two i64
    // words. Their private Vec growth is an opaque bound, never a Layout
    // invoice. Message/RecordBatch slots use the SAME schema table envelope;
    // a DictionaryBatch adds one table in the existing numerical author.
    let tables = if dictionary { 3 } else { 2 };
    let headers = schema_backing_upper_bound(0, usize::from(dictionary), 0, 0, 0)?;
    let descriptors = mul(
        add(body.nodes, body.buffers)?,
        std::mem::size_of::<[i64; 2]>(),
    )?;
    // Two length-prefixed vectors with alignment padding are additional to
    // the schema envelope's existing vector/root padding.
    let mut backing = add(add(headers, descriptors)?, mul(2, 4 + 7)?)?;
    if body.variadic_counts != 0 {
        backing = add(backing, add(mul(body.variadic_counts, 8)?, 4 + 7)?)?;
    }
    wire.message(backing, body, dictionary)?;
    let mut observe = || work.step().map_err(IpcConstructionError::<H>::from);
    requests.geometric::<[i64; 2], IpcConstructionError<H>>(body.nodes, 0, 1, &mut observe)?;
    requests.geometric::<[i64; 2], IpcConstructionError<H>>(body.buffers, 0, 1, &mut observe)?;
    requests.geometric::<i64, IpcConstructionError<H>>(body.variadic_counts, 4, 1, &mut observe)?;
    // Vec<u8>'s locked minimum nonzero growth is eight; Arrow MutableBuffer
    // capacity is separately the rounded scratch extent from the real branch.
    requests.geometric::<u8, IpcConstructionError<H>>(body.copied, 8, 1, &mut observe)?;
    requests.exact::<u8, IpcConstructionError<H>>(body.scratch, 1, &mut observe)?;
    requests.geometric::<u8, IpcConstructionError<H>>(backing, 8, 1, &mut observe)?;
    requests.exact::<u8, IpcConstructionError<H>>(backing, 1, &mut observe)?;
    let location = locked_field_location_layout();
    for capacity in [4, 8] {
        let extent = Layout::from_size_align(mul(location.size(), capacity)?, location.align())
            .map_err(|_| IpcGeometryError::Overflow)?;
        requests.layout(extent, 1, &mut observe)?;
    }
    requests.geometric::<u32, IpcConstructionError<H>>(tables, 4, 1, &mut observe)?;
    Ok(())
}

fn dictionary_messages<H>(
    array: &dyn Array,
    requests: &mut SchemaRequestAccumulator,
    wire: &mut IpcStreamWireFacts,
    work: &mut EvaluationCheckpoints<'_>,
    depth: usize,
) -> Result<(), IpcConstructionError<H>> {
    work.step()?;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(IpcGeometryError::SourceModel.into());
    }
    match array.data_type() {
        DataType::Dictionary(keys, values)
            if keys.as_ref() == &DataType::Int32
                && matches!(values.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let values = concrete::<DictionaryArray<Int32Type>>(array)?.values();
            // writer.rs::encode_dictionaries and DictionaryTracker::insert_column
            // EACH invoke column.to_data(). The cloned values then go through
            // make_array before recursive dictionary discovery. These are three
            // distinct operations, not reuse of one metadata envelope.
            for source in [array, array, values.as_ref()] {
                let metadata = crate::array_backing_geometry::source_metadata_bytes(source, work)?;
                requests.bytes = add(requests.bytes, metadata)?;
            }
            let mut body = Body::default();
            window::<H>(values.as_ref(), 0, values.len(), &mut body, work, depth + 1)?;
            message_upper(&body, true, requests, wire, work)?;
        }
        DataType::List(_) => dictionary_messages::<H>(
            concrete::<ListArray>(array)?.values().as_ref(),
            requests,
            wire,
            work,
            depth + 1,
        )?,
        DataType::LargeList(_) => dictionary_messages::<H>(
            concrete::<LargeListArray>(array)?.values().as_ref(),
            requests,
            wire,
            work,
            depth + 1,
        )?,
        DataType::RunEndEncoded(ends, _) => {
            macro_rules! runs {
                ($key:ty) => {{
                    let run = concrete::<RunArray<$key>>(array)?;
                    // Original dictionary discovery reconstructs the values
                    // from column.to_data before descending into its field.
                    let metadata =
                        crate::array_backing_geometry::source_metadata_bytes(array, work)?;
                    requests.bytes = add(requests.bytes, metadata)?;
                    dictionary_messages::<H>(
                        run.values().as_ref(),
                        requests,
                        wire,
                        work,
                        depth + 1,
                    )?;
                }};
            }
            match ends.data_type() {
                DataType::Int16 => runs!(Int16Type),
                DataType::Int32 => runs!(Int32Type),
                DataType::Int64 => runs!(Int64Type),
                _ => return Err(IpcGeometryError::SourceModel.into()),
            }
        }
        DataType::ListView(_) => dictionary_messages::<H>(
            concrete::<ListViewArray>(array)?.values().as_ref(),
            requests,
            wire,
            work,
            depth + 1,
        )?,
        DataType::LargeListView(_) => dictionary_messages::<H>(
            concrete::<LargeListViewArray>(array)?.values().as_ref(),
            requests,
            wire,
            work,
            depth + 1,
        )?,
        DataType::Union(fields, _) => {
            let union = concrete::<UnionArray>(array)?;
            for (id, _) in fields.iter() {
                work.step()?;
                dictionary_messages::<H>(
                    union.child(id).as_ref(),
                    requests,
                    wire,
                    work,
                    depth + 1,
                )?;
            }
        }
        DataType::FixedSizeList(_, _) => dictionary_messages::<H>(
            concrete::<FixedSizeListArray>(array)?.values().as_ref(),
            requests,
            wire,
            work,
            depth + 1,
        )?,
        DataType::Map(_, _) => dictionary_messages::<H>(
            concrete::<MapArray>(array)?.entries(),
            requests,
            wire,
            work,
            depth + 1,
        )?,
        DataType::Struct(_) => {
            for child in concrete::<StructArray>(array)?.columns() {
                dictionary_messages::<H>(child.as_ref(), requests, wire, work, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Observe one actual default StreamWriter::write before invoking the locked
/// library. The stream must be a fresh writer, uncompressed MetadataV5 with
/// alignment 64, using the schema sealed by the preceding schema observer.
/// Repeated dictionary IDs may avoid replacement; every actual dictionary
/// occurrence is a conservative potential message, without expanding support.
/// Existing Array payloads remain source-owned. Only copied selected extents,
/// temporary Arrow metadata and opaque private growth are observed here.
pub fn observe_stream_writer_batch_construction<'a, H>(
    batch: &'a RecordBatch,
    schema: &IpcStreamSchemaFacts<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<IpcStreamBatchFacts<'a>, IpcConstructionError<H>> {
    control.checkpoint(0)?;
    if !std::ptr::eq(schema.source, batch.schema_ref().as_ref()) {
        return Err(IpcGeometryError::SourceModel.into());
    }
    let mut work = EvaluationCheckpoints::new(control);
    let mut requests = SchemaRequestAccumulator::default();
    let mut wire = IpcStreamWireFacts::default();
    // encode() allocates flattened_fields() and an EncodedData vector of that
    // capacity; each EncodedData is its two public Vec<u8> members. This is
    // private-operation geometry, not an exact private struct Layout request.
    requests.exact::<&Field, IpcConstructionError<H>>(schema.fields, 1, &mut || {
        work.step().map_err(Into::into)
    })?;
    requests.exact::<Vec<u8>, IpcConstructionError<H>>(schema.fields, 2, &mut || {
        work.step().map_err(Into::into)
    })?;
    requests.exact::<i64, IpcConstructionError<H>>(schema.dictionaries, 1, &mut || {
        work.step().map_err(Into::into)
    })?;
    let tracker = novarocks_type_contract::owned_resources::hashmap::
        incremental_insert_allocation_upper_bound_observed::<i64, arrow_data::ArrayData, IpcConstructionError<H>>(
            schema.dictionaries, || work.step().map_err(Into::into),
        )?;
    requests.bytes = add(requests.bytes, tracker)?;
    creation
        .request_layout(Layout::array::<i64>(schema.views).map_err(|_| IpcGeometryError::Overflow)?)
        .map_err(IpcConstructionError::Creator)?;
    let mut body = Body {
        variadic_sequence: Vec::with_capacity(schema.views),
        ..Body::default()
    };
    let mut source_buffers = SourceBufferSlots::default();
    let mut to_data_metadata_upper = 0;
    for array in batch.columns() {
        // ONE original borrowed visitor supplies actual source buffer slots
        // and one to_data operation's metadata envelope. This contribution is
        // for record_batch_to_bytes ONLY; dictionary operations are separate.
        let metadata = crate::array_backing_geometry::observe_source_metadata(
            array.as_ref(),
            false,
            &mut work,
            &mut source_buffers,
        )?;
        to_data_metadata_upper = add(to_data_metadata_upper, metadata)?;
        requests.bytes = add(requests.bytes, metadata)?;
        window::<H>(array.as_ref(), 0, array.len(), &mut body, &mut work, 0)?;
        dictionary_messages::<H>(array.as_ref(), &mut requests, &mut wire, &mut work, 0)?;
    }
    message_upper::<H>(&body, false, &mut requests, &mut wire, &mut work)?;
    work.flush()?;
    creation
        .observe_opaque_upper_bound(requests.bytes)
        .map_err(IpcConstructionError::Creator)?;
    Ok(IpcStreamBatchFacts {
        source: batch,
        buffer_slots_upper: source_buffers.slots,
        to_data_metadata_upper,
        wire,
        construction_upper: add(
            requests.bytes,
            mul(schema.views, std::mem::size_of::<i64>())?,
        )?,
        variadic_sequence: body.variadic_sequence,
        _geometry_storage: creation.storage_lifetime(),
    })
}

/// Numeric loans from the SAME borrowed immutable source walk. No source
/// invoice, allocation measurement, semantic proof, storage owner or grant is
/// created here. The second operation below observes its own contribution.
pub struct IpcStreamBatchFacts<'a> {
    source: &'a RecordBatch,
    buffer_slots_upper: usize,
    to_data_metadata_upper: usize,
    wire: IpcStreamWireFacts,
    construction_upper: usize,
    variadic_sequence: Vec<i64>,
    _geometry_storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl IpcStreamBatchFacts<'_> {
    pub fn variadic_buffer_counts(&self) -> &[i64] {
        &self.variadic_sequence
    }
    pub fn wire(&self) -> IpcStreamWireFacts {
        self.wire
    }
    pub fn construction_upper_bound(&self) -> usize {
        self.construction_upper
    }
    pub fn buffer_slots_upper_bound(&self) -> usize {
        self.buffer_slots_upper
    }
}

#[derive(Default)]
struct SourceBufferSlots {
    slots: usize,
}
impl<'a> crate::array_backing_geometry::BorrowedSourceObservation<'a> for SourceBufferSlots {
    fn node(
        &mut self,
        node: crate::array_backing_geometry::BorrowedSourceNode<'a>,
        _work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        // Null contributes zero data slots and no invented validity. Other
        // actual nodes contribute their real buffers plus an existing mask.
        self.slots = self
            .slots
            .checked_add(node.data_buffers)
            .and_then(|slots| slots.checked_add(usize::from(node.array.nulls().is_some())))
            .ok_or(KernelFailure::ResourceExhausted)?;
        Ok(())
    }
}

/// Observe the independent record_batch_bytes/logical_bytes operation before
/// its allocating to_data + empty-then-insert HashSet<usize> body. Its source
/// loan must be the same immutable RecordBatch. Existing buffer deduplication
/// and returned byte counts remain wholly owned by the original caller.
pub fn observe_stream_logical_size_construction<H>(
    batch: &RecordBatch,
    facts: &IpcStreamBatchFacts<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), IpcConstructionError<H>> {
    control.checkpoint(0)?;
    if !std::ptr::eq(facts.source, batch) {
        return Err(IpcGeometryError::SourceModel.into());
    }
    let mut work = EvaluationCheckpoints::new(control);
    // HashSet's exact locked table delegate is HashMap<usize, ()>. Its
    // occurrence bound comes from real visited source buffers, including
    // duplicate identities; it never claims the source retained capacity.
    let table = novarocks_type_contract::owned_resources::hashmap::
        incremental_insert_allocation_upper_bound_observed::<usize, (), IpcConstructionError<H>>(
            facts.buffer_slots_upper, || work.step().map_err(Into::into),
        )?;
    let upper = add(table, facts.to_data_metadata_upper)?;
    work.flush()?;
    creation
        .observe_opaque_upper_bound(upper)
        .map_err(IpcConstructionError::Creator)?;
    Ok(())
}

#[cfg(test)]
mod retained_carrier_tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{UnionFields, UnionMode};
    use novarocks_type_contract::ValueTypeStorageLifetime;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Control(AtomicBool);
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            if self.0.load(Ordering::SeqCst) {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("writer does not wait");
        }
    }
    fn sources() -> Vec<ArrayRef> {
        let fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("text", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let dense: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields.clone(),
                ScalarBuffer::from(vec![4_i8, 9, 4]),
                Some(ScalarBuffer::from(vec![0_i32, 0, 1])),
                vec![
                    Arc::new(Int32Array::from(vec![7, 8])),
                    Arc::new(StringArray::from(vec![Some("text")])),
                ],
            )
            .unwrap(),
        );
        let sparse: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![4_i8, 9, 4]),
                None,
                vec![
                    Arc::new(Int32Array::from(vec![7, 8, 9])),
                    Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
                ],
            )
            .unwrap(),
        );
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![
            "a genuinely external view value",
            "short",
        ]));
        let list_view: ArrayRef = Arc::new(
            ListViewArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                ScalarBuffer::from(vec![2_i32, 0, 1]),
                ScalarBuffer::from(vec![2_i32, 1, 2]),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                None,
            )
            .unwrap(),
        );
        let large_view: ArrayRef = Arc::new(
            LargeListViewArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                ScalarBuffer::from(vec![2_i64, 0, 1]),
                ScalarBuffer::from(vec![2_i64, 1, 2]),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                None,
            )
            .unwrap(),
        );
        let run: ArrayRef = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![2, 5]),
                &StringArray::from(vec!["first", "second"]),
            )
            .unwrap(),
        );
        vec![dense, sparse, view, list_view, large_view, run]
    }
    #[test]
    fn original_stream_writer_model_preserves_native_union_view_and_run_carriers() {
        for array in sources() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
            let control = Control(AtomicBool::new(false));
            let requests = AtomicUsize::new(0);
            let mut exact = |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(())
            };
            let mut opaque = |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(())
            };
            let mut creation =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let facts =
                observe_stream_writer_schema_construction(schema.as_ref(), &mut creation, &control)
                    .unwrap();
            let batch_facts =
                observe_stream_writer_batch_construction(&batch, &facts, &mut creation, &control)
                    .unwrap();
            assert!(requests.load(Ordering::SeqCst) > 0);
            let mut bytes = Vec::new();
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            let wire_upper = facts.wire().wire_bytes_upper_bound()
                + batch_facts.wire().wire_bytes_upper_bound()
                + 8;
            assert!(
                bytes.len() <= wire_upper,
                "same writer bounds its actual {:?} stream: {} <= {wire_upper}",
                batch.columns()[0].data_type(),
                bytes.len()
            );
            if let Some(view) = batch.columns()[0]
                .as_any()
                .downcast_ref::<StringViewArray>()
            {
                assert_eq!(
                    batch_facts.variadic_buffer_counts(),
                    &[view.data_buffers().len() as i64]
                );
            } else {
                assert!(batch_facts.variadic_buffer_counts().is_empty());
            }
            let decoded =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
            assert_eq!(decoded.num_rows(), batch.num_rows());
            assert_eq!(
                decoded.columns()[0].data_type(),
                batch.columns()[0].data_type()
            );
            assert_eq!(decoded.columns()[0].to_data(), batch.columns()[0].to_data());
        }
    }
    #[test]
    fn original_stream_writer_batch_creator_refusal_wins_before_pending_control() {
        let array = sources().remove(0);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let control = Control(AtomicBool::new(false));
        let mut exact = |_| Ok::<_, u32>(());
        let mut opaque = |_| Ok::<_, u32>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let facts =
            observe_stream_writer_schema_construction(schema.as_ref(), &mut creation, &control)
                .unwrap();
        let calls = AtomicUsize::new(0);
        let mut fail = |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            control.0.store(true, Ordering::SeqCst);
            Err::<(), _>(71_u32)
        };
        let mut failing =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut fail);
        assert!(matches!(
            observe_stream_writer_batch_construction(&batch, &facts, &mut failing, &control),
            Err(IpcConstructionError::Creator(71))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
