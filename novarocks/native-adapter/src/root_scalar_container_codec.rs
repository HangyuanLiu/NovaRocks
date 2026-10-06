// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! ScalarValueV1 container records (List, Map, Struct) from one Native row.
//!
//! The whole typed record -- every nested child with its presence, length and
//! count bytes -- is written into one record buffer whose capacity the caller
//! prepaid ([`NativeScalarContainerEncoder::scratch_capacity_bytes`]). The
//! 64 KiB payload ceiling is checked before each append, so an oversized value
//! is refused before its next byte is written and the buffer never grows. The
//! walk visits at most one node per payload byte, so its work is bounded by
//! the same ceiling. Turns then copy the finished record into root segments.

use arrow::array::{Array, ListArray, MapArray, StructArray};
use novarocks_execution::exec::chunk::{Chunk, ChunkFieldSchema};
use novarocks_result_contract::{
    BorrowedScalarLeaf, RootProfileV1, SCALAR_RECORD_MAX_BYTES, ScalarField, ScalarLeafCursor,
    ScalarRecordWriter, ScalarSchema, ScalarValueType,
};
use novarocks_result_render::{RenderTurn, RenderTurnStatus};
use novarocks_type_contract::result_scalar_type::scalar_field_matches_storage;

use crate::root_scalar_leaf_codec::{NativeScalarLeafError, borrow_leaf_at, expected_logical};

pub struct NativeScalarContainerEncoder {
    record: Vec<u8>,
    position: usize,
}

impl NativeScalarContainerEncoder {
    /// The record buffer plus the cursor itself, prepaid by the caller.
    pub const fn scratch_capacity_bytes() -> usize {
        SCALAR_RECORD_MAX_BYTES + size_of::<Self>()
    }

    /// Encode the single row of a one-column container input. The caller has
    /// already checked cumulative row admission and keeps the input permit
    /// until this encoder exits.
    pub fn try_encode(
        chunk: &Chunk,
        schema: &ScalarSchema,
        prepaid_scratch_capacity: usize,
    ) -> Result<Self, NativeScalarLeafError> {
        if !is_container(schema.field()) {
            return Err(NativeScalarLeafError::Type);
        }
        let batch = &chunk.batch;
        if batch.num_columns() != 1
            || batch.schema_ref().fields().len() != 1
            || chunk.chunk_schema().slots().len() != 1
        {
            return Err(NativeScalarLeafError::Shape);
        }
        if batch.num_rows() == 0 {
            return Err(NativeScalarLeafError::EmptyInput);
        }
        if batch.num_rows() != 1 {
            return Err(NativeScalarLeafError::Shape);
        }
        Self::validate_schema(chunk, schema)?;
        if Self::scratch_capacity_bytes() > prepaid_scratch_capacity {
            return Err(NativeScalarLeafError::ScratchLimit);
        }
        let mut record = Vec::with_capacity(SCALAR_RECORD_MAX_BYTES);
        let column = batch.column(0).as_ref();
        if column.is_null(0) {
            let cursor = ScalarLeafCursor::try_new(schema, BorrowedScalarLeaf::Null)?;
            record.resize(cursor.encoded_len(), 0);
            cursor.copy_range(0, &mut record)?;
        } else {
            let mut writer = ScalarRecordWriter::new(schema, &mut record)?;
            encode_body(&mut writer, schema.field(), column, 0, 1)?;
            writer.finish(schema)?;
        }
        Ok(Self {
            record,
            position: 0,
        })
    }

    /// Schema-only validation of an empty one-column container input. Emits
    /// nothing; absence is decided by the root owner at its sealed End.
    pub fn validate_empty(
        chunk: &Chunk,
        schema: &ScalarSchema,
    ) -> Result<(), NativeScalarLeafError> {
        if !is_container(schema.field()) {
            return Err(NativeScalarLeafError::Type);
        }
        let batch = &chunk.batch;
        if batch.num_columns() != 1
            || batch.schema_ref().fields().len() != 1
            || chunk.chunk_schema().slots().len() != 1
            || batch.num_rows() != 0
        {
            return Err(NativeScalarLeafError::Shape);
        }
        Self::validate_schema(chunk, schema)
    }

    fn validate_schema(chunk: &Chunk, schema: &ScalarSchema) -> Result<(), NativeScalarLeafError> {
        let slot = &chunk.chunk_schema().slots()[0];
        if schema.source_slot() != Some(slot.slot_id().as_u32()) {
            return Err(NativeScalarLeafError::Slot);
        }
        if !logical_matches(schema.field(), slot.field_schema()) {
            return Err(NativeScalarLeafError::LogicalMetadata);
        }
        let batch_field = &chunk.batch.schema_ref().fields()[0];
        for field in [slot.field(), batch_field.as_ref()] {
            if !scalar_field_matches_storage(schema.field(), field.data_type(), field.is_nullable())
            {
                return Err(NativeScalarLeafError::Type);
            }
        }
        if !scalar_field_matches_storage(
            schema.field(),
            chunk.batch.column(0).data_type(),
            schema.field().nullable,
        ) {
            return Err(NativeScalarLeafError::Type);
        }
        Ok(())
    }

    pub fn encoded_len(&self) -> usize {
        self.record.len()
    }

    pub fn step(&mut self, output: &mut [u8]) -> RenderTurn {
        let count = output
            .len()
            .min(RootProfileV1::EMIT_BYTES_PER_TURN)
            .min(self.record.len() - self.position);
        output[..count].copy_from_slice(&self.record[self.position..self.position + count]);
        self.position += count;
        let complete = self.position == self.record.len();
        RenderTurn {
            emitted_bytes: count,
            examined_bytes: count,
            visited_cells: usize::from(complete),
            completed_rows: u64::from(complete),
            status: if complete {
                RenderTurnStatus::InputComplete
            } else if count == output.len() {
                RenderTurnStatus::NeedsOutput
            } else {
                RenderTurnStatus::Yielded
            },
        }
    }
}

pub(crate) fn is_container(field: &ScalarField) -> bool {
    matches!(
        field.value_type,
        ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
    )
}

/// Nested logical facts must match exactly: container nodes carry none, and
/// each leaf carries exactly the logical type its frozen scalar type implies.
fn logical_matches(expected: &ScalarField, schema: &ChunkFieldSchema) -> bool {
    match &expected.value_type {
        ScalarValueType::List(child) => {
            schema.logical_type().is_none()
                && schema.children().len() == 1
                && logical_matches(child, &schema.children()[0])
        }
        ScalarValueType::Map { key, value } => {
            schema.logical_type().is_none()
                && schema.children().len() == 2
                && logical_matches(key, &schema.children()[0])
                && logical_matches(value, &schema.children()[1])
        }
        ScalarValueType::Struct(fields) => {
            schema.logical_type().is_none()
                && schema.children().len() == fields.len()
                && fields
                    .iter()
                    .zip(schema.children())
                    .all(|(field, child)| logical_matches(&field.field, child))
        }
        leaf => schema.children().is_empty() && schema.logical_type() == expected_logical(leaf),
    }
}

fn exact<T: Array + 'static>(array: &dyn Array) -> Result<&T, NativeScalarLeafError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(NativeScalarLeafError::Type)
}

fn encode_node(
    writer: &mut ScalarRecordWriter<'_>,
    field: &ScalarField,
    array: &dyn Array,
    index: usize,
    depth: usize,
) -> Result<(), NativeScalarLeafError> {
    if !is_container(field) {
        // A leaf's absence is decided by its selected value, not only the
        // outer validity bit, so both facts come from one selection.
        let leaf = borrow_leaf_at(field, array, index)?;
        let null = matches!(leaf, BorrowedScalarLeaf::Null);
        writer.presence(field, null)?;
        if !null {
            writer.leaf(field, leaf)?;
        }
        return Ok(());
    }
    let null = array.is_null(index);
    writer.presence(field, null)?;
    if null {
        return Ok(());
    }
    encode_body(writer, field, array, index, depth)
}

fn encode_body(
    writer: &mut ScalarRecordWriter<'_>,
    field: &ScalarField,
    array: &dyn Array,
    index: usize,
    depth: usize,
) -> Result<(), NativeScalarLeafError> {
    if depth > RootProfileV1::MAX_DEPTH {
        return Err(NativeScalarLeafError::Type);
    }
    match &field.value_type {
        ScalarValueType::List(element) => {
            let list = exact::<ListArray>(array)?;
            let offsets = list.value_offsets();
            let (start, end) = (offsets[index] as usize, offsets[index + 1] as usize);
            writer.count(end - start)?;
            let values = list.values().as_ref();
            for child in start..end {
                encode_node(writer, element, values, child, depth + 1)?;
            }
            Ok(())
        }
        ScalarValueType::Map { key, value } => {
            let map = exact::<MapArray>(array)?;
            let offsets = map.value_offsets();
            let (start, end) = (offsets[index] as usize, offsets[index + 1] as usize);
            writer.count(end - start)?;
            let keys = map.keys().as_ref();
            let values = map.values().as_ref();
            for entry in start..end {
                encode_node(writer, key, keys, entry, depth + 1)?;
                encode_node(writer, value, values, entry, depth + 1)?;
            }
            Ok(())
        }
        ScalarValueType::Struct(fields) => {
            let structure = exact::<StructArray>(array)?;
            if structure.num_columns() != fields.len() {
                return Err(NativeScalarLeafError::Type);
            }
            for (named, child) in fields.iter().zip(structure.columns()) {
                encode_node(writer, &named.field, child.as_ref(), index, depth + 1)?;
            }
            Ok(())
        }
        // Leaves are always reached through encode_node.
        _ => Err(NativeScalarLeafError::Type),
    }
}
