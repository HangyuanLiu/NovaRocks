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

//! Final StatisticsArtifactV1 source. Only the frozen root purpose selects it.
//! Its immutable plan facts remain with preparation's owner; every new batch
//! backing is preflighted before growth under the original last-edge permit.

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{Buffer, MutableBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_result_contract::RootProfileV1;
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    MAX_CONNECTOR_STATISTICS_COLUMNS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES, MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES,
};
use novarocks_types::SlotId;

use crate::exec::chunk::{
    Chunk, ChunkSchemaRef, RootArrayStorageLimits, borrowed_root_chunk_schema_storage,
};
use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
use crate::exec::node::unpivot::{UnpivotConstant, UnpivotValueMapping};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator, RootPreparedPull};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::io::RootInputPermit;
use crate::runtime::runtime_state::RuntimeState;

const NAME: &str = "BOUNDED_STATISTICS_MATERIALIZER";
// Finalization validates four offset vectors and fixed canonical carriers.
// Keep even their scalar loads/comparisons within a separate 1024-work turn.
const BATCH_ROWS: usize = 32;
const BYTE_QUANTUM: usize = RootProfileV1::EMIT_BYTES_PER_TURN;
const WORK_QUANTUM: usize = RootProfileV1::CELLS_PER_TURN;

struct Definition {
    arena: Arc<ExprArena>,
    mappings: Vec<UnpivotValueMapping>,
    input_slots: Vec<SlotId>,
    input_ordinals: Vec<usize>,
    schema: ChunkSchemaRef,
    fixed_capacity: usize,
    batch_rows: usize,
}

pub(crate) struct StatisticsMaterializerFactory(Arc<Definition>);
impl StatisticsMaterializerFactory {
    #[expect(
        clippy::too_many_arguments,
        reason = "Frozen source roles and bounds are independently validated"
    )]
    pub(crate) fn try_new(
        arena: Arc<ExprArena>,
        value_slot: SlotId,
        literal_slots: Vec<SlotId>,
        mappings: Vec<UnpivotValueMapping>,
        input_slots: &[SlotId],
        schema: ChunkSchemaRef,
        passthrough_count: usize,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, String> {
        let fields = schema.slots();
        if fields.len() != 4
            || passthrough_count != 0
            || value_slot != fields[2].slot_id()
            || literal_slots.as_slice()
                != [
                    fields[0].slot_id(),
                    fields[1].slot_id(),
                    fields[3].slot_id(),
                ]
            || max_rows == 0
            || max_rows > MAX_CONNECTOR_STATISTICS_ARTIFACTS
            || max_bytes != MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
            || mappings.is_empty()
            || mappings.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS
        {
            return Err(
                "statistics materializer roles or bounds differ from its frozen domain".into(),
            );
        }
        if !schema.arrow_schema_ref().metadata().is_empty() {
            return Err("statistics schema metadata must be empty".into());
        }
        let names = ["input_fields", "blob_type", "body", "properties"];
        if fields.iter().zip(names).any(|(field, name)| {
            field.field().name() != name || !field.field().metadata().is_empty()
        }) || [0, 1, 3].iter().any(|&i| fields[i].field().is_nullable())
            || !matches!(fields[1].field().data_type(), DataType::Utf8)
            || !matches!(fields[2].field().data_type(), DataType::Binary)
        {
            return Err(
                "statistics materializer output schema differs from its frozen domain".into(),
            );
        }
        let DataType::List(item) = fields[0].field().data_type() else {
            return Err("statistics input_fields must be List<Int32>".into());
        };
        if item.name() != "item"
            || item.is_nullable()
            || !item.metadata().is_empty()
            || !matches!(item.data_type(), DataType::Int32)
        {
            return Err("statistics field IDs have an invalid item schema".into());
        }
        let DataType::Map(entries, false) = fields[3].field().data_type() else {
            return Err("statistics properties must be an unordered Map".into());
        };
        let DataType::Struct(children) = entries.data_type() else {
            return Err("statistics map entries must be Struct".into());
        };
        if entries.name() != "entries"
            || entries.is_nullable()
            || !entries.metadata().is_empty()
            || children.len() != 2
            || children.iter().zip(["key", "value"]).any(|(f, name)| {
                f.name() != name
                    || f.is_nullable()
                    || !f.metadata().is_empty()
                    || !matches!(f.data_type(), DataType::Utf8)
            })
        {
            return Err("statistics properties have an invalid entries schema".into());
        }
        for mapping in &mappings {
            let [
                UnpivotConstant::Int32List(ids),
                UnpivotConstant::Scalar {
                    expr_id,
                    nullable: false,
                },
                UnpivotConstant::Utf8Map(properties),
            ] = mapping.constants.as_slice()
            else {
                return Err("statistics constants differ from the frozen roles".into());
            };
            let Some(ExprNode::Literal(LiteralValue::Utf8(blob))) = arena.node(*expr_id) else {
                return Err("statistics blob_type must be an exact UTF8 literal".into());
            };
            if ids.is_empty()
                || ids.len() > MAX_CONNECTOR_STATISTICS_COLUMNS
                || blob.is_empty()
                || blob.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                || !properties.is_empty()
            {
                return Err("statistics literal exceeds its domain limits".into());
            }
            let mut sorted = [0i32; MAX_CONNECTOR_STATISTICS_COLUMNS];
            sorted[..ids.len()].copy_from_slice(ids);
            sorted[..ids.len()].sort_unstable();
            if sorted[0] <= 0
                || sorted[..ids.len()]
                    .windows(2)
                    .any(|pair| pair[0] == pair[1])
            {
                return Err("statistics field IDs must be positive and unique".into());
            }
        }
        // Resolve exact frozen input positions once under preparation's owner.
        // CPU continuations never perform data-dependent hash-table probes.
        let input_ordinals = mappings
            .iter()
            .map(|mapping| {
                input_slots
                    .iter()
                    .position(|slot| *slot == mapping.input_value_slot_id)
                    .ok_or("statistics body is absent from the frozen input port")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let metadata = borrowed_root_chunk_schema_storage(
            &schema,
            RootArrayStorageLimits {
                bytes: MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
                nodes: 2 * RootProfileV1::SCHEMA_TYPE_NODES,
                depth: RootProfileV1::MAX_DEPTH,
            },
        )
        .map_err(|_| "statistics output metadata has no bounded source proof")?;
        // One extra full metadata copy conservatively covers each actual
        // List/Map/inline Struct DataType alias inspected by the input oracle.
        // Object allowance mirrors that oracle, including inline Map entries.
        let objects = size_of::<ListArray>()
            + size_of::<Int32Array>()
            + 3 * size_of::<StringArray>()
            + size_of::<BinaryArray>()
            + size_of::<MapArray>()
            + size_of::<StructArray>()
            + 8 * 4 * size_of::<usize>();
        let buffers = crate::exec::chunk::ARROW_BUFFER_OWNER_METADATA_BOUND;
        let fixed_capacity = metadata
            .checked_mul(2)
            .and_then(|n| n.checked_add(objects))
            .and_then(|n| n.checked_add(11 * buffers))
            .and_then(|n| n.checked_add(6 * size_of::<ArrayRef>()))
            .and_then(|n| {
                n.checked_add(
                    size_of::<Chunk>() + size_of::<RecordBatch>() + size_of::<Workspace>(),
                )
            })
            .filter(|&n| n < MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES)
            .ok_or("statistics fixed backing exceeds its batch profile")?;
        Ok(Self(Arc::new(Definition {
            arena,
            mappings,
            input_slots: input_slots.to_vec(),
            input_ordinals,
            schema,
            fixed_capacity,
            batch_rows: max_rows.min(BATCH_ROWS),
        })))
    }
}
impl OperatorFactory for StatisticsMaterializerFactory {
    fn name(&self) -> &str {
        NAME
    }
    fn create(&self, _: i32, _: i32) -> Box<dyn Operator> {
        Box::new(StatisticsMaterializer {
            definition: self.0.clone(),
            input: None,
            workspace: None,
            cursor: 0,
            rows: 0,
            body_bytes: 0,
            metadata_bytes: 0,
            finishing: false,
            finished: false,
        })
    }
}
struct StatisticsMaterializer {
    definition: Arc<Definition>,
    input: Option<Chunk>,
    workspace: Option<Workspace>,
    cursor: usize,
    rows: usize,
    body_bytes: usize,
    metadata_bytes: usize,
    finishing: bool,
    finished: bool,
}
impl Operator for StatisticsMaterializer {
    fn name(&self) -> &str {
        NAME
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn cancel(&mut self) {
        drop(self.workspace.take());
        drop(self.input.take());
        self.finished = true;
    }
    fn close(&mut self) -> Result<(), String> {
        self.cancel();
        Ok(())
    }
}
impl ProcessorOperator for StatisticsMaterializer {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.input.is_none()
    }
    fn has_output(&self) -> bool {
        self.input.is_some() && !self.finished
    }
    fn push_chunk(&mut self, _: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("statistics input position is full".into());
        }
        let expanded = chunk
            .len()
            .checked_mul(self.definition.mappings.len())
            .ok_or("statistics expanded row count overflow")?;
        if self
            .rows
            .checked_add(expanded)
            .is_none_or(|n| n > MAX_CONNECTOR_STATISTICS_ARTIFACTS)
        {
            return Err("statistics expanded row count exceeds its domain".into());
        }
        if chunk.chunk_schema_ref().slot_ids() != self.definition.input_slots {
            return Err("statistics input order differs from its frozen port".into());
        }
        for &ordinal in &self.definition.input_ordinals {
            let array = body_array(&chunk, ordinal)?;
            if array.len() != chunk.len() {
                return Err("statistics body row count differs from input".into());
            }
        }
        self.cursor = 0;
        if !chunk.is_empty() {
            self.input = Some(chunk);
        }
        Ok(())
    }
    fn pull_chunk(&mut self, _: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err("statistics materialization requires the original root input grant".into())
    }
    fn pull_chunk_with_root_input(
        &mut self,
        _: &RuntimeState,
        permit: &RootInputPermit,
    ) -> Result<RootPreparedPull, String> {
        let Some(input) = self.input.as_ref() else {
            return Ok(RootPreparedPull::Empty);
        };
        if self.workspace.is_none() {
            let total = input.len() * self.definition.mappings.len();
            let mut sizes = Sizes::default();
            let mut rows = 0;
            for flat in self.cursor..total.min(self.cursor + self.definition.batch_rows) {
                let (ids, blob, body) = record(input, &self.definition, flat)?;
                if body.len() > MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES {
                    return Err("statistics body exceeds its artifact limit".into());
                }
                let next = sizes.add(ids.len(), blob.len(), body.len())?;
                if self
                    .body_bytes
                    .checked_add(next.body)
                    .is_none_or(|n| n > MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES)
                {
                    return Err("statistics aggregate body exceeds its domain".into());
                }
                if self
                    .metadata_bytes
                    .checked_add(next.metadata(rows + 1)?)
                    .is_none_or(|n| n > 16 * 1024 * 1024)
                {
                    return Err("statistics aggregate metadata exceeds its domain".into());
                }
                let backing = next.capacity(rows + 1, self.definition.fixed_capacity)?;
                if backing > MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                    || backing > permit.retained_bytes()
                {
                    if rows == 0 {
                        return Err("one statistics record exceeds its pregranted backing".into());
                    }
                    break;
                }
                sizes = next;
                rows += 1;
            }
            // Every capacity, fixed owner and simultaneous temporary is known
            // before any MutableBuffer can grow. No candidate/probe batch.
            self.workspace = Some(Workspace::new(
                self.cursor,
                rows,
                sizes,
                permit.generation(),
            ));
            return Ok(RootPreparedPull::Yielded);
        }
        let workspace = self.workspace.as_mut().unwrap();
        if workspace.generation != permit.generation() {
            return Err("statistics continuation changed its original grant".into());
        }
        if workspace.row < workspace.rows {
            let (examined, work) = workspace.advance(input, &self.definition)?;
            debug_assert!(examined <= BYTE_QUANTUM && work <= WORK_QUANTUM);
            // Final offset/array validation gets its own finite turn, even
            // when this copy used the entire byte quantum.
            return Ok(RootPreparedPull::Yielded);
        }
        let workspace = self.workspace.take().unwrap();
        let rows = workspace.rows;
        let body = workspace.sizes.body;
        let metadata = workspace.sizes.metadata(rows)?;
        let chunk = workspace.finish(&self.definition.schema)?;
        self.cursor += rows;
        self.rows += rows;
        self.body_bytes += body;
        self.metadata_bytes += metadata;
        if self.cursor == input.len() * self.definition.mappings.len() {
            drop(self.input.take());
            self.cursor = 0;
            if self.finishing {
                self.finished = true;
            }
        }
        Ok(RootPreparedPull::Chunk(chunk))
    }
    fn release_root_pull_workspace(&mut self) {
        drop(self.workspace.take());
    }
    fn set_finishing(&mut self, _: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        self.finished = self.input.is_none();
        Ok(())
    }
}
fn body_array(input: &Chunk, index: usize) -> Result<&BinaryArray, String> {
    input
        .batch
        .columns()
        .get(index)
        .ok_or("statistics body ordinal is absent")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "statistics aggregate must produce an exact BinaryArray".into())
}
fn record<'a>(
    input: &'a Chunk,
    definition: &'a Definition,
    flat: usize,
) -> Result<(&'a [i32], &'a str, &'a [u8]), String> {
    let mapping = &definition.mappings[flat / input.len()];
    let row = flat % input.len();
    let array = body_array(input, definition.input_ordinals[flat / input.len()])?;
    if array.is_null(row) {
        return Err("statistics aggregate produced a null body".into());
    }
    let UnpivotConstant::Int32List(ids) = &mapping.constants[0] else {
        unreachable!()
    };
    let UnpivotConstant::Scalar { expr_id, .. } = mapping.constants[1] else {
        unreachable!()
    };
    let Some(ExprNode::Literal(LiteralValue::Utf8(blob))) = definition.arena.node(expr_id) else {
        unreachable!()
    };
    Ok((ids, blob, array.value(row)))
}
#[derive(Clone, Copy, Default)]
struct Sizes {
    ids: usize,
    blob: usize,
    body: usize,
}
impl Sizes {
    fn metadata(self, rows: usize) -> Result<usize, String> {
        rows.checked_mul(24)
            .and_then(|n| n.checked_add(self.ids.checked_mul(4)?))
            .and_then(|n| n.checked_add(self.blob))
            .ok_or_else(|| "statistics metadata size overflow".into())
    }
    fn add(self, ids: usize, blob: usize, body: usize) -> Result<Self, String> {
        Ok(Self {
            ids: self
                .ids
                .checked_add(ids)
                .ok_or("statistics ID size overflow")?,
            blob: self
                .blob
                .checked_add(blob)
                .ok_or("statistics label size overflow")?,
            body: self
                .body
                .checked_add(body)
                .ok_or("statistics body size overflow")?,
        })
    }
    fn capacity(self, rows: usize, fixed: usize) -> Result<usize, String> {
        let offsets = round64(
            (rows + 1)
                .checked_mul(4)
                .ok_or("statistics offset overflow")?,
        )?;
        fixed
            .checked_add(
                offsets
                    .checked_mul(4)
                    .ok_or("statistics offsets capacity overflow")?,
            )
            .and_then(|n| n.checked_add(round64(self.ids.checked_mul(4)?).ok()?))
            .and_then(|n| n.checked_add(round64(self.blob).ok()?))
            .and_then(|n| n.checked_add(round64(self.body).ok()?))
            .and_then(|n| n.checked_add(2 * 64))
            .ok_or_else(|| "statistics backing capacity overflow".into())
    }
}
fn round64(bytes: usize) -> Result<usize, String> {
    bytes
        .checked_add(63)
        .map(|n| n & !63)
        .ok_or_else(|| "statistics alignment capacity overflow".into())
}
struct Workspace {
    start: usize,
    rows: usize,
    sizes: Sizes,
    generation: u64,
    row: usize,
    phase: u8,
    offset: usize,
    ids_offsets: MutableBuffer,
    ids: MutableBuffer,
    blob_offsets: MutableBuffer,
    blob: MutableBuffer,
    body_offsets: MutableBuffer,
    body: MutableBuffer,
    map_offsets: MutableBuffer,
}
impl Workspace {
    fn new(start: usize, rows: usize, sizes: Sizes, generation: u64) -> Self {
        let offsets = || {
            let mut b = MutableBuffer::with_capacity(4 * (rows + 1));
            b.push(0i32);
            b
        };
        Self {
            start,
            rows,
            sizes,
            generation,
            row: 0,
            phase: 0,
            offset: 0,
            ids_offsets: offsets(),
            ids: MutableBuffer::with_capacity(4 * sizes.ids),
            blob_offsets: offsets(),
            blob: MutableBuffer::with_capacity(sizes.blob),
            body_offsets: offsets(),
            body: MutableBuffer::with_capacity(sizes.body),
            map_offsets: offsets(),
        }
    }
    fn advance(
        &mut self,
        input: &Chunk,
        definition: &Definition,
    ) -> Result<(usize, usize), String> {
        let mut examined = 0;
        let mut work = 0;
        // 64 bytes cover record lookup/source offsets and four output offsets;
        // eight work units cover fixed carrier/slot/constant lookups and writes.
        while self.row < self.rows && work + 8 <= WORK_QUANTUM && examined + 64 <= BYTE_QUANTUM {
            let (ids, blob, body) = record(input, definition, self.start + self.row)?;
            work += 8;
            examined += 64;
            let left = (BYTE_QUANTUM - examined) / 2;
            let copied = match self.phase {
                0 => {
                    let count = (left / 4).min(ids.len() - self.offset);
                    self.ids
                        .extend_from_slice(&ids[self.offset..self.offset + count]);
                    self.offset += count;
                    count * 4
                }
                1 => {
                    let count = left.min(blob.len() - self.offset);
                    self.blob
                        .extend_from_slice(&blob.as_bytes()[self.offset..self.offset + count]);
                    self.offset += count;
                    count
                }
                2 => {
                    let count = left.min(body.len() - self.offset);
                    self.body
                        .extend_from_slice(&body[self.offset..self.offset + count]);
                    self.offset += count;
                    count
                }
                _ => unreachable!(),
            };
            examined += 2 * copied;
            let len = match self.phase {
                0 => ids.len(),
                1 => blob.len(),
                _ => body.len(),
            };
            if self.offset == len {
                self.phase += 1;
                self.offset = 0;
                if self.phase == 3 {
                    self.ids_offsets.push((self.ids.len() / 4) as i32);
                    self.blob_offsets.push(self.blob.len() as i32);
                    self.body_offsets.push(self.body.len() as i32);
                    self.map_offsets.push(0i32);
                    self.row += 1;
                    self.phase = 0;
                }
            }
            if copied == 0 && self.offset != len {
                break;
            }
        }
        Ok((examined, work))
    }
    fn finish(self, schema: &ChunkSchemaRef) -> Result<Chunk, String> {
        let offsets = |b: MutableBuffer| OffsetBuffer::new(ScalarBuffer::from(Buffer::from(b)));
        let DataType::List(item) = schema.slots()[0].field().data_type() else {
            unreachable!()
        };
        let DataType::Map(entries_field, false) = schema.slots()[3].field().data_type() else {
            unreachable!()
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            unreachable!()
        };
        let ids: ArrayRef = Arc::new(Int32Array::new(
            ScalarBuffer::from(Buffer::from(self.ids)),
            None,
        ));
        let fields: ArrayRef = Arc::new(ListArray::new(
            item.clone(),
            offsets(self.ids_offsets),
            ids,
            None,
        ));
        // SAFETY: each source is an immutable Rust str; its bytes are copied
        // unchanged. Offsets are appended only after that whole str, start at
        // zero, are monotone, and end exactly at this preallocated buffer len.
        // Revalidating the entire concatenation would defeat CPU continuation.
        let labels: ArrayRef = Arc::new(unsafe {
            StringArray::new_unchecked(offsets(self.blob_offsets), Buffer::from(self.blob), None)
        });
        let body: ArrayRef = Arc::new(BinaryArray::new(
            offsets(self.body_offsets),
            Buffer::from(self.body),
            None,
        ));
        let empty_utf8 = || {
            let mut zero = MutableBuffer::with_capacity(4);
            zero.push(0i32);
            Arc::new(StringArray::new(
                offsets(zero),
                Buffer::from(MutableBuffer::with_capacity(0)),
                None,
            )) as ArrayRef
        };
        let entries =
            StructArray::new(entry_fields.clone(), vec![empty_utf8(), empty_utf8()], None);
        let properties: ArrayRef = Arc::new(MapArray::new(
            entries_field.clone(),
            offsets(self.map_offsets),
            entries,
            None,
            false,
        ));
        let batch = RecordBatch::try_new(
            schema.arrow_schema_ref(),
            vec![fields, labels, body, properties],
        )
        .map_err(|e| format!("statistics batch construction failed: {e}"))?;
        Chunk::try_new_with_chunk_schema(batch, schema.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, RootArrayStorageLimits, borrowed_root_chunk_storage};
    use crate::runtime::fragment::io::{
        ResultWriteCredit, RootInputAdmission, RootInputAuthority, RootResultWriteSpec,
    };
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_result_contract::{
        FrozenRootOutput, InternalResultDomain, RootOutputContract, RootProfileId,
    };
    use novarocks_types::arrow_metadata_owner::{
        ArrowMetadataOwner, FieldMetadataOrigins, MetadataOwnedField, MetadataOwnerLimits,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn metadata() -> ArrowMetadataOwner {
        ArrowMetadataOwner::try_new(
            Vec::new(),
            MetadataOwnerLimits {
                entries: 0,
                construction_bytes: 0,
            },
        )
        .unwrap()
    }
    fn schema() -> ChunkSchemaRef {
        let mut owners = Vec::<MetadataOwnedField>::new();
        let mut field = |name: &str, ty| {
            let owner = metadata().into_field(name.into(), ty, false);
            let field = owner.field().clone();
            owners.push(owner);
            field
        };
        let item = field("item", DataType::Int32);
        let key = field("key", DataType::Utf8);
        let value = field("value", DataType::Utf8);
        let entries = field("entries", DataType::Struct(vec![key, value].into()));
        let fields = vec![
            field("input_fields", DataType::List(item)),
            field("blob_type", DataType::Utf8),
            field("body", DataType::Binary),
            field("properties", DataType::Map(entries, false)),
        ];
        let source = metadata().into_schema(fields.into());
        ChunkSchema::try_ref_from_owned_schema_and_slot_ids(
            &source,
            &FieldMetadataOrigins::try_new(owners, 8).unwrap(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
            ],
        )
        .unwrap()
    }
    fn factory(
        ids: Vec<i32>,
        blob: String,
        max_rows: usize,
        mappings: usize,
    ) -> Result<StatisticsMaterializerFactory, String> {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::Literal(LiteralValue::Utf8(blob)), DataType::Utf8);
        let mapping = UnpivotValueMapping {
            input_value_slot_id: SlotId::new(99),
            constants: vec![
                UnpivotConstant::Int32List(ids),
                UnpivotConstant::Scalar {
                    expr_id,
                    nullable: false,
                },
                UnpivotConstant::Utf8Map(Vec::new()),
            ],
        };
        StatisticsMaterializerFactory::try_new(
            Arc::new(arena),
            SlotId::new(3),
            vec![SlotId::new(1), SlotId::new(2), SlotId::new(4)],
            vec![mapping; mappings],
            &[SlotId::new(99)],
            schema(),
            0,
            max_rows,
            MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
        )
    }
    fn operator(factory: StatisticsMaterializerFactory) -> StatisticsMaterializer {
        StatisticsMaterializer {
            definition: factory.0,
            input: None,
            workspace: None,
            cursor: 0,
            rows: 0,
            body_bytes: 0,
            metadata_bytes: 0,
            finishing: false,
            finished: false,
        }
    }
    fn input(rows: &[&[u8]]) -> (Chunk, std::sync::Weak<BinaryArray>) {
        let array = Arc::new(BinaryArray::from_iter_values(rows.iter().copied()));
        let weak = Arc::downgrade(&array);
        let schema = Arc::new(
            ChunkSchema::try_new(vec![crate::exec::chunk::ChunkSlotSchema::new_with_field(
                SlotId::new(99),
                arrow::datatypes::Field::new("aggregate", DataType::Binary, false),
                None,
                None,
            )])
            .unwrap(),
        );
        (
            Chunk::try_new_with_chunk_schema(
                RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap(),
                schema,
            )
            .unwrap(),
            weak,
        )
    }
    fn permit() -> RootInputPermit {
        let spec = RootResultWriteSpec {
            task: TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            contract: Arc::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            )),
        };
        let authority = RootInputAuthority::new(&spec);
        let RootInputAdmission::Granted(permit) = authority
            .try_acquire(ResultWriteCredit::new(authority.required_bytes(), |_| {}))
            .unwrap()
        else {
            panic!("one original grant");
        };
        permit
    }
    #[test]
    fn unicode_large_body_continues_without_growth_and_finalizes_on_a_separate_turn() {
        let label = "统计λ".repeat(7000);
        let body = vec![0xff; 512 * 1024];
        let (chunk, weak) = input(&[&body]);
        let mut source = operator(factory(vec![1, 3, 9], label.clone(), 4096, 1).unwrap());
        let state = RuntimeState::default();
        let permit = permit();
        source.push_chunk(&state, chunk).unwrap();
        source.set_finishing(&state).unwrap();
        assert!(matches!(
            source.pull_chunk_with_root_input(&state, &permit).unwrap(),
            RootPreparedPull::Yielded
        ));
        let reserved = source
            .workspace
            .as_ref()
            .unwrap()
            .sizes
            .capacity(1, source.definition.fixed_capacity)
            .unwrap();
        let capacities = |w: &Workspace| {
            [
                w.ids_offsets.capacity(),
                w.ids.capacity(),
                w.blob_offsets.capacity(),
                w.blob.capacity(),
                w.body_offsets.capacity(),
                w.body.capacity(),
                w.map_offsets.capacity(),
            ]
        };
        let original = capacities(source.workspace.as_ref().unwrap());
        let mut turns = 0;
        while source.workspace.as_ref().unwrap().row == 0 {
            let w = source.workspace.as_mut().unwrap();
            let (examined, work) = w
                .advance(source.input.as_ref().unwrap(), &source.definition)
                .unwrap();
            assert!(examined <= BYTE_QUANTUM && work <= WORK_QUANTUM);
            assert_eq!(capacities(w), original);
            turns += 1;
        }
        assert!(turns > 16);
        let RootPreparedPull::Chunk(output) =
            source.pull_chunk_with_root_input(&state, &permit).unwrap()
        else {
            panic!("separate finalization turn");
        };
        assert_eq!(
            output
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            label
        );
        assert_eq!(
            output
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            body
        );
        let actual = borrowed_root_chunk_storage(
            &output,
            RootArrayStorageLimits {
                bytes: MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
                nodes: 8192,
                depth: 64,
            },
        )
        .unwrap();
        assert!(
            actual <= reserved,
            "actual {actual} exceeds prepaid {reserved}"
        );
        assert!(weak.upgrade().is_none());
        assert!(source.is_finished());
    }
    #[test]
    fn real_local_program_builder_selects_the_protected_source_only_for_statistics_root() {
        use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
        use crate::exec::node::{unpivot::UnpivotNode, values::ValuesNode};
        use crate::exec::pipeline::{
            binding::{ExchangeBindings, ScanBindings},
            dependency::DependencyManager,
        };
        use novarocks_local_program::StaticSinkProgram;
        for protected in [true, false] {
            let definition = factory(vec![1], "theta".into(), 4096, 1).unwrap().0;
            let (chunk, _) = input(&[b"actual decoded aggregate"]);
            let plan = ExecPlan {
                arena: definition.arena.as_ref().clone(),
                root: ExecNode {
                    kind: ExecNodeKind::Unpivot(UnpivotNode {
                        input: Box::new(ExecNode {
                            kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 1 }),
                        }),
                        node_id: 2,
                        passthrough_columns: Vec::new(),
                        value_output_slot_id: SlotId::new(3),
                        literal_output_slot_ids: vec![
                            SlotId::new(1),
                            SlotId::new(2),
                            SlotId::new(4),
                        ],
                        value_mappings: definition.mappings.clone(),
                        output_chunk_schema: definition.schema.clone(),
                        max_output_rows: 4096,
                        max_output_bytes: 32 << 20,
                    }),
                },
            };
            let profile = plan
                .local_compile_profile(
                    std::num::NonZeroUsize::new(1).unwrap(),
                    Some(std::num::NonZeroUsize::new(1).unwrap()),
                )
                .unwrap();
            let sink = if protected {
                StaticSinkProgram::RootResult(Arc::new(RootOutputContract::new(
                    RootProfileId::V1,
                    FrozenRootOutput::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
                )))
            } else {
                StaticSinkProgram::Noop
            };
            let (program, bindings) = plan
                .into_local_program_and_bindings(
                    profile,
                    std::collections::BTreeMap::new(),
                    if protected {
                        vec![crate::exec::node::ExternalSinkRequirement::Result]
                    } else {
                        Vec::new()
                    },
                    sink,
                )
                .unwrap();
            let state = RuntimeState::default();
            let graph = crate::exec::pipeline::builder::build_native_pipeline_graph_for_local_program_with_runtime_settings(&program, &bindings, false, DependencyManager::new(), None, ExchangeBindings::default(), ScanBindings::default(), 1, Some(1), None, crate::runtime::execution_runtime::test_execution_function_set(), state.error_state(), 1, 1024, 1024).unwrap();
            assert_eq!(graph.pipelines.len(), 1);
            let factories = &graph.pipelines[0].factories;
            assert_eq!(factories.len(), 2);
            assert_eq!(factories[1].name() == NAME, protected);
            if protected {
                let mut source = factories[0].create(0, 0);
                let input = source
                    .as_processor_mut()
                    .unwrap()
                    .pull_chunk(&state)
                    .unwrap()
                    .unwrap();
                let mut materializer = factories[1].create(0, 0);
                let materializer = materializer.as_processor_mut().unwrap();
                materializer.push_chunk(&state, input).unwrap();
                assert!(materializer.pull_chunk(&state).is_err());
                let permit = permit();
                loop {
                    match materializer
                        .pull_chunk_with_root_input(&state, &permit)
                        .unwrap()
                    {
                        RootPreparedPull::Yielded => {}
                        RootPreparedPull::Chunk(chunk) => {
                            assert_eq!(
                                chunk
                                    .batch
                                    .column(2)
                                    .as_any()
                                    .downcast_ref::<BinaryArray>()
                                    .unwrap()
                                    .value(0),
                                b"actual decoded aggregate"
                            );
                            assert!(
                                borrowed_root_chunk_storage(
                                    &chunk,
                                    RootArrayStorageLimits {
                                        bytes: 32 << 20,
                                        nodes: 8192,
                                        depth: 64
                                    }
                                )
                                .is_ok()
                            );
                            break;
                        }
                        RootPreparedPull::Empty => panic!("actual output disappeared"),
                    }
                }
            }
        }
    }

    #[test]
    fn a_completed_copy_always_yields_before_arrow_finalization() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "test".into(), 1, 1).unwrap());
        let bytes = vec![0; 32752];
        let (chunk, _) = input(&[&bytes]);
        source.push_chunk(&state, chunk).unwrap();
        let permit = permit();
        assert!(matches!(
            source.pull_chunk_with_root_input(&state, &permit).unwrap(),
            RootPreparedPull::Yielded
        ));
        while source.workspace.as_ref().unwrap().row == 0 {
            assert!(matches!(
                source.pull_chunk_with_root_input(&state, &permit).unwrap(),
                RootPreparedPull::Yielded
            ));
        }
        assert!(matches!(
            source.pull_chunk_with_root_input(&state, &permit).unwrap(),
            RootPreparedPull::Chunk(_)
        ));
    }
    #[test]
    fn one_bounded_artifact_per_batch_does_not_shrink_the_upstream_aggregate() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 4096, 1).unwrap());
        let bytes = vec![0xff; MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES];
        let (chunk, weak) = input(&[&bytes, &bytes]);
        source.push_chunk(&state, chunk).unwrap();
        source.set_finishing(&state).unwrap();
        for index in 0..2 {
            let permit = permit();
            loop {
                match source.pull_chunk_with_root_input(&state, &permit).unwrap() {
                    RootPreparedPull::Yielded => {}
                    RootPreparedPull::Chunk(chunk) => {
                        assert_eq!(chunk.len(), 1);
                        assert_eq!(
                            chunk
                                .batch
                                .column(2)
                                .as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .value(0),
                            bytes
                        );
                        assert!(
                            borrowed_root_chunk_storage(
                                &chunk,
                                RootArrayStorageLimits {
                                    bytes: 32 << 20,
                                    nodes: 8192,
                                    depth: 64
                                }
                            )
                            .is_ok()
                        );
                        break;
                    }
                    RootPreparedPull::Empty => panic!("expected bounded artifact"),
                }
            }
            assert_eq!(weak.upgrade().is_some(), index == 0);
        }
        assert!(source.is_finished());
    }

    #[test]
    fn per_node_row_limit_preserves_mapping_major_order_across_original_grants() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 2).unwrap());
        let (chunk, _) = input(&[b"a", b"b", b"c"]);
        source.push_chunk(&state, chunk).unwrap();
        source.set_finishing(&state).unwrap();
        let mut bodies = Vec::new();
        for _ in 0..6 {
            let permit = permit();
            assert!(matches!(
                source.pull_chunk_with_root_input(&state, &permit).unwrap(),
                RootPreparedPull::Yielded
            ));
            loop {
                match source.pull_chunk_with_root_input(&state, &permit).unwrap() {
                    RootPreparedPull::Yielded => {}
                    RootPreparedPull::Chunk(chunk) => {
                        assert_eq!(chunk.len(), 1);
                        bodies.push(
                            chunk
                                .batch
                                .column(2)
                                .as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .value(0)
                                .to_vec(),
                        );
                        break;
                    }
                    RootPreparedPull::Empty => panic!("pending record disappeared"),
                }
            }
        }
        assert_eq!(bodies, [b"a", b"b", b"c", b"a", b"b", b"c"]);
        assert!(source.is_finished());
    }
    #[test]
    fn invalid_static_ids_fail_before_materialization_and_dynamic_limits_before_workspace() {
        for ids in [vec![-1], vec![1, 1], Vec::new()] {
            assert!(factory(ids, "theta".into(), 1, 1).is_err());
        }
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 1).unwrap());
        source.metadata_bytes = 16 * 1024 * 1024;
        let (chunk, _) = input(&[b"body"]);
        source.push_chunk(&state, chunk).unwrap();
        assert!(
            source
                .pull_chunk_with_root_input(&state, &permit())
                .err()
                .unwrap()
                .contains("metadata")
        );
        assert!(source.workspace.is_none());
    }
    #[test]
    fn cancellation_and_generation_conflict_leave_cleanup_under_the_original_grant() {
        let state = RuntimeState::default();
        let mut source = operator(factory(vec![1], "theta".into(), 1, 1).unwrap());
        let (chunk, weak) = input(&[b"body"]);
        source.push_chunk(&state, chunk).unwrap();
        let permit = permit();
        source.pull_chunk_with_root_input(&state, &permit).unwrap();
        // An issuer change is validated by the final edge/session; source
        // additionally rejects a changed continuation generation.
        source.workspace.as_mut().unwrap().generation += 1;
        assert!(source.pull_chunk_with_root_input(&state, &permit).is_err());
        assert!(source.workspace.is_some());
        source.release_root_pull_workspace();
        assert!(source.workspace.is_none());
        assert!(weak.upgrade().is_some());
        source.cancel();
        assert!(weak.upgrade().is_none());
        assert!(source.is_finished());
    }
}
