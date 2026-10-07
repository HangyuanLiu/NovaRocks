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
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::agg::{AggregateAllocator, AggregateVec};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use novarocks_execution_contract::{SafeDetail, TaskFailure, TaskFailureCategory};
use novarocks_local_program::QuotaNeed;
use std::ptr::NonNull;
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct OwnedBytes(pub AggregateVec<u8>);
impl std::borrow::Borrow<[u8]> for OwnedBytes {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

pub(super) struct Budget {
    limit: usize,
    task: Option<Arc<MemTracker>>,
    tracker: Option<Arc<MemTracker>>,
}
impl Budget {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            task: None,
            tracker: None,
        }
    }
    pub fn bind(&mut self, state: &RuntimeState) -> Result<AggregateAllocator, String> {
        let task = state
            .mem_tracker()
            .ok_or_else(|| "quota operator requires an exact task memory tracker".to_string())?;
        if let Some(old) = &self.task {
            if !Arc::ptr_eq(old, &task) {
                return Err("quota owner cannot cross task memory trackers".into());
            }
        } else {
            let tracker = MemTracker::new_child("MV quota resident state", &task);
            let limit =
                i64::try_from(self.limit).map_err(|_| capacity(state, u64::MAX, self.limit))?;
            if limit <= 0 {
                return Err("quota state budget must be positive".into());
            }
            tracker.install_limit_once(limit)?;
            tracker.bind_runtime_error(&state.error_state());
            self.task = Some(task);
            self.tracker = Some(tracker);
        }
        Ok(AggregateAllocator::new(
            self.tracker.as_ref().unwrap().clone(),
        ))
    }
    pub fn allocator(&self) -> Result<AggregateAllocator, String> {
        Ok(AggregateAllocator::new(
            self.tracker
                .as_ref()
                .ok_or_else(|| "quota owner has not been bound".to_string())?
                .clone(),
        ))
    }
}
pub(super) fn allocation_failure(state: &RuntimeState) -> String {
    let failure = state.error_state().task_failure().unwrap_or_else(|| {
        TaskFailure::new(
            TaskFailureCategory::Internal,
            SafeDetail::truncating("quota allocator was refused without a typed tracker cause"),
        )
    });
    state.fail_task(failure)
}
pub(super) fn reserve<T>(
    values: &mut AggregateVec<T>,
    additional: usize,
    state: &RuntimeState,
) -> Result<(), String> {
    values
        .try_reserve(additional)
        .map_err(|_| allocation_failure(state))
}
pub(super) fn bytes(
    value: &[u8],
    allocator: AggregateAllocator,
    state: &RuntimeState,
) -> Result<AggregateVec<u8>, String> {
    let mut out = AggregateVec::new_in(allocator);
    out.try_reserve_exact(value.len())
        .map_err(|_| allocation_failure(state))?;
    out.extend_from_slice(value);
    Ok(out)
}
pub(super) fn capacity(state: &RuntimeState, requested: u64, limit: usize) -> String {
    state.fail_task(TaskFailure::capacity_refused(
        SafeDetail::truncating("MV quota resident state"),
        requested,
        limit as u64,
    ))
}
pub(super) fn execution(state: &RuntimeState, message: &str) -> String {
    state.fail_task(TaskFailure::new(
        TaskFailureCategory::Execution,
        SafeDetail::truncating(message),
    ))
}
pub(super) fn array(chunk: &Chunk, column: usize) -> Result<&ArrayRef, String> {
    chunk
        .columns()
        .get(column)
        .ok_or_else(|| "quota column ordinal is out of bounds".into())
}
pub(super) fn binary(chunk: &Chunk, column: usize, row: usize) -> Result<&[u8], String> {
    let a = array(chunk, column)?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| "quota entry/key must be Binary".to_string())?;
    if a.is_null(row) {
        return Err("quota entry/key cannot be NULL".into());
    }
    Ok(a.value(row))
}
pub(super) fn need(
    chunk: &Chunk,
    kind: QuotaNeed,
    row: usize,
    state: &RuntimeState,
) -> Result<u64, String> {
    let a = array(chunk, kind.column())?;
    if a.is_null(row) {
        return Err(execution(state, "quota demand cannot be NULL"));
    }
    let count = match kind {
        QuotaNeed::NegativeWeight { .. } => {
            let a = a
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "quota weight must be Int64".to_string())?;
            let w = a.value(row);
            if w >= 0 {
                return Err(execution(
                    state,
                    "quota negative weight must be strictly negative",
                ));
            }
            crate::exec::expr::agg::checked_weight_neg(w).map_err(|f| state.fail_task(f))? as u64
        }
        QuotaNeed::Count { .. } => {
            if let Some(a) = a.as_any().downcast_ref::<UInt64Array>() {
                a.value(row)
            } else {
                let a = a
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "quota count must be Int64 or UInt64".to_string())?;
                u64::try_from(a.value(row))
                    .map_err(|_| execution(state, "quota count cannot be negative"))?
            }
        }
    };
    if count == 0 {
        return Err(execution(state, "quota demand must be strictly positive"));
    }
    Ok(count)
}
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct Location {
    pub file: AggregateVec<u8>,
    pub position: i64,
}
pub(super) fn location(
    chunk: &Chunk,
    file: usize,
    position: usize,
    row: usize,
) -> Result<(&str, i64), String> {
    let f = array(chunk, file)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "quota file must be Utf8".to_string())?;
    let p = array(chunk, position)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "quota position must be Int64".to_string())?;
    if f.is_null(row) || p.is_null(row) || f.value(row).is_empty() || p.value(row) < 0 {
        return Err(
            "quota location must name a nonempty exact file and nonnegative position".into(),
        );
    }
    Ok((f.value(row), p.value(row)))
}
pub(super) struct OutputRow {
    pub id: Option<AggregateVec<u8>>,
    pub location: Location,
}

/// Evidence is minted only from custom buffers backed by exact tracked vectors.
/// Private fields prevent a caller from declaring arbitrary Arrow arrays paid.
#[derive(Debug)]
pub(crate) struct QuotaOutputMemory {
    task: Arc<MemTracker>,
    buffers: AggregateVec<Buffer>,
    error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
}
fn tracker_belongs_to_task(task: &Arc<MemTracker>, tracker: &Arc<MemTracker>) -> bool {
    Arc::ptr_eq(task, tracker)
        || task
            .children()
            .iter()
            .any(|child| tracker_belongs_to_task(child, tracker))
}
impl QuotaOutputMemory {
    pub(crate) fn validate_columns(&self, columns: &[ArrayRef]) -> Result<(), String> {
        fn covered(data: &arrow::array::ArrayData, owners: &[Buffer]) -> bool {
            data.buffers()
                .iter()
                .chain(data.nulls().map(|nulls| nulls.buffer()))
                .all(|buffer| {
                    buffer.capacity() == 0
                        || owners
                            .iter()
                            .any(|owner| buffer.data_ptr() == owner.data_ptr())
                })
                && data.child_data().iter().all(|child| covered(child, owners))
        }
        if columns
            .iter()
            .all(|column| covered(&column.to_data(), &self.buffers))
        {
            Ok(())
        } else {
            Err("quota output contains an Arrow buffer without tracked allocation evidence".into())
        }
    }
    pub(crate) fn validate_tracker(&self, tracker: &Arc<MemTracker>) -> Result<(), String> {
        if tracker_belongs_to_task(&self.task, tracker) {
            Ok(())
        } else {
            Err("quota output cannot cross its exact task memory hierarchy".into())
        }
    }
    pub(crate) fn refuse_transfer(&self, message: &str) {
        self.error.set_failure(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::truncating(message),
        ));
    }
}

// Arrow borrows immutable, allocator-tracked buffers. The custom owner retains
// the allocation and its exact Layout charge until the last Arrow buffer drops.
fn buffer<T: arrow::datatypes::ArrowNativeType + std::panic::RefUnwindSafe>(
    values: AggregateVec<T>,
    evidence: &mut AggregateVec<Buffer>,
    state: &RuntimeState,
) -> Result<Buffer, String> {
    let task = state
        .mem_tracker()
        .ok_or_else(|| "quota output requires an exact task tracker".to_string())?;
    if !tracker_belongs_to_task(&task, values.allocator().tracker()) {
        return Err(execution(
            state,
            "quota Arrow buffer allocator belongs to another task",
        ));
    }
    let length = values.len() * std::mem::size_of::<T>();
    let pointer = NonNull::new(values.as_ptr() as *mut u8).expect("vector pointer is nonnull");
    let owner = Arc::new(values);
    // SAFETY: the immutable owner keeps this allocation alive, correctly aligned
    // for T, and unmodified for the complete buffer lifetime.
    let buffer = unsafe { Buffer::from_custom_allocation(pointer, length, owner) };
    reserve(evidence, 1, state)?;
    evidence.push(buffer.clone());
    Ok(buffer)
}
pub(super) fn output(
    schema: ChunkSchemaRef,
    rows: AggregateVec<OutputRow>,
    with_ids: bool,
    allocator: AggregateAllocator,
    state: &RuntimeState,
) -> Result<Chunk, String> {
    let mut evidence = AggregateVec::new_in(allocator.clone());
    let mut file_data = AggregateVec::new_in(allocator.clone());
    let mut file_offsets = AggregateVec::new_in(allocator.clone());
    let mut positions = AggregateVec::new_in(allocator.clone());
    let mut id_data = AggregateVec::new_in(allocator.clone());
    let mut id_offsets = AggregateVec::new_in(allocator);
    reserve(&mut file_offsets, rows.len() + 1, state)?;
    reserve(&mut positions, rows.len(), state)?;
    file_offsets.push(0i32);
    if with_ids {
        reserve(&mut id_offsets, rows.len() + 1, state)?;
        id_offsets.push(0i32);
    }
    for row in rows.iter() {
        reserve(&mut file_data, row.location.file.len(), state)?;
        file_data.extend_from_slice(&row.location.file);
        file_offsets.push(
            i32::try_from(file_data.len())
                .map_err(|_| execution(state, "quota file output exceeds Arrow offset limit"))?,
        );
        positions.push(row.location.position);
        if with_ids {
            let id = row
                .id
                .as_ref()
                .ok_or_else(|| "quota output entry ID is missing".to_string())?;
            reserve(&mut id_data, id.len(), state)?;
            id_data.extend_from_slice(id);
            id_offsets.push(i32::try_from(id_data.len()).map_err(|_| {
                execution(state, "quota entry ID output exceeds Arrow offset limit")
            })?);
        }
    }
    let mut arrays = Vec::with_capacity(if with_ids { 3 } else { 2 });
    if with_ids {
        arrays.push(Arc::new(
            BinaryArray::try_new(
                OffsetBuffer::new(ScalarBuffer::new(
                    buffer(id_offsets, &mut evidence, state)?,
                    0,
                    rows.len() + 1,
                )),
                buffer(id_data, &mut evidence, state)?,
                None,
            )
            .map_err(|e| e.to_string())?,
        ) as ArrayRef);
    }
    arrays.push(Arc::new(
        StringArray::try_new(
            OffsetBuffer::new(ScalarBuffer::new(
                buffer(file_offsets, &mut evidence, state)?,
                0,
                rows.len() + 1,
            )),
            buffer(file_data, &mut evidence, state)?,
            None,
        )
        .map_err(|e| e.to_string())?,
    ) as ArrayRef);
    arrays.push(Arc::new(Int64Array::new(
        ScalarBuffer::new(buffer(positions, &mut evidence, state)?, 0, rows.len()),
        None,
    )) as ArrayRef);
    let memory = QuotaOutputMemory {
        task: state
            .mem_tracker()
            .ok_or_else(|| "quota output requires exact task tracker".to_string())?,
        buffers: evidence,
        error: state.error_state(),
    };
    Chunk::try_new_quota_output(schema, arrays, Arc::new(memory))
}
pub(super) struct DriverEos {
    count: usize,
    done: Option<AggregateVec<bool>>,
}
impl DriverEos {
    pub fn new(count: usize) -> Self {
        Self { count, done: None }
    }
    pub fn bind(
        &mut self,
        allocator: AggregateAllocator,
        state: &RuntimeState,
    ) -> Result<(), String> {
        if self.count == 0 {
            return Err("quota input DOP must be positive".into());
        }
        if self.done.is_none() {
            let mut done = AggregateVec::new_in(allocator);
            done.try_reserve_exact(self.count)
                .map_err(|_| allocation_failure(state))?;
            done.resize(self.count, false);
            self.done = Some(done);
        }
        Ok(())
    }
    pub fn finish(&mut self, driver: usize) -> Result<bool, String> {
        let done = self
            .done
            .as_mut()
            .ok_or_else(|| "quota EOS owner has not been bound".to_string())?;
        let flag = done
            .get_mut(driver)
            .ok_or_else(|| "quota driver identity exceeds its frozen local DOP".to_string())?;
        if *flag {
            return Err("quota driver EOS was delivered twice".into());
        }
        *flag = true;
        Ok(done.iter().all(|flag| *flag))
    }
    pub fn complete(&self) -> bool {
        self.done
            .as_ref()
            .is_some_and(|done| done.iter().all(|flag| *flag))
    }
    pub fn clear(&mut self) {
        drop(self.done.take());
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::operators::ValuesSourceFactory;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::verification::TaskVerificationHolder;
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_types::{SlotId, identity::*};
    pub fn state() -> (
        Arc<RuntimeState>,
        Arc<MemTracker>,
        Arc<TaskVerificationHolder>,
    ) {
        let execution =
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap();
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let holder = Arc::new(TaskVerificationHolder::new(identity));
        let tracker = MemTracker::new_root("quota test task");
        let state = Arc::new(
            RuntimeState::new(None, None, None, None, None, Some(tracker.clone()), None)
                .with_verification(holder.clone()),
        );
        (state, tracker, holder)
    }
    pub fn schema(types: &[DataType]) -> ChunkSchemaRef {
        let schema = Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), false))
                .collect::<Vec<_>>(),
        );
        ChunkSchema::try_ref_from_schema_and_slot_ids(
            &schema,
            &(0..types.len())
                .map(|i| SlotId::new(i as u32 + 1))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }
    pub fn chunk(columns: Vec<ArrayRef>) -> Chunk {
        let schema = schema(
            &columns
                .iter()
                .map(|a| a.data_type().clone())
                .collect::<Vec<_>>(),
        );
        Chunk::try_new_with_columns(schema, columns).unwrap()
    }
    pub fn seeds(ids: &[&[u8]], counts: &[i64]) -> Chunk {
        chunk(vec![
            Arc::new(BinaryArray::from_iter_values(ids.iter().copied())),
            Arc::new(Int64Array::from(counts.to_vec())),
        ])
    }
    pub fn candidates(ids: &[&[u8]], files: &[&str], positions: &[i64]) -> Chunk {
        chunk(vec![
            Arc::new(BinaryArray::from_iter_values(ids.iter().copied())),
            Arc::new(StringArray::from(files.to_vec())),
            Arc::new(Int64Array::from(positions.to_vec())),
        ])
    }
    pub fn driver(
        chunk: Chunk,
        sink: &dyn OperatorFactory,
        dop: i32,
        index: i32,
        state: Arc<RuntimeState>,
    ) -> PipelineDriver {
        let mut operators = vec![
            ValuesSourceFactory::new(chunk, 100 + index).create(1, 0),
            sink.create(dop, index),
        ];
        for op in &mut operators {
            op.prepare().unwrap();
            op.bind_runtime_state(&state).unwrap();
        }
        PipelineDriver::new(index, operators, None, Vec::new(), state, None)
    }
    pub fn run(driver: &mut PipelineDriver) -> DriverState {
        for _ in 0..64 {
            let result = driver.process(std::time::Duration::from_millis(10));
            if matches!(
                result,
                DriverState::Finished | DriverState::Failed(_) | DriverState::Canceled
            ) {
                return result;
            }
        }
        panic!("quota driver did not reach its expected terminal state")
    }
    pub struct Collector {
        pub chunks: Arc<std::sync::Mutex<Vec<Chunk>>>,
        pub finished: bool,
    }
    impl Operator for Collector {
        fn name(&self) -> &str {
            "QuotaTestCollector"
        }
        fn is_finished(&self) -> bool {
            self.finished
        }
        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }
        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }
    impl ProcessorOperator for Collector {
        fn need_input(&self) -> bool {
            !self.finished
        }
        fn has_output(&self) -> bool {
            false
        }
        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.chunks.lock().unwrap().push(chunk);
            Ok(())
        }
        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }
        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }
    }

    #[test]
    fn quota_output_cannot_mint_proof_for_another_tasks_allocator() {
        let (first, first_tracker, _) = state();
        let (second, second_tracker, _) = state();
        let mut budget = Budget::new(1024 * 1024);
        let allocator = budget.bind(&second).unwrap();
        let mut rows = AggregateVec::new_in(allocator.clone());
        reserve(&mut rows, 1, &second).unwrap();
        rows.push(OutputRow {
            id: None,
            location: Location {
                file: bytes(b"file", allocator.clone(), &second).unwrap(),
                position: 1,
            },
        });
        assert!(
            output(
                schema(&[DataType::Utf8, DataType::Int64]),
                rows,
                false,
                allocator,
                &first
            )
            .is_err()
        );
        assert_eq!(first_tracker.current(), 0);
        assert_eq!(second_tracker.current(), 0);
    }

    #[test]
    fn quota_output_tracks_physical_arrow_owners_once_across_views() {
        let (state, tracker, _) = state();
        let mut budget = Budget::new(1024 * 1024);
        let allocator = budget.bind(&state).unwrap();
        let mut rows = AggregateVec::new_in(allocator.clone());
        reserve(&mut rows, 1, &state).unwrap();
        rows.push(OutputRow {
            id: None,
            location: Location {
                file: bytes(b"exact_file", allocator.clone(), &state).unwrap(),
                position: 3,
            },
        });
        let mut chunk = output(
            schema(&[DataType::Utf8, DataType::Int64]),
            rows,
            false,
            allocator,
            &state,
        )
        .unwrap();
        let charged = tracker.current();
        assert!(charged > 0);
        chunk.try_transfer_to(&tracker).unwrap();
        assert_eq!(tracker.current(), charged);
        let child = MemTracker::new_child("sink", &tracker);
        chunk.transfer_to(&child);
        assert_eq!(tracker.current(), charged);
        assert!(chunk.take_memory_lease().is_none());
        let clone = chunk.clone();
        let slice = chunk.slice(0, 1);
        let array = chunk.columns()[0].clone();
        drop(chunk);
        drop(clone);
        drop(slice);
        assert!(tracker.current() > 0);
        drop(array);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_output_rejects_unpaid_mixed_arrays_and_other_tasks() {
        let (state, tracker, _) = state();
        let mut budget = Budget::new(1024 * 1024);
        let allocator = budget.bind(&state).unwrap();
        let mut rows = AggregateVec::new_in(allocator.clone());
        reserve(&mut rows, 1, &state).unwrap();
        rows.push(OutputRow {
            id: None,
            location: Location {
                file: bytes(b"exact_file", allocator.clone(), &state).unwrap(),
                position: 3,
            },
        });
        let mut chunk = output(
            schema(&[DataType::Utf8, DataType::Int64]),
            rows,
            false,
            allocator,
            &state,
        )
        .unwrap();
        let foreign = MemTracker::new_root("foreign task");
        assert!(chunk.try_transfer_to(&foreign).is_err());
        assert_eq!(foreign.current(), 0);
        let mut evidence = AggregateVec::new_in(budget.allocator().unwrap());
        for column in chunk.columns() {
            for buffer in column.to_data().buffers() {
                reserve(&mut evidence, 1, &state).unwrap();
                evidence.push(buffer.clone());
            }
        }
        let proof = QuotaOutputMemory {
            task: tracker.clone(),
            buffers: evidence,
            error: state.error_state(),
        };
        proof.validate_columns(chunk.columns()).unwrap();
        assert!(
            proof
                .validate_columns(&[
                    chunk.columns()[0].clone(),
                    Arc::new(Int64Array::from(vec![3]))
                ])
                .is_err()
        );
        drop(proof);
        drop(chunk);
        assert_eq!(tracker.current(), 0);
    }
}
