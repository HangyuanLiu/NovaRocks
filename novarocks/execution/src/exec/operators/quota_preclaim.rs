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
use super::quota::*;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::agg::{
    AggregateHashMap, AggregateHashSet, AggregateVec, aggregate_hash_map, aggregate_hash_set,
};
use crate::exec::hash_table::content_key::ContentKeyEncoder;
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_local_program::QuotaPreclaimSpec;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A quota-specific producer owns its witness and lifecycle independently of joins.
pub(crate) trait QuotaContentFilterObserver: Send + Sync {
    fn bind_runtime(&self, state: &RuntimeState) -> Result<(), String>;
    fn activate(&self, state: &RuntimeState) -> Result<(), String>;
    fn observe(&self, chunk: &Chunk) -> Result<(), String>;
    fn complete(&self) -> Result<(), String>;
    fn fail(&self);
}
struct Entry {
    id: AggregateVec<u8>,
    key: AggregateVec<u8>,
    need: u64,
    claimed: AtomicU64,
    positions: AggregateVec<Location>,
}
struct Storage {
    ids: AggregateHashSet<OwnedBytes>,
    entries: AggregateHashMap<u64, AggregateVec<Entry>>,
}
struct Data {
    storage: Option<Storage>,
    encoder: Option<ContentKeyEncoder>,
    budget: Budget,
    eos: DriverEos,
    probe_eos: DriverEos,
    cancelled: bool,
}
pub(crate) struct QuotaPreclaimShared {
    spec: QuotaPreclaimSpec,
    domains: u32,
    data: Mutex<Data>,
    dep: DependencyHandle,
    hash_mask: u64,
    observer: Option<Arc<dyn QuotaContentFilterObserver>>,
}
impl QuotaPreclaimShared {
    pub fn new(
        node: i32,
        spec: QuotaPreclaimSpec,
        domains: u32,
        drivers: usize,
        probe_drivers: usize,
        manager: &DependencyManager,
        observer: Option<Arc<dyn QuotaContentFilterObserver>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(Data {
                storage: None,
                encoder: None,
                budget: Budget::new(spec.max_state_bytes),
                eos: DriverEos::new(drivers),
                probe_eos: DriverEos::new(probe_drivers),
                cancelled: false,
            }),
            spec,
            domains,
            dep: manager.get_or_create(format!("quota_preclaim_build:{node}")),
            hash_mask: u64::MAX,
            observer,
        })
    }
    fn bind_encoder(
        &self,
        state: &RuntimeState,
        schema: &ChunkSchemaRef,
    ) -> Result<ContentKeyEncoder, String> {
        let mut data = self.data.lock().unwrap();
        if let Some(encoder) = &data.encoder {
            return Ok(encoder.clone());
        }
        let allocator = data.budget.allocator()?;
        let mut types = AggregateVec::new_in(allocator);
        types
            .try_reserve_exact(self.spec.target_value_columns.len())
            .map_err(|_| allocation_failure(state))?;
        for column in &self.spec.target_value_columns {
            types.push(
                schema
                    .field(*column)
                    .ok_or_else(|| "quota content field is absent from target schema".to_string())?
                    .data_type()
                    .clone(),
            );
        }
        let encoder = ContentKeyEncoder::try_new_types_tracked(types).map_err(|error| {
            if state.error_state().task_failure().is_some() {
                allocation_failure(state)
            } else {
                execution(state, &error)
            }
        })?;
        data.encoder = Some(encoder.clone());
        Ok(encoder)
    }
    fn hash(&self, key: &[u8]) -> u64 {
        let mut h = std::hash::DefaultHasher::new();
        key.hash(&mut h);
        h.finish() & self.hash_mask
    }
    fn bind(&self, state: &RuntimeState) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        let allocator = data.budget.bind(state)?;
        data.eos.bind(allocator.clone(), state)?;
        data.probe_eos.bind(allocator.clone(), state)?;
        if data.storage.is_none() && !data.cancelled {
            data.storage = Some(Storage {
                ids: aggregate_hash_set(allocator.clone()),
                entries: aggregate_hash_map(allocator),
            });
        }
        drop(data);
        if let Some(observer) = &self.observer {
            observer.bind_runtime(state)?;
        }
        Ok(())
    }
    fn ingest(&self, state: &RuntimeState, chunk: &Chunk) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled || data.eos.complete() {
            return Err("quota preclaim demand arrived outside its input lifetime".into());
        }
        let allocator = data.budget.allocator()?;
        let storage = data
            .storage
            .as_mut()
            .ok_or_else(|| "quota preclaim has not been bound".to_string())?;
        for row in 0..chunk.len() {
            let id = binary(chunk, self.spec.demand_entry_id_column, row)?;
            let key = binary(chunk, self.spec.demand_key_column, row)?;
            let count = need(chunk, self.spec.demand_need, row, state)?;
            count
                .checked_mul(self.domains as u64)
                .ok_or_else(|| execution(state, "quota preselection candidate bound overflow"))?;
            if id.is_empty() || storage.ids.contains(id) {
                return Err(execution(
                    state,
                    "quota demand contains an empty or duplicate entry ID",
                ));
            }
            let hash = self.hash(key);
            if storage
                .entries
                .get(&hash)
                .is_some_and(|bucket| bucket.iter().any(|entry| entry.key.as_slice() == key))
            {
                return Err(execution(
                    state,
                    "quota demand repeats a complete content key",
                ));
            }
            storage
                .entries
                .try_reserve(1)
                .map_err(|_| allocation_failure(state))?;
            storage
                .ids
                .try_reserve(1)
                .map_err(|_| allocation_failure(state))?;
            let stored_id = OwnedBytes(bytes(id, allocator.clone(), state)?);
            let entry = Entry {
                id: bytes(id, allocator.clone(), state)?,
                key: bytes(key, allocator.clone(), state)?,
                need: count,
                claimed: AtomicU64::new(0),
                positions: AggregateVec::new_in(allocator.clone()),
            };
            let bucket = storage
                .entries
                .entry(hash)
                .or_insert_with(|| AggregateVec::new_in(allocator.clone()));
            reserve(bucket, 1, state)?;
            bucket.push(entry);
            storage.ids.insert(stored_id);
        }
        drop(data);
        if let Some(observer) = &self.observer {
            observer.observe(chunk)?;
        }
        Ok(())
    }
    fn claim(
        &self,
        state: &RuntimeState,
        key: &[u8],
        file: &str,
        position: i64,
    ) -> Result<Option<OutputRow>, String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled {
            return Err("quota preclaim was cancelled".into());
        }
        let hash = self.hash(key);
        let allocator = data.budget.allocator()?;
        let storage = data
            .storage
            .as_mut()
            .ok_or_else(|| "quota preclaim has not been bound".to_string())?;
        let Some(entry) = storage
            .entries
            .get_mut(&hash)
            .and_then(|bucket| bucket.iter_mut().find(|entry| entry.key.as_slice() == key))
        else {
            return Ok(None);
        };
        if entry.claimed.load(Ordering::Acquire) >= entry.need {
            return Ok(None);
        }
        if entry
            .positions
            .iter()
            .any(|l| l.file.as_slice() == file.as_bytes() && l.position == position)
        {
            return Err(execution(
                state,
                "quota preclaim repeated an exact target position",
            ));
        }
        reserve(&mut entry.positions, 1, state)?;
        let stored = Location {
            file: bytes(file.as_bytes(), allocator.clone(), state)?,
            position,
        };
        let output = OutputRow {
            id: Some(bytes(&entry.id, allocator.clone(), state)?),
            location: Location {
                file: bytes(file.as_bytes(), allocator, state)?,
                position,
            },
        };
        entry.positions.push(stored);
        entry.claimed.fetch_add(1, Ordering::AcqRel);
        Ok(Some(output))
    }
    fn finish(&self, driver: usize) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled {
            return Ok(());
        }
        if data.eos.finish(driver)? {
            if let Some(observer) = &self.observer {
                observer.complete()?;
            }
            self.dep.set_ready();
        }
        Ok(())
    }
    fn close_probe(&self, driver: usize) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled {
            return Ok(());
        }
        if data.probe_eos.finish(driver)? {
            drop(data.storage.take());
            drop(data.encoder.take());
            data.eos.clear();
            data.probe_eos.clear();
        }
        Ok(())
    }
    fn cancel(&self) {
        let mut data = self.data.lock().unwrap();
        data.cancelled = true;
        drop(data.storage.take());
        drop(data.encoder.take());
        data.eos.clear();
        data.probe_eos.clear();
        if let Some(observer) = &self.observer {
            observer.fail();
        }
    }
}
pub(crate) struct QuotaPreclaimBuildFactory {
    pub shared: Arc<QuotaPreclaimShared>,
}
impl OperatorFactory for QuotaPreclaimBuildFactory {
    fn name(&self) -> &str {
        "QuotaPreclaimBuild"
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn create(&self, _dop: i32, driver: i32) -> Box<dyn Operator> {
        Box::new(Build {
            shared: self.shared.clone(),
            driver: driver as usize,
            finished: false,
        })
    }
}
struct Build {
    shared: Arc<QuotaPreclaimShared>,
    driver: usize,
    finished: bool,
}
impl Operator for Build {
    fn name(&self) -> &str {
        "QuotaPreclaimBuild"
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.shared.bind(state)
    }
    fn activate(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(observer) = &self.shared.observer {
            observer.activate(state)?;
        }
        Ok(())
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn cancel(&mut self) {
        self.shared.cancel();
        self.finished = true;
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}
impl ProcessorOperator for Build {
    fn need_input(&self) -> bool {
        !self.finished
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Err("quota demand input already ended".into());
        }
        self.shared.ingest(state, &chunk)
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if !self.finished {
            self.shared.finish(self.driver)?;
            self.finished = true;
        }
        Ok(())
    }
}
pub(crate) struct QuotaPreclaimProbeFactory {
    pub shared: Arc<QuotaPreclaimShared>,
    pub target_schema: ChunkSchemaRef,
    pub output: ChunkSchemaRef,
}
impl OperatorFactory for QuotaPreclaimProbeFactory {
    fn name(&self) -> &str {
        "QuotaPreclaimProbe"
    }
    fn create(&self, _dop: i32, driver: i32) -> Box<dyn Operator> {
        Box::new(Probe {
            shared: self.shared.clone(),
            driver: driver as usize,
            closed: false,
            encoder: None,
            target_schema: self.target_schema.clone(),
            schema: self.output.clone(),
            pending: None,
            finishing: false,
        })
    }
}
struct Probe {
    driver: usize,
    closed: bool,
    shared: Arc<QuotaPreclaimShared>,
    encoder: Option<ContentKeyEncoder>,
    target_schema: ChunkSchemaRef,
    schema: ChunkSchemaRef,
    pending: Option<(Chunk, usize)>,
    finishing: bool,
}
impl Operator for Probe {
    fn close(&mut self) -> Result<(), String> {
        if !self.closed {
            drop(self.encoder.take());
            self.shared.close_probe(self.driver)?;
            self.closed = true;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "QuotaPreclaimProbe"
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.shared.bind(state)?;
        self.encoder = Some(self.shared.bind_encoder(state, &self.target_schema)?);
        Ok(())
    }
    fn is_finished(&self) -> bool {
        self.finishing && self.pending.is_none()
    }
    fn cancel(&mut self) {
        self.pending = None;
        drop(self.encoder.take());
        self.finishing = true;
        self.shared.cancel();
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}
impl ProcessorOperator for Probe {
    fn need_input(&self) -> bool {
        !self.finishing && self.pending.is_none() && self.shared.dep.is_ready()
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        Some(self.shared.dep.clone())
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("quota target arrived before demand EOS or while input is pending".into());
        }
        self.pending = Some((chunk, 0));
        Ok(())
    }
    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let Some((chunk, start)) = self.pending.take() else {
            return Ok(None);
        };
        let allocator = self.shared.data.lock().unwrap().budget.allocator()?;
        let mut columns = AggregateVec::new_in(allocator.clone());
        columns
            .try_reserve_exact(self.shared.spec.target_value_columns.len())
            .map_err(|_| allocation_failure(state))?;
        for column in &self.shared.spec.target_value_columns {
            columns.push(array(&chunk, *column)?.clone());
        }
        let encoder = self
            .encoder
            .as_ref()
            .ok_or_else(|| "quota content encoder has not been bound".to_string())?;
        let mut rows = AggregateVec::new_in(allocator.clone());
        let mut row = start;
        while row < chunk.len() && rows.len() < state.chunk_size() {
            let (file, position) = location(
                &chunk,
                self.shared.spec.target_file_column,
                self.shared.spec.target_position_column,
                row,
            )?;
            let len = encoder.encoded_row_len(&columns, row)?;
            let mut key = AggregateVec::new_in(allocator.clone());
            key.try_reserve_exact(len)
                .map_err(|_| allocation_failure(state))?;
            encoder.encode_row_to(&columns, row, &mut |part| {
                key.extend_from_slice(part);
                Ok(())
            })?;
            if let Some(candidate) = self.shared.claim(state, &key, file, position)? {
                reserve(&mut rows, 1, state)?;
                rows.push(candidate);
            }
            row += 1;
        }
        if row < chunk.len() {
            self.pending = Some((chunk, row));
        }
        if rows.is_empty() {
            Ok(None)
        } else {
            output(self.schema.clone(), rows, true, allocator, state).map(Some)
        }
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::quota::tests::*;
    use super::*;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use novarocks_local_program::{QuotaDomainId, QuotaNeed};
    fn spec(limit: usize) -> QuotaPreclaimSpec {
        QuotaPreclaimSpec {
            demand_entry_id_column: 0,
            demand_key_column: 1,
            demand_need: QuotaNeed::Count { column: 2 },
            target_value_columns: vec![0],
            target_file_column: 1,
            target_position_column: 2,
            preselection_domain: QuotaDomainId::try_new(5).unwrap(),
            max_state_bytes: limit,
        }
    }
    fn demands(values: &[f64], counts: &[i64], ids: &[&[u8]]) -> Chunk {
        let array = Arc::new(Float64Array::from(values.to_vec())) as ArrayRef;
        let encoder = ContentKeyEncoder::try_new_types(vec![DataType::Float64]).unwrap();
        let keys = (0..values.len())
            .map(|r| encoder.encode_row(&[array.clone()], r).unwrap())
            .collect::<Vec<_>>();
        chunk(vec![
            Arc::new(BinaryArray::from_iter_values(ids.iter().copied())),
            Arc::new(BinaryArray::from_iter_values(
                keys.iter().map(Vec::as_slice),
            )),
            Arc::new(Int64Array::from(counts.to_vec())),
        ])
    }
    #[test]
    fn quota_schema_encoder_is_charged_once_shared_by_drivers_and_physically_released() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let shared = QuotaPreclaimShared::new(5, spec(1 << 20), 1, 1, 3, &manager, None);
        shared.bind(&state).unwrap();
        let before = tracker.current();
        let target = schema(&[DataType::Float64, DataType::Utf8, DataType::Int64]);
        let first = shared.bind_encoder(&state, &target).unwrap();
        let charged = tracker.current();
        assert_eq!(
            charged - before,
            (first.encoded_prefix_len() + std::mem::size_of::<DataType>()) as i64
        );
        let second = shared.bind_encoder(&state, &target).unwrap();
        let cloned = first.clone();
        assert_eq!(tracker.current(), charged);
        shared.cancel();
        assert_eq!(tracker.current(), charged - before);
        drop(first);
        drop(second);
        assert_eq!(tracker.current(), charged - before);
        drop(cloned);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_schema_encoder_refuses_large_prefix_before_allocating_it() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let shared = QuotaPreclaimShared::new(5, spec(256), 1, 1, 1, &manager, None);
        shared.bind(&state).unwrap();
        let fields = (0..512)
            .map(|i| {
                Arc::new(arrow::datatypes::Field::new(
                    format!("f{i}"),
                    DataType::Int64,
                    false,
                ))
            })
            .collect::<Vec<_>>();
        let target = schema(&[
            DataType::Struct(fields.into()),
            DataType::Utf8,
            DataType::Int64,
        ]);
        assert!(shared.bind_encoder(&state, &target).is_err());
        assert!(matches!(
            state.error_state().task_failure().unwrap().category(),
            novarocks_execution_contract::TaskFailureCategory::CapacityRefused { .. }
        ));
        shared.cancel();
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn quota_preclaim_shared_drivers_gate_eos_and_compare_collision_bytes() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let mut shared = QuotaPreclaimShared::new(5, spec(1 << 20), 3, 2, 2, &manager, None);
        Arc::get_mut(&mut shared).unwrap().hash_mask = 0;
        let factory = QuotaPreclaimBuildFactory {
            shared: shared.clone(),
        };
        let mut first = driver(
            demands(&[0.0, -0.0], &[2, 1], &[b"positive", b"negative"]),
            &factory,
            2,
            0,
            state.clone(),
        );
        let mut second = driver(demands(&[], &[], &[]), &factory, 2, 1, state.clone());
        assert_eq!(run(&mut first), DriverState::Finished);
        assert!(!shared.dep.is_ready());
        assert_eq!(run(&mut second), DriverState::Finished);
        assert!(shared.dep.is_ready());
        let encoder = ContentKeyEncoder::try_new_types(vec![DataType::Float64]).unwrap();
        let array = Arc::new(Float64Array::from(vec![0., -0., 1.])) as ArrayRef;
        let key = |row| encoder.encode_row(&[array.clone()], row).unwrap();
        assert!(shared.claim(&state, &key(2), "wrong", 0).unwrap().is_none());
        assert_eq!(
            shared
                .claim(&state, &key(0), "a", 0)
                .unwrap()
                .unwrap()
                .id
                .unwrap()
                .as_slice(),
            b"positive"
        );
        assert_eq!(
            shared
                .claim(&state, &key(1), "a", 1)
                .unwrap()
                .unwrap()
                .id
                .unwrap()
                .as_slice(),
            b"negative"
        );
        assert!(shared.claim(&state, &key(1), "a", 2).unwrap().is_none());
        assert!(shared.claim(&state, &key(0), "b", 0).unwrap().is_some());
        assert!(shared.claim(&state, &key(0), "b", 1).unwrap().is_none());
        shared.cancel();
        drop(first);
        drop(second);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_preclaim_probe_runs_real_driver_and_retains_at_most_need() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let shared = QuotaPreclaimShared::new(5, spec(1 << 20), 1, 1, 1, &manager, None);
        let build = QuotaPreclaimBuildFactory {
            shared: shared.clone(),
        };
        let mut demand = driver(
            demands(&[7.], &[2], &[b"entry"]),
            &build,
            1,
            0,
            state.clone(),
        );
        assert_eq!(run(&mut demand), DriverState::Finished);
        let probe = QuotaPreclaimProbeFactory {
            shared: shared.clone(),
            target_schema: schema(&[DataType::Float64, DataType::Utf8, DataType::Int64]),
            output: schema(&[DataType::Binary, DataType::Utf8, DataType::Int64]),
        };
        let input = chunk(vec![
            Arc::new(Float64Array::from(vec![7.; 100])),
            Arc::new(StringArray::from(vec!["file"; 100])),
            Arc::new(Int64Array::from((0..100).collect::<Vec<_>>())),
        ]);
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let mut operators = vec![
            crate::exec::operators::ValuesSourceFactory::new(input, 10).create(1, 0),
            probe.create(1, 0),
            Box::new(Collector {
                chunks: chunks.clone(),
                finished: false,
            }) as Box<dyn Operator>,
        ];
        for op in &mut operators {
            op.bind_runtime_state(&state).unwrap();
        }
        let mut driver = PipelineDriver::new(0, operators, None, Vec::new(), state.clone(), None);
        assert_eq!(run(&mut driver), DriverState::Finished);
        assert_eq!(
            chunks.lock().unwrap().iter().map(Chunk::len).sum::<usize>(),
            2
        );
        drop(driver);
        drop(demand);
        chunks.lock().unwrap().clear();
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_preclaim_duplicate_ids_and_capacity_refusal_are_explicit() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let shared = QuotaPreclaimShared::new(5, spec(1 << 20), 1, 1, 1, &manager, None);
        shared.bind(&state).unwrap();
        assert!(
            shared
                .ingest(&state, &demands(&[0., 1.], &[1, 1], &[b"same", b"same"]))
                .is_err()
        );
        shared.cancel();
        assert_eq!(tracker.current(), 0);
        let (state, tracker, _) = super::super::quota::tests::state();
        let shared = QuotaPreclaimShared::new(5, spec(128), 1, 1, 1, &manager, None);
        let factory = QuotaPreclaimBuildFactory {
            shared: shared.clone(),
        };
        let mut driver = driver(
            demands(&[0.], &[1], &[b"entry"]),
            &factory,
            1,
            0,
            state.clone(),
        );
        assert!(matches!(run(&mut driver), DriverState::Failed(_)));
        assert!(matches!(
            state.error_state().task_failure().unwrap().category(),
            novarocks_execution_contract::TaskFailureCategory::CapacityRefused { .. }
        ));
        drop(driver);
        shared.cancel();
        assert_eq!(tracker.current(), 0);
    }
}
