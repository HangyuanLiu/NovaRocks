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
use crate::exec::expr::agg::{AggregateHashMap, AggregateVec, aggregate_hash_map};
use crate::exec::pipeline::dependency::{DependencyHandle, DependencyManager};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::verification::TaskVerificationHolder;
use novarocks_execution_contract::VerificationInstance;
use novarocks_local_program::QuotaTrimSpec;
use std::sync::{Arc, Mutex};

struct Entry {
    need: u64,
    seen: u64,
    heap: AggregateVec<Location>,
}
struct Storage {
    entries: AggregateHashMap<OwnedBytes, Entry>,
}
struct Data {
    storage: Option<Storage>,
    budget: Budget,
    seeds: DriverEos,
    candidates: DriverEos,
    holder: Option<Arc<TaskVerificationHolder>>,
    started: bool,
    verified: bool,
    cancelled: bool,
    drained: bool,
}
pub(crate) struct QuotaTrimShared {
    spec: QuotaTrimSpec,
    domains: u32,
    instance: VerificationInstance,
    data: Mutex<Data>,
    seed_dep: DependencyHandle,
    output_dep: DependencyHandle,
}
impl QuotaTrimShared {
    pub fn new(
        node: i32,
        spec: QuotaTrimSpec,
        domains: u32,
        seed_drivers: usize,
        candidate_drivers: usize,
        manager: &DependencyManager,
    ) -> Arc<Self> {
        Arc::new(Self {
            data: Mutex::new(Data {
                storage: None,
                budget: Budget::new(spec.max_state_bytes),
                seeds: DriverEos::new(seed_drivers),
                candidates: DriverEos::new(candidate_drivers),
                holder: None,
                started: false,
                verified: false,
                cancelled: false,
                drained: false,
            }),
            spec,
            domains,
            instance: VerificationInstance {
                plan_node_id: node,
                local_instance_id: 0,
            },
            seed_dep: manager.get_or_create(format!("quota_trim_seeds:{node}")),
            output_dep: manager.get_or_create(format!("quota_trim_verified:{node}")),
        })
    }
    fn bind(&self, state: &RuntimeState) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        let allocator = data.budget.bind(state)?;
        let holder = state
            .verification()
            .ok_or_else(|| "QuotaTrim requires exact task verification facts".to_string())?;
        if let Some(previous) = &data.holder {
            if !Arc::ptr_eq(previous, holder) {
                return Err("QuotaTrim cannot cross task verification holders".into());
            }
        } else {
            holder.register(self.instance)?;
            data.holder = Some(holder.clone());
        }
        data.seeds.bind(allocator.clone(), state)?;
        data.candidates.bind(allocator.clone(), state)?;
        if data.storage.is_none() && !data.cancelled && !data.drained {
            data.storage = Some(Storage {
                entries: aggregate_hash_map(allocator),
            });
        }
        Ok(())
    }
    fn activate(&self) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if !data.started {
            data.holder
                .as_ref()
                .ok_or_else(|| "QuotaTrim was not registered during preparation".to_string())?
                .start(self.instance)?;
            data.started = true;
        }
        Ok(())
    }
    fn seeds(&self, state: &RuntimeState, chunk: &Chunk) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled || data.seeds.complete() {
            return Err("quota seeds arrived outside their input lifetime".into());
        }
        let allocator = data.budget.allocator()?;
        let storage = data
            .storage
            .as_mut()
            .ok_or_else(|| "quota trim has not been bound".to_string())?;
        for row in 0..chunk.len() {
            let id = binary(chunk, self.spec.seed_entry_id_column, row)?;
            let count = need(chunk, self.spec.seed_need, row, state)?;
            count
                .checked_mul(self.domains as u64)
                .ok_or_else(|| execution(state, "quota candidate bound overflow"))?;
            if id.is_empty() || storage.entries.contains_key(id) {
                return Err(execution(
                    state,
                    "quota seeds contain an empty or duplicate entry ID",
                ));
            }
            storage
                .entries
                .try_reserve(1)
                .map_err(|_| allocation_failure(state))?;
            storage.entries.insert(
                OwnedBytes(bytes(id, allocator.clone(), state)?),
                Entry {
                    need: count,
                    seen: 0,
                    heap: AggregateVec::new_in(allocator.clone()),
                },
            );
        }
        Ok(())
    }
    fn candidates(&self, state: &RuntimeState, chunk: &Chunk) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled || !data.seeds.complete() || data.candidates.complete() {
            return Err("quota candidates arrived before seed EOS or after candidate EOS".into());
        }
        let allocator = data.budget.allocator()?;
        let storage = data
            .storage
            .as_mut()
            .ok_or_else(|| "quota trim has not been bound".to_string())?;
        for row in 0..chunk.len() {
            let id = binary(chunk, self.spec.candidate_entry_id_column, row)?;
            let (file, position) = location(
                chunk,
                self.spec.candidate_file_column,
                self.spec.candidate_position_column,
                row,
            )?;
            let entry = storage.entries.get_mut(id).ok_or_else(|| {
                execution(state, "quota candidate references an unknown entry ID")
            })?;
            entry.seen = entry
                .seen
                .checked_add(1)
                .ok_or_else(|| execution(state, "quota candidate count overflow"))?;
            if entry.seen
                > entry
                    .need
                    .checked_mul(self.domains as u64)
                    .ok_or_else(|| execution(state, "quota candidate bound overflow"))?
            {
                return Err(execution(
                    state,
                    "quota candidates exceed the exact preselection domain bound",
                ));
            }
            if entry
                .heap
                .iter()
                .any(|l| l.file.as_slice() == file.as_bytes() && l.position == position)
            {
                return Err(execution(
                    state,
                    "quota candidates repeat an exact target position",
                ));
            }
            let full = entry.heap.len() as u64 == entry.need;
            if full {
                let largest = &entry.heap[0];
                if (file.as_bytes(), position) >= (largest.file.as_slice(), largest.position) {
                    continue;
                }
            }
            let value = Location {
                file: bytes(file.as_bytes(), allocator.clone(), state)?,
                position,
            };
            if full {
                entry.heap[0] = value;
                sift_down(&mut entry.heap, 0);
            } else {
                reserve(&mut entry.heap, 1, state)?;
                entry.heap.push(value);
                let last = entry.heap.len() - 1;
                sift_up(&mut entry.heap, last);
            }
        }
        Ok(())
    }
    fn finish_seeds(&self, state: &RuntimeState, driver: usize) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled {
            return Ok(());
        }
        if data.seeds.finish(driver)? {
            self.seed_dep.set_ready();
        }
        self.verify(state, &mut data)
    }
    fn finish_candidates(&self, state: &RuntimeState, driver: usize) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled {
            return Ok(());
        }
        data.candidates.finish(driver)?;
        self.verify(state, &mut data)
    }
    fn verify(&self, state: &RuntimeState, data: &mut Data) -> Result<(), String> {
        if data.verified || !data.seeds.complete() || !data.candidates.complete() {
            return Ok(());
        }
        if !data.started {
            return Err("quota verification cannot complete before activation".into());
        }
        let storage = data
            .storage
            .as_mut()
            .ok_or_else(|| "quota trim has not been bound".to_string())?;
        let mut requested = 0u64;
        let mut matched = 0u64;
        let mut deficient = false;
        for entry in storage.entries.values() {
            requested = requested
                .checked_add(entry.need)
                .ok_or_else(|| execution(state, "quota total requested count overflow"))?;
            matched = matched
                .checked_add(entry.heap.len() as u64)
                .ok_or_else(|| execution(state, "quota total matched count overflow"))?;
            deficient |= entry.heap.len() as u64 != entry.need;
        }
        // Both exact input frontiers have closed. Preserve the complete
        // conclusion even when a deficit prevents any target output.
        data.holder
            .as_ref()
            .ok_or_else(|| "quota verification holder disappeared".to_string())?
            .complete(self.instance, requested, matched)?;
        if deficient {
            // This Task has genuinely consumed both complete frontiers. Close
            // without locators so peer Tasks can also reach their own EOS and
            // release facts. FE decides the statement's consistency failure
            // from the complete exact receipt before any external commit.
            drop(data.storage.take());
            data.verified = true;
            data.drained = true;
            data.seeds.clear();
            data.candidates.clear();
            self.output_dep.set_ready();
            return Ok(());
        }
        // Descending storage lets the one output driver pop exact stable ascending
        // (file,position) selections without allocating another result relation.
        for entry in storage.entries.values_mut() {
            entry.heap.sort_unstable_by(|a, b| b.cmp(a));
        }
        data.verified = true;
        self.output_dep.set_ready();
        Ok(())
    }
    fn pull(&self, state: &RuntimeState, schema: ChunkSchemaRef) -> Result<Option<Chunk>, String> {
        let mut data = self.data.lock().unwrap();
        if data.cancelled || data.drained {
            return Ok(None);
        }
        if !data.verified {
            return Err("quota output cannot precede complete verification".into());
        }
        let allocator = data.budget.allocator()?;
        let mut rows = AggregateVec::new_in(allocator.clone());
        if let Some(storage) = data.storage.as_mut() {
            while rows.len() < state.chunk_size() {
                let Some(entry) = storage
                    .entries
                    .values_mut()
                    .find(|entry| !entry.heap.is_empty())
                else {
                    break;
                };
                reserve(&mut rows, 1, state)?;
                rows.push(OutputRow {
                    id: None,
                    location: entry.heap.pop().unwrap(),
                });
            }
        }
        if rows.is_empty() {
            drop(data.storage.take());
            data.drained = true;
            data.seeds.clear();
            data.candidates.clear();
            return Ok(None);
        }
        drop(data);
        output(schema, rows, false, allocator, state).map(Some)
    }
    fn cancel(&self) {
        let mut data = self.data.lock().unwrap();
        data.cancelled = true;
        drop(data.storage.take());
        data.seeds.clear();
        data.candidates.clear();
        data.drained = true;
    }
    fn done(&self) -> bool {
        let data = self.data.lock().unwrap();
        data.cancelled || data.drained
    }
}
fn sift_up(heap: &mut [Location], mut index: usize) {
    while index > 0 {
        let parent = (index - 1) / 2;
        if heap[parent] >= heap[index] {
            break;
        }
        heap.swap(parent, index);
        index = parent;
    }
}
fn sift_down(heap: &mut [Location], mut index: usize) {
    loop {
        let left = index * 2 + 1;
        if left >= heap.len() {
            break;
        }
        let right = left + 1;
        let larger = if right < heap.len() && heap[right] > heap[left] {
            right
        } else {
            left
        };
        if heap[index] >= heap[larger] {
            break;
        }
        heap.swap(index, larger);
        index = larger;
    }
}

pub(crate) struct QuotaTrimInputFactory {
    pub shared: Arc<QuotaTrimShared>,
    pub seeds: bool,
}
impl OperatorFactory for QuotaTrimInputFactory {
    fn name(&self) -> &str {
        if self.seeds {
            "QuotaTrimSeeds"
        } else {
            "QuotaTrimCandidates"
        }
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn create(&self, _dop: i32, driver: i32) -> Box<dyn Operator> {
        Box::new(Input {
            shared: self.shared.clone(),
            seeds: self.seeds,
            driver: driver as usize,
            finished: false,
        })
    }
}
struct Input {
    shared: Arc<QuotaTrimShared>,
    seeds: bool,
    driver: usize,
    finished: bool,
}
impl Operator for Input {
    fn name(&self) -> &str {
        if self.seeds {
            "QuotaTrimSeeds"
        } else {
            "QuotaTrimCandidates"
        }
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.shared.bind(state)
    }
    fn activate(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.shared.activate()
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
impl ProcessorOperator for Input {
    fn need_input(&self) -> bool {
        !self.finished && (self.seeds || self.shared.seed_dep.is_ready())
    }
    fn has_output(&self) -> bool {
        false
    }
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        if self.seeds {
            None
        } else {
            Some(self.shared.seed_dep.clone())
        }
    }
    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("quota trim input cannot currently be accepted".into());
        }
        if self.seeds {
            self.shared.seeds(state, &chunk)
        } else {
            self.shared.candidates(state, &chunk)
        }
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }
    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if !self.finished {
            if self.seeds {
                self.shared.finish_seeds(state, self.driver)?;
            } else {
                self.shared.finish_candidates(state, self.driver)?;
            }
            self.finished = true;
        }
        Ok(())
    }
}
pub(crate) struct QuotaTrimSourceFactory {
    pub shared: Arc<QuotaTrimShared>,
    pub output: ChunkSchemaRef,
}
impl OperatorFactory for QuotaTrimSourceFactory {
    fn name(&self) -> &str {
        "QuotaTrimSource"
    }
    fn is_source(&self) -> bool {
        true
    }
    fn create(&self, _dop: i32, _driver: i32) -> Box<dyn Operator> {
        Box::new(Source {
            shared: self.shared.clone(),
            output: self.output.clone(),
        })
    }
}
struct Source {
    shared: Arc<QuotaTrimShared>,
    output: ChunkSchemaRef,
}
impl Operator for Source {
    fn name(&self) -> &str {
        "QuotaTrimSource"
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.shared.bind(state)
    }
    fn activate(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.shared.activate()
    }
    fn is_finished(&self) -> bool {
        self.shared.done()
    }
    fn cancel(&mut self) {
        self.shared.cancel();
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}
impl ProcessorOperator for Source {
    fn need_input(&self) -> bool {
        false
    }
    fn has_output(&self) -> bool {
        self.shared.output_dep.is_ready() && !self.shared.done()
    }
    fn precondition_dependency(&self) -> Option<DependencyHandle> {
        Some(self.shared.output_dep.clone())
    }
    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("quota trim source cannot accept input".into())
    }
    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        self.shared.pull(state, self.output.clone())
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::quota::tests::*;
    use super::*;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use novarocks_execution_contract::{
        TaskFailureCategory, TaskVerificationObservation, VerificationState,
    };
    use novarocks_local_program::{QuotaDomainId, QuotaNeed};
    fn spec(limit: usize) -> QuotaTrimSpec {
        QuotaTrimSpec {
            seed_entry_id_column: 0,
            seed_need: QuotaNeed::Count { column: 1 },
            candidate_entry_id_column: 0,
            candidate_file_column: 1,
            candidate_position_column: 2,
            preselection_domain: QuotaDomainId::try_new(5).unwrap(),
            max_state_bytes: limit,
        }
    }
    #[test]
    fn quota_trim_real_drivers_wait_all_eos_then_emit_stable_exact_need() {
        let (state, tracker, holder) = state();
        let manager = DependencyManager::new();
        let shared = QuotaTrimShared::new(9, spec(1 << 20), 3, 1, 3, &manager);
        let seed_factory = QuotaTrimInputFactory {
            shared: shared.clone(),
            seeds: true,
        };
        let candidate_factory = QuotaTrimInputFactory {
            shared: shared.clone(),
            seeds: false,
        };
        let mut seed = driver(seeds(&[b"id"], &[2]), &seed_factory, 1, 0, state.clone());
        let mut first = driver(
            candidates(&[b"id", b"id"], &["z", "b"], &[0, 2]),
            &candidate_factory,
            3,
            0,
            state.clone(),
        );
        let mut second = driver(
            candidates(&[b"id", b"id"], &["c", "a"], &[0, 4]),
            &candidate_factory,
            3,
            1,
            state.clone(),
        );
        let mut last = driver(
            candidates(&[], &[], &[]),
            &candidate_factory,
            3,
            2,
            state.clone(),
        );
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let source_factory = QuotaTrimSourceFactory {
            shared: shared.clone(),
            output: schema(&[DataType::Utf8, DataType::Int64]),
        };
        let mut operators = vec![
            source_factory.create(1, 0),
            Box::new(Collector {
                chunks: chunks.clone(),
                finished: false,
            }) as Box<dyn Operator>,
        ];
        for op in &mut operators {
            op.bind_runtime_state(&state).unwrap();
        }
        let mut output_driver =
            PipelineDriver::new(10, operators, None, Vec::new(), state.clone(), None);
        assert!(matches!(
            first.process(std::time::Duration::from_millis(10)),
            DriverState::Blocked(_)
        ));
        assert!(!shared.seed_dep.is_ready());
        assert_eq!(run(&mut seed), DriverState::Finished);
        assert_eq!(run(&mut first), DriverState::Finished);
        assert_eq!(run(&mut second), DriverState::Finished);
        assert!(!shared.output_dep.is_ready());
        assert_eq!(run(&mut last), DriverState::Finished);
        assert!(shared.output_dep.is_ready());
        assert_eq!(run(&mut output_driver), DriverState::Finished);
        let got = chunks
            .lock()
            .unwrap()
            .iter()
            .flat_map(|chunk| {
                let files = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let positions = chunk.columns()[1]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..chunk.len())
                    .map(|r| (files.value(r).to_string(), positions.value(r)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(got, vec![("a".into(), 4), ("b".into(), 2)]);
        assert!(
            matches!(holder.seal().observation,TaskVerificationObservation::Available(records) if records.len()==1&&records[0].state==VerificationState::Completed{requested:2,matched:2})
        );
        drop(seed);
        drop(first);
        drop(second);
        drop(last);
        drop(output_driver);
        chunks.lock().unwrap().clear();
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_trim_real_driver_shortfall_and_all_missing_close_without_any_output() {
        for matched in [0usize, 1] {
            let (state, tracker, holder) = state();
            let manager = DependencyManager::new();
            let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
            let seed_factory = QuotaTrimInputFactory {
                shared: shared.clone(),
                seeds: true,
            };
            let candidates_factory = QuotaTrimInputFactory {
                shared: shared.clone(),
                seeds: false,
            };
            let mut seed = driver(seeds(&[b"id"], &[2]), &seed_factory, 1, 0, state.clone());
            let input = if matched == 0 {
                candidates(&[], &[], &[])
            } else {
                candidates(&[b"id"], &["file"], &[0])
            };
            let mut candidates = driver(input, &candidates_factory, 1, 0, state.clone());
            let chunks = Arc::new(Mutex::new(Vec::new()));
            let source_factory = QuotaTrimSourceFactory {
                shared: shared.clone(),
                output: schema(&[DataType::Utf8, DataType::Int64]),
            };
            let mut operators = vec![
                source_factory.create(1, 0),
                Box::new(Collector {
                    chunks: chunks.clone(),
                    finished: false,
                }) as Box<dyn Operator>,
            ];
            for op in &mut operators {
                op.bind_runtime_state(&state).unwrap();
            }
            let mut output =
                PipelineDriver::new(10, operators, None, Vec::new(), state.clone(), None);
            assert_eq!(run(&mut seed), DriverState::Finished);
            assert_eq!(run(&mut candidates), DriverState::Finished);
            assert!(shared.output_dep.is_ready());
            assert_eq!(run(&mut output), DriverState::Finished);
            assert!(chunks.lock().unwrap().is_empty());
            assert!(state.error_state().task_failure().is_none());
            assert!(
                matches!(holder.seal().observation,TaskVerificationObservation::Available(records) if records[0].state==VerificationState::Completed{requested:2,matched:matched as u64})
            );
            drop(seed);
            drop(candidates);
            drop(output);
            assert_eq!(tracker.current(), 0);
        }
    }
    #[test]
    fn quota_trim_three_domains_reach_real_eos_after_one_complete_deficit() {
        // Finish the deficient domain first. Its runtime must not fail and
        // prevent the other domains from producing their genuine conclusions.
        let mut domains = Vec::new();
        for (domain, matched) in [0usize, 2, 2].into_iter().enumerate() {
            let (state, tracker, holder) = state();
            let manager = DependencyManager::new();
            let shared = QuotaTrimShared::new(9, spec(1 << 20), 3, 1, 1, &manager);
            let seed_factory = QuotaTrimInputFactory {
                shared: shared.clone(),
                seeds: true,
            };
            let candidates_factory = QuotaTrimInputFactory {
                shared: shared.clone(),
                seeds: false,
            };
            let seed = driver(
                seeds(&[b"id", b"complete"], &[2, 1]),
                &seed_factory,
                1,
                0,
                state.clone(),
            );
            let input = if matched == 0 {
                candidates(&[b"complete"], &["file"], &[2])
            } else {
                candidates(
                    &[b"id", b"id", b"complete"],
                    &["file", "file", "file"],
                    &[0, 1, 2],
                )
            };
            let candidate = driver(input, &candidates_factory, 1, 0, state.clone());
            let chunks = Arc::new(Mutex::new(Vec::new()));
            let source_factory = QuotaTrimSourceFactory {
                shared: shared.clone(),
                output: schema(&[DataType::Utf8, DataType::Int64]),
            };
            let mut operators = vec![
                source_factory.create(1, 0),
                Box::new(Collector {
                    chunks: chunks.clone(),
                    finished: false,
                }) as Box<dyn Operator>,
            ];
            for op in &mut operators {
                op.bind_runtime_state(&state).unwrap();
            }
            let output = PipelineDriver::new(10, operators, None, Vec::new(), state.clone(), None);
            domains.push((
                domain, matched, state, tracker, holder, seed, candidate, output, chunks,
            ));
        }
        // Every domain is installed before the first one discovers a deficit.
        for (
            domain,
            matched,
            state,
            tracker,
            holder,
            mut seed,
            mut candidate,
            mut output,
            chunks,
        ) in domains
        {
            assert_eq!(run(&mut seed), DriverState::Finished, "domain {domain}");
            assert_eq!(
                run(&mut candidate),
                DriverState::Finished,
                "domain {domain}"
            );
            assert_eq!(run(&mut output), DriverState::Finished, "domain {domain}");
            assert!(state.error_state().task_failure().is_none());
            assert!(matches!(holder.seal().observation,
                TaskVerificationObservation::Available(records)
                if records[0].state == VerificationState::Completed { requested: 3, matched: matched as u64 + 1 }));
            let rows = chunks.lock().unwrap().iter().map(Chunk::len).sum::<usize>();
            assert_eq!(rows, if domain == 0 { 0 } else { 3 });
            drop(seed);
            drop(candidate);
            drop(output);
            chunks.lock().unwrap().clear();
            assert_eq!(tracker.current(), 0);
        }
    }
    #[test]
    fn quota_trim_rejects_duplicates_unknown_entries_and_exact_domain_overrun() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
        shared.bind(&state).unwrap();
        shared.activate().unwrap();
        assert!(
            shared
                .seeds(&state, &seeds(&[b"same", b"same"], &[1, 1]))
                .is_err()
        );
        shared.cancel();
        assert_eq!(tracker.current(), 0);
        let (state, tracker, _) = super::super::quota::tests::state();
        let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
        shared.bind(&state).unwrap();
        shared.activate().unwrap();
        shared.seeds(&state, &seeds(&[b"id"], &[1])).unwrap();
        shared.finish_seeds(&state, 0).unwrap();
        assert!(
            shared
                .candidates(&state, &candidates(&[b"unknown"], &["file"], &[0]))
                .is_err()
        );
        shared.cancel();
        assert_eq!(tracker.current(), 0);
        let (state, tracker, _) = super::super::quota::tests::state();
        let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
        shared.bind(&state).unwrap();
        shared.activate().unwrap();
        shared.seeds(&state, &seeds(&[b"id"], &[1])).unwrap();
        shared.finish_seeds(&state, 0).unwrap();
        assert!(
            shared
                .candidates(&state, &candidates(&[b"id", b"id"], &["a", "z"], &[0, 0]))
                .is_err()
        );
        shared.cancel();
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_trim_checked_negative_weight_and_capacity_failures_retain_typed_causes() {
        let (state, tracker, _) = state();
        let manager = DependencyManager::new();
        let mut negative = spec(1 << 20);
        negative.seed_need = QuotaNeed::NegativeWeight { column: 1 };
        let shared = QuotaTrimShared::new(9, negative, 1, 1, 1, &manager);
        let factory = QuotaTrimInputFactory {
            shared: shared.clone(),
            seeds: true,
        };
        let mut driver = driver(seeds(&[b"id"], &[i64::MIN]), &factory, 1, 0, state.clone());
        assert!(matches!(run(&mut driver), DriverState::Failed(_)));
        assert!(matches!(
            state.error_state().task_failure().unwrap().category(),
            TaskFailureCategory::Execution
        ));
        drop(driver);
        shared.cancel();
        assert_eq!(tracker.current(), 0);
        let (state, tracker, _) = super::super::quota::tests::state();
        tracker.install_limit_once(64).unwrap();
        let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
        let factory = QuotaTrimInputFactory {
            shared: shared.clone(),
            seeds: true,
        };
        let mut driver = super::super::quota::tests::driver(
            seeds(&[b"id"], &[1]),
            &factory,
            1,
            0,
            state.clone(),
        );
        assert!(matches!(run(&mut driver), DriverState::Failed(_)));
        assert!(matches!(
            state.error_state().task_failure().unwrap().category(),
            TaskFailureCategory::CapacityRefused { .. }
        ));
        drop(driver);
        shared.cancel();
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn quota_trim_cancellation_preserves_started_or_prestart_facts_and_frees_state() {
        for started in [false, true] {
            let (state, tracker, holder) = state();
            let manager = DependencyManager::new();
            let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
            shared.bind(&state).unwrap();
            if started {
                shared.activate().unwrap();
                shared.seeds(&state, &seeds(&[b"id"], &[1])).unwrap();
                assert!(tracker.current() > 0);
            }
            shared.cancel();
            assert_eq!(tracker.current(), 0);
            let expected = if started {
                VerificationState::Started
            } else {
                VerificationState::NotStarted
            };
            assert!(
                matches!(holder.seal().observation,TaskVerificationObservation::Available(records) if records[0].state==expected)
            );
        }
    }

    #[test]
    fn quota_trim_deficit_cannot_complete_before_every_seed_and_candidate_eos() {
        for seeds_closed in [false, true] {
            let (state, tracker, holder) = state();
            let manager = DependencyManager::new();
            let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 2, 2, &manager);
            shared.bind(&state).unwrap();
            shared.activate().unwrap();
            shared.seeds(&state, &seeds(&[b"id"], &[2])).unwrap();
            shared.finish_seeds(&state, 0).unwrap();
            shared.finish_candidates(&state, 0).unwrap();
            if seeds_closed {
                shared.finish_seeds(&state, 1).unwrap();
            } else {
                shared.finish_candidates(&state, 1).unwrap();
            }
            assert!(!shared.output_dep.is_ready());
            assert!(state.error_state().task_failure().is_none());
            assert!(
                matches!(holder.seal().observation, TaskVerificationObservation::Available(records)
                if records[0].state == VerificationState::Started)
            );
            shared.cancel();
            assert_eq!(tracker.current(), 0);
        }
    }
    #[test]
    fn quota_trim_missing_verification_holder_is_rejected_during_prepare() {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("missing fact task");
        let state = RuntimeState::new(None, None, None, None, None, Some(tracker), None);
        let manager = DependencyManager::new();
        let shared = QuotaTrimShared::new(9, spec(1 << 20), 1, 1, 1, &manager);
        assert!(shared.bind(&state).unwrap_err().contains("verification"));
    }
}
