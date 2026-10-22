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

//! Shared state foundation for DISTINCT set-operation execution.
//!
//! Responsibilities:
//! - Maintains hash-based row membership and stage coordination for multi-input set operations.
//! - Applies operation-specific probe/update/output semantics through `DistinctSetSemantics`.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSharedState`, `DistinctSetSemantics`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::hash_table::key_builder::build_group_key_views;
use crate::exec::hash_table::key_table::KeyTable;
use crate::runtime::mem_tracker::MemTracker;

use super::failure::{SetStateFailure, fail};
use super::set_op_stage::SetOpStageController;
use crate::runtime::fragment::ExecutionResult;

pub trait DistinctSetSemantics: Clone + Send + Sync + 'static {
    type Marker: Clone + Send + Sync + 'static;

    const NODE_NAME: &'static str;
    const SINK_OPERATOR_NAME: &'static str;
    const SOURCE_OPERATOR_NAME: &'static str;
    const SOURCE_REJECT_INPUT_ERROR: &'static str;
    const SOURCE_SLOT_MISMATCH_PREFIX: &'static str;

    fn new_marker() -> Self::Marker;
    fn apply_probe(marker: &mut Self::Marker, stage: usize);
    fn marker_selected(marker: &Self::Marker, stage_total: usize) -> bool;
}

struct DistinctSetInner<M> {
    key_table: Option<KeyTable>,
    markers: Vec<M>,
}

#[derive(Default)]
pub(crate) struct SetSelectionCursor {
    next_group: usize,
    indices: Vec<u64>,
}
impl SetSelectionCursor {
    pub(crate) fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
    pub(crate) fn take_indices(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.indices)
    }
}
pub(crate) enum SetSelectionProgress {
    Ready { exhausted: bool },
    Yield,
}

#[derive(Clone)]
pub(crate) struct DistinctSetSharedState<S: DistinctSetSemantics> {
    controller: SetOpStageController,
    output_chunk_schema: ChunkSchemaRef,
    inner: Arc<Mutex<DistinctSetInner<S::Marker>>>,
    _semantics: PhantomData<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSharedState<S> {
    #[cfg(test)]
    pub(crate) fn new(
        controller: SetOpStageController,
        output_chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            controller,
            output_chunk_schema,
            inner: Arc::new(Mutex::new(DistinctSetInner {
                key_table: None,
                markers: Vec::new(),
            })),
            _semantics: PhantomData,
        }
    }

    pub(crate) fn controller(&self) -> &SetOpStageController {
        &self.controller
    }

    pub(crate) fn output_chunk_schema(&self) -> ChunkSchemaRef {
        Arc::clone(&self.output_chunk_schema)
    }

    pub(crate) fn ingest_build(
        &self,
        chunk: &Chunk,
        tracker: Arc<MemTracker>,
    ) -> ExecutionResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let arrays = chunk.columns().to_vec();
        if arrays.is_empty() {
            return Err(fail(SetStateFailure::ZeroColumns { node: S::NODE_NAME }));
        }
        let key_views = build_group_key_views(&arrays)?;
        let num_rows = chunk.len();

        let mut guard = self.inner.lock().expect("distinct set state lock");
        if guard.key_table.is_none() {
            let key_types = arrays
                .iter()
                .map(|a| a.data_type().clone())
                .collect::<Vec<_>>();
            guard.key_table = Some(KeyTable::new_with_tracker(
                key_types,
                false,
                MemTracker::new_child("DistinctSetKeyTable", &tracker),
            )?);
        }
        let (rows, hashes) = {
            let table = guard.key_table.as_mut().expect("key table");
            let rows = table.build_rows_fallback(&arrays)?;
            let hashes = table.build_group_hashes(&key_views, num_rows)?;
            (rows, hashes)
        };

        for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
            let row_bytes = rows
                .get(row)
                .map(|row| row.as_slice())
                .ok_or_else(|| fail(SetStateFailure::MissingRow { build: true, row }))?;
            let lookup = {
                let table = guard.key_table.as_mut().expect("key table");
                table.find_or_insert_from_row(&key_views, row, row_bytes, hash)?
            };
            if lookup.is_new {
                if lookup.group_id != guard.markers.len() {
                    return Err(fail(SetStateFailure::GroupSequence {
                        node: S::NODE_NAME,
                        expected: guard.markers.len(),
                        actual: lookup.group_id,
                    }));
                }
                guard.markers.push(S::new_marker());
            }
        }
        Ok(())
    }

    pub(crate) fn ingest_probe(&self, stage: usize, chunk: &Chunk) -> ExecutionResult<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let arrays = chunk.columns().to_vec();
        if arrays.is_empty() {
            return Err(fail(SetStateFailure::ZeroColumns { node: S::NODE_NAME }));
        }
        let key_views = build_group_key_views(&arrays)?;
        let num_rows = chunk.len();

        let mut guard = self.inner.lock().expect("distinct set state lock");
        if guard.key_table.is_none() {
            return Ok(());
        };
        let (rows, hashes) = {
            let table = guard.key_table.as_ref().expect("key table");
            let rows = table.build_rows_fallback(&arrays)?;
            let hashes = table.build_group_hashes(&key_views, num_rows)?;
            (rows, hashes)
        };

        for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
            let row_bytes = rows
                .get(row)
                .map(|row| row.as_slice())
                .ok_or_else(|| fail(SetStateFailure::MissingRow { build: false, row }))?;
            let group_id_opt = {
                let table = guard.key_table.as_ref().expect("key table");
                table.lookup_serialized(row_bytes, hash)?
            };
            let Some(group_id) = group_id_opt else {
                continue;
            };
            let markers = guard.markers.len();
            let marker = guard.markers.get_mut(group_id).ok_or_else(|| {
                fail(SetStateFailure::GroupBounds {
                    node: S::NODE_NAME,
                    group: group_id,
                    markers,
                })
            })?;
            S::apply_probe(marker, stage);
        }
        Ok(())
    }

    pub(crate) fn finish_stage(
        &self,
        stage: usize,
    ) -> crate::runtime::fragment::ExecutionResult<()> {
        self.controller
            .producer_finished(stage)
            .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)
    }

    pub(crate) fn snapshot_arrays(&self) -> ExecutionResult<Vec<ArrayRef>> {
        let guard = self.inner.lock().expect("distinct set state lock");
        let Some(table) = guard.key_table.as_ref() else {
            return Ok(Vec::new());
        };
        if guard.markers.is_empty() {
            return Ok(Vec::new());
        }
        table.key_columns().iter().map(|c| c.to_array()).collect()
    }

    /// Retain at most one configured output batch of indices. A CPU yield
    /// preserves both the group cursor and its partial batch on the source.
    pub(crate) fn advance_selection(
        &self,
        cursor: &mut SetSelectionCursor,
        rows: std::num::NonZeroUsize,
        mut interval: impl FnMut() -> crate::runtime::fragment::ExecutionResult<bool>,
    ) -> crate::runtime::fragment::ExecutionResult<SetSelectionProgress> {
        let stage_total = self.controller.stage_total();
        let guard = self.inner.lock().expect("distinct set state lock");
        while cursor.next_group < guard.markers.len() && cursor.indices.len() < rows.get() {
            if !interval()? {
                return Ok(SetSelectionProgress::Yield);
            }
            let count = (guard.markers.len() - cursor.next_group)
                .min(novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize);
            let end = cursor.next_group + count;
            while cursor.next_group < end && cursor.indices.len() < rows.get() {
                let group_id = cursor.next_group;
                if S::marker_selected(&guard.markers[group_id], stage_total) {
                    cursor.indices.push(group_id as u64);
                }
                cursor.next_group += 1;
            }
        }
        Ok(SetSelectionProgress::Ready {
            exhausted: cursor.next_group == guard.markers.len(),
        })
    }
}
