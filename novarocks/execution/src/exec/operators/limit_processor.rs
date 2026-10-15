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
//! Row-limit processor for LIMIT/OFFSET semantics.
//!
//! Responsibilities:
//! - Tracks consumed and emitted row counts against configured limit and offset bounds.
//! - Truncates or drops chunks deterministically once the row budget is exhausted.
//!
//! Key exported interfaces:
//! - Types: `LimitProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::fragment::ExecutionResult;

use crate::exec::chunk::Chunk;

use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, Operator, ProcessorOperator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use std::sync::Arc;
use std::sync::Mutex;

/// Factory for limit processors that enforce LIMIT/OFFSET row budgets.
pub struct LimitProcessorFactory {
    name: String,
    state: Arc<Mutex<LimitState>>,
}

#[derive(Debug)]
struct LimitState {
    remaining_offset: usize,
    remaining_limit: Option<usize>,
}

impl LimitProcessorFactory {
    pub fn new(node_id: i32, limit: Option<usize>, offset: usize) -> Self {
        let name = if node_id >= 0 {
            format!("LIMIT (id={node_id})")
        } else {
            "LIMIT".to_string()
        };
        Self {
            name,
            state: Arc::new(Mutex::new(LimitState {
                remaining_offset: offset,
                remaining_limit: limit,
            })),
        }
    }
}

impl OperatorFactory for LimitProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        // A zero row budget is complete before any upstream input exists,
        // including when OFFSET is nonzero. The driver observes this terminal
        // state and stands down the input without waiting for its first chunk.
        let finished = self.state.lock().expect("limit state lock").remaining_limit == Some(0);
        Box::new(LimitProcessorOperator {
            name: self.name.clone(),
            state: Arc::clone(&self.state),
            pending_output: None,
            finishing: false,
            finished,
            construction_factory: None,
            prepared: false,
        })
    }
}

struct LimitProcessorOperator {
    name: String,
    state: Arc<Mutex<LimitState>>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finished: bool,
    construction_factory: Option<Arc<dyn InputConstructionFactory>>,
    prepared: bool,
}

impl Operator for LimitProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }

    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction_factory = Some(factory);
    }

    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.construction_factory.is_none() {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        self.prepared = true;
        Ok(())
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

impl ProcessorOperator for LimitProcessorOperator {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending_output.is_none()
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if !self.prepared {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        if self.finished {
            return Ok(());
        }
        if self.pending_output.is_some() {
            return Err("limit received input while output buffer is full"
                .to_string()
                .into());
        }
        let chunk_rows = chunk.len();
        if chunk_rows == 0 {
            return Ok(());
        }

        let (skip_rows, take_rows, exhausted_limit) = {
            let mut state = self.state.lock().expect("limit state lock");
            if state.remaining_limit == Some(0) {
                (0usize, 0usize, true)
            } else {
                let skip_rows = state.remaining_offset.min(chunk_rows);
                state.remaining_offset -= skip_rows;

                let available_rows = chunk_rows - skip_rows;
                let take_rows = match state.remaining_limit {
                    Some(remaining) => {
                        let take = remaining.min(available_rows);
                        state.remaining_limit = Some(remaining - take);
                        take
                    }
                    None => available_rows,
                };
                let exhausted_limit = state.remaining_limit == Some(0);
                (skip_rows, take_rows, exhausted_limit)
            }
        };

        if take_rows > 0 {
            let factory = self
                .construction_factory
                .as_deref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let owner = factory.begin_input_construction(&chunk)?;
            self.pending_output = Some(chunk.slice_typed_observed::<ConstructionFailure, _>(
                skip_rows,
                take_rows,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?);
        }

        if exhausted_limit && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let out = self.pending_output.take();
        if self.pending_output.is_none() {
            let exhausted_limit = {
                let state = self.state.lock().expect("limit state lock");
                state.remaining_limit == Some(0)
            };
            if self.finishing || exhausted_limit {
                self.finished = true;
            }
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn make_chunk(start: i32, rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let mut keys = Vec::with_capacity(rows);
        let mut vals = Vec::with_capacity(rows);
        for i in 0..rows {
            keys.push(start + i as i32);
            vals.push(format!("v{}", start + i as i32));
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .expect("build record batch");
        let pure = novarocks_type_contract::ValueTypeStorageLifetime::pure();
        let slots = batch
            .schema_ref()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let binding = novarocks_type_contract::TypedValueBinding::try_new(
                    novarocks_type_contract::ValueTypeRef::try_new(
                        match index {
                            0 => novarocks_type_contract::ValueKind::Scalar(DataType::Int32),
                            1 => novarocks_type_contract::ValueKind::String,
                            _ => unreachable!("fixture has two declared columns"),
                        },
                        field.is_nullable(),
                        pure.clone(),
                    )
                    .unwrap(),
                    novarocks_type_contract::ValueLayout::try_new(field.data_type(), pure.clone())
                        .unwrap(),
                )
                .unwrap();
                crate::exec::chunk::ChunkSlotSchema::try_new_typed(
                    SlotId::new(index as u32 + 1),
                    field.clone(),
                    binding,
                    None,
                    pure.clone(),
                )
                .unwrap()
            })
            .collect();
        let schema = Arc::new(crate::exec::chunk::ChunkSchema::try_new_typed(slots, pure).unwrap());
        Chunk::try_new_typed_with_columns_observed::<String, _>(
            schema,
            batch.columns().to_vec(),
            rows,
            |_| Ok(()),
        )
        .unwrap()
    }

    fn prepare_limit(op: &mut dyn Operator) {
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("limit fixture"),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        op.set_construction_factory(factory);
        op.prepare().unwrap();
    }

    fn chunk_values(chunk: &Chunk) -> Vec<i32> {
        let arr = chunk
            .columns()
            .first()
            .expect("first column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    #[test]
    fn zero_limit_is_finished_without_upstream_input_even_with_offset() {
        for offset in [0, 7, usize::MAX] {
            let mut op = LimitProcessorFactory::new(1, Some(0), offset).create(1, 0);
            assert!(
                op.is_finished(),
                "zero limit must not wait for offset {offset}"
            );
            let processor = op.as_processor_mut().expect("processor op");
            assert!(!processor.need_input());
            assert!(!processor.has_output());
            assert!(
                processor
                    .pull_chunk(&RuntimeState::default())
                    .unwrap()
                    .is_none()
            );
        }
        for limit in [None, Some(1), Some(4)] {
            let op = LimitProcessorFactory::new(1, limit, 7).create(1, 0);
            assert!(!op.is_finished());
            assert!(op.as_processor_ref().unwrap().need_input());
        }
    }

    #[test]
    fn limit_refuses_unprepared_input_before_consuming_row_budget() {
        let factory = LimitProcessorFactory::new(1, Some(2), 1);
        let mut op = factory.create(1, 0);
        let error = op
            .as_processor_mut()
            .unwrap()
            .push_chunk(&RuntimeState::default(), make_chunk(0, 4))
            .unwrap_err();
        assert!(matches!(
            error.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::MissingOwner
            )
        ));
        let state = factory.state.lock().unwrap();
        assert_eq!(state.remaining_offset, 1);
        assert_eq!(state.remaining_limit, Some(2));
        assert!(!op.as_processor_ref().unwrap().has_output());
    }

    #[test]
    fn limit_processor_applies_offset_then_limit() {
        let state = RuntimeState::default();
        let mut op = LimitProcessorFactory::new(1, Some(4), 3).create(1, 0);
        prepare_limit(op.as_mut());
        let p = op.as_processor_mut().expect("processor op");

        p.push_chunk(&state, make_chunk(0, 5))
            .expect("push chunk #1");
        let out1 = p
            .pull_chunk(&state)
            .expect("pull chunk #1")
            .expect("chunk #1 exists");
        assert_eq!(chunk_values(&out1), vec![3, 4]);

        p.push_chunk(&state, make_chunk(5, 5))
            .expect("push chunk #2");
        let out2 = p
            .pull_chunk(&state)
            .expect("pull chunk #2")
            .expect("chunk #2 exists");
        assert_eq!(chunk_values(&out2), vec![5, 6]);

        assert!(op.is_finished());
    }

    #[test]
    fn limit_processor_supports_offset_only() {
        let state = RuntimeState::default();
        let mut op = LimitProcessorFactory::new(2, None, 2).create(1, 0);
        prepare_limit(op.as_mut());
        let p = op.as_processor_mut().expect("processor op");

        p.push_chunk(&state, make_chunk(0, 3))
            .expect("push chunk #1");
        let out1 = p
            .pull_chunk(&state)
            .expect("pull chunk #1")
            .expect("chunk #1 exists");
        assert_eq!(chunk_values(&out1), vec![2]);

        p.push_chunk(&state, make_chunk(3, 2))
            .expect("push chunk #2");
        let out2 = p
            .pull_chunk(&state)
            .expect("pull chunk #2")
            .expect("chunk #2 exists");
        assert_eq!(chunk_values(&out2), vec![3, 4]);
        assert!(!op.is_finished());
    }
}
