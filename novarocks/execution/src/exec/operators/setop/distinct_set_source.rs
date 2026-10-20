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

//! Generic source side for DISTINCT set-operation result emission.
//!
//! Responsibilities:
//! - Reads shared state and emits rows selected by operation-specific set semantics.
//! - Handles completion ordering and shared error propagation before producing output.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use std::marker::PhantomData;
use std::sync::Arc;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{
    Source, SourceContractFailure, SourceKernel, SourceStart, source_contract_failure,
};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{ArrayRef, UInt64Array};
use std::num::NonZeroUsize;

use super::distinct_set_shared::{
    DistinctSetSemantics, DistinctSetSharedState, SetSelectionCursor, SetSelectionProgress,
};

pub struct DistinctSetSourceFactory<S: DistinctSetSemantics> {
    name: String,
    state: DistinctSetSharedState<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSourceFactory<S> {
    pub(crate) fn new(state: DistinctSetSharedState<S>, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("{} (id={node_id})", S::SOURCE_OPERATOR_NAME)
        } else {
            S::SOURCE_OPERATOR_NAME.to_string()
        };
        Self { name, state }
    }
}

impl<S: DistinctSetSemantics> OperatorFactory for DistinctSetSourceFactory<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(DistinctSetSourceOperator::<S> {
            name: self.name.clone(),
            state: self.state.clone(),
            output_chunk_schema: self.state.output_chunk_schema(),
            output: None,
            active: driver_id == 0,
            chunk_size: None,
            _semantics: PhantomData,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct DistinctSetSourceOperator<S: DistinctSetSemantics> {
    name: String,
    state: DistinctSetSharedState<S>,
    output_chunk_schema: ChunkSchemaRef,
    output: Option<OutputCursor>,
    active: bool,
    chunk_size: Option<NonZeroUsize>,
    _semantics: PhantomData<S>,
}

struct OutputCursor {
    arrays: Vec<ArrayRef>,
    selection: SetSelectionCursor,
}

impl<S: DistinctSetSemantics> OperatorLifecycle for DistinctSetSourceOperator<S> {
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.chunk_size = Some(NonZeroUsize::new(state.chunk_size()).ok_or_else(|| {
            source_contract_failure(SourceContractFailure::MissingRuntimeBinding)
        })?);
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.output = None;
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.output = None;
        Ok(())
    }
    fn cancel(&mut self) {
        self.output = None;
    }
}

impl<S: DistinctSetSemantics> SourceKernel for DistinctSetSourceOperator<S> {
    fn start(&self) -> SourceStart {
        if self.active {
            SourceStart::Ready
        } else {
            SourceStart::Finished
        }
    }
    fn poll_output(
        &mut self,
        ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        let chunk_size = self
            .chunk_size
            .ok_or_else(|| source_contract_failure(SourceContractFailure::MissingRuntimeBinding))?;
        let observable = self.state.controller().observable();
        let observed_generation = observable.generation();
        let output_stage = self.state.controller().stage_total();
        if !self.state.controller().is_stage_ready(output_stage) {
            return Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Dependency,
                    observable,
                    observed_generation,
                    deadline: None,
                }),
                0,
            ));
        }
        if ctx.work().remaining() == 0 {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        if self.output.is_none() {
            self.output = Some(OutputCursor {
                arrays: self.state.snapshot_arrays()?,
                selection: SetSelectionCursor::default(),
            });
        }
        let cursor = self
            .output
            .as_mut()
            .expect("the output snapshot is installed");
        let progress = self
            .state
            .advance_selection(&mut cursor.selection, chunk_size, || {
                ctx.checkpoint()?;
                Ok(ctx.work().try_consume(1))
            })?;
        let SetSelectionProgress::Ready { exhausted } = progress else {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        };
        if cursor.selection.is_empty() {
            self.output = None;
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        ctx.checkpoint()?;
        let indices_ref = Arc::new(UInt64Array::from(cursor.selection.take_indices())) as ArrayRef;
        let mut out_arrays = Vec::with_capacity(cursor.arrays.len());
        for column in &cursor.arrays {
            out_arrays.push(
                arrow::compute::take(column.as_ref(), &indices_ref, None).map_err(|source| {
                    super::failure::fail(super::failure::SetStateFailure::Gather(source))
                })?,
            );
        }
        if self.output_chunk_schema.slot_ids().len() != out_arrays.len() {
            return Err(source_contract_failure(
                SourceContractFailure::ColumnCount {
                    expected: self.output_chunk_schema.slot_ids().len(),
                    actual: out_arrays.len(),
                },
            ));
        }
        let output =
            Chunk::try_new_with_columns(Arc::clone(&self.output_chunk_schema), out_arrays)?;
        let next = if exhausted {
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        if matches!(next, NextStep::Finished) {
            self.output = None;
        }
        Ok(StepResult::new(Some(output), next, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{
        chunk::ChunkSchema,
        operators::setop::{except_shared::ExceptSemantics, set_op_stage::SetOpStageController},
        pipeline::step_context::{StepContext, TurnWork},
    };
    use crate::runtime::{mem_tracker::MemTracker, query_options::QueryOptions};
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_types::SlotId;
    use std::num::NonZeroU64;

    fn chunk(values: Vec<i32>) -> Chunk {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
    }
    #[test]
    fn selection_yields_with_one_cursor_and_emits_the_expected_rows_once() {
        let interval = novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK as usize;
        let count = interval * 2 + 7;
        let input = chunk((0..count as i32).collect());
        let shared = DistinctSetSharedState::<ExceptSemantics>::new(
            SetOpStageController::new("EXCEPT", vec![1, 1]).unwrap(),
            input.chunk_schema_ref(),
        );
        shared
            .ingest_build(&input, MemTracker::new_root("set source fixture"))
            .unwrap();
        shared.finish_stage(0).unwrap();
        shared
            .ingest_probe(1, &chunk((0..(interval * 2) as i32).collect()))
            .unwrap();
        shared.finish_stage(1).unwrap();
        let state = RuntimeState::for_local_execution(
            Some(QueryOptions {
                batch_size: Some(3),
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let mut source = DistinctSetSourceFactory::new(shared.clone(), 7).create(1, 0);
        source.bind_runtime_state(&state).unwrap();
        let control = state.error_state();
        let mut rows = Vec::new();
        let mut yields = 0;
        let mut ended = false;
        let expected = ((interval * 2) as i32..count as i32).collect::<Vec<_>>();
        let max_turns = count.div_ceil(interval) + expected.len().div_ceil(3);
        for turn in 1..=max_turns {
            let mut work = TurnWork::new(NonZeroU64::new(turn as u64).unwrap(), 1);
            let mut context = StepContext::new(&control, &mut work);
            let step = source.as_mut().drive(&mut context).unwrap();
            if let Some(output) = step.output {
                assert!(output.len() <= 3);
                let column = output.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                rows.extend(column.values().iter().copied());
            }
            if matches!(step.next, NextStep::Yield) {
                yields += 1;
            }
            if matches!(step.next, NextStep::Finished) {
                ended = true;
                break;
            }
            if matches!(step.next, NextStep::Runnable) {
                let spent = source.as_mut().drive(&mut context).unwrap();
                assert!(spent.output.is_none());
                assert!(matches!(spent.next, NextStep::Yield));
                yields += 1;
            }
        }
        assert!(ended);
        assert!(
            yields >= 2,
            "the long unselected prefix must yield instead of replaying or blocking"
        );
        assert_eq!(rows, expected);
        let mut inactive = DistinctSetSourceFactory::new(shared, 7).create(2, 1);
        let mut work = TurnWork::new(NonZeroU64::MIN, 1);
        let step = inactive
            .as_mut()
            .drive(&mut StepContext::new(&control, &mut work))
            .unwrap();
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Finished));
    }
}
