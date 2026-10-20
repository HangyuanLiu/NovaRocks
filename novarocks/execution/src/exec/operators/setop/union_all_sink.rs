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
//! UNION ALL sink branch operator.
//!
//! Responsibilities:
//! - Pushes branch chunks into shared UNION ALL queues without deduplication.
//! - Marks branch completion for source-side termination detection.
//!
//! Key exported interfaces:
//! - Types: `UnionAllSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{SinkKernel, SinkStep, Transform},
    step::NextStep,
    step_context::StepContext,
};
use crate::runtime::mem_tracker::MemTracker;
use std::sync::Arc;

use super::union_all_shared::UnionAllSharedState;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for UNION ALL sink branches that enqueue branch chunks into shared state.
pub struct UnionAllSinkFactory {
    name: String,
    state: UnionAllSharedState,
}

impl UnionAllSinkFactory {
    pub(crate) fn new(state: UnionAllSharedState, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("UnionAllSink (id={node_id})")
        } else {
            "UnionAllSink".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for UnionAllSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(UnionAllSinkOperator {
            name: self.name.clone(),
            state: self.state.clone(),
            memory: None,
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct UnionAllSinkOperator {
    name: String,
    state: UnionAllSharedState,
    memory: Option<QueueInputMemory>,
}
struct QueueInputMemory {
    tracker: Option<Arc<MemTracker>>,
}

impl OperatorLifecycle for UnionAllSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.memory = Some(QueueInputMemory {
            tracker: self.state.queue_mem_tracker(state),
        });
        Ok(())
    }
}
impl InputEncodingPolicy for UnionAllSinkOperator {}
impl SinkKernel for UnionAllSinkOperator {
    fn consume(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let memory = self.memory.as_ref().ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "union input runtime binding is missing",
                ),
            )
        })?;
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if !chunk.is_empty() {
            self.state.push_chunk(memory.tracker.as_ref(), chunk);
        }
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        self.state.producer_finished();
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
