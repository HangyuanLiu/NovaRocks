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
//! Streaming aggregate source operator for Phase 2 — reads pre-aggregated chunks.
//!
//! Responsibilities:
//! - Reads intermediate chunks from the shared `AggregateStreamingState` buffer filled by
//!   the paired `AggregateStreamingSinkOperator`.
//! - Signals the pipeline driver when new chunks are available via an `Observable`.
//! - Reports `is_finished()` once the sink is done and the buffer is fully drained.
//!
//! Key exported interfaces:
//! - Types: `AggregateStreamingSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use crate::exec::operators::aggregate::streaming_state::AggregateStreamingState;
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceRead};
use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};

/// Factory that constructs streaming aggregate source operators for Phase 2 pre-aggregation.
pub struct AggregateStreamingSourceFactory {
    name: String,
    state: AggregateStreamingState,
}

impl AggregateStreamingSourceFactory {
    pub(crate) fn new(node_id: i32, state: AggregateStreamingState) -> Self {
        let name = if node_id >= 0 {
            format!("AGG_STREAMING_SOURCE (id={node_id})")
        } else {
            "AGG_STREAMING_SOURCE".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for AggregateStreamingSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(AggregateStreamingSourceOperator {
            name: self.name.clone(),
            state: self.state.clone(),
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct AggregateStreamingSourceOperator {
    name: String,
    state: AggregateStreamingState,
}

impl OperatorLifecycle for AggregateStreamingSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }
}

impl SourceKernel for AggregateStreamingSourceOperator {
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        let observable = self.state.observable();
        let observed_generation = observable.generation();
        let (output, next) = match self.state.next_chunk() {
            SourceRead::Chunk(chunk) => (Some(chunk), NextStep::Runnable),
            SourceRead::Finished => (None, NextStep::Finished),
            SourceRead::Pending => (
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Input,
                    observable,
                    observed_generation,
                    deadline: None,
                }),
            ),
        };
        Ok(StepResult::new(output, next, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::step_context::{StepContext, TurnWork};
    use crate::runtime::runtime_state::RuntimeErrorState;
    use std::num::NonZeroU64;

    #[test]
    fn empty_streaming_source_waits_for_every_producer_before_eos() {
        let shared = AggregateStreamingState::new(2);
        let mut source = AggregateStreamingSourceFactory::new(7, shared.clone()).create(1, 0);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::new(1).unwrap(), 8);
        let mut context = StepContext::new(&control, &mut work);
        let source = source.as_mut();
        assert!(matches!(
            source.drive(&mut context).unwrap().next,
            NextStep::Wait(_)
        ));
        shared.mark_sink_finished();
        assert!(matches!(
            source.drive(&mut context).unwrap().next,
            NextStep::Wait(_)
        ));
        shared.mark_sink_finished();
        let eos = source.drive(&mut context).unwrap();
        assert!(eos.output.is_none());
        assert!(matches!(eos.next, NextStep::Finished));
    }
}
