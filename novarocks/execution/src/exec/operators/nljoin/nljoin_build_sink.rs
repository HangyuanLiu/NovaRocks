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
//! Nested-loop join build sink.
//!
//! Responsibilities:
//! - Collects build-side chunks for later nested-loop probing.
//! - Publishes build artifacts and completion state to NL-join probe operators.
//!
//! Key exported interfaces:
//! - Types: `NlJoinBuildSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::ExecutionResult;

use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{SinkKernel, SinkStep, Transform},
    step::NextStep,
    step_context::StepContext,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;

use super::nljoin_shared::{NlJoinBuildArtifact, NlJoinSharedState};

/// Factory for nested-loop join build sinks that materialize build-side chunks.
pub struct NlJoinBuildSinkFactory {
    name: String,
    state: Arc<NlJoinSharedState>,
}

impl NlJoinBuildSinkFactory {
    pub(crate) fn new(state: Arc<NlJoinSharedState>) -> Self {
        let node_id = state.node_id();
        let name = if node_id >= 0 {
            format!("NlJoinBuildSink (id={node_id})")
        } else {
            "NlJoinBuildSink".to_string()
        };
        Self { name, state }
    }
}

impl OperatorFactory for NlJoinBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(NlJoinBuildSinkOperator {
            name: self.name.clone(),
            state: Arc::clone(&self.state),
            build_batches: Vec::new(),
            build_batches_mem_tracker: None,
            artifact_memory: None,
        }))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct NlJoinBuildSinkOperator {
    name: String,
    state: Arc<NlJoinSharedState>,
    build_batches: Vec<Chunk>,
    build_batches_mem_tracker: Option<Arc<MemTracker>>,
    artifact_memory: Option<BuildArtifactMemory>,
}
struct BuildArtifactMemory {
    batches: Option<Arc<MemTracker>>,
}

impl OperatorLifecycle for NlJoinBuildSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        let batches = MemTracker::new_child("BuildBatches", &tracker);
        self.build_batches_mem_tracker = Some(Arc::clone(&batches));
        for chunk in self.build_batches.iter_mut() {
            chunk.transfer_to(&batches);
        }
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        let batches = state.mem_tracker().map(|root| {
            let artifact = MemTracker::new_child(
                format!("JoinBuildArtifact: {}", self.state.build_dep().name()),
                &root,
            );
            MemTracker::new_child("BuildBatches", &artifact)
        });
        self.artifact_memory = Some(BuildArtifactMemory { batches });
        Ok(())
    }
}
impl InputEncodingPolicy for NlJoinBuildSinkOperator {}
impl SinkKernel for NlJoinBuildSinkOperator {
    fn consume(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let mut chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if !chunk.is_empty() {
            if let Some(tracker) = self.build_batches_mem_tracker.as_ref() {
                chunk.transfer_to(tracker);
            }
            self.build_batches.push(chunk);
        }
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        let binding = self.artifact_memory.as_ref().ok_or_else(|| {
            novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "nested-loop join build runtime binding is missing",
                ),
            )
        })?;
        let mut batches = std::mem::take(&mut self.build_batches);
        if let Some(tracker) = &binding.batches {
            for chunk in &mut batches {
                chunk.transfer_to(tracker);
            }
        }
        let artifact = Arc::new(NlJoinBuildArtifact::new(batches));
        self.state.set_build(artifact)?;
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{
        chunk::{ChunkSchema, ChunkSlotSchema},
        pipeline::{
            dependency::DependencyManager, input::RawInput, step::InputResult,
            step_context::TurnWork,
        },
    };
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field},
    };
    use novarocks_types::SlotId;

    #[test]
    fn build_steps_publish_once_and_retained_rows_outlive_the_sink() {
        let root = MemTracker::new_root("nested-loop build shape");
        let runtime = RuntimeState::for_local_execution(
            None,
            None,
            None,
            None,
            None,
            Some(root.clone()),
            None,
        );
        let shared = Arc::new(NlJoinSharedState::new(17, DependencyManager::default()));
        let factory = NlJoinBuildSinkFactory::new(shared.clone());
        let mut sink = factory.create(1, 0);
        sink.set_mem_tracker(MemTracker::new_child("sink", &root));
        sink.bind_runtime_state(&runtime).unwrap();
        sink.prepare().unwrap();
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        let chunk =
            Chunk::try_new_with_columns(schema, vec![Arc::new(Int32Array::from(vec![7, 11]))])
                .unwrap();
        let control = runtime.error_state();
        let mut turn = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut ctx = StepContext::new(&control, &mut turn);
        let step = sink.as_mut();
        assert!(matches!(
            step.try_push(&mut ctx, RawInput::for_test(chunk)),
            InputResult::Accepted(Ok(_))
        ));
        assert!(!shared.build_dep().is_ready());
        let consumed = step.drive(&mut ctx).unwrap();
        assert!(consumed.output.is_none());
        assert_eq!(consumed.committed_rows, 2);
        assert!(matches!(consumed.next, NextStep::NeedInput));
        assert!(!shared.has_build());
        step.finish(&mut ctx).unwrap();
        assert!(
            !shared.has_build(),
            "EOS signals control without publishing payload work"
        );
        let finalized = step.drive(&mut ctx).unwrap();
        assert!(finalized.output.is_none());
        assert!(matches!(finalized.next, NextStep::Finished));
        assert!(shared.build_dep().is_ready());
        let artifact = shared.get_build().unwrap();
        assert_eq!(artifact.build_batches.len(), 1);
        let values = artifact.build_batches[0].columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[7, 11]);
        step.finish(&mut ctx).unwrap();
        assert!(matches!(
            step.drive(&mut ctx).unwrap().next,
            NextStep::Finished
        ));
        assert!(Arc::ptr_eq(&artifact, &shared.get_build().unwrap()));
        let last_alias = artifact.build_batches[0].clone();
        drop(sink);
        drop(factory);
        drop(shared);
        drop(artifact);
        assert!(
            root.current() > 0,
            "the retained batch owns its artifact charge"
        );
        drop(last_alias);
        assert_eq!(root.current(), 0);
    }
}
