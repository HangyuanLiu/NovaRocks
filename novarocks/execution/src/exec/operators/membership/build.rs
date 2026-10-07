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

//! The single local build driver's sink of one membership node.

use std::sync::Arc;

use super::shared::MembershipShared;
use crate::exec::chunk::Chunk;
use crate::exec::expr::json_in_pair::JsonPairTask;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::observable::Observable;
use crate::runtime::runtime_state::RuntimeState;

/// Creates the one RHS build sink of a membership node. The builder gathers
/// the build input to exactly one local driver.
pub(crate) struct MembershipBuildSinkFactory {
    name: String,
    shared: Arc<MembershipShared>,
}

impl MembershipBuildSinkFactory {
    pub(crate) fn new(shared: Arc<MembershipShared>) -> Self {
        Self {
            name: format!("MEMBERSHIP_BUILD_SINK (id={})", shared.node_id()),
            shared,
        }
    }
}

impl OperatorFactory for MembershipBuildSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(MembershipBuildSink {
            name: self.name.clone(),
            shared: Arc::clone(&self.shared),
            dop,
            task: None,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct MembershipBuildSink {
    name: String,
    shared: Arc<MembershipShared>,
    dop: i32,
    task: Option<JsonPairTask>,
    finished: bool,
}

impl MembershipBuildSink {
    fn task(&self) -> Result<&JsonPairTask, String> {
        self.task
            .as_ref()
            .ok_or_else(|| "membership RHS build sink is not bound to its task".to_string())
    }
}

impl Drop for MembershipBuildSink {
    fn drop(&mut self) {
        // A sink dropped without its normal finish never completes the RHS.
        if !self.finished {
            self.finished = true;
            self.shared.stop();
        }
    }
}

impl Operator for MembershipBuildSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.dop != 1 {
            return Err(format!(
                "membership node {} RHS requires exactly one local build driver, got {}",
                self.shared.node_id(),
                self.dop
            ));
        }
        self.task = Some(self.shared.bind(state)?);
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        // A sink closed without its normal finish never completes the RHS.
        if !self.finished {
            self.finished = true;
            self.shared.stop();
        }
        Ok(())
    }

    fn cancel(&mut self) {
        if !self.finished {
            self.finished = true;
            self.shared.stop();
        }
    }

    fn on_driver_failure(&mut self) {
        if self.finished {
            return;
        }
        self.finished = true;
        // Only the task's own typed cause is published to probes; any other
        // driver error reaches them through the fragment's first error.
        let failure = self.task.as_ref().and_then(JsonPairTask::task_failure);
        self.shared.fail(failure);
    }

    fn is_finished(&self) -> bool {
        self.finished || self.shared.consumers_gone()
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for MembershipBuildSink {
    fn need_input(&self) -> bool {
        !self.is_finished()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Err(format!(
                "membership node {} RHS input arrived after its local EOS",
                self.shared.node_id()
            ));
        }
        let task = self.task()?;
        self.shared.ingest(task, chunk)
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        let result = self.shared.complete(self.task()?);
        self.finished = true;
        result
    }

    fn early_finish_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(self.shared.consumers_left()))
    }
}
