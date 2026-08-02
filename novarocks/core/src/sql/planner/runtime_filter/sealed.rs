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

//! Immutable carrier for planner-sealed runtime-filter facts.

use std::sync::Arc;

use super::activation::ActivationDecisionCatalog;
use super::graph::RuntimeFilterGraph;
use super::progress::JoinBuildProgressCatalog;

/// The only planner semantic origin after activation has been decided.
///
/// Downstream preparation may retain a clone of this handle, but cannot mutate
/// or rebuild any of the contained planning facts.
#[derive(Clone, Debug)]
pub(crate) struct SealedRuntimeFilterPlan {
    graph: Arc<RuntimeFilterGraph>,
    activation_decisions: Arc<ActivationDecisionCatalog>,
    join_progress: Arc<JoinBuildProgressCatalog>,
}

impl SealedRuntimeFilterPlan {
    pub(crate) fn new(
        graph: RuntimeFilterGraph,
        activation_decisions: ActivationDecisionCatalog,
        join_progress: JoinBuildProgressCatalog,
    ) -> Self {
        Self {
            graph: Arc::new(graph),
            activation_decisions: Arc::new(activation_decisions),
            join_progress: Arc::new(join_progress),
        }
    }

    pub(crate) fn graph(&self) -> &RuntimeFilterGraph {
        &self.graph
    }

    pub(crate) fn activation_decisions(&self) -> &ActivationDecisionCatalog {
        &self.activation_decisions
    }

    pub(crate) fn join_progress(&self) -> &JoinBuildProgressCatalog {
        &self.join_progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_immutable_planner_facts() {
        let sealed = SealedRuntimeFilterPlan::new(
            RuntimeFilterGraph::default(),
            ActivationDecisionCatalog::new(),
            JoinBuildProgressCatalog::new(),
        );
        let clone = sealed.clone();
        assert!(std::ptr::eq(sealed.graph(), clone.graph()));
        assert!(std::ptr::eq(
            sealed.activation_decisions(),
            clone.activation_decisions()
        ));
        assert!(std::ptr::eq(sealed.join_progress(), clone.join_progress()));
    }
}
