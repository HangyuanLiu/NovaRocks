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

use crate::runtime_filter::model::contract::LateApplyGranularity;
use crate::runtime_filter::model::graph::RuntimeFilterGraphData;
use crate::runtime_filter::model::validation::ActivationContract;

pub(super) type DraftRuntimeFilterGraph = RuntimeFilterGraphData<ActivationConstraint>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivationConstraint {
    LiveOnly {
        late_apply: LateApplyGranularity,
        reason: RequiredLiveReason,
    },
    BlockingOrBatchLive {
        fallback: ActivationFallback,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivationFallback {
    BlockingSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequiredLiveReason {
    OrderedBoundContract,
    FencedFinalDomainContract,
}

impl ActivationContract for ActivationConstraint {
    fn satisfies_required_non_blocking(&self) -> bool {
        matches!(self, Self::LiveOnly { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::model::contract::ConsumerActivation;
    use crate::runtime_filter::model::refined_wait_graph::ConsumerWaitBehavior;

    fn draft_wait(activation: &ActivationConstraint) -> ConsumerWaitBehavior {
        match activation {
            ActivationConstraint::LiveOnly { .. } => ConsumerWaitBehavior::NeverBlocks,
            ActivationConstraint::BlockingOrBatchLive { .. } => {
                ConsumerWaitBehavior::BlocksUntilComplete
            }
        }
    }

    fn sealed_wait(activation: ConsumerActivation) -> ConsumerWaitBehavior {
        match activation {
            ConsumerActivation::BlockingSnapshot => ConsumerWaitBehavior::BlocksUntilComplete,
            ConsumerActivation::NonBlockingLive { .. } => ConsumerWaitBehavior::NeverBlocks,
        }
    }

    #[test]
    fn activation_constraints_preserve_required_live_contract() {
        let ordered = ActivationConstraint::LiveOnly {
            late_apply: LateApplyGranularity::Batch,
            reason: RequiredLiveReason::OrderedBoundContract,
        };
        let fenced = ActivationConstraint::LiveOnly {
            late_apply: LateApplyGranularity::RowGroup,
            reason: RequiredLiveReason::FencedFinalDomainContract,
        };
        let fallback = ActivationConstraint::BlockingOrBatchLive {
            fallback: ActivationFallback::BlockingSnapshot,
        };

        assert!(ordered.satisfies_required_non_blocking());
        assert!(fenced.satisfies_required_non_blocking());
        assert!(!fallback.satisfies_required_non_blocking());
    }

    #[test]
    fn draft_graph_type_is_a_generic_graph_specialization() {
        let draft = DraftRuntimeFilterGraph::default();
        assert!(draft.is_empty());
    }

    #[test]
    fn draft_and_sealed_activation_project_to_the_same_wait_behavior() {
        assert_eq!(
            draft_wait(&ActivationConstraint::BlockingOrBatchLive {
                fallback: ActivationFallback::BlockingSnapshot,
            }),
            ConsumerWaitBehavior::BlocksUntilComplete
        );
        assert_eq!(
            sealed_wait(ConsumerActivation::BlockingSnapshot),
            ConsumerWaitBehavior::BlocksUntilComplete
        );
        assert_eq!(
            draft_wait(&ActivationConstraint::LiveOnly {
                late_apply: LateApplyGranularity::Batch,
                reason: RequiredLiveReason::OrderedBoundContract,
            }),
            ConsumerWaitBehavior::NeverBlocks
        );
    }
}
